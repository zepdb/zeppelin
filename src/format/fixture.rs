//! Test-only-in-practice support for the tracked release artifact corpus.
//!
//! The public surface is deliberately narrow. Integration tests need to call
//! the crate's private builders and decoders, but the persisted-format registry
//! must remain the single inventory that decides whether a fixture is missing.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use ulid::Ulid;

use crate::compaction::gc::{
    gc_candidate_store_key, save_gc_candidates, CompactionStaging, GcCandidate,
};
use crate::config::{ApiKeyConfig, IndexingConfig, SecurityConfig};
use crate::embedding::artifact::encode_matrix_payload;
use crate::embedding::{
    ArtifactChecksum, CenteringArtifact, ContentHash, DeterministicDev, EmbeddingProfileId,
    EncoderDocumentInput, EncoderExecutionRef, EncoderInputRef, ExactScorerVersion, FdeArtifact,
    FdeArtifactRow, FdeGenerationId, InputModality, MatrixArtifact, MatrixArtifactRow, MatrixDtype,
    MultiVectorEmbedding, MultiVectorEncoder, MultiVectorEpoch, MultiVectorEpochId,
    NormalizationRecipe, RetrievalUnitRecord, TextContentRef, VectorTransformRecipe,
    DETERMINISTIC_DEV_IMPLEMENTATION, DETERMINISTIC_DEV_VERSION,
};
use crate::error::{Result, ZeppelinError};
use crate::fts::global_index::{global_fts_key, GlobalInvertedIndex};
use crate::fts::inverted_index::{fts_index_key, FtsFieldStats, FtsSegmentMeta, InvertedIndex};
use crate::fts::{FtsFieldConfig, FtsLanguage};
use crate::index::hierarchical::{
    build::build_hierarchical, deserialize_tree_node, serialize_tree_node, tree_node_key, TreeMeta,
};
use crate::index::ivf_flat::build::{
    attrs_key, bootstrap_key, build_ivf_flat, centroids_key, deserialize_attrs,
    deserialize_fixed_stride_f32_block, deserialize_id_block, parse_cluster_data_object_v5,
    serialize_bootstrap, serialize_cluster_data_object_v5, serialize_colocated_rq_cluster,
    Zbp5ClusterBlocks,
};
use crate::index::ivf_flat::membership::membership_key;
use crate::index::ivf_flat::sketch::{sketch_key, sketch_rotation_seed, ResidentSketch};
use crate::index::late_interaction::{
    build_late_interaction_segment, BuiltLateSegmentArtifact, CandidateFdeSource,
    FdeAlgorithmVersion, FdeParams, FinalProjection, FlatCalibrationSource, InnerProjection,
    LateCandidateBuild, LateCandidateBuildConfig, LateFlatCandidateBuildConfig, LateRoutingMetric,
    LateRowMatrixSource, LateSegmentBuildConfig, LateSegmentBuildRow,
};
use crate::index::quantization::pq::{deserialize_pq_cluster, serialize_pq_cluster, PqCodebook};
use crate::index::quantization::rabitq::StructuredRotation;
use crate::index::quantization::rq::{RqClusterCodes, RqClusterCodesOnly};
use crate::index::quantization::sq::{
    deserialize_sq_cluster, deserialize_sq_codes_only, serialize_sq_cluster,
    serialize_sq_codes_only, SqCalibration,
};
use crate::index::quantization::QuantizationType;
use crate::namespace::branching::deletion::BranchVisibilityRemovalMarker;
use crate::namespace::branching::NamespaceCreationKind;
use crate::namespace::manager::{
    CompactionHealth, NamespaceDestructionRecord, NamespaceIndexConfig, NamespaceMetadata,
    NamespaceState,
};
use crate::namespace::{BranchId, NamespaceId, NamespaceIncarnationId};
use crate::security::{AuditRecord, DecisionId, PolicyHead, PolicySnapshot, PrincipalId};
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, DistanceMetric, IndexType, VectorEntry};
use crate::wal::fragment::WalFragment;
use crate::wal::input_fragment::EncoderInputWalFragment;
use crate::wal::late_section::LateStateSection;
use crate::wal::lease::Lease;
use crate::wal::manifest::{
    CoarsePayloadEncoding, FragmentRef, Manifest, NamedSnapshot, SegmentRef,
};

use super::FORMATS;

/// Namespace embedded in the immutable unreleased fixture set.
pub const FIXTURE_NAMESPACE: &str = "artifact-corpus";
/// Dense vector dimension shared by fixture builders and structural probes.
pub const FIXTURE_DIMENSIONS: usize = 8;
/// Stable current IVF segment identity; no ULID is minted by the generator.
pub const FIXTURE_SEGMENT_ID: &str = "seg_01ARZ3NDEKTSV4RRFFQ69G5FAV";
/// Stable current WAL fragment identity.
pub const FIXTURE_FRAGMENT_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
/// Stable typed-input fragment identity.
pub const FIXTURE_INPUT_FRAGMENT_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAX";

/// One file emitted by a real persisted-artifact builder.
#[derive(Clone, Debug)]
pub struct GeneratedArtifact {
    /// Registry family that owns these bytes.
    pub family: &'static str,
    /// Relative path beneath one corpus version directory.
    pub path: String,
    /// Exact object-store key when the artifact participates in query-through.
    pub object_key: Option<String>,
    /// Builder or encoder that produced the bytes.
    pub producer: &'static str,
    /// Complete immutable bytes.
    pub bytes: Bytes,
}

/// Builder output plus the fixed ANN query used by the comparator.
#[derive(Clone, Debug)]
pub struct GeneratedCorpus {
    /// Every generated artifact, in stable relative-path order.
    pub artifacts: Vec<GeneratedArtifact>,
    /// Query vector selected from the fixed-seed input corpus.
    pub ann_query: Vec<f32>,
}

fn artifact(
    family: &'static str,
    path: impl Into<String>,
    object_key: Option<String>,
    producer: &'static str,
    bytes: impl Into<Bytes>,
) -> GeneratedArtifact {
    GeneratedArtifact {
        family,
        path: path.into(),
        object_key,
        producer,
        bytes: bytes.into(),
    }
}

fn fixed_ulid(value: &str, label: &str) -> Result<Ulid> {
    Ulid::from_string(value).map_err(|error| {
        ZeppelinError::Validation(format!("invalid fixed {label} ULID {value}: {error}"))
    })
}

fn fts_config() -> HashMap<String, FtsFieldConfig> {
    HashMap::from([(
        "body".to_string(),
        FtsFieldConfig {
            language: FtsLanguage::English,
            stemming: true,
            remove_stopwords: true,
            case_sensitive: false,
            k1: 1.2,
            b: 0.75,
            max_token_length: 40,
        },
    )])
}

fn fixed_namespace_metadata(now: DateTime<Utc>) -> NamespaceMetadata {
    NamespaceMetadata {
        name: FIXTURE_NAMESPACE.to_string(),
        dimensions: FIXTURE_DIMENSIONS,
        distance_metric: DistanceMetric::Euclidean,
        index_type: IndexType::IvfFlat,
        vector_count: 12,
        created_at: now,
        updated_at: now,
        state: NamespaceState::Active,
        destruction_record_key: None,
        deletion_intent: None,
        full_text_search: fts_config(),
        index_config: Some(NamespaceIndexConfig {
            nlist: 4,
            quantization: QuantizationType::TwoBit,
            pq_m: 2,
            hierarchical: false,
            fts_index: true,
            bitmap_index: true,
        }),
        compaction_health: CompactionHealth::default(),
        creation_kind: NamespaceCreationKind::Root,
        branch_identity: None,
        branch_prepare: None,
        branch_activation: None,
        late_interaction: None,
        incarnation_id: Some(NamespaceIncarnationId::from_uuid(uuid::Uuid::from_u128(
            0x11111111_2222_3333_4444_555555555555,
        ))),
    }
}

fn dense_config(quantization: QuantizationType) -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: 4,
        target_rows_per_cluster: 3,
        max_num_centroids: 4,
        default_nprobe: 4,
        max_nprobe: 4,
        kmeans_max_iterations: 8,
        quantization,
        pq_m: 2,
        bitmap_index: true,
        ..IndexingConfig::default()
    }
}

async fn get_artifact(store: &ZeppelinStore, key: &str) -> Result<Bytes> {
    store.get(key).await
}

fn usize_range(range: &std::ops::Range<u64>, label: &str) -> Result<std::ops::Range<usize>> {
    let start = usize::try_from(range.start)
        .map_err(|_| ZeppelinError::Serialization(format!("{label} start does not fit usize")))?;
    let end = usize::try_from(range.end)
        .map_err(|_| ZeppelinError::Serialization(format!("{label} end does not fit usize")))?;
    Ok(start..end)
}

/// Decode one corpus artifact with its registry probe and any required typed decoder.
pub fn validate_artifact(family: &str, bytes: &[u8]) -> Result<()> {
    let format = FORMATS
        .iter()
        .find(|format| format.name == family)
        .ok_or_else(|| ZeppelinError::Validation(format!("unknown format family {family}")))?;
    if format.current_version != 0 {
        (format.decode_probe)(bytes)?;
    }
    match family {
        "namespace_metadata" => NamespaceMetadata::from_bytes(bytes).map(drop),
        "manifest" => Manifest::from_bytes(bytes).map(drop),
        "namespace_lease" => serde_json::from_slice::<Lease>(bytes)
            .map(drop)
            .map_err(Into::into),
        "named_snapshot" => NamedSnapshot::from_bytes(bytes).map(drop),
        "source_payload" => {
            if bytes.is_empty() {
                Err(ZeppelinError::Serialization(
                    "source payload fixture is empty".to_string(),
                ))
            } else {
                Ok(())
            }
        }
        "cluster_id_block" => deserialize_id_block(bytes).map(drop),
        "cluster_f32_block" => {
            let stride = FIXTURE_DIMENSIONS * std::mem::size_of::<f32>();
            if bytes.len() % stride != 0 {
                return Err(ZeppelinError::Serialization(
                    "fixture f32 block is not fixed-stride".to_string(),
                ));
            }
            deserialize_fixed_stride_f32_block(bytes, bytes.len() / stride, FIXTURE_DIMENSIONS)
                .map(drop)
        }
        "rabitq_codes_only" => RqClusterCodesOnly::from_bytes(bytes)
            .map(drop)
            .map_err(|error| ZeppelinError::Index(error.to_string())),
        "cluster_attributes" => deserialize_attrs(bytes).map(drop),
        "sq8_calibration" => SqCalibration::from_bytes(bytes).map(drop),
        "sq8_cluster" => deserialize_sq_cluster(bytes).map(drop),
        "sq8_codes_only" => deserialize_sq_codes_only(bytes).map(drop),
        "pq_codebook" => PqCodebook::from_bytes(bytes).map(drop),
        "pq_cluster" => deserialize_pq_cluster(bytes).map(drop),
        "hierarchical_tree_metadata" => serde_json::from_slice::<TreeMeta>(bytes)
            .map(drop)
            .map_err(Into::into),
        "hierarchical_tree_node" => deserialize_tree_node(bytes).map(drop),
        "fts_segment_metadata" => FtsSegmentMeta::from_bytes(bytes).map(drop),
        "compaction_staging" => serde_json::from_slice::<CompactionStaging>(bytes)
            .map(drop)
            .map_err(Into::into),
        "branch_visibility_marker" => {
            serde_json::from_slice::<BranchVisibilityRemovalMarker>(bytes)
                .map(drop)
                .map_err(Into::into)
        }
        "security_policy_head" => {
            let head: PolicyHead = serde_json::from_slice(bytes)?;
            head.validate("_security").map_err(Into::into)
        }
        "security_policy_snapshot" => {
            let snapshot: PolicySnapshot = serde_json::from_slice(bytes)?;
            snapshot.verify_checksum().map_err(Into::into)
        }
        "security_audit_jsonl" => {
            for line in bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
            {
                serde_json::from_slice::<AuditRecord>(line)?;
            }
            Ok(())
        }
        "namespace_destruction_record" => NamespaceDestructionRecord::from_bytes(bytes).map(drop),
        _ => Ok(()),
    }
}

/// Decode a manifest with its namespace binding and checksum projection enforced.
pub fn validate_manifest_for_namespace(bytes: &[u8], namespace: &str) -> Result<()> {
    Manifest::from_bytes_for_namespace(bytes, namespace).map(drop)
}

/// Re-encode a current checksum-input artifact through its production encoder.
pub fn reencode_checksum_input(family: &str, bytes: &[u8]) -> Result<Bytes> {
    let format = FORMATS
        .iter()
        .find(|format| format.name == family)
        .ok_or_else(|| ZeppelinError::Validation(format!("unknown format family {family}")))?;
    if !format.checksum_input {
        return Err(ZeppelinError::Validation(format!(
            "format family {family} is not a checksum input"
        )));
    }
    match family {
        "manifest" => Manifest::from_bytes(bytes)?.to_bytes(),
        "wal_fragment" => WalFragment::from_bytes(bytes)?.to_bytes(),
        "input_wal_fragment" => EncoderInputWalFragment::from_bytes(bytes)?.to_bytes(),
        "ivf_bootstrap" => {
            let sections = crate::index::ivf_flat::build::deserialize_bootstrap(bytes)?;
            let summary = sections.filter_summary.ok_or_else(|| {
                ZeppelinError::Serialization(
                    "current bootstrap fixture has no filter summary".to_string(),
                )
            })?;
            serialize_bootstrap(
                sections.centroids,
                sections.sketch,
                &sections.bitmap_complete_fields,
                summary,
            )
        }
        "cluster_data_object" => {
            let layouts = parse_cluster_data_object_v5(bytes)?;
            let mut blocks = Vec::with_capacity(layouts.len());
            for layout in &layouts {
                let coarse = usize_range(&layout.coarse, "coarse")?;
                let ids = usize_range(&layout.ids, "IDs")?;
                let vectors = usize_range(&layout.vectors, "vectors")?;
                let vector_bytes = vectors.end - vectors.start;
                let dim = if layout.row_count == 0 {
                    FIXTURE_DIMENSIONS
                } else {
                    vector_bytes
                        .checked_div(layout.row_count)
                        .and_then(|stride| stride.checked_div(std::mem::size_of::<f32>()))
                        .ok_or_else(|| {
                            ZeppelinError::Serialization(
                                "cluster vector block dimension overflow".to_string(),
                            )
                        })?
                };
                blocks.push(Zbp5ClusterBlocks {
                    cluster_idx: layout.cluster_idx,
                    row_count: layout.row_count,
                    dim,
                    coarse: &bytes[coarse],
                    ids: &bytes[ids],
                    vectors: &bytes[vectors],
                });
            }
            Ok(serialize_cluster_data_object_v5(&blocks)?.bytes)
        }
        "resident_sketch" => ResidentSketch::from_bytes(bytes)?.to_current_bytes_for_fixture(),
        "late_state_section" => LateStateSection::from_bytes(bytes)?.to_bytes(),
        _ => Err(ZeppelinError::Validation(format!(
            "checksum-input family {family} has no corpus re-encoder"
        ))),
    }
}

/// Generate the current corpus with real builders over caller-supplied fixed data.
///
/// The caller supplies vectors from `tests/common/vectors.rs`; this seam owns
/// all format-specific construction so tests cannot re-declare wire structs.
pub async fn generate_current_corpus(
    store: &ZeppelinStore,
    vectors: &[VectorEntry],
    now: DateTime<Utc>,
) -> Result<GeneratedCorpus> {
    if vectors.len() != 12
        || vectors
            .iter()
            .any(|vector| vector.values.len() != FIXTURE_DIMENSIONS)
    {
        return Err(ZeppelinError::Validation(
            "artifact corpus requires exactly 12 fixed eight-dimensional vectors".to_string(),
        ));
    }
    let ann_query = vectors[0].values.clone();
    let mut artifacts = Vec::new();

    let metadata = fixed_namespace_metadata(now);
    artifacts.push(artifact(
        "namespace_metadata",
        "meta.json",
        Some(NamespaceMetadata::object_store_key(FIXTURE_NAMESPACE)),
        "NamespaceMetadata::to_bytes",
        metadata.to_bytes()?,
    ));

    let lease = Lease {
        holder_id: "artifact-generator".to_string(),
        fencing_token: 7,
        acquired_at: now,
        expires_at: now + chrono::Duration::seconds(30),
        version: None,
    };
    artifacts.push(artifact(
        "namespace_lease",
        "lease.json",
        None,
        "Lease serde JSON encoder",
        serde_json::to_vec_pretty(&lease)?,
    ));

    let snapshot = NamedSnapshot {
        generation: 1,
        created_at: now,
    };
    artifacts.push(artifact(
        "named_snapshot",
        "snapshot.bin",
        None,
        "NamedSnapshot::to_bytes",
        snapshot.to_bytes()?,
    ));

    let mut fragment = WalFragment::try_new(vectors[8..].to_vec(), vec!["retired-row".into()])?;
    fragment.id = fixed_ulid(FIXTURE_FRAGMENT_ID, "WAL fragment")?;
    let fragment_bytes = fragment.to_bytes()?;
    let fragment_key = WalFragment::object_store_key(FIXTURE_NAMESPACE, &fragment.id);
    artifacts.push(artifact(
        "wal_fragment",
        "wal_fragment.bin",
        Some(fragment_key.clone()),
        "WalFragment::try_new/to_bytes with fixed ULID",
        fragment_bytes.clone(),
    ));

    let input = EncoderInputRef::Text {
        content: TextContentRef::Inline("fixed deterministic source text".to_string()),
    };
    let record = RetrievalUnitRecord {
        id: "late-source-0".to_string(),
        content_hash: input.content_hash()?,
        input,
        parent_id: Some("parent-0".to_string()),
        unit_ordinal: Some(0),
        attributes: Some(HashMap::from([(
            "body".to_string(),
            AttributeValue::String("fixed semantic source".to_string()),
        )])),
    };
    let mut input_fragment = EncoderInputWalFragment::try_new(vec![record], Vec::new())?;
    input_fragment.id = fixed_ulid(FIXTURE_INPUT_FRAGMENT_ID, "input fragment")?;
    artifacts.push(artifact(
        "input_wal_fragment",
        "input_wal.bin",
        None,
        "EncoderInputWalFragment::try_new/to_bytes with fixed ULID",
        input_fragment.to_bytes()?,
    ));
    artifacts.push(artifact(
        "source_payload",
        "source_payload.bin",
        None,
        "caller-owned source payload",
        Bytes::from_static(b"fixed caller-owned source payload\n"),
    ));

    let index = build_ivf_flat(
        &vectors[..8],
        &dense_config(QuantizationType::TwoBit),
        store,
        FIXTURE_NAMESPACE,
        FIXTURE_SEGMENT_ID,
    )
    .await?;

    let centroids_key = centroids_key(FIXTURE_NAMESPACE, FIXTURE_SEGMENT_ID);
    artifacts.push(artifact(
        "ivf_centroids",
        "segment/centroids.bin",
        Some(centroids_key.clone()),
        "build_ivf_flat",
        get_artifact(store, &centroids_key).await?,
    ));
    let sketch_key = sketch_key(FIXTURE_NAMESPACE, FIXTURE_SEGMENT_ID);
    let sketch_bytes = get_artifact(store, &sketch_key).await?;
    artifacts.push(artifact(
        "resident_sketch",
        "segment/sketch.bin",
        Some(sketch_key.clone()),
        "build_ivf_flat/build_resident_sketch with fixed rotation seed",
        sketch_bytes.clone(),
    ));
    let bootstrap_key = bootstrap_key(FIXTURE_NAMESPACE, FIXTURE_SEGMENT_ID);
    let bootstrap_bytes = get_artifact(store, &bootstrap_key).await?;
    artifacts.push(artifact(
        "ivf_bootstrap",
        "segment/bootstrap.bin",
        Some(bootstrap_key.clone()),
        "build_ivf_flat/build_bootstrap_artifact",
        bootstrap_bytes.clone(),
    ));
    let bootstrap = crate::index::ivf_flat::build::deserialize_bootstrap(&bootstrap_bytes)?;
    let filter_summary = bootstrap.filter_summary.ok_or_else(|| {
        ZeppelinError::Serialization("fixture bootstrap omitted its v3 filter summary".to_string())
    })?;
    artifacts.push(artifact(
        "filter_cardinality_summary",
        "segment/filter_summary.bin",
        None,
        "build_ivf_flat/filter summary embedded in ZBS1 v3",
        Bytes::copy_from_slice(filter_summary),
    ));
    let membership_key = membership_key(FIXTURE_NAMESPACE, FIXTURE_SEGMENT_ID);
    artifacts.push(artifact(
        "ivf_membership",
        "segment/membership.bin",
        Some(membership_key.clone()),
        "build_ivf_flat/build_membership_artifact",
        get_artifact(store, &membership_key).await?,
    ));

    let mut first_layout = None;
    for (object_number, object_ref) in index.cluster_objects.iter().enumerate() {
        let bytes = get_artifact(store, &object_ref.key).await?;
        if first_layout.is_none() {
            let layout = parse_cluster_data_object_v5(&bytes)?
                .into_iter()
                .find(|layout| layout.row_count > 0)
                .ok_or_else(|| {
                    ZeppelinError::Serialization(
                        "fixture cluster object has no non-empty layout".to_string(),
                    )
                })?;
            first_layout = Some((bytes.clone(), layout));
        }
        artifacts.push(artifact(
            "cluster_data_object",
            format!("segment/cluster_data_{object_number}.bin"),
            Some(object_ref.key.clone()),
            "build_ivf_flat/serialize_cluster_data_object_v5",
            bytes,
        ));
    }

    let (first_object, layout) = first_layout.ok_or_else(|| {
        ZeppelinError::Serialization("fixture IVF build emitted no cluster objects".to_string())
    })?;
    let coarse_range = usize_range(&layout.coarse, "coarse")?;
    let ids_range = usize_range(&layout.ids, "IDs")?;
    let vectors_range = usize_range(&layout.vectors, "vectors")?;
    let coarse = Bytes::copy_from_slice(&first_object[coarse_range]);
    let ids_block = Bytes::copy_from_slice(&first_object[ids_range]);
    let vectors_block = Bytes::copy_from_slice(&first_object[vectors_range]);
    artifacts.push(artifact(
        "rabitq_codes_only",
        "segment/rabitq_codes_only.bin",
        None,
        "ZBP5 coarse block emitted by build_ivf_flat",
        coarse.clone(),
    ));
    artifacts.push(artifact(
        "cluster_id_block",
        "segment/cluster_ids.bin",
        None,
        "ZBP5 ID block emitted by build_ivf_flat",
        ids_block.clone(),
    ));
    artifacts.push(artifact(
        "cluster_f32_block",
        "segment/cluster_f32.bin",
        None,
        "ZBP5 fixed-stride f32 block emitted by build_ivf_flat",
        vectors_block.clone(),
    ));

    let ids = deserialize_id_block(&ids_block)?;
    let cluster_vectors =
        deserialize_fixed_stride_f32_block(&vectors_block, layout.row_count, FIXTURE_DIMENSIONS)?;
    let padded_vectors = cluster_vectors
        .iter()
        .map(|vector| {
            let mut padded = vector.clone();
            padded.resize(256, 0.0);
            padded
        })
        .collect::<Vec<_>>();
    let row_refs = padded_vectors.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let mut padded_centroid = index.centroids[layout.cluster_idx].clone();
    padded_centroid.resize(256, 0.0);
    let rotation = StructuredRotation::new(256, sketch_rotation_seed())?;
    let rq = RqClusterCodes::encode(&ids, &row_refs, &padded_centroid, &rotation)
        .map_err(|error| ZeppelinError::Index(error.to_string()))?;
    artifacts.push(artifact(
        "rabitq_cluster",
        "segment/rabitq_cluster.bin",
        None,
        "RqClusterCodes::encode/to_bytes",
        rq.to_bytes(),
    ));
    artifacts.push(artifact(
        "cluster_vector_section",
        "segment/cluster_twobit.bin",
        None,
        "serialize_colocated_rq_cluster incremental builder",
        serialize_colocated_rq_cluster(&padded_vectors, &rq, 256)?,
    ));

    let mut cluster_indexes = Vec::new();
    for cluster_idx in 0..index.num_clusters() {
        let attrs_key = attrs_key(FIXTURE_NAMESPACE, FIXTURE_SEGMENT_ID, cluster_idx);
        let attrs_bytes = get_artifact(store, &attrs_key).await?;
        let attrs = deserialize_attrs(&attrs_bytes)?;
        let attrs_refs = attrs.iter().map(Option::as_ref).collect::<Vec<_>>();
        let inverted = InvertedIndex::build(&attrs_refs, &fts_config());
        let fts_key = fts_index_key(FIXTURE_NAMESPACE, FIXTURE_SEGMENT_ID, cluster_idx);
        let fts_bytes = inverted.to_bytes()?;
        store.put(&fts_key, fts_bytes.clone()).await?;
        if cluster_idx == 0 {
            artifacts.push(artifact(
                "cluster_attributes",
                "segment/attrs_0.bin",
                Some(attrs_key.clone()),
                "build_ivf_flat/serialize_attrs",
                attrs_bytes,
            ));
            let bitmap_key = crate::index::bitmap::bitmap_key(
                FIXTURE_NAMESPACE,
                FIXTURE_SEGMENT_ID,
                cluster_idx,
            );
            artifacts.push(artifact(
                "cluster_bitmap_index",
                "segment/bitmap_0.bin",
                Some(bitmap_key.clone()),
                "build_ivf_flat/build_cluster_bitmaps",
                get_artifact(store, &bitmap_key).await?,
            ));
            artifacts.push(artifact(
                "fts_cluster_index",
                "segment/fts_0.bin",
                Some(fts_key.clone()),
                "InvertedIndex::build/to_bytes",
                fts_bytes,
            ));
        } else {
            artifacts.push(artifact(
                "cluster_attributes",
                format!("segment/attrs_{cluster_idx}.bin"),
                Some(attrs_key),
                "build_ivf_flat/serialize_attrs",
                attrs_bytes,
            ));
            let bitmap_key = crate::index::bitmap::bitmap_key(
                FIXTURE_NAMESPACE,
                FIXTURE_SEGMENT_ID,
                cluster_idx,
            );
            artifacts.push(artifact(
                "cluster_bitmap_index",
                format!("segment/bitmap_{cluster_idx}.bin"),
                Some(bitmap_key.clone()),
                "build_ivf_flat/build_cluster_bitmaps",
                get_artifact(store, &bitmap_key).await?,
            ));
            artifacts.push(artifact(
                "fts_cluster_index",
                format!("segment/fts_{cluster_idx}.bin"),
                Some(fts_key),
                "InvertedIndex::build/to_bytes",
                fts_bytes,
            ));
        }
        cluster_indexes.push(inverted);
    }
    let global =
        GlobalInvertedIndex::build(&cluster_indexes.iter().enumerate().collect::<Vec<_>>());
    let global_key = global_fts_key(FIXTURE_NAMESPACE, FIXTURE_SEGMENT_ID);
    let global_bytes = global.to_bytes()?;
    store.put(&global_key, global_bytes.clone()).await?;
    artifacts.push(artifact(
        "fts_global_index",
        "segment/global_fts.bin",
        Some(global_key.clone()),
        "GlobalInvertedIndex::build/to_bytes",
        global_bytes,
    ));

    let mut field_stats = BTreeMap::new();
    if let Some(field) = global.fields.get("body") {
        field_stats.insert(
            "body".to_string(),
            FtsFieldStats {
                doc_count: field.doc_count,
                avg_doc_length: field.avg_doc_length,
                term_doc_freqs: field
                    .postings
                    .iter()
                    .map(|(term, postings)| (term.clone(), postings.df))
                    .collect(),
            },
        );
    }
    let fts_meta = FtsSegmentMeta {
        fields: vec!["body".to_string()],
        total_docs: global.total_docs,
        field_stats,
    };
    artifacts.push(artifact(
        "fts_segment_metadata",
        "segment/fts_meta.json",
        None,
        "FtsSegmentMeta::to_bytes over production indexes",
        fts_meta.to_bytes()?,
    ));

    append_quantization_artifacts(store, vectors, &mut artifacts).await?;
    append_hierarchical_artifacts(store, vectors, &mut artifacts).await?;
    append_embedding_and_late_artifacts(&mut artifacts).await?;
    append_control_artifacts(store, now, &mut artifacts).await?;

    let late_state = LateStateSection::new();
    let late_bytes = late_state.to_bytes()?;
    let late_ref = late_state.reference(FIXTURE_NAMESPACE)?;
    store.put(&late_ref.key, late_bytes.clone()).await?;
    artifacts.push(artifact(
        "late_state_section",
        "late/state.bin",
        Some(late_ref.key.clone()),
        "LateStateSection::to_bytes",
        late_bytes,
    ));

    let segment = SegmentRef {
        id: FIXTURE_SEGMENT_ID.to_string(),
        vector_count: 8,
        cluster_count: index.num_clusters(),
        quantization: QuantizationType::TwoBit,
        hierarchical: false,
        bitmap_fields: {
            let mut fields = index.bitmap_fields.clone();
            fields.sort();
            fields
        },
        fts_fields: vec!["body".to_string()],
        has_global_fts: true,
        cluster_owners: Vec::new(),
        sketch: index.sketch_ref.clone(),
        cluster_objects: index.cluster_objects.clone(),
        bootstrap: index.bootstrap_ref.clone(),
        membership: index.membership_ref.clone(),
        artifact_origin: None,
    };
    let incarnation = uuid::Uuid::from_u128(0x11111111_2222_3333_4444_555555555555);
    let mut manifest = Manifest::new_at(now);
    manifest.bind_namespace_incarnation(incarnation)?;
    manifest.add_fragment_at(
        FragmentRef {
            id: fragment.id,
            vector_count: fragment.vectors.len(),
            delete_count: fragment.deletes.len(),
            sequence_number: 0,
            size_bytes: fragment_bytes.len() as u64,
            artifact_origin: None,
        },
        now,
    );
    manifest.add_segment_with_limits_at(segment, 1_000, 10, now);
    manifest.set_coarse_payload_encoding(FIXTURE_SEGMENT_ID, CoarsePayloadEncoding::TwoBit);
    manifest.late_state = Some(late_ref);
    manifest.write(store, FIXTURE_NAMESPACE).await?;
    let manifest_bytes =
        get_artifact(store, &Manifest::object_store_key(FIXTURE_NAMESPACE)).await?;
    artifacts.push(artifact(
        "manifest",
        "manifest.bin",
        Some(Manifest::object_store_key(FIXTURE_NAMESPACE)),
        "Manifest::write/to_bytes",
        manifest_bytes.clone(),
    ));
    let decoded_manifest = Manifest::from_bytes_for_namespace(&manifest_bytes, FIXTURE_NAMESPACE)?;
    artifacts.push(artifact(
        "manifest",
        "manifest_legacy_json.json",
        None,
        "Manifest serde JSON compatibility encoder",
        serde_json::to_vec_pretty(&decoded_manifest)?,
    ));

    artifacts.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(GeneratedCorpus {
        artifacts,
        ann_query,
    })
}

async fn append_quantization_artifacts(
    _store: &ZeppelinStore,
    vectors: &[VectorEntry],
    artifacts: &mut Vec<GeneratedArtifact>,
) -> Result<()> {
    let rows = vectors[..4]
        .iter()
        .map(|vector| vector.values.as_slice())
        .collect::<Vec<_>>();
    let ids = vectors[..4]
        .iter()
        .map(|vector| vector.id.clone())
        .collect::<Vec<_>>();

    let calibration = SqCalibration::calibrate(&rows, FIXTURE_DIMENSIONS);
    let sq_codes = rows
        .iter()
        .map(|row| calibration.encode(row))
        .collect::<Vec<_>>();
    artifacts.push(artifact(
        "sq8_calibration",
        "quantization/sq8_calibration.bin",
        None,
        "SqCalibration::calibrate/to_bytes",
        calibration.to_bytes(),
    ));
    artifacts.push(artifact(
        "sq8_cluster",
        "quantization/sq8_cluster.bin",
        None,
        "SqCalibration::encode/serialize_sq_cluster",
        serialize_sq_cluster(&ids, &sq_codes, FIXTURE_DIMENSIONS)?,
    ));
    artifacts.push(artifact(
        "sq8_codes_only",
        "quantization/sq8_codes_only.bin",
        None,
        "SqCalibration::encode/serialize_sq_codes_only",
        serialize_sq_codes_only(&sq_codes, FIXTURE_DIMENSIONS)?,
    ));

    let codebook = PqCodebook::train(&rows, FIXTURE_DIMENSIONS, 2, 8)?;
    let pq_codes = rows
        .iter()
        .map(|row| codebook.encode(row))
        .collect::<Vec<_>>();
    artifacts.push(artifact(
        "pq_codebook",
        "quantization/pq_codebook.bin",
        None,
        "PqCodebook::train/to_bytes",
        codebook.to_bytes(),
    ));
    artifacts.push(artifact(
        "pq_cluster",
        "quantization/pq_cluster.bin",
        None,
        "PqCodebook::encode/serialize_pq_cluster",
        serialize_pq_cluster(&ids, &pq_codes, 2)?,
    ));
    Ok(())
}

async fn append_hierarchical_artifacts(
    store: &ZeppelinStore,
    vectors: &[VectorEntry],
    artifacts: &mut Vec<GeneratedArtifact>,
) -> Result<()> {
    const SEGMENT: &str = "hierarchical_fixture";
    const ROOT_ID: &str = "root_01ARZ3NDEKTSV4RRFFQ69G5FAY";

    let mut config = dense_config(QuantizationType::None);
    config.leaf_size = Some(64);
    let built =
        build_hierarchical(&vectors[..4], &config, store, FIXTURE_NAMESPACE, SEGMENT).await?;
    let generated_root_key = tree_node_key(FIXTURE_NAMESPACE, SEGMENT, &built.meta.root_node_id);
    let generated_root = get_artifact(store, &generated_root_key).await?;
    let decoded_root = deserialize_tree_node(&generated_root)?;
    let normalized_root = serialize_tree_node(&decoded_root, FIXTURE_DIMENSIONS);
    let normalized_meta = TreeMeta {
        root_node_id: ROOT_ID.to_string(),
        ..built.meta.clone()
    };
    artifacts.push(artifact(
        "hierarchical_tree_metadata",
        "hierarchical/tree_meta.json",
        None,
        "build_hierarchical plus fixed root-ULID normalization",
        serde_json::to_vec_pretty(&normalized_meta)?,
    ));
    artifacts.push(artifact(
        "hierarchical_tree_node",
        "hierarchical/tree_node.bin",
        None,
        "build_hierarchical/serialize_tree_node after fixed root-ULID normalization",
        normalized_root,
    ));
    Ok(())
}

fn deterministic_dev_epoch() -> Result<MultiVectorEpoch> {
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
            supported_modalities: vec![InputModality::Text],
        },
        preprocessing_digest: ArtifactChecksum::digest(b"golden-artifact-corpus-v1"),
        vector_dimension: FIXTURE_DIMENSIONS as u32,
        max_query_vectors: 8,
        max_document_vectors: 8,
        output_normalization: NormalizationRecipe::L2,
        exact_scoring_transform: VectorTransformRecipe::Identity,
        matrix_dtype: MatrixDtype::F16,
        exact_scorer: ExactScorerVersion::MaxSimV1,
    };
    epoch.id = epoch.canonical_id()?;
    Ok(epoch)
}

async fn append_embedding_and_late_artifacts(artifacts: &mut Vec<GeneratedArtifact>) -> Result<()> {
    let epoch = deterministic_dev_epoch()?;
    // This is the sole `allow_dev = true` construction in the fixture path.
    let encoder = DeterministicDev::new(true, &epoch)?;
    let inputs = [
        "golden airship searches the northern sky",
        "durable object storage keeps immutable segments",
        "lexical and vector retrieval share one manifest",
    ]
    .into_iter()
    .map(|text| {
        let input = EncoderInputRef::Text {
            content: TextContentRef::Inline(text.to_string()),
        };
        let hash = input.content_hash()?;
        EncoderDocumentInput::new(input, hash, None)
    })
    .collect::<Result<Vec<_>>>()?;
    let batch = encoder.encode_documents(&inputs).await?;

    let fde_params = FdeParams {
        algorithm: FdeAlgorithmVersion::PaperV1,
        repetitions: 2,
        simhash_bits: 1,
        input_dimension: FIXTURE_DIMENSIONS as u32,
        inner: InnerProjection::Rademacher { d_proj: 4 },
        final_projection: FinalProjection::None,
    };
    let transform = crate::index::late_interaction::FdeTransform::generate(&fde_params, 17)?;
    let transform_bytes = transform.to_bytes();
    let fde_generation = FdeGenerationId::new([4; 32]);
    let source_checksum = 0x1020_3040_5060_7080_u64;

    let mut matrix_rows = Vec::new();
    let mut fde_rows = Vec::new();
    let mut segment_rows = Vec::new();
    for (ordinal, (input, embedding)) in inputs.iter().zip(batch.embeddings().iter()).enumerate() {
        let content_hash = input.content_hash();
        matrix_rows.push(MatrixArtifactRow::new(content_hash, embedding.clone()));
        let fde = transform.encode_document(&embedding.matrix_ref()?)?;
        fde_rows.push(FdeArtifactRow::new(
            content_hash,
            fde.clone(),
            transform.output_dimension(),
        )?);
        segment_rows.push(LateSegmentBuildRow {
            id: format!("late-{ordinal}"),
            content_hash,
            source_sequence: ordinal as u64 + 1,
            parent_id: Some("golden-parent".to_string()),
            unit_ordinal: Some(ordinal as u32),
            attributes: Some(HashMap::from([(
                "body".to_string(),
                AttributeValue::String(format!("late fixture row {ordinal}")),
            )])),
            matrix: LateRowMatrixSource::Fresh {
                exact_payload: encode_matrix_payload(
                    MatrixDtype::F16,
                    FIXTURE_DIMENSIONS,
                    embedding,
                )?,
                exact_matrix: embedding.clone(),
            },
            fde: CandidateFdeSource::Raw(fde),
        });
    }

    let dev_matrix = MatrixArtifact::new(
        MatrixDtype::F16,
        epoch.id,
        source_checksum,
        FIXTURE_DIMENSIONS,
        matrix_rows,
    )?
    .to_bytes()?;
    let probe_matrix = MatrixArtifact::new(
        MatrixDtype::F16,
        MultiVectorEpochId::new([0; 32]),
        0,
        1,
        vec![MatrixArtifactRow::new(
            ContentHash::new([1; 32]),
            MultiVectorEmbedding::new(vec![0.5], 1, 1, 1)?,
        )],
    )?
    .to_bytes()?;
    artifacts.push(artifact(
        "matrix_fragment",
        "late/matrix_fragment.bin",
        None,
        "DeterministicDev/MatrixArtifact::new/to_bytes",
        probe_matrix.bytes().clone(),
    ));

    let _dev_fde = FdeArtifact::new(
        fde_generation,
        dev_matrix.checksum(),
        transform.output_dimension(),
        fde_rows,
    )?
    .to_bytes()?;
    let probe_fde = FdeArtifact::new(
        FdeGenerationId::new([0; 32]),
        ArtifactChecksum::new([0; 32]),
        1,
        vec![FdeArtifactRow::new(
            ContentHash::new([2; 32]),
            vec![0.25],
            1,
        )?],
    )?
    .to_bytes()?;
    artifacts.push(artifact(
        "fde_fragment",
        "late/fde_fragment.bin",
        None,
        "FdeTransform::encode_document/FdeArtifact::new/to_bytes",
        probe_fde.bytes().clone(),
    ));
    artifacts.push(artifact(
        "fde_transform",
        "late/fde_transform.bin",
        None,
        "FdeTransform::generate with fixed seed/to_bytes",
        transform_bytes,
    ));
    artifacts.push(artifact(
        "centering",
        "late/centering.bin",
        None,
        "CenteringArtifact::new/to_bytes",
        CenteringArtifact::new(vec![0.0])?
            .to_bytes()?
            .bytes()
            .clone(),
    ));

    let common = |segment_id: &str, candidate| LateSegmentBuildConfig {
        namespace: FIXTURE_NAMESPACE.to_string(),
        segment_id: segment_id.to_string(),
        profile: EmbeddingProfileId::new("golden-profile"),
        semantic_epoch: epoch.id,
        fde_generation,
        matrix_dtype: MatrixDtype::F16,
        vector_dimension: FIXTURE_DIMENSIONS,
        fde_dimension: transform.output_dimension(),
        coverage_sequence: 3,
        max_matrix_object_bytes: 64 * 1024,
        max_attribute_object_bytes: 64 * 1024,
        candidate,
        artifact_origin: None,
        fts_artifacts: Vec::new(),
        carried_matrix_blocks: Vec::new(),
        flat_calibration: FlatCalibrationSource::Recalibrate,
    };
    let ivf = build_late_interaction_segment(
        common(
            "late_ivf_fixture",
            LateCandidateBuild::Ivf(LateCandidateBuildConfig {
                fde_dimension: transform.output_dimension(),
                nlist: 2,
                probe_budget: 2,
                candidate_k: 3,
                routing_metric: LateRoutingMetric::NegativeL2,
                kmeans_max_iters: 8,
                kmeans_epsilon: 1e-6,
                max_cluster_bytes: 64 * 1024,
                max_bootstrap_bytes: 64 * 1024,
            }),
        ),
        segment_rows.clone(),
    )?;
    append_late_segment_outputs(&ivf.artifacts, artifacts, false)?;

    let flat = build_late_interaction_segment(
        common(
            "late_flat_fixture",
            LateCandidateBuild::FlatSq8(LateFlatCandidateBuildConfig {
                fde_dimension: transform.output_dimension(),
                candidate_k: 3,
                max_artifact_bytes: 64 * 1024,
            }),
        ),
        segment_rows,
    )?;
    append_late_segment_outputs(&flat.artifacts, artifacts, true)?;
    Ok(())
}

fn append_late_segment_outputs(
    outputs: &[BuiltLateSegmentArtifact],
    artifacts: &mut Vec<GeneratedArtifact>,
    flat_only: bool,
) -> Result<()> {
    let mut captured_matrix = false;
    let mut captured_attributes = false;
    let mut candidate_cluster = 0_usize;
    for output in outputs {
        let bytes = &output.bytes;
        let (family, path) = if bytes.starts_with(b"ZLB1") && !flat_only {
            (
                "late_candidate_bootstrap",
                "late/candidate_bootstrap.bin".to_string(),
            )
        } else if bytes.starts_with(b"ZLC1") && !flat_only {
            let path = format!("late/candidate_cluster_{candidate_cluster}.bin");
            candidate_cluster += 1;
            ("late_candidate_cluster", path)
        } else if bytes.starts_with(b"ZFQ1") && flat_only {
            ("late_flat_candidate", "late/flat_candidate.bin".to_string())
        } else if bytes.starts_with(b"ZMB1") && !flat_only && !captured_matrix {
            captured_matrix = true;
            ("late_matrix_block", "late/matrix_block.bin".to_string())
        } else if bytes.starts_with(b"ZAB1") && !flat_only && !captured_attributes {
            captured_attributes = true;
            (
                "late_attribute_block",
                "late/attribute_block.bin".to_string(),
            )
        } else {
            continue;
        };
        artifacts.push(artifact(
            family,
            path,
            None,
            "build_late_interaction_segment",
            bytes.clone(),
        ));
    }
    Ok(())
}

async fn append_control_artifacts(
    store: &ZeppelinStore,
    now: DateTime<Utc>,
    artifacts: &mut Vec<GeneratedArtifact>,
) -> Result<()> {
    artifacts.push(artifact(
        "scoped_ann_descriptor",
        "scoped/ann_descriptor.json",
        None,
        "ScopedAnnDescriptor::to_bytes",
        crate::retrieval_scope::scoped_ann_descriptor_fixture(4)?,
    ));
    artifacts.push(artifact(
        "scoped_fts_index",
        "scoped/fts_index.bin",
        None,
        "ScopedFtsIndex::to_bytes",
        crate::retrieval_scope::scoped_fts_artifact_fixture(1)?,
    ));
    artifacts.push(artifact(
        "quarantine_evidence",
        "late/quarantine_evidence.bin",
        None,
        "enrichment quarantine evidence encoder",
        crate::embedding::coordinator::quarantine_evidence_fixture()?,
    ));

    let staging = CompactionStaging {
        fencing_token: 7,
        keys: BTreeSet::from([
            format!("{FIXTURE_NAMESPACE}/segments/staged/bootstrap.bin"),
            format!("{FIXTURE_NAMESPACE}/segments/staged/cluster_0.bin"),
        ]),
    };
    artifacts.push(artifact(
        "compaction_staging",
        "control/compaction_staging.json",
        None,
        "CompactionStaging serde JSON encoder",
        serde_json::to_vec_pretty(&staging)?,
    ));

    let gc_candidates = [GcCandidate {
        key: format!("{FIXTURE_NAMESPACE}/segments/orphan/cluster_0.bin"),
        first_seen_unreachable_at: now,
        unreachable_since_manifest_version: 6,
    }];
    save_gc_candidates(store, FIXTURE_NAMESPACE, &gc_candidates).await?;
    let gc_key = gc_candidate_store_key(FIXTURE_NAMESPACE);
    artifacts.push(artifact(
        "gc_candidate_ledger",
        "control/gc_candidates.json",
        None,
        "save_gc_candidates",
        get_artifact(store, &gc_key).await?,
    ));

    let namespace = NamespaceId::parse(FIXTURE_NAMESPACE.to_string())
        .map_err(|_| ZeppelinError::Validation("fixture namespace ID is invalid".to_string()))?;
    let incarnation = NamespaceIncarnationId::from_uuid(uuid::Uuid::from_u128(
        0x11111111_2222_3333_4444_555555555555,
    ));
    let branch_id = BranchId::from_ulid(fixed_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAZ", "branch")?);
    let marker = BranchVisibilityRemovalMarker {
        domain: BranchVisibilityRemovalMarker::DOMAIN.to_string(),
        branch_id,
        target_namespace: namespace.clone(),
        target_incarnation: incarnation.clone(),
        fenced_generation: 9,
        destruction_record_key: format!(
            "{FIXTURE_NAMESPACE}/_lifecycle/destruction/01ARZ3NDEKTSV4RRFFQ69G5FB0.json"
        ),
        intent_sha256: "11".repeat(32),
        parent_root_sha256: "22".repeat(32),
        reader_safety_floor_secs: 30,
    };
    artifacts.push(artifact(
        "branch_visibility_marker",
        "control/branch_visibility_marker.json",
        None,
        "BranchVisibilityRemovalMarker serde JSON encoder",
        serde_json::to_vec_pretty(&marker)?,
    ));

    let mut security = SecurityConfig::default();
    security.api_keys = vec![ApiKeyConfig {
        key_id: "zpk1_golden".to_string(),
        name: "golden-corpus".to_string(),
        sha256_hex: "33".repeat(32),
        actions: vec!["*".to_string()],
        namespaces: vec!["*".to_string()],
        expires_at: None,
    }];
    let policy_snapshot = PolicySnapshot::from_bootstrap(&security, now)?;
    let policy_object_key = "_security/policies/01ARZ3NDEKTSV4RRFFQ69G5FB1.json".to_string();
    let policy_head = PolicyHead::new(&policy_snapshot, policy_object_key)?;
    artifacts.push(artifact(
        "security_policy_head",
        "security/policy_head.json",
        None,
        "PolicyHead::new/production JSON encoder",
        serde_json::to_vec(&policy_head)?,
    ));
    artifacts.push(artifact(
        "security_policy_snapshot",
        "security/policy_snapshot.json",
        None,
        "PolicySnapshot::from_bootstrap/production JSON encoder",
        serde_json::to_vec(&policy_snapshot)?,
    ));
    artifacts.push(artifact(
        "security_audit_jsonl",
        "security/audit_records.jsonl",
        None,
        "AuditRecord::open_unsafe_boot/to_json_line",
        AuditRecord::open_unsafe_boot(now, "golden-node").to_json_line()?,
    ));

    let decision_id: DecisionId = serde_json::from_str("\"01ARZ3NDEKTSV4RRFFQ69G5FB2\"")?;
    let destruction = NamespaceDestructionRecord {
        namespace,
        manifest_version_destroyed: 9,
        object_count: 17,
        byte_count: 4_096,
        actor: PrincipalId::new("golden-operator")?,
        approver: Some(PrincipalId::new("golden-approver")?),
        decision_id,
        parent_root: None,
        incarnation: Some(incarnation),
        preservation_head: None,
        ts: now,
    };
    artifacts.push(artifact(
        "namespace_destruction_record",
        "control/namespace_destruction_record.json",
        None,
        "NamespaceDestructionRecord::to_bytes",
        destruction.to_bytes()?,
    ));
    Ok(())
}
