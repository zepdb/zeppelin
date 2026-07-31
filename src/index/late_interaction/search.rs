//! Exhaustive exact search over manifest-selected semantic overlays.
//!
//! The root manifest and its content-addressed late section are acquired as one
//! owned snapshot. Input fragments are replayed before any matrix is scored so
//! tombstones and newer uncovered versions suppress every older derived row.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use crate::cache::DiskCache;
use crate::config::MmliSegmentConfig;
use crate::embedding::transform::{apply_vector_transform, load_vector_transform_mean};
use crate::embedding::{
    ArtifactChecksum, ContentHash, EmbeddingProfileId, EmbeddingProfileRef, EncoderQueryInput,
    FdeGenerationId, MatrixArtifact, MultiVectorEncoderProvider, MultiVectorEpochId,
    RetrievalUnitRecord, SemanticCoverageState, SemanticState,
};
use crate::error::{Result, ZeppelinError};
use crate::index::filter::evaluate_filter_on_optional_attributes;
use crate::index::topk::TopK;
use crate::namespace::branching::ArtifactOrigin;
use crate::storage::read_plan::ReadPlanConfig;
use crate::storage::{NamespaceObjectFamily, ZeppelinStore};
use crate::types::{AttributeValue, ConsistencyLevel, Filter, VectorId};
use crate::wal::{EncoderInputWalFragment, LateInteractionSegmentRef, LateStateSection, Manifest};

use super::segment_search::{
    search_segment, SegmentSearchBounds, SegmentSearchRequest, SegmentSearchTrace,
};
use super::{max_sim, FdeTransform, LateInteractionError};

const SEMANTIC_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Whether a strong query may advance from its initial root to a newer live root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestRefresh {
    /// Poll the namespace's authoritative live manifest until the wait budget ends.
    Live,
    /// Keep the supplied root fixed, as required by historical and snapshot reads.
    Fixed,
}

/// Coverage represented by the exact root-plus-section snapshot used for scoring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LateInteractionCoverage {
    /// Every live source version has applicable derived output.
    Complete,
    /// Eventual search omitted one or more pending or failed live versions.
    Partial,
}

/// Immutable identity of the semantic recipe and root used for one result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LateInteractionProvenance {
    /// Root-manifest generation used throughout the search.
    pub manifest_generation: u64,
    /// Active operator-facing profile.
    pub profile: EmbeddingProfileId,
    /// Exact multi-vector encoder epoch.
    pub epoch: MultiVectorEpochId,
    /// Exact MUVERA fixed-dimensional generation.
    pub fde_generation: FdeGenerationId,
    /// Consistency semantics actually used.
    pub consistency: ConsistencyLevel,
}

/// One exact MaxSim result, ordered best first.
#[derive(Clone, Debug, PartialEq)]
pub struct LateInteractionRankedResult {
    /// Caller-owned retrieval-unit identity.
    pub id: VectorId,
    /// Raw higher-is-better MaxSim score.
    pub score: f32,
    /// Optional parent identity retained by typed ingest.
    pub parent_id: Option<String>,
    /// Optional unit ordinal retained by typed ingest.
    pub unit_ordinal: Option<u32>,
    /// Attributes from the final live source version.
    pub attributes: Option<HashMap<String, AttributeValue>>,
    /// Semantic recipe and root used to produce this result.
    pub provenance: LateInteractionProvenance,
}

/// Complete result of one exhaustive late-interaction search.
#[derive(Debug)]
pub struct LateInteractionSearchOutput {
    /// Best-first exact results.
    pub results: Vec<LateInteractionRankedResult>,
    /// Root manifest used for encoding, filtering, and scoring.
    pub manifest: Manifest,
    /// Snapshot-wide semantic provenance.
    pub provenance: LateInteractionProvenance,
    /// Whether every live source version was represented.
    pub semantic_coverage: LateInteractionCoverage,
    /// Highest contiguous mutation sequence with applicable output.
    pub covered_sequence: u64,
    /// Live source versions omitted because enrichment is pending.
    pub pending_records: u64,
    /// Live source versions omitted because enrichment failed.
    pub failed_records: u64,
    /// Planned row-dependent reads when an immutable segment was searched.
    pub read_trace: Option<LateInteractionReadTrace>,
}

/// Read-plan accounting for one candidate or truth wave.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LateInteractionWaveTrace {
    /// Logical ranges submitted to the planner.
    pub logical_ranges: usize,
    /// Physical ranged requests emitted by the planner.
    pub planned_requests: usize,
    /// Physical bytes including coalesced gaps.
    pub planned_bytes: u64,
}

/// Query-visible accounting for both immutable-segment read waves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LateInteractionReadTrace {
    /// Candidate-cluster wave.
    pub candidate_wave: LateInteractionWaveTrace,
    /// Exact matrix-and-attribute wave.
    pub truth_wave: LateInteractionWaveTrace,
}

/// Typed inputs to the exhaustive late-interaction search engine.
pub struct LateInteractionSearchRequest<'a> {
    /// Object-storage source of truth.
    pub store: &'a ZeppelinStore,
    /// Existing immutable-artifact cache used only for resident bootstrap bytes.
    pub bootstrap_cache: Option<&'a DiskCache>,
    /// Active provider shared with document enrichment.
    pub encoder_provider: &'a dyn MultiVectorEncoderProvider,
    /// Logical namespace whose live root may be polled.
    pub namespace: &'a str,
    /// Owned initial root selected by the caller's consistency/snapshot policy.
    pub manifest: Manifest,
    /// Non-empty text passed only to the active profile's query adapter.
    pub text: &'a str,
    /// Maximum number of exact results.
    pub top_k: usize,
    /// Server-owned mandatory filter already combined with the caller filter.
    pub effective_filter: Option<&'a Filter>,
    /// Existing query consistency contract.
    pub consistency: ConsistencyLevel,
    /// Maximum strong semantic-coverage wait.
    pub semantic_wait: Duration,
    /// Maximum aggregate bytes of selected exact matrix objects.
    pub max_overlay_bytes: u64,
    /// Late-segment build/query bounds selected by configuration and the lab.
    pub segment_config: MmliSegmentConfig,
    /// Whether strong semantic wait may select a newer live root.
    pub manifest_refresh: ManifestRefresh,
}

/// Encode and exhaustively MaxSim-score every applicable live overlay row.
///
/// Strong mode waits only by acquiring a new complete root-plus-section pair;
/// eventual mode scores covered final versions and reports partial coverage.
pub async fn search(
    request: LateInteractionSearchRequest<'_>,
) -> Result<LateInteractionSearchOutput> {
    let LateInteractionSearchRequest {
        store,
        bootstrap_cache,
        encoder_provider,
        namespace,
        manifest,
        text,
        top_k,
        effective_filter,
        consistency,
        semantic_wait,
        max_overlay_bytes,
        segment_config,
        manifest_refresh,
    } = request;
    let execution = SearchExecution {
        store,
        bootstrap_cache,
        encoder_provider,
        text,
        top_k,
        effective_filter,
        consistency,
        max_overlay_bytes,
        segment_config,
    };
    if text.trim().is_empty() {
        return Err(ZeppelinError::LateInteractionQueryEmpty);
    }
    let requested_generation = manifest.version();
    let requested_incarnation = manifest.local_origin()?.incarnation.as_uuid();
    let started = tokio::time::Instant::now();
    let mut manifest = manifest;

    loop {
        let snapshot = OwnedLateSnapshot::load(store, manifest).await?;
        let replay = replay_snapshot(store, &snapshot).await?;
        let complete = replay.pending_records == 0 && replay.failed_records == 0;

        if consistency == ConsistencyLevel::Eventual || complete {
            return execute_snapshot(&execution, snapshot, replay).await;
        }

        if manifest_refresh == ManifestRefresh::Fixed || started.elapsed() >= semantic_wait {
            return Err(LateInteractionError::SemanticIndexLag {
                requested_generation,
                covered_sequence: replay.covered_sequence,
                pending_records: replay.pending_records,
                failed_records: replay.failed_records,
            }
            .into());
        }

        let remaining = semantic_wait.saturating_sub(started.elapsed());
        tokio::time::sleep(SEMANTIC_POLL_INTERVAL.min(remaining)).await;
        manifest = Manifest::read_versioned_required_for_incarnation(
            store,
            namespace,
            requested_incarnation,
        )
        .await?
        .0;
    }
}

struct SearchExecution<'a> {
    store: &'a ZeppelinStore,
    bootstrap_cache: Option<&'a DiskCache>,
    encoder_provider: &'a dyn MultiVectorEncoderProvider,
    text: &'a str,
    top_k: usize,
    effective_filter: Option<&'a Filter>,
    consistency: ConsistencyLevel,
    max_overlay_bytes: u64,
    segment_config: MmliSegmentConfig,
}

struct OwnedLateSnapshot {
    manifest: Manifest,
    section: LateStateSection,
    section_origin: ArtifactOrigin,
    profile: EmbeddingProfileRef,
}

impl OwnedLateSnapshot {
    async fn load(store: &ZeppelinStore, manifest: Manifest) -> Result<Self> {
        let section_reference = manifest
            .late_state
            .as_ref()
            .ok_or(LateInteractionError::MissingLateState)?;
        let section_origin = manifest.late_section_origin(section_reference)?;
        let section = manifest
            .load_late_state(store)
            .await?
            .ok_or(LateInteractionError::MissingLateState)?;
        let profile = section
            .active_profile
            .clone()
            .ok_or(LateInteractionError::MissingActiveProfile)?;
        profile.validate()?;
        Ok(Self {
            manifest,
            section,
            section_origin,
            profile,
        })
    }

    fn provenance(&self, consistency: ConsistencyLevel) -> LateInteractionProvenance {
        LateInteractionProvenance {
            manifest_generation: self.manifest.version(),
            profile: self.profile.profile.clone(),
            epoch: self.profile.epoch.id,
            fde_generation: self.profile.fde.generation,
            consistency,
        }
    }

    fn active_segment(&self) -> Result<Option<&LateInteractionSegmentRef>> {
        let Some(active_id) = self.section.active_late_segment.as_deref() else {
            return Ok(None);
        };
        self.section
            .late_interaction_segments
            .iter()
            .find(|segment| segment.id == active_id)
            .map(Some)
            .ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "active late segment {active_id} is absent from its selected section"
                ))
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct VersionIdentity {
    source_key: String,
    source_checksum: u64,
    row_ordinal: u32,
    record_id: VectorId,
    content_hash: ContentHash,
    sequence: u64,
    epoch: MultiVectorEpochId,
    fde_generation: FdeGenerationId,
}

#[derive(Clone)]
struct LiveVersion {
    identity: VersionIdentity,
    record: RetrievalUnitRecord,
    referenced_bytes: u64,
}

#[derive(Clone, Copy)]
struct MatrixLocation {
    overlay_index: usize,
    row_index: usize,
}

struct CoveredLiveVersion {
    record: RetrievalUnitRecord,
    location: MatrixLocation,
}

struct ReplayState {
    covered: Vec<CoveredLiveVersion>,
    touched_ids: BTreeSet<VectorId>,
    covered_sequence: u64,
    pending_records: u64,
    failed_records: u64,
}

async fn replay_snapshot(
    store: &ZeppelinStore,
    snapshot: &OwnedLateSnapshot,
) -> Result<ReplayState> {
    let mut references = snapshot.manifest.input_fragments.iter().collect::<Vec<_>>();
    references.sort_by_key(|reference| reference.sequence_number);
    let mut live = BTreeMap::<VectorId, LiveVersion>::new();
    let mut touched_ids = BTreeSet::new();

    for reference in references {
        let origin = snapshot.manifest.input_fragment_origin(reference)?;
        let fragment = Manifest::read_input_fragment_checked(store, reference, &origin).await?;
        let source_key = EncoderInputWalFragment::s3_key(origin.namespace.as_str(), &reference.id);
        for (row_index, record) in fragment.upserts.iter().enumerate() {
            touched_ids.insert(record.id.clone());
            let row_ordinal = u32::try_from(row_index)
                .map_err(|_| LateInteractionError::CoverageArithmeticOverflow)?;
            let identity = VersionIdentity {
                source_key: source_key.clone(),
                source_checksum: fragment.checksum,
                row_ordinal,
                record_id: record.id.clone(),
                content_hash: record.content_hash,
                sequence: reference.sequence_number,
                epoch: snapshot.profile.epoch.id,
                fde_generation: snapshot.profile.fde.generation,
            };
            live.insert(
                record.id.clone(),
                LiveVersion {
                    identity,
                    record: record.clone(),
                    referenced_bytes: record.input.referenced_content_bytes()?,
                },
            );
        }
        for deleted in &fragment.deletes {
            touched_ids.insert(deleted.clone());
            live.remove(deleted);
        }
    }

    classify_live_versions(snapshot, live, touched_ids)
}

fn classify_live_versions(
    snapshot: &OwnedLateSnapshot,
    live: BTreeMap<VectorId, LiveVersion>,
    touched_ids: BTreeSet<VectorId>,
) -> Result<ReplayState> {
    let mut covered_locations = BTreeMap::<VersionIdentity, MatrixLocation>::new();
    for (overlay_index, overlay) in snapshot.section.semantic_overlays.iter().enumerate() {
        if overlay.semantic_epoch != snapshot.profile.epoch.id
            || overlay.fde_generation != snapshot.profile.fde.generation
        {
            continue;
        }
        for (row_index, version) in overlay.covered_versions.records.iter().enumerate() {
            let identity = VersionIdentity {
                source_key: overlay.source_fragment.key.clone(),
                source_checksum: overlay.source_fragment.checksum,
                row_ordinal: version.row_ordinal,
                record_id: version.record_id.clone(),
                content_hash: version.content_hash,
                sequence: version.sequence,
                epoch: overlay.semantic_epoch,
                fde_generation: overlay.fde_generation,
            };
            if covered_locations
                .insert(
                    identity,
                    MatrixLocation {
                        overlay_index,
                        row_index,
                    },
                )
                .is_some()
            {
                return Err(LateInteractionError::DuplicateVersionCoverage.into());
            }
        }
    }

    let failed = snapshot
        .section
        .quarantine_evidence
        .iter()
        .filter(|evidence| {
            evidence.semantic_epoch == snapshot.profile.epoch.id
                && evidence.fde_generation == snapshot.profile.fde.generation
        })
        .flat_map(|evidence| {
            evidence
                .failed_versions
                .records
                .iter()
                .map(move |version| VersionIdentity {
                    source_key: evidence.source_fragment.key.clone(),
                    source_checksum: evidence.source_fragment.checksum,
                    row_ordinal: version.row_ordinal,
                    record_id: version.record_id.clone(),
                    content_hash: version.content_hash,
                    sequence: version.sequence,
                    epoch: evidence.semantic_epoch,
                    fde_generation: evidence.fde_generation,
                })
        })
        .collect::<BTreeSet<_>>();

    let mut covered = Vec::new();
    let mut pending_records = 0_u64;
    let mut pending_bytes = 0_u64;
    let mut failed_records = 0_u64;
    let mut first_hole = None;
    for version in live.into_values() {
        if failed.contains(&version.identity) {
            failed_records = failed_records
                .checked_add(1)
                .ok_or(LateInteractionError::CoverageArithmeticOverflow)?;
            first_hole = Some(
                first_hole.map_or(version.identity.sequence, |current: u64| {
                    current.min(version.identity.sequence)
                }),
            );
        } else if let Some(location) = covered_locations.get(&version.identity).copied() {
            covered.push(CoveredLiveVersion {
                record: version.record,
                location,
            });
        } else {
            pending_records = pending_records
                .checked_add(1)
                .ok_or(LateInteractionError::CoverageArithmeticOverflow)?;
            pending_bytes = pending_bytes
                .checked_add(version.referenced_bytes)
                .ok_or(LateInteractionError::CoverageArithmeticOverflow)?;
            first_hole = Some(
                first_hole.map_or(version.identity.sequence, |current: u64| {
                    current.min(version.identity.sequence)
                }),
            );
        }
    }
    let covered_sequence = first_hole
        .map(|sequence| sequence.saturating_sub(1))
        .unwrap_or_else(|| snapshot.manifest.next_sequence.saturating_sub(1));
    let computed = SemanticCoverageState {
        profile: snapshot.profile.profile.clone(),
        epoch: snapshot.profile.epoch.id,
        fde_generation: snapshot.profile.fde.generation,
        contiguous_sequence: covered_sequence,
        pending_record_count: pending_records,
        pending_bytes,
        failed_record_count: failed_records,
        state: if failed_records > 0 {
            SemanticState::Failed
        } else if pending_records == 0 {
            SemanticState::Ready
        } else {
            SemanticState::Pending
        },
    };
    if snapshot.manifest.semantic_coverage.as_ref() != Some(&computed) {
        return Err(LateInteractionError::CoverageMetadataMismatch.into());
    }

    Ok(ReplayState {
        covered,
        touched_ids,
        covered_sequence,
        pending_records,
        failed_records,
    })
}

async fn execute_snapshot(
    request: &SearchExecution<'_>,
    snapshot: OwnedLateSnapshot,
    replay: ReplayState,
) -> Result<LateInteractionSearchOutput> {
    let provenance = snapshot.provenance(request.consistency);
    let semantic_coverage = if replay.pending_records == 0 && replay.failed_records == 0 {
        LateInteractionCoverage::Complete
    } else {
        LateInteractionCoverage::Partial
    };

    let filtered_overlays = replay
        .covered
        .into_iter()
        .filter(|candidate| {
            request.effective_filter.is_none_or(|filter| {
                evaluate_filter_on_optional_attributes(filter, candidate.record.attributes.as_ref())
            })
        })
        .collect::<Vec<_>>();
    let active_segment = snapshot.active_segment()?;

    if request.top_k == 0 || (filtered_overlays.is_empty() && active_segment.is_none()) {
        return Ok(LateInteractionSearchOutput {
            results: Vec::new(),
            manifest: snapshot.manifest,
            provenance,
            semantic_coverage,
            covered_sequence: replay.covered_sequence,
            pending_records: replay.pending_records,
            failed_records: replay.failed_records,
            read_trace: None,
        });
    }

    let encoder = request
        .encoder_provider
        .encoder_for(&snapshot.profile)
        .await?;
    let raw_query = encoder
        .encode_query(EncoderQueryInput::new(request.text)?)
        .await?;
    if encoder.epoch() != snapshot.profile.epoch.id
        || encoder.output_dimension() != snapshot.profile.epoch.vector_dimension as usize
        || raw_query.vector_dimension() != snapshot.profile.epoch.vector_dimension as usize
    {
        return Err(LateInteractionError::EncoderIdentityMismatch.into());
    }
    let exact_mean = load_vector_transform_mean(
        request.store,
        &snapshot.profile.epoch.exact_scoring_transform,
        snapshot.profile.epoch.vector_dimension as usize,
    )
    .await?;
    let query = apply_vector_transform(
        &raw_query,
        &snapshot.profile.epoch.exact_scoring_transform,
        exact_mean.as_deref(),
        snapshot.profile.epoch.max_query_vectors as usize,
    )?;
    let query_matrix = query.matrix_ref()?;

    let segment_output = if let Some(segment) = active_segment {
        let candidate_mean = load_vector_transform_mean(
            request.store,
            &snapshot.profile.fde.candidate_vector_transform,
            snapshot.profile.epoch.vector_dimension as usize,
        )
        .await?;
        let candidate_query = apply_vector_transform(
            &raw_query,
            &snapshot.profile.fde.candidate_vector_transform,
            candidate_mean.as_deref(),
            snapshot.profile.epoch.max_query_vectors as usize,
        )?;
        let transform_ref = &snapshot.profile.fde.transform_artifact;
        let transform_bytes = request.store.get(&transform_ref.key).await?;
        if u64::try_from(transform_bytes.len()).ok() != Some(transform_ref.size_bytes)
            || ArtifactChecksum::digest(&transform_bytes) != transform_ref.checksum
        {
            return Err(ZeppelinError::Serialization(
                "candidate FDE transform size or checksum mismatch".to_string(),
            ));
        }
        let fde_transform = FdeTransform::from_bytes(&transform_bytes)?;
        if fde_transform.params() != snapshot.profile.fde.params
            || fde_transform.output_dimension() != segment.fde_dimension as usize
        {
            return Err(ZeppelinError::Serialization(
                "candidate FDE transform disagrees with the active segment".to_string(),
            ));
        }
        let candidate_query_fde = fde_transform.encode_query(&candidate_query.matrix_ref()?)?;
        let read_plan = ReadPlanConfig::new(
            request.segment_config.read_gap_budget_bytes,
            request.segment_config.read_max_request_bytes,
            request.segment_config.read_max_concurrency,
        )
        .map_err(|error| ZeppelinError::Validation(error.to_string()))?;
        search_segment(SegmentSearchRequest {
            store: request.store,
            bootstrap_cache: request.bootstrap_cache,
            segment,
            exact_query: query_matrix,
            candidate_query_fde: &candidate_query_fde,
            mandatory_filter: None,
            request_filter: request.effective_filter,
            excluded_ids: &replay.touched_ids,
            top_k: request.top_k,
            read_plan: &read_plan,
            bounds: SegmentSearchBounds {
                max_resident_bytes: request.segment_config.max_resident_bootstrap_bytes,
                max_cluster_bytes: request.segment_config.max_cluster_object_bytes,
                max_vectors_per_document: snapshot.profile.epoch.max_document_vectors as usize,
                max_attribute_payload_bytes: request.segment_config.read_max_request_bytes,
            },
        })
        .await?
    } else {
        Default::default()
    };

    let selected_overlays = filtered_overlays
        .iter()
        .map(|candidate| candidate.location.overlay_index)
        .collect::<BTreeSet<_>>();
    let matrices = load_selected_matrices(
        request.store,
        &snapshot,
        &selected_overlays,
        request.max_overlay_bytes,
    )
    .await?;

    struct Scored {
        id: VectorId,
        score: f32,
        parent_id: Option<String>,
        unit_ordinal: Option<u32>,
        attributes: Option<HashMap<String, AttributeValue>>,
    }
    fn compare_scored(left: &Scored, right: &Scored) -> Ordering {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.id.cmp(&right.id))
    }
    let mut top_k = TopK::new(request.top_k, compare_scored);
    for row in segment_output.rows {
        top_k.push(Scored {
            id: row.id,
            score: row.score,
            parent_id: row.parent_id,
            unit_ordinal: row.unit_ordinal,
            attributes: row.attributes,
        });
    }
    for candidate in filtered_overlays {
        let matrix = matrices
            .get(&candidate.location.overlay_index)
            .ok_or(LateInteractionError::MatrixCoverageMismatch)?;
        let row = matrix
            .rows()
            .get(candidate.location.row_index)
            .ok_or(LateInteractionError::MatrixCoverageMismatch)?;
        if row.content_hash() != candidate.record.content_hash {
            return Err(LateInteractionError::MatrixCoverageMismatch.into());
        }
        let document = row.embedding().matrix_ref()?;
        let score = max_sim(&query_matrix, &document)?;
        top_k.push(Scored {
            id: candidate.record.id,
            score,
            parent_id: candidate.record.parent_id,
            unit_ordinal: candidate.record.unit_ordinal,
            attributes: candidate.record.attributes,
        });
    }

    let results = top_k
        .into_sorted_vec()
        .into_iter()
        .filter(|scored| {
            request.effective_filter.is_none_or(|filter| {
                evaluate_filter_on_optional_attributes(filter, scored.attributes.as_ref())
            })
        })
        .map(|scored| LateInteractionRankedResult {
            id: scored.id,
            score: scored.score,
            parent_id: scored.parent_id,
            unit_ordinal: scored.unit_ordinal,
            attributes: scored.attributes,
            provenance: provenance.clone(),
        })
        .collect();

    let read_trace = active_segment.map(|_| map_segment_trace(segment_output.trace));

    Ok(LateInteractionSearchOutput {
        results,
        manifest: snapshot.manifest,
        provenance,
        semantic_coverage,
        covered_sequence: replay.covered_sequence,
        pending_records: replay.pending_records,
        failed_records: replay.failed_records,
        read_trace,
    })
}

fn map_segment_trace(trace: SegmentSearchTrace) -> LateInteractionReadTrace {
    LateInteractionReadTrace {
        candidate_wave: LateInteractionWaveTrace {
            logical_ranges: trace.candidate_wave.logical_ranges,
            planned_requests: trace.candidate_wave.planned_requests,
            planned_bytes: trace.candidate_wave.planned_bytes,
        },
        truth_wave: LateInteractionWaveTrace {
            logical_ranges: trace.truth_wave.logical_ranges,
            planned_requests: trace.truth_wave.planned_requests,
            planned_bytes: trace.truth_wave.planned_bytes,
        },
    }
}

async fn load_selected_matrices(
    store: &ZeppelinStore,
    snapshot: &OwnedLateSnapshot,
    selected_overlays: &BTreeSet<usize>,
    max_overlay_bytes: u64,
) -> Result<BTreeMap<usize, MatrixArtifact>> {
    let requested_bytes = selected_overlays.iter().try_fold(0_u64, |total, index| {
        let overlay = snapshot
            .section
            .semantic_overlays
            .get(*index)
            .ok_or(LateInteractionError::MatrixCoverageMismatch)?;
        total
            .checked_add(overlay.embeddings.size_bytes)
            .ok_or(LateInteractionError::OverlayPayloadSizeOverflow)
    })?;
    if requested_bytes > max_overlay_bytes {
        tracing::warn!(
            namespace = snapshot.manifest.local_origin()?.namespace.as_str(),
            requested_bytes,
            max_bytes = max_overlay_bytes,
            "semantic overlay query byte budget exceeded"
        );
        return Err(LateInteractionError::OverlayPayloadBudgetExceeded {
            requested_bytes,
            max_bytes: max_overlay_bytes,
        }
        .into());
    }

    let resolved_artifacts = snapshot
        .section
        .resolved_artifacts(&snapshot.section_origin)?;
    let mut matrices = BTreeMap::new();
    for index in selected_overlays {
        let overlay = snapshot
            .section
            .semantic_overlays
            .get(*index)
            .ok_or(LateInteractionError::MatrixCoverageMismatch)?;
        let resolved = resolved_artifacts
            .iter()
            .find(|artifact| {
                artifact.family == NamespaceObjectFamily::MatrixFragment
                    && artifact.key == overlay.embeddings.key
            })
            .ok_or(LateInteractionError::MatrixCoverageMismatch)?;
        let bytes = store.get(&resolved.key).await?;
        if u64::try_from(bytes.len()).ok() != Some(overlay.embeddings.size_bytes) {
            return Err(LateInteractionError::MatrixCoverageMismatch.into());
        }
        let matrix = MatrixArtifact::from_bytes(
            &bytes,
            overlay.embeddings.checksum,
            snapshot.profile.epoch.matrix_dtype,
            snapshot.profile.epoch.id,
            overlay.source_fragment.checksum,
            snapshot.profile.epoch.vector_dimension as usize,
            overlay.covered_versions.records.len(),
            snapshot.profile.epoch.max_document_vectors as usize,
        )?;
        if matrix.dtype() != overlay.embeddings.dtype
            || matrix.semantic_epoch() != overlay.embeddings.semantic_epoch
            || matrix.source_fragment_checksum() != overlay.embeddings.source_fragment_checksum
            || matrix.vector_dimension() != overlay.embeddings.vector_dimension as usize
            || matrix.rows().len() != overlay.embeddings.row_count as usize
            || matrix.rows().len() != overlay.covered_versions.records.len()
            || matrix
                .rows()
                .iter()
                .zip(&overlay.covered_versions.records)
                .any(|(row, version)| row.content_hash() != version.content_hash)
        {
            return Err(LateInteractionError::MatrixCoverageMismatch.into());
        }
        let total_vectors = matrix.rows().iter().try_fold(0_u64, |total, row| {
            total
                .checked_add(row.embedding().vector_count() as u64)
                .ok_or(LateInteractionError::CoverageArithmeticOverflow)
        })?;
        if total_vectors != overlay.embeddings.total_vectors {
            return Err(LateInteractionError::MatrixCoverageMismatch.into());
        }
        matrices.insert(*index, matrix);
    }
    Ok(matrices)
}
