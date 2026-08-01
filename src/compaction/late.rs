//! Full-rebuild compaction for late-interaction namespaces.
//!
//! The immutable segment is built once from an exact root-plus-section
//! snapshot. Publication retries only the section/root transaction, preserving
//! input fragments and section additions that appeared after the build.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::Rng;
use tracing::{info, warn};
use ulid::Ulid;

use crate::embedding::artifact::{
    decode_matrix_payload, encode_matrix_payload, matrix_row_payloads,
};
use crate::embedding::coordinator::apply_candidate_document_pooling;
use crate::embedding::transform::{apply_vector_transform, load_vector_transform_mean};
use crate::embedding::{
    ArtifactChecksum, ContentHash, EmbeddingProfileRef, FdeArtifact, MatrixArtifact,
    PhysicalInputFragmentIdentity, RecordVersionRef, RetrievalUnitRecord, SemanticOverlayRef,
    SemanticState,
};
use crate::embedding::{MatrixDtype, VectorTransformRecipe};
use crate::error::{Result, ZeppelinError};
use crate::fts::inverted_index::InvertedIndex;
use crate::fts::FtsFieldConfig;
use crate::index::late_interaction::{
    build_late_interaction_segment, decode_all_candidate_rows, decode_attribute_block,
    decode_matrix_block, CandidateFdeSource, FdeTransform, FetchedLateCandidateCluster,
    FlatCalibrationSource, LateCandidateBuild, LateFlatCandidateBuildConfig, LateInteractionError,
    LateRowMatrixSource, LateSegmentBuildConfig, LateSegmentBuildRow, MatrixBlockRef,
    PrebuiltLateFtsArtifact, ResidentFlatCandidateIndex,
};
use crate::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};
use crate::storage::{CreateOnlyOutcome, NamespaceObjectFamily, NamespaceObjectKey, ZeppelinStore};
use crate::types::{IndexType, VectorId};
use crate::wal::{
    EncoderInputWalFragment, InputFragmentRef, LateCandidateKind, LateInteractionSegmentRef,
    LateSegmentObjectRef, LateStateSection, Manifest,
};

use super::{
    check_lease_lost, check_upload_window, drop_compaction_staging, merge_pending_deletes,
    validate_deferred_deletes_are_local, CompactionResult, Compactor, NamespaceIncarnationId,
    NamespaceMetadata, NamespaceState, MAX_CAS_RETRIES, NAMESPACE_INCARNATION_METADATA_KEY,
};

const LATE_FTS_FORMAT_VERSION: u32 = 1;

#[derive(Clone)]
struct SnapshottedInput {
    reference: InputFragmentRef,
    origin: ArtifactOrigin,
    key: String,
    fragment: EncoderInputWalFragment,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct VersionIdentity {
    source_key: String,
    source_checksum: u64,
    row_ordinal: u32,
    record_id: VectorId,
    content_hash: ContentHash,
    sequence: u64,
}

#[derive(Clone)]
struct DerivedRow {
    exact_matrix: crate::embedding::MultiVectorEmbedding,
    exact_payload: Bytes,
    raw_fde: Vec<f32>,
}

enum LiveRow {
    Baseline(Box<LateSegmentBuildRow>),
    Input {
        input_index: usize,
        row_ordinal: usize,
    },
}

struct LateBuildSnapshot {
    local_origin: ArtifactOrigin,
    profile: EmbeddingProfileRef,
    inputs: Vec<SnapshottedInput>,
    consumed_overlays: Vec<SemanticOverlayRef>,
    old_segment: Option<LateInteractionSegmentRef>,
    rows: Vec<LateSegmentBuildRow>,
    coverage_sequence: u64,
    fts_configs: HashMap<String, FtsFieldConfig>,
    /// Fully-untouched matrix blocks carried by reference from the old
    /// segment; empty for full rebuilds.
    carried_matrix_blocks: Vec<MatrixBlockRef>,
    /// Flat-SQ8 calibration authority for this build.
    flat_calibration: FlatCalibrationSource,
    /// True when a visible ref is still foreign-owned: compaction must run
    /// even with no pending inputs so the branch fully materializes and the
    /// source root can eventually release.
    requires_foreign_materialization: bool,
}

/// Reuse decision for one late compaction over an existing flat segment.
enum IncrementalDecision {
    /// Carry untouched blocks and codes; only changed rows are rewritten.
    Incremental {
        changed: BTreeSet<VectorId>,
        changed_fraction: f64,
    },
    /// Rebuild everything from scratch (always correctness-preserving).
    Full { reason: &'static str },
}

impl Compactor {
    /// Compact the exact late-interaction root-plus-section snapshot once.
    pub(super) async fn compact_late_with_signaled(
        &self,
        namespace: &str,
        fencing_token: Option<u64>,
        lease_lost: Option<Arc<AtomicBool>>,
    ) -> Result<CompactionResult> {
        let started = Instant::now();
        check_lease_lost(namespace, lease_lost.as_deref())?;
        let snapshot = self.snapshot_late_build(namespace).await?;
        if snapshot.inputs.is_empty() {
            if snapshot.requires_foreign_materialization {
                info!(
                    namespace,
                    "late compaction materializes a foreign-backed branch view"
                );
                crate::metrics::LATE_COMPACTION_MODE_TOTAL
                    .with_label_values(&[namespace, "materialize"])
                    .inc();
            } else {
                return Ok(CompactionResult {
                    segment_id: None,
                    vectors_compacted: 0,
                    fragments_removed: 0,
                    old_segment_removed: None,
                });
            }
        }

        let fragments_removed = snapshot.inputs.len();
        let old_segment_id = snapshot
            .old_segment
            .as_ref()
            .map(|segment| segment.id.clone());
        let vectors_compacted = snapshot.rows.len();
        let segment_id = (!snapshot.rows.is_empty()).then(|| format!("seg_{}", Ulid::new()));
        let upload_phase_start = segment_id.as_ref().map(|_| Instant::now());

        let built = if let Some(segment_id) = segment_id.as_ref() {
            check_lease_lost(namespace, lease_lost.as_deref())?;
            let fts_artifacts =
                build_global_fts(namespace, segment_id, &snapshot.rows, &snapshot.fts_configs)
                    .await?;
            let fde_dimension = snapshot
                .rows
                .first()
                .map(|row| row.fde.dimension())
                .ok_or_else(|| {
                    ZeppelinError::Validation(
                        "late segment build lost its non-empty row set".to_string(),
                    )
                })?;
            Some(build_late_interaction_segment(
                LateSegmentBuildConfig {
                    namespace: namespace.to_string(),
                    segment_id: segment_id.clone(),
                    profile: snapshot.profile.profile.clone(),
                    semantic_epoch: snapshot.profile.epoch.id,
                    fde_generation: snapshot.profile.fde.generation,
                    matrix_dtype: snapshot.profile.epoch.matrix_dtype,
                    vector_dimension: snapshot.profile.epoch.vector_dimension as usize,
                    fde_dimension,
                    coverage_sequence: snapshot.coverage_sequence,
                    max_matrix_object_bytes: self.mmli_config.segment.max_matrix_object_bytes,
                    max_attribute_object_bytes: self.mmli_config.segment.max_matrix_object_bytes,
                    // Flat SQ8 is the adopted candidate design for this corpus
                    // regime; nlist/probe_budget remain configured but unused
                    // by this kind (retained for the future IVF scale phase).
                    candidate: LateCandidateBuild::FlatSq8(LateFlatCandidateBuildConfig {
                        fde_dimension,
                        candidate_k: self.mmli_config.segment.candidate_k,
                        max_artifact_bytes: self.mmli_config.segment.max_resident_bootstrap_bytes,
                    }),
                    artifact_origin: None,
                    fts_artifacts,
                    carried_matrix_blocks: snapshot.carried_matrix_blocks.clone(),
                    flat_calibration: snapshot.flat_calibration.clone(),
                },
                snapshot.rows.clone(),
            )?)
        } else {
            None
        };

        if let Some(built) = built.as_ref() {
            let upload_start = upload_phase_start.ok_or_else(|| {
                ZeppelinError::Validation(
                    "built late segment has no upload-window start".to_string(),
                )
            })?;
            if let Some(token) = fencing_token {
                let keys = built
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.key.clone())
                    .collect::<BTreeSet<_>>();
                super::gc::write_compaction_staging(&self.store, namespace, token, keys).await?;
            }
            for artifact in &built.artifacts {
                check_lease_lost(namespace, lease_lost.as_deref())?;
                check_upload_window(namespace, upload_start, self.compaction_upload_window())?;
                put_create_verified(&self.store, artifact.key.as_str(), artifact.bytes.clone())
                    .await?;
            }
            check_lease_lost(namespace, lease_lost.as_deref())?;
            check_upload_window(namespace, upload_start, self.compaction_upload_window())?;
        }

        if let Some(delay) = self.test_pre_cas_delay {
            warn!(
                delay_ms = delay.as_millis() as u64,
                "test hook: delaying late compaction before final CAS"
            );
            tokio::time::sleep(delay).await;
        }

        for attempt in 0..MAX_CAS_RETRIES {
            if let Some(upload_start) = upload_phase_start {
                check_upload_window(namespace, upload_start, self.compaction_upload_window())?;
            }
            check_lease_lost(namespace, lease_lost.as_deref())?;
            let Some((mut manifest, version)) =
                Manifest::read_versioned(&self.store, namespace).await?
            else {
                return Err(ZeppelinError::ManifestNotFound {
                    namespace: namespace.to_string(),
                });
            };
            let fresh_local = manifest.local_origin()?;
            if fresh_local != snapshot.local_origin {
                return Err(ZeppelinError::ManifestConflict {
                    namespace: namespace.to_string(),
                });
            }
            if let Some(token) = fencing_token {
                if manifest.fencing_token > token {
                    return Err(ZeppelinError::FencingTokenStale {
                        namespace: namespace.to_string(),
                        our_token: token,
                        manifest_token: manifest.fencing_token,
                    });
                }
                manifest.fencing_token = token;
            }

            let previous_section_ref = manifest.late_state.clone().ok_or_else(|| {
                ZeppelinError::Validation(
                    "late compaction CAS candidate has no late-state section".to_string(),
                )
            })?;
            let previous_section_origin = manifest.late_section_origin(&previous_section_ref)?;
            let mut section = manifest
                .load_late_state(&self.store)
                .await?
                .ok_or_else(|| {
                    ZeppelinError::Validation(
                        "late compaction CAS candidate has no loadable late-state section"
                            .to_string(),
                    )
                })?;
            section.rebase_nested_artifact_origins(&previous_section_origin, &fresh_local)?;
            section.canonicalize_artifact_origins()?;

            if section.active_profile.as_ref() != Some(&snapshot.profile)
                || active_late_segment(&section) != snapshot.old_segment.as_ref()
            {
                return Err(ZeppelinError::ManifestConflict {
                    namespace: namespace.to_string(),
                });
            }
            require_snapshotted_inputs(&manifest, &snapshot.inputs)?;
            require_snapshotted_overlays(&section, &snapshot.consumed_overlays, namespace)?;

            remove_consumed_overlays(&mut section, &snapshot.consumed_overlays);
            replace_active_late_segment(
                &mut section,
                snapshot.old_segment.as_ref(),
                built.as_ref().map(|candidate| candidate.reference.clone()),
                namespace,
            )?;
            if snapshot.requires_foreign_materialization {
                let copies =
                    rebind_foreign_section_residents(&mut section, namespace, &fresh_local)?;
                for (source_key, target_key) in &copies {
                    let bytes = self.store.get(source_key).await?;
                    put_create_verified(&self.store, target_key, bytes).await?;
                }
                if !copies.is_empty() {
                    info!(
                        namespace,
                        copied = copies.len(),
                        "materialization copied foreign section residents"
                    );
                }
            }
            section.canonicalize_artifact_origins()?;
            section.validate_for_origin(&fresh_local)?;
            let new_section_ref = section.put_create(&self.store, namespace).await?;

            remove_snapshotted_inputs(&mut manifest, &snapshot.inputs)?;
            manifest.semantic_coverage = Some(
                manifest
                    .compute_semantic_coverage(&self.store, &section, &snapshot.profile)
                    .await?,
            );
            manifest.late_state = Some(new_section_ref.clone());
            manifest.updated_at = self.clock.now();

            let mut deferred_deletes = snapshot
                .inputs
                .iter()
                .filter(|input| input.origin == fresh_local)
                .map(|input| input.key.clone())
                .collect::<Vec<_>>();
            if previous_section_origin == fresh_local
                && previous_section_ref.key != new_section_ref.key
            {
                deferred_deletes.push(previous_section_ref.key.clone());
            }
            deferred_deletes.sort();
            deferred_deletes.dedup();
            validate_deferred_deletes_are_local(namespace, &deferred_deletes)?;
            merge_pending_deletes(&mut manifest, &deferred_deletes, &HashSet::new());
            manifest.canonicalize_artifact_origins()?;
            manifest.validate_pending_deletes_are_local(namespace)?;
            check_lease_lost(namespace, lease_lost.as_deref())?;

            match manifest
                .write_conditional(&self.store, namespace, &version)
                .await
            {
                Ok(_) => {
                    if let (Some(token), Some(_)) = (fencing_token, built.as_ref()) {
                        drop_compaction_staging(&self.store, namespace, token).await;
                    }
                    info!(
                        segment_id = segment_id.as_deref(),
                        vectors_compacted,
                        fragments_removed,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        attempt,
                        "late compaction complete"
                    );
                    return Ok(CompactionResult {
                        segment_id,
                        vectors_compacted,
                        fragments_removed,
                        old_segment_removed: old_segment_id,
                    });
                }
                Err(ZeppelinError::ManifestConflict { .. }) => {
                    warn!(
                        attempt,
                        "late compaction manifest CAS conflict; republishing"
                    );
                    let backoff_ms = (50_u64 * (1 << attempt.min(5))).min(2_000);
                    let jitter_ms = rand::thread_rng().gen_range(0..50);
                    tokio::time::sleep(Duration::from_millis(backoff_ms + jitter_ms)).await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: namespace.to_string(),
        })
    }

    async fn snapshot_late_build(&self, namespace: &str) -> Result<LateBuildSnapshot> {
        let metadata_key = NamespaceMetadata::s3_key(namespace);
        let (metadata_bytes, object_metadata) = self
            .store
            .get_with_object_metadata(&metadata_key)
            .await
            .map_err(|error| match error {
                ZeppelinError::NotFound { .. } => ZeppelinError::NamespaceNotFound {
                    namespace: namespace.to_string(),
                },
                error => error,
            })?;
        let metadata = NamespaceMetadata::from_bytes(&metadata_bytes)?;
        validate_late_metadata(namespace, &metadata)?;

        let (manifest, _) = Manifest::read_versioned_required(&self.store, namespace).await?;
        let local_origin = manifest.local_origin()?;
        let incarnation = object_metadata
            .user_metadata
            .get(NAMESPACE_INCARNATION_METADATA_KEY)
            .ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "late namespace {namespace} metadata omitted its incarnation"
                ))
            })?;
        if NamespaceIncarnationId::parse(incarnation)? != local_origin.incarnation {
            return Err(ZeppelinError::ManifestConflict {
                namespace: namespace.to_string(),
            });
        }

        let section_reference = manifest.late_state.clone().ok_or_else(|| {
            ZeppelinError::Validation(
                "late-interaction namespace has no late-state section".to_string(),
            )
        })?;
        let section_origin = manifest.late_section_origin(&section_reference)?;
        let mut section = manifest
            .load_late_state(&self.store)
            .await?
            .ok_or_else(|| {
                ZeppelinError::Validation(
                    "late-interaction namespace has no loadable late-state section".to_string(),
                )
            })?;
        let requires_foreign_materialization =
            !manifest.visible_refs_are_local_with_late_state(Some(&section))?;
        section.rebase_nested_artifact_origins(&section_origin, &local_origin)?;
        section.canonicalize_artifact_origins()?;
        section.validate_for_origin(&local_origin)?;
        let profile = section.active_profile.clone().ok_or_else(|| {
            ZeppelinError::Validation(
                "late-interaction namespace has no active embedding profile".to_string(),
            )
        })?;
        let late_config = metadata.late_interaction.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(
                "validated late namespace lost its admission config".to_string(),
            )
        })?;
        profile.validate_for_modalities(&late_config.accepted_modalities)?;

        let coverage = manifest
            .compute_semantic_coverage(&self.store, &section, &profile)
            .await?;
        if coverage.state != SemanticState::Ready {
            return Err(LateInteractionError::SemanticIndexLag {
                requested_generation: manifest.version(),
                covered_sequence: coverage.contiguous_sequence,
                pending_records: coverage.pending_record_count,
                failed_records: coverage.failed_record_count,
            }
            .into());
        }

        let inputs = load_snapshotted_inputs(&self.store, &manifest).await?;
        let old_segment = active_late_segment(&section).cloned();
        if let Some(old) = old_segment.as_ref() {
            validate_old_segment_recipe(old, &profile)?;
        }
        let (baseline, carried_matrix_blocks, flat_calibration) = match old_segment.as_ref() {
            Some(segment) => {
                let decision = plan_incremental_rebuild(
                    segment,
                    &section,
                    &local_origin,
                    &inputs,
                    self.mmli_config.segment.incremental_max_changed_fraction,
                )?;
                match decision {
                    IncrementalDecision::Incremental {
                        changed,
                        changed_fraction,
                    } => {
                        let (baseline, carried, calibration) = assemble_incremental_baseline(
                            &self.store,
                            segment,
                            &profile,
                            &changed,
                            self.mmli_config.segment.max_resident_bootstrap_bytes,
                            self.mmli_config.segment.max_matrix_object_bytes,
                        )
                        .await?;
                        info!(
                            namespace,
                            mode = "incremental",
                            changed_rows = changed.len(),
                            changed_fraction,
                            carried_blocks = carried.len(),
                            "late compaction reuses untouched matrix blocks and codes"
                        );
                        crate::metrics::LATE_COMPACTION_MODE_TOTAL
                            .with_label_values(&[namespace, "incremental"])
                            .inc();
                        (baseline, carried, calibration)
                    }
                    IncrementalDecision::Full { reason } => {
                        info!(
                            namespace,
                            mode = "full",
                            reason,
                            "late compaction performs a full rebuild"
                        );
                        crate::metrics::LATE_COMPACTION_MODE_TOTAL
                            .with_label_values(&[namespace, "full"])
                            .inc();
                        let baseline = load_old_segment_rows(
                            &self.store,
                            segment,
                            &profile,
                            self.mmli_config.segment.max_resident_bootstrap_bytes,
                            self.mmli_config.segment.max_cluster_object_bytes,
                            self.mmli_config.segment.max_matrix_object_bytes,
                        )
                        .await?;
                        (baseline, Vec::new(), FlatCalibrationSource::Recalibrate)
                    }
                }
            }
            None => (Vec::new(), Vec::new(), FlatCalibrationSource::Recalibrate),
        };
        let (derived, failed, consumed_overlays) =
            load_overlay_rows(&self.store, &section, &local_origin, &profile, &inputs).await?;
        let rows = replay_late_rows(baseline, &inputs, &derived, &failed)?;
        let coverage_sequence = inputs
            .iter()
            .map(|input| input.reference.sequence_number)
            .chain(old_segment.iter().map(|segment| segment.coverage_sequence))
            .max()
            .unwrap_or(0);

        Ok(LateBuildSnapshot {
            local_origin,
            profile,
            inputs,
            consumed_overlays,
            old_segment,
            rows,
            coverage_sequence,
            fts_configs: metadata.full_text_search,
            carried_matrix_blocks,
            flat_calibration,
            requires_foreign_materialization,
        })
    }
}

/// Rebind every section-resident foreign artifact to a target-owned key.
///
/// Branch materialization rebuilds the active segment target-owned, but the
/// small immutable residents — the profile's FDE transform, centering means,
/// typed sources, and quarantine evidence — still reference the source
/// namespace and would keep the locality projection foreign forever,
/// permanently blocking source-root release. Each foreign ref is rewritten
/// to `{namespace}/{suffix}` (the suffixes are content-addressed, so the
/// checksummed identity is unchanged) and the returned `(source, target)`
/// pairs tell the caller which object bytes to copy before publication.
fn rebind_foreign_section_residents(
    section: &mut LateStateSection,
    namespace: &str,
    local: &ArtifactOrigin,
) -> Result<Vec<(String, String)>> {
    let origins = section.artifact_origins.clone();
    let mut copies: Vec<(String, String)> = Vec::new();
    let mut rebind =
        |key: &mut String, origin_slot: &mut Option<ArtifactOriginIndex>| -> Result<()> {
            let Some(index) = origin_slot.as_ref() else {
                return Ok(());
            };
            let table_index = usize::try_from(index.get()).map_err(|_| {
                ZeppelinError::Serialization(format!(
                    "section origin index {} does not fit this platform",
                    index.get()
                ))
            })?;
            let origin = origins.get(table_index).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "section origin index {table_index} is out of bounds for table length {}",
                    origins.len()
                ))
            })?;
            if origin == local {
                *origin_slot = None;
                return Ok(());
            }
            let prefix = format!("{}/", origin.namespace.as_str());
            let suffix = key.strip_prefix(&prefix).ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "foreign section artifact {key} is not under its origin namespace {}",
                    origin.namespace.as_str()
                ))
            })?;
            let target_key = format!("{namespace}/{suffix}");
            copies.push((key.clone(), target_key.clone()));
            *key = target_key;
            *origin_slot = None;
            Ok(())
        };
    for source in &mut section.source_inventory {
        rebind(&mut source.key, &mut source.artifact_origin)?;
    }
    if let Some(profile) = section.active_profile.as_mut() {
        if let Some(mean) = profile.epoch.exact_scoring_transform.mean_mut() {
            rebind(&mut mean.key, &mut mean.artifact_origin)?;
        }
        rebind(
            &mut profile.fde.transform_artifact.key,
            &mut profile.fde.transform_artifact.artifact_origin,
        )?;
        if let Some(mean) = profile.fde.candidate_vector_transform.mean_mut() {
            rebind(&mut mean.key, &mut mean.artifact_origin)?;
        }
    }
    for evidence in &mut section.quarantine_evidence {
        rebind(
            &mut evidence.source_fragment.key,
            &mut evidence.source_fragment.artifact_origin,
        )?;
        rebind(&mut evidence.key, &mut evidence.artifact_origin)?;
    }
    Ok(copies)
}

/// Decide whether the old flat segment can be reused incrementally.
///
/// Incremental reuse requires the flat candidate kind, a locally-owned old
/// segment (branch materialization stays a full-corpus rebuild), and bounded
/// churn: the fraction of ids touched by the new inputs over the previous
/// row count must not exceed the configured threshold. A threshold of zero
/// disables incremental reuse entirely.
fn plan_incremental_rebuild(
    segment: &LateInteractionSegmentRef,
    section: &LateStateSection,
    local_origin: &ArtifactOrigin,
    inputs: &[SnapshottedInput],
    max_changed_fraction: f32,
) -> Result<IncrementalDecision> {
    if !max_changed_fraction.is_finite() || max_changed_fraction <= 0.0 {
        return Ok(IncrementalDecision::Full {
            reason: "incremental reuse disabled by configuration",
        });
    }
    if segment.candidate_kind != LateCandidateKind::FlatSq8 {
        return Ok(IncrementalDecision::Full {
            reason: "old segment does not use the flat candidate kind",
        });
    }
    let segment_origin = resolve_section_origin(section, local_origin, segment.artifact_origin)?;
    if segment_origin != local_origin {
        return Ok(IncrementalDecision::Full {
            reason: "old segment is foreign-owned; branch materialization",
        });
    }
    let mut changed = BTreeSet::new();
    for input in inputs {
        for record in &input.fragment.upserts {
            changed.insert(record.id.clone());
        }
        for deleted in &input.fragment.deletes {
            changed.insert(deleted.clone());
        }
    }
    if segment.record_count == 0 {
        return Ok(IncrementalDecision::Full {
            reason: "old segment has no rows",
        });
    }
    let changed_fraction = changed.len() as f64 / segment.record_count as f64;
    if changed_fraction > f64::from(max_changed_fraction) {
        return Ok(IncrementalDecision::Full {
            reason: "changed-row fraction exceeds the incremental threshold",
        });
    }
    Ok(IncrementalDecision::Incremental {
        changed,
        changed_fraction,
    })
}

/// Assemble the incremental baseline from the old flat segment.
///
/// Reads only the flat artifact plus matrix blocks containing changed rows.
/// Untouched blocks are carried by reference with their rows' SQ8 codes
/// reused verbatim under the frozen calibration; rows co-located with a
/// change move into new blocks with their stored payload bytes carried
/// verbatim (never re-encoded).
async fn assemble_incremental_baseline(
    store: &ZeppelinStore,
    segment: &LateInteractionSegmentRef,
    profile: &EmbeddingProfileRef,
    changed: &BTreeSet<VectorId>,
    max_resident_bytes: usize,
    max_object_bytes: usize,
) -> Result<(
    Vec<LateSegmentBuildRow>,
    Vec<MatrixBlockRef>,
    FlatCalibrationSource,
)> {
    let flat = segment.flat_candidate_ref()?;
    let flat_bytes = store.get(&flat.key).await?;
    let resident = ResidentFlatCandidateIndex::from_bytes(&flat_bytes, flat, max_resident_bytes)?;

    let known_blocks = segment
        .matrix_objects
        .iter()
        .map(|block| block.key.as_str())
        .collect::<BTreeSet<_>>();
    let mut touched_keys = BTreeSet::new();
    for row in resident.rows() {
        if !known_blocks.contains(row.matrix_locator.object_key.as_str()) {
            return Err(ZeppelinError::Serialization(
                "flat candidate row references an unknown matrix block".to_string(),
            ));
        }
        if changed.contains(&row.id) {
            touched_keys.insert(row.matrix_locator.object_key.clone());
        }
    }
    let carried_blocks = segment
        .matrix_objects
        .iter()
        .filter(|block| !touched_keys.contains(&block.key))
        .cloned()
        .collect::<Vec<_>>();

    let row_limit = usize::try_from(segment.record_count).map_err(|_| {
        ZeppelinError::Validation("late segment row count exceeds usize".to_string())
    })?;
    let mut displaced = BTreeMap::new();
    for reference in &segment.matrix_objects {
        if !touched_keys.contains(&reference.key) {
            continue;
        }
        if usize::try_from(reference.size_bytes)
            .ok()
            .is_none_or(|size| size > max_object_bytes)
        {
            return Err(ZeppelinError::Validation(format!(
                "late matrix object {} exceeds configured bound",
                reference.key
            )));
        }
        let bytes = store.get(&reference.key).await?;
        for row in decode_matrix_block(
            &bytes,
            reference,
            row_limit,
            profile.epoch.max_document_vectors as usize,
        )? {
            if displaced.insert(row.id.clone(), row).is_some() {
                return Err(ZeppelinError::Serialization(
                    "touched late segment blocks contain duplicate row IDs".to_string(),
                ));
            }
        }
    }

    let mut baseline = Vec::with_capacity(resident.rows().len().saturating_sub(changed.len()));
    for (index, row) in resident.rows().iter().enumerate() {
        if changed.contains(&row.id) {
            // The old version stays behind in its (touched, dropped) block.
            displaced.remove(&row.id);
            continue;
        }
        let code = resident.row_code(index)?.to_vec();
        let matrix = if touched_keys.contains(&row.matrix_locator.object_key) {
            let decoded = displaced.remove(&row.id).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "surviving flat candidate {} has no decoded matrix row",
                    row.id
                ))
            })?;
            let ordinal = row.unit_ordinal.unwrap_or(0);
            if decoded.ordinal != ordinal
                || decoded.content_hash != row.content_hash
                || decoded.locator != row.matrix_locator
            {
                return Err(ZeppelinError::Serialization(format!(
                    "surviving flat candidate {} disagrees with its matrix row",
                    row.id
                )));
            }
            LateRowMatrixSource::Fresh {
                exact_matrix: decoded.embedding,
                exact_payload: decoded.payload,
            }
        } else {
            LateRowMatrixSource::Carried {
                locator: row.matrix_locator.clone(),
            }
        };
        baseline.push(LateSegmentBuildRow {
            id: row.id.clone(),
            content_hash: row.content_hash,
            source_sequence: row.source_sequence,
            parent_id: row.parent_id.clone(),
            unit_ordinal: row.unit_ordinal,
            attributes: row.filter_attributes.clone(),
            matrix,
            fde: CandidateFdeSource::Sq8(code),
        });
    }
    if !displaced.is_empty() {
        return Err(ZeppelinError::Serialization(
            "touched late segment blocks contain rows outside the flat index".to_string(),
        ));
    }
    Ok((
        baseline,
        carried_blocks,
        FlatCalibrationSource::Frozen(resident.calibration().clone()),
    ))
}

fn validate_late_metadata(namespace: &str, metadata: &NamespaceMetadata) -> Result<()> {
    if metadata.name != namespace {
        return Err(ZeppelinError::Serialization(format!(
            "namespace metadata name {} does not match {namespace}",
            metadata.name
        )));
    }
    match metadata.state {
        NamespaceState::Active => {}
        NamespaceState::Creating => {
            return Err(ZeppelinError::Validation(format!(
                "late namespace {namespace} is still creating"
            )))
        }
        NamespaceState::Deleting => {
            return Err(ZeppelinError::NamespaceDeleting {
                namespace: namespace.to_string(),
            })
        }
    }
    if metadata.index_type != IndexType::LateInteractionFde
        || metadata.late_interaction.is_none()
        || metadata.dimensions != 0
        || metadata.index_config.is_some()
    {
        return Err(ZeppelinError::Validation(format!(
            "namespace {namespace} is not a valid late-interaction namespace"
        )));
    }
    for (field, config) in &metadata.full_text_search {
        config.validate(&format!("full_text_search.{field}"))?;
    }
    Ok(())
}

fn active_late_segment(section: &LateStateSection) -> Option<&LateInteractionSegmentRef> {
    let active = section.active_late_segment.as_deref()?;
    section
        .late_interaction_segments
        .iter()
        .find(|segment| segment.id == active)
}

fn validate_old_segment_recipe(
    segment: &LateInteractionSegmentRef,
    profile: &EmbeddingProfileRef,
) -> Result<()> {
    if segment.profile != profile.profile
        || segment.semantic_epoch != profile.epoch.id
        || segment.fde_generation != profile.fde.generation
        || segment.matrix_dtype != profile.epoch.matrix_dtype
        || segment.vector_dimension != profile.epoch.vector_dimension
    {
        return Err(ZeppelinError::Validation(
            "active late segment disagrees with the active profile".to_string(),
        ));
    }
    Ok(())
}

async fn load_snapshotted_inputs(
    store: &ZeppelinStore,
    manifest: &Manifest,
) -> Result<Vec<SnapshottedInput>> {
    let mut references = manifest.input_fragments.iter().collect::<Vec<_>>();
    references.sort_by_key(|reference| reference.sequence_number);
    let mut sequences = BTreeSet::new();
    let mut inputs = Vec::with_capacity(references.len());
    for reference in references {
        if !sequences.insert(reference.sequence_number) {
            return Err(ZeppelinError::Validation(format!(
                "duplicate typed-input sequence {}",
                reference.sequence_number
            )));
        }
        let origin = manifest.input_fragment_origin(reference)?;
        let key = EncoderInputWalFragment::s3_key(origin.namespace.as_str(), &reference.id);
        let fragment = Manifest::read_input_fragment_checked(store, reference, &origin).await?;
        if fragment.referenced_content_bytes()? != reference.referenced_content_bytes
            || fragment.modality_counts() != reference.modality_counts
        {
            return Err(ZeppelinError::Serialization(format!(
                "input fragment {} accounting metadata mismatch",
                reference.id
            )));
        }
        inputs.push(SnapshottedInput {
            reference: reference.clone(),
            origin,
            key,
            fragment,
        });
    }
    Ok(inputs)
}

async fn load_old_segment_rows(
    store: &ZeppelinStore,
    segment: &LateInteractionSegmentRef,
    profile: &EmbeddingProfileRef,
    max_resident_bytes: usize,
    max_cluster_bytes: usize,
    max_object_bytes: usize,
) -> Result<Vec<LateSegmentBuildRow>> {
    let row_limit = usize::try_from(segment.record_count).map_err(|_| {
        ZeppelinError::Validation("late segment row count exceeds usize".to_string())
    })?;
    let mut matrices = BTreeMap::new();
    for reference in &segment.matrix_objects {
        if usize::try_from(reference.size_bytes)
            .ok()
            .is_none_or(|size| size > max_object_bytes)
        {
            return Err(ZeppelinError::Validation(format!(
                "late matrix object {} exceeds configured bound",
                reference.key
            )));
        }
        let bytes = store.get(&reference.key).await?;
        for row in decode_matrix_block(
            &bytes,
            reference,
            row_limit,
            profile.epoch.max_document_vectors as usize,
        )? {
            if matrices.insert(row.id.clone(), row).is_some() {
                return Err(ZeppelinError::Serialization(
                    "active late segment has duplicate matrix row IDs".to_string(),
                ));
            }
        }
    }
    let mut attributes = BTreeMap::new();
    for reference in &segment.attribute_objects {
        if usize::try_from(reference.size_bytes)
            .ok()
            .is_none_or(|size| size > max_object_bytes)
        {
            return Err(ZeppelinError::Validation(format!(
                "late attribute object {} exceeds configured bound",
                reference.key
            )));
        }
        let bytes = store.get(&reference.key).await?;
        for row in decode_attribute_block(&bytes, reference, row_limit, max_object_bytes)? {
            if attributes.insert(row.id.clone(), row).is_some() {
                return Err(ZeppelinError::Serialization(
                    "active late segment has duplicate attribute row IDs".to_string(),
                ));
            }
        }
    }

    let mut rows = Vec::new();
    match segment.candidate_kind {
        LateCandidateKind::Ivf => {
            let candidate_index = segment.ivf_candidate_index()?;
            let bootstrap_bytes = store.get(&candidate_index.bootstrap.key).await?;
            let mut cluster_bytes = Vec::with_capacity(candidate_index.clusters.len());
            for reference in &candidate_index.clusters {
                cluster_bytes.push(store.get(&reference.key).await?);
            }
            let fetched = candidate_index
                .clusters
                .iter()
                .zip(&cluster_bytes)
                .map(|(reference, bytes)| FetchedLateCandidateCluster {
                    reference,
                    bytes: bytes.as_ref(),
                })
                .collect::<Vec<_>>();
            let candidates = decode_all_candidate_rows(
                candidate_index,
                &bootstrap_bytes,
                &fetched,
                max_resident_bytes,
                max_cluster_bytes,
            )?;
            rows.reserve(candidates.len());
            for candidate in candidates {
                let matrix = matrices.remove(&candidate.id).ok_or_else(|| {
                    ZeppelinError::Serialization(format!(
                        "candidate {} has no exact matrix row",
                        candidate.id
                    ))
                })?;
                let attributes_row = attributes.remove(&candidate.id).ok_or_else(|| {
                    ZeppelinError::Serialization(format!(
                        "candidate {} has no exact attribute row",
                        candidate.id
                    ))
                })?;
                let ordinal = candidate.unit_ordinal.unwrap_or(0);
                if matrix.ordinal != ordinal
                    || attributes_row.ordinal != ordinal
                    || matrix.content_hash != candidate.content_hash
                    || matrix.locator != candidate.matrix_locator
                    || attributes_row.locator != candidate.attr_locator
                    || attributes_row.attributes != candidate.filter_attributes
                {
                    return Err(ZeppelinError::Serialization(format!(
                        "candidate {} disagrees with its exact matrix or attribute row",
                        candidate.id
                    )));
                }
                rows.push(LateSegmentBuildRow {
                    id: candidate.id,
                    content_hash: candidate.content_hash,
                    source_sequence: candidate.source_sequence,
                    parent_id: candidate.parent_id,
                    unit_ordinal: candidate.unit_ordinal,
                    attributes: attributes_row.attributes,
                    matrix: LateRowMatrixSource::Fresh {
                        exact_matrix: matrix.embedding,
                        exact_payload: matrix.payload,
                    },
                    fde: candidate.fde,
                });
            }
        }
        LateCandidateKind::FlatSq8 => {
            // The flat artifact stores SQ8 codes, not raw FDEs, so baseline
            // FDEs are recomputed from the stored exact matrices through the
            // exact enrichment seam (candidate transform, f16 persistence
            // boundary, pooling, FDE encode). This requires the identity
            // exact-scoring transform so the stored matrix equals the encoder
            // output at the persisted dtype.
            if !matches!(
                profile.epoch.exact_scoring_transform,
                VectorTransformRecipe::Identity
            ) {
                return Err(ZeppelinError::Validation(
                    "flat late segment rebuild requires an identity exact-scoring transform"
                        .to_string(),
                ));
            }
            let flat = segment.flat_candidate_ref()?;
            let flat_bytes = store.get(&flat.key).await?;
            let resident =
                ResidentFlatCandidateIndex::from_bytes(&flat_bytes, flat, max_resident_bytes)?;
            let transform = load_profile_fde_transform(store, profile).await?;
            if transform.output_dimension()
                != usize::try_from(flat.recipe.fde_dimension).map_err(|_| {
                    ZeppelinError::Validation("flat FDE dimension exceeds usize".to_string())
                })?
            {
                return Err(ZeppelinError::Validation(
                    "flat late segment FDE dimension disagrees with the profile transform"
                        .to_string(),
                ));
            }
            let dimension = profile.epoch.vector_dimension as usize;
            let max_vectors = profile.epoch.max_document_vectors as usize;
            let candidate_mean = load_vector_transform_mean(
                store,
                &profile.fde.candidate_vector_transform,
                dimension,
            )
            .await?;
            rows.reserve(resident.rows().len());
            for row in resident.rows() {
                let matrix = matrices.remove(&row.id).ok_or_else(|| {
                    ZeppelinError::Serialization(format!(
                        "flat candidate {} has no exact matrix row",
                        row.id
                    ))
                })?;
                let attributes_row = attributes.remove(&row.id).ok_or_else(|| {
                    ZeppelinError::Serialization(format!(
                        "flat candidate {} has no exact attribute row",
                        row.id
                    ))
                })?;
                let ordinal = row.unit_ordinal.unwrap_or(0);
                if matrix.ordinal != ordinal
                    || attributes_row.ordinal != ordinal
                    || matrix.content_hash != row.content_hash
                    || matrix.locator != row.matrix_locator
                    || attributes_row.locator != row.attr_locator
                    || attributes_row.attributes != row.filter_attributes
                {
                    return Err(ZeppelinError::Serialization(format!(
                        "flat candidate {} disagrees with its exact matrix or attribute row",
                        row.id
                    )));
                }
                let centered = apply_vector_transform(
                    &matrix.embedding,
                    &profile.fde.candidate_vector_transform,
                    candidate_mean.as_deref(),
                    max_vectors,
                )?;
                let encoded = encode_matrix_payload(MatrixDtype::F16, dimension, &centered)?;
                let decoded = decode_matrix_payload(
                    &encoded,
                    MatrixDtype::F16,
                    dimension,
                    centered.vector_count(),
                    max_vectors,
                )?;
                let pooled = apply_candidate_document_pooling(
                    &decoded,
                    profile.fde.candidate_document_pooling,
                )?;
                let raw_fde = transform.encode_document(&pooled.matrix_ref()?)?;
                rows.push(LateSegmentBuildRow {
                    id: row.id.clone(),
                    content_hash: row.content_hash,
                    source_sequence: row.source_sequence,
                    parent_id: row.parent_id.clone(),
                    unit_ordinal: row.unit_ordinal,
                    attributes: attributes_row.attributes,
                    matrix: LateRowMatrixSource::Fresh {
                        exact_matrix: matrix.embedding,
                        exact_payload: matrix.payload,
                    },
                    fde: CandidateFdeSource::Raw(raw_fde),
                });
            }
        }
    }
    if !matrices.is_empty()
        || !attributes.is_empty()
        || u64::try_from(rows.len()).ok() != Some(segment.record_count)
    {
        return Err(ZeppelinError::Serialization(
            "active late segment artifact rows do not form one exact closure".to_string(),
        ));
    }
    Ok(rows)
}

/// Load and verify the profile's immutable FDE transform artifact.
async fn load_profile_fde_transform(
    store: &ZeppelinStore,
    profile: &EmbeddingProfileRef,
) -> Result<FdeTransform> {
    let transform_ref = &profile.fde.transform_artifact;
    let transform_bytes = store.get(&transform_ref.key).await?;
    if u64::try_from(transform_bytes.len()).ok() != Some(transform_ref.size_bytes)
        || ArtifactChecksum::digest(&transform_bytes) != transform_ref.checksum
    {
        return Err(ZeppelinError::Serialization(
            "late compaction FDE transform metadata mismatch".to_string(),
        ));
    }
    let transform = FdeTransform::from_bytes(&transform_bytes)?;
    if transform.params() != profile.fde.params {
        return Err(ZeppelinError::Validation(
            "late compaction FDE transform recipe mismatch".to_string(),
        ));
    }
    Ok(transform)
}

async fn load_overlay_rows(
    store: &ZeppelinStore,
    section: &LateStateSection,
    section_origin: &ArtifactOrigin,
    profile: &EmbeddingProfileRef,
    inputs: &[SnapshottedInput],
) -> Result<(
    BTreeMap<VersionIdentity, DerivedRow>,
    BTreeSet<VersionIdentity>,
    Vec<SemanticOverlayRef>,
)> {
    let input_by_key = inputs
        .iter()
        .enumerate()
        .map(|(index, input)| (input.key.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let transform = load_profile_fde_transform(store, profile).await?;

    let mut derived = BTreeMap::new();
    let mut consumed = Vec::new();
    for overlay in &section.semantic_overlays {
        if overlay.semantic_epoch != profile.epoch.id
            || overlay.fde_generation != profile.fde.generation
        {
            continue;
        }
        let Some(&input_index) = input_by_key.get(overlay.source_fragment.key.as_str()) else {
            continue;
        };
        let input = &inputs[input_index];
        validate_overlay_source(section, section_origin, overlay, input)?;
        let matrix_bytes = store.get(&overlay.embeddings.key).await?;
        if u64::try_from(matrix_bytes.len()).ok() != Some(overlay.embeddings.size_bytes) {
            return Err(ZeppelinError::Serialization(format!(
                "semantic overlay matrix {} size mismatch",
                overlay.embeddings.key
            )));
        }
        let matrix = MatrixArtifact::from_bytes(
            &matrix_bytes,
            overlay.embeddings.checksum,
            profile.epoch.matrix_dtype,
            profile.epoch.id,
            input.fragment.checksum,
            profile.epoch.vector_dimension as usize,
            overlay.covered_versions.records.len(),
            profile.epoch.max_document_vectors as usize,
        )?;
        let fde_bytes = store.get(&overlay.fde_vectors.key).await?;
        if u64::try_from(fde_bytes.len()).ok() != Some(overlay.fde_vectors.size_bytes) {
            return Err(ZeppelinError::Serialization(format!(
                "semantic overlay FDE {} size mismatch",
                overlay.fde_vectors.key
            )));
        }
        let fde = FdeArtifact::from_bytes(
            &fde_bytes,
            overlay.fde_vectors.checksum,
            profile.fde.generation,
            overlay.embeddings.checksum,
            transform.output_dimension(),
            overlay.covered_versions.records.len(),
        )?;
        validate_overlay_artifact_metadata(overlay, &matrix, &fde, &input.fragment)?;
        let matrix_payloads = matrix_row_payloads(&matrix_bytes, &matrix)?;
        for (((version, matrix_row), matrix_payload), fde_row) in overlay
            .covered_versions
            .records
            .iter()
            .zip(matrix.rows())
            .zip(matrix_payloads)
            .zip(fde.rows())
        {
            if matrix_row.content_hash() != version.content_hash
                || fde_row.content_hash() != version.content_hash
            {
                return Err(ZeppelinError::Validation(
                    "semantic overlay content hashes disagree".to_string(),
                ));
            }
            let identity = version_identity(&overlay.source_fragment, version);
            if derived
                .insert(
                    identity,
                    DerivedRow {
                        exact_matrix: matrix_row.embedding().clone(),
                        exact_payload: matrix_payload,
                        raw_fde: fde_row.values().to_vec(),
                    },
                )
                .is_some()
            {
                return Err(ZeppelinError::Validation(
                    "semantic overlays duplicate one exact source version".to_string(),
                ));
            }
        }
        consumed.push(overlay.clone());
    }

    let mut failed = BTreeSet::new();
    for evidence in &section.quarantine_evidence {
        if evidence.semantic_epoch != profile.epoch.id
            || evidence.fde_generation != profile.fde.generation
        {
            continue;
        }
        let Some(&input_index) = input_by_key.get(evidence.source_fragment.key.as_str()) else {
            continue;
        };
        let input = &inputs[input_index];
        validate_source_identity(
            section,
            section_origin,
            &evidence.source_fragment,
            input,
            "quarantine",
        )?;
        for version in &evidence.failed_versions.records {
            validate_record_version(version, input)?;
            failed.insert(version_identity(&evidence.source_fragment, version));
        }
    }
    Ok((derived, failed, consumed))
}

fn validate_overlay_source(
    section: &LateStateSection,
    section_origin: &ArtifactOrigin,
    overlay: &SemanticOverlayRef,
    input: &SnapshottedInput,
) -> Result<()> {
    validate_source_identity(
        section,
        section_origin,
        &overlay.source_fragment,
        input,
        "semantic overlay",
    )?;
    if overlay.semantic_epoch != overlay.embeddings.semantic_epoch
        || overlay.fde_generation != overlay.fde_vectors.generation
        || overlay.covered_versions.records.is_empty()
        || overlay.embeddings.row_count as usize != overlay.covered_versions.records.len()
        || overlay.fde_vectors.row_count as usize != overlay.covered_versions.records.len()
    {
        return Err(ZeppelinError::Validation(
            "semantic overlay recipe or row-count mismatch".to_string(),
        ));
    }
    let mut previous = None;
    for version in &overlay.covered_versions.records {
        if previous.is_some_and(|ordinal| ordinal >= version.row_ordinal) {
            return Err(ZeppelinError::Validation(
                "semantic overlay coverage rows are not strictly increasing".to_string(),
            ));
        }
        previous = Some(version.row_ordinal);
        validate_record_version(version, input)?;
    }
    Ok(())
}

fn validate_source_identity(
    section: &LateStateSection,
    section_origin: &ArtifactOrigin,
    source: &PhysicalInputFragmentIdentity,
    input: &SnapshottedInput,
    kind: &str,
) -> Result<()> {
    let origin = resolve_section_origin(section, section_origin, source.artifact_origin)?;
    if origin != &input.origin
        || source.key != input.key
        || source.id != input.reference.id
        || source.checksum != input.fragment.checksum
        || source.size_bytes != input.reference.size_bytes
    {
        return Err(ZeppelinError::Validation(format!(
            "{kind} source identity does not match its exact input fragment"
        )));
    }
    Ok(())
}

fn resolve_section_origin<'a>(
    section: &'a LateStateSection,
    section_origin: &'a ArtifactOrigin,
    index: Option<ArtifactOriginIndex>,
) -> Result<&'a ArtifactOrigin> {
    let Some(index) = index else {
        return Ok(section_origin);
    };
    let index = usize::try_from(index.get()).map_err(|_| {
        ZeppelinError::Serialization("late-state origin index exceeds usize".to_string())
    })?;
    section.artifact_origins.get(index).ok_or_else(|| {
        ZeppelinError::Serialization(format!("late-state origin index {index} is out of bounds"))
    })
}

fn validate_record_version(version: &RecordVersionRef, input: &SnapshottedInput) -> Result<()> {
    let ordinal = usize::try_from(version.row_ordinal).map_err(|_| {
        ZeppelinError::Validation("semantic overlay row ordinal exceeds usize".to_string())
    })?;
    let record = input.fragment.upserts.get(ordinal).ok_or_else(|| {
        ZeppelinError::Validation("semantic overlay row ordinal is out of bounds".to_string())
    })?;
    if version.record_id != record.id
        || version.content_hash != record.content_hash
        || version.sequence != input.reference.sequence_number
    {
        return Err(ZeppelinError::Validation(
            "semantic overlay covered-version identity mismatch".to_string(),
        ));
    }
    Ok(())
}

fn validate_overlay_artifact_metadata(
    overlay: &SemanticOverlayRef,
    matrix: &MatrixArtifact,
    fde: &FdeArtifact,
    input: &EncoderInputWalFragment,
) -> Result<()> {
    if overlay.embeddings.dtype != matrix.dtype()
        || overlay.embeddings.row_count as usize != matrix.rows().len()
        || overlay.embeddings.source_fragment_checksum != input.checksum
        || overlay.embeddings.semantic_epoch != matrix.semantic_epoch()
        || overlay.embeddings.vector_dimension as usize != matrix.vector_dimension()
        || overlay.fde_vectors.row_count as usize != fde.rows().len()
        || overlay.fde_vectors.generation != fde.generation()
        || overlay.fde_vectors.embedding_fragment_checksum != fde.embedding_fragment_checksum()
        || overlay.fde_vectors.fde_dimension as usize != fde.fde_dimension()
    {
        return Err(ZeppelinError::Validation(
            "semantic overlay metadata disagrees with decoded artifacts".to_string(),
        ));
    }
    let total_vectors = matrix.rows().iter().try_fold(0_u64, |total, row| {
        total
            .checked_add(u64::try_from(row.embedding().vector_count()).map_err(|_| {
                ZeppelinError::Validation("matrix vector count exceeds u64".to_string())
            })?)
            .ok_or_else(|| {
                ZeppelinError::Validation("matrix total vector count overflows".to_string())
            })
    })?;
    if total_vectors != overlay.embeddings.total_vectors {
        return Err(ZeppelinError::Validation(
            "semantic overlay matrix total-vector metadata mismatch".to_string(),
        ));
    }
    Ok(())
}

fn version_identity(
    source: &PhysicalInputFragmentIdentity,
    version: &RecordVersionRef,
) -> VersionIdentity {
    VersionIdentity {
        source_key: source.key.clone(),
        source_checksum: source.checksum,
        row_ordinal: version.row_ordinal,
        record_id: version.record_id.clone(),
        content_hash: version.content_hash,
        sequence: version.sequence,
    }
}

fn input_version_identity(
    input: &SnapshottedInput,
    row_ordinal: usize,
    record: &RetrievalUnitRecord,
) -> Result<VersionIdentity> {
    Ok(VersionIdentity {
        source_key: input.key.clone(),
        source_checksum: input.fragment.checksum,
        row_ordinal: u32::try_from(row_ordinal)
            .map_err(|_| ZeppelinError::Validation("input row ordinal exceeds u32".to_string()))?,
        record_id: record.id.clone(),
        content_hash: record.content_hash,
        sequence: input.reference.sequence_number,
    })
}

fn replay_late_rows(
    baseline: Vec<LateSegmentBuildRow>,
    inputs: &[SnapshottedInput],
    derived: &BTreeMap<VersionIdentity, DerivedRow>,
    failed: &BTreeSet<VersionIdentity>,
) -> Result<Vec<LateSegmentBuildRow>> {
    let mut live = baseline
        .into_iter()
        .map(|row| (row.id.clone(), LiveRow::Baseline(Box::new(row))))
        .collect::<BTreeMap<_, _>>();
    for (input_index, input) in inputs.iter().enumerate() {
        for (row_ordinal, record) in input.fragment.upserts.iter().enumerate() {
            live.insert(
                record.id.clone(),
                LiveRow::Input {
                    input_index,
                    row_ordinal,
                },
            );
        }
        for deleted in &input.fragment.deletes {
            live.remove(deleted);
        }
    }

    let mut rows = Vec::with_capacity(live.len());
    for (_, source) in live {
        match source {
            LiveRow::Baseline(row) => rows.push(*row),
            LiveRow::Input {
                input_index,
                row_ordinal,
            } => {
                let input = &inputs[input_index];
                let record = &input.fragment.upserts[row_ordinal];
                let identity = input_version_identity(input, row_ordinal, record)?;
                if failed.contains(&identity) {
                    return Err(ZeppelinError::Validation(format!(
                        "late compaction is blocked by failed enrichment for {}",
                        record.id
                    )));
                }
                let output = derived.get(&identity).ok_or_else(|| {
                    ZeppelinError::Validation(format!(
                        "late compaction is blocked by pending enrichment for {}",
                        record.id
                    ))
                })?;
                rows.push(LateSegmentBuildRow {
                    id: record.id.clone(),
                    content_hash: record.content_hash,
                    source_sequence: input.reference.sequence_number,
                    parent_id: record.parent_id.clone(),
                    unit_ordinal: record.unit_ordinal,
                    attributes: record.attributes.clone(),
                    matrix: LateRowMatrixSource::Fresh {
                        exact_matrix: output.exact_matrix.clone(),
                        exact_payload: output.exact_payload.clone(),
                    },
                    fde: CandidateFdeSource::Raw(output.raw_fde.clone()),
                });
            }
        }
    }
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(rows)
}

async fn build_global_fts(
    namespace: &str,
    segment_id: &str,
    rows: &[LateSegmentBuildRow],
    configs: &HashMap<String, FtsFieldConfig>,
) -> Result<Vec<PrebuiltLateFtsArtifact>> {
    if configs.is_empty() {
        return Ok(Vec::new());
    }
    let attributes = rows
        .iter()
        .map(|row| row.attributes.clone())
        .collect::<Vec<_>>();
    let configs = configs.clone();
    let bytes = tokio::task::spawn_blocking(move || {
        let refs = attributes.iter().map(Option::as_ref).collect::<Vec<_>>();
        InvertedIndex::build(&refs, &configs).to_bytes()
    })
    .await
    .map_err(|error| ZeppelinError::Index(format!("late FTS build task failed: {error}")))??;
    let checksum = ArtifactChecksum::digest(&bytes);
    let key = format!(
        "{}{segment_id}/fts_{}.bin",
        NamespaceObjectFamily::LateSegment.namespace_prefix(namespace),
        checksum.to_hex()
    );
    let owned = NamespaceObjectKey::classify(namespace, key.clone())?;
    if owned.family() != NamespaceObjectFamily::LateSegment {
        return Err(ZeppelinError::Validation(
            "late FTS key is outside the late-segment family".to_string(),
        ));
    }
    Ok(vec![PrebuiltLateFtsArtifact {
        reference: LateSegmentObjectRef {
            key,
            checksum,
            size_bytes: u64::try_from(bytes.len()).map_err(|_| {
                ZeppelinError::Serialization("late FTS size exceeds u64".to_string())
            })?,
            format_version: LATE_FTS_FORMAT_VERSION,
        },
        bytes,
    }])
}

async fn put_create_verified(store: &ZeppelinStore, key: &str, bytes: Bytes) -> Result<()> {
    match store.put_create_outcome(key, bytes.clone()).await? {
        CreateOnlyOutcome::Created { .. } => Ok(()),
        CreateOnlyOutcome::AlreadyExists => {
            let existing = store.get(key).await?;
            if existing != bytes {
                return Err(ZeppelinError::Serialization(format!(
                    "late segment content-address collision at {key}"
                )));
            }
            Ok(())
        }
    }
}

fn same_input_descriptor(left: &InputFragmentRef, right: &InputFragmentRef) -> bool {
    left.id == right.id
        && left.upsert_count == right.upsert_count
        && left.delete_count == right.delete_count
        && left.sequence_number == right.sequence_number
        && left.size_bytes == right.size_bytes
        && left.referenced_content_bytes == right.referenced_content_bytes
        && left.modality_counts == right.modality_counts
}

fn require_snapshotted_inputs(manifest: &Manifest, inputs: &[SnapshottedInput]) -> Result<()> {
    for input in inputs {
        let found = manifest.input_fragments.iter().any(|candidate| {
            same_input_descriptor(candidate, &input.reference)
                && manifest
                    .input_fragment_origin(candidate)
                    .is_ok_and(|origin| origin == input.origin)
        });
        if !found {
            return Err(ZeppelinError::ManifestConflict {
                namespace: input.origin.namespace.as_str().to_string(),
            });
        }
    }
    Ok(())
}

fn remove_snapshotted_inputs(manifest: &mut Manifest, inputs: &[SnapshottedInput]) -> Result<()> {
    let mut retained = Vec::with_capacity(manifest.input_fragments.len());
    let mut removed = 0_usize;
    for candidate in &manifest.input_fragments {
        let origin = manifest.input_fragment_origin(candidate)?;
        if inputs.iter().any(|input| {
            same_input_descriptor(candidate, &input.reference) && origin == input.origin
        }) {
            removed += 1;
        } else {
            retained.push(candidate.clone());
        }
    }
    if removed != inputs.len() {
        return Err(ZeppelinError::ManifestConflict {
            namespace: manifest.local_origin()?.namespace.as_str().to_string(),
        });
    }
    manifest.input_fragments = retained;
    Ok(())
}

fn require_snapshotted_overlays(
    section: &LateStateSection,
    overlays: &[SemanticOverlayRef],
    namespace: &str,
) -> Result<()> {
    if overlays
        .iter()
        .any(|overlay| !section.semantic_overlays.contains(overlay))
    {
        return Err(ZeppelinError::ManifestConflict {
            namespace: namespace.to_string(),
        });
    }
    Ok(())
}

fn remove_consumed_overlays(section: &mut LateStateSection, overlays: &[SemanticOverlayRef]) {
    section
        .semantic_overlays
        .retain(|candidate| !overlays.contains(candidate));
}

fn replace_active_late_segment(
    section: &mut LateStateSection,
    old: Option<&LateInteractionSegmentRef>,
    new: Option<LateInteractionSegmentRef>,
    namespace: &str,
) -> Result<()> {
    if let Some(old) = old {
        let before = section.late_interaction_segments.len();
        section
            .late_interaction_segments
            .retain(|candidate| candidate != old);
        if section.late_interaction_segments.len() + 1 != before {
            return Err(ZeppelinError::ManifestConflict {
                namespace: namespace.to_string(),
            });
        }
    }
    if let Some(new) = new {
        section.active_late_segment = Some(new.id.clone());
        section.late_interaction_segments.push(new);
        section
            .late_interaction_segments
            .sort_by(|left, right| left.id.cmp(&right.id));
    } else {
        if !section.late_interaction_segments.is_empty() {
            return Err(ZeppelinError::ManifestConflict {
                namespace: namespace.to_string(),
            });
        }
        section.active_late_segment = None;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::embedding::{
        ContentHash, EncoderInputRef, ModalityCounts, MultiVectorEmbedding, RetrievalUnitRecord,
        TextContentRef,
    };
    use crate::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};
    use crate::namespace::{NamespaceId, NamespaceIncarnationId};

    use super::{
        encode_matrix_payload, input_version_identity, replay_late_rows, resolve_section_origin,
        same_input_descriptor, BTreeMap, BTreeSet, CandidateFdeSource, DerivedRow,
        EncoderInputWalFragment, InputFragmentRef, LateRowMatrixSource, LateSegmentBuildRow,
        LateStateSection, MatrixDtype, SnapshottedInput,
    };

    fn input_ref(sequence_number: u64) -> InputFragmentRef {
        InputFragmentRef {
            id: ulid::Ulid::from(7_u128),
            upsert_count: 2,
            delete_count: 1,
            sequence_number,
            size_bytes: 91,
            referenced_content_bytes: 17,
            modality_counts: ModalityCounts {
                text: 1,
                image: 1,
                image_text: 0,
            },
            artifact_origin: None,
        }
    }

    #[test]
    fn exact_input_snapshot_ignores_only_rebased_origin_index() {
        let left = input_ref(4);
        let mut right = left.clone();
        right.artifact_origin = Some(ArtifactOriginIndex::new(3));
        assert!(same_input_descriptor(&left, &right));

        right.sequence_number = 5;
        assert!(!same_input_descriptor(&left, &right));
    }

    #[test]
    fn section_origin_resolution_is_explicit_and_bounds_checked() {
        let local = origin("local", 1);
        let foreign = origin("foreign", 2);
        let mut section = LateStateSection::new();
        section.artifact_origins.push(foreign.clone());

        assert_eq!(
            resolve_section_origin(&section, &local, None).expect("local origin"),
            &local
        );
        assert_eq!(
            resolve_section_origin(&section, &local, Some(ArtifactOriginIndex::new(0)))
                .expect("foreign origin"),
            &foreign
        );
        assert!(
            resolve_section_origin(&section, &local, Some(ArtifactOriginIndex::new(1))).is_err()
        );
    }

    #[test]
    fn replay_applies_new_versions_and_tombstones_over_old_baseline() {
        let replacement = text_record("same", "new contents");
        let fragment = EncoderInputWalFragment::try_new(
            vec![replacement.clone()],
            vec!["deleted".to_string()],
        )
        .expect("input fragment");
        let origin = origin("local", 3);
        let reference = InputFragmentRef {
            id: fragment.id,
            upsert_count: fragment.upserts.len(),
            delete_count: fragment.deletes.len(),
            sequence_number: 9,
            size_bytes: fragment.to_bytes().expect("input bytes").len() as u64,
            referenced_content_bytes: fragment
                .referenced_content_bytes()
                .expect("referenced bytes"),
            modality_counts: fragment.modality_counts(),
            artifact_origin: None,
        };
        let input = SnapshottedInput {
            key: EncoderInputWalFragment::s3_key(origin.namespace.as_str(), &reference.id),
            reference,
            origin,
            fragment,
        };
        let identity = input_version_identity(&input, 0, &replacement).expect("version identity");
        let exact_matrix = matrix(0.75);
        let derived = BTreeMap::from([(
            identity,
            DerivedRow {
                exact_matrix: exact_matrix.clone(),
                exact_payload: encode_matrix_payload(MatrixDtype::F16, 2, &exact_matrix)
                    .expect("test payload"),
                raw_fde: vec![0.25, 0.5],
            },
        )]);
        let baseline = vec![baseline_row("same", 2), baseline_row("deleted", 3)];

        let rows = replay_late_rows(baseline, &[input], &derived, &BTreeSet::new())
            .expect("replayed rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "same");
        assert_eq!(rows[0].source_sequence, 9);
        assert_eq!(rows[0].content_hash, replacement.content_hash);
        match &rows[0].matrix {
            LateRowMatrixSource::Fresh {
                exact_matrix: fresh,
                ..
            } => assert_eq!(*fresh, exact_matrix),
            LateRowMatrixSource::Carried { .. } => panic!("replayed row must be fresh"),
        }
        match &rows[0].fde {
            CandidateFdeSource::Raw(fde) => assert_eq!(*fde, vec![0.25, 0.5]),
            CandidateFdeSource::Sq8(_) => panic!("replayed row must carry a raw FDE"),
        }
    }

    #[test]
    fn replay_applies_same_fragment_delete_after_upsert() {
        let replacement = text_record("same", "transient contents");
        let fragment = EncoderInputWalFragment {
            id: ulid::Ulid::from(8_u128),
            upserts: vec![replacement],
            deletes: vec!["same".to_string()],
            checksum: 0,
        };
        let origin = origin("local", 4);
        let input = SnapshottedInput {
            key: EncoderInputWalFragment::s3_key(origin.namespace.as_str(), &fragment.id),
            reference: InputFragmentRef {
                id: fragment.id,
                upsert_count: 1,
                delete_count: 1,
                sequence_number: 10,
                size_bytes: 1,
                referenced_content_bytes: 0,
                modality_counts: fragment.modality_counts(),
                artifact_origin: None,
            },
            origin,
            fragment,
        };

        let rows = replay_late_rows(
            vec![baseline_row("same", 2)],
            &[input],
            &BTreeMap::new(),
            &BTreeSet::new(),
        )
        .expect("replayed rows");
        assert!(rows.is_empty());
    }

    fn origin(namespace: &str, incarnation: u128) -> ArtifactOrigin {
        ArtifactOrigin {
            namespace: NamespaceId::parse(namespace.to_string()).expect("namespace"),
            incarnation: NamespaceIncarnationId::from_uuid(uuid::Uuid::from_u128(incarnation)),
        }
    }

    fn text_record(id: &str, text: &str) -> RetrievalUnitRecord {
        let input = EncoderInputRef::Text {
            content: TextContentRef::Inline(text.to_string()),
        };
        RetrievalUnitRecord {
            id: id.to_string(),
            content_hash: input.content_hash().expect("content hash"),
            input,
            parent_id: None,
            unit_ordinal: None,
            attributes: None,
        }
    }

    fn matrix(value: f32) -> MultiVectorEmbedding {
        MultiVectorEmbedding::new(vec![value, value], 1, 2, 1).expect("matrix")
    }

    fn baseline_row(id: &str, sequence: u64) -> LateSegmentBuildRow {
        let exact_matrix = matrix(sequence as f32);
        LateSegmentBuildRow {
            id: id.to_string(),
            content_hash: ContentHash::new([sequence as u8; 32]),
            source_sequence: sequence,
            parent_id: None,
            unit_ordinal: None,
            attributes: None,
            matrix: LateRowMatrixSource::Fresh {
                exact_payload: encode_matrix_payload(MatrixDtype::F16, 2, &exact_matrix)
                    .expect("test payload"),
                exact_matrix,
            },
            fde: CandidateFdeSource::Raw(vec![sequence as f32, 0.0]),
        }
    }
}
