//! Exact-key reachability and two-pass storage garbage collection.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use ulid::Ulid;

use crate::config::GcConfig;
use crate::error::Result;
use crate::fts::global_index::global_fts_key;
use crate::fts::inverted_index::fts_index_key;
use crate::index::bitmap::bitmap_key;
use crate::index::hierarchical::tree_meta_key;
use crate::index::ivf_flat::build::{attrs_key, centroids_key, cluster_key};
use crate::index::quantization::pq::{pq_cluster_key, pq_codebook_key};
use crate::index::quantization::sq::{sq_calibration_key, sq_cluster_key};
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::wal::fragment::WalFragment;
use crate::wal::manifest::Manifest;
use crate::wal::Lease;

const GC_CANDIDATE_STORE_VERSION: u32 = 1;

/// Lease-scoped side-object recording compaction uploads that have not yet
/// been committed into the manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionStaging {
    /// Fencing token of the lease that owns these in-flight uploads.
    pub fencing_token: u64,
    /// Exact S3 keys uploaded by the compaction and not yet manifest-live.
    #[serde(default)]
    pub keys: BTreeSet<String>,
}

/// S3 key for a compaction staging side object.
#[must_use]
pub fn staging_key(namespace: &str, fencing_token: u64) -> String {
    format!("{namespace}/_staging/{fencing_token}.json")
}

/// Exact-key set of every S3 object still referenced by `manifest`.
#[must_use]
pub fn reachable_keys(namespace: &str, manifest: &Manifest) -> BTreeSet<String> {
    reachable_keys_with_staging(namespace, manifest, &BTreeSet::new())
}

/// Exact-key set of every manifest-referenced object plus active staged uploads.
#[must_use]
pub fn reachable_keys_with_staging(
    namespace: &str,
    manifest: &Manifest,
    staging: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();

    for fragment in &manifest.fragments {
        keys.insert(WalFragment::s3_key(namespace, &fragment.id));
    }

    for segment in &manifest.segments {
        if segment.hierarchical {
            keys.insert(tree_meta_key(namespace, &segment.id));
        } else {
            keys.insert(centroids_key(namespace, &segment.id));
        }

        if let Some(sketch) = &segment.sketch {
            keys.insert(sketch.key.clone());
        }
        if let Some(bootstrap) = &segment.bootstrap {
            keys.insert(bootstrap.key.clone());
        }
        if let Some(membership) = &segment.membership {
            keys.insert(membership.key.clone());
        }

        if segment.cluster_objects.is_empty() {
            for cluster_idx in 0..segment.cluster_count {
                keys.insert(cluster_key(
                    namespace,
                    segment.cluster_owner(cluster_idx),
                    cluster_idx,
                ));
            }
        } else {
            for object_ref in &segment.cluster_objects {
                keys.insert(object_ref.key.clone());
            }
        }

        for cluster_idx in 0..segment.cluster_count {
            let owner = segment.cluster_owner(cluster_idx);
            keys.insert(attrs_key(namespace, owner, cluster_idx));

            if !segment.bitmap_fields.is_empty() {
                keys.insert(bitmap_key(namespace, owner, cluster_idx));
            }

            if !segment.fts_fields.is_empty() {
                keys.insert(fts_index_key(namespace, owner, cluster_idx));
            }

            match segment.quantization {
                QuantizationType::Scalar => {
                    keys.insert(sq_cluster_key(namespace, owner, cluster_idx));
                }
                QuantizationType::Product => {
                    keys.insert(pq_cluster_key(namespace, owner, cluster_idx));
                }
                QuantizationType::None => {}
            }
        }

        match segment.quantization {
            QuantizationType::Scalar => {
                keys.insert(sq_calibration_key(namespace, &segment.id));
            }
            QuantizationType::Product => {
                keys.insert(pq_codebook_key(namespace, &segment.id));
            }
            QuantizationType::None => {}
        }

        if segment.has_global_fts {
            keys.insert(global_fts_key(namespace, &segment.id));
        }
    }

    keys.extend(manifest.pending_deletes.iter().cloned());
    keys.extend(staging.iter().cloned());
    keys
}

/// A manifest-derived orphan candidate for the future two-pass GC sweep.
///
/// This is not an authoritative refcount. It only records that `key` left the
/// exact reachable union at a manifest CAS time; delete decisions must still
/// revalidate against `reachable_keys()` from a fresh manifest snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcCandidate {
    /// Exact S3 key that left the manifest-derived reachable union.
    pub key: String,
    /// Manifest commit time at which the key was first observed unreachable.
    pub first_seen_unreachable_at: DateTime<Utc>,
    /// Manifest generation at which the key was first observed unreachable.
    ///
    /// Legacy candidate records decode as `0`, treating them as logically
    /// very old while still requiring the wall-clock horizon before delete.
    #[serde(default)]
    pub unreachable_since_manifest_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GcCandidateStore {
    version: u32,
    candidates: Vec<GcCandidate>,
}

/// Summary of one mark/sweep GC cycle.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcCycleReport {
    /// Number of newly-unreachable objects added to the candidate ledger.
    pub candidates_marked: usize,
    /// Number of objects deleted from storage.
    pub objects_deleted: usize,
    /// Known bytes reclaimed from manifest-recorded artifact sizes.
    pub bytes_reclaimed: u64,
    /// Number of candidates skipped instead of deleted.
    pub candidates_skipped: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkipReason {
    UnknownShape,
    NotPersistedLongEnough,
    ReachableNow,
    UlidTooYoung,
    NewerThanInflightCompaction,
    NotEnoughManifestGenerations,
    NotListedThisCycle,
    DeleteFailed,
}

impl SkipReason {
    fn label(self) -> &'static str {
        match self {
            Self::UnknownShape => "unknown_shape",
            Self::NotPersistedLongEnough => "unreachable_horizon",
            Self::ReachableNow => "reachable_now",
            Self::UlidTooYoung => "ulid_age_floor",
            Self::NewerThanInflightCompaction => "inflight_watermark",
            Self::NotEnoughManifestGenerations => "manifest_generation_guard",
            Self::NotListedThisCycle => "not_listed",
            Self::DeleteFailed => "delete_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteDecision {
    Delete,
    Skip(SkipReason),
}

#[derive(Debug, Clone, Copy)]
struct DeletePredicateContext {
    horizon_secs: u64,
    now: DateTime<Utc>,
    oldest_inflight_ulid_ms: Option<u64>,
    current_manifest_version: u64,
    min_newer_manifest_versions: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedGcArtifact {
    WalFragment { ulid: Ulid },
    SegmentArtifact { ulid: Ulid },
}

impl ParsedGcArtifact {
    fn ulid(self) -> Ulid {
        match self {
            Self::WalFragment { ulid } | Self::SegmentArtifact { ulid } => ulid,
        }
    }
}

/// S3 key for the persisted per-namespace GC candidate ledger.
#[must_use]
pub fn gc_candidate_store_key(namespace: &str) -> String {
    format!("{namespace}/_gc/candidates.json")
}

/// Load the persisted per-namespace GC candidate ledger.
pub async fn load_gc_candidates(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<Vec<GcCandidate>> {
    match store.get(&gc_candidate_store_key(namespace)).await {
        Ok(data) => decode_gc_candidates(&data),
        Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

/// Persist the per-namespace GC candidate ledger as JSON.
pub async fn save_gc_candidates(
    store: &ZeppelinStore,
    namespace: &str,
    candidates: &[GcCandidate],
) -> Result<()> {
    let store_doc = GcCandidateStore {
        version: GC_CANDIDATE_STORE_VERSION,
        candidates: candidates.to_vec(),
    };
    store
        .put(
            &gc_candidate_store_key(namespace),
            Bytes::from(serde_json::to_vec_pretty(&store_doc)?),
        )
        .await
}

fn decode_gc_candidates(data: &[u8]) -> Result<Vec<GcCandidate>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    match serde_json::from_slice::<GcCandidateStore>(data) {
        Ok(store) => Ok(store.candidates),
        Err(wrapper_error) => match serde_json::from_slice::<Vec<GcCandidate>>(data) {
            Ok(candidates) => Ok(candidates),
            Err(_) => Err(wrapper_error.into()),
        },
    }
}

/// Pure mark pass: add newly unreachable known artifacts and drop resurrected candidates.
#[must_use]
pub fn mark_gc_candidates(
    namespace: &str,
    listed_keys: &BTreeSet<String>,
    reachable: &BTreeSet<String>,
    existing: &[GcCandidate],
    now: DateTime<Utc>,
    manifest_version: u64,
) -> Vec<GcCandidate> {
    let mut by_key = BTreeMap::new();
    for candidate in existing {
        if reachable.contains(&candidate.key) {
            continue;
        }
        if parse_gc_artifact_key(namespace, &candidate.key).is_some() {
            by_key.insert(
                candidate.key.clone(),
                (
                    candidate.first_seen_unreachable_at,
                    candidate.unreachable_since_manifest_version,
                ),
            );
        }
    }

    for key in listed_keys {
        if reachable.contains(key) {
            continue;
        }
        if parse_gc_artifact_key(namespace, key).is_none() {
            continue;
        }
        by_key.entry(key.clone()).or_insert((now, manifest_version));
    }

    by_key
        .into_iter()
        .map(
            |(key, (first_seen_unreachable_at, unreachable_since_manifest_version))| GcCandidate {
                key,
                first_seen_unreachable_at,
                unreachable_since_manifest_version,
            },
        )
        .collect()
}

/// Run one complete two-pass mark/sweep GC cycle for a namespace.
///
/// The cycle lists namespace objects, persists newly-unreachable candidates,
/// then deletes only candidates that have remained unreachable for
/// `gc.horizon_secs` and still fail the fresh reachability check.
pub async fn run_gc_cycle(
    store: &ZeppelinStore,
    namespace: &str,
    gc: &GcConfig,
) -> Result<GcCycleReport> {
    let now = Utc::now();
    let prefix = format!("{namespace}/");
    let listed_keys = match store.list_prefix(&prefix).await {
        Ok(keys) => keys.into_iter().collect::<BTreeSet<_>>(),
        Err(e) => {
            warn!(namespace, error = %e, "gc listing failed; aborting cycle");
            return Ok(GcCycleReport::default());
        }
    };

    let persisted = match load_gc_candidates(store, namespace).await {
        Ok(candidates) => candidates,
        Err(e) => {
            warn!(namespace, error = %e, "gc candidate load failed; aborting cycle");
            return Ok(GcCycleReport::default());
        }
    };

    let mark_manifest = match read_manifest_for_gc(store, namespace).await {
        Ok(Some(manifest)) => manifest,
        Ok(None) => {
            warn!(namespace, "gc manifest missing; skipping namespace");
            return Ok(GcCycleReport::default());
        }
        Err(e) => {
            warn!(namespace, error = %e, "gc manifest read failed; aborting cycle");
            return Ok(GcCycleReport::default());
        }
    };
    let mark_staging = match active_staged_keys(store, namespace).await {
        Ok(staging) => staging,
        Err(e) => {
            warn!(namespace, error = %e, "gc active staging read failed; aborting cycle");
            return Ok(GcCycleReport::default());
        }
    };
    let mark_reachable = reachable_keys_with_staging(namespace, &mark_manifest, &mark_staging);
    let unknown_shape_skips = listed_keys
        .iter()
        .filter(|key| !mark_reachable.contains(*key))
        .filter(|key| parse_gc_artifact_key(namespace, key).is_none())
        .inspect(|key| log_gc_skip(namespace, key, SkipReason::UnknownShape))
        .count();
    let marked_candidates = mark_gc_candidates(
        namespace,
        &listed_keys,
        &mark_reachable,
        &persisted,
        now,
        mark_manifest.version(),
    );
    let candidates_marked = marked_candidates.len().saturating_sub(
        persisted
            .iter()
            .filter(|candidate| {
                marked_candidates
                    .iter()
                    .any(|next| next.key == candidate.key)
            })
            .count(),
    );

    if let Err(e) = save_gc_candidates(store, namespace, &marked_candidates).await {
        warn!(namespace, error = %e, "gc candidate mark persist failed; skipping sweep");
        return Ok(GcCycleReport {
            candidates_marked: 0,
            objects_deleted: 0,
            bytes_reclaimed: 0,
            candidates_skipped: marked_candidates.len(),
        });
    }
    crate::metrics::GC_CANDIDATES_MARKED_TOTAL
        .with_label_values(&[namespace])
        .inc_by(candidates_marked as u64);

    let sweep_manifest = match read_manifest_for_gc(store, namespace).await {
        Ok(Some(manifest)) => manifest,
        Ok(None) => {
            warn!(
                namespace,
                "gc manifest missing before sweep; skipping deletes"
            );
            return Ok(GcCycleReport {
                candidates_marked,
                candidates_skipped: unknown_shape_skips,
                ..GcCycleReport::default()
            });
        }
        Err(e) => {
            warn!(namespace, error = %e, "gc manifest re-read failed; skipping deletes");
            return Ok(GcCycleReport {
                candidates_marked,
                candidates_skipped: unknown_shape_skips,
                ..GcCycleReport::default()
            });
        }
    };
    let sweep_staging = match active_staged_keys(store, namespace).await {
        Ok(staging) => staging,
        Err(e) => {
            warn!(namespace, error = %e, "gc active staging re-read failed; skipping sweep");
            return Ok(GcCycleReport {
                candidates_marked,
                candidates_skipped: unknown_shape_skips,
                ..GcCycleReport::default()
            });
        }
    };
    let sweep_reachable = reachable_keys_with_staging(namespace, &sweep_manifest, &sweep_staging);
    let oldest_inflight_ms = oldest_inflight_ulid_ms(namespace, &sweep_staging);
    let known_sizes = known_reclaimable_sizes(namespace, &sweep_manifest);

    let mut retained = Vec::new();
    let mut objects_deleted = 0usize;
    let mut bytes_reclaimed = 0u64;
    let mut candidates_skipped = unknown_shape_skips;

    for candidate in marked_candidates {
        if !listed_keys.contains(&candidate.key) {
            log_gc_skip(namespace, &candidate.key, SkipReason::NotListedThisCycle);
            candidates_skipped += 1;
            continue;
        }
        match should_delete_candidate(
            namespace,
            &candidate,
            &sweep_reachable,
            DeletePredicateContext {
                horizon_secs: gc.horizon_secs,
                now,
                oldest_inflight_ulid_ms: oldest_inflight_ms,
                current_manifest_version: sweep_manifest.version(),
                min_newer_manifest_versions: None,
            },
        ) {
            DeleteDecision::Delete => match store.delete(&candidate.key).await {
                Ok(()) | Err(crate::error::ZeppelinError::NotFound { .. }) => {
                    let reclaimed = known_sizes.get(&candidate.key).copied().unwrap_or(0);
                    objects_deleted += 1;
                    bytes_reclaimed += reclaimed;
                    crate::metrics::GC_OBJECTS_DELETED_TOTAL
                        .with_label_values(&[namespace])
                        .inc();
                    crate::metrics::GC_BYTES_RECLAIMED_TOTAL
                        .with_label_values(&[namespace])
                        .inc_by(reclaimed);
                    info!(
                        namespace,
                        key = %candidate.key,
                        reclaimed_bytes = reclaimed,
                        "gc deleted unreachable object"
                    );
                }
                Err(e) => {
                    log_gc_skip(namespace, &candidate.key, SkipReason::DeleteFailed);
                    warn!(
                        namespace,
                        key = %candidate.key,
                        error = %e,
                        "gc delete failed; retaining candidate"
                    );
                    candidates_skipped += 1;
                    retained.push(candidate);
                }
            },
            DeleteDecision::Skip(reason) => {
                log_gc_skip(namespace, &candidate.key, reason);
                candidates_skipped += 1;
                retained.push(candidate);
            }
        }
    }

    if let Err(e) = save_gc_candidates(store, namespace, &retained).await {
        warn!(
            namespace,
            error = %e,
            "gc candidate cleanup persist failed after sweep"
        );
    }

    info!(
        namespace,
        candidates_marked,
        objects_deleted,
        bytes_reclaimed,
        candidates_skipped,
        "gc cycle complete"
    );

    Ok(GcCycleReport {
        candidates_marked,
        objects_deleted,
        bytes_reclaimed,
        candidates_skipped,
    })
}

async fn read_manifest_for_gc(store: &ZeppelinStore, namespace: &str) -> Result<Option<Manifest>> {
    Manifest::read(store, namespace).await
}

fn log_gc_skip(namespace: &str, key: &str, reason: SkipReason) {
    crate::metrics::GC_CANDIDATES_SKIPPED_TOTAL
        .with_label_values(&[namespace, reason.label()])
        .inc();
    warn!(
        namespace,
        key,
        reason = reason.label(),
        "gc skipped candidate"
    );
}

fn should_delete_candidate(
    namespace: &str,
    candidate: &GcCandidate,
    reachable: &BTreeSet<String>,
    context: DeletePredicateContext,
) -> DeleteDecision {
    let Some(parsed) = parse_gc_artifact_key(namespace, &candidate.key) else {
        return DeleteDecision::Skip(SkipReason::UnknownShape);
    };
    if reachable.contains(&candidate.key) {
        return DeleteDecision::Skip(SkipReason::ReachableNow);
    }
    let unreachable_for = context
        .now
        .signed_duration_since(candidate.first_seen_unreachable_at)
        .num_seconds();
    if unreachable_for < i64::try_from(context.horizon_secs).unwrap_or(i64::MAX) {
        return DeleteDecision::Skip(SkipReason::NotPersistedLongEnough);
    }
    // The manifest generation is an additional retention guard only. It makes
    // "unreachable since version V" auditable, but cannot replace the
    // wall-clock horizon above because stale cached readers do not announce
    // their observed manifest epochs.
    if let Some(min_newer_manifest_versions) = context.min_newer_manifest_versions {
        let newer_versions = context
            .current_manifest_version
            .saturating_sub(candidate.unreachable_since_manifest_version);
        if newer_versions < min_newer_manifest_versions {
            return DeleteDecision::Skip(SkipReason::NotEnoughManifestGenerations);
        }
    }

    let now_ms = u64::try_from(context.now.timestamp_millis()).unwrap_or(0);
    let artifact_ulid = parsed.ulid();
    let ulid_age_secs = super::fragment_age_secs(&artifact_ulid, now_ms);
    if ulid_age_secs < context.horizon_secs {
        return DeleteDecision::Skip(SkipReason::UlidTooYoung);
    }
    if let Some(oldest_ms) = context.oldest_inflight_ulid_ms {
        if artifact_ulid.timestamp_ms() > oldest_ms {
            return DeleteDecision::Skip(SkipReason::NewerThanInflightCompaction);
        }
    }

    DeleteDecision::Delete
}

fn oldest_inflight_ulid_ms(namespace: &str, staged: &BTreeSet<String>) -> Option<u64> {
    staged
        .iter()
        .filter_map(|key| parse_gc_artifact_key(namespace, key))
        .map(|artifact| artifact.ulid().timestamp_ms())
        .min()
}

fn known_reclaimable_sizes(namespace: &str, manifest: &Manifest) -> BTreeMap<String, u64> {
    let mut sizes = BTreeMap::new();
    for fragment in &manifest.fragments {
        sizes.insert(
            WalFragment::s3_key(namespace, &fragment.id),
            fragment.size_bytes,
        );
    }
    for segment in &manifest.segments {
        if let Some(sketch) = &segment.sketch {
            sizes.insert(sketch.key.clone(), sketch.size_bytes);
        }
    }
    sizes
}

fn parse_gc_artifact_key(namespace: &str, key: &str) -> Option<ParsedGcArtifact> {
    let wal_prefix = format!("{namespace}/wal/");
    if let Some(name) = key.strip_prefix(&wal_prefix) {
        let id = name.strip_suffix(".wal")?;
        if id.contains('/') {
            return None;
        }
        return Ulid::from_string(id)
            .ok()
            .map(|ulid| ParsedGcArtifact::WalFragment { ulid });
    }

    let segment_prefix = format!("{namespace}/segments/");
    let rest = key.strip_prefix(&segment_prefix)?;
    let (segment_id, file_name) = rest.split_once('/')?;
    if file_name.contains('/') {
        return None;
    }
    let ulid_text = segment_id.strip_prefix("seg_")?;
    let ulid = Ulid::from_string(ulid_text).ok()?;
    if is_known_segment_artifact_name(file_name) {
        Some(ParsedGcArtifact::SegmentArtifact { ulid })
    } else {
        None
    }
}

fn is_known_segment_artifact_name(file_name: &str) -> bool {
    matches!(
        file_name,
        "centroids.bin"
            | "tree_meta.json"
            | "coarse_sketch.bin"
            | "bootstrap.bin"
            | "membership.bin"
            | "pq_codebook.bin"
            | "sq_calibration.bin"
            | "global_fts.bin"
    ) || numbered_bin(file_name, "cluster_")
        || numbered_bin(file_name, "cluster_group_")
        || numbered_bin(file_name, "attrs_")
        || numbered_bin(file_name, "bitmap_")
        || numbered_bin(file_name, "fts_index_")
        || numbered_bin(file_name, "sq_cluster_")
        || numbered_bin(file_name, "pq_cluster_")
}

fn numbered_bin(file_name: &str, prefix: &str) -> bool {
    let Some(number) = file_name
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_suffix(".bin"))
    else {
        return false;
    };
    !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
}

/// Build GC candidates from the exact key delta across a manifest CAS.
///
/// The returned keys are `reachable(old_manifest) - reachable(new_manifest)`.
/// This preserves carried-object correctness by construction: any object still
/// referenced by at least one live segment remains in the reachable union and
/// is not emitted. Crash-orphans that never entered a manifest are invisible to
/// this delta and remain the periodic LIST sweep's responsibility.
#[must_use]
pub fn gc_candidates_from_manifest_delta(
    namespace: &str,
    old_manifest: &Manifest,
    new_manifest: &Manifest,
    commit_time: DateTime<Utc>,
) -> Vec<GcCandidate> {
    let old_reachable = reachable_keys(namespace, old_manifest);
    let new_reachable = reachable_keys(namespace, new_manifest);

    old_reachable
        .difference(&new_reachable)
        .map(|key| GcCandidate {
            key: key.clone(),
            first_seen_unreachable_at: commit_time,
            unreachable_since_manifest_version: new_manifest.version(),
        })
        .collect()
}

/// Keep only candidates that are still unreachable in `manifest`.
///
/// Candidate state is an accelerator, not truth. The future delete stage must
/// call this shape of check after reading the authoritative manifest so a key
/// that became live again is skipped.
#[must_use]
pub fn revalidate_unreachable_candidates(
    namespace: &str,
    manifest: &Manifest,
    candidates: &[GcCandidate],
) -> Vec<GcCandidate> {
    let reachable = reachable_keys(namespace, manifest);
    candidates
        .iter()
        .filter(|candidate| !reachable.contains(&candidate.key))
        .cloned()
        .collect()
}

/// Write the exact staged-key set for the current compaction lease.
pub async fn write_compaction_staging(
    store: &ZeppelinStore,
    namespace: &str,
    fencing_token: u64,
    keys: BTreeSet<String>,
) -> Result<()> {
    let staging = CompactionStaging {
        fencing_token,
        keys,
    };
    let data = Bytes::from(serde_json::to_vec_pretty(&staging)?);
    store
        .put(&staging_key(namespace, fencing_token), data)
        .await
}

/// Clear the staged-key side object for a compaction lease.
pub async fn clear_compaction_staging(
    store: &ZeppelinStore,
    namespace: &str,
    fencing_token: u64,
) -> Result<()> {
    match store.delete(&staging_key(namespace, fencing_token)).await {
        Ok(()) => Ok(()),
        Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Return staged keys for the currently active lease, excluding stale or
/// expired lease records so a dead compactor cannot pin uploads forever.
pub async fn active_staged_keys(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<BTreeSet<String>> {
    let lease_data = match store.get(&format!("{namespace}/lease.json")).await {
        Ok(data) => data,
        Err(crate::error::ZeppelinError::NotFound { .. }) => return Ok(BTreeSet::new()),
        Err(e) => return Err(e),
    };
    let lease: Lease = serde_json::from_slice(&lease_data)?;
    if lease.expires_at <= Utc::now() {
        return Ok(BTreeSet::new());
    }

    let mut staged = BTreeSet::new();
    let prefix = format!("{namespace}/_staging/");
    for key in store.list_prefix(&prefix).await? {
        let data = store.get(&key).await?;
        let entry: CompactionStaging = serde_json::from_slice(&data)?;
        if entry.fencing_token == lease.fencing_token {
            staged.extend(entry.keys);
        }
    }
    Ok(staged)
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Utc;
    use object_store::memory::InMemory;
    use std::sync::Arc;
    use ulid::Ulid;

    use crate::fts::global_index::global_fts_key;
    use crate::fts::inverted_index::fts_index_key;
    use crate::index::bitmap::bitmap_key;
    use crate::index::hierarchical::tree_meta_key;
    use crate::index::ivf_flat::build::{attrs_key, bootstrap_key, centroids_key, cluster_key};
    use crate::index::ivf_flat::membership::membership_key;
    use crate::index::ivf_flat::sketch::sketch_key;
    use crate::index::quantization::pq::{pq_cluster_key, pq_codebook_key};
    use crate::index::quantization::sq::{sq_calibration_key, sq_cluster_key};
    use crate::index::quantization::QuantizationType;
    use crate::wal::fragment::WalFragment;
    use crate::wal::manifest::{
        BootstrapRef, ClusterDataObjectRef, FragmentRef, Manifest, MembershipRef, SegmentRef,
        SketchRef,
    };

    const NS: &str = "gc_ns";

    fn fragment_ref(id: Ulid) -> FragmentRef {
        FragmentRef {
            id,
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 100,
        }
    }

    fn segment_ref(id: &str, cluster_count: usize) -> SegmentRef {
        SegmentRef {
            id: id.to_string(),
            vector_count: 10,
            cluster_count,
            quantization: QuantizationType::None,
            hierarchical: false,
            bitmap_fields: Vec::new(),
            fts_fields: Vec::new(),
            has_global_fts: false,
            cluster_owners: Vec::new(),
            sketch: None,
            cluster_objects: Vec::new(),
            bootstrap: None,
            membership: None,
        }
    }

    fn candidate_keys(candidates: &[GcCandidate]) -> BTreeSet<String> {
        candidates
            .iter()
            .map(|candidate| candidate.key.clone())
            .collect()
    }

    struct ReachabilityCase {
        name: &'static str,
        manifest: Manifest,
        present: Vec<String>,
        absent: Vec<String>,
    }

    #[test]
    fn reachable_keys_are_the_exact_manifest_references() {
        let frag_a = Ulid::from_parts(1, 10);
        let frag_b = Ulid::from_parts(2, 20);

        let mut carried_manifest = Manifest::new();
        let mut carried = segment_ref("seg_new", 3);
        carried.cluster_owners = vec![
            "seg_old".to_string(),
            "seg_new".to_string(),
            "seg_older".to_string(),
        ];
        carried_manifest.segments.push(carried);

        let mut legacy_manifest = Manifest::new();
        legacy_manifest.segments.push(segment_ref("seg_legacy", 2));

        let mut pending_manifest = Manifest::new();
        pending_manifest.pending_deletes = vec![
            "gc_ns/wal/pruned-but-undeleted.wal".to_string(),
            "gc_ns/segments/seg_pruned/cluster_0.bin".to_string(),
        ];

        let mut fragments_manifest = Manifest::new();
        fragments_manifest.fragments = vec![fragment_ref(frag_a), fragment_ref(frag_b)];

        let mut metadata_manifest = Manifest::new();
        let mut metadata = segment_ref("seg_meta", 2);
        metadata.quantization = QuantizationType::Product;
        metadata.bitmap_fields = vec!["color".to_string()];
        metadata.fts_fields = vec!["body".to_string()];
        metadata.has_global_fts = true;
        metadata.sketch = Some(SketchRef {
            key: sketch_key(NS, "seg_meta"),
            version: 3,
            code_dims: 8,
            bytes_per_vector: 8,
            size_bytes: 512,
        });
        metadata.bootstrap = Some(BootstrapRef {
            key: bootstrap_key(NS, "seg_meta"),
            size_bytes: 1024,
        });
        metadata.membership = Some(MembershipRef {
            key: membership_key(NS, "seg_meta"),
            size_bytes: 256,
            entry_count: 10,
        });
        metadata.cluster_objects = vec![ClusterDataObjectRef {
            key: format!("{NS}/segments/seg_meta/cluster_group_0.bin"),
            clusters: vec![0, 1],
            live_offset: 0,
            live_len: 0,
            size_bytes: 2048,
        }];
        metadata_manifest.segments.push(metadata);

        let mut no_global_fts_manifest = Manifest::new();
        let mut no_global_fts = segment_ref("seg_no_global_fts", 1);
        no_global_fts.fts_fields = vec!["body".to_string()];
        no_global_fts.has_global_fts = false;
        no_global_fts_manifest.segments.push(no_global_fts);

        let mut hierarchical_manifest = Manifest::new();
        let mut hierarchical = segment_ref("seg_tree", 1);
        hierarchical.hierarchical = true;
        hierarchical.quantization = QuantizationType::Scalar;
        hierarchical_manifest.segments.push(hierarchical);

        let mut multi_manifest = Manifest::new();
        let mut first = segment_ref("seg_first", 2);
        first.cluster_owners = vec!["seg_shared".to_string(), "seg_first".to_string()];
        let mut second = segment_ref("seg_second", 2);
        second.cluster_owners = vec!["seg_second".to_string(), "seg_shared".to_string()];
        multi_manifest.segments = vec![first, second];

        let cases = vec![
            ReachabilityCase {
                name: "carried cluster owners",
                manifest: carried_manifest,
                present: vec![
                    cluster_key(NS, "seg_old", 0),
                    cluster_key(NS, "seg_new", 1),
                    cluster_key(NS, "seg_older", 2),
                    attrs_key(NS, "seg_old", 0),
                    attrs_key(NS, "seg_older", 2),
                ],
                absent: vec![cluster_key(NS, "seg_new", 0)],
            },
            ReachabilityCase {
                name: "legacy self-owned layout",
                manifest: legacy_manifest,
                present: vec![
                    cluster_key(NS, "seg_legacy", 0),
                    cluster_key(NS, "seg_legacy", 1),
                    attrs_key(NS, "seg_legacy", 0),
                    attrs_key(NS, "seg_legacy", 1),
                    centroids_key(NS, "seg_legacy"),
                ],
                absent: vec![cluster_key(NS, "some_other_segment", 0)],
            },
            ReachabilityCase {
                name: "pending deletes are still reachable",
                manifest: pending_manifest,
                present: vec![
                    "gc_ns/wal/pruned-but-undeleted.wal".to_string(),
                    "gc_ns/segments/seg_pruned/cluster_0.bin".to_string(),
                ],
                absent: Vec::new(),
            },
            ReachabilityCase {
                name: "fragments",
                manifest: fragments_manifest,
                present: vec![
                    WalFragment::s3_key(NS, &frag_a),
                    WalFragment::s3_key(NS, &frag_b),
                ],
                absent: Vec::new(),
            },
            ReachabilityCase {
                name: "per-segment metadata",
                manifest: metadata_manifest,
                present: vec![
                    centroids_key(NS, "seg_meta"),
                    sketch_key(NS, "seg_meta"),
                    bootstrap_key(NS, "seg_meta"),
                    membership_key(NS, "seg_meta"),
                    format!("{NS}/segments/seg_meta/cluster_group_0.bin"),
                    attrs_key(NS, "seg_meta", 0),
                    attrs_key(NS, "seg_meta", 1),
                    bitmap_key(NS, "seg_meta", 0),
                    bitmap_key(NS, "seg_meta", 1),
                    fts_index_key(NS, "seg_meta", 0),
                    fts_index_key(NS, "seg_meta", 1),
                    pq_codebook_key(NS, "seg_meta"),
                    pq_cluster_key(NS, "seg_meta", 0),
                    pq_cluster_key(NS, "seg_meta", 1),
                    global_fts_key(NS, "seg_meta"),
                ],
                absent: vec![
                    cluster_key(NS, "seg_meta", 0),
                    sq_calibration_key(NS, "seg_meta"),
                    global_fts_key(NS, "seg_no_global_fts"),
                ],
            },
            ReachabilityCase {
                name: "global fts omitted when manifest says absent",
                manifest: no_global_fts_manifest,
                present: vec![fts_index_key(NS, "seg_no_global_fts", 0)],
                absent: vec![global_fts_key(NS, "seg_no_global_fts")],
            },
            ReachabilityCase {
                name: "hierarchical metadata",
                manifest: hierarchical_manifest,
                present: vec![
                    tree_meta_key(NS, "seg_tree"),
                    attrs_key(NS, "seg_tree", 0),
                    sq_calibration_key(NS, "seg_tree"),
                    sq_cluster_key(NS, "seg_tree", 0),
                ],
                absent: vec![centroids_key(NS, "seg_tree")],
            },
            ReachabilityCase {
                name: "multiple live segments share one object",
                manifest: multi_manifest,
                present: vec![
                    cluster_key(NS, "seg_shared", 0),
                    cluster_key(NS, "seg_shared", 1),
                    cluster_key(NS, "seg_first", 1),
                    cluster_key(NS, "seg_second", 0),
                ],
                absent: Vec::new(),
            },
        ];

        for case in cases {
            let reachable = reachable_keys(NS, &case.manifest);
            for key in case.present {
                assert!(
                    reachable.contains(&key),
                    "{}: expected reachable key {key}",
                    case.name
                );
            }
            for key in case.absent {
                assert!(
                    !reachable.contains(&key),
                    "{}: key must not be marked reachable: {key}",
                    case.name
                );
            }
        }
    }

    #[test]
    fn reachable_keys_are_deduplicated() {
        let mut manifest = Manifest::new();
        let mut first = segment_ref("seg_first", 1);
        first.cluster_owners = vec!["seg_shared".to_string()];
        let mut second = segment_ref("seg_second", 1);
        second.cluster_owners = vec!["seg_shared".to_string()];
        manifest.segments = vec![first, second];

        let reachable = reachable_keys(NS, &manifest);
        let shared_key = cluster_key(NS, "seg_shared", 0);
        assert_eq!(
            reachable.iter().filter(|key| *key == &shared_key).count(),
            1,
            "shared carried object must appear once"
        );
    }

    #[test]
    fn cas_delta_candidates_are_exact_keys_that_left_reachability() {
        let commit_time = Utc::now();
        let compacted_fragment = Ulid::from_parts(10, 1);
        let new_fragment = Ulid::from_parts(11, 1);

        let mut old_manifest = Manifest::new();
        old_manifest.fragments = vec![fragment_ref(compacted_fragment)];
        old_manifest.segments = vec![segment_ref("seg_old", 2)];

        let mut new_segment = segment_ref("seg_new", 2);
        new_segment.cluster_owners = vec!["seg_old".to_string(), "seg_new".to_string()];
        let mut new_manifest = Manifest::new();
        new_manifest.fragments = vec![fragment_ref(new_fragment)];
        new_manifest.segments = vec![new_segment];

        let candidates =
            gc_candidates_from_manifest_delta(NS, &old_manifest, &new_manifest, commit_time);

        assert_eq!(
            candidate_keys(&candidates),
            BTreeSet::from([
                WalFragment::s3_key(NS, &compacted_fragment),
                centroids_key(NS, "seg_old"),
                cluster_key(NS, "seg_old", 1),
                attrs_key(NS, "seg_old", 1),
            ]),
            "only keys whose live reference set dropped to zero are candidates"
        );
        assert_eq!(
            candidates.len(),
            4,
            "each key that left reachability must be emitted exactly once"
        );
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.first_seen_unreachable_at == commit_time),
            "every delta candidate must be stamped with the manifest commit time"
        );
        assert!(
            !candidate_keys(&candidates).contains(&WalFragment::s3_key(NS, &new_fragment)),
            "keys added by the new manifest are not orphan candidates"
        );
        assert!(
            !candidate_keys(&candidates).contains(&cluster_key(NS, "seg_old", 0)),
            "carried cluster object remains reachable through the new segment"
        );
    }

    #[test]
    fn shared_carried_object_is_candidate_only_after_last_reference_releases_it() {
        let commit_time = Utc::now();
        let shared_cluster_key = cluster_key(NS, "seg_shared", 0);
        let shared_attrs_key = attrs_key(NS, "seg_shared", 0);

        let mut old_manifest = Manifest::new();
        let mut first = segment_ref("seg_first", 1);
        first.cluster_owners = vec!["seg_shared".to_string()];
        let mut second = segment_ref("seg_second", 1);
        second.cluster_owners = vec!["seg_shared".to_string()];
        old_manifest.segments = vec![first, second.clone()];

        let mut one_reference_released = Manifest::new();
        one_reference_released.segments = vec![second.clone()];
        let first_release = gc_candidates_from_manifest_delta(
            NS,
            &old_manifest,
            &one_reference_released,
            commit_time,
        );
        let first_release_keys = candidate_keys(&first_release);
        assert!(
            !first_release_keys.contains(&shared_cluster_key),
            "shared cluster data must survive while any segment still references it"
        );
        assert!(
            !first_release_keys.contains(&shared_attrs_key),
            "shared sidecar data must survive while any segment still references it"
        );

        let no_references = Manifest::new();
        let second_release = gc_candidates_from_manifest_delta(
            NS,
            &one_reference_released,
            &no_references,
            commit_time,
        );
        assert!(
            candidate_keys(&second_release)
                .is_superset(&BTreeSet::from([shared_cluster_key, shared_attrs_key,])),
            "shared carried keys become candidates when the last reference is gone"
        );
    }

    #[test]
    fn crash_orphan_absent_from_both_manifests_is_not_a_delta_candidate() {
        let commit_time = Utc::now();
        let crash_orphan = format!("{NS}/segments/crash_orphan/cluster_0.bin");

        let mut old_manifest = Manifest::new();
        old_manifest.segments = vec![segment_ref("seg_live", 1)];
        let mut new_manifest = Manifest::new();
        new_manifest.segments = vec![segment_ref("seg_live", 1)];

        let candidates =
            gc_candidates_from_manifest_delta(NS, &old_manifest, &new_manifest, commit_time);

        assert!(
            !candidate_keys(&candidates).contains(&crash_orphan),
            "PUT-without-CAS orphans never entered a manifest and require the LIST backstop"
        );
    }

    #[test]
    fn candidate_revalidation_recomputes_reachability_from_manifest_before_deletion() {
        let commit_time = Utc::now();
        let live_key = cluster_key(NS, "seg_live", 0);
        let dead_key = format!("{NS}/wal/dead.wal");

        let mut manifest = Manifest::new();
        manifest.segments = vec![segment_ref("seg_live", 1)];

        let candidates = vec![
            GcCandidate {
                key: live_key,
                first_seen_unreachable_at: commit_time,
                unreachable_since_manifest_version: 3,
            },
            GcCandidate {
                key: dead_key.clone(),
                first_seen_unreachable_at: commit_time,
                unreachable_since_manifest_version: 3,
            },
        ];

        let still_unreachable = revalidate_unreachable_candidates(NS, &manifest, &candidates);

        assert_eq!(
            candidate_keys(&still_unreachable),
            BTreeSet::from([dead_key]),
            "delete decisions must re-read exact manifest reachability, not trust candidate state"
        );
    }

    #[test]
    fn reachable_keys_with_staging_unions_active_staged_keys() {
        let mut manifest = Manifest::new();
        manifest.segments.push(segment_ref("seg_live", 1));
        let staged: BTreeSet<String> = [
            format!("{NS}/segments/seg_staged/centroids.bin"),
            format!("{NS}/segments/seg_staged/cluster_group_0.bin"),
        ]
        .into_iter()
        .collect();

        let reachable = reachable_keys_with_staging(NS, &manifest, &staged);

        assert!(reachable.contains(&centroids_key(NS, "seg_live")));
        for key in staged {
            assert!(
                reachable.contains(&key),
                "active lease staged key must be treated as reachable: {key}"
            );
        }
    }

    fn ulid_seconds_ago(seconds: i64, entropy: u128) -> Ulid {
        let ts = (Utc::now() - chrono::Duration::seconds(seconds)).timestamp_millis() as u64;
        Ulid::from_parts(ts, entropy)
    }

    fn delete_context(horizon_secs: u64, now: DateTime<Utc>) -> DeletePredicateContext {
        DeletePredicateContext {
            horizon_secs,
            now,
            oldest_inflight_ulid_ms: None,
            current_manifest_version: 10,
            min_newer_manifest_versions: None,
        }
    }

    #[test]
    fn delete_predicate_table_is_fail_closed() {
        let now = Utc::now();
        let old_id = ulid_seconds_ago(30, 91);
        let old_key = WalFragment::s3_key(NS, &old_id);
        let candidate = GcCandidate {
            key: old_key.clone(),
            first_seen_unreachable_at: now - chrono::Duration::seconds(10),
            unreachable_since_manifest_version: 2,
        };

        let cases = [
            (
                "unknown key shape",
                GcCandidate {
                    key: format!("{NS}/segments/not-a-segment/centroids.bin"),
                    first_seen_unreachable_at: now - chrono::Duration::seconds(10),
                    unreachable_since_manifest_version: 2,
                },
                BTreeSet::new(),
                5,
                DeleteDecision::Skip(SkipReason::UnknownShape),
            ),
            (
                "unreachable for less than horizon",
                GcCandidate {
                    key: old_key.clone(),
                    first_seen_unreachable_at: now - chrono::Duration::seconds(2),
                    unreachable_since_manifest_version: 2,
                },
                BTreeSet::new(),
                5,
                DeleteDecision::Skip(SkipReason::NotPersistedLongEnough),
            ),
            (
                "reachable again now",
                candidate.clone(),
                BTreeSet::from([old_key.clone()]),
                5,
                DeleteDecision::Skip(SkipReason::ReachableNow),
            ),
            (
                "active staged key is reachable",
                candidate.clone(),
                BTreeSet::from([old_key.clone()]),
                5,
                DeleteDecision::Skip(SkipReason::ReachableNow),
            ),
            (
                "all conditions met",
                candidate,
                BTreeSet::new(),
                5,
                DeleteDecision::Delete,
            ),
        ];

        for (name, candidate, reachable, horizon, expected) in cases {
            let actual =
                should_delete_candidate(NS, &candidate, &reachable, delete_context(horizon, now));
            assert_eq!(actual, expected, "{name}");
        }
    }

    #[test]
    fn delete_predicate_honors_ulid_age_floor_and_inflight_watermark() {
        let now = Utc::now();
        let young_id = ulid_seconds_ago(1, 92);
        let young_candidate = GcCandidate {
            key: WalFragment::s3_key(NS, &young_id),
            first_seen_unreachable_at: now - chrono::Duration::seconds(10),
            unreachable_since_manifest_version: 2,
        };
        assert_eq!(
            should_delete_candidate(
                NS,
                &young_candidate,
                &BTreeSet::new(),
                delete_context(5, now),
            ),
            DeleteDecision::Skip(SkipReason::UlidTooYoung)
        );

        let newer_than_inflight = ulid_seconds_ago(10, 93);
        let oldest_inflight = ulid_seconds_ago(20, 94);
        let candidate = GcCandidate {
            key: WalFragment::s3_key(NS, &newer_than_inflight),
            first_seen_unreachable_at: now - chrono::Duration::seconds(30),
            unreachable_since_manifest_version: 2,
        };
        assert_eq!(
            should_delete_candidate(
                NS,
                &candidate,
                &BTreeSet::new(),
                DeletePredicateContext {
                    oldest_inflight_ulid_ms: Some(oldest_inflight.timestamp_ms()),
                    ..delete_context(5, now)
                },
            ),
            DeleteDecision::Skip(SkipReason::NewerThanInflightCompaction)
        );
    }

    #[test]
    fn delete_predicate_epoch_guard_is_additional_to_time_horizon() {
        let now = Utc::now();
        let old_id = ulid_seconds_ago(30, 190);
        let key = WalFragment::s3_key(NS, &old_id);

        let time_and_epoch_ok = GcCandidate {
            key: key.clone(),
            first_seen_unreachable_at: now - chrono::Duration::seconds(30),
            unreachable_since_manifest_version: 7,
        };
        assert_eq!(
            should_delete_candidate(
                NS,
                &time_and_epoch_ok,
                &BTreeSet::new(),
                DeletePredicateContext {
                    min_newer_manifest_versions: Some(3),
                    ..delete_context(5, now)
                },
            ),
            DeleteDecision::Delete
        );

        let time_not_ok = GcCandidate {
            key: key.clone(),
            first_seen_unreachable_at: now - chrono::Duration::seconds(2),
            unreachable_since_manifest_version: 7,
        };
        assert_eq!(
            should_delete_candidate(
                NS,
                &time_not_ok,
                &BTreeSet::new(),
                DeletePredicateContext {
                    min_newer_manifest_versions: Some(3),
                    ..delete_context(5, now)
                },
            ),
            DeleteDecision::Skip(SkipReason::NotPersistedLongEnough),
            "a satisfied generation guard must not bypass the wall-clock horizon"
        );

        let epoch_not_ok = GcCandidate {
            key,
            first_seen_unreachable_at: now - chrono::Duration::seconds(30),
            unreachable_since_manifest_version: 8,
        };
        assert_eq!(
            should_delete_candidate(
                NS,
                &epoch_not_ok,
                &BTreeSet::new(),
                DeletePredicateContext {
                    min_newer_manifest_versions: Some(3),
                    ..delete_context(5, now)
                },
            ),
            DeleteDecision::Skip(SkipReason::NotEnoughManifestGenerations)
        );
    }

    #[test]
    fn candidate_store_decodes_empty_versioned_and_legacy_json() {
        assert!(decode_gc_candidates(b"").unwrap().is_empty());

        let candidate = GcCandidate {
            key: WalFragment::s3_key(NS, &ulid_seconds_ago(30, 95)),
            first_seen_unreachable_at: Utc::now(),
            unreachable_since_manifest_version: 42,
        };
        let versioned = serde_json::to_vec(&GcCandidateStore {
            version: GC_CANDIDATE_STORE_VERSION,
            candidates: vec![candidate.clone()],
        })
        .unwrap();
        assert_eq!(
            decode_gc_candidates(&versioned).unwrap(),
            vec![candidate.clone()]
        );

        let legacy = serde_json::to_vec(&vec![candidate.clone()]).unwrap();
        assert_eq!(decode_gc_candidates(&legacy).unwrap(), vec![candidate]);
    }

    #[test]
    fn legacy_candidate_without_epoch_decodes_as_very_old_generation() {
        #[derive(Serialize)]
        struct LegacyCandidateStore<'a> {
            version: u32,
            candidates: Vec<LegacyCandidate<'a>>,
        }

        #[derive(Serialize)]
        struct LegacyCandidate<'a> {
            key: &'a str,
            first_seen_unreachable_at: DateTime<Utc>,
        }

        let key = WalFragment::s3_key(NS, &ulid_seconds_ago(30, 191));
        let first_seen_unreachable_at = Utc::now() - chrono::Duration::seconds(60);
        let legacy = serde_json::to_vec(&LegacyCandidateStore {
            version: GC_CANDIDATE_STORE_VERSION,
            candidates: vec![LegacyCandidate {
                key: &key,
                first_seen_unreachable_at,
            }],
        })
        .unwrap();

        assert_eq!(
            decode_gc_candidates(&legacy).unwrap(),
            vec![GcCandidate {
                key,
                first_seen_unreachable_at,
                unreachable_since_manifest_version: 0,
            }],
            "legacy candidates default to epoch 0, which is still time-gated"
        );
    }

    #[tokio::test]
    async fn candidate_store_round_trips_on_storage() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let candidate = GcCandidate {
            key: WalFragment::s3_key(NS, &ulid_seconds_ago(30, 96)),
            first_seen_unreachable_at: Utc::now(),
            unreachable_since_manifest_version: 42,
        };

        save_gc_candidates(&store, NS, std::slice::from_ref(&candidate))
            .await
            .unwrap();

        assert_eq!(
            load_gc_candidates(&store, NS).await.unwrap(),
            vec![candidate]
        );
    }

    #[test]
    fn mark_pass_drops_candidate_that_became_reachable_again() {
        let now = Utc::now();
        let resurrected = WalFragment::s3_key(NS, &ulid_seconds_ago(30, 97));
        let still_dead = WalFragment::s3_key(NS, &ulid_seconds_ago(30, 98));
        let existing = vec![
            GcCandidate {
                key: resurrected.clone(),
                first_seen_unreachable_at: now - chrono::Duration::seconds(10),
                unreachable_since_manifest_version: 5,
            },
            GcCandidate {
                key: still_dead.clone(),
                first_seen_unreachable_at: now - chrono::Duration::seconds(10),
                unreachable_since_manifest_version: 5,
            },
        ];
        let listed = BTreeSet::from([resurrected.clone(), still_dead.clone()]);
        let reachable = BTreeSet::from([resurrected]);

        let marked = mark_gc_candidates(NS, &listed, &reachable, &existing, now, 9);

        assert_eq!(candidate_keys(&marked), BTreeSet::from([still_dead]));
        assert_eq!(marked[0].unreachable_since_manifest_version, 5);
    }

    #[test]
    fn mark_pass_stamps_new_candidates_with_manifest_version() {
        let now = Utc::now();
        let newly_dead = WalFragment::s3_key(NS, &ulid_seconds_ago(30, 99));
        let listed = BTreeSet::from([newly_dead.clone()]);

        let marked = mark_gc_candidates(NS, &listed, &BTreeSet::new(), &[], now, 12);

        assert_eq!(
            marked,
            vec![GcCandidate {
                key: newly_dead,
                first_seen_unreachable_at: now,
                unreachable_since_manifest_version: 12,
            }]
        );
    }
}
