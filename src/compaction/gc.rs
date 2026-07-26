//! Manifest-aware garbage collection for immutable WAL and segment objects.
//!
//! Zeppelin never deletes an object merely because an S3/MinIO listing does
//! not match one in-memory view. The live
//! [`Manifest`][crate::wal::manifest::Manifest] is authoritative, every
//! retained manifest-history generation is an additional live root, and
//! uploads recorded by the active compaction lease are temporarily protected.
//! This module derives the exact union of those references, records known-shape
//! objects outside that union in a persisted
//! [`GcCandidate`][crate::compaction::gc::GcCandidate] ledger, and
//! revalidates the union before a later sweep physically deletes anything.
//!
//! [`run_gc_cycle`][crate::compaction::gc::run_gc_cycle] is called by the
//! background compaction loop before it considers new compaction work.
//! [`drain_pending_deletes`][crate::compaction::gc::drain_pending_deletes]
//! handles objects that an already-published manifest explicitly scheduled for
//! removal. The helper
//! [`reachable_keys`][crate::compaction::gc::reachable_keys] is also used by
//! namespace clone/restore and storage oracles that need the same artifact
//! vocabulary as GC.
//!
//! This file talks to object storage only through
//! [`ZeppelinStore`][crate::storage::ZeppelinStore]. It does not mutate query
//! caches or make a candidate ledger authoritative. A configured time horizon
//! protects readers that may still use a cached older manifest; the cache
//! itself remains disposable and is not consulted during deletion. There is no
//! dry-run mode: reports such as
//! [`GcCycleReport`][crate::compaction::gc::GcCycleReport] describe work already
//! attempted in the current cycle.
//!
//! ## Reading map
//!
//! 1. Start with [`reachable_keys`][crate::compaction::gc::reachable_keys] and
//!    [`reachable_keys_with_staging`][crate::compaction::gc::reachable_keys_with_staging]
//!    to see how a manifest becomes an exact object-key set.
//! 2. Read [`CompactionStaging`][crate::compaction::gc::CompactionStaging] and
//!    [`active_staged_keys`][crate::compaction::gc::active_staged_keys] for the
//!    upload-before-manifest safety bridge.
//! 3. Read [`GcCandidate`][crate::compaction::gc::GcCandidate] and
//!    [`mark_gc_candidates`][crate::compaction::gc::mark_gc_candidates] for the
//!    persisted mark pass.
//! 4. Read
//!    [`drain_pending_deletes`][crate::compaction::gc::drain_pending_deletes]
//!    for manifest-owned deferred deletion.
//! 5. Finish with [`run_gc_cycle`][crate::compaction::gc::run_gc_cycle] for
//!    history pruning, mark persistence, fresh revalidation, and the batched
//!    sweep.
//!
//! ## Authority and deletion flow
//!
//! ```text
//! live manifest on S3 -----------+
//! retained manifest history ----+---- exact reachable union
//! active-lease staged uploads ---+             |
//!                                              | subtract from namespace LIST
//!                                              v
//!                                  persist candidate ledger (mark)
//!                                              |
//!                                 wait for configured time horizon
//!                                              |
//!                           re-read manifest + history + active staging
//!                              | reachable/failure       | still unreachable
//!                              v                         v
//!                         keep candidate           DELETE immutable object
//! ```
//!
//! History pruning precedes artifact deletion. A generation protected by the
//! count window, PITR window, or a named snapshot therefore continues to pin
//! every object it references. Deletes use deterministic batches of at most
//! 1,000 keys and are not transactional across batches: a later failure can
//! follow earlier successful batches. Candidate and `pending_deletes` state
//! retain every member of an uncertain batch, and absence is an idempotent
//! completion.
//!
//! ## Invariants
//!
//! - The manifest-derived union, not LIST output or the candidate ledger,
//!   decides reachability.
//! - Retained history and active compaction staging are live roots.
//! - Only recognized immutable artifact key shapes may enter the LIST-derived
//!   candidate sweep; unfamiliar listed keys fail closed and remain in object
//!   storage. Explicit manifest `pending_deletes` use their separate drain
//!   contract.
//! - A candidate must survive the wall-clock horizon and a fresh reachability
//!   check. Its embedded ULID must also be old enough.
//! - An object shared by carried clusters is collectible only after its last
//!   manifest reference disappears.
//! - GC never edits immutable WAL or segment objects in place.
//!
//! ## Rust concepts used here
//!
//! [`BTreeSet`][std::collections::BTreeSet] represents mathematical key unions
//! and differences while
//! deduplicating shared artifacts and keeping reports deterministic. This is
//! closest to Java's `TreeSet`; in C it would require an explicitly owned set
//! implementation and cleanup discipline. Borrowed `&Manifest` and `&str`
//! parameters cannot be retained after a call, while returned sets and reports
//! are owned values. Async store calls borrow the shared
//! [`ZeppelinStore`][crate::storage::ZeppelinStore] gateway across `.await`; no
//! mutex guard or raw backend pointer is exposed. Rust enums make each
//! delete/skip outcome explicit and exhaustive instead of relying on integer
//! status codes as C code often would.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use ulid::Ulid;

use crate::config::GcConfig;
use crate::error::{Result, ZeppelinError};
use crate::fts::global_index::global_fts_key;
use crate::fts::inverted_index::fts_index_key;
use crate::index::bitmap::bitmap_key;
use crate::index::hierarchical::tree_meta_key;
use crate::index::ivf_flat::build::{attrs_key, centroids_key, cluster_key};
use crate::index::quantization::pq::{pq_cluster_key, pq_codebook_key};
use crate::index::quantization::sq::{sq_calibration_key, sq_cluster_key};
use crate::index::quantization::QuantizationType;
use crate::namespace::branching::{ArtifactOrigin, BranchError};
use crate::namespace::manager::{NamespaceIncarnationId, NamespaceMetadata};
use crate::namespace::{BranchId, BranchRoot, ManifestDigest, ManifestGeneration};
use crate::security::{NamespaceId, PreservationService};
use crate::storage::store::DELETE_MANY_MAX_KEYS;
use crate::storage::{ListedObject, NamespaceObjectKey, StorageVersion, ZeppelinStore};
use crate::wal::fragment::WalFragment;
use crate::wal::manifest::{
    Manifest, ManifestHistoryObservation, ManifestHistoryPruneResult, ManifestHistoryRetention,
    ManifestVersion, NamedSnapshot, NamedSnapshotObservation,
};
use crate::wal::Lease;

/// Persisted JSON wrapper version written for new candidate ledgers.
const GC_CANDIDATE_STORE_VERSION: u32 = 1;

/// Maximum fresh-read/CAS attempts made while pruning `pending_deletes`.
const GC_MANIFEST_CAS_RETRIES: usize = 10;

/// Maximum number of reads polled concurrently within one variable-size GC batch.
const GC_READ_BATCH_CONCURRENCY: usize = 32;

/// Object-store key proven to belong to the namespace whose GC cycle may delete it.
///
/// Reachability may legitimately contain foreign physical keys. Destructive GC
/// paths must cross this classifier before calling `delete_many`, so a borrowed
/// source artifact can never become a target-owned deletion candidate by
/// accident.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TargetOwnedDeletionKey {
    target: NamespaceId,
    key: String,
}

impl TargetOwnedDeletionKey {
    fn classify(namespace: &str, key: String) -> Result<Self> {
        let target = NamespaceId::new(namespace.to_string())?;
        NamespaceObjectKey::classify(target.as_str(), key.clone()).map_err(|error| {
            ZeppelinError::Serialization(format!(
                "GC target {} cannot delete unowned key {key}: {error}",
                target.as_str()
            ))
        })?;
        Ok(Self { target, key })
    }

    fn as_str(&self) -> &str {
        &self.key
    }
}

/// Exercise the same ownership classifier used by every destructive GC path.
#[cfg(feature = "branching-test-support")]
pub(crate) fn classify_target_owned_deletion_key_for_test_support(
    namespace: &str,
    key: String,
) -> Result<String> {
    TargetOwnedDeletionKey::classify(namespace, key).map(|owned| owned.key)
}

async fn delete_target_owned_many(
    store: &ZeppelinStore,
    keys: &[TargetOwnedDeletionKey],
) -> Result<usize> {
    debug_assert!(keys
        .first()
        .is_none_or(|first| keys.iter().all(|key| key.target == first.target)));
    store
        .delete_many(keys.iter().map(|key| key.key.clone()).collect())
        .await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GcReadMode {
    Sequential,
    WarmBounded,
}

impl GcReadMode {
    fn is_bounded(self) -> bool {
        self == Self::WarmBounded
    }
}

/// Process-local discriminator for one lifetime of a namespace name.
///
/// Namespace names can be deleted and recreated. A GC history memo therefore
/// belongs to the name and its authoritative per-create identity. Legacy
/// metadata without that identity falls back to its creation timestamp.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct GcNamespaceIncarnation {
    name: String,
    created_at: DateTime<Utc>,
    incarnation_id: Option<NamespaceIncarnationId>,
}

impl GcNamespaceIncarnation {
    /// Creates one process-local namespace incarnation identity.
    #[must_use]
    pub fn new(name: String, created_at: DateTime<Utc>) -> Self {
        assert!(!name.is_empty(), "GC namespace incarnation cannot be empty");
        Self {
            name,
            created_at,
            incarnation_id: None,
        }
    }

    /// Captures the lifecycle identity carried by an authoritative metadata GET.
    #[must_use]
    pub fn from_metadata(metadata: &NamespaceMetadata) -> Self {
        assert!(
            !metadata.name.is_empty(),
            "GC namespace incarnation cannot be empty"
        );
        Self {
            name: metadata.name.clone(),
            created_at: metadata.created_at,
            incarnation_id: metadata.incarnation_id.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_incarnation_id(
        name: String,
        created_at: DateTime<Utc>,
        incarnation_id: NamespaceIncarnationId,
    ) -> Self {
        Self {
            name,
            created_at,
            incarnation_id: Some(incarnation_id),
        }
    }

    /// Returns the namespace name used for object-store keys.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Stateful background GC entrypoint with a disposable validated history memo.
///
/// The runner owns no authority: every cycle freshly lists history metadata and
/// reuses a decoded body only when the exact key and nonempty S3 ETag match the
/// prior completed cycle. One-shot callers continue to use [`run_gc_cycle`] or
/// [`run_gc_cycle_at`] and therefore never retain process state.
pub struct GcRunner {
    store: ZeppelinStore,
    gc: GcConfig,
    preservation: Option<Arc<PreservationService>>,
    namespaces: BTreeMap<String, NamespaceGcMemo>,
}

#[derive(Debug, Clone)]
struct NamespaceGcMemo {
    incarnation: GcNamespaceIncarnation,
    history: BTreeMap<String, CachedHistory>,
    inventory: Option<InventoryFingerprint>,
    live_root_identity: LiveRootIdentity,
    next_due_at: Option<DateTime<Utc>>,
    last_now: DateTime<Utc>,
    last_cycle_complete: bool,
    candidate_phase_due: bool,
    config: GcConfigFingerprint,
}

/// One validated recursive observation of every object below a namespace.
///
/// This is disposable routing state, never storage authority by itself. Root
/// bodies still have to be read and paired with their LIST-observed identity
/// before they can authorize a physical delete.
#[derive(Debug, Clone)]
struct NamespaceInventory {
    objects: BTreeMap<String, ListedObject>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InventoryFingerprint(BTreeMap<String, InventoryObjectFingerprint>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct InventoryObjectFingerprint {
    size: u64,
    version: StorageVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GcConfigFingerprint {
    horizon_secs: u64,
    compaction_upload_window_secs: u64,
    skew_slop_secs: u64,
    allow_unsafe_short_horizon: bool,
    manifest_history_keep_count: usize,
    pitr_retention_secs: u64,
}

#[derive(Debug, Clone)]
struct CachedHistory {
    storage_version: StorageVersion,
    manifest: Manifest,
    stored_bytes: Bytes,
    reachable_keys: BTreeSet<String>,
}

#[derive(Debug)]
struct HistorySnapshot {
    entries: Vec<(ManifestHistoryObservation, Manifest, Bytes)>,
    cacheable: BTreeMap<String, CachedHistory>,
}

/// Exact authoritative live-manifest identity that supplied branch roots.
///
/// Root retention decisions are useful only while this complete observation
/// remains current. The ETag binds the stored bytes, while generation,
/// incarnation, and the root map make the safety-relevant state explicit for
/// review and memo invalidation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveRootIdentity {
    storage_etag: String,
    manifest_generation: u64,
    namespace_incarnation: Option<uuid::Uuid>,
    branch_roots: BTreeMap<BranchId, BranchRoot>,
}

/// One live-root observation plus the manifest used to validate rooted history.
///
/// The version is retained alongside the manifest so later phases of the same
/// cycle can reuse this authoritative read instead of issuing an identical GET.
/// [`LiveRootIdentity::matches_inventory`] decides when that reuse is sound.
#[derive(Debug, Clone)]
struct LiveRootObservation {
    identity: LiveRootIdentity,
    manifest: Manifest,
    version: Box<ManifestVersion>,
    rooted_generations: BTreeMap<ManifestGeneration, ManifestDigest>,
}

struct MemoizedHistoryPruneResult {
    result: ManifestHistoryPruneResult,
    retained_history_observations: Vec<ManifestHistoryObservation>,
    snapshot_observations: Vec<NamedSnapshotObservation>,
}

struct GcCycleOutcome {
    report: GcCycleReport,
    completed: Option<CompletedGcState>,
    candidate_phase_due: bool,
}

struct CompletedGcState {
    history: BTreeMap<String, CachedHistory>,
    inventory: Option<InventoryFingerprint>,
    live_root_identity: LiveRootIdentity,
    next_due_at: Option<DateTime<Utc>>,
}

impl GcCycleOutcome {
    fn incomplete(report: GcCycleReport) -> Self {
        Self {
            report,
            completed: None,
            candidate_phase_due: false,
        }
    }

    fn incomplete_with_candidate_phase(report: GcCycleReport) -> Self {
        Self {
            report,
            completed: None,
            candidate_phase_due: true,
        }
    }

    fn complete(report: GcCycleReport, completed: CompletedGcState) -> Self {
        Self {
            report,
            completed: Some(completed),
            candidate_phase_due: false,
        }
    }
}

impl LiveRootObservation {
    fn from_authority(
        namespace: &str,
        manifest: Manifest,
        version: ManifestVersion,
    ) -> Result<Self> {
        let storage_etag = version
            .require_etag(namespace, "GC live branch-root observation")?
            .to_string();
        let rooted_generations = manifest.rooted_generations()?;
        let identity = LiveRootIdentity {
            storage_etag,
            manifest_generation: manifest.version(),
            namespace_incarnation: manifest.namespace_incarnation(),
            branch_roots: manifest.branch_roots().clone(),
        };
        Ok(Self {
            identity,
            manifest,
            version: Box::new(version),
            rooted_generations,
        })
    }
}

impl LiveRootIdentity {
    fn matches_inventory(&self, namespace: &str, inventory: &NamespaceInventory) -> bool {
        inventory
            .object(&Manifest::s3_key(namespace))
            .and_then(|object| object.version.as_ref())
            .and_then(StorageVersion::etag)
            == Some(self.storage_etag.as_str())
    }
}

async fn load_live_root_observation(
    store: &ZeppelinStore,
    namespace: &str,
    inventory: Option<&NamespaceInventory>,
) -> Result<Option<LiveRootObservation>> {
    let observed = match inventory {
        Some(inventory) => {
            read_versioned_manifest_from_inventory(store, namespace, inventory).await?
        }
        None => Manifest::read_versioned(store, namespace).await?,
    };
    observed
        .map(|(manifest, version)| {
            LiveRootObservation::from_authority(namespace, manifest, version)
        })
        .transpose()
}

async fn revalidate_live_root_observation(
    store: &ZeppelinStore,
    namespace: &str,
    expected: &LiveRootObservation,
) -> Result<()> {
    let Some((manifest, version)) = Manifest::read_versioned(store, namespace).await? else {
        return Err(ZeppelinError::Serialization(format!(
            "namespace {namespace} live manifest disappeared during GC root observation"
        )));
    };
    let actual = LiveRootObservation::from_authority(namespace, manifest, version)?;
    if actual.identity != expected.identity {
        return Err(ZeppelinError::ManifestConflict {
            namespace: namespace.to_string(),
        });
    }
    Ok(())
}

impl InventoryFingerprint {
    fn from_listed<'a>(objects: impl IntoIterator<Item = &'a ListedObject>) -> Option<Self> {
        let mut entries = BTreeMap::new();
        for object in objects {
            let version = object.version.clone()?;
            if entries
                .insert(
                    object.key.clone(),
                    InventoryObjectFingerprint {
                        size: object.size,
                        version,
                    },
                )
                .is_some()
            {
                return None;
            }
        }
        Some(Self(entries))
    }

    fn matches_listed_prefix<'a>(
        &self,
        prefix: &str,
        listed: impl IntoIterator<Item = &'a ListedObject>,
    ) -> bool {
        let mut expected = BTreeMap::new();
        for object in listed {
            let Some(version) = object.version.clone() else {
                return false;
            };
            if !object.key.starts_with(prefix)
                || expected
                    .insert(
                        object.key.clone(),
                        InventoryObjectFingerprint {
                            size: object.size,
                            version,
                        },
                    )
                    .is_some()
            {
                return false;
            }
        }
        let actual = self
            .0
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        actual == expected
    }
}

fn malformed_control_key(
    family: &'static str,
    key: impl Into<String>,
    reason: impl Into<String>,
) -> ZeppelinError {
    ZeppelinError::MalformedControlKey {
        family,
        key: key.into(),
        reason: reason.into(),
    }
}

fn listed_object_identity_matches(before: &ListedObject, after: &ListedObject) -> bool {
    before.size == after.size
        && before
            .version
            .as_ref()
            .zip(after.version.as_ref())
            .is_some_and(|(before, after)| before == after)
}

fn listed_object_horizon_satisfied(
    object: &ListedObject,
    now: DateTime<Utc>,
    horizon_secs: u64,
) -> bool {
    if horizon_secs == 0 {
        return true;
    }
    let Ok(horizon_secs) = i64::try_from(horizon_secs) else {
        return false;
    };
    object
        .last_modified
        .checked_add_signed(chrono::Duration::seconds(horizon_secs))
        .is_some_and(|deadline| now >= deadline)
}

impl NamespaceInventory {
    fn from_listed(namespace: &str, listed: Vec<ListedObject>) -> Result<Self> {
        let history_prefix = Manifest::history_prefix(namespace);
        let snapshot_prefix = NamedSnapshot::prefix(namespace);
        let staging_prefix = format!("{namespace}/_staging/");
        let gc_prefix = format!("{namespace}/_gc/");
        let candidate_key = gc_candidate_store_key(namespace);
        let mut objects = BTreeMap::new();
        let mut history_objects = Vec::new();
        let mut snapshot_objects = Vec::new();

        for object in listed {
            NamespaceObjectKey::classify(namespace, object.key.clone())?;
            if object.key.starts_with(&history_prefix) {
                history_objects.push(object.clone());
            } else if object.key.starts_with(&snapshot_prefix) {
                snapshot_objects.push(object.clone());
            } else if let Some(suffix) = object.key.strip_prefix(&staging_prefix) {
                let Some(token) = suffix.strip_suffix(".json") else {
                    return Err(malformed_control_key(
                        "staging",
                        object.key,
                        "key must end with .json",
                    ));
                };
                let token = token.parse::<u64>().map_err(|_| {
                    malformed_control_key(
                        "staging",
                        object.key.clone(),
                        "key must contain a decimal fencing token",
                    )
                })?;
                if staging_key(namespace, token) != object.key {
                    return Err(malformed_control_key(
                        "staging",
                        object.key,
                        "key is not canonical",
                    ));
                }
            } else if object.key.starts_with(&gc_prefix) && object.key != candidate_key {
                return Err(malformed_control_key(
                    "gc",
                    object.key,
                    "unrecognized reserved key",
                ));
            }

            let duplicate_key = object.key.clone();
            if objects.insert(object.key.clone(), object).is_some() {
                return Err(malformed_control_key(
                    "namespace-inventory",
                    duplicate_key,
                    "duplicate key",
                ));
            }
        }

        // Delegate reserved-key grammar to the modules that own it. These
        // parsers validate the full batch before any retention DELETE begins.
        Manifest::history_observations_from_listed(namespace, history_objects)?;
        NamedSnapshot::validate_listed_objects(namespace, snapshot_objects)?;

        Ok(Self { objects })
    }

    fn fingerprint(&self) -> Option<InventoryFingerprint> {
        InventoryFingerprint::from_listed(self.objects.values())
    }

    fn all_objects(&self) -> Vec<ListedObject> {
        self.objects.values().cloned().collect()
    }

    fn all_keys(&self) -> BTreeSet<String> {
        self.objects.keys().cloned().collect()
    }

    fn object(&self, key: &str) -> Option<&ListedObject> {
        self.objects.get(key)
    }

    fn history_observations(&self, namespace: &str) -> Result<Vec<ManifestHistoryObservation>> {
        let prefix = Manifest::history_prefix(namespace);
        Manifest::history_observations_from_listed(
            namespace,
            self.objects
                .range(prefix.clone()..)
                .take_while(|(key, _)| key.starts_with(&prefix))
                .map(|(_, object)| object.clone())
                .collect(),
        )
    }

    fn snapshot_objects(&self, namespace: &str) -> Result<Vec<ListedObject>> {
        let prefix = NamedSnapshot::prefix(namespace);
        NamedSnapshot::validate_listed_objects(
            namespace,
            self.objects
                .range(prefix.clone()..)
                .take_while(|(key, _)| key.starts_with(&prefix))
                .map(|(_, object)| object.clone())
                .collect(),
        )
    }

    fn staging_objects(&self, namespace: &str) -> Vec<ListedObject> {
        let prefix = format!("{namespace}/_staging/");
        self.objects
            .range(prefix.clone()..)
            .take_while(|(key, _)| key.starts_with(&prefix))
            .map(|(_, object)| object.clone())
            .collect()
    }

    fn remove(&mut self, key: &str) {
        self.objects.remove(key);
    }

    fn upsert(&mut self, key: String, size: u64, version: Option<StorageVersion>) {
        self.objects.insert(
            key.clone(),
            ListedObject {
                key,
                size,
                last_modified: Utc::now(),
                version,
            },
        );
    }

    fn matches_listed_prefix<'a>(
        &self,
        prefix: &str,
        listed: impl IntoIterator<Item = &'a ListedObject>,
    ) -> bool {
        self.fingerprint()
            .is_some_and(|fingerprint| fingerprint.matches_listed_prefix(prefix, listed))
    }
}

impl From<&GcConfig> for GcConfigFingerprint {
    fn from(gc: &GcConfig) -> Self {
        let GcConfig {
            horizon_secs,
            compaction_upload_window_secs,
            skew_slop_secs,
            allow_unsafe_short_horizon,
            manifest_history_keep_count,
            pitr_retention_secs,
        } = gc;
        Self {
            horizon_secs: *horizon_secs,
            compaction_upload_window_secs: *compaction_upload_window_secs,
            skew_slop_secs: *skew_slop_secs,
            allow_unsafe_short_horizon: *allow_unsafe_short_horizon,
            manifest_history_keep_count: *manifest_history_keep_count,
            pitr_retention_secs: *pitr_retention_secs,
        }
    }
}

/// Lease-scoped record of compaction uploads not yet committed to a manifest.
///
/// The record is a temporary GC root, not a visibility mechanism: queries do
/// not read its keys. A staging object is useful only while its fencing token
/// matches the unexpired namespace lease observed by [`active_staged_keys`].
///
/// The type is encoded as self-describing JSON so its named fields can evolve
/// without depending on a positional binary layout.
///
/// # Examples
///
/// During lease token 17, a compactor can record the centroid and cluster keys
/// it uploaded. The record protects those keys from GC but does not make the
/// unfinished segment query-visible.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionStaging {
    /// Fencing token of the lease holder that owns these in-flight uploads.
    pub fencing_token: u64,
    /// Exact object keys uploaded by compaction but not yet manifest-visible.
    #[serde(default)]
    pub keys: BTreeSet<String>,
}

struct ActiveStagingObservation {
    keys: BTreeSet<String>,
    lease_expires_at: Option<DateTime<Utc>>,
}

/// Builds the object key for one lease holder's compaction staging record.
///
/// # Parameters
///
/// - `namespace`: Namespace that owns the compaction and lease.
/// - `fencing_token`: Monotonic token assigned to that lease acquisition.
///
/// # Returns
///
/// An owned key under `<namespace>/_staging/`, ending in `.json`.
///
/// # Examples
///
/// Namespace `catalog` with fencing token `17` maps to
/// `catalog/_staging/17.json`.
#[must_use]
pub fn staging_key(namespace: &str, fencing_token: u64) -> String {
    format!("{namespace}/_staging/{fencing_token}.json")
}

fn local_artifact_origin_for_gc(
    namespace: &str,
    manifest: &Manifest,
) -> Result<Option<ArtifactOrigin>> {
    let has_explicit_origins = !manifest.artifact_origins.is_empty()
        || manifest
            .fragments
            .iter()
            .any(|fragment| fragment.artifact_origin.is_some())
        || manifest
            .segments
            .iter()
            .any(|segment| segment.artifact_origin.is_some());
    let Some(incarnation) = manifest.namespace_incarnation() else {
        if has_explicit_origins {
            return Err(ZeppelinError::Serialization(format!(
                "manifest for namespace {namespace} has artifact origins but no local incarnation"
            )));
        }
        return Ok(None);
    };
    Ok(Some(ArtifactOrigin {
        namespace: NamespaceId::new(namespace.to_string())?,
        incarnation: NamespaceIncarnationId::from_uuid(incarnation),
    }))
}

/// Derives every immutable artifact key referenced by one manifest.
///
/// This pure function expands WAL fragments, segment metadata, cluster data,
/// attribute sidecars, bitmap/FTS indexes, quantization artifacts, and
/// `pending_deletes`. Pending-delete keys remain protected until the drain path
/// confirms their deletion and conditionally removes them from the manifest.
/// The returned set does not include the manifest object, history objects, GC
/// ledgers, leases, or staging records themselves; their key families are not
/// sweepable artifact shapes.
///
/// # Parameters
///
/// - `namespace`: Namespace prefix used by deterministic artifact-key helpers.
/// - `manifest`: Borrowed manifest snapshot whose references should be expanded.
///
/// # Returns
///
/// A lexicographically ordered, duplicate-free owned set of exact object keys.
/// Shared carried-cluster objects appear once.
///
/// # Errors
///
/// Returns an artifact-origin integrity error when an indexed reference cannot
/// be resolved or an explicit stored key falls outside its declared physical
/// origin. Legacy local-only manifests remain namespace-routed.
///
/// # Consistency
///
/// The result describes the supplied snapshot, not necessarily the current
/// manifest by the time the caller uses it. Delete callers must obtain a fresh
/// authoritative snapshot immediately before deciding.
///
/// # Performance
///
/// Performs no object-store I/O. Work and memory are proportional to fragments,
/// segments, clusters, and enabled sidecar families in the manifest, with
/// `BTreeSet` insertion costing `O(log n)` per distinct key.
///
/// # Examples
///
/// A two-cluster scalar-quantized segment contributes its tree or centroid
/// metadata, two cluster and attribute objects, two scalar-code objects, and
/// one calibration object. If both clusters reuse one carried object, that key
/// remains present only once.
///
/// # Rust Notes for Java/C Engineers
///
/// `&Manifest` is a temporary shared borrow, similar to a read-only Java
/// reference or `const Manifest *` in C, but Rust also guarantees it is non-null
/// and valid for the call. The returned [`BTreeSet`] owns cloned key strings and
/// therefore remains valid after the manifest borrow ends.
pub fn reachable_keys(namespace: &str, manifest: &Manifest) -> Result<BTreeSet<String>> {
    reachable_keys_with_staging(namespace, manifest, &BTreeSet::new())
}

/// Unions immutable artifact roots from already-decoded retained manifests.
///
/// The helper performs no storage I/O. Garbage collection uses it immediately
/// after the retention pass so mark planning and conservative pending-delete
/// checks do not re-list and re-download the same history bodies. A caller must
/// still take a fresh history observation before any physical deletion whose
/// safety depends on retained history.
fn reachable_keys_from_manifests(
    namespace: &str,
    manifests: &[Manifest],
) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for manifest in manifests {
        keys.extend(reachable_keys(namespace, manifest)?);
    }
    Ok(keys)
}

/// Unions one manifest's references with caller-validated staged uploads.
///
/// The caller is responsible for supplying only keys belonging to the active,
/// unexpired lease; [`active_staged_keys`] performs that validation for normal
/// GC use. Staged keys are protected from deletion but do not become visible to
/// query readers.
///
/// # Parameters
///
/// - `namespace`: Namespace prefix for manifest-derived artifact keys.
/// - `manifest`: Borrowed manifest snapshot to expand.
/// - `staging`: Borrowed exact keys currently protected by compaction staging.
///
/// # Returns
///
/// An owned, ordered union containing manifest and staged keys exactly once.
///
/// # Errors
///
/// Returns an artifact-origin integrity error rather than deriving any
/// unresolved immutable key from the logical namespace.
///
/// # Consistency
///
/// This is a snapshot calculation. Sweep code re-reads both authoritative
/// manifest state and active staging rather than trusting an earlier union.
///
/// # Examples
///
/// If the manifest references segment A while the active compactor has
/// uploaded two objects for segment B, the result protects A and both B objects;
/// only A is query-visible.
pub fn reachable_keys_with_staging(
    namespace: &str,
    manifest: &Manifest,
    staging: &BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    let local_origin = local_artifact_origin_for_gc(namespace, manifest)?;
    let origins = local_origin
        .as_ref()
        .map(|local_origin| manifest.artifact_origin_resolver(local_origin))
        .transpose()?;

    for fragment in &manifest.fragments {
        let physical_namespace = match origins {
            Some(origins) => origins
                .locate_fragment(fragment)?
                .physical_origin
                .namespace(),
            None => namespace,
        };
        keys.insert(WalFragment::s3_key(physical_namespace, &fragment.id));
    }

    for segment in &manifest.segments {
        let physical_namespace = match origins {
            Some(origins) => origins.locate_segment(segment)?.physical_namespace(),
            None => namespace,
        };
        if segment.hierarchical {
            keys.insert(tree_meta_key(physical_namespace, &segment.id));
            for node_id in manifest.hierarchical_routing_nodes(&segment.id) {
                keys.insert(crate::index::hierarchical::tree_node_key(
                    physical_namespace,
                    &segment.id,
                    node_id,
                ));
            }
        } else {
            keys.insert(centroids_key(physical_namespace, &segment.id));
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
                    physical_namespace,
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
            keys.insert(attrs_key(physical_namespace, owner, cluster_idx));

            if !segment.bitmap_fields.is_empty() {
                keys.insert(bitmap_key(physical_namespace, owner, cluster_idx));
            }

            if !segment.fts_fields.is_empty() {
                keys.insert(fts_index_key(physical_namespace, owner, cluster_idx));
            }

            match segment.quantization {
                QuantizationType::Scalar => {
                    keys.insert(sq_cluster_key(physical_namespace, owner, cluster_idx));
                }
                QuantizationType::Product => {
                    keys.insert(pq_cluster_key(physical_namespace, owner, cluster_idx));
                }
                QuantizationType::None => {}
            }
        }

        match segment.quantization {
            QuantizationType::Scalar => {
                keys.insert(sq_calibration_key(physical_namespace, &segment.id));
            }
            QuantizationType::Product => {
                keys.insert(pq_codebook_key(physical_namespace, &segment.id));
            }
            QuantizationType::None => {}
        }

        if segment.has_global_fts {
            keys.insert(global_fts_key(physical_namespace, &segment.id));
        }
    }

    keys.extend(manifest.pending_deletes.iter().cloned());
    keys.extend(staging.iter().cloned());
    Ok(keys)
}

/// Loads retained manifest history and unions every referenced artifact key.
///
/// Each retained history generation is a PITR root even when its objects are
/// absent from the current live manifest. A key remains protected until all
/// retained generations that reference it have been pruned.
///
/// # Parameters
///
/// - `store`: Borrowed object-store gateway used for history LIST and GETs.
/// - `namespace`: Namespace whose immutable manifest history is inspected.
///
/// # Returns
///
/// A duplicate-free, ordered set of artifact keys referenced by any retained
/// history manifest. An empty set means no history objects were listed.
///
/// # Errors
///
/// Propagates history listing, GET, key validation, and manifest decode errors.
/// If a listed history object disappears before its GET, returns `NotFound`
/// rather than silently treating that generation as unrooted.
///
/// # Side Effects
///
/// Performs one history-prefix LIST and one full-object GET per listed history
/// generation. It does not delete history or data artifacts.
///
/// # Consistency
///
/// The current live manifest is deliberately not included; callers union it
/// separately. A concurrent history-prune race can make a listed object vanish,
/// in which case this function fails closed and the GC cycle skips deletion.
///
/// # Examples
///
/// If retained generations 7 and 8 both reference segment A while only 8
/// references segment B, the returned set contains both segments' artifacts.
pub async fn retained_manifest_history_reachable_keys(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for entry in Manifest::list_history(store, namespace).await? {
        let manifest = Manifest::read_history(store, namespace, entry.version)
            .await?
            .ok_or_else(|| crate::error::ZeppelinError::NotFound { key: entry.key })?;
        keys.extend(reachable_keys(namespace, &manifest)?);
    }
    Ok(keys)
}

/// Builds the complete GC root union for a supplied live manifest and staging set.
///
/// # Parameters
///
/// - `store`: Borrowed gateway used to discover retained history.
/// - `namespace`: Namespace whose history and artifact keys are addressed.
/// - `manifest`: Borrowed current manifest snapshot.
/// - `staging`: Borrowed keys already validated as active compaction staging.
///
/// # Returns
///
/// The ordered union of current-manifest, retained-history, and staged keys.
///
/// # Errors
///
/// Propagates retained-history LIST, GET, and decode failures. No partial union
/// is returned because deleting from an incomplete root set would be unsafe.
///
/// # Performance
///
/// Adds one history LIST and one GET per retained generation to the in-memory
/// set construction performed by [`reachable_keys_with_staging`].
///
/// # Examples
///
/// A segment removed from the live manifest remains in this result while a
/// retained generation or active staging record still names its exact keys.
pub async fn reachable_keys_with_retained_history_and_staging(
    store: &ZeppelinStore,
    namespace: &str,
    manifest: &Manifest,
    staging: &BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    let retained_history = retained_manifest_history_reachable_keys(store, namespace).await?;
    reachable_keys_with_retained_history_and_staging_keys(
        namespace,
        manifest,
        staging,
        &retained_history,
    )
}

/// Unions already-loaded retained-history keys with live and staged roots.
///
/// # Parameters
///
/// - `namespace`: Namespace used to expand live manifest references.
/// - `manifest`: Borrowed current manifest snapshot.
/// - `staging`: Borrowed active staging keys.
/// - `retained_history`: Borrowed union computed from retained generations.
///
/// # Returns
///
/// A newly owned, ordered union of all three root families.
///
/// # Examples
///
/// The mark and sweep phases use this helper with separately loaded snapshots
/// so the second phase can revalidate instead of reusing the first phase's set.
fn reachable_keys_with_retained_history_and_staging_keys(
    namespace: &str,
    manifest: &Manifest,
    staging: &BTreeSet<String>,
    retained_history: &BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    let mut keys = reachable_keys_with_staging(namespace, manifest, staging)?;
    keys.extend(retained_history.iter().cloned());
    Ok(keys)
}

/// A known-shape artifact observed outside the authoritative reachable union.
///
/// This is neither an authoritative reference count nor permission to delete.
/// It records when and at which manifest generation the mark pass first saw the
/// key unreachable. The sweep must still re-list the object, wait out the
/// horizon, recognize the key shape, and revalidate fresh reachability.
///
/// The persisted representation is JSON. The generation field defaults to zero
/// so older ledgers remain readable; the wall-clock horizon still applies to
/// those legacy records.
///
/// # Examples
///
/// A WAL key first absent from generation 42 at 12:00 remains a candidate with
/// that original time and generation across later marks. Seeing it reachable
/// again removes the record instead of resetting or deleting the object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcCandidate {
    /// Exact S3 key that left the manifest-derived reachable union.
    pub key: String,
    /// Wall-clock time at which the key was first recorded unreachable.
    ///
    /// Manifest-delta candidates use the commit time; LIST-discovered candidates
    /// use the mark-cycle time.
    pub first_seen_unreachable_at: DateTime<Utc>,
    /// Manifest generation whose reachability snapshot first excluded the key.
    ///
    /// Legacy candidate records decode as `0`, treating them as logically
    /// very old while still requiring the wall-clock horizon before delete.
    #[serde(default)]
    pub unreachable_since_manifest_version: u64,
}

/// Persisted wrapper that distinguishes the current ledger from a legacy array.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GcCandidateStore {
    /// Wrapper schema marker written with new ledgers.
    ///
    /// The decoder accepts other numeric values so old data remains readable;
    /// the next successful mark rewrites any noncurrent value canonically.
    version: u32,
    /// Complete candidate ledger replacing the previous object contents.
    candidates: Vec<GcCandidate>,
}

/// Persisted representation observed while loading the candidate ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateLedgerEncoding {
    /// No ledger object exists yet.
    Missing,
    /// A ledger object exists but has an empty body.
    EmptyBody,
    /// The ledger uses the legacy bare-array JSON representation.
    LegacyArray,
    /// The ledger uses the versioned wrapper with the recorded schema value.
    Versioned(u32),
}

/// Candidate contents paired with the representation that produced them.
#[derive(Debug, Clone)]
struct LoadedCandidateLedger {
    candidates: Vec<GcCandidate>,
    encoding: CandidateLedgerEncoding,
}

impl LoadedCandidateLedger {
    fn missing() -> Self {
        Self {
            candidates: Vec::new(),
            encoding: CandidateLedgerEncoding::Missing,
        }
    }

    fn is_canonical(&self) -> bool {
        self.encoding == CandidateLedgerEncoding::Versioned(GC_CANDIDATE_STORE_VERSION)
    }
}

/// Observable counters from one attempted mark/sweep cycle.
///
/// Counts describe completed or skipped work, not a dry-run plan. Several
/// storage failures are logged and converted into an early `Ok` report so the
/// background loop can retry on a later tick; consult logs and metrics when a
/// report contains less progress than expected.
/// Manifest-history prune counts are emitted in the cycle-complete log and are
/// not stored in this report.
///
/// # Examples
///
/// A report with `candidates_marked = 3` and `objects_deleted = 0` commonly
/// describes a first pass whose new candidates have not yet crossed the
/// horizon. A later cycle may delete them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcCycleReport {
    /// Number of newly-unreachable objects added to the candidate ledger.
    pub candidates_marked: usize,
    /// Number of candidate and pending-delete removals accepted as complete.
    ///
    /// S3 reports both deletion and prior absence as successful DeleteObjects
    /// members, so this is an accepted-completion count rather than proof that
    /// every key had bytes immediately before the request.
    pub objects_deleted: usize,
    /// Number of pending-delete members accepted as deleted or already absent.
    pub pending_deletes_deleted: usize,
    /// Number of manifest pending-delete entries pruned after confirmed delete/absence.
    pub pending_deletes_pruned: usize,
    /// Number of manifest pending-delete entries retained for retry.
    pub pending_deletes_retained: usize,
    /// Bytes reclaimed for deleted keys whose sizes were available in metadata.
    ///
    /// Unknown sizes contribute zero, so this is a lower bound rather than the
    /// sum of physical object sizes returned by S3. In the current sweep, most
    /// true orphans are absent from the fresh manifest that supplies size
    /// metadata and therefore contribute zero.
    pub bytes_reclaimed: u64,
    /// Number of candidates skipped instead of deleted.
    pub candidates_skipped: usize,
}

/// Observable result of draining the live manifest's deferred-delete queue.
///
/// # Examples
///
/// If one two-key batch is accepted and a later one-key batch is uncertain, the
/// report contains two completed objects, two pruned entries, and one retained
/// entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingDeleteDrainReport {
    /// Number of pending-delete members accepted as deleted or already absent.
    pub objects_deleted: usize,
    /// Entries removed after successful DELETE or confirmed prior absence.
    pub entries_pruned: usize,
    /// Number of entries kept because deletion failed.
    pub entries_retained: usize,
}

struct PendingDeleteDrainOutcome {
    report: PendingDeleteDrainReport,
    complete: bool,
    observed_pending_deletes: Option<Vec<String>>,
}

struct PreparedPendingDeleteDrain {
    outcome: PendingDeleteDrainOutcome,
    refreshed_history: Vec<HistorySnapshot>,
    predelete_inventory: Option<NamespaceInventory>,
}

impl PendingDeleteDrainOutcome {
    fn new(
        report: PendingDeleteDrainReport,
        complete: bool,
        observed_pending_deletes: Option<Vec<String>>,
    ) -> Self {
        Self {
            report,
            complete,
            observed_pending_deletes,
        }
    }
}

/// Auditable reason that a known or listed object was not deleted this cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkipReason {
    /// The object key is outside the explicit immutable-artifact allowlist.
    UnknownShape,
    /// The candidate has not remained marked for the configured horizon.
    NotPersistedLongEnough,
    /// A freshly computed root union references the candidate again.
    ReachableNow,
    /// The artifact's creation ULID is younger than the configured horizon.
    UlidTooYoung,
    /// A conservative in-flight watermark protects this newer artifact.
    NewerThanInflightCompaction,
    /// An optional generation guard has not observed enough newer commits.
    NotEnoughManifestGenerations,
    /// The object was absent from the namespace LIST captured for this cycle.
    NotListedThisCycle,
    /// The key's object identity was missing or changed between inventories.
    ObjectIdentityChanged,
    /// The listed object's own modification time has not crossed the horizon.
    ObjectModifiedTooRecently,
    /// The object-store DELETE failed and the candidate remains retryable.
    DeleteFailed,
}

impl SkipReason {
    /// Returns the stable low-cardinality label used by GC metrics and logs.
    ///
    /// # Returns
    ///
    /// A process-static string; no allocation occurs.
    ///
    /// # Examples
    ///
    /// `ReachableNow` maps to `"reachable_now"`.
    fn label(self) -> &'static str {
        match self {
            Self::UnknownShape => "unknown_shape",
            Self::NotPersistedLongEnough => "unreachable_horizon",
            Self::ReachableNow => "reachable_now",
            Self::UlidTooYoung => "ulid_age_floor",
            Self::NewerThanInflightCompaction => "inflight_watermark",
            Self::NotEnoughManifestGenerations => "manifest_generation_guard",
            Self::NotListedThisCycle => "not_listed",
            Self::ObjectIdentityChanged => "object_identity_changed",
            Self::ObjectModifiedTooRecently => "object_modified_too_recently",
            Self::DeleteFailed => "delete_failed",
        }
    }
}

/// Pure outcome of evaluating the candidate deletion safety gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteDecision {
    /// All current safety predicates permit a physical DELETE attempt.
    Delete,
    /// The candidate remains stored, with the auditable reason attached.
    Skip(SkipReason),
}

/// Immutable inputs used to evaluate one candidate at a common cycle time.
#[derive(Debug, Clone, Copy)]
struct DeletePredicateContext {
    /// Required wall-clock and artifact-age grace period, in seconds.
    horizon_secs: u64,
    /// Timestamp captured once for the current cycle or test case.
    now: DateTime<Utc>,
    /// Oldest recognized artifact ULID among active staged uploads, in milliseconds.
    oldest_inflight_ulid_ms: Option<u64>,
    /// Generation of the freshly read live manifest.
    current_manifest_version: u64,
    /// Optional extra number of newer manifest generations required before delete.
    ///
    /// Production cycles currently pass `None`; tests exercise the independent
    /// guard retained for future policy use.
    min_newer_manifest_versions: Option<u64>,
}

/// Recognized immutable artifact families whose keys carry a creation ULID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedGcArtifact {
    /// One immutable WAL fragment with its file-name creation/time-ordering ULID.
    WalFragment {
        /// Creation and ordering identifier decoded from the WAL fragment key.
        ulid: Ulid,
    },
    /// One known segment artifact with its `seg_<ulid>` creation identifier.
    SegmentArtifact {
        /// Creation identifier decoded from the enclosing segment directory.
        ulid: Ulid,
    },
}

impl ParsedGcArtifact {
    /// Extracts the creation/time-ordering ULID shared by both artifact families.
    ///
    /// # Returns
    ///
    /// The copied [`Ulid`] embedded in this parsed value.
    ///
    /// # Examples
    ///
    /// Both a WAL key and `segments/seg_<ulid>/centroids.bin` return the ULID
    /// encoded in their path.
    fn ulid(self) -> Ulid {
        match self {
            Self::WalFragment { ulid } | Self::SegmentArtifact { ulid } => ulid,
        }
    }
}

/// Builds the key of the persisted per-namespace candidate ledger.
///
/// # Parameters
///
/// - `namespace`: Namespace that owns the ledger.
///
/// # Returns
///
/// An owned key of the form `<namespace>/_gc/candidates.json`.
///
/// # Examples
///
/// Namespace `catalog` maps to `catalog/_gc/candidates.json`.
#[must_use]
pub fn gc_candidate_store_key(namespace: &str) -> String {
    format!("{namespace}/_gc/candidates.json")
}

/// Loads and decodes the persisted candidate ledger for a namespace.
///
/// # Parameters
///
/// - `store`: Borrowed object-store gateway used for the full-object GET.
/// - `namespace`: Namespace whose ledger should be read.
///
/// # Returns
///
/// The owned candidate vector in persisted order. A missing or empty object
/// means no candidates and returns an empty vector.
///
/// # Errors
///
/// Propagates non-`NotFound` GET failures and JSON decoding errors. It accepts
/// the current versioned wrapper and the legacy bare-array representation.
///
/// # Performance
///
/// Performs one full GET and allocates one owned key string per candidate.
///
/// # Examples
///
/// A namespace that has never run the mark pass returns `[]`; malformed ledger
/// bytes return a serialization error rather than silently resetting history.
pub async fn load_gc_candidates(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<Vec<GcCandidate>> {
    load_candidate_ledger(store, namespace)
        .await
        .map(|ledger| ledger.candidates)
}

async fn load_candidate_ledger(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<LoadedCandidateLedger> {
    match store.get(&gc_candidate_store_key(namespace)).await {
        Ok(data) => decode_candidate_ledger(&data),
        Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(LoadedCandidateLedger::missing()),
        Err(e) => Err(e),
    }
}

/// Replaces the per-namespace candidate ledger with versioned JSON.
///
/// # Parameters
///
/// - `store`: Borrowed gateway used for one full-object PUT.
/// - `namespace`: Namespace that owns the ledger.
/// - `candidates`: Borrowed complete next ledger; entries are cloned into the
///   serialized wrapper and remain owned by the caller.
///
/// # Returns
///
/// `Ok(())` after object storage accepts the complete replacement.
///
/// # Errors
///
/// Propagates JSON serialization, key validation, and object-store PUT errors.
/// A failure leaves the previously persisted object or backend-defined write
/// outcome; callers must not proceed to sweep when mark persistence fails.
///
/// # Side Effects
///
/// Performs one unconditional PUT to `<namespace>/_gc/candidates.json`. This
/// maintenance ledger does not publish or hide query data.
///
/// # Performance
///
/// Clones the candidate vector and key strings, creates pretty-printed JSON,
/// and uploads the entire ledger.
///
/// # Examples
///
/// Saving candidates A and B replaces a previous A-only ledger. Saving an
/// empty slice records an empty versioned ledger after a successful sweep.
///
/// # Rust Notes for Java/C Engineers
///
/// The slice `&[GcCandidate]` is a borrowed view, like a bounded read-only array
/// view. `to_vec()` deliberately creates owned elements for serialization; C
/// code would need explicit allocation and cleanup, while Rust drops temporary
/// storage automatically on success or error.
pub async fn save_gc_candidates(
    store: &ZeppelinStore,
    namespace: &str,
    candidates: &[GcCandidate],
) -> Result<()> {
    save_gc_candidates_with_version(store, namespace, candidates)
        .await
        .map(|_| ())
}

async fn save_gc_candidates_with_version(
    store: &ZeppelinStore,
    namespace: &str,
    candidates: &[GcCandidate],
) -> Result<(u64, Option<StorageVersion>)> {
    let store_doc = GcCandidateStore {
        version: GC_CANDIDATE_STORE_VERSION,
        candidates: candidates.to_vec(),
    };
    let data = Bytes::from(serde_json::to_vec_pretty(&store_doc)?);
    let size = u64::try_from(data.len()).map_err(|_| {
        ZeppelinError::Validation(format!(
            "GC candidate ledger for {namespace} does not fit in u64 bytes"
        ))
    })?;
    let version = store
        .put_with_version(&gc_candidate_store_key(namespace), data)
        .await?;
    Ok((size, version))
}

/// Deletes live-manifest `pending_deletes` and prunes only confirmed entries.
///
/// A key leaves `pending_deletes` only after its object delete succeeds or the
/// object is already absent. Delete failures keep the entry in the manifest for
/// a later GC/compaction cycle. Artifacts still referenced by retained history
/// or younger than the configured horizon remain queued without a DELETE.
///
/// ```text
/// current pending_deletes entry
///          |
///          +-- retained history references it --> keep
///          +-- artifact ULID younger than horizon -> keep
///          `-- DELETE succeeds / already absent
///                         |
///                         v
///             remove entry with manifest CAS
///                         |
///                 CAS conflict -> reload and retry
/// ```
///
/// # Parameters
///
/// - `store`: Borrowed gateway used for history reads, object DELETEs, and the
///   conditional manifest update.
/// - `namespace`: Namespace whose live deferred-delete queue is drained.
/// - `gc`: Borrowed policy supplying the artifact-age horizon.
///
/// # Returns
///
/// Counts of accepted delete completions, entries pruned after delete or prior
/// absence, and entries retained in the successful pass. A missing manifest or
/// empty queue returns a zero/default report (plus any deletes accumulated
/// across an earlier CAS retry).
///
/// # Errors
///
/// Propagates retained-history discovery failures, manifest read/decode errors,
/// non-conflict manifest publication errors, and exhaustion of ten CAS retries.
/// Individual DELETE failures are not propagated: their keys remain in
/// `pending_deletes`. Physical deletes may already have succeeded before a
/// later error or CAS exhaustion is returned.
///
/// # Side Effects
///
/// Lists and GETs retained manifest history, classifies the complete queue, then
/// issues deterministic DeleteObjects batches of at most 1,000 unique eligible
/// keys. It updates deletion metrics and conditionally publishes a manifest
/// with only confirmed batches removed.
///
/// # Consistency
///
/// Object deletion precedes manifest pruning. This ordering is safe because
/// `pending_deletes` names artifacts already removed from the live data view,
/// while retained history is checked separately. ETag CAS prevents this drain
/// from overwriting concurrent manifest changes. On conflict, the fresh queue
/// is re-read; repeated DELETE sees `NotFound` and completes idempotently.
///
/// # Performance
///
/// Initial history discovery costs one LIST plus one GET per retained
/// generation. Each CAS attempt costs one manifest GET and at most one DELETE
/// request per 1,000 eligible keys. After a CAS conflict, an attempt that could
/// delete refreshes history again before its first batch. A successful pruning
/// attempt also writes one manifest-history object and conditionally updates
/// the live manifest inside [`Manifest::write_conditional`].
///
/// # Examples
///
/// If A is old, B is still PITR-reachable, and C's DELETE fails, A is deleted
/// and pruned, while B and C remain queued. A concurrent manifest writer can
/// force a CAS retry but cannot cause its unrelated changes to be overwritten.
pub async fn drain_pending_deletes(
    store: &ZeppelinStore,
    namespace: &str,
    gc: &GcConfig,
) -> Result<PendingDeleteDrainReport> {
    drain_pending_deletes_at(store, namespace, gc, Utc::now()).await
}

/// Drains pending deletes using an explicit cycle timestamp.
pub async fn drain_pending_deletes_at(
    store: &ZeppelinStore,
    namespace: &str,
    gc: &GcConfig,
    now: DateTime<Utc>,
) -> Result<PendingDeleteDrainReport> {
    let Some(live_roots) = load_live_root_observation(store, namespace, None).await? else {
        return Ok(PendingDeleteDrainReport::default());
    };
    let history = load_history_snapshot(store, namespace, None).await?;
    validate_rooted_history_snapshot(&live_roots, &history)?;
    let history_reachable = history_snapshot_reachable_keys(namespace, &history)?;
    Ok(drain_pending_deletes_with_retained_history(
        store,
        namespace,
        gc,
        &history_reachable,
        now,
        Some(&live_roots),
    )
    .await?
    .report)
}

/// Drains pending deletes using a retained-history union already loaded by the caller.
///
/// This is the retrying implementation behind [`drain_pending_deletes`]. The
/// supplied set serves the first attempt. A later CAS attempt refreshes history
/// before deleting when its newly read queue contains a potentially eligible
/// key.
///
/// # Parameters
///
/// - `store`: Borrowed object-store gateway.
/// - `namespace`: Namespace whose current manifest is updated.
/// - `gc`: Borrowed GC horizon policy.
/// - `retained_history`: Exact artifact keys pinned by retained generations.
///
/// # Returns
///
/// The same counts as [`PendingDeleteDrainReport`], including deletes accumulated
/// before a successful retry.
///
/// # Errors
///
/// Returns manifest read/publication errors and a `ManifestConflict` after all
/// retry attempts. Delete failures are retained as queue entries instead.
/// Earlier physical deletes can have succeeded before an error is returned.
///
/// # Examples
///
/// A caller that already computed retained history for the current GC cycle can
/// drain the queue without a second history scan.
async fn drain_pending_deletes_with_retained_history(
    store: &ZeppelinStore,
    namespace: &str,
    gc: &GcConfig,
    retained_history: &BTreeSet<String>,
    now: DateTime<Utc>,
    live_roots: Option<&LiveRootObservation>,
) -> Result<PendingDeleteDrainOutcome> {
    drain_pending_deletes_with_retained_history_from(
        store,
        namespace,
        gc,
        retained_history,
        now,
        None,
        None,
        live_roots,
    )
    .await
    .map(|(outcome, _)| outcome)
}

#[allow(clippy::too_many_arguments)]
async fn drain_pending_deletes_with_retained_history_from(
    store: &ZeppelinStore,
    namespace: &str,
    gc: &GcConfig,
    retained_history: &BTreeSet<String>,
    now: DateTime<Utc>,
    mut initial_manifest: Option<(Manifest, ManifestVersion)>,
    retry_history: Option<&BTreeMap<String, CachedHistory>>,
    live_roots: Option<&LiveRootObservation>,
) -> Result<(PendingDeleteDrainOutcome, Vec<HistorySnapshot>)> {
    let mut deleted_keys = BTreeSet::new();
    let mut complete = true;
    let mut refreshed_history = Vec::new();

    for attempt in 0..GC_MANIFEST_CAS_RETRIES {
        let observed = match initial_manifest.take() {
            Some(observed) => Some(observed),
            None => Manifest::read_versioned(store, namespace).await?,
        };
        let Some((mut manifest, version)) = observed else {
            return Ok((
                PendingDeleteDrainOutcome::new(
                    PendingDeleteDrainReport {
                        objects_deleted: deleted_keys.len(),
                        ..PendingDeleteDrainReport::default()
                    },
                    complete,
                    None,
                ),
                refreshed_history,
            ));
        };

        if manifest.pending_deletes.is_empty() {
            return Ok((
                PendingDeleteDrainOutcome::new(
                    PendingDeleteDrainReport {
                        objects_deleted: deleted_keys.len(),
                        ..PendingDeleteDrainReport::default()
                    },
                    complete,
                    Some(Vec::new()),
                ),
                refreshed_history,
            ));
        }

        let retry_may_delete = attempt > 0
            && manifest.pending_deletes.iter().any(|key| {
                !retained_history.contains(key)
                    && pending_delete_horizon_satisfied(namespace, key, now, gc.horizon_secs)
            });
        let retry_roots = if retry_may_delete {
            let snapshot = load_history_snapshot(store, namespace, retry_history).await?;
            if let Some(live_roots) = live_roots {
                validate_rooted_history_snapshot(live_roots, &snapshot)?;
            }
            let roots = history_snapshot_reachable_keys(namespace, &snapshot)?;
            refreshed_history.push(snapshot);
            Some(roots)
        } else {
            None
        };
        let retained_history = retry_roots.as_ref().unwrap_or(retained_history);

        let pending = manifest.pending_deletes.clone();
        let mut confirmed_absent = BTreeSet::new();
        let mut retained = BTreeSet::new();
        let mut eligible = BTreeSet::new();
        let mut live = manifest.clone();
        live.pending_deletes.clear();
        let live_reachable = reachable_keys(namespace, &live)?;

        for key in &pending {
            let deletion_key = TargetOwnedDeletionKey::classify(namespace, key.clone())?;
            if retained_history.contains(key) {
                retained.insert(key.clone());
                continue;
            }

            if !pending_delete_horizon_satisfied(namespace, key, now, gc.horizon_secs) {
                retained.insert(key.clone());
                continue;
            }
            if live_reachable.contains(key) {
                return Err(ZeppelinError::Serialization(format!(
                    "pending-delete key {key} is also reachable from the live manifest"
                )));
            }
            eligible.insert(deletion_key);
        }

        let eligible = eligible.into_iter().collect::<Vec<_>>();
        for batch in eligible.chunks(DELETE_MANY_MAX_KEYS) {
            if let Some(live_roots) = live_roots {
                revalidate_live_root_observation(store, namespace, live_roots).await?;
            }
            match delete_target_owned_many(store, batch).await {
                Ok(deleted) => {
                    debug_assert_eq!(deleted, batch.len());
                    for key in batch {
                        let key = key.as_str().to_string();
                        confirmed_absent.insert(key.clone());
                        if deleted_keys.insert(key.clone()) {
                            crate::metrics::GC_OBJECTS_DELETED_TOTAL
                                .with_label_values(&[namespace])
                                .inc();
                            info!(namespace, key = %key, "gc accepted pending-delete completion");
                        }
                    }
                }
                Err(error) => {
                    complete = false;
                    retained.extend(batch.iter().map(|key| key.as_str().to_string()));
                    warn!(
                        namespace,
                        batch_size = batch.len(),
                        error = %error,
                        "gc pending-delete batch uncertain; retaining every manifest entry"
                    );
                }
            }
        }

        if confirmed_absent.is_empty() {
            return Ok((
                PendingDeleteDrainOutcome::new(
                    PendingDeleteDrainReport {
                        objects_deleted: deleted_keys.len(),
                        entries_pruned: 0,
                        entries_retained: retained.len(),
                    },
                    complete,
                    Some(manifest.pending_deletes),
                ),
                refreshed_history,
            ));
        }

        manifest
            .pending_deletes
            .retain(|key| !confirmed_absent.contains(key));
        manifest.updated_at = now;

        match manifest.write_conditional(store, namespace, &version).await {
            Ok(_) => {
                return Ok((
                    PendingDeleteDrainOutcome::new(
                        PendingDeleteDrainReport {
                            objects_deleted: deleted_keys.len(),
                            entries_pruned: confirmed_absent.len(),
                            entries_retained: retained.len(),
                        },
                        complete,
                        Some(manifest.pending_deletes),
                    ),
                    refreshed_history,
                ));
            }
            Err(crate::error::ZeppelinError::ManifestConflict { .. }) => {
                warn!(
                    namespace,
                    attempt, "gc pending-delete manifest CAS conflict; retrying"
                );
            }
            Err(e) => return Err(e),
        }
    }

    Err(crate::error::ZeppelinError::ManifestConflict {
        namespace: namespace.to_string(),
    })
}

/// Drains only keys authorized by one full pre-delete namespace inventory.
///
/// A CAS conflict may retry publication of keys already confirmed absent, but
/// it never expands the destructive set. Newly observed pending entries wait
/// for a later cycle and a new full inventory.
async fn drain_pending_deletes_with_inventory_authority_from(
    store: &ZeppelinStore,
    namespace: &str,
    gc: &GcConfig,
    retained_history: &BTreeSet<String>,
    now: DateTime<Utc>,
    initial_manifest: (Manifest, ManifestVersion),
    live_roots: Option<&LiveRootObservation>,
) -> Result<PendingDeleteDrainOutcome> {
    let mut observed = Some(initial_manifest);
    let mut confirmed_absent = BTreeSet::new();
    let mut deleted_keys = BTreeSet::new();
    let mut complete = true;

    for attempt in 0..GC_MANIFEST_CAS_RETRIES {
        let Some((mut manifest, version)) = observed.take() else {
            return Ok(PendingDeleteDrainOutcome::new(
                PendingDeleteDrainReport {
                    objects_deleted: deleted_keys.len(),
                    ..PendingDeleteDrainReport::default()
                },
                complete,
                None,
            ));
        };
        if manifest.pending_deletes.is_empty() {
            return Ok(PendingDeleteDrainOutcome::new(
                PendingDeleteDrainReport {
                    objects_deleted: deleted_keys.len(),
                    ..PendingDeleteDrainReport::default()
                },
                complete,
                Some(Vec::new()),
            ));
        }

        if attempt == 0 {
            let mut live = manifest.clone();
            live.pending_deletes.clear();
            let live_reachable = reachable_keys(namespace, &live)?;
            let mut eligible = BTreeSet::new();
            for key in &manifest.pending_deletes {
                let deletion_key = TargetOwnedDeletionKey::classify(namespace, key.clone())?;
                if retained_history.contains(key)
                    || !pending_delete_horizon_satisfied(namespace, key, now, gc.horizon_secs)
                {
                    continue;
                }
                if live_reachable.contains(key) {
                    return Err(ZeppelinError::Serialization(format!(
                        "pending-delete key {key} is also reachable from the live manifest"
                    )));
                }
                eligible.insert(deletion_key);
            }
            let eligible = eligible.into_iter().collect::<Vec<_>>();
            for batch in eligible.chunks(DELETE_MANY_MAX_KEYS) {
                if let Some(live_roots) = live_roots {
                    revalidate_live_root_observation(store, namespace, live_roots).await?;
                }
                match delete_target_owned_many(store, batch).await {
                    Ok(deleted) => {
                        debug_assert_eq!(deleted, batch.len());
                        for key in batch {
                            let key = key.as_str().to_string();
                            confirmed_absent.insert(key.clone());
                            if deleted_keys.insert(key.clone()) {
                                crate::metrics::GC_OBJECTS_DELETED_TOTAL
                                    .with_label_values(&[namespace])
                                    .inc();
                                info!(namespace, key = %key, "gc accepted pending-delete completion");
                            }
                        }
                    }
                    Err(error) => {
                        complete = false;
                        warn!(
                            namespace,
                            batch_size = batch.len(),
                            error = %error,
                            "gc pending-delete batch uncertain; retaining every manifest entry"
                        );
                    }
                }
            }
        }

        let removable = manifest
            .pending_deletes
            .iter()
            .filter(|key| confirmed_absent.contains(*key))
            .cloned()
            .collect::<BTreeSet<_>>();
        if removable.is_empty() {
            let retained = manifest
                .pending_deletes
                .iter()
                .collect::<BTreeSet<_>>()
                .len();
            return Ok(PendingDeleteDrainOutcome::new(
                PendingDeleteDrainReport {
                    objects_deleted: deleted_keys.len(),
                    entries_pruned: 0,
                    entries_retained: retained,
                },
                complete,
                Some(manifest.pending_deletes),
            ));
        }

        let mut live = manifest.clone();
        live.pending_deletes.clear();
        let live_reachable = reachable_keys(namespace, &live)?;
        if let Some(key) = removable.iter().find(|key| live_reachable.contains(*key)) {
            return Err(ZeppelinError::Serialization(format!(
                "pending-delete key {key} became live after it was confirmed absent"
            )));
        }
        manifest
            .pending_deletes
            .retain(|key| !removable.contains(key));
        manifest.updated_at = now;

        match manifest.write_conditional(store, namespace, &version).await {
            Ok(_) => {
                return Ok(PendingDeleteDrainOutcome::new(
                    PendingDeleteDrainReport {
                        objects_deleted: deleted_keys.len(),
                        entries_pruned: removable.len(),
                        entries_retained: manifest
                            .pending_deletes
                            .iter()
                            .collect::<BTreeSet<_>>()
                            .len(),
                    },
                    complete,
                    Some(manifest.pending_deletes),
                ));
            }
            Err(ZeppelinError::ManifestConflict { .. }) => {
                warn!(
                    namespace,
                    attempt,
                    "gc pending-delete manifest CAS conflict; retrying confirmed pruning only"
                );
                observed = Manifest::read_versioned(store, namespace).await?;
                if observed
                    .as_ref()
                    .is_some_and(|(_, version)| !version.has_e_tag())
                {
                    return Err(ZeppelinError::Serialization(format!(
                        "manifest {} has no ETag after pending-delete CAS conflict",
                        Manifest::s3_key(namespace)
                    )));
                }
            }
            Err(e) => return Err(e),
        }
    }

    Err(ZeppelinError::ManifestConflict {
        namespace: namespace.to_string(),
    })
}

async fn prepare_warm_pending_delete_drain(
    store: &ZeppelinStore,
    namespace: &str,
    gc: &GcConfig,
    prune_reachable: &BTreeSet<String>,
    prior_history: &BTreeMap<String, CachedHistory>,
    now: DateTime<Utc>,
    live_roots: &LiveRootObservation,
) -> Result<PreparedPendingDeleteDrain> {
    let Some((manifest, _)) = Manifest::read_versioned(store, namespace).await? else {
        return Ok(PreparedPendingDeleteDrain {
            outcome: PendingDeleteDrainOutcome::new(
                PendingDeleteDrainReport::default(),
                true,
                None,
            ),
            refreshed_history: Vec::new(),
            predelete_inventory: None,
        });
    };

    let requires_history_refresh = manifest.pending_deletes.iter().any(|key| {
        !prune_reachable.contains(key)
            && pending_delete_horizon_satisfied(namespace, key, now, gc.horizon_secs)
    });
    if !requires_history_refresh {
        let retained = manifest
            .pending_deletes
            .iter()
            .collect::<BTreeSet<_>>()
            .len();
        return Ok(PreparedPendingDeleteDrain {
            outcome: PendingDeleteDrainOutcome::new(
                PendingDeleteDrainReport {
                    objects_deleted: 0,
                    entries_pruned: 0,
                    entries_retained: retained,
                },
                true,
                Some(manifest.pending_deletes),
            ),
            refreshed_history: Vec::new(),
            predelete_inventory: None,
        });
    }

    let prefix = format!("{namespace}/");
    let predelete_inventory =
        NamespaceInventory::from_listed(namespace, store.list_prefix_meta(&prefix).await?)?;
    let observations = predelete_inventory.history_observations(namespace)?;
    if observations
        .iter()
        .any(|observation| !matches!(observation.storage_version, Some(StorageVersion::Etag(_))))
    {
        return Err(ZeppelinError::Serialization(format!(
            "namespace {namespace} history lacks ETags for pending-delete validation"
        )));
    }
    let history = load_history_snapshot_from_observations(
        store,
        namespace,
        observations,
        Some(prior_history),
    )
    .await?;
    validate_rooted_history_snapshot(live_roots, &history)?;
    let retained_history = history_snapshot_reachable_keys(namespace, &history)?;
    let observed = read_versioned_manifest_from_inventory(store, namespace, &predelete_inventory)
        .await?
        .ok_or_else(|| ZeppelinError::NotFound {
            key: Manifest::s3_key(namespace),
        })?;
    let outcome = drain_pending_deletes_with_inventory_authority_from(
        store,
        namespace,
        gc,
        &retained_history,
        now,
        observed,
        Some(live_roots),
    )
    .await?;
    Ok(PreparedPendingDeleteDrain {
        outcome,
        refreshed_history: vec![history],
        predelete_inventory: Some(predelete_inventory),
    })
}

/// Checks whether a deferred-delete key is old enough for physical removal.
///
/// Age comes from the artifact ULID embedded in the key, not
/// `Manifest::updated_at`, because unrelated writes continually refresh the
/// manifest timestamp. Unknown key shapes fail closed. A zero horizon is an
/// explicit test/emergency override that accepts every shape immediately.
///
/// # Parameters
///
/// - `namespace`: Namespace prefix required to parse the exact key shape.
/// - `key`: Candidate WAL or known segment-artifact object key.
/// - `now`: Reference wall-clock time for deterministic age calculation.
/// - `horizon_secs`: Required minimum creation age in whole seconds.
///
/// # Returns
///
/// `true` when the horizon is zero or the parsed artifact ULID is at least that
/// old; `false` for young or unrecognized keys.
///
/// # Examples
///
/// With a five-second horizon, a 30-second-old WAL key is eligible even if the
/// manifest changed one second ago. `wal/not-a-ulid.wal` remains queued.
fn pending_delete_horizon_satisfied(
    namespace: &str,
    key: &str,
    now: DateTime<Utc>,
    horizon_secs: u64,
) -> bool {
    if horizon_secs == 0 {
        return true;
    }

    // Pending-delete artifacts carry their creation ULID in the key itself:
    // WAL fragments use the fragment ID, and segment artifacts live under
    // `segments/seg_<ulid>/`. That per-artifact creation clock is the
    // authoritative age; manifest.updated_at moves on every write and must not
    // gate deletion in a busy namespace. Unknown key shapes stay retained.
    let Some(parsed) = parse_gc_artifact_key(namespace, key) else {
        return false;
    };
    let now_ms = u64::try_from(now.timestamp_millis()).unwrap_or(0);
    let ulid_age_secs = super::fragment_age_secs(&parsed.ulid(), now_ms);
    ulid_age_secs >= horizon_secs
}

/// Decodes empty, versioned, or legacy candidate-ledger JSON.
///
/// # Parameters
///
/// - `data`: Borrowed complete object bytes. The decoder never retains this
///   slice; returned candidates own their strings and timestamps.
///
/// # Returns
///
/// Candidate contents plus whether the bytes were empty, a legacy array, or a
/// versioned wrapper. Callers use that distinction to preserve migrations even
/// when two representations contain the same candidates.
///
/// # Errors
///
/// If neither JSON shape decodes, returns the versioned-wrapper decode error.
/// The current wrapper's numeric `version` field is recorded but not validated.
///
/// # Examples
///
/// Both `{"version":1,"candidates":[]}` and `[]` decode to an empty ledger;
/// truncated JSON returns a serialization error.
///
/// # Rust Notes for Java/C Engineers
///
/// The nested `match` tries two strongly typed serde targets without mutating
/// the input. Each successful branch returns one owned ledger and its encoding;
/// the compiler requires every failure branch to produce the same `Result` type.
fn decode_candidate_ledger(data: &[u8]) -> Result<LoadedCandidateLedger> {
    if data.is_empty() {
        return Ok(LoadedCandidateLedger {
            candidates: Vec::new(),
            encoding: CandidateLedgerEncoding::EmptyBody,
        });
    }
    match serde_json::from_slice::<GcCandidateStore>(data) {
        Ok(store) => Ok(LoadedCandidateLedger {
            candidates: store.candidates,
            encoding: CandidateLedgerEncoding::Versioned(store.version),
        }),
        Err(wrapper_error) => match serde_json::from_slice::<Vec<GcCandidate>>(data) {
            Ok(candidates) => Ok(LoadedCandidateLedger {
                candidates,
                encoding: CandidateLedgerEncoding::LegacyArray,
            }),
            Err(_) => Err(wrapper_error.into()),
        },
    }
}

/// Computes the next ledger from LIST output, reachability, and existing marks.
///
/// Existing known-shape candidates preserve their first-observed timestamp and
/// manifest generation while they remain unreachable. Candidates that became
/// reachable are dropped. Newly listed, unreachable, recognized artifacts are
/// stamped with `now` and `manifest_version`; unknown side objects are ignored
/// so they can never enter the deletion pipeline.
///
/// # Parameters
///
/// - `namespace`: Namespace used to validate artifact key shapes.
/// - `listed_keys`: Exact namespace keys observed by the current LIST.
/// - `reachable`: Complete root union for the mark snapshot.
/// - `existing`: Borrowed previously persisted candidate ledger.
/// - `now`: Timestamp assigned only to newly marked candidates.
/// - `manifest_version`: Mark-snapshot generation assigned only to new marks.
///
/// # Returns
///
/// An owned vector ordered lexicographically by key. It contains every valid
/// existing unreachable candidate plus newly observed candidates, with no
/// duplicates.
///
/// # Consistency
///
/// This pure result is not safe to sweep until it has been persisted and all
/// authoritative roots have been re-read. LIST alone does not determine truth.
///
/// # Performance
///
/// Builds a [`BTreeMap`] in `O((e + l) log n)` time for `e` existing entries and
/// `l` listed keys, cloning strings retained in the returned ledger.
///
/// # Examples
///
/// Candidate A keeps its original mark, candidate B disappears because a new
/// manifest references it, and newly orphaned WAL C receives the current time
/// and version. An unrelated `_gc/` object is ignored as an unknown shape.
///
/// # Rust Notes for Java/C Engineers
///
/// Iterator-driven collection and `entry(...).or_insert(...)` express a
/// deterministic map update without exposing iterator invalidation. Rust's
/// borrowing rules prevent `existing` from being mutated while the new owned
/// map and vector are assembled.
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

/// Runs one complete history-prune, pending-drain, mark, and sweep cycle.
///
/// The cycle first prunes unretained manifest history and drains explicit
/// `pending_deletes`. A warm runner reuses the prune result for non-destructive
/// reachability decisions and refreshes history only when a pending entry could
/// be physically deleted; a stateless cold cycle retains the original complete
/// post-prune refresh. It then lists the namespace once, persists newly
/// unreachable known artifacts, and re-reads the live manifest, active staging,
/// and retained history before attempting any candidate DELETE.
///
/// ```text
/// prune unretained history
///          |
///          v
/// drain manifest pending_deletes
///          |
///          v
/// LIST namespace + read mark roots + save candidate ledger
///          |
///          v
/// re-read sweep roots (manifest, history, active staging)
///          |
///          +-- safety predicate fails --> keep candidate
///          `-- all predicates pass ----> DELETE in <=1,000-key batches
/// ```
///
/// This maintenance operation is best-effort. Most storage and decoding
/// failures emit structured warnings, stop the unsafe remainder of the cycle,
/// and return an `Ok` partial report so the background service can retry later.
/// A zero/partial report must therefore not be read as proof that no garbage
/// exists.
///
/// # Parameters
///
/// - `store`: Borrowed authoritative object-store gateway. No query cache is
///   consulted or invalidated.
/// - `namespace`: Namespace prefix whose immutable artifacts may be collected.
/// - `gc`: Borrowed retention and horizon policy. A short horizon must already
///   have passed boot-time safety validation unless explicitly overridden.
///
/// # Returns
///
/// A report of marks, completed or idempotently absent deletes, lower-bound
/// reclaimed bytes, and skips observed before the cycle stopped. Counts include
/// pending-delete work completed before the LIST/mark phase.
///
/// # Errors
///
/// The current implementation catches its ordinary storage, serialization,
/// manifest, and CAS failures, logs them, and returns a partial `Ok` report.
/// The `Result` return type preserves an error-capable API boundary for callers,
/// but no current fallible branch propagates an error from this function.
/// Earlier history or artifact DELETEs may have succeeded before a caught later
/// failure; this operation is not transactional.
///
/// # Side Effects
///
/// May delete old manifest-history objects, delete deferred and candidate data
/// artifacts, conditionally update the live manifest, overwrite the candidate
/// ledger, increment GC metrics, and emit structured logs. It never modifies a
/// WAL fragment or segment object in place.
///
/// # Consistency
///
/// The mark ledger is a hint. Sweep authority comes from the second live
/// manifest read plus freshly loaded retained history and active staging.
/// Candidate ledger replacements use unconditional PUTs and have no cross-cycle
/// lock, but an unchanged canonical ledger is not rewritten. A competing cycle
/// may delay or re-mark work; a newly changed mark must be durably written before
/// sweep, and no ledger entry bypasses fresh reachability and age predicates.
/// The horizon protects in-flight readers of stale cached manifests even though
/// this function does not inspect caches. A ledger candidate absent from the
/// cycle's original LIST is not deleted and is omitted from the cleaned ledger;
/// if it later appears again, it must be marked and wait through the horizon
/// again.
///
/// # Performance
///
/// A cold cycle includes the retention scan, a complete post-prune history
/// refresh, and the final sweep refresh. A warm runner omits the middle refresh
/// when its pending queue is empty, young, or already protected by prune roots.
/// It also overlaps independent retention LISTs and bounded body reads, then
/// overlaps the namespace, candidate-ledger, manifest, and staging inputs to
/// mark. Sweep overlaps its manifest and staging inputs before taking the final
/// history observation. Results remain inspected in the former sequential error
/// order. The one-shot path keeps its original request sequence and two ledger
/// PUTs. A warm cycle performs zero PUTs when the canonical ledger is unchanged,
/// one when only mark or cleanup changes it, and two when a newly durable mark
/// is also cleaned in the same cycle. Artifact DELETEs use deterministic
/// all-or-uncertain batches; those writes are not part of the read fan-out.
///
/// # Examples
///
/// On the first cycle, orphan A is listed, absent from all roots, and written to
/// the ledger but is too new to delete. After the horizon, a second cycle marks
/// it with the original timestamp, re-reads roots, and deletes it. If a retained
/// PITR generation starts referencing A, the fresh union drops the candidate
/// instead. If the sweep manifest GET fails, A remains for a later cycle.
///
/// # Rust Notes for Java/C Engineers
///
/// Each `match` handles success and failure exhaustively at the I/O boundary.
/// Owned sets and manifests are moved between phases, while the store and config
/// are shared borrows. Unlike a Java exception or C error code that might skip
/// cleanup implicitly, each early return constructs the exact partial report;
/// Rust also drops all owned temporary collections automatically.
impl GcRunner {
    /// Creates a stateful runner with an initially empty disposable memo.
    #[must_use]
    pub fn new(store: ZeppelinStore, gc: GcConfig) -> Self {
        Self {
            store,
            gc,
            preservation: None,
            namespaces: BTreeMap::new(),
        }
    }

    /// Attach the boot-composed preservation authority to destructive cycles.
    #[must_use]
    pub fn with_preservation_service(
        mut self,
        preservation: Option<Arc<PreservationService>>,
    ) -> Self {
        self.preservation = preservation;
        self
    }

    /// Replaces the GC policy used by subsequent cycles.
    ///
    /// Existing disposable history bodies remain available, but the exact
    /// configuration fingerprint no longer matches. The next call therefore
    /// performs a full authoritative cycle before a later idle decision can be
    /// admitted under the new policy.
    pub fn update_config(&mut self, gc: GcConfig) {
        self.gc = gc;
    }

    /// Runs one cycle and commits history bodies only after a complete refresh.
    pub async fn run_cycle_at(
        &mut self,
        incarnation: GcNamespaceIncarnation,
        now: DateTime<Utc>,
    ) -> Result<GcCycleReport> {
        if let Some(preservation) = &self.preservation {
            let namespace = NamespaceId::new(incarnation.name().to_string())?;
            let guard = preservation.guard_namespace(&namespace)?;
            if guard.is_locked() {
                preservation
                    .record_maintenance_deferral(false, &namespace, &guard)
                    .await?;
                info!(
                    namespace = incarnation.name(),
                    lock_count = guard.lock_ids().len(),
                    "gc_deferred_preservation"
                );
                return Ok(GcCycleReport::default());
            }
        }

        let mut previous = self
            .namespaces
            .remove(incarnation.name())
            .filter(|memo| memo.incarnation == incarnation);
        let mut initial_inventory = None;

        if let Some(memo) = previous.as_mut() {
            let prefix = format!("{}/", incarnation.name());
            let listed = match self.store.list_prefix_meta(&prefix).await {
                Ok(listed) => listed,
                Err(error) => {
                    warn!(
                        namespace = incarnation.name(),
                        error = %error,
                        "gc idle inventory refresh failed; skipping cycle"
                    );
                    memo.last_now = now;
                    memo.last_cycle_complete = false;
                    self.namespaces
                        .insert(memo.incarnation.name.clone(), memo.clone());
                    return Ok(GcCycleReport::default());
                }
            };
            let inventory = match NamespaceInventory::from_listed(incarnation.name(), listed) {
                Ok(inventory) => inventory,
                Err(error) => {
                    warn!(
                        namespace = incarnation.name(),
                        error = %error,
                        "gc namespace inventory validation failed; aborting cycle"
                    );
                    memo.last_now = now;
                    memo.last_cycle_complete = false;
                    self.namespaces
                        .insert(memo.incarnation.name.clone(), memo.clone());
                    return Err(error);
                }
            };
            let fingerprint = inventory.fingerprint();
            let config = GcConfigFingerprint::from(&self.gc);
            let before_deadline = memo.next_due_at.is_none_or(|deadline| now < deadline);
            let inventory_matches = fingerprint
                .as_ref()
                .is_some_and(|inventory| memo.inventory.as_ref() == Some(inventory));
            let live_root_identity_matches = memo
                .live_root_identity
                .matches_inventory(incarnation.name(), &inventory);
            if memo.last_cycle_complete
                && !memo.candidate_phase_due
                && memo.config == config
                && now >= memo.last_now
                && before_deadline
                && inventory_matches
                && live_root_identity_matches
            {
                memo.last_now = now;
                debug!(
                    namespace = incarnation.name(),
                    next_due_at = ?memo.next_due_at,
                    "gc idle inventory unchanged; skipping full cycle"
                );
                self.namespaces
                    .insert(memo.incarnation.name.clone(), memo.clone());
                return Ok(GcCycleReport::default());
            }
            initial_inventory = Some(inventory);
        }

        let outcome = run_gc_cycle_at_inner(
            &self.store,
            incarnation.name(),
            &self.gc,
            now,
            previous.as_ref(),
            initial_inventory,
        )
        .await;

        match outcome {
            Ok(outcome) => {
                if let Some(completed) = outcome.completed {
                    self.namespaces.insert(
                        incarnation.name.clone(),
                        NamespaceGcMemo {
                            incarnation,
                            history: completed.history,
                            inventory: completed.inventory,
                            live_root_identity: completed.live_root_identity,
                            next_due_at: completed.next_due_at,
                            last_now: now,
                            last_cycle_complete: true,
                            candidate_phase_due: false,
                            config: GcConfigFingerprint::from(&self.gc),
                        },
                    );
                } else if let Some(mut previous) = previous {
                    previous.last_now = now;
                    previous.last_cycle_complete = false;
                    previous.candidate_phase_due |= outcome.candidate_phase_due;
                    self.namespaces
                        .insert(previous.incarnation.name.clone(), previous);
                }
                Ok(outcome.report)
            }
            Err(error) => {
                if let Some(mut previous) = previous {
                    previous.last_now = now;
                    previous.last_cycle_complete = false;
                    self.namespaces
                        .insert(previous.incarnation.name.clone(), previous);
                }
                Err(error)
            }
        }
    }

    /// Drops memo state for a namespace that is no longer active.
    pub(crate) fn forget_namespace(&mut self, namespace: &str) {
        self.namespaces.remove(namespace);
    }

    /// Retains memo state only for freshly discovered active incarnations.
    pub(crate) fn retain_namespaces(&mut self, active: &BTreeSet<GcNamespaceIncarnation>) {
        self.namespaces
            .retain(|_, memo| active.contains(&memo.incarnation));
    }
}

async fn load_history_observation(
    store: &ZeppelinStore,
    namespace: &str,
    observation: &ManifestHistoryObservation,
    prior: Option<&BTreeMap<String, CachedHistory>>,
) -> Result<(Manifest, Bytes, Option<CachedHistory>)> {
    let cached = matching_cached_history(observation, prior);
    load_history_observation_owned(store, namespace, observation, cached).await
}

fn matching_cached_history(
    observation: &ManifestHistoryObservation,
    prior: Option<&BTreeMap<String, CachedHistory>>,
) -> Option<CachedHistory> {
    let listed_version = observation.storage_version.as_ref()?;
    listed_version.etag()?;
    prior
        .and_then(|entries| entries.get(&observation.history.key))
        .filter(|cached| cached.storage_version == *listed_version)
        .cloned()
}

async fn load_history_observation_owned(
    store: &ZeppelinStore,
    namespace: &str,
    observation: &ManifestHistoryObservation,
    cached: Option<CachedHistory>,
) -> Result<(Manifest, Bytes, Option<CachedHistory>)> {
    if let Some(cached) = cached {
        return Ok((
            cached.manifest.clone(),
            cached.stored_bytes.clone(),
            Some(cached),
        ));
    }

    let (bytes, get_etag) = store.get_with_meta(&observation.history.key).await?;
    let manifest = Manifest::decode_history_body(&bytes, namespace, &observation.history)?;
    let cacheable = match observation.storage_version.as_ref() {
        Some(StorageVersion::Etag(list_etag)) => {
            if get_etag.as_deref() != Some(list_etag.as_str()) {
                return Err(ZeppelinError::Serialization(format!(
                    "manifest history {} changed between LIST ETag {:?} and GET ETag {:?}",
                    observation.history.key, list_etag, get_etag
                )));
            }
            Some(CachedHistory {
                storage_version: StorageVersion::Etag(list_etag.clone()),
                manifest: manifest.clone(),
                stored_bytes: bytes.clone(),
                reachable_keys: reachable_keys(namespace, &manifest)?,
            })
        }
        Some(StorageVersion::BackendVersion(_)) | None => None,
    };
    Ok((manifest, bytes, cacheable))
}

async fn collect_bounded_ordered<T>(futures: Vec<BoxFuture<'static, Result<T>>>) -> Vec<Result<T>> {
    futures::stream::iter(futures)
        .buffered(GC_READ_BATCH_CONCURRENCY)
        .collect()
        .await
}

async fn load_history_observations_bounded(
    store: &ZeppelinStore,
    namespace: &str,
    observations: &[ManifestHistoryObservation],
    prior: Option<&BTreeMap<String, CachedHistory>>,
) -> Vec<Result<(Manifest, Bytes, Option<CachedHistory>)>> {
    let futures = observations
        .iter()
        .map(|observation| {
            let store = store.clone();
            let namespace = namespace.to_string();
            let observation = observation.clone();
            let cached = matching_cached_history(&observation, prior);
            async move {
                load_history_observation_owned(&store, &namespace, &observation, cached).await
            }
            .boxed()
        })
        .collect();
    collect_bounded_ordered(futures).await
}

async fn load_snapshot_observations_bounded(
    store: &ZeppelinStore,
    namespace: &str,
    mut objects: Vec<ListedObject>,
) -> Vec<Result<NamedSnapshotObservation>> {
    objects.sort_by(|left, right| left.key.cmp(&right.key));
    let futures = objects
        .into_iter()
        .map(|object| {
            let store = store.clone();
            let namespace = namespace.to_string();
            async move { NamedSnapshot::read_listed_observation(&store, &namespace, object).await }
                .boxed()
        })
        .collect();
    collect_bounded_ordered(futures).await
}

async fn load_history_snapshot(
    store: &ZeppelinStore,
    namespace: &str,
    prior: Option<&BTreeMap<String, CachedHistory>>,
) -> Result<HistorySnapshot> {
    let observations = Manifest::list_history_observations(store, namespace).await?;
    load_history_snapshot_from_observations(store, namespace, observations, prior).await
}

async fn load_history_snapshot_from_observations(
    store: &ZeppelinStore,
    namespace: &str,
    observations: Vec<ManifestHistoryObservation>,
    prior: Option<&BTreeMap<String, CachedHistory>>,
) -> Result<HistorySnapshot> {
    let mut entries = Vec::with_capacity(observations.len());
    let mut cacheable = BTreeMap::new();
    if prior.is_some() {
        let loaded =
            load_history_observations_bounded(store, namespace, &observations, prior).await;
        for (observation, result) in observations.into_iter().zip(loaded) {
            let (manifest, bytes, cached) = result?;
            if let Some(cached) = cached {
                cacheable.insert(observation.history.key.clone(), cached);
            }
            entries.push((observation, manifest, bytes));
        }
    } else {
        for observation in observations {
            let (manifest, bytes, cached) =
                load_history_observation(store, namespace, &observation, prior).await?;
            if let Some(cached) = cached {
                cacheable.insert(observation.history.key.clone(), cached);
            }
            entries.push((observation, manifest, bytes));
        }
    }
    Ok(HistorySnapshot { entries, cacheable })
}

fn require_every_rooted_generation_observed(
    live_roots: &LiveRootObservation,
    observed: &BTreeSet<ManifestGeneration>,
) -> Result<()> {
    if let Some(missing) = live_roots
        .rooted_generations
        .keys()
        .find(|generation| !observed.contains(*generation))
    {
        return Err(BranchError::BranchRootInvalid {
            branch_id: None,
            reason: format!(
                "live branch root references missing manifest history generation {}",
                missing.get()
            ),
        }
        .into());
    }
    Ok(())
}

fn validate_rooted_history_snapshot(
    live_roots: &LiveRootObservation,
    snapshot: &HistorySnapshot,
) -> Result<()> {
    let mut observed = BTreeSet::new();
    for (observation, _, stored_bytes) in &snapshot.entries {
        if let Some(generation) = live_roots
            .rooted_generations
            .keys()
            .copied()
            .find(|generation| generation.get() == observation.history.version)
        {
            live_roots
                .manifest
                .validate_rooted_history_bytes(generation, stored_bytes)?;
            observed.insert(generation);
        }
    }
    require_every_rooted_generation_observed(live_roots, &observed)
}

async fn prune_history_with_memo_at(
    store: &ZeppelinStore,
    namespace: &str,
    retention: ManifestHistoryRetention,
    now: DateTime<Utc>,
    prior: Option<&BTreeMap<String, CachedHistory>>,
    live_roots: &LiveRootObservation,
) -> Result<MemoizedHistoryPruneResult> {
    if retention.keep_count == 0 {
        return Err(ZeppelinError::Config(
            "gc.manifest_history_keep_count must be greater than zero".to_string(),
        ));
    }
    if prior.is_some() {
        return prune_history_with_memo_parallel_at(
            store, namespace, retention, now, prior, live_roots,
        )
        .await;
    }

    let observations = Manifest::list_history_observations(store, namespace).await?;
    let keep_from = observations.len().saturating_sub(retention.keep_count);
    let snapshot_observations = NamedSnapshot::list_observations(store, namespace).await?;
    let pinned_generations = snapshot_observations
        .iter()
        .map(|observation| observation.snapshot.generation)
        .collect::<BTreeSet<_>>();
    let retention_window = retention
        .pitr_retention_secs
        .saturating_add(retention.skew_slop_secs);
    let mut retained_manifests = Vec::new();
    let mut retained_history_observations = Vec::new();
    let mut prunable = Vec::new();
    let mut observed_rooted_generations = BTreeSet::new();

    for (index, observation) in observations.into_iter().enumerate() {
        let (manifest, stored_bytes, _) =
            load_history_observation(store, namespace, &observation, prior).await?;
        let keep_by_count = index >= keep_from;
        let keep_by_pin = pinned_generations.contains(&observation.history.version);
        let rooted_generation = live_roots
            .rooted_generations
            .keys()
            .copied()
            .find(|generation| generation.get() == observation.history.version);
        if let Some(generation) = rooted_generation {
            live_roots
                .manifest
                .validate_rooted_history_bytes(generation, &stored_bytes)?;
            observed_rooted_generations.insert(generation);
        }
        let keep_by_root = rooted_generation.is_some();
        let keep_by_time = retention.pitr_retention_secs > 0
            && now.signed_duration_since(manifest.updated_at).num_seconds()
                <= retention_window as i64;
        if keep_by_count || keep_by_time || keep_by_pin || keep_by_root {
            retained_history_observations.push(observation.clone());
            retained_manifests.push(manifest.clone());
        } else {
            prunable.push(observation.history.key);
        }
    }

    require_every_rooted_generation_observed(live_roots, &observed_rooted_generations)?;

    let deletion_keys = prunable
        .iter()
        .cloned()
        .map(|key| TargetOwnedDeletionKey::classify(namespace, key))
        .collect::<Result<Vec<_>>>()?;
    for batch in deletion_keys.chunks(DELETE_MANY_MAX_KEYS) {
        revalidate_live_root_observation(store, namespace, live_roots).await?;
        delete_target_owned_many(store, batch).await?;
    }

    Ok(MemoizedHistoryPruneResult {
        result: ManifestHistoryPruneResult {
            pruned: prunable.len(),
            retained_manifests,
        },
        retained_history_observations,
        snapshot_observations,
    })
}

async fn prune_history_with_memo_parallel_at(
    store: &ZeppelinStore,
    namespace: &str,
    retention: ManifestHistoryRetention,
    now: DateTime<Utc>,
    prior: Option<&BTreeMap<String, CachedHistory>>,
    live_roots: &LiveRootObservation,
) -> Result<MemoizedHistoryPruneResult> {
    let snapshot_prefix = NamedSnapshot::prefix(namespace);
    let (history_result, snapshot_result) = tokio::join!(
        Manifest::list_history_observations(store, namespace),
        store.list_prefix_meta(&snapshot_prefix),
    );
    let observations = history_result?;
    let snapshot_objects = snapshot_result?;
    prune_history_with_memo_parallel_from_observations(
        store,
        namespace,
        retention,
        now,
        prior,
        observations,
        snapshot_objects,
        None,
        live_roots,
    )
    .await
}

async fn prune_history_with_inventory_at(
    store: &ZeppelinStore,
    namespace: &str,
    retention: ManifestHistoryRetention,
    now: DateTime<Utc>,
    prior: Option<&BTreeMap<String, CachedHistory>>,
    inventory: &mut NamespaceInventory,
    live_roots: &LiveRootObservation,
) -> Result<MemoizedHistoryPruneResult> {
    if retention.keep_count == 0 {
        return Err(ZeppelinError::Config(
            "gc.manifest_history_keep_count must be greater than zero".to_string(),
        ));
    }
    let observations = inventory.history_observations(namespace)?;
    let snapshot_objects = inventory.snapshot_objects(namespace)?;
    prune_history_with_memo_parallel_from_observations(
        store,
        namespace,
        retention,
        now,
        prior,
        observations,
        snapshot_objects,
        Some(inventory),
        live_roots,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn prune_history_with_memo_parallel_from_observations(
    store: &ZeppelinStore,
    namespace: &str,
    retention: ManifestHistoryRetention,
    now: DateTime<Utc>,
    prior: Option<&BTreeMap<String, CachedHistory>>,
    observations: Vec<ManifestHistoryObservation>,
    snapshot_objects: Vec<ListedObject>,
    mut inventory: Option<&mut NamespaceInventory>,
    live_roots: &LiveRootObservation,
) -> Result<MemoizedHistoryPruneResult> {
    let keep_from = observations.len().saturating_sub(retention.keep_count);

    let (history_results, snapshot_results) = tokio::join!(
        load_history_observations_bounded(store, namespace, &observations, prior),
        load_snapshot_observations_bounded(store, namespace, snapshot_objects),
    );
    let mut snapshot_observations = snapshot_results.into_iter().collect::<Result<Vec<_>>>()?;
    let history_entries = history_results.into_iter().collect::<Result<Vec<_>>>()?;
    snapshot_observations.sort_by(|left, right| left.snapshot.name.cmp(&right.snapshot.name));
    let pinned_generations = snapshot_observations
        .iter()
        .map(|observation| observation.snapshot.generation)
        .collect::<BTreeSet<_>>();
    let retention_window = retention
        .pitr_retention_secs
        .saturating_add(retention.skew_slop_secs);
    let mut retained_manifests = Vec::new();
    let mut retained_history_observations = Vec::new();
    let mut prunable = Vec::new();
    let mut observed_rooted_generations = BTreeSet::new();

    for (index, (observation, (manifest, stored_bytes, _))) in
        observations.into_iter().zip(history_entries).enumerate()
    {
        let keep_by_count = index >= keep_from;
        let keep_by_pin = pinned_generations.contains(&observation.history.version);
        let rooted_generation = live_roots
            .rooted_generations
            .keys()
            .copied()
            .find(|generation| generation.get() == observation.history.version);
        if let Some(generation) = rooted_generation {
            live_roots
                .manifest
                .validate_rooted_history_bytes(generation, &stored_bytes)?;
            observed_rooted_generations.insert(generation);
        }
        let keep_by_root = rooted_generation.is_some();
        let keep_by_time = retention.pitr_retention_secs > 0
            && now.signed_duration_since(manifest.updated_at).num_seconds()
                <= retention_window as i64;
        if keep_by_count || keep_by_time || keep_by_pin || keep_by_root {
            retained_history_observations.push(observation);
            retained_manifests.push(manifest);
        } else {
            prunable.push(observation.history.key);
        }
    }

    require_every_rooted_generation_observed(live_roots, &observed_rooted_generations)?;

    let deletion_keys = prunable
        .iter()
        .cloned()
        .map(|key| TargetOwnedDeletionKey::classify(namespace, key))
        .collect::<Result<Vec<_>>>()?;
    for batch in deletion_keys.chunks(DELETE_MANY_MAX_KEYS) {
        revalidate_live_root_observation(store, namespace, live_roots).await?;
        delete_target_owned_many(store, batch).await?;
        if let Some(inventory) = inventory.as_deref_mut() {
            for key in batch {
                inventory.remove(key.as_str());
            }
        }
    }

    Ok(MemoizedHistoryPruneResult {
        result: ManifestHistoryPruneResult {
            pruned: prunable.len(),
            retained_manifests,
        },
        retained_history_observations,
        snapshot_observations,
    })
}

fn history_snapshot_reachable_keys(
    namespace: &str,
    snapshot: &HistorySnapshot,
) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for (observation, manifest, _) in &snapshot.entries {
        if let Some(cached) = snapshot.cacheable.get(&observation.history.key) {
            keys.extend(cached.reachable_keys.iter().cloned());
        } else {
            keys.extend(reachable_keys(namespace, manifest)?);
        }
    }
    Ok(keys)
}

fn history_observations_match_snapshot(
    expected: &[ManifestHistoryObservation],
    actual: &HistorySnapshot,
) -> bool {
    expected
        .iter()
        .eq(actual.entries.iter().map(|(observation, _, _)| observation))
}

enum NextGcDeadlineError {
    Reachability(ZeppelinError),
    InvalidSchedule,
}

impl From<ZeppelinError> for NextGcDeadlineError {
    fn from(error: ZeppelinError) -> Self {
        Self::Reachability(error)
    }
}

fn next_gc_deadline(
    namespace: &str,
    gc: &GcConfig,
    now: DateTime<Utc>,
    candidates: &[GcCandidate],
    manifest: &Manifest,
    history: &HistorySnapshot,
    staging: &ActiveStagingObservation,
) -> std::result::Result<Option<DateTime<Utc>>, NextGcDeadlineError> {
    let mut next = None;
    let mut consider_deadline = |deadline: DateTime<Utc>, include_overdue: bool| {
        let deadline = if include_overdue && deadline <= now {
            now
        } else {
            deadline
        };
        if deadline >= now && next.is_none_or(|current| deadline < current) {
            next = Some(deadline);
        }
    };

    let retained_history = history_snapshot_reachable_keys(namespace, history)?;
    let candidate_reachable = reachable_keys_with_retained_history_and_staging_keys(
        namespace,
        manifest,
        &staging.keys,
        &retained_history,
    )?;
    let mut live_without_pending = manifest.clone();
    live_without_pending.pending_deletes.clear();
    let pending_reachable = reachable_keys_with_retained_history_and_staging_keys(
        namespace,
        &live_without_pending,
        &staging.keys,
        &retained_history,
    )?;

    for candidate in candidates {
        if candidate_reachable.contains(&candidate.key) {
            continue;
        }
        let first_seen = deadline_after_secs(candidate.first_seen_unreachable_at, gc.horizon_secs)
            .map_err(|()| NextGcDeadlineError::InvalidSchedule)?;
        let artifact = parse_gc_artifact_key(namespace, &candidate.key)
            .ok_or(NextGcDeadlineError::InvalidSchedule)?;
        let artifact_created = DateTime::<Utc>::from_timestamp_millis(
            i64::try_from(artifact.ulid().timestamp_ms())
                .map_err(|_| NextGcDeadlineError::InvalidSchedule)?,
        )
        .ok_or(NextGcDeadlineError::InvalidSchedule)?;
        let artifact_due = deadline_after_secs(artifact_created, gc.horizon_secs)
            .map_err(|()| NextGcDeadlineError::InvalidSchedule)?;
        let deadline = first_seen.max(artifact_due);
        consider_deadline(deadline, true);
    }

    for key in &manifest.pending_deletes {
        if pending_reachable.contains(key) {
            continue;
        }
        let Some(artifact) = parse_gc_artifact_key(namespace, key) else {
            continue;
        };
        let artifact_created = DateTime::<Utc>::from_timestamp_millis(
            i64::try_from(artifact.ulid().timestamp_ms())
                .map_err(|_| NextGcDeadlineError::InvalidSchedule)?,
        )
        .ok_or(NextGcDeadlineError::InvalidSchedule)?;
        let deadline = deadline_after_secs(artifact_created, gc.horizon_secs)
            .map_err(|()| NextGcDeadlineError::InvalidSchedule)?;
        consider_deadline(deadline, true);
    }

    if gc.pitr_retention_secs > 0 {
        let retention = gc
            .pitr_retention_secs
            .checked_add(gc.skew_slop_secs)
            .and_then(|seconds| seconds.checked_add(1))
            .ok_or(NextGcDeadlineError::InvalidSchedule)?;
        for (_, manifest, _) in &history.entries {
            consider_deadline(
                deadline_after_secs(manifest.updated_at, retention)
                    .map_err(|()| NextGcDeadlineError::InvalidSchedule)?,
                false,
            );
        }
    }

    if let Some(expires_at) = staging.lease_expires_at {
        consider_deadline(expires_at, false);
    }

    Ok(next)
}

fn deadline_after_secs(
    base: DateTime<Utc>,
    seconds: u64,
) -> std::result::Result<DateTime<Utc>, ()> {
    let seconds = i64::try_from(seconds).map_err(|_| ())?;
    base.checked_add_signed(chrono::Duration::seconds(seconds))
        .ok_or(())
}

struct MarkReadInputs {
    listed_objects: Vec<ListedObject>,
    persisted_ledger: LoadedCandidateLedger,
    manifest: Manifest,
    manifest_version: ManifestVersion,
    staging: ActiveStagingObservation,
}

async fn read_inventory_object(
    store: &ZeppelinStore,
    key: &str,
    listed: Option<&ListedObject>,
    require_etag: bool,
) -> Result<Option<Bytes>> {
    match store.get_with_meta(key).await {
        Ok((bytes, get_etag)) => {
            let object = listed.ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "object {key} appeared after the namespace inventory LIST"
                ))
            })?;
            match object.version.as_ref() {
                Some(StorageVersion::Etag(list_etag)) => {
                    if get_etag.as_deref() != Some(list_etag.as_str()) {
                        return Err(ZeppelinError::Serialization(format!(
                            "object {key} changed between LIST ETag {list_etag:?} and GET ETag {get_etag:?}"
                        )));
                    }
                }
                Some(StorageVersion::BackendVersion(_)) | None if require_etag => {
                    return Err(ZeppelinError::Serialization(format!(
                        "object {key} has no LIST ETag for pre-delete validation"
                    )));
                }
                Some(StorageVersion::BackendVersion(_)) | None => {}
            }
            Ok(Some(bytes))
        }
        Err(ZeppelinError::NotFound { .. }) if listed.is_none() => Ok(None),
        Err(error) => Err(error),
    }
}

async fn load_candidates_from_inventory(
    store: &ZeppelinStore,
    namespace: &str,
    inventory: &NamespaceInventory,
    require_etag: bool,
) -> Result<LoadedCandidateLedger> {
    let key = gc_candidate_store_key(namespace);
    match read_inventory_object(store, &key, inventory.object(&key), require_etag).await? {
        Some(bytes) => decode_candidate_ledger(&bytes),
        None => Ok(LoadedCandidateLedger::missing()),
    }
}

async fn read_versioned_manifest_from_inventory(
    store: &ZeppelinStore,
    namespace: &str,
    inventory: &NamespaceInventory,
) -> Result<Option<(Manifest, ManifestVersion)>> {
    let key = Manifest::s3_key(namespace);
    let listed = inventory.object(&key);
    match store.get_with_meta(&key).await {
        Ok((bytes, get_etag)) => {
            let object = listed.ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "manifest {key} appeared after the namespace inventory LIST"
                ))
            })?;
            let Some(StorageVersion::Etag(list_etag)) = object.version.as_ref() else {
                return Err(ZeppelinError::Serialization(format!(
                    "manifest {key} has no LIST ETag for pending-delete validation"
                )));
            };
            if get_etag.as_deref() != Some(list_etag.as_str()) {
                return Err(ZeppelinError::Serialization(format!(
                    "manifest {key} changed between LIST ETag {list_etag:?} and GET ETag {get_etag:?}"
                )));
            }
            let manifest = Manifest::from_bytes_for_namespace(&bytes, namespace)?;
            let version = ManifestVersion::for_manifest(get_etag, &manifest, bytes, false);
            Ok(Some((manifest, version)))
        }
        Err(ZeppelinError::NotFound { .. }) if listed.is_none() => Ok(None),
        Err(error) => Err(error),
    }
}

async fn active_staging_from_inventory(
    store: &ZeppelinStore,
    namespace: &str,
    now: DateTime<Utc>,
    inventory: &NamespaceInventory,
    require_etag: bool,
) -> Result<ActiveStagingObservation> {
    let lease_key = format!("{namespace}/lease.json");
    let Some(lease_data) = read_inventory_object(
        store,
        &lease_key,
        inventory.object(&lease_key),
        require_etag,
    )
    .await?
    else {
        return Ok(ActiveStagingObservation {
            keys: BTreeSet::new(),
            lease_expires_at: None,
        });
    };
    let lease: Lease = serde_json::from_slice(&lease_data)?;
    if lease.expires_at <= now {
        return Ok(ActiveStagingObservation {
            keys: BTreeSet::new(),
            lease_expires_at: None,
        });
    }

    let objects = inventory.staging_objects(namespace);
    let active_key = staging_key(namespace, lease.fencing_token);
    let active_was_listed = inventory.object(&active_key).is_some();
    let futures = objects
        .into_iter()
        .map(|object| {
            let store = store.clone();
            async move {
                let key = object.key.clone();
                let data = read_inventory_object(&store, &key, Some(&object), require_etag)
                    .await?
                    .ok_or_else(|| ZeppelinError::NotFound { key: key.clone() })?;
                let entry = serde_json::from_slice::<CompactionStaging>(&data)?;
                Ok((key, entry))
            }
            .boxed()
        })
        .collect();
    let mut staged = BTreeSet::new();
    for result in collect_bounded_ordered(futures).await {
        let (key, entry) = result?;
        if staging_key(namespace, entry.fencing_token) != key {
            return Err(ZeppelinError::Serialization(format!(
                "staging key {key} contains mismatched token {}",
                entry.fencing_token
            )));
        }
        if entry.fencing_token == lease.fencing_token {
            staged.extend(entry.keys);
        }
    }
    if !active_was_listed {
        // The active token has exactly one canonical staging key. Probe it
        // after reading the lease so a compactor that publishes its staging
        // root between the full inventory LIST and this lease observation is
        // not omitted merely because the earlier LIST did not contain it.
        match store.get(&active_key).await {
            Ok(data) => {
                let entry: CompactionStaging = serde_json::from_slice(&data)?;
                if entry.fencing_token != lease.fencing_token {
                    return Err(ZeppelinError::Serialization(format!(
                        "staging key {active_key} contains token {}, expected {}",
                        entry.fencing_token, lease.fencing_token
                    )));
                }
                staged.extend(entry.keys);
            }
            Err(ZeppelinError::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    let lease_expires_at = (!staged.is_empty()).then_some(lease.expires_at);
    Ok(ActiveStagingObservation {
        keys: staged,
        lease_expires_at,
    })
}

enum MarkReadFailure {
    NamespaceList(ZeppelinError),
    CandidateLedger(ZeppelinError),
    ManifestMissing,
    Manifest(ZeppelinError),
    Staging(ZeppelinError),
}

fn assemble_mark_read_inputs(
    listed_objects: Result<Vec<ListedObject>>,
    persisted_ledger: Result<LoadedCandidateLedger>,
    manifest: Result<Option<(Manifest, ManifestVersion)>>,
    staging: Result<ActiveStagingObservation>,
) -> std::result::Result<MarkReadInputs, MarkReadFailure> {
    let listed_objects = listed_objects.map_err(MarkReadFailure::NamespaceList)?;
    let persisted_ledger = persisted_ledger.map_err(MarkReadFailure::CandidateLedger)?;
    let (manifest, manifest_version) = manifest
        .map_err(MarkReadFailure::Manifest)?
        .ok_or(MarkReadFailure::ManifestMissing)?;
    let staging = staging.map_err(MarkReadFailure::Staging)?;
    Ok(MarkReadInputs {
        listed_objects,
        persisted_ledger,
        manifest,
        manifest_version,
        staging,
    })
}

async fn load_mark_read_inputs(
    store: &ZeppelinStore,
    namespace: &str,
    now: DateTime<Utc>,
    read_mode: GcReadMode,
) -> std::result::Result<MarkReadInputs, MarkReadFailure> {
    let prefix = format!("{namespace}/");
    if read_mode.is_bounded() {
        let (listed, candidates, manifest, staging) = tokio::join!(
            store.list_prefix_meta(&prefix),
            load_candidate_ledger(store, namespace),
            Manifest::read_versioned(store, namespace),
            active_staging_observation_at_with_mode(store, namespace, now, GcReadMode::WarmBounded,),
        );
        assemble_mark_read_inputs(listed, candidates, manifest, staging)
    } else {
        let listed = store
            .list_prefix_meta(&prefix)
            .await
            .map_err(MarkReadFailure::NamespaceList)?;
        // Validate reserved control-key grammar before any later body read can
        // mask corruption already present in this authoritative inventory.
        NamespaceInventory::from_listed(namespace, listed.clone())
            .map_err(MarkReadFailure::NamespaceList)?;
        let candidates = load_candidate_ledger(store, namespace)
            .await
            .map_err(MarkReadFailure::CandidateLedger)?;
        let (manifest, manifest_version) = Manifest::read_versioned(store, namespace)
            .await
            .map_err(MarkReadFailure::Manifest)?
            .ok_or(MarkReadFailure::ManifestMissing)?;
        let staging =
            active_staging_observation_at_with_mode(store, namespace, now, GcReadMode::Sequential)
                .await
                .map_err(MarkReadFailure::Staging)?;
        Ok(MarkReadInputs {
            listed_objects: listed,
            persisted_ledger: candidates,
            manifest,
            manifest_version,
            staging,
        })
    }
}

/// Loads mark inputs from an inventory, optionally reusing an earlier read.
///
/// `observed_manifest` must have been proven current against this exact
/// `inventory`. When supplied, the live-manifest GET is skipped: reading it
/// again would be required to return the same bytes under the same LIST ETag,
/// so the read carries no information.
///
/// The observation is boxed because it is held across the awaits of a deeply
/// nested GC call graph, where an inline `Manifest` grows every enclosing
/// future and overflows the stack.
async fn load_mark_read_inputs_from_inventory(
    store: &ZeppelinStore,
    namespace: &str,
    now: DateTime<Utc>,
    inventory: &NamespaceInventory,
    observed_manifest: Option<Box<(Manifest, ManifestVersion)>>,
) -> std::result::Result<MarkReadInputs, MarkReadFailure> {
    let (candidates, manifest, staging) = tokio::join!(
        load_candidates_from_inventory(store, namespace, inventory, false),
        async {
            match observed_manifest {
                Some(observed) => Ok(Some(*observed)),
                None => read_versioned_manifest_from_inventory(store, namespace, inventory).await,
            }
        },
        active_staging_from_inventory(store, namespace, now, inventory, false),
    );
    assemble_mark_read_inputs(Ok(inventory.all_objects()), candidates, manifest, staging)
}

/// Loads mark inputs sequentially, optionally reusing an earlier read.
///
/// `observed_manifest` carries the same currency proof required by
/// [`load_mark_read_inputs_from_inventory`]: it must have been shown current
/// against this exact `inventory`.
async fn load_sequential_mark_reads_from_inventory(
    store: &ZeppelinStore,
    namespace: &str,
    now: DateTime<Utc>,
    inventory: &NamespaceInventory,
    observed_manifest: Option<Box<(Manifest, ManifestVersion)>>,
) -> std::result::Result<MarkReadInputs, MarkReadFailure> {
    let candidates = load_candidate_ledger(store, namespace)
        .await
        .map_err(MarkReadFailure::CandidateLedger)?;
    let (manifest, manifest_version) = match observed_manifest {
        Some(observed) => *observed,
        None => Manifest::read_versioned(store, namespace)
            .await
            .map_err(MarkReadFailure::Manifest)?
            .ok_or(MarkReadFailure::ManifestMissing)?,
    };
    let staging =
        active_staging_observation_at_with_mode(store, namespace, now, GcReadMode::Sequential)
            .await
            .map_err(MarkReadFailure::Staging)?;
    Ok(MarkReadInputs {
        listed_objects: inventory.all_objects(),
        persisted_ledger: candidates,
        manifest,
        manifest_version,
        staging,
    })
}

struct SweepReadInputs {
    manifest: Manifest,
    manifest_version: ManifestVersion,
    staging: ActiveStagingObservation,
}

enum SweepReadFailure {
    ManifestMissing,
    Manifest(ZeppelinError),
    Staging(ZeppelinError),
}

fn assemble_sweep_read_inputs(
    manifest: Result<Option<(Manifest, ManifestVersion)>>,
    staging: Result<ActiveStagingObservation>,
) -> std::result::Result<SweepReadInputs, SweepReadFailure> {
    let (manifest, manifest_version) = manifest
        .map_err(SweepReadFailure::Manifest)?
        .ok_or(SweepReadFailure::ManifestMissing)?;
    let staging = staging.map_err(SweepReadFailure::Staging)?;
    Ok(SweepReadInputs {
        manifest,
        manifest_version,
        staging,
    })
}

async fn load_sweep_read_inputs(
    store: &ZeppelinStore,
    namespace: &str,
    now: DateTime<Utc>,
    read_mode: GcReadMode,
) -> std::result::Result<SweepReadInputs, SweepReadFailure> {
    if read_mode.is_bounded() {
        let (manifest, staging) = tokio::join!(
            Manifest::read_versioned(store, namespace),
            active_staging_observation_at_with_mode(store, namespace, now, GcReadMode::WarmBounded,),
        );
        assemble_sweep_read_inputs(manifest, staging)
    } else {
        let (manifest, manifest_version) = Manifest::read_versioned(store, namespace)
            .await
            .map_err(SweepReadFailure::Manifest)?
            .ok_or(SweepReadFailure::ManifestMissing)?;
        let staging =
            active_staging_observation_at_with_mode(store, namespace, now, GcReadMode::Sequential)
                .await
                .map_err(SweepReadFailure::Staging)?;
        Ok(SweepReadInputs {
            manifest,
            manifest_version,
            staging,
        })
    }
}

async fn load_sweep_read_inputs_from_inventory(
    store: &ZeppelinStore,
    namespace: &str,
    now: DateTime<Utc>,
    inventory: &NamespaceInventory,
    require_etag: bool,
) -> std::result::Result<SweepReadInputs, SweepReadFailure> {
    let (manifest, staging) = tokio::join!(
        read_versioned_manifest_from_inventory(store, namespace, inventory),
        active_staging_from_inventory(store, namespace, now, inventory, require_etag),
    );
    assemble_sweep_read_inputs(manifest, staging)
}

/// Runs one stateless GC cycle using the current wall clock.
pub async fn run_gc_cycle(
    store: &ZeppelinStore,
    namespace: &str,
    gc: &GcConfig,
) -> Result<GcCycleReport> {
    run_gc_cycle_at(store, namespace, gc, Utc::now()).await
}

/// Runs one complete garbage-collection cycle at an explicit wall time.
pub async fn run_gc_cycle_at(
    store: &ZeppelinStore,
    namespace: &str,
    gc: &GcConfig,
    now: DateTime<Utc>,
) -> Result<GcCycleReport> {
    Ok(run_gc_cycle_at_inner(store, namespace, gc, now, None, None)
        .await?
        .report)
}

async fn run_gc_cycle_at_inner(
    store: &ZeppelinStore,
    namespace: &str,
    gc: &GcConfig,
    now: DateTime<Utc>,
    prior_history: Option<&NamespaceGcMemo>,
    initial_inventory: Option<NamespaceInventory>,
) -> Result<GcCycleOutcome> {
    let mut initial_inventory = initial_inventory;
    let cycle_opening_inventory_objects = initial_inventory
        .as_ref()
        .map(|inventory| inventory.objects.clone());
    let live_roots =
        match load_live_root_observation(store, namespace, initial_inventory.as_ref()).await {
            Ok(Some(observation)) => observation,
            Ok(None) => {
                warn!(namespace, "gc manifest missing before root observation");
                return Ok(GcCycleOutcome::incomplete(GcCycleReport::default()));
            }
            Err(error @ ZeppelinError::MalformedControlKey { .. })
            | Err(error @ ZeppelinError::Branch(_)) => return Err(error),
            Err(error) => {
                warn!(namespace, error = %error, "gc live branch-root observation failed");
                return Ok(GcCycleOutcome::incomplete(GcCycleReport::default()));
            }
        };
    let prior_entries = prior_history.map(|memo| &memo.history);
    let retention = ManifestHistoryRetention {
        keep_count: gc.manifest_history_keep_count,
        pitr_retention_secs: gc.pitr_retention_secs,
        skew_slop_secs: gc.skew_slop_secs,
    };
    let history_prune = match initial_inventory.as_mut() {
        Some(inventory) => {
            prune_history_with_inventory_at(
                store,
                namespace,
                retention,
                now,
                prior_entries,
                inventory,
                &live_roots,
            )
            .await
        }
        None => {
            prune_history_with_memo_at(store, namespace, retention, now, prior_entries, &live_roots)
                .await
        }
    };
    let history_prune = match history_prune {
        Ok(result) => result,
        Err(error @ ZeppelinError::MalformedControlKey { .. })
        | Err(error @ ZeppelinError::Branch(_)) => return Err(error),
        Err(e) => {
            warn!(
                namespace,
                error = %e,
                "gc manifest-history prune failed; aborting cycle"
            );
            return Ok(GcCycleOutcome::incomplete(GcCycleReport::default()));
        }
    };
    let MemoizedHistoryPruneResult {
        result: history_prune,
        retained_history_observations: prune_history_observations,
        snapshot_observations: prune_snapshot_observations,
    } = history_prune;
    let manifest_history_pruned = history_prune.pruned;
    let prune_reachable =
        reachable_keys_from_manifests(namespace, &history_prune.retained_manifests)?;
    if prior_entries.is_none() && initial_inventory.is_none() {
        let prefix = format!("{namespace}/");
        let listed = match store.list_prefix_meta(&prefix).await {
            Ok(listed) => listed,
            Err(error) => {
                warn!(
                    namespace,
                    error = %error,
                    "gc cold namespace inventory failed before pending-delete drain"
                );
                return Ok(GcCycleOutcome::incomplete(GcCycleReport::default()));
            }
        };
        initial_inventory = Some(NamespaceInventory::from_listed(namespace, listed)?);
    }
    let force_candidate_phase = prior_history.is_some_and(|memo| memo.candidate_phase_due);
    let (retained_history, pending_outcome, pending_history_snapshots, pending_predelete_inventory) =
        if force_candidate_phase {
            debug!(
                namespace,
                "gc candidate phase is due; deferring pending-delete drain for one cycle"
            );
            (
                prune_reachable,
                PendingDeleteDrainOutcome::new(PendingDeleteDrainReport::default(), true, None),
                Vec::new(),
                None,
            )
        } else if let Some(prior_entries) = prior_entries {
            let prepared = match prepare_warm_pending_delete_drain(
                store,
                namespace,
                gc,
                &prune_reachable,
                prior_entries,
                now,
                &live_roots,
            )
            .await
            {
                Ok(prepared) => prepared,
                Err(error @ ZeppelinError::MalformedControlKey { .. }) => return Err(error),
                Err(e) => {
                    warn!(
                        namespace,
                        error = %e,
                        "gc pending-delete preparation failed; aborting cycle"
                    );
                    return Ok(GcCycleOutcome::incomplete(GcCycleReport::default()));
                }
            };
            (
                prune_reachable,
                prepared.outcome,
                prepared.refreshed_history,
                prepared.predelete_inventory,
            )
        } else {
            let retained_history_snapshot = match load_history_snapshot(store, namespace, None)
                .await
            {
                Ok(snapshot) => snapshot,
                Err(error @ ZeppelinError::MalformedControlKey { .. }) => return Err(error),
                Err(e) => {
                    warn!(
                        namespace,
                        error = %e,
                        "gc retained history re-read failed before pending-delete drain; aborting cycle"
                    );
                    return Ok(GcCycleOutcome::incomplete(GcCycleReport::default()));
                }
            };
            let retained_history =
                history_snapshot_reachable_keys(namespace, &retained_history_snapshot)?;
            let (pending_outcome, mut retry_history_snapshots) =
                match drain_pending_deletes_with_retained_history_from(
                    store,
                    namespace,
                    gc,
                    &retained_history,
                    now,
                    None,
                    None,
                    Some(&live_roots),
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(error @ ZeppelinError::MalformedControlKey { .. }) => return Err(error),
                    Err(e) => {
                        warn!(
                            namespace,
                            error = %e,
                            "gc pending-delete drain failed; aborting cycle"
                        );
                        return Ok(GcCycleOutcome::incomplete(GcCycleReport::default()));
                    }
                };
            let mut pending_history_snapshots = vec![retained_history_snapshot];
            pending_history_snapshots.append(&mut retry_history_snapshots);
            (
                retained_history,
                pending_outcome,
                pending_history_snapshots,
                None,
            )
        };
    let PendingDeleteDrainOutcome {
        report: mut pending_report,
        complete: pending_complete,
        observed_pending_deletes,
    } = pending_outcome;
    let mut base_report = GcCycleReport {
        objects_deleted: pending_report.objects_deleted,
        pending_deletes_deleted: pending_report.objects_deleted,
        pending_deletes_pruned: pending_report.entries_pruned,
        pending_deletes_retained: pending_report.entries_retained,
        ..GcCycleReport::default()
    };
    if prior_entries.is_none()
        && (pending_report.objects_deleted > 0 || pending_report.entries_pruned > 0)
    {
        // The early cold inventory was validated before pending work. Once
        // that protocol mutates objects or the live manifest, mark must relist
        // instead of treating the now-stale inventory as current authority.
        initial_inventory = None;
    }
    let mut inventory_is_predelete = false;
    if let Some(predelete_inventory) = pending_predelete_inventory {
        if pending_report.objects_deleted > 0 || pending_report.entries_pruned > 0 {
            debug!(
                namespace,
                "gc pending-delete authority mutated storage; deferring candidate mark/sweep to the next cycle"
            );
            return Ok(GcCycleOutcome::incomplete_with_candidate_phase(base_report));
        }
        initial_inventory = Some(predelete_inventory);
        inventory_is_predelete = true;
    }

    let read_mode = if prior_entries.is_some() {
        GcReadMode::WarmBounded
    } else {
        GcReadMode::Sequential
    };
    let mark_read_from_inventory = initial_inventory.is_some();
    // The opening branch-root observation already read the live manifest and
    // proved its bytes against a LIST ETag. When mark reads an inventory that
    // still carries that same ETag, a second GET is constrained to return the
    // identical bytes, so reuse the observation rather than pay for it. A
    // changed ETag means the manifest moved and mark must read it again.
    let reusable_live_manifest = initial_inventory
        .as_ref()
        .filter(|inventory| live_roots.identity.matches_inventory(namespace, inventory))
        .map(|_| {
            Box::new((
                live_roots.manifest.clone(),
                live_roots.version.as_ref().clone(),
            ))
        });
    let mark_inputs = match initial_inventory.as_ref() {
        Some(inventory) if read_mode.is_bounded() => {
            load_mark_read_inputs_from_inventory(
                store,
                namespace,
                now,
                inventory,
                reusable_live_manifest,
            )
            .await
        }
        Some(inventory) => {
            load_sequential_mark_reads_from_inventory(
                store,
                namespace,
                now,
                inventory,
                reusable_live_manifest,
            )
            .await
        }
        None => load_mark_read_inputs(store, namespace, now, read_mode).await,
    };
    let MarkReadInputs {
        listed_objects,
        persisted_ledger,
        manifest: mark_manifest,
        manifest_version: mark_manifest_version,
        staging: mark_staging,
    } = match mark_inputs {
        Ok(inputs) => inputs,
        Err(MarkReadFailure::NamespaceList(error @ ZeppelinError::MalformedControlKey { .. })) => {
            return Err(error)
        }
        Err(MarkReadFailure::NamespaceList(e)) => {
            warn!(namespace, error = %e, "gc listing failed; aborting cycle");
            return Ok(GcCycleOutcome::incomplete(base_report));
        }
        Err(MarkReadFailure::CandidateLedger(e)) => {
            warn!(namespace, error = %e, "gc candidate load failed; aborting cycle");
            return Ok(GcCycleOutcome::incomplete(base_report));
        }
        Err(MarkReadFailure::ManifestMissing) => {
            warn!(namespace, "gc manifest missing; skipping namespace");
            return Ok(GcCycleOutcome::incomplete(base_report));
        }
        Err(MarkReadFailure::Manifest(e)) => {
            warn!(namespace, error = %e, "gc manifest read failed; aborting cycle");
            return Ok(GcCycleOutcome::incomplete(base_report));
        }
        Err(MarkReadFailure::Staging(e)) => {
            warn!(namespace, error = %e, "gc active staging read failed; aborting cycle");
            return Ok(GcCycleOutcome::incomplete(base_report));
        }
    };
    let mark_live_roots = match LiveRootObservation::from_authority(
        namespace,
        mark_manifest.clone(),
        mark_manifest_version,
    ) {
        Ok(observation) => observation,
        Err(error @ ZeppelinError::Branch(_)) => return Err(error),
        Err(error) => {
            warn!(namespace, error = %error, "gc mark root revalidation failed");
            return Ok(GcCycleOutcome::incomplete(base_report));
        }
    };
    if mark_live_roots.identity != live_roots.identity {
        warn!(
            namespace,
            "gc live branch-root observation changed before mark"
        );
        return Ok(GcCycleOutcome::incomplete(base_report));
    }
    let persisted_is_canonical = persisted_ledger.is_canonical();
    let persisted = persisted_ledger.candidates;
    if force_candidate_phase {
        let retained = mark_manifest
            .pending_deletes
            .iter()
            .collect::<BTreeSet<_>>()
            .len();
        pending_report.entries_retained = retained;
        base_report.pending_deletes_retained = retained;
    }
    let listed_keys = listed_objects
        .iter()
        .map(|object| object.key.clone())
        .collect::<BTreeSet<_>>();
    let mut completed_inventory = match initial_inventory.take() {
        Some(inventory) => Some(inventory),
        None => match NamespaceInventory::from_listed(namespace, listed_objects) {
            Ok(inventory) => Some(inventory),
            Err(error) => return Err(error),
        },
    };
    let mut mark_reachable = reachable_keys_with_retained_history_and_staging_keys(
        namespace,
        &mark_manifest,
        &mark_staging.keys,
        &retained_history,
    )?;
    extend_scoped_artifact_roots(
        namespace,
        &mark_manifest,
        &retained_history,
        &listed_keys,
        &mut mark_reachable,
    );
    let unknown_shape_skips =
        listed_keys
            .iter()
            .try_fold(0_usize, |count, key| -> Result<usize> {
                if mark_reachable.contains(key) {
                    return Ok(count);
                }
                let owned = NamespaceObjectKey::classify(namespace, key.clone())?;
                if !owned.allows_deferred_delete()
                    || parse_gc_artifact_key(namespace, key).is_some()
                {
                    return Ok(count);
                }
                log_gc_skip(namespace, key, SkipReason::UnknownShape);
                Ok(count + 1)
            })?;
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

    let candidate_ledger_key = gc_candidate_store_key(namespace);
    let mark_put_required = !read_mode.is_bounded()
        || !mark_read_from_inventory
        || !persisted_is_canonical
        || persisted != marked_candidates;
    if mark_put_required {
        match save_gc_candidates_with_version(store, namespace, &marked_candidates).await {
            Ok((size, version)) => {
                if let Some(inventory) = completed_inventory.as_mut() {
                    inventory.upsert(candidate_ledger_key.clone(), size, version);
                }
            }
            Err(e) => {
                warn!(namespace, error = %e, "gc candidate mark persist failed; skipping sweep");
                let mut report = base_report;
                report.candidates_marked = 0;
                report.candidates_skipped = marked_candidates.len();
                return Ok(GcCycleOutcome::incomplete(report));
            }
        }
    } else {
        debug!(
            namespace,
            "gc candidate mark already canonical and unchanged; skipping persist"
        );
    }
    let durable_mark = marked_candidates.clone();
    let durable_mark_identity = completed_inventory
        .as_ref()
        .and_then(|inventory| inventory.object(&candidate_ledger_key))
        .cloned();
    crate::metrics::GC_CANDIDATES_MARKED_TOTAL
        .with_label_values(&[namespace])
        .inc_by(candidates_marked as u64);

    let mark_oldest_inflight_ms = oldest_inflight_ulid_ms(namespace, &mark_staging.keys);
    let candidate_may_delete = marked_candidates.iter().any(|candidate| {
        listed_keys.contains(&candidate.key)
            && matches!(
                should_delete_candidate(
                    namespace,
                    candidate,
                    &mark_reachable,
                    DeletePredicateContext {
                        horizon_secs: gc.horizon_secs,
                        now,
                        oldest_inflight_ulid_ms: mark_oldest_inflight_ms,
                        current_manifest_version: mark_manifest.version(),
                        min_newer_manifest_versions: None,
                    },
                ),
                DeleteDecision::Delete
            )
    });
    let mark_inventory_objects = cycle_opening_inventory_objects.or_else(|| {
        completed_inventory
            .as_ref()
            .map(|inventory| inventory.objects.clone())
    });
    let mut used_fresh_inventory = false;
    let mut unaccounted_artifact_drift = false;
    if candidate_may_delete && !inventory_is_predelete {
        let prefix = format!("{namespace}/");
        let listed = match store.list_prefix_meta(&prefix).await {
            Ok(listed) => listed,
            Err(e) => {
                warn!(namespace, error = %e, "gc fresh pre-delete inventory failed");
                let mut report = base_report;
                report.candidates_marked = candidates_marked;
                report.candidates_skipped = unknown_shape_skips + marked_candidates.len();
                return Ok(GcCycleOutcome::incomplete(report));
            }
        };
        let fresh = match NamespaceInventory::from_listed(namespace, listed) {
            Ok(inventory) => inventory,
            Err(error) => return Err(error),
        };
        unaccounted_artifact_drift = fresh.objects.values().any(|object| {
            let Some(previous) = mark_inventory_objects
                .as_ref()
                .and_then(|objects| objects.get(&object.key))
            else {
                return parse_gc_artifact_key(namespace, &object.key).is_some();
            };
            parse_gc_artifact_key(namespace, &object.key).is_some()
                && !listed_object_identity_matches(previous, object)
        });
        completed_inventory = Some(fresh);
        used_fresh_inventory = true;
    } else if candidate_may_delete && inventory_is_predelete {
        used_fresh_inventory = true;
    }

    if candidate_may_delete
        && !durable_mark_identity
            .as_ref()
            .zip(
                completed_inventory
                    .as_ref()
                    .and_then(|inventory| inventory.object(&candidate_ledger_key)),
            )
            .is_some_and(|(before, after)| listed_object_identity_matches(before, after))
    {
        warn!(
            namespace,
            "gc candidate ledger changed after durable mark; skipping sweep"
        );
        let mut report = base_report;
        report.candidates_marked = candidates_marked;
        report.candidates_skipped = unknown_shape_skips + marked_candidates.len();
        return Ok(GcCycleOutcome::incomplete(report));
    }

    let sweep_inputs = match completed_inventory
        .as_ref()
        .filter(|_| read_mode.is_bounded())
    {
        Some(inventory) => {
            load_sweep_read_inputs_from_inventory(
                store,
                namespace,
                now,
                inventory,
                used_fresh_inventory,
            )
            .await
        }
        None => load_sweep_read_inputs(store, namespace, now, read_mode).await,
    };
    let SweepReadInputs {
        manifest: sweep_manifest,
        manifest_version: sweep_manifest_version,
        staging: sweep_staging,
    } = match sweep_inputs {
        Ok(inputs) => inputs,
        Err(SweepReadFailure::ManifestMissing) => {
            warn!(
                namespace,
                "gc manifest missing before sweep; skipping deletes"
            );
            let mut report = base_report;
            report.candidates_marked = candidates_marked;
            report.candidates_skipped = unknown_shape_skips;
            return Ok(GcCycleOutcome::incomplete(report));
        }
        Err(SweepReadFailure::Manifest(e)) => {
            warn!(namespace, error = %e, "gc manifest re-read failed; skipping deletes");
            let mut report = base_report;
            report.candidates_marked = candidates_marked;
            report.candidates_skipped = unknown_shape_skips;
            return Ok(GcCycleOutcome::incomplete(report));
        }
        Err(SweepReadFailure::Staging(e)) => {
            warn!(namespace, error = %e, "gc active staging re-read failed; skipping sweep");
            let mut report = base_report;
            report.candidates_marked = candidates_marked;
            report.candidates_skipped = unknown_shape_skips;
            return Ok(GcCycleOutcome::incomplete(report));
        }
    };
    let sweep_live_roots = match LiveRootObservation::from_authority(
        namespace,
        sweep_manifest.clone(),
        sweep_manifest_version,
    ) {
        Ok(observation) => observation,
        Err(error @ ZeppelinError::Branch(_)) => return Err(error),
        Err(error) => {
            warn!(namespace, error = %error, "gc sweep root revalidation failed");
            let mut report = base_report;
            report.candidates_marked = candidates_marked;
            report.candidates_skipped = unknown_shape_skips;
            return Ok(GcCycleOutcome::incomplete(report));
        }
    };
    if sweep_live_roots.identity != live_roots.identity {
        warn!(
            namespace,
            "gc live branch-root observation changed before sweep"
        );
        let mut report = base_report;
        report.candidates_marked = candidates_marked;
        report.candidates_skipped = unknown_shape_skips;
        return Ok(GcCycleOutcome::incomplete(report));
    }
    let sweep_history = match completed_inventory
        .as_ref()
        .filter(|_| read_mode.is_bounded())
    {
        Some(inventory) => match inventory.history_observations(namespace) {
            Ok(observations) => {
                if used_fresh_inventory
                    && observations.iter().any(|observation| {
                        !matches!(observation.storage_version, Some(StorageVersion::Etag(_)))
                    })
                {
                    Err(ZeppelinError::Serialization(format!(
                        "namespace {namespace} history lacks ETags for pre-delete validation"
                    )))
                } else {
                    load_history_snapshot_from_observations(
                        store,
                        namespace,
                        observations,
                        prior_entries,
                    )
                    .await
                }
            }
            Err(error) => Err(error),
        },
        None => load_history_snapshot(store, namespace, prior_entries).await,
    };
    let sweep_history_snapshot = match sweep_history {
        Ok(snapshot) => snapshot,
        Err(error @ ZeppelinError::MalformedControlKey { .. }) => return Err(error),
        Err(e) => {
            warn!(namespace, error = %e, "gc retained history re-read failed; skipping sweep");
            let mut report = base_report;
            report.candidates_marked = candidates_marked;
            report.candidates_skipped = unknown_shape_skips;
            return Ok(GcCycleOutcome::incomplete(report));
        }
    };
    validate_rooted_history_snapshot(&sweep_live_roots, &sweep_history_snapshot)?;
    let sweep_retained_history =
        history_snapshot_reachable_keys(namespace, &sweep_history_snapshot)?;
    let sweep_listed_keys = completed_inventory
        .as_ref()
        .map_or_else(|| listed_keys.clone(), NamespaceInventory::all_keys);
    let mut sweep_reachable = reachable_keys_with_retained_history_and_staging_keys(
        namespace,
        &sweep_manifest,
        &sweep_staging.keys,
        &sweep_retained_history,
    )?;
    extend_scoped_artifact_roots(
        namespace,
        &sweep_manifest,
        &sweep_retained_history,
        &sweep_listed_keys,
        &mut sweep_reachable,
    );
    let oldest_inflight_ms = oldest_inflight_ulid_ms(namespace, &sweep_staging.keys);
    let known_sizes = known_reclaimable_sizes(namespace, &sweep_manifest)?;

    let mut retained_with_order = Vec::new();
    let mut deletable = Vec::new();
    let mut objects_deleted = pending_report.objects_deleted;
    let mut bytes_reclaimed = 0u64;
    let mut candidates_skipped = unknown_shape_skips;
    let mut cycle_complete = pending_complete;

    for (order, candidate) in marked_candidates.into_iter().enumerate() {
        if !listed_keys.contains(&candidate.key) {
            log_gc_skip(namespace, &candidate.key, SkipReason::NotListedThisCycle);
            candidates_skipped += 1;
            continue;
        }
        if !sweep_listed_keys.contains(&candidate.key) {
            log_gc_skip(namespace, &candidate.key, SkipReason::NotListedThisCycle);
            candidates_skipped += 1;
            continue;
        }
        if used_fresh_inventory
            && !mark_inventory_objects
                .as_ref()
                .and_then(|objects| objects.get(&candidate.key))
                .zip(
                    completed_inventory
                        .as_ref()
                        .and_then(|inventory| inventory.object(&candidate.key)),
                )
                .is_some_and(|(before, after)| listed_object_identity_matches(before, after))
        {
            cycle_complete = false;
            log_gc_skip(namespace, &candidate.key, SkipReason::ObjectIdentityChanged);
            candidates_skipped += 1;
            retained_with_order.push((
                order,
                GcCandidate {
                    key: candidate.key,
                    first_seen_unreachable_at: now,
                    unreachable_since_manifest_version: sweep_manifest.version(),
                },
            ));
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
            DeleteDecision::Delete
                if !completed_inventory
                    .as_ref()
                    .and_then(|inventory| inventory.object(&candidate.key))
                    .is_some_and(|object| {
                        listed_object_horizon_satisfied(object, now, gc.horizon_secs)
                    }) =>
            {
                log_gc_skip(
                    namespace,
                    &candidate.key,
                    SkipReason::ObjectModifiedTooRecently,
                );
                candidates_skipped += 1;
                retained_with_order.push((order, candidate));
            }
            DeleteDecision::Delete => deletable.push((order, candidate)),
            DeleteDecision::Skip(reason) => {
                log_gc_skip(namespace, &candidate.key, reason);
                candidates_skipped += 1;
                retained_with_order.push((order, candidate));
            }
        }
    }

    let deletable = deletable
        .into_iter()
        .map(|(order, candidate)| {
            let key = TargetOwnedDeletionKey::classify(namespace, candidate.key.clone())?;
            Ok((order, candidate, key))
        })
        .collect::<Result<Vec<_>>>()?;
    for batch in deletable.chunks(DELETE_MANY_MAX_KEYS) {
        revalidate_live_root_observation(store, namespace, &sweep_live_roots).await?;
        let keys = batch
            .iter()
            .map(|(_, _, key)| key.clone())
            .collect::<Vec<_>>();
        match delete_target_owned_many(store, &keys).await {
            Ok(deleted) => {
                debug_assert_eq!(deleted, batch.len());
                for (_, candidate, _) in batch {
                    if let Some(inventory) = completed_inventory.as_mut() {
                        inventory.remove(&candidate.key);
                    }
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
                        "gc accepted unreachable-object delete completion"
                    );
                }
            }
            Err(error) => {
                cycle_complete = false;
                warn!(
                    namespace,
                    batch_size = batch.len(),
                    error = %error,
                    "gc sweep batch uncertain; retaining every candidate"
                );
                for (order, candidate, _) in batch {
                    log_gc_skip(namespace, &candidate.key, SkipReason::DeleteFailed);
                    candidates_skipped += 1;
                    retained_with_order.push((*order, candidate.clone()));
                }
            }
        }
    }
    retained_with_order.sort_by_key(|(order, _)| *order);
    let retained = retained_with_order
        .into_iter()
        .map(|(_, candidate)| candidate)
        .collect::<Vec<_>>();

    let cleanup_put_required = !read_mode.is_bounded() || retained != durable_mark;
    if cleanup_put_required {
        match save_gc_candidates_with_version(store, namespace, &retained).await {
            Ok((size, version)) => {
                if let Some(inventory) = completed_inventory.as_mut() {
                    inventory.upsert(candidate_ledger_key, size, version);
                }
            }
            Err(e) => {
                cycle_complete = false;
                warn!(
                    namespace,
                    error = %e,
                    "gc candidate cleanup persist failed after sweep"
                );
            }
        }
    } else {
        debug!(
            namespace,
            "gc candidate cleanup equals durable mark; skipping persist"
        );
    }

    let pending_history_stable = pending_history_snapshots
        .iter()
        .all(|snapshot| history_observations_match_snapshot(&prune_history_observations, snapshot));
    let history_inputs_stable = pending_history_stable
        && history_observations_match_snapshot(
            &prune_history_observations,
            &sweep_history_snapshot,
        );
    let snapshot_prefix = NamedSnapshot::prefix(namespace);
    let snapshot_inputs_stable = completed_inventory.as_ref().is_some_and(|inventory| {
        inventory.matches_listed_prefix(
            &snapshot_prefix,
            prune_snapshot_observations
                .iter()
                .map(|observation| &observation.object),
        )
    });
    let pending_inputs_stable = force_candidate_phase
        || observed_pending_deletes
            .as_ref()
            .is_some_and(|pending| pending == &sweep_manifest.pending_deletes);
    if !(history_inputs_stable && snapshot_inputs_stable && pending_inputs_stable) {
        debug!(
            namespace,
            history_inputs_stable,
            snapshot_inputs_stable,
            pending_inputs_stable,
            "gc decision inputs changed during cycle; disabling next idle admission"
        );
        completed_inventory = None;
    }
    if unaccounted_artifact_drift {
        debug!(
            namespace,
            "gc fresh inventory contained unmarked artifact drift; disabling next idle admission"
        );
        completed_inventory = None;
    }

    info!(
        namespace,
        candidates_marked,
        objects_deleted,
        pending_deletes_deleted = pending_report.objects_deleted,
        pending_deletes_pruned = pending_report.entries_pruned,
        pending_deletes_retained = pending_report.entries_retained,
        manifest_history_pruned,
        bytes_reclaimed,
        candidates_skipped,
        cycle_complete,
        "gc cycle finished"
    );

    let report = GcCycleReport {
        candidates_marked,
        objects_deleted,
        pending_deletes_deleted: pending_report.objects_deleted,
        pending_deletes_pruned: pending_report.entries_pruned,
        pending_deletes_retained: pending_report.entries_retained,
        bytes_reclaimed,
        candidates_skipped,
    };
    if cycle_complete {
        let next_due_at = match next_gc_deadline(
            namespace,
            gc,
            now,
            &retained,
            &sweep_manifest,
            &sweep_history_snapshot,
            &sweep_staging,
        ) {
            Ok(deadline) => deadline,
            Err(NextGcDeadlineError::Reachability(error)) => return Err(error),
            Err(NextGcDeadlineError::InvalidSchedule) => {
                completed_inventory = None;
                None
            }
        };
        Ok(GcCycleOutcome::complete(
            report,
            CompletedGcState {
                history: sweep_history_snapshot.cacheable,
                inventory: completed_inventory.and_then(|inventory| inventory.fingerprint()),
                live_root_identity: sweep_live_roots.identity,
                next_due_at,
            },
        ))
    } else {
        Ok(GcCycleOutcome::incomplete(report))
    }
}

/// Records one fail-closed candidate skip in metrics and structured logs.
///
/// # Parameters
///
/// - `namespace`: Namespace metric label and log field.
/// - `key`: Exact object key that was not deleted.
/// - `reason`: Low-cardinality policy or failure classification.
///
/// # Side Effects
///
/// Increments `GC_CANDIDATES_SKIPPED_TOTAL` and emits a warning. It performs no
/// object-store operation and does not alter the candidate ledger itself.
///
/// # Examples
///
/// A newly recognized orphan logs `unreachable_horizon`; an unfamiliar object
/// under the namespace logs `unknown_shape` and remains untouched.
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

/// Applies every fail-closed safety predicate to one marked candidate.
///
/// Predicates are ordered from structural/current authority through elapsed
/// time, optional manifest generations, artifact creation age, and the active
/// compaction watermark. Passing this pure function permits a DELETE attempt;
/// it does not perform the delete.
///
/// # Parameters
///
/// - `namespace`: Namespace required to parse the exact artifact key.
/// - `candidate`: Borrowed persisted mark being evaluated.
/// - `reachable`: Borrowed root union from fresh sweep snapshots.
/// - `context`: Copyable horizon, time, generation, and in-flight watermark.
///
/// # Returns
///
/// [`DeleteDecision::Delete`] only when all enabled guards pass; otherwise a
/// precise [`SkipReason`] identifying the first failed guard.
///
/// # Consistency
///
/// Wall-clock persistence is mandatory because cached readers do not announce
/// manifest epochs. The optional generation guard can strengthen but never
/// replace that horizon. The artifact's own ULID is a second age floor, stopping
/// an old ledger timestamp from authorizing deletion of a newly created object.
///
/// # Examples
///
/// A key marked ten seconds ago with a five-second horizon is still skipped if
/// the fresh manifest references it, if its artifact ULID is only one second
/// old, or if it is newer than the oldest active staged upload.
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

/// Finds the oldest creation timestamp among recognized active staged artifacts.
///
/// # Parameters
///
/// - `namespace`: Namespace used to parse staged artifact keys.
/// - `staged`: Borrowed exact keys protected by the active lease.
///
/// # Returns
///
/// The minimum embedded ULID timestamp in milliseconds, or `None` if no staged
/// key has a recognized GC artifact shape. Unknown staged keys remain reachable
/// through the root union but do not contribute a watermark.
///
/// # Examples
///
/// Staged objects created at milliseconds 100 and 120 produce `Some(100)`;
/// candidate artifacts newer than 100 are conservatively skipped.
fn oldest_inflight_ulid_ms(namespace: &str, staged: &BTreeSet<String>) -> Option<u64> {
    staged
        .iter()
        .filter_map(|key| parse_gc_artifact_key(namespace, key))
        .map(|artifact| artifact.ulid().timestamp_ms())
        .min()
}

/// Indexes artifact sizes that the supplied manifest records explicitly.
///
/// The manifest currently carries byte sizes for WAL fragments and coarse
/// sketches, not every segment sidecar. Missing entries intentionally yield
/// zero-byte accounting rather than guessing from a separate HEAD/GET.
///
/// # Parameters
///
/// - `namespace`: Namespace used to derive WAL keys.
/// - `manifest`: Borrowed snapshot containing known size metadata.
///
/// # Returns
///
/// An ordered key-to-byte-count map for WAL fragments and sketch objects whose
/// sizes are present in this snapshot.
///
/// # Performance
///
/// Performs no object-store I/O. Work is linear in fragments and segments.
///
/// # Examples
///
/// A 100-byte fragment and 512-byte sketch produce two entries; an attribute
/// sidecar has no entry and contributes zero to the cycle's lower-bound report.
fn known_reclaimable_sizes(namespace: &str, manifest: &Manifest) -> Result<BTreeMap<String, u64>> {
    let mut sizes = BTreeMap::new();
    let local_origin = local_artifact_origin_for_gc(namespace, manifest)?;
    let origins = local_origin
        .as_ref()
        .map(|local_origin| manifest.artifact_origin_resolver(local_origin))
        .transpose()?;
    for fragment in &manifest.fragments {
        let physical_namespace = match origins {
            Some(origins) => origins
                .locate_fragment(fragment)?
                .physical_origin
                .namespace(),
            None => namespace,
        };
        sizes.insert(
            WalFragment::s3_key(physical_namespace, &fragment.id),
            fragment.size_bytes,
        );
    }
    for segment in &manifest.segments {
        if let Some(sketch) = &segment.sketch {
            sizes.insert(sketch.key.clone(), sketch.size_bytes);
        }
    }
    Ok(sizes)
}

/// Extends exact manifest reachability with policy artifacts owned by a live segment.
///
/// Scoped ANN/BM25 artifacts are immutable derivatives whose descriptor cannot
/// be listed in the manifest without turning each policy slice into authority.
/// Their authority instead comes from the parent source segment: every listed
/// known-shape scope key is protected while that source segment is live or PITR
/// retained, and becomes ordinary GC input after the parent leaves all roots.
fn extend_scoped_artifact_roots(
    namespace: &str,
    manifest: &Manifest,
    retained_history: &BTreeSet<String>,
    listed_keys: &BTreeSet<String>,
    reachable: &mut BTreeSet<String>,
) {
    let mut parent_ids = manifest
        .segments
        .iter()
        .map(|segment| segment.id.clone())
        .collect::<BTreeSet<_>>();
    for key in retained_history {
        if let Some((segment_id, path, _)) = parse_segment_key(namespace, key) {
            if !path.contains('/')
                && (is_known_segment_artifact_name(path) || is_known_tree_node_name(path))
            {
                parent_ids.insert(segment_id.to_string());
            }
        }
    }
    for key in listed_keys {
        let Some(parent_id) = scoped_artifact_parent_segment_id(namespace, key) else {
            continue;
        };
        if parent_ids.contains(parent_id) {
            reachable.insert(key.clone());
        }
    }
}

/// Parses only immutable artifact key shapes that GC is allowed to delete.
///
/// Valid WAL keys are `<namespace>/wal/<ulid>.wal`. Valid segment keys live
/// directly beneath `<namespace>/segments/seg_<ulid>/` or in the explicit
/// `security_scopes` derivative grammar. Invalid ULIDs, arbitrary nested paths,
/// and maintenance/control objects are rejected.
///
/// # Parameters
///
/// - `namespace`: Exact namespace prefix expected at the beginning of the key.
/// - `key`: Full object key to classify.
///
/// # Returns
///
/// A typed artifact family and copied ULID for known shapes, or `None` for every
/// unrecognized key. `None` means retain, never "safe to delete."
///
/// # Examples
///
/// `catalog/wal/<ulid>.wal` and
/// `catalog/segments/seg_<ulid>/attrs_2.bin` parse. `catalog/lease.json`, a
/// nested `attrs_2.bin/extra`, or `segments/seg_human-name/...` does not.
///
/// # Rust Notes for Java/C Engineers
///
/// The `Option` return type forces callers to handle parse failure explicitly;
/// there is no null pointer or sentinel ULID. The `?` operators return `None`
/// from the helper as soon as a required path component is absent.
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

    let (_, path, ulid) = parse_segment_key(namespace, key)?;
    if (!path.contains('/')
        && (is_known_segment_artifact_name(path) || is_known_tree_node_name(path)))
        || is_known_scoped_artifact_path(path)
    {
        Some(ParsedGcArtifact::SegmentArtifact { ulid })
    } else {
        None
    }
}

fn parse_segment_key<'a>(namespace: &str, key: &'a str) -> Option<(&'a str, &'a str, Ulid)> {
    let segment_prefix = format!("{namespace}/segments/");
    let rest = key.strip_prefix(&segment_prefix)?;
    let (segment_id, path) = rest.split_once('/')?;
    let ulid_text = segment_id.strip_prefix("seg_")?;
    let ulid = Ulid::from_string(ulid_text).ok()?;
    Some((segment_id, path, ulid))
}

fn scoped_artifact_parent_segment_id<'a>(namespace: &str, key: &'a str) -> Option<&'a str> {
    let (segment_id, path, _) = parse_segment_key(namespace, key)?;
    is_known_scoped_artifact_path(path).then_some(segment_id)
}

fn is_known_scoped_artifact_path(path: &str) -> bool {
    let Some(rest) = path.strip_prefix("security_scopes/") else {
        return false;
    };
    if let Some(digest) = rest
        .strip_prefix("ann/")
        .and_then(|name| name.strip_suffix(".json"))
    {
        return is_sha256_hex(digest);
    }
    if let Some(digest) = rest
        .strip_prefix("fts/")
        .and_then(|name| name.strip_suffix(".bin"))
    {
        return is_sha256_hex(digest);
    }
    let Some((artifact_id, file_name)) = rest
        .strip_prefix("segments/")
        .and_then(|nested| nested.split_once('/'))
    else {
        return false;
    };
    if file_name.contains('/') {
        return false;
    }
    let Some(artifact_ulid) = artifact_id.strip_prefix("ann_") else {
        return false;
    };
    if Ulid::from_string(artifact_ulid).is_err() {
        return false;
    }
    is_known_segment_artifact_name(file_name) || is_known_tree_node_name(file_name)
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_known_tree_node_name(file_name: &str) -> bool {
    let Some(node_id) = file_name
        .strip_prefix("node_")
        .and_then(|name| name.strip_suffix(".bin"))
    else {
        return false;
    };
    if let Some(root_ulid) = node_id.strip_prefix("root_") {
        return Ulid::from_string(root_ulid).is_ok();
    }
    let Some((depth, node_ulid)) = node_id
        .strip_prefix("n_")
        .and_then(|rest| rest.split_once('_'))
    else {
        return false;
    };
    !depth.is_empty()
        && depth.bytes().all(|byte| byte.is_ascii_digit())
        && Ulid::from_string(node_ulid).is_ok()
}

/// Checks the allowlist of immutable files permitted directly under a segment.
///
/// # Parameters
///
/// - `file_name`: Final path component with no `/`, already separated from the
///   `seg_<ulid>` directory by the caller.
///
/// # Returns
///
/// `true` for fixed metadata names or decimal-numbered cluster/index sidecars;
/// `false` for unknown extensions, prefixes, or nested paths.
///
/// # Examples
///
/// `centroids.bin`, `cluster_group_12.bin`, and `fts_index_0.bin` are known;
/// `notes.txt` and `cluster_x.bin` are not.
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

/// Recognizes `<prefix><nonempty decimal digits>.bin` exactly.
///
/// # Parameters
///
/// - `file_name`: Candidate final path component.
/// - `prefix`: Required family prefix, such as `cluster_` or `bitmap_`.
///
/// # Returns
///
/// `true` only when at least one ASCII digit lies between the prefix and `.bin`.
///
/// # Examples
///
/// `numbered_bin("attrs_42.bin", "attrs_")` is true; `attrs_.bin`,
/// `attrs_-1.bin`, and `attrs_42.dat` are false.
fn numbered_bin(file_name: &str, prefix: &str) -> bool {
    let Some(number) = file_name
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_suffix(".bin"))
    else {
        return false;
    };
    !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
}

/// Builds candidates from the exact reachability delta across a manifest commit.
///
/// The returned keys are `reachable(old_manifest) - reachable(new_manifest)`.
/// This preserves carried-object correctness by construction: any object still
/// referenced by at least one live segment remains in the reachable union and
/// is not emitted. Crash-orphans that never entered a manifest are invisible to
/// this delta and remain the periodic LIST sweep's responsibility.
///
/// # Parameters
///
/// - `namespace`: Namespace used to derive exact keys in both snapshots.
/// - `old_manifest`: Borrowed manifest that was authoritative before the commit.
/// - `new_manifest`: Borrowed manifest successfully published by the commit.
/// - `commit_time`: Wall-clock time assigned to every key leaving reachability.
///
/// # Returns
///
/// Owned candidates in lexicographic key order, stamped with `commit_time` and
/// the new manifest's generation. An empty vector means no exact reference was
/// released.
///
/// # Errors
///
/// Returns an artifact-origin integrity error from either manifest instead of
/// computing a partial reachability delta.
///
/// # Consistency
///
/// Call this only for the old/new pair around a successful manifest CAS. The
/// output accelerates marking but remains non-authoritative and must be
/// revalidated before deletion.
///
/// # Performance
///
/// Expands both manifests into ordered sets, then performs a linear set
/// difference. It does no storage I/O.
///
/// # Examples
///
/// If a new segment carries cluster 0 from an old segment but replaces cluster
/// 1, only the old centroid, cluster-1, and cluster-1 sidecar keys enter the
/// delta. A never-published upload appears in neither manifest and is found only
/// by periodic LIST-based marking.
pub fn gc_candidates_from_manifest_delta(
    namespace: &str,
    old_manifest: &Manifest,
    new_manifest: &Manifest,
    commit_time: DateTime<Utc>,
) -> Result<Vec<GcCandidate>> {
    let old_reachable = reachable_keys(namespace, old_manifest)?;
    let new_reachable = reachable_keys(namespace, new_manifest)?;

    Ok(old_reachable
        .difference(&new_reachable)
        .map(|key| GcCandidate {
            key: key.clone(),
            first_seen_unreachable_at: commit_time,
            unreachable_since_manifest_version: new_manifest.version(),
        })
        .collect())
}

/// Keeps only candidates still unreachable in a supplied manifest snapshot.
///
/// Candidate state is an accelerator, not truth. The future delete stage must
/// call this shape of check after reading the authoritative manifest so a key
/// that became live again is skipped.
///
/// # Parameters
///
/// - `namespace`: Namespace used to expand exact manifest artifact keys.
/// - `manifest`: Borrowed freshly loaded manifest snapshot.
/// - `candidates`: Borrowed marks to filter without modifying the input slice.
///
/// # Returns
///
/// Owned clones of candidates absent from this manifest's reachable set, in
/// input order. It does not apply history, staging, age, LIST, or key-shape
/// guards and therefore is not a complete delete predicate.
///
/// # Errors
///
/// Returns an artifact-origin integrity error instead of treating a corrupt
/// manifest as proof that a candidate is unreachable.
///
/// # Examples
///
/// If candidates A and B are supplied and a fresh manifest references A again,
/// the result contains only B. Sweep code must still union retained history and
/// active staging before acting on B.
///
/// # Rust Notes for Java/C Engineers
///
/// Filtering borrows each entry and `.cloned()` creates owned output records.
/// The caller keeps its original slice; Rust prevents the returned vector from
/// containing dangling pointers into it.
pub fn revalidate_unreachable_candidates(
    namespace: &str,
    manifest: &Manifest,
    candidates: &[GcCandidate],
) -> Result<Vec<GcCandidate>> {
    let reachable = reachable_keys(namespace, manifest)?;
    Ok(candidates
        .iter()
        .filter(|candidate| !reachable.contains(&candidate.key))
        .cloned()
        .collect())
}

/// Writes the exact staged-key set for one compaction lease token.
///
/// Compaction calls this after listing its uploaded segment prefix and before
/// attempting manifest publication. The staging object pins those uploads for
/// GC but does not make them query-visible.
///
/// # Parameters
///
/// - `store`: Borrowed gateway used for one JSON PUT.
/// - `namespace`: Namespace owning the lease and uploads.
/// - `fencing_token`: Token of the lease holder publishing this staging root.
/// - `keys`: Owned, duplicate-free exact upload keys moved into the record.
///
/// # Returns
///
/// `Ok(())` after storage accepts the staging object.
///
/// # Errors
///
/// Propagates JSON serialization, invalid-key, and PUT failures. The caller
/// must not assume GC protection was published after an error.
///
/// # Side Effects
///
/// Unconditionally replaces `<namespace>/_staging/<token>.json` with the
/// complete set. It does not write the live manifest.
///
/// # Performance
///
/// Serializes the set to pretty JSON and performs one full-object PUT.
///
/// # Examples
///
/// A compactor uploads centroids and two cluster groups, then writes all three
/// keys here. GC protects them while this token remains the active lease even
/// though readers still use the old manifest.
///
/// # Rust Notes for Java/C Engineers
///
/// `keys` is moved into [`CompactionStaging`], so the caller cannot accidentally
/// keep mutating the exact set being serialized. Java would pass a mutable
/// object reference; C would require a documented ownership transfer.
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
        .map(|_| ())
}

/// Clears one lease token's staging record idempotently.
///
/// # Parameters
///
/// - `store`: Borrowed gateway used for the DELETE.
/// - `namespace`: Namespace owning the staging object.
/// - `fencing_token`: Token encoded in the staging key to remove.
///
/// # Returns
///
/// `Ok(())` when the object is deleted or was already absent.
///
/// # Errors
///
/// Propagates invalid-key and non-`NotFound` storage DELETE errors. A failure may
/// leave a stale staging object, but [`active_staged_keys`] ignores it once the
/// token no longer matches an unexpired lease.
///
/// # Side Effects
///
/// Performs one object-store DELETE attempt and does not change the manifest.
///
/// # Examples
///
/// Cleanup after successful publication removes token 17's record. A retry
/// after it was already removed also succeeds.
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

/// Returns staged uploads owned by the currently active, unexpired lease.
///
/// The function reads the authoritative lease object first. A missing or
/// expired lease returns an empty set without scanning staging. With an active
/// lease, all staging records are listed and decoded, but only records whose
/// embedded fencing token equals the lease token contribute keys. Stale records
/// may remain in storage, but they cannot pin uploads forever.
///
/// # Parameters
///
/// - `store`: Borrowed gateway used for the lease GET, staging LIST, and record
///   GETs.
/// - `namespace`: Namespace whose lease and staging prefix are inspected.
///
/// # Returns
///
/// An ordered, duplicate-free owned set of keys for the observed active token.
/// Missing or expired lease state returns an empty set.
///
/// # Errors
///
/// Propagates lease GET failures other than `NotFound`, malformed lease JSON,
/// staging LIST/GET failures, and malformed staging JSON. One bad stale record
/// still aborts the scan because records are decoded before token comparison;
/// callers fail closed and skip sweeping rather than use a partial set.
///
/// # Consistency
///
/// Lease, LIST, and record GETs are separate object-store operations, not one
/// atomic snapshot. The returned set corresponds to the unexpired lease read at
/// function entry. GC also applies the configured horizon and re-runs this scan
/// before sweep, covering upload/publication races conservatively.
///
/// # Performance
///
/// A missing/expired lease costs one GET. An active lease costs one lease GET,
/// one staging-prefix LIST, and one full GET plus JSON decode per listed staging
/// object. Record bodies are read with bounded concurrency after the lease and
/// LIST succeed, collected in key order, and all outcomes are inspected before
/// an error is returned.
///
/// # Examples
///
/// If lease token 9 is active and staging records exist for tokens 8 and 9,
/// only token 9's keys are returned. Once token 9 expires, the same records
/// produce an empty root set.
///
/// # Rust Notes for Java/C Engineers
///
/// `BTreeSet::extend` moves owned strings from each decoded matching record into
/// the result. No shared mutable collection or lock crosses `.await`; Java code
/// might use a mutable `TreeSet`, while C would need explicit string ownership
/// and cleanup on every error branch.
pub async fn active_staged_keys(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<BTreeSet<String>> {
    active_staged_keys_at(store, namespace, Utc::now()).await
}

/// Reads active compaction staging roots at an explicit wall time.
pub async fn active_staged_keys_at(
    store: &ZeppelinStore,
    namespace: &str,
    now: DateTime<Utc>,
) -> Result<BTreeSet<String>> {
    Ok(active_staging_observation_at(store, namespace, now)
        .await?
        .keys)
}

async fn active_staging_observation_at(
    store: &ZeppelinStore,
    namespace: &str,
    now: DateTime<Utc>,
) -> Result<ActiveStagingObservation> {
    active_staging_observation_at_with_mode(store, namespace, now, GcReadMode::WarmBounded).await
}

async fn active_staging_observation_at_with_mode(
    store: &ZeppelinStore,
    namespace: &str,
    now: DateTime<Utc>,
    read_mode: GcReadMode,
) -> Result<ActiveStagingObservation> {
    let lease_data = match store.get(&format!("{namespace}/lease.json")).await {
        Ok(data) => data,
        Err(crate::error::ZeppelinError::NotFound { .. }) => {
            return Ok(ActiveStagingObservation {
                keys: BTreeSet::new(),
                lease_expires_at: None,
            });
        }
        Err(e) => return Err(e),
    };
    let lease: Lease = serde_json::from_slice(&lease_data)?;
    if lease.expires_at <= now {
        return Ok(ActiveStagingObservation {
            keys: BTreeSet::new(),
            lease_expires_at: None,
        });
    }

    let mut staged = BTreeSet::new();
    let prefix = format!("{namespace}/_staging/");
    let mut keys = store.list_prefix(&prefix).await?;
    if read_mode.is_bounded() {
        keys.sort();
        let futures = keys
            .into_iter()
            .map(|key| {
                let store = store.clone();
                async move {
                    let data = store.get(&key).await?;
                    serde_json::from_slice::<CompactionStaging>(&data).map_err(Into::into)
                }
                .boxed()
            })
            .collect();
        let results = collect_bounded_ordered(futures).await;
        for result in results {
            let entry = result?;
            if entry.fencing_token == lease.fencing_token {
                staged.extend(entry.keys);
            }
        }
    } else {
        for key in keys {
            let data = store.get(&key).await?;
            let entry: CompactionStaging = serde_json::from_slice(&data)?;
            if entry.fencing_token == lease.fencing_token {
                staged.extend(entry.keys);
            }
        }
    }
    let lease_expires_at = (!staged.is_empty()).then_some(lease.expires_at);
    Ok(ActiveStagingObservation {
        keys: staged,
        lease_expires_at,
    })
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Unit coverage for exact reachability, mark persistence, and delete gates.
    //!
    //! These tests use deterministic in-memory manifests for pure set logic and
    //! an in-memory `object_store` backend for candidate-ledger round trips.
    //! They deliberately cover carried cluster ownership, optional index
    //! sidecars, legacy persistence, horizon boundaries, and fail-closed
    //! decisions. Integration suites exercise full cycles against broader store
    //! and fault-injection setups.

    use super::*;

    use chrono::Utc;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use ulid::Ulid;

    use crate::fts::global_index::global_fts_key;
    use crate::fts::inverted_index::fts_index_key;
    use crate::index::bitmap::bitmap_key;
    use crate::index::hierarchical::{tree_meta_key, tree_node_key};
    use crate::index::ivf_flat::build::{attrs_key, bootstrap_key, centroids_key, cluster_key};
    use crate::index::ivf_flat::membership::membership_key;
    use crate::index::ivf_flat::sketch::sketch_key;
    use crate::index::quantization::pq::{pq_cluster_key, pq_codebook_key};
    use crate::index::quantization::sq::{sq_calibration_key, sq_cluster_key};
    use crate::index::quantization::QuantizationType;
    use crate::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};
    use crate::wal::fragment::WalFragment;
    use crate::wal::manifest::{
        BootstrapRef, ClusterDataObjectRef, FragmentRef, Manifest, MembershipRef, SegmentRef,
        SketchRef,
    };

    /// Stable namespace prefix used to make expected artifact keys readable.
    const NS: &str = "gc_ns";

    fn test_branch_root(
        branch_entropy: u128,
        generation: ManifestGeneration,
        source_manifest_sha256: ManifestDigest,
    ) -> BranchRoot {
        BranchRoot {
            branch_id: BranchId::from_ulid(Ulid::from_parts(1, branch_entropy)),
            source_generation: generation,
            source_manifest_sha256,
            fork_view_sha256: crate::namespace::ForkViewDigest::new([0x41; 32]),
            source_config_sha256: crate::namespace::SourceDataPlaneConfigDigest::new([0x42; 32]),
            target_namespace: NamespaceId::parse(format!("gc_child_{branch_entropy}"))
                .expect("test branch target must be valid"),
            target_incarnation: NamespaceIncarnationId::from_uuid(uuid::Uuid::from_u128(
                branch_entropy + 100,
            )),
            created_at: Utc::now(),
        }
    }

    async fn write_test_manifest(store: &ZeppelinStore, incarnation: uuid::Uuid) {
        let mut manifest = Manifest::new();
        manifest
            .bind_namespace_incarnation(incarnation)
            .expect("test manifest must bind one incarnation");
        manifest
            .write(store, NS)
            .await
            .expect("test manifest must publish");
    }

    async fn insert_test_branch_roots(
        store: &ZeppelinStore,
        count: usize,
    ) -> (ManifestGeneration, Vec<BranchRoot>) {
        let (mut manifest, version) = Manifest::read_versioned(store, NS)
            .await
            .expect("test manifest read must succeed")
            .expect("test manifest must exist");
        let generation = ManifestGeneration::new(manifest.version())
            .expect("published manifest generation must be nonzero");
        let digest = version
            .exact_manifest_digest()
            .expect("versioned read must retain exact bytes");
        let roots = (1..=count)
            .map(|entropy| test_branch_root(entropy as u128, generation, digest))
            .collect::<Vec<_>>();
        for root in &roots {
            manifest
                .insert_branch_root_candidate(root.clone(), count.max(1))
                .expect("test root must be valid");
        }
        manifest
            .write_conditional(store, NS, &version)
            .await
            .expect("root-bearing manifest must publish");
        (generation, roots)
    }

    async fn advance_test_manifest(store: &ZeppelinStore, writes: usize) {
        for offset in 0..writes {
            let (mut manifest, version) = Manifest::read_versioned(store, NS)
                .await
                .expect("test manifest read must succeed")
                .expect("test manifest must exist");
            manifest.updated_at += chrono::Duration::seconds((offset + 1) as i64);
            manifest
                .write_conditional(store, NS, &version)
                .await
                .expect("test manifest successor must publish");
        }
    }

    #[tokio::test]
    async fn branch_roots_retain_history_after_one_of_two_shared_generation_roots_is_removed() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        write_test_manifest(&store, uuid::Uuid::from_u128(700)).await;
        let (rooted_generation, roots) = insert_test_branch_roots(&store, 2).await;
        advance_test_manifest(&store, 3).await;

        let (mut manifest, version) = Manifest::read_versioned(&store, NS).await.unwrap().unwrap();
        manifest
            .remove_branch_root_candidate(&roots[0])
            .expect("one exact root must be removable");
        manifest
            .write_conditional(&store, NS, &version)
            .await
            .expect("single-root successor must publish");

        let live_roots = load_live_root_observation(&store, NS, None)
            .await
            .unwrap()
            .unwrap();
        let result = prune_history_with_memo_at(
            &store,
            NS,
            ManifestHistoryRetention {
                keep_count: 1,
                pitr_retention_secs: 0,
                skew_slop_secs: 0,
            },
            Utc::now() + chrono::Duration::hours(1),
            None,
            &live_roots,
        )
        .await
        .expect("the remaining shared-generation root must authorize retention");

        assert_eq!(live_roots.identity.branch_roots.len(), 1);
        assert!(result
            .retained_history_observations
            .iter()
            .any(|observation| observation.history.version == rooted_generation.get()));
        assert!(store
            .exists(&Manifest::history_key(NS, rooted_generation.get()))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn missing_rooted_history_fails_before_pruning_any_generation() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        write_test_manifest(&store, uuid::Uuid::from_u128(701)).await;
        let (rooted_generation, _) = insert_test_branch_roots(&store, 1).await;
        advance_test_manifest(&store, 2).await;
        let survivor = Manifest::history_key(NS, rooted_generation.get() + 1);
        store
            .delete(&Manifest::history_key(NS, rooted_generation.get()))
            .await
            .unwrap();
        let live_roots = load_live_root_observation(&store, NS, None)
            .await
            .unwrap()
            .unwrap();

        let error = match prune_history_with_memo_at(
            &store,
            NS,
            ManifestHistoryRetention {
                keep_count: 1,
                pitr_retention_secs: 0,
                skew_slop_secs: 0,
            },
            Utc::now() + chrono::Duration::hours(1),
            None,
            &live_roots,
        )
        .await
        {
            Ok(_) => panic!("missing rooted history must fail closed"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            ZeppelinError::Branch(error)
                if matches!(*error, BranchError::BranchRootInvalid { .. })
        ));
        assert!(store.exists(&survivor).await.unwrap());
    }

    #[tokio::test]
    async fn rooted_history_digest_mismatch_fails_before_pruning_any_generation() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        write_test_manifest(&store, uuid::Uuid::from_u128(702)).await;
        let (rooted_generation, _) = insert_test_branch_roots(&store, 1).await;
        advance_test_manifest(&store, 2).await;
        let survivor = Manifest::history_key(NS, rooted_generation.get() + 1);
        let rooted_key = Manifest::history_key(NS, rooted_generation.get());
        let mut rooted = Manifest::read_history(&store, NS, rooted_generation.get())
            .await
            .unwrap()
            .unwrap();
        rooted.updated_at += chrono::Duration::seconds(1);
        store
            .put(&rooted_key, rooted.to_bytes().unwrap())
            .await
            .unwrap();
        let live_roots = load_live_root_observation(&store, NS, None)
            .await
            .unwrap()
            .unwrap();

        let error = match prune_history_with_memo_at(
            &store,
            NS,
            ManifestHistoryRetention {
                keep_count: 1,
                pitr_retention_secs: 0,
                skew_slop_secs: 0,
            },
            Utc::now() + chrono::Duration::hours(1),
            None,
            &live_roots,
        )
        .await
        {
            Ok(_) => panic!("digest-mismatched rooted history must fail closed"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            ZeppelinError::Branch(error)
                if matches!(*error, BranchError::ManifestDigestMismatch { .. })
        ));
        assert!(store.exists(&survivor).await.unwrap());
    }

    #[tokio::test]
    async fn live_root_map_growth_invalidates_root_observation_and_idle_memo() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let incarnation_uuid = uuid::Uuid::from_u128(703);
        write_test_manifest(&store, incarnation_uuid).await;
        let opening = load_live_root_observation(&store, NS, None)
            .await
            .unwrap()
            .unwrap();
        let gc = GcConfig::default();
        let now = Utc::now();
        let incarnation = GcNamespaceIncarnation::with_incarnation_id(
            NS.to_string(),
            now,
            NamespaceIncarnationId::from_uuid(incarnation_uuid),
        );
        let mut runner = GcRunner::new(store.clone(), gc);
        runner
            .run_cycle_at(incarnation.clone(), now)
            .await
            .expect("opening GC cycle must complete");
        assert!(runner.namespaces[NS]
            .live_root_identity
            .branch_roots
            .is_empty());

        insert_test_branch_roots(&store, 1).await;
        let error = revalidate_live_root_observation(&store, NS, &opening)
            .await
            .unwrap_err();
        assert!(matches!(error, ZeppelinError::ManifestConflict { .. }));

        runner
            .run_cycle_at(incarnation, now + chrono::Duration::seconds(1))
            .await
            .expect("root-map change must force an authoritative GC cycle");
        assert_eq!(
            runner.namespaces[NS].live_root_identity.branch_roots.len(),
            1
        );
    }

    #[test]
    fn gc_runner_drops_memo_for_same_timestamp_new_incarnation() {
        let now = Utc::now();
        let old = GcNamespaceIncarnation::with_incarnation_id(
            NS.to_string(),
            now,
            NamespaceIncarnationId::new(),
        );
        let replacement = GcNamespaceIncarnation::with_incarnation_id(
            NS.to_string(),
            now,
            NamespaceIncarnationId::new(),
        );
        let gc = GcConfig::default();
        let mut runner = GcRunner::new(ZeppelinStore::new(Arc::new(InMemory::new())), gc.clone());
        runner.namespaces.insert(
            NS.to_string(),
            NamespaceGcMemo {
                incarnation: old,
                history: BTreeMap::new(),
                inventory: None,
                live_root_identity: LiveRootIdentity {
                    storage_etag: "old-live-manifest-etag".to_string(),
                    manifest_generation: 1,
                    namespace_incarnation: None,
                    branch_roots: BTreeMap::new(),
                },
                next_due_at: None,
                last_now: now,
                last_cycle_complete: true,
                candidate_phase_due: false,
                config: GcConfigFingerprint::from(&gc),
            },
        );

        runner.retain_namespaces(&BTreeSet::from([replacement]));

        assert!(runner.namespaces.is_empty());
    }

    #[tokio::test]
    async fn bounded_read_collection_drains_all_futures_and_preserves_input_order() {
        let count = GC_READ_BATCH_CONCURRENCY + 3;
        let barrier = Arc::new(tokio::sync::Barrier::new(GC_READ_BATCH_CONCURRENCY));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let futures = (0..count)
            .map(|index| {
                let barrier = Arc::clone(&barrier);
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                let completed = Arc::clone(&completed);
                async move {
                    let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now_active, Ordering::SeqCst);
                    if index < GC_READ_BATCH_CONCURRENCY {
                        barrier.wait().await;
                    }
                    if index == 1 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    active.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                    match index {
                        1 => Err(ZeppelinError::Serialization(
                            "lower-index failure".to_string(),
                        )),
                        2 => Err(ZeppelinError::Serialization(
                            "higher-index failure".to_string(),
                        )),
                        _ => Ok(index),
                    }
                }
                .boxed()
            })
            .collect();

        let results = collect_bounded_ordered(futures).await;
        assert_eq!(results.len(), count);
        assert_eq!(completed.load(Ordering::SeqCst), count);
        assert_eq!(peak.load(Ordering::SeqCst), GC_READ_BATCH_CONCURRENCY);
        for (index, result) in results.iter().enumerate() {
            match index {
                1 => assert!(result
                    .as_ref()
                    .unwrap_err()
                    .to_string()
                    .contains("lower-index failure")),
                2 => assert!(result
                    .as_ref()
                    .unwrap_err()
                    .to_string()
                    .contains("higher-index failure")),
                _ => assert_eq!(*result.as_ref().unwrap(), index),
            }
        }
        let error = results.into_iter().collect::<Result<Vec<_>>>().unwrap_err();
        assert!(error.to_string().contains("lower-index failure"));
    }

    #[test]
    fn joined_phase_errors_keep_the_former_sequential_priority() {
        let error = |label: &str| ZeppelinError::Serialization(label.to_string());

        let mark = assemble_mark_read_inputs(
            Err(error("namespace-list")),
            Err(error("candidate-ledger")),
            Err(error("manifest")),
            Err(error("staging")),
        );
        assert!(matches!(
            mark,
            Err(MarkReadFailure::NamespaceList(error))
                if error.to_string().contains("namespace-list")
        ));

        let mark = assemble_mark_read_inputs(
            Ok(Vec::new()),
            Err(error("candidate-ledger")),
            Err(error("manifest")),
            Err(error("staging")),
        );
        assert!(matches!(
            mark,
            Err(MarkReadFailure::CandidateLedger(error))
                if error.to_string().contains("candidate-ledger")
        ));

        let mark = assemble_mark_read_inputs(
            Ok(Vec::new()),
            Ok(LoadedCandidateLedger {
                candidates: Vec::new(),
                encoding: CandidateLedgerEncoding::Versioned(GC_CANDIDATE_STORE_VERSION),
            }),
            Ok(None),
            Err(error("staging")),
        );
        assert!(matches!(mark, Err(MarkReadFailure::ManifestMissing)));

        let sweep = assemble_sweep_read_inputs(Err(error("manifest")), Err(error("staging")));
        assert!(matches!(
            sweep,
            Err(SweepReadFailure::Manifest(error))
                if error.to_string().contains("manifest")
        ));

        let sweep = assemble_sweep_read_inputs(Ok(None), Err(error("staging")));
        assert!(matches!(sweep, Err(SweepReadFailure::ManifestMissing)));
    }

    /// Builds the smallest manifest fragment descriptor useful to reachability tests.
    ///
    /// # Parameters
    ///
    /// - `id`: ULID embedded in the expected WAL object key.
    ///
    /// # Returns
    ///
    /// An owned one-vector, 100-byte descriptor with sequence number zero.
    ///
    /// # Examples
    ///
    /// The returned descriptor contributes exactly one `wal/<id>.wal` key when
    /// inserted into a manifest.
    fn fragment_ref(id: Ulid) -> FragmentRef {
        FragmentRef {
            id,
            vector_count: 1,
            delete_count: 0,
            sequence_number: 0,
            size_bytes: 100,
            artifact_origin: None,
        }
    }

    /// Builds a minimal unquantized IVF segment descriptor for focused test mutation.
    ///
    /// # Parameters
    ///
    /// - `id`: Segment identifier copied into the owned descriptor.
    /// - `cluster_count`: Number of self-owned cluster and attribute objects.
    ///
    /// # Returns
    ///
    /// A ten-vector flat segment with no optional bitmap, FTS, quantization,
    /// sketch, bootstrap, membership, or grouped-cluster artifacts.
    ///
    /// # Examples
    ///
    /// Tests start from `segment_ref("seg_a", 2)` and selectively enable
    /// carried ownership or optional sidecars to isolate one reachability rule.
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
            artifact_origin: None,
        }
    }

    /// Projects candidates into a deterministic set for order-independent assertions.
    ///
    /// # Parameters
    ///
    /// - `candidates`: Borrowed candidate slice whose exact keys are compared.
    ///
    /// # Returns
    ///
    /// An owned, sorted, duplicate-free key set.
    ///
    /// # Examples
    ///
    /// Two candidates in either vector order compare against the same expected
    /// `BTreeSet`.
    fn candidate_keys(candidates: &[GcCandidate]) -> BTreeSet<String> {
        candidates
            .iter()
            .map(|candidate| candidate.key.clone())
            .collect()
    }

    /// One table row describing keys that must and must not be manifest-reachable.
    struct ReachabilityCase {
        /// Human-readable label included when an assertion fails.
        name: &'static str,
        /// Owned manifest configuration under test.
        manifest: Manifest,
        /// Exact keys required in the derived reachability set.
        present: Vec<String>,
        /// Exact keys required to remain outside the derived set.
        absent: Vec<String>,
    }

    /// Protects the complete manifest-to-artifact expansion across layout variants.
    ///
    /// This table catches accidental omission or over-inclusion of fragments,
    /// carried clusters, deferred deletes, hierarchical metadata, grouped data,
    /// quantization, bitmap, local/global FTS, sketch, bootstrap, or membership
    /// objects. Either mistake could leak storage or delete visible data.
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
            rotation_seed: None,
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
        let hierarchical_node = format!("root_{}", Ulid::from_parts(2_500, 1));
        hierarchical_manifest
            .set_hierarchical_routing_nodes("seg_tree", vec![hierarchical_node.clone()]);

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
                    tree_node_key(NS, "seg_tree", &hierarchical_node),
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
            let reachable = reachable_keys(NS, &case.manifest).unwrap();
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
    fn reachability_routes_immutable_refs_through_their_physical_origin() {
        const SOURCE: &str = "gc_source";
        let source_origin = ArtifactOrigin {
            namespace: NamespaceId::new(SOURCE).unwrap(),
            incarnation: NamespaceIncarnationId::from_uuid(uuid::Uuid::from_u128(2)),
        };
        let fragment_id = Ulid::from_parts(1, 44);
        let mut fragment = fragment_ref(fragment_id);
        fragment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        let mut segment = segment_ref("seg_foreign", 1);
        segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        segment.bitmap_fields = vec!["color".to_string()];
        segment.fts_fields = vec!["body".to_string()];
        segment.has_global_fts = true;
        segment.quantization = QuantizationType::Product;
        segment.sketch = Some(SketchRef {
            key: sketch_key(SOURCE, "seg_foreign"),
            version: 3,
            code_dims: 8,
            bytes_per_vector: 8,
            size_bytes: 512,
            rotation_seed: None,
        });
        segment.bootstrap = Some(BootstrapRef {
            key: bootstrap_key(SOURCE, "seg_foreign"),
            size_bytes: 1024,
        });
        segment.membership = Some(MembershipRef {
            key: membership_key(SOURCE, "seg_foreign"),
            size_bytes: 256,
            entry_count: 10,
        });
        let mut hierarchical = segment_ref("seg_foreign_tree", 1);
        hierarchical.artifact_origin = Some(ArtifactOriginIndex::new(0));
        hierarchical.hierarchical = true;
        hierarchical.quantization = QuantizationType::Scalar;
        let tree_node = format!("root_{}", Ulid::from_parts(2, 46));

        let mut manifest = Manifest::new();
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();
        manifest.artifact_origins = vec![source_origin];
        manifest.fragments = vec![fragment];
        manifest.segments = vec![segment, hierarchical];
        manifest.set_hierarchical_routing_nodes("seg_foreign_tree", vec![tree_node.clone()]);

        let reachable = reachable_keys(NS, &manifest).unwrap();

        assert!(reachable.contains(&WalFragment::s3_key(SOURCE, &fragment_id)));
        assert!(reachable.contains(&centroids_key(SOURCE, "seg_foreign")));
        assert!(reachable.contains(&cluster_key(SOURCE, "seg_foreign", 0)));
        assert!(reachable.contains(&attrs_key(SOURCE, "seg_foreign", 0)));
        assert!(reachable.contains(&bitmap_key(SOURCE, "seg_foreign", 0)));
        assert!(reachable.contains(&fts_index_key(SOURCE, "seg_foreign", 0)));
        assert!(reachable.contains(&pq_cluster_key(SOURCE, "seg_foreign", 0)));
        assert!(reachable.contains(&pq_codebook_key(SOURCE, "seg_foreign")));
        assert!(reachable.contains(&global_fts_key(SOURCE, "seg_foreign")));
        assert!(reachable.contains(&sketch_key(SOURCE, "seg_foreign")));
        assert!(reachable.contains(&bootstrap_key(SOURCE, "seg_foreign")));
        assert!(reachable.contains(&membership_key(SOURCE, "seg_foreign")));
        assert!(reachable.contains(&tree_meta_key(SOURCE, "seg_foreign_tree")));
        assert!(reachable.contains(&tree_node_key(SOURCE, "seg_foreign_tree", &tree_node,)));
        assert!(reachable.contains(&sq_cluster_key(SOURCE, "seg_foreign_tree", 0)));
        assert!(reachable.contains(&sq_calibration_key(SOURCE, "seg_foreign_tree")));
        assert!(!reachable.contains(&WalFragment::s3_key(NS, &fragment_id)));
        assert!(!reachable.contains(&centroids_key(NS, "seg_foreign")));
    }

    #[test]
    fn deletion_keys_must_be_owned_by_the_gc_target() {
        let local =
            TargetOwnedDeletionKey::classify(NS, format!("{NS}/segments/seg_local/cluster_0.bin"))
                .unwrap();
        assert_eq!(
            local.as_str(),
            format!("{NS}/segments/seg_local/cluster_0.bin")
        );

        let error = TargetOwnedDeletionKey::classify(
            NS,
            "gc_source/segments/seg_foreign/cluster_0.bin".to_string(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("outside exact namespace prefix"));
    }

    #[test]
    fn reachability_rejects_an_unresolvable_artifact_origin() {
        let mut fragment = fragment_ref(Ulid::from_parts(1, 45));
        fragment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        let mut manifest = Manifest::new();
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();
        manifest.fragments = vec![fragment];

        let error = reachable_keys(NS, &manifest).unwrap_err();

        assert!(error.to_string().contains("artifact origin index"));
    }

    /// Verifies that two live references to one carried object produce one root key.
    ///
    /// This protects the set semantics used by manifest deltas: shared object
    /// reachability is about presence, not a duplicate count in a vector.
    #[test]
    fn reachable_keys_are_deduplicated() {
        let mut manifest = Manifest::new();
        let mut first = segment_ref("seg_first", 1);
        first.cluster_owners = vec!["seg_shared".to_string()];
        let mut second = segment_ref("seg_second", 1);
        second.cluster_owners = vec!["seg_shared".to_string()];
        manifest.segments = vec![first, second];

        let reachable = reachable_keys(NS, &manifest).unwrap();
        let shared_key = cluster_key(NS, "seg_shared", 0);
        assert_eq!(
            reachable.iter().filter(|key| *key == &shared_key).count(),
            1,
            "shared carried object must appear once"
        );
    }

    /// Verifies that a manifest delta marks only exact keys whose final reference left.
    ///
    /// A carried cluster survives while the old centroid and replaced cluster
    /// artifacts become candidates stamped at the publication time.
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
            gc_candidates_from_manifest_delta(NS, &old_manifest, &new_manifest, commit_time)
                .unwrap();

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

    /// Protects shared carried artifacts until their final live segment releases them.
    ///
    /// The test would catch a per-segment deletion scheme that ignores global
    /// reachability and removes data still referenced by another segment.
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
        )
        .unwrap();
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
        )
        .unwrap();
        assert!(
            candidate_keys(&second_release)
                .is_superset(&BTreeSet::from([shared_cluster_key, shared_attrs_key,])),
            "shared carried keys become candidates when the last reference is gone"
        );
    }

    /// Demonstrates why manifest deltas cannot discover uploads never published by CAS.
    ///
    /// The absent-from-both key must not appear in the delta; periodic namespace
    /// LIST is the separate backstop for this crash-orphan class.
    #[test]
    fn crash_orphan_absent_from_both_manifests_is_not_a_delta_candidate() {
        let commit_time = Utc::now();
        let crash_orphan = format!("{NS}/segments/crash_orphan/cluster_0.bin");

        let mut old_manifest = Manifest::new();
        old_manifest.segments = vec![segment_ref("seg_live", 1)];
        let mut new_manifest = Manifest::new();
        new_manifest.segments = vec![segment_ref("seg_live", 1)];

        let candidates =
            gc_candidates_from_manifest_delta(NS, &old_manifest, &new_manifest, commit_time)
                .unwrap();

        assert!(
            !candidate_keys(&candidates).contains(&crash_orphan),
            "PUT-without-CAS orphans never entered a manifest and require the LIST backstop"
        );
    }

    /// Ensures a newly live key is removed when candidates meet a fresh manifest.
    ///
    /// This prevents the persisted ledger from acting like an authoritative
    /// reference count after a key is resurrected.
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

        let still_unreachable =
            revalidate_unreachable_candidates(NS, &manifest, &candidates).unwrap();

        assert_eq!(
            candidate_keys(&still_unreachable),
            BTreeSet::from([dead_key]),
            "delete decisions must re-read exact manifest reachability, not trust candidate state"
        );
    }

    /// Ensures active compaction uploads join the GC root set without becoming visible.
    ///
    /// The test catches a mark pass that protects only manifest artifacts during
    /// the upload-before-publication interval.
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

        let reachable = reachable_keys_with_staging(NS, &manifest, &staged).unwrap();

        assert!(reachable.contains(&centroids_key(NS, "seg_live")));
        for key in staged {
            assert!(
                reachable.contains(&key),
                "active lease staged key must be treated as reachable: {key}"
            );
        }
    }

    /// Scope artifacts published after compaction's prefix LIST remain owned by
    /// their source segment and become collectable after that parent disappears.
    #[test]
    fn scoped_artifacts_follow_parent_segment_reachability() {
        let source_id = format!("seg_{}", Ulid::from_parts(10_000, 7));
        let artifact_id = format!("ann_{}", Ulid::from_parts(11_000, 8));
        let digest = "ab".repeat(32);
        let descriptor = format!("{NS}/segments/{source_id}/security_scopes/ann/{digest}.json");
        let fts = format!("{NS}/segments/{source_id}/security_scopes/fts/{digest}.bin");
        let cluster = format!(
            "{NS}/segments/{source_id}/security_scopes/segments/{artifact_id}/cluster_0.bin"
        );
        let tree_node = format!(
            "{NS}/segments/{source_id}/security_scopes/segments/{artifact_id}/node_root_{}.bin",
            Ulid::from_parts(12_000, 9)
        );
        let listed = BTreeSet::from([
            descriptor.clone(),
            fts.clone(),
            cluster.clone(),
            tree_node.clone(),
        ]);

        for key in &listed {
            assert!(
                parse_gc_artifact_key(NS, key).is_some(),
                "known scoped artifact must enter the GC grammar: {key}"
            );
        }

        let mut live = Manifest::new();
        live.segments.push(segment_ref(&source_id, 1));
        let retained_history = BTreeSet::new();
        let mut reachable = reachable_keys(NS, &live).unwrap();
        extend_scoped_artifact_roots(NS, &live, &retained_history, &listed, &mut reachable);
        assert!(listed.is_subset(&reachable));

        let removed = Manifest::new();
        let mut unreachable = reachable_keys(NS, &removed).unwrap();
        extend_scoped_artifact_roots(NS, &removed, &retained_history, &listed, &mut unreachable);
        assert!(listed.is_disjoint(&unreachable));
        let candidates = mark_gc_candidates(
            NS,
            &listed,
            &unreachable,
            &[],
            Utc::now(),
            removed.version(),
        );
        assert_eq!(candidate_keys(&candidates), listed);
    }

    #[test]
    fn scoped_gc_grammar_rejects_arbitrary_nested_keys() {
        let source_id = format!("seg_{}", Ulid::from_parts(20_000, 1));
        let artifact_id = format!("ann_{}", Ulid::from_parts(21_000, 2));
        for key in [
            format!("{NS}/segments/{source_id}/security_scopes/notes.txt"),
            format!(
                "{NS}/segments/{source_id}/security_scopes/segments/{artifact_id}/cluster_0.bin/extra"
            ),
            format!("{NS}/segments/{source_id}/security_scopes/ann/not-a-digest.json"),
        ] {
            assert!(
                parse_gc_artifact_key(NS, &key).is_none(),
                "unknown nested key must remain fail-closed: {key}"
            );
        }
    }

    #[test]
    fn direct_hierarchical_nodes_enter_pending_delete_and_orphan_gc_grammar() {
        let now = Utc::now();
        let segment_ulid = ulid_seconds_ago(30, 301);
        let node_ulid = ulid_seconds_ago(29, 302);
        let segment_id = format!("seg_{segment_ulid}");
        let node_id = format!("n_2_{node_ulid}");
        let key = crate::index::hierarchical::tree_node_key(NS, &segment_id, &node_id);

        assert!(
            parse_gc_artifact_key(NS, &key).is_some(),
            "a direct routing node must be a production GC artifact"
        );
        assert!(
            pending_delete_horizon_satisfied(NS, &key, now, 5),
            "an old direct routing node must drain from pending_deletes"
        );

        let listed = BTreeSet::from([key.clone()]);
        let marked = mark_gc_candidates(
            NS,
            &listed,
            &BTreeSet::new(),
            &[],
            now,
            Manifest::new().version(),
        );
        assert_eq!(candidate_keys(&marked), listed);
    }

    /// Creates a deterministic-entropy ULID with a wall-clock-relative timestamp.
    ///
    /// # Parameters
    ///
    /// - `seconds`: Signed number of seconds before the current instant.
    /// - `entropy`: Low 80-bit ULID entropy used to keep fixtures distinct.
    ///
    /// # Returns
    ///
    /// A ULID whose timestamp drives artifact-age predicate tests.
    ///
    /// # Panics
    ///
    /// The test-only cast assumes the current UTC timestamp in milliseconds is
    /// non-negative; this is true for normal post-epoch test execution.
    ///
    /// # Examples
    ///
    /// `ulid_seconds_ago(30, 1)` produces an artifact roughly 30 seconds old.
    fn ulid_seconds_ago(seconds: i64, entropy: u128) -> Ulid {
        let ts = (Utc::now() - chrono::Duration::seconds(seconds)).timestamp_millis() as u64;
        Ulid::from_parts(ts, entropy)
    }

    /// Builds the production-like baseline predicate context used by table tests.
    ///
    /// # Parameters
    ///
    /// - `horizon_secs`: Required elapsed time and artifact age.
    /// - `now`: Shared reference instant for deterministic comparisons.
    ///
    /// # Returns
    ///
    /// A context at manifest version 10 with no staged watermark or optional
    /// generation guard; individual tests override only the field under study.
    ///
    /// # Examples
    ///
    /// A test can start with `delete_context(5, now)` and set only
    /// `oldest_inflight_ulid_ms` to isolate the in-flight watermark rule.
    fn delete_context(horizon_secs: u64, now: DateTime<Utc>) -> DeletePredicateContext {
        DeletePredicateContext {
            horizon_secs,
            now,
            oldest_inflight_ulid_ms: None,
            current_manifest_version: 10,
            min_newer_manifest_versions: None,
        }
    }

    /// Proves old deferred artifacts drain even when unrelated writes refresh the manifest.
    ///
    /// A regression to `manifest.updated_at` would keep garbage forever in a
    /// busy namespace and fail this property.
    #[test]
    fn pending_delete_horizon_uses_key_ulid_age_not_manifest_update_time() {
        let now = Utc::now();
        let old_id = ulid_seconds_ago(30, 201);
        let key = WalFragment::s3_key(NS, &old_id);

        assert!(
            pending_delete_horizon_satisfied(NS, &key, now, 5),
            "a busy namespace must drain old pending deletes even when manifest.updated_at is fresh"
        );
    }

    /// Proves a young artifact remains queued regardless of any old manifest timestamp.
    ///
    /// This guards the creation-ULID age floor that covers recent uploads.
    #[test]
    fn pending_delete_horizon_retains_young_key_even_with_old_manifest_update_time() {
        let now = Utc::now();
        let young_id = ulid_seconds_ago(1, 202);
        let key = WalFragment::s3_key(NS, &young_id);

        assert!(
            !pending_delete_horizon_satisfied(NS, &key, now, 5),
            "manifest.updated_at must not allow a key younger than the horizon to drain"
        );
    }

    /// Verifies malformed or unfamiliar pending-delete keys fail closed.
    ///
    /// Without this guard an accidental control-object key could be physically
    /// removed merely because it appeared in a deferred queue.
    #[test]
    fn pending_delete_horizon_retains_unparseable_key() {
        let now = Utc::now();
        let key = format!("{NS}/wal/not-a-ulid.wal");

        assert!(
            !pending_delete_horizon_satisfied(NS, &key, now, 5),
            "keys without parseable artifact ULIDs must be retained fail-closed"
        );
    }

    /// Preserves zero horizon as the explicit immediate-drain test override.
    ///
    /// This case intentionally accepts an unparseable key only because zero
    /// bypasses all time-based protection.
    #[test]
    fn pending_delete_horizon_zero_drains_immediately() {
        let now = Utc::now();
        let key = format!("{NS}/wal/not-a-ulid.wal");

        assert!(
            pending_delete_horizon_satisfied(NS, &key, now, 0),
            "zero horizon remains the test hook for immediate drain"
        );
    }

    /// Exercises structural, time, reachability, staging, and success decisions as a table.
    ///
    /// The table protects the default-deny behavior and confirms that an active
    /// staged key is treated exactly like any other fresh root.
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

    /// Verifies the artifact ULID floor and oldest in-flight watermark are independent.
    ///
    /// A mature mark cannot authorize a newly created key, and an active older
    /// upload conservatively protects all candidate artifacts newer than it.
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

    /// Proves an optional manifest-generation guard strengthens rather than replaces time.
    ///
    /// Deletion requires both the configured horizon and the requested number
    /// of newer generations when the optional guard is enabled.
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

    /// Protects decoding compatibility for empty, wrapped, and legacy ledgers.
    ///
    /// This catches a migration that would strand bare-array candidate JSON or
    /// reinterpret empty storage as corrupt.
    #[test]
    fn candidate_store_decodes_empty_versioned_and_legacy_json() {
        let empty = decode_candidate_ledger(b"").unwrap();
        assert!(empty.candidates.is_empty());
        assert_eq!(empty.encoding, CandidateLedgerEncoding::EmptyBody);

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
        let versioned = decode_candidate_ledger(&versioned).unwrap();
        assert_eq!(versioned.candidates, vec![candidate.clone()]);
        assert_eq!(
            versioned.encoding,
            CandidateLedgerEncoding::Versioned(GC_CANDIDATE_STORE_VERSION)
        );

        let legacy = serde_json::to_vec(&vec![candidate.clone()]).unwrap();
        let legacy = decode_candidate_ledger(&legacy).unwrap();
        assert_eq!(legacy.candidates, vec![candidate]);
        assert_eq!(legacy.encoding, CandidateLedgerEncoding::LegacyArray);
    }

    /// Verifies pre-generation candidate records default to generation zero.
    ///
    /// The legacy record remains wall-clock gated even though its generation is
    /// treated as old enough for any future optional epoch guard.
    #[test]
    fn legacy_candidate_without_epoch_decodes_as_very_old_generation() {
        /// Test-only wrapper matching the persisted schema before epoch tracking.
        #[derive(Serialize)]
        struct LegacyCandidateStore<'a> {
            /// Wrapper version retained by the historical encoding.
            version: u32,
            /// Borrowed legacy records serialized into the fixture bytes.
            candidates: Vec<LegacyCandidate<'a>>,
        }

        /// Test-only candidate shape that predates the manifest-version field.
        #[derive(Serialize)]
        struct LegacyCandidate<'a> {
            /// Borrowed artifact key; serde copies its characters into fixture JSON.
            key: &'a str,
            /// Original wall-clock mark retained by the legacy representation.
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
            decode_candidate_ledger(&legacy).unwrap().candidates,
            vec![GcCandidate {
                key,
                first_seen_unreachable_at,
                unreachable_since_manifest_version: 0,
            }],
            "legacy candidates default to epoch 0, which is still time-gated"
        );
    }

    /// Round-trips the versioned candidate ledger through the storage abstraction.
    ///
    /// This protects the integration between JSON encoding, the derived ledger
    /// key, and full-object PUT/GET behavior without requiring an external S3
    /// service.
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

    #[tokio::test]
    async fn gc_horizon_honors_injected_now() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let now = Utc::now();
        let mut manifest = Manifest::new_at(now);
        manifest
            .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
            .unwrap();
        manifest.write(&store, NS).await.unwrap();
        let old_ms =
            u64::try_from((now - chrono::Duration::seconds(60)).timestamp_millis()).unwrap();
        let orphan = WalFragment::s3_key(NS, &Ulid::from_parts(old_ms, 101));
        store
            .put(&orphan, Bytes::from_static(b"orphan"))
            .await
            .unwrap();
        let gc = GcConfig {
            horizon_secs: 30,
            compaction_upload_window_secs: 2,
            skew_slop_secs: 0,
            allow_unsafe_short_horizon: true,
            manifest_history_keep_count: 1,
            pitr_retention_secs: 0,
        };

        let first = run_gc_cycle_at(&store, NS, &gc, now).await.unwrap();
        assert_eq!(first.candidates_marked, 1);
        assert!(store.exists(&orphan).await.unwrap());

        let second = run_gc_cycle_at(&store, NS, &gc, now + chrono::Duration::seconds(31))
            .await
            .unwrap();
        assert_eq!(second.objects_deleted, 1);
        assert!(!store.exists(&orphan).await.unwrap());
    }

    /// Ensures marking drops resurrected entries but preserves old marks for dead keys.
    ///
    /// Preserving the first mark lets a continuously unreachable key mature;
    /// dropping the resurrected one prevents stale-ledger deletion authority.
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

    /// Ensures a new mark records the exact manifest generation used for reachability.
    ///
    /// This keeps later auditing and the optional generation guard tied to the
    /// snapshot that first classified the object as unreachable.
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
