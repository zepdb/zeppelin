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
//!    history pruning, mark persistence, fresh revalidation, and the sequential
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
//! every object it references. Deletes are sequential and not transactional:
//! a later failure can follow earlier successful deletes. Candidate and
//! `pending_deletes` state retain retryable work, and `NotFound` is treated as
//! an idempotent completion where the relevant path explicitly says so.
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
use crate::wal::manifest::{Manifest, ManifestHistoryRetention};
use crate::wal::Lease;

/// Persisted JSON wrapper version written for new candidate ledgers.
const GC_CANDIDATE_STORE_VERSION: u32 = 1;

/// Maximum fresh-read/CAS attempts made while pruning `pending_deletes`.
const GC_MANIFEST_CAS_RETRIES: usize = 10;

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
#[must_use]
pub fn reachable_keys(namespace: &str, manifest: &Manifest) -> BTreeSet<String> {
    reachable_keys_with_staging(namespace, manifest, &BTreeSet::new())
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
        keys.extend(reachable_keys(namespace, &manifest));
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
    Ok(reachable_keys_with_retained_history_and_staging_keys(
        namespace,
        manifest,
        staging,
        &retained_history,
    ))
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
) -> BTreeSet<String> {
    let mut keys = reachable_keys_with_staging(namespace, manifest, staging);
    keys.extend(retained_history.iter().cloned());
    keys
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
    /// The current decoder accepts the wrapper without rejecting other numeric
    /// values; this field records format intent rather than enforcing migration.
    version: u32,
    /// Complete candidate ledger replacing the previous object contents.
    candidates: Vec<GcCandidate>,
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
    /// Candidate `NotFound` outcomes count here as idempotent completion;
    /// pending-delete `NotFound` outcomes are pruned but not counted deleted.
    pub objects_deleted: usize,
    /// Number of pending-delete objects physically deleted by this cycle.
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
/// If one object is deleted, one was already absent, and one DELETE failed, the
/// report contains one deleted object, two pruned entries, and one retained
/// entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingDeleteDrainReport {
    /// Number of pending-delete objects physically deleted from storage.
    pub objects_deleted: usize,
    /// Entries removed after successful DELETE or confirmed prior absence.
    pub entries_pruned: usize,
    /// Number of entries kept because deletion failed.
    pub entries_retained: usize,
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
    match store.get(&gc_candidate_store_key(namespace)).await {
        Ok(data) => decode_gc_candidates(&data),
        Err(crate::error::ZeppelinError::NotFound { .. }) => Ok(Vec::new()),
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
/// Counts of successful physical deletes, entries pruned after delete or prior
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
/// Lists and GETs retained manifest history, issues at most one sequential
/// DELETE per eligible queue entry per attempt, updates deletion metrics, and
/// conditionally publishes a manifest with confirmed entries removed.
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
/// History discovery costs one LIST plus one GET per retained generation. Each
/// CAS attempt costs one manifest GET, up to one DELETE per pending key, and at
/// most one manifest history write plus conditional live-manifest PUT inside
/// [`Manifest::write_conditional`].
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
    let history_reachable = retained_manifest_history_reachable_keys(store, namespace).await?;
    drain_pending_deletes_with_retained_history(store, namespace, gc, &history_reachable, now).await
}

/// Drains pending deletes using a retained-history union already loaded by the caller.
///
/// This is the retrying implementation behind [`drain_pending_deletes`].
/// Supplying the set avoids repeating history LIST/GET work inside each manifest
/// CAS attempt.
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
) -> Result<PendingDeleteDrainReport> {
    let mut deleted_keys = BTreeSet::new();

    for attempt in 0..GC_MANIFEST_CAS_RETRIES {
        let Some((mut manifest, version)) = Manifest::read_versioned(store, namespace).await?
        else {
            return Ok(PendingDeleteDrainReport {
                objects_deleted: deleted_keys.len(),
                ..PendingDeleteDrainReport::default()
            });
        };

        if manifest.pending_deletes.is_empty() {
            return Ok(PendingDeleteDrainReport {
                objects_deleted: deleted_keys.len(),
                ..PendingDeleteDrainReport::default()
            });
        }

        let pending = manifest.pending_deletes.clone();
        let mut confirmed_absent = BTreeSet::new();
        let mut retained = BTreeSet::new();

        for key in &pending {
            if retained_history.contains(key) {
                retained.insert(key.clone());
                continue;
            }

            if !pending_delete_horizon_satisfied(namespace, key, now, gc.horizon_secs) {
                retained.insert(key.clone());
                continue;
            }

            match store.delete(key).await {
                Ok(()) => {
                    deleted_keys.insert(key.clone());
                    confirmed_absent.insert(key.clone());
                    crate::metrics::GC_OBJECTS_DELETED_TOTAL
                        .with_label_values(&[namespace])
                        .inc();
                    info!(
                        namespace,
                        key = %key,
                        "gc deleted pending-delete object"
                    );
                }
                Err(crate::error::ZeppelinError::NotFound { .. }) => {
                    confirmed_absent.insert(key.clone());
                    debug_pending_delete_absent(namespace, key);
                }
                Err(e) => {
                    retained.insert(key.clone());
                    warn!(
                        namespace,
                        key = %key,
                        error = %e,
                        "gc pending-delete failed; retaining manifest entry"
                    );
                }
            }
        }

        if confirmed_absent.is_empty() {
            return Ok(PendingDeleteDrainReport {
                objects_deleted: deleted_keys.len(),
                entries_pruned: 0,
                entries_retained: retained.len(),
            });
        }

        manifest
            .pending_deletes
            .retain(|key| !confirmed_absent.contains(key));
        manifest.updated_at = now;

        match manifest.write_conditional(store, namespace, &version).await {
            Ok(()) => {
                return Ok(PendingDeleteDrainReport {
                    objects_deleted: deleted_keys.len(),
                    entries_pruned: confirmed_absent.len(),
                    entries_retained: retained.len(),
                });
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

/// Records that an already-absent deferred object can be pruned idempotently.
///
/// # Parameters
///
/// - `namespace`: Namespace label attached to the structured debug event.
/// - `key`: Exact pending-delete key confirmed absent by the store.
///
/// # Side Effects
///
/// Emits one structured debug log and performs no storage operation.
///
/// # Examples
///
/// A retry after an earlier successful DELETE logs absence before the manifest
/// entry is removed by CAS.
fn debug_pending_delete_absent(namespace: &str, key: &str) {
    tracing::debug!(
        namespace,
        key,
        "gc pending-delete object already absent; pruning manifest entry"
    );
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
/// An empty vector for empty bytes, the wrapper's candidates for versioned JSON,
/// or the array contents for the legacy bare-vector representation.
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
/// the input. Each successful branch returns one owned `Vec`; the compiler
/// requires every failure branch to produce the same `Result` type.
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
/// The cycle first prunes unretained manifest history, rebuilds the retained
/// history root set, and drains explicit `pending_deletes`. It then lists the
/// namespace once, persists newly unreachable known artifacts, and re-reads the
/// live manifest, active staging, and retained history before attempting any
/// candidate DELETE.
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
///          `-- all predicates pass ----> DELETE sequentially
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
/// Candidate ledger writes are unconditional and have no cross-cycle lock; a
/// competing cycle may delay/re-mark work, but no ledger entry bypasses fresh
/// reachability and age predicates. The horizon protects in-flight readers of
/// stale cached manifests even though this function does not inspect caches. A
/// ledger candidate absent from the cycle's original LIST is not deleted and is
/// omitted from the cleaned ledger; if it later appears again, it must be marked
/// and wait through the horizon again.
///
/// # Performance
///
/// One cycle includes history and snapshot LIST/GET/DELETE work, one namespace
/// LIST materialized in memory, candidate-ledger GET plus one or two full PUTs,
/// at least two live-manifest GETs for mark and sweep plus any pending-drain CAS
/// attempts, two active-staging scans, another retained-history scan for the
/// sweep, and sequential artifact DELETEs. Network roundtrips grow with retained
/// generations, staging records, pending entries, and mature candidates;
/// candidates are not deleted concurrently.
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
    let history_prune = match Manifest::prune_history_with_retention_at(
        store,
        namespace,
        ManifestHistoryRetention {
            keep_count: gc.manifest_history_keep_count,
            pitr_retention_secs: gc.pitr_retention_secs,
            skew_slop_secs: gc.skew_slop_secs,
        },
        now,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            warn!(
                namespace,
                error = %e,
                "gc manifest-history prune failed; aborting cycle"
            );
            return Ok(GcCycleReport::default());
        }
    };
    let manifest_history_pruned = history_prune.pruned;
    let retained_history = match retained_manifest_history_reachable_keys(store, namespace).await {
        Ok(keys) => keys,
        Err(e) => {
            warn!(
                namespace,
                error = %e,
                "gc retained history re-read failed before pending-delete drain; aborting cycle"
            );
            return Ok(GcCycleReport::default());
        }
    };
    let pending_report = match drain_pending_deletes_with_retained_history(
        store,
        namespace,
        gc,
        &retained_history,
        now,
    )
    .await
    {
        Ok(report) => report,
        Err(e) => {
            warn!(
                namespace,
                error = %e,
                "gc pending-delete drain failed; aborting cycle"
            );
            return Ok(GcCycleReport::default());
        }
    };
    let base_report = GcCycleReport {
        objects_deleted: pending_report.objects_deleted,
        pending_deletes_deleted: pending_report.objects_deleted,
        pending_deletes_pruned: pending_report.entries_pruned,
        pending_deletes_retained: pending_report.entries_retained,
        ..GcCycleReport::default()
    };

    let prefix = format!("{namespace}/");
    let listed_keys = match store.list_prefix(&prefix).await {
        Ok(keys) => keys.into_iter().collect::<BTreeSet<_>>(),
        Err(e) => {
            warn!(namespace, error = %e, "gc listing failed; aborting cycle");
            return Ok(base_report);
        }
    };

    let persisted = match load_gc_candidates(store, namespace).await {
        Ok(candidates) => candidates,
        Err(e) => {
            warn!(namespace, error = %e, "gc candidate load failed; aborting cycle");
            return Ok(base_report);
        }
    };

    let mark_manifest = match read_manifest_for_gc(store, namespace).await {
        Ok(Some(manifest)) => manifest,
        Ok(None) => {
            warn!(namespace, "gc manifest missing; skipping namespace");
            return Ok(base_report);
        }
        Err(e) => {
            warn!(namespace, error = %e, "gc manifest read failed; aborting cycle");
            return Ok(base_report);
        }
    };
    let mark_staging = match active_staged_keys_at(store, namespace, now).await {
        Ok(staging) => staging,
        Err(e) => {
            warn!(namespace, error = %e, "gc active staging read failed; aborting cycle");
            return Ok(base_report);
        }
    };
    let mark_reachable = reachable_keys_with_retained_history_and_staging_keys(
        namespace,
        &mark_manifest,
        &mark_staging,
        &retained_history,
    );
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
        let mut report = base_report;
        report.candidates_marked = 0;
        report.candidates_skipped = marked_candidates.len();
        return Ok(report);
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
            let mut report = base_report;
            report.candidates_marked = candidates_marked;
            report.candidates_skipped = unknown_shape_skips;
            return Ok(report);
        }
        Err(e) => {
            warn!(namespace, error = %e, "gc manifest re-read failed; skipping deletes");
            let mut report = base_report;
            report.candidates_marked = candidates_marked;
            report.candidates_skipped = unknown_shape_skips;
            return Ok(report);
        }
    };
    let sweep_staging = match active_staged_keys_at(store, namespace, now).await {
        Ok(staging) => staging,
        Err(e) => {
            warn!(namespace, error = %e, "gc active staging re-read failed; skipping sweep");
            let mut report = base_report;
            report.candidates_marked = candidates_marked;
            report.candidates_skipped = unknown_shape_skips;
            return Ok(report);
        }
    };
    let sweep_retained_history =
        match retained_manifest_history_reachable_keys(store, namespace).await {
            Ok(keys) => keys,
            Err(e) => {
                warn!(namespace, error = %e, "gc retained history re-read failed; skipping sweep");
                let mut report = base_report;
                report.candidates_marked = candidates_marked;
                report.candidates_skipped = unknown_shape_skips;
                return Ok(report);
            }
        };
    let sweep_reachable = reachable_keys_with_retained_history_and_staging_keys(
        namespace,
        &sweep_manifest,
        &sweep_staging,
        &sweep_retained_history,
    );
    let oldest_inflight_ms = oldest_inflight_ulid_ms(namespace, &sweep_staging);
    let known_sizes = known_reclaimable_sizes(namespace, &sweep_manifest);

    let mut retained = Vec::new();
    let mut objects_deleted = pending_report.objects_deleted;
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
        pending_deletes_deleted = pending_report.objects_deleted,
        pending_deletes_pruned = pending_report.entries_pruned,
        pending_deletes_retained = pending_report.entries_retained,
        manifest_history_pruned,
        bytes_reclaimed,
        candidates_skipped,
        "gc cycle complete"
    );

    Ok(GcCycleReport {
        candidates_marked,
        objects_deleted,
        pending_deletes_deleted: pending_report.objects_deleted,
        pending_deletes_pruned: pending_report.entries_pruned,
        pending_deletes_retained: pending_report.entries_retained,
        bytes_reclaimed,
        candidates_skipped,
    })
}

/// Reads the current authoritative manifest without consulting a cache.
///
/// # Parameters
///
/// - `store`: Borrowed object-store gateway.
/// - `namespace`: Namespace whose live manifest key is loaded.
///
/// # Returns
///
/// `Some(manifest)` when present or `None` when the manifest object is absent.
///
/// # Errors
///
/// Propagates object-store and manifest decoding errors to the enclosing phase,
/// which logs them and skips unsafe deletion work.
///
/// # Performance
///
/// Performs one full live-manifest GET.
///
/// # Examples
///
/// Both mark and sweep call this separately so a publication between phases is
/// visible to the sweep check.
async fn read_manifest_for_gc(store: &ZeppelinStore, namespace: &str) -> Result<Option<Manifest>> {
    Manifest::read(store, namespace).await
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

/// Parses only immutable artifact key shapes that GC is allowed to delete.
///
/// Valid WAL keys are `<namespace>/wal/<ulid>.wal`. Valid segment keys live
/// directly beneath `<namespace>/segments/seg_<ulid>/` and use the explicit
/// metadata or numbered sidecar names recognized by
/// `is_known_segment_artifact_name`. Extra path components, invalid ULIDs, and
/// maintenance/control objects are rejected.
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
/// object, processed sequentially.
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
    let lease_data = match store.get(&format!("{namespace}/lease.json")).await {
        Ok(data) => data,
        Err(crate::error::ZeppelinError::NotFound { .. }) => return Ok(BTreeSet::new()),
        Err(e) => return Err(e),
    };
    let lease: Lease = serde_json::from_slice(&lease_data)?;
    if lease.expires_at <= now {
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

    /// Stable namespace prefix used to make expected artifact keys readable.
    const NS: &str = "gc_ns";

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

        let reachable = reachable_keys(NS, &manifest);
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
            gc_candidates_from_manifest_delta(NS, &old_manifest, &new_manifest, commit_time);

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

        let still_unreachable = revalidate_unreachable_candidates(NS, &manifest, &candidates);

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

        let reachable = reachable_keys_with_staging(NS, &manifest, &staged);

        assert!(reachable.contains(&centroids_key(NS, "seg_live")));
        for key in staged {
            assert!(
                reachable.contains(&key),
                "active lease staged key must be treated as reachable: {key}"
            );
        }
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
            decode_gc_candidates(&legacy).unwrap(),
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
