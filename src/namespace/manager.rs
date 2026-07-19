//! Authoritative namespace metadata and lifecycle management.
//!
//! A namespace is Zeppelin's top-level isolation boundary. This module owns the
//! persisted `{namespace}/meta.json` record that describes its vector shape,
//! search configuration, lifecycle state, and compaction health. It also
//! coordinates creation with an initial manifest and deletion with a durable
//! tombstone. Vector data, WAL fragments, and segments remain the responsibility
//! of their own modules.
//!
//! S3 or MinIO is authoritative. [`crate::namespace::manager::NamespaceManager`]
//! keeps a short-lived process-local [`dashmap::DashMap`] registry only to avoid
//! repeated metadata GETs. Read-only lookups may observe a cached record until
//! its TTL expires; mutation paths reload the object and use its ETag for
//! compare-and-swap publication.
//!
//! ## Lifecycle
//!
//! ```text
//! reserve meta.json as creating with If-None-Match: *
//!                 |
//!                 v
//!          create empty manifest
//!                 |
//!                 v
//!        CAS meta.json -> active
//!                 |
//!                 v
//!              active
//!                 |
//!                 | CAS meta.json -> deleting + bind evidence key
//!                 v
//!       write immutable destruction evidence
//!                 |
//!                 v
//!     delete manifest and namespace objects
//!                 |
//!                 | delete meta.json last
//!                 v
//!              absent
//! ```
//!
//! The `deleting` record is a tombstone: it prevents ordinary reads, writes,
//! and same-name recreation while cleanup can be resumed safely. Deleting the
//! metadata object last ensures an absent namespace never hides reachable data
//! behind an incomplete cleanup.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::namespace::manager::NamespaceMetadata`] and
//!    [`crate::namespace::manager::NamespaceState`] for the persisted contract.
//! 2. Read [`crate::namespace::manager::NamespaceManager::create_with_fts_and_index_config`]
//!    for atomic name reservation and initial manifest creation.
//! 3. Read [`crate::namespace::manager::NamespaceManager::get_including_deleting`]
//!    and [`crate::namespace::manager::NamespaceManager::list`] for cache and S3
//!    discovery behavior.
//! 4. Read [`crate::namespace::graph::NamespaceGraph`] for the graph-owned,
//!    evidence-producing, resumable governed deletion protocol. The lower-level
//!    [`crate::namespace::manager::NamespaceManager::start_delete`] helper exists
//!    only for managers constructed without preservation governance.
//! 5. Finish with [`crate::namespace::manager::NamespaceManager::update_index_config`]
//!    and the compaction-health methods for ETag-protected metadata updates.
//!
//! ## Rust concepts used here
//!
//! [`dashmap::DashMap`] provides sharded concurrent access to disposable cache
//! entries, similar to Java's `ConcurrentHashMap`; C would require explicit
//! bucket locks and lifetime management. Persisted state is cloned into owned
//! values before a map guard is released, so no borrow or lock guard crosses an
//! `.await`. Mutation helpers use `Result` and exhaustive matching to separate
//! missing namespaces, CAS conflicts, deletion tombstones, and storage failure.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, instrument, warn};

use crate::config::IndexingConfig;
use crate::error::{Result, ZeppelinError};
use crate::fts::FtsFieldConfig;
use crate::index::quantization::QuantizationType;
use crate::security::{DecisionId, PreservationHeadProof, PreservationService, PrincipalId};
use crate::storage::{DeletePrefixOutcome, ObjectUserMetadata, ZeppelinStore};
use crate::time::Clock;
use crate::types::{DistanceMetric, IndexType};
use crate::wal::LeaseManager;

use super::branching::{
    ActivationNonce, ArtifactOrigin, BranchActivationEvidence, BranchError, BranchPrepareStage,
    ForkIdentity, ForkPrepareIntent, NamespaceCreationKind,
};
pub use super::types::{is_valid_namespace_name, NamespaceIncarnationId};
use super::{BranchRoot, NamespaceId};

/// Default lifetime of a process-local namespace registry entry.
const DEFAULT_NAMESPACE_REGISTRY_TTL: Duration = Duration::from_secs(5);
/// Maximum CAS attempts when adding an incarnation to legacy metadata.
const MAX_NAMESPACE_INCARNATION_MIGRATION_ATTEMPTS: usize = 8;
pub(crate) const NAMESPACE_INCARNATION_METADATA_KEY: &str = "zeppelin-namespace-incarnation";
/// Consecutive compaction failures before a namespace is reported degraded.
pub const COMPACTION_DEGRADED_FAILURE_THRESHOLD: u32 = 5;

/// Lifecycle state stored in `meta.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NamespaceState {
    /// Namespace accepts reads and writes.
    #[default]
    Active,
    /// Namespace name is reserved while its first manifest is published.
    Creating,
    /// Namespace is being deleted; clients may observe status but not use it.
    Deleting,
}

impl NamespaceState {
    /// Returns the stable lowercase representation used by API responses.
    ///
    /// # Returns
    ///
    /// Returns `"active"`, `"creating"`, or `"deleting"` without allocation.
    ///
    /// # Examples
    ///
    /// A deletion status response renders [`NamespaceState::Deleting`] as
    /// `"deleting"`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            NamespaceState::Active => "active",
            NamespaceState::Creating => "creating",
            NamespaceState::Deleting => "deleting",
        }
    }
}

/// Per-namespace indexing parameters persisted in `meta.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceIndexConfig {
    /// Number of IVF centroids/clusters to train for future compactions.
    pub nlist: usize,
    /// Vector quantization mode.
    pub quantization: QuantizationType,
    /// Number of product-quantization subquantizers.
    pub pq_m: usize,
    /// Whether future compactions build a hierarchical index.
    pub hierarchical: bool,
    /// Whether future compactions build FTS segment indexes.
    pub fts_index: bool,
    /// Whether future compactions build bitmap segment indexes.
    pub bitmap_index: bool,
}

impl NamespaceIndexConfig {
    /// Builds a persisted namespace override from server indexing defaults.
    ///
    /// # Parameters
    ///
    /// - `config`: Borrowed boot-time settings whose namespace-relevant fields
    ///   should be frozen into `meta.json`.
    ///
    /// # Returns
    ///
    /// Returns an owned, serializable configuration independent of later
    /// process-default changes.
    ///
    /// # Examples
    ///
    /// If the server default uses 256 centroids and scalar quantization, a new
    /// namespace records those choices for future compactions.
    #[must_use]
    pub fn from_indexing_config(config: &IndexingConfig) -> Self {
        Self {
            nlist: config.default_num_centroids,
            quantization: config.quantization,
            pq_m: config.pq_m,
            hierarchical: config.hierarchical,
            fts_index: config.fts_index,
            bitmap_index: config.bitmap_index,
        }
    }

    /// Overlays namespace-specific settings onto a complete server config.
    ///
    /// Settings that are not persisted per namespace, such as query limits,
    /// remain inherited from `base`.
    ///
    /// # Parameters
    ///
    /// - `base`: Borrowed current server indexing configuration.
    ///
    /// # Returns
    ///
    /// Returns an owned clone with the six namespace-controlled build fields
    /// replaced. The input is unchanged.
    ///
    /// # Performance
    ///
    /// Clones the small configuration value and performs no storage I/O.
    ///
    /// # Examples
    ///
    /// A namespace may retain `nlist = 128` while inheriting a newly tuned
    /// server-wide `default_nprobe` value.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `base.clone()` creates a separate owned structure before mutation. This
    /// resembles copying a Java value object or a C struct, but Rust prevents
    /// accidental mutation of the borrowed original.
    #[must_use]
    pub fn apply_to_indexing_config(&self, base: &IndexingConfig) -> IndexingConfig {
        let mut config = base.clone();
        config.default_num_centroids = self.nlist;
        config.quantization = self.quantization;
        config.pq_m = self.pq_m;
        config.hierarchical = self.hierarchical;
        config.fts_index = self.fts_index;
        config.bitmap_index = self.bitmap_index;
        config
    }

    /// Validates namespace index settings against vector dimensionality.
    ///
    /// # Parameters
    ///
    /// - `dimensions`: Positive vector dimension already selected for the
    ///   namespace.
    ///
    /// # Returns
    ///
    /// Returns unit when the configuration can build an index of this shape.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Validation`] when `nlist` or `pq_m` is zero, or
    /// when product quantization cannot divide the vector into equal
    /// subquantizers. Validation has no side effects.
    ///
    /// # Examples
    ///
    /// Product quantization with 384 dimensions and `pq_m = 8` is valid;
    /// `pq_m = 10` is rejected because 384 is not divisible by 10.
    pub fn validate(&self, dimensions: usize) -> Result<()> {
        if self.nlist == 0 {
            return Err(ZeppelinError::Validation(
                "index_config.nlist must be >= 1".into(),
            ));
        }
        if self.pq_m == 0 {
            return Err(ZeppelinError::Validation(
                "index_config.pq_m must be >= 1".into(),
            ));
        }
        if self.quantization == QuantizationType::Product && dimensions % self.pq_m != 0 {
            return Err(ZeppelinError::Validation(format!(
                "index_config.pq_m ({}) must divide dimensions ({}) when quantization=product",
                self.pq_m, dimensions
            )));
        }
        Ok(())
    }
}

/// Stable compaction status stored in namespace metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CompactionStatus {
    /// No compaction outcome has been recorded yet.
    #[default]
    Never,
    /// Last compaction completed successfully.
    Success,
    /// Last compaction failed.
    Failure,
}

impl CompactionStatus {
    /// Returns the stable snake-case representation used by API responses.
    ///
    /// # Returns
    ///
    /// Returns a process-long string literal and performs no allocation.
    ///
    /// # Examples
    ///
    /// [`CompactionStatus::Failure`] renders as `"failure"`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::Success => "success",
            Self::Failure => "failure",
        }
    }
}

/// Namespace compaction and index health persisted in `meta.json`.
///
/// The record is operational status, not the authority for which artifacts are
/// visible; the namespace manifest owns visibility. Defaulted fields preserve
/// compatibility with metadata written before health reporting existed.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompactionHealth {
    /// Last compaction completion/failure time.
    #[serde(default)]
    pub last_compaction_at: Option<DateTime<Utc>>,
    /// Last recorded compaction status.
    #[serde(default)]
    pub last_compaction_status: CompactionStatus,
    /// Last failure message, cleared on success.
    #[serde(default)]
    pub last_compaction_error: Option<String>,
    /// Consecutive failure count since the last success.
    #[serde(default)]
    pub consecutive_failures: u32,
}

/// Authoritative namespace metadata stored as `{namespace}/meta.json`.
///
/// This JSON object fixes the namespace's vector shape and search settings,
/// records its deletion tombstone, and exposes compaction health. New fields use
/// Serde defaults so older metadata remains readable. Changes are published as
/// whole-object ETag compare-and-swap writes rather than in-place field updates.
///
/// # Examples
///
/// An active 384-dimensional cosine namespace may have zero visible vectors,
/// an FTS configuration for `title`, and a compaction failure count. Queries
/// still discover actual WAL fragments and segments from its manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceMetadata {
    /// Unique namespace identifier.
    pub name: String,
    /// Vector dimensionality.
    pub dimensions: usize,
    /// Distance metric used for queries.
    pub distance_metric: DistanceMetric,
    /// Index algorithm type.
    pub index_type: IndexType,
    /// Total number of vectors (approximate).
    pub vector_count: u64,
    /// Timestamp when the namespace was created.
    pub created_at: DateTime<Utc>,
    /// Timestamp of the last metadata update.
    pub updated_at: DateTime<Utc>,
    /// Lifecycle state for recoverable namespace deletion.
    #[serde(default)]
    pub state: NamespaceState,
    /// Immutable destruction evidence committed by the governed delete protocol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destruction_record_key: Option<String>,
    /// Durable deletion intent installed before the destruction fence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletion_intent: Option<NamespaceDeletionIntent>,
    /// Per-field full-text search configuration.
    /// Empty map means FTS is not enabled for this namespace.
    #[serde(default)]
    pub full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
    /// Per-namespace indexing parameters. `None` means legacy metadata and is
    /// resolved from the current server config by callers.
    #[serde(default)]
    pub index_config: Option<NamespaceIndexConfig>,
    /// Compaction/index health surfaced through namespace reads.
    #[serde(default)]
    pub compaction_health: CompactionHealth,
    /// Create-only recovery family. Old metadata defaults to an ordinary root.
    #[serde(default)]
    pub creation_kind: NamespaceCreationKind,
    /// Immutable final fork proof, absent until the direct-parent root wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_identity: Option<ForkIdentity>,
    /// Immutable authorization and policy-head proof installed by branch activation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_activation: Option<BranchActivationEvidence>,
    /// Non-visible monotonic prepare milestone, valid only while `creating`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_prepare: Option<ForkPrepareIntent>,
    /// Runtime identity read from S3 user metadata, absent only for legacy
    /// objects written before incarnation IDs were introduced.
    #[serde(skip)]
    pub incarnation_id: Option<NamespaceIncarnationId>,
}

/// Exact, resumable intent for one namespace lifetime's governed deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceDeletionIntent {
    /// Namespace incarnation bound by the intent.
    pub incarnation: NamespaceIncarnationId,
    /// Deterministic immutable destruction-evidence key.
    pub destruction_record_key: String,
    /// Actor/decision evidence reference, opaque to the namespace layer.
    pub decision_evidence_ref: String,
    /// Exact activation attempt revoked before a never-visible fork entered
    /// cancellation. Absent for ordinary and already-visible deletion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_activation_nonce: Option<ActivationNonce>,
    /// Exact direct parent root identity for a branch deletion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_root: Option<BranchRoot>,
    /// Fenced manifest generation, once the deletion fence wins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fenced_generation: Option<u64>,
    /// Durable target-visibility removal marker and grace deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<VisibilityRemoval>,
    /// Parent-root release acknowledgement for a branch deletion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_release: Option<RootReleaseState>,
}

impl NamespaceDeletionIntent {
    /// Return whether this is the exact governed-deletion binding written by
    /// the pre-graph protocol.
    ///
    /// Historical intents self-referenced the destruction record and derived
    /// its key from the target incarnation. Requiring both properties prevents
    /// a merely self-referential record from being treated as legacy evidence
    /// for a different namespace lifetime.
    pub(crate) fn is_legacy_direct_evidence_binding(&self) -> bool {
        self.decision_evidence_ref == self.destruction_record_key
            && self.destruction_record_key
                == format!(
                    "_audit/destruction/{}.json",
                    self.incarnation.as_uuid().simple()
                )
    }

    /// Compare the immutable identity that authorizes one parent-root release.
    ///
    /// `root_release` is the sole field advanced by the acknowledgement CAS;
    /// every other field must still match the snapshot checked before the
    /// parent manifest mutation.
    fn has_same_root_release_identity(&self, other: &Self) -> bool {
        let mut expected = self.clone();
        expected.root_release = None;
        let mut current = other.clone();
        current.root_release = None;
        expected == current
    }
}

/// Exact namespace-lifetime identity required by graph-owned cleanup
/// primitives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GovernedDeletionIdentity {
    incarnation: NamespaceIncarnationId,
    destruction_record_key: String,
    decision_evidence_ref: String,
    fenced_generation: u64,
}

impl GovernedDeletionIdentity {
    /// Bind cleanup work to one durable intent after its manifest fence wins.
    pub(crate) fn from_intent(intent: &NamespaceDeletionIntent) -> Result<Self> {
        let fenced_generation = intent.fenced_generation.ok_or_else(|| {
            ZeppelinError::Validation(
                "governed deletion cleanup requires a fenced generation".to_string(),
            )
        })?;
        Ok(Self {
            incarnation: intent.incarnation.clone(),
            destruction_record_key: intent.destruction_record_key.clone(),
            decision_evidence_ref: intent.decision_evidence_ref.clone(),
            fenced_generation,
        })
    }
}

/// Durable evidence that a branch target's live visibility was removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilityRemoval {
    /// Deterministic marker object key under the target lifecycle prefix.
    pub marker_key: String,
    /// S3 `last_modified` observed after marker creation or adoption.
    pub observed_at: DateTime<Utc>,
    /// Earliest safe parent-root release time.
    pub not_before: DateTime<Utc>,
}

/// Durable acknowledgement of exact parent-root release.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RootReleaseState {
    /// Root release has not yet been attempted.
    Pending,
    /// Root was removed and the acknowledgement was persisted.
    Released {
        /// S3-backed timestamp at which the release acknowledgement won.
        acked_at: DateTime<Utc>,
    },
    /// Retry observed an already-absent root after all safety checks.
    Converged {
        /// S3-backed timestamp at which absence was observed safely.
        observed_at: DateTime<Utc>,
    },
}

/// Immutable evidence that authorizes removal of one fenced live manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NamespaceDestructionRecord {
    /// Namespace lifetime being destroyed.
    pub(crate) namespace: NamespaceId,
    /// Exact fenced manifest generation whose visibility root is removed.
    pub(crate) manifest_version_destroyed: u64,
    /// Exact namespace-prefix object count observed while fenced.
    pub(crate) object_count: usize,
    /// Exact namespace-prefix byte count observed while fenced.
    pub(crate) byte_count: u64,
    /// Principal that requested destruction.
    pub(crate) actor: PrincipalId,
    /// Distinct approval principal when the policy required one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) approver: Option<PrincipalId>,
    /// Authorization decision bound to the destruction request.
    pub(crate) decision_id: DecisionId,
    /// Exact parent root bound to a branch destruction, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) parent_root: Option<BranchRoot>,
    /// Exact namespace lifetime represented by the evidence, when newly minted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) incarnation: Option<NamespaceIncarnationId>,
    /// Strong preservation head observed at the evidence boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) preservation_head: Option<PreservationHeadProof>,
    /// Wall-clock evidence timestamp.
    pub(crate) ts: DateTime<Utc>,
}

impl NamespaceDestructionRecord {
    /// Validate protocol-extension fields against one durable intent.
    ///
    /// Current graph-owned evidence requires both an exact incarnation and a
    /// strong preservation head. The exact historical direct-record binding may
    /// omit either trailing field, but any incarnation it does carry must match.
    pub(crate) fn protocol_fields_match(&self, intent: &NamespaceDeletionIntent) -> bool {
        let legacy_binding = intent.is_legacy_direct_evidence_binding();
        self.incarnation
            .as_ref()
            .map_or(legacy_binding, |value| value == &intent.incarnation)
            && (self.preservation_head.is_some() || legacy_binding)
    }

    /// Strictly decode one governed-destruction evidence object.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|error| {
            ZeppelinError::Serialization(format!("destruction record is invalid: {error}"))
        })
    }

    /// Encode one governed-destruction evidence object as immutable JSON bytes.
    pub(crate) fn to_bytes(&self) -> Result<Bytes> {
        serde_json::to_vec(self).map(Bytes::from).map_err(|error| {
            ZeppelinError::Serialization(format!(
                "destruction record serialization failed: {error}"
            ))
        })
    }
}

impl NamespaceMetadata {
    /// Validate the persisted namespace-creation lifecycle as one domain.
    pub(crate) fn validate_creation_lifecycle(&self) -> Result<()> {
        match &self.creation_kind {
            NamespaceCreationKind::Root => {
                if self.branch_identity.is_some()
                    || self.branch_activation.is_some()
                    || self.branch_prepare.is_some()
                {
                    return Err(ZeppelinError::Serialization(format!(
                        "root namespace {} carries branch-only metadata",
                        self.name
                    )));
                }
                if self
                    .deletion_intent
                    .as_ref()
                    .is_some_and(|intent| intent.branch_activation_nonce.is_some())
                {
                    return Err(ZeppelinError::Serialization(format!(
                        "root namespace {} carries a branch-activation cancellation marker",
                        self.name
                    )));
                }
            }
            NamespaceCreationKind::Fork(reservation) => {
                if reservation.target_namespace.as_str() != self.name {
                    return Err(BranchError::IntentMismatch {
                        target: reservation.target_namespace.clone(),
                    }
                    .into());
                }
                if reservation.depth == 0 {
                    return Err(ZeppelinError::Serialization(format!(
                        "fork reservation {} has zero ancestry depth",
                        self.name
                    )));
                }
                if reservation.target_incarnation.is_nil()
                    || reservation.source_incarnation.is_nil()
                {
                    return Err(ZeppelinError::Serialization(format!(
                        "fork reservation {} contains a nil namespace incarnation",
                        self.name
                    )));
                }
                if let Some(incarnation) = self.incarnation_id.as_ref() {
                    if incarnation != &reservation.target_incarnation {
                        return Err(BranchError::IntentMismatch {
                            target: reservation.target_namespace.clone(),
                        }
                        .into());
                    }
                }
                if let Some(identity) = self.branch_identity.as_ref() {
                    if !identity.matches_reservation(reservation) {
                        return Err(BranchError::IntentMismatch {
                            target: reservation.target_namespace.clone(),
                        }
                        .into());
                    }
                }

                match self.state {
                    NamespaceState::Creating => {
                        if self.branch_activation.is_some() {
                            return Err(ZeppelinError::Serialization(format!(
                                "creating fork {} carries activation evidence",
                                self.name
                            )));
                        }
                        if let Some(intent) = self.deletion_intent.as_ref() {
                            if intent.incarnation != reservation.target_incarnation {
                                return Err(BranchError::IntentMismatch {
                                    target: reservation.target_namespace.clone(),
                                }
                                .into());
                            }
                            if intent.fenced_generation.is_some()
                                || intent.visibility.is_some()
                                || intent.root_release.is_some()
                                || self.destruction_record_key.is_some()
                            {
                                return Err(ZeppelinError::Serialization(format!(
                                    "creating fork {} carries active-deletion state",
                                    self.name
                                )));
                            }
                        }
                        let prepare = self.branch_prepare.as_ref().ok_or_else(|| {
                            ZeppelinError::Serialization(format!(
                                "creating fork {} has no preparation intent",
                                self.name
                            ))
                        })?;
                        if prepare.branch_id != reservation.branch_id
                            || prepare.target_incarnation != reservation.target_incarnation
                        {
                            return Err(BranchError::IntentMismatch {
                                target: reservation.target_namespace.clone(),
                            }
                            .into());
                        }
                        if let Some(intent) = self.deletion_intent.as_ref() {
                            if intent.branch_activation_nonce.is_some() {
                                let identity = self.branch_identity.as_ref().ok_or_else(|| {
                                    ZeppelinError::Serialization(format!(
                                        "activation-cancelled fork {} has no final branch identity",
                                        self.name
                                    ))
                                })?;
                                if prepare.stage != BranchPrepareStage::ManifestPublished
                                    || !identity.matches_reservation(reservation)
                                    || intent
                                        .parent_root
                                        .as_ref()
                                        .is_some_and(|root| !identity.matches_root(root))
                                {
                                    return Err(BranchError::IntentMismatch {
                                        target: reservation.target_namespace.clone(),
                                    }
                                    .into());
                                }
                                if intent.decision_evidence_ref.trim().is_empty() {
                                    return Err(ZeppelinError::Serialization(format!(
                                        "activation-cancelled fork {} has no deletion decision evidence",
                                        self.name
                                    )));
                                }
                            }
                        }
                        match prepare.stage {
                            BranchPrepareStage::Reserved => {
                                if self.branch_identity.is_some() || prepare.provisional.is_none() {
                                    return Err(ZeppelinError::Serialization(format!(
                                        "reserved fork {} must carry only provisional data-plane state",
                                        self.name
                                    )));
                                }
                                let provisional =
                                    prepare.provisional.as_ref().ok_or_else(|| {
                                        ZeppelinError::Serialization(format!(
                                            "reserved fork {} has no provisional data-plane state",
                                            self.name
                                        ))
                                    })?;
                                let full_text_search = self
                                    .full_text_search
                                    .iter()
                                    .map(|(field, config)| {
                                        serde_json::to_value(config)
                                            .map(|value| (field.clone(), value))
                                    })
                                    .collect::<std::result::Result<
                                        std::collections::BTreeMap<_, _>,
                                        _,
                                    >>()?;
                                if provisional.dimensions != self.dimensions
                                    || provisional.distance_metric != self.distance_metric
                                    || provisional.index_type != self.index_type
                                    || provisional.full_text_search != full_text_search
                                    || self.index_config.as_ref() != Some(&provisional.index_config)
                                {
                                    return Err(BranchError::IntentMismatch {
                                        target: reservation.target_namespace.clone(),
                                    }
                                    .into());
                                }
                            }
                            BranchPrepareStage::Rooted
                            | BranchPrepareStage::ManifestPublished
                            | BranchPrepareStage::ActivationPending { .. } => {
                                if self.branch_identity.is_none() || prepare.provisional.is_some() {
                                    return Err(ZeppelinError::Serialization(format!(
                                        "rooted fork {} must retain one final identity and no provisional state",
                                        self.name
                                    )));
                                }
                            }
                        }
                    }
                    NamespaceState::Active | NamespaceState::Deleting => {
                        if self
                            .deletion_intent
                            .as_ref()
                            .is_some_and(|intent| intent.branch_activation_nonce.is_some())
                        {
                            return Err(ZeppelinError::Serialization(format!(
                                "visible or deleting fork {} carries a never-visible activation cancellation marker",
                                self.name
                            )));
                        }
                        if self.branch_prepare.is_some()
                            || self.branch_identity.is_none()
                            || self.branch_activation.is_none()
                        {
                            return Err(ZeppelinError::Serialization(format!(
                                "visible or deleting fork {} must retain identity and activation evidence and clear preparation state",
                                self.name
                            )));
                        }
                        if self.branch_activation.as_ref().is_some_and(|evidence| {
                            self.branch_identity
                                .as_ref()
                                .is_none_or(|identity| !evidence.matches_identity(identity))
                        }) {
                            return Err(ZeppelinError::Serialization(format!(
                                "fork {} carries mismatched activation evidence",
                                self.name
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Return the authoritative namespace lifetime carried by object metadata.
    ///
    /// Legacy metadata can legitimately omit the incarnation. Callers may then
    /// use a matching bound manifest, but must fail loud if both authorities are
    /// unbound rather than inventing an identity.
    pub fn artifact_origin(&self) -> Result<Option<ArtifactOrigin>> {
        let Some(incarnation) = self.incarnation_id.clone() else {
            return Ok(None);
        };
        if incarnation.is_nil() {
            return Err(ZeppelinError::Serialization(format!(
                "namespace {} has a nil incarnation identity",
                self.name
            )));
        }
        let namespace = NamespaceId::parse(self.name.clone()).map_err(|_| {
            ZeppelinError::Validation(format!(
                "namespace metadata name violates namespace grammar: {}",
                self.name
            ))
        })?;
        Ok(Some(ArtifactOrigin {
            namespace,
            incarnation,
        }))
    }

    /// Builds the object-store key for a namespace metadata record.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Validated top-level namespace name without a slash.
    ///
    /// # Returns
    ///
    /// Returns an owned key in the form `{namespace}/meta.json`.
    ///
    /// # Examples
    ///
    /// Namespace `catalog` maps to `catalog/meta.json`.
    pub fn s3_key(namespace: &str) -> String {
        format!("{namespace}/meta.json")
    }

    /// Serializes this metadata record as pretty-printed JSON bytes.
    ///
    /// JSON is self-describing and therefore safe for nested types with Serde
    /// defaults and optional fields.
    ///
    /// # Returns
    ///
    /// Returns owned immutable [`Bytes`] ready for an object-store PUT.
    ///
    /// # Errors
    ///
    /// Returns the shared JSON serialization error if any nested value cannot
    /// be encoded. No storage write occurs here.
    ///
    /// # Examples
    ///
    /// A caller serializes a complete replacement immediately before an ETag
    /// CAS; fields are not patched independently in S3.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// [`Bytes`] owns or shares an immutable byte buffer. It is closer to a
    /// reference-counted read-only byte array than a C pointer-length pair;
    /// cloning it does not necessarily copy the underlying allocation.
    pub fn to_bytes(&self) -> Result<Bytes> {
        self.validate_creation_lifecycle()?;
        let json = serde_json::to_vec_pretty(self)?;
        Ok(Bytes::from(json))
    }

    /// Decodes a complete namespace metadata record from JSON bytes.
    ///
    /// # Parameters
    ///
    /// - `data`: Borrowed object body returned from authoritative storage.
    ///
    /// # Returns
    ///
    /// Returns an owned metadata value. Missing fields with `#[serde(default)]`
    /// receive compatibility defaults.
    ///
    /// # Errors
    ///
    /// Returns a JSON error when the object is malformed or contains an
    /// incompatible field representation. Corrupt metadata is never replaced
    /// with an empty namespace.
    ///
    /// # Examples
    ///
    /// Metadata written before `compaction_health` existed decodes with a
    /// default “never compacted” health record.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let metadata: Self = serde_json::from_slice(data)?;
        metadata.validate_creation_lifecycle()?;
        Ok(metadata)
    }

    fn user_metadata(&self) -> ObjectUserMetadata {
        let mut metadata = ObjectUserMetadata::new();
        if let Some(incarnation_id) = self.incarnation_id.as_ref() {
            metadata.insert(
                NAMESPACE_INCARNATION_METADATA_KEY,
                incarnation_id.as_string(),
            );
        }
        metadata
    }

    fn attach_user_metadata(mut self, metadata: &ObjectUserMetadata) -> Result<Self> {
        self.incarnation_id = metadata
            .get(NAMESPACE_INCARNATION_METADATA_KEY)
            .map(NamespaceIncarnationId::parse)
            .transpose()?;
        self.validate_creation_lifecycle()?;
        Ok(self)
    }
}

/// One disposable metadata snapshot held by the process-local registry.
#[derive(Debug, Clone)]
struct RegistryEntry {
    /// Owned metadata snapshot safe to return after releasing the map guard.
    meta: NamespaceMetadata,
    /// Monotonic instant when S3 last supplied or confirmed this snapshot.
    fetched_at: Instant,
}

/// Result of an idempotent namespace create request.
///
/// Both variants carry authoritative metadata. Callers can distinguish whether
/// this request performed initialization without treating a matching existing
/// namespace as an error.
///
/// # Examples
///
/// A deployment race yields one [`CreateNamespaceOutcome::Created`] and one
/// [`CreateNamespaceOutcome::Existing`] when both requests use identical
/// immutable settings.
#[derive(Debug, Clone)]
pub enum CreateNamespaceOutcome {
    /// The namespace did not exist and was created by this request.
    Created(NamespaceMetadata),
    /// The namespace already existed with the same immutable configuration.
    Existing(NamespaceMetadata),
}

/// Result of create-only reservation of a non-visible metadata record.
#[derive(Debug, Clone)]
pub(crate) enum ReserveMetadataOutcome {
    /// This caller created the reservation.
    Reserved(NamespaceMetadata),
    /// The name already had authoritative metadata; the caller must compare it.
    Existing(NamespaceMetadata),
}

/// Result of revoking one exact pending branch-activation attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BranchActivationRevocationOutcome {
    /// This caller won the CAS and restored the non-visible prepared state.
    Revoked,
    /// The target is already non-visible and has no matching pending attempt.
    AlreadyPrepared,
    /// The exact activation attempt already won the visibility CAS.
    ActivationCommitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchActivationRevocationPlan {
    PublishPrepared,
    Outcome(BranchActivationRevocationOutcome),
}

fn branch_activation_conflict(target: &NamespaceId) -> ZeppelinError {
    ZeppelinError::ManifestConflict {
        namespace: target.as_str().to_string(),
    }
}

fn validate_exact_branch_activation_identity(
    metadata: &NamespaceMetadata,
    target: &NamespaceId,
    expected_identity: &ForkIdentity,
) -> Result<()> {
    let matches = metadata.name == target.as_str()
        && expected_identity.target_namespace == *target
        && matches!(&metadata.creation_kind,
            NamespaceCreationKind::Fork(reservation)
                if expected_identity.matches_reservation(reservation))
        && metadata.branch_identity.as_ref() == Some(expected_identity);
    if matches {
        Ok(())
    } else {
        Err(BranchError::IntentMismatch {
            target: target.clone(),
        }
        .into())
    }
}

fn begin_branch_activation_metadata(
    metadata: &mut NamespaceMetadata,
    target: &NamespaceId,
    expected_identity: &ForkIdentity,
    nonce: ActivationNonce,
    updated_at: DateTime<Utc>,
) -> Result<bool> {
    validate_exact_branch_activation_identity(metadata, target, expected_identity)?;
    match metadata.state {
        NamespaceState::Active => return Err(branch_activation_conflict(target)),
        NamespaceState::Deleting => {
            return Err(ZeppelinError::NamespaceDeleting {
                namespace: target.as_str().to_string(),
            })
        }
        NamespaceState::Creating => {}
    }
    if metadata.deletion_intent.is_some() {
        return Err(BranchError::CancellationInProgress {
            target: target.clone(),
        }
        .into());
    }
    if metadata.branch_activation.is_some() {
        return Err(ZeppelinError::Serialization(format!(
            "creating fork {target} carries activation evidence"
        )));
    }
    let prepare = metadata.branch_prepare.as_mut().ok_or_else(|| {
        ZeppelinError::Serialization(format!("creating fork {target} has no preparation intent"))
    })?;
    match prepare.stage {
        BranchPrepareStage::ManifestPublished => {
            prepare.stage = BranchPrepareStage::ActivationPending { nonce };
            metadata.updated_at = updated_at;
            Ok(true)
        }
        BranchPrepareStage::ActivationPending { nonce: current } if current == nonce => Ok(false),
        BranchPrepareStage::ActivationPending { .. } => Err(branch_activation_conflict(target)),
        BranchPrepareStage::Reserved | BranchPrepareStage::Rooted => {
            Err(BranchError::CreatingRecoveryRequired {
                target: target.clone(),
            }
            .into())
        }
    }
}

fn commit_branch_activation_metadata(
    metadata: &mut NamespaceMetadata,
    target: &NamespaceId,
    expected_identity: &ForkIdentity,
    evidence: &BranchActivationEvidence,
    updated_at: DateTime<Utc>,
) -> Result<bool> {
    validate_exact_branch_activation_identity(metadata, target, expected_identity)?;
    if !evidence.matches_identity(expected_identity) {
        return Err(BranchError::IntentMismatch {
            target: target.clone(),
        }
        .into());
    }
    match metadata.state {
        NamespaceState::Active | NamespaceState::Deleting => {
            return if metadata.branch_activation.as_ref() == Some(evidence) {
                Ok(false)
            } else {
                Err(branch_activation_conflict(target))
            };
        }
        NamespaceState::Creating => {}
    }
    if metadata.deletion_intent.is_some() {
        return Err(BranchError::CancellationInProgress {
            target: target.clone(),
        }
        .into());
    }
    if metadata.branch_activation.is_some() {
        return Err(ZeppelinError::Serialization(format!(
            "creating fork {target} carries activation evidence"
        )));
    }
    let nonce = evidence.activation_nonce();
    let prepare = metadata.branch_prepare.as_ref().ok_or_else(|| {
        ZeppelinError::Serialization(format!("creating fork {target} has no preparation intent"))
    })?;
    if prepare.stage != (BranchPrepareStage::ActivationPending { nonce }) {
        return Err(branch_activation_conflict(target));
    }
    metadata.state = NamespaceState::Active;
    metadata.branch_prepare = None;
    metadata.branch_activation = Some(evidence.clone());
    metadata.updated_at = updated_at;
    Ok(true)
}

fn revoke_branch_activation_metadata(
    metadata: &mut NamespaceMetadata,
    target: &NamespaceId,
    expected_identity: &ForkIdentity,
    nonce: ActivationNonce,
    updated_at: DateTime<Utc>,
) -> Result<BranchActivationRevocationPlan> {
    validate_exact_branch_activation_identity(metadata, target, expected_identity)?;
    match metadata.state {
        NamespaceState::Active | NamespaceState::Deleting => {
            return if metadata
                .branch_activation
                .as_ref()
                .is_some_and(|evidence| evidence.activation_nonce() == nonce)
            {
                Ok(BranchActivationRevocationPlan::Outcome(
                    BranchActivationRevocationOutcome::ActivationCommitted,
                ))
            } else {
                Err(branch_activation_conflict(target))
            };
        }
        NamespaceState::Creating => {}
    }
    if metadata.branch_activation.is_some() {
        return Err(ZeppelinError::Serialization(format!(
            "creating fork {target} carries activation evidence"
        )));
    }
    let prepare = metadata.branch_prepare.as_mut().ok_or_else(|| {
        ZeppelinError::Serialization(format!("creating fork {target} has no preparation intent"))
    })?;
    match prepare.stage {
        BranchPrepareStage::ActivationPending { nonce: current } if current == nonce => {
            prepare.stage = BranchPrepareStage::ManifestPublished;
            metadata.updated_at = updated_at;
            Ok(BranchActivationRevocationPlan::PublishPrepared)
        }
        BranchPrepareStage::ActivationPending { .. } => Err(branch_activation_conflict(target)),
        BranchPrepareStage::Reserved
        | BranchPrepareStage::Rooted
        | BranchPrepareStage::ManifestPublished => Ok(BranchActivationRevocationPlan::Outcome(
            BranchActivationRevocationOutcome::AlreadyPrepared,
        )),
    }
}

/// Coordinates namespace CRUD against authoritative S3 metadata.
///
/// The registry is a bounded-staleness read optimization. It is disposable and
/// never used as the precondition for a metadata mutation; updates fetch the
/// current ETag and replace the complete `meta.json` object conditionally.
pub struct NamespaceManager {
    /// Shared gateway for authoritative metadata and manifest operations.
    store: ZeppelinStore,
    /// In-memory registry for fast lookups.
    registry: DashMap<String, RegistryEntry>,
    /// Maximum age at which a read-only lookup may reuse a registry entry.
    registry_ttl: Duration,
    /// Explicit wall clock used only for persisted metadata stamps.
    clock: Clock,
    /// Boot-composed preservation authority for every physical cleanup pass.
    preservation: Option<Arc<PreservationService>>,
}

impl NamespaceManager {
    /// Creates a manager using the default five-second registry TTL.
    ///
    /// # Parameters
    ///
    /// - `store`: Cloneable gateway to the authoritative object store.
    ///
    /// # Returns
    ///
    /// Returns an empty process-local manager. Existing namespaces are loaded
    /// lazily or by [`Self::scan_and_register`].
    ///
    /// # Examples
    ///
    /// A newly started node constructs a manager, then startup scans S3 to seed
    /// its disposable registry.
    pub fn new(store: ZeppelinStore) -> Self {
        Self::with_clock(store, DEFAULT_NAMESPACE_REGISTRY_TTL, Clock::system())
    }

    /// Creates a manager with an explicit metadata-cache TTL.
    ///
    /// # Parameters
    ///
    /// - `store`: Cloneable gateway to authoritative object storage.
    /// - `registry_ttl`: Maximum age for cached read-only metadata. Zero forces
    ///   every ordinary lookup to reload S3.
    ///
    /// # Returns
    ///
    /// Returns an empty manager configured with the requested TTL.
    ///
    /// # Examples
    ///
    /// Tests may use 100 milliseconds to observe a lifecycle change made by a
    /// second manager without waiting five seconds.
    #[must_use]
    pub fn new_with_registry_ttl(store: ZeppelinStore, registry_ttl: Duration) -> Self {
        Self::with_clock(store, registry_ttl, Clock::system())
    }

    /// Creates a manager with explicit registry lifetime and wall clock.
    #[must_use]
    pub fn with_clock(store: ZeppelinStore, registry_ttl: Duration, clock: Clock) -> Self {
        Self {
            store,
            registry: DashMap::new(),
            registry_ttl,
            clock,
            preservation: None,
        }
    }

    /// Attach the preservation authority to the deepest namespace purge seam.
    #[must_use]
    pub fn with_preservation_service(
        mut self,
        preservation: Option<Arc<PreservationService>>,
    ) -> Self {
        self.preservation = preservation;
        self
    }

    /// Creates an active vector namespace without full-text fields.
    ///
    /// This convenience entry point delegates to [`Self::create_with_fts`] with
    /// an empty FTS configuration and default per-namespace index settings.
    ///
    /// # Parameters
    ///
    /// - `name`: Client-selected namespace name valid as both an S3 prefix and
    ///   one URL path segment.
    /// - `dimensions`: Positive vector dimensionality.
    /// - `distance_metric`: Metric fixed for future indexing and queries.
    ///
    /// # Returns
    ///
    /// Returns the newly persisted metadata after both `meta.json` and the
    /// initial empty manifest are written.
    ///
    /// # Errors
    ///
    /// Propagates validation, create conflict, serialization, manifest, and
    /// storage errors from the full creation path.
    ///
    /// # Examples
    ///
    /// Creating `catalog` with 384 cosine dimensions reserves
    /// `catalog/meta.json` and initializes `catalog/manifest.json`.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn create(
        &self,
        name: &str,
        dimensions: usize,
        distance_metric: DistanceMetric,
    ) -> Result<NamespaceMetadata> {
        self.create_with_fts(
            name,
            dimensions,
            distance_metric,
            std::collections::HashMap::new(),
        )
        .await
    }

    /// Creates a namespace with optional per-field full-text configuration.
    ///
    /// # Parameters
    ///
    /// - `name`: Valid S3- and URL-safe namespace name.
    /// - `dimensions`: Positive vector dimensionality.
    /// - `distance_metric`: Namespace-wide vector distance metric.
    /// - `full_text_search`: Owned field-to-analyzer configuration; an empty
    ///   map disables FTS.
    ///
    /// # Returns
    ///
    /// Returns persisted metadata after successful namespace initialization.
    ///
    /// # Errors
    ///
    /// Propagates invalid FTS settings and every failure documented by
    /// [`Self::create_with_fts_and_index_config`].
    ///
    /// # Examples
    ///
    /// A namespace can enable stemming for `title` while leaving all other
    /// attributes outside the lexical index.
    #[instrument(skip(self, full_text_search), fields(namespace = name))]
    pub async fn create_with_fts(
        &self,
        name: &str,
        dimensions: usize,
        distance_metric: DistanceMetric,
        full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
    ) -> Result<NamespaceMetadata> {
        self.create_with_fts_and_index_config(
            name,
            dimensions,
            distance_metric,
            full_text_search,
            None,
        )
        .await
    }

    /// Atomically reserves a namespace name and initializes its empty manifest.
    ///
    /// The create-only `meta.json` PUT is the concurrency boundary: two creators
    /// cannot silently overwrite one another's dimensions or analyzers. The
    /// durable record remains `creating` until the first manifest exists and a
    /// metadata CAS publishes `active`. A replacement node can therefore finish
    /// an interrupted create without confusing later manifest loss with an empty
    /// namespace.
    ///
    /// ```text
    /// validate request
    ///       |
    ///       v
    /// PUT creating meta if absent ---- already exists --> conflict/deleting error
    ///       |
    ///       v
    /// write empty manifest ------- failure -------> creating meta remains
    ///       |
    ///       v
    /// CAS meta.json active ------- failure -------> restart recovery retries
    ///       |
    ///       v
    /// cache metadata and return active namespace
    /// ```
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace name accepted by [`is_valid_namespace_name`].
    /// - `dimensions`: Positive dimensionality shared by every vector.
    /// - `distance_metric`: Metric persisted for index build and query scoring.
    /// - `full_text_search`: Owned analyzer configuration for indexed fields.
    /// - `index_config`: Optional explicit segment-build settings; `None`
    ///   preserves legacy/default resolution behavior.
    ///
    /// # Returns
    ///
    /// Returns the active metadata after the name and initial manifest are
    /// persisted.
    ///
    /// # Errors
    ///
    /// Returns validation errors for an unsafe name, zero dimensions, invalid
    /// FTS settings, or incompatible index parameters. Returns
    /// [`ZeppelinError::NamespaceAlreadyExists`] when the create-only PUT loses
    /// to an active namespace, and [`ZeppelinError::NamespaceDeleting`] for a
    /// tombstoned name. Serialization and object-store failures propagate.
    ///
    /// Metadata is written before the initial manifest. If manifest creation or
    /// the activation CAS fails, a durable `creating` record remains. A later
    /// authoritative lookup explicitly resumes initialization; it never treats
    /// a missing manifest beneath `active` metadata as an empty namespace.
    ///
    /// # Side Effects
    ///
    /// Performs a conditional metadata PUT, writes an empty manifest, CASes the
    /// metadata active, inserts a registry snapshot, and emits a structured
    /// creation event.
    ///
    /// # Consistency
    ///
    /// S3 owns name uniqueness. No registry check can reserve a name. The
    /// create-only PUT prevents a time-of-check/time-of-use overwrite between
    /// nodes.
    ///
    /// # Performance
    ///
    /// The successful path performs two sequential object-store writes. A
    /// conflict may add one metadata GET to distinguish active from deleting.
    ///
    /// # Examples
    ///
    /// Two nodes concurrently create `catalog` with different dimensions. One
    /// create-only PUT wins; the other returns a conflict and cannot replace the
    /// winner's metadata.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Owned maps and optional index settings move into `NamespaceMetadata`, so
    /// the persisted value cannot borrow request memory that disappears after
    /// the async handler returns. Pattern matching keeps conflict, tombstone,
    /// and generic storage failures distinct.
    #[instrument(skip(self, full_text_search), fields(namespace = name))]
    pub async fn create_with_fts_and_index_config(
        &self,
        name: &str,
        dimensions: usize,
        distance_metric: DistanceMetric,
        full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
        index_config: Option<NamespaceIndexConfig>,
    ) -> Result<NamespaceMetadata> {
        // Validate namespace name
        if !is_valid_namespace_name(name) {
            return Err(ZeppelinError::Validation(format!(
                "invalid namespace name '{}': must be 1-255 chars, start with alphanumeric, \
                 and contain only alphanumeric, dash, underscore, or dot characters",
                name,
            )));
        }
        if dimensions == 0 {
            return Err(ZeppelinError::Validation(
                "dimensions must be > 0".to_string(),
            ));
        }
        for (field, config) in &full_text_search {
            config.validate(&format!("full_text_search.{field}"))?;
        }
        if let Some(index_config) = index_config.as_ref() {
            index_config.validate(dimensions)?;
        }

        // Atomic create: write meta.json only if it doesn't already exist.
        // Uses S3 `If-None-Match: *` (PutMode::Create) to prevent TOCTOU races
        // where two concurrent creators both pass an exists() check and silently
        // overwrite each other's configuration.
        let key = NamespaceMetadata::s3_key(name);

        let now = self.clock.now();
        let meta = NamespaceMetadata {
            name: name.to_string(),
            dimensions,
            distance_metric,
            index_type: IndexType::default(),
            vector_count: 0,
            created_at: now,
            updated_at: now,
            state: NamespaceState::Creating,
            destruction_record_key: None,
            deletion_intent: None,
            full_text_search,
            index_config,
            compaction_health: CompactionHealth::default(),
            creation_kind: NamespaceCreationKind::Root,
            branch_identity: None,
            branch_activation: None,
            branch_prepare: None,
            incarnation_id: Some(NamespaceIncarnationId::new()),
        };

        // Atomic write — returns NamespaceAlreadyExists if meta.json exists
        let user_metadata = meta.user_metadata();
        match self
            .store
            .put_if_not_exists_with_user_metadata(&key, meta.to_bytes()?, name, &user_metadata)
            .await
        {
            Ok(()) => {}
            Err(ZeppelinError::NamespaceAlreadyExists { .. }) => {
                if let Ok(existing) = self.read_metadata_from_s3(name).await {
                    if existing.state == NamespaceState::Deleting {
                        return Err(ZeppelinError::NamespaceDeleting {
                            namespace: name.to_string(),
                        });
                    }
                }
                return Err(ZeppelinError::NamespaceAlreadyExists {
                    namespace: name.to_string(),
                });
            }
            Err(e) => return Err(e),
        }

        // Publish the first manifest before exposing the namespace as active.
        // Reusing the persisted creation timestamp makes a crash retry produce
        // byte-identical generation-one history.
        let mut manifest = crate::wal::Manifest::new_at(meta.created_at);
        manifest.bind_namespace_incarnation(
            meta.incarnation_id
                .as_ref()
                .ok_or_else(|| {
                    ZeppelinError::Serialization(format!(
                        "new namespace {name} is missing its incarnation identity"
                    ))
                })?
                .as_uuid(),
        )?;
        match manifest
            .publish_initial_create_only(&self.store, name)
            .await
        {
            Ok(_) => {}
            Err(ZeppelinError::ManifestConflict { .. }) => {
                // A concurrent recovery may have published the identical
                // bootstrap and activated the namespace while this creator was
                // in flight. Adopt only that exact metadata incarnation; never
                // rebase this empty candidate over the newer live manifest.
                let current = self.read_metadata_from_s3(name).await?;
                if current.incarnation_id != meta.incarnation_id {
                    return Err(ZeppelinError::ManifestConflict {
                        namespace: name.to_string(),
                    });
                }
                let recovered = self.recover_creating_namespace(current).await?;
                let recovered = self.ensure_active(recovered)?;
                info!(namespace = name, dimensions, %distance_metric, "created namespace");
                return Ok(recovered);
            }
            Err(error) => return Err(error),
        }

        let meta = self.activate_created_namespace(name).await?;
        let meta = self.ensure_active(meta)?;

        info!(namespace = name, dimensions, %distance_metric, "created namespace");
        Ok(meta)
    }

    /// Completes a namespace whose durable name reservation is still creating.
    ///
    /// A process may stop after publishing `meta.json` but before publishing the
    /// first live manifest or activating the metadata. Only the explicit
    /// `creating` state authorizes this recovery. An `active` namespace with a
    /// missing manifest remains a loud integrity failure.
    async fn recover_creating_namespace(
        &self,
        mut meta: NamespaceMetadata,
    ) -> Result<NamespaceMetadata> {
        if meta.state != NamespaceState::Creating {
            return Ok(meta);
        }

        if let NamespaceCreationKind::Fork(reservation) = &meta.creation_kind {
            if meta.incarnation_id.as_ref() != Some(&reservation.target_incarnation) {
                return Err(BranchError::IntentMismatch {
                    target: reservation.target_namespace.clone(),
                }
                .into());
            }
            // Fork reservations are never bootstrapped as empty roots and never
            // activated by generic namespace recovery. NamespaceGraph owns all
            // non-visible preparation milestones.
            return Ok(meta);
        }

        // A legacy interrupted create has no identity in meta.json user
        // metadata. Establish or recover that identity before inspecting or
        // publishing its bootstrap manifest. Active legacy namespaces are not
        // migrated by ordinary reads; this branch is part of creation recovery,
        // which is already a mutation path.
        if meta.incarnation_id.is_none() {
            meta = self
                .read_or_migrate_namespace_incarnation(&meta.name)
                .await?;
            if meta.state != NamespaceState::Creating {
                return self.ensure_active(meta);
            }
        }

        let name = meta.name.clone();
        const MAX_BOOTSTRAP_ATTEMPTS: usize = 10;
        let mut bootstrap_ready = false;
        for _ in 0..MAX_BOOTSTRAP_ATTEMPTS {
            match crate::wal::Manifest::read(&self.store, &name).await? {
                Some(mut manifest) => {
                    let is_empty_bootstrap = manifest.fragments.is_empty()
                        && manifest.segments.is_empty()
                        && manifest.compaction_watermark.is_none()
                        && manifest.active_segment.is_none()
                        && manifest.next_sequence == 0
                        && manifest.pending_deletes.is_empty()
                        && manifest.fencing_token == 0;
                    if !is_empty_bootstrap {
                        return Err(ZeppelinError::Serialization(format!(
                        "creating namespace {name} has non-empty bootstrap manifest generation {}",
                        manifest.version()
                    )));
                    }
                    let expected_incarnation = meta
                        .incarnation_id
                        .as_ref()
                        .ok_or_else(|| {
                            ZeppelinError::Serialization(format!(
                                "creating namespace {name} is missing its incarnation identity"
                            ))
                        })?
                        .as_uuid();
                    match (manifest.version(), manifest.namespace_incarnation()) {
                        (1, None) => {
                            manifest =
                                crate::wal::Manifest::read_versioned_required_for_incarnation(
                                    &self.store,
                                    &name,
                                    expected_incarnation,
                                )
                                .await?
                                .0;
                            if manifest.namespace_incarnation() != Some(expected_incarnation) {
                                return Err(ZeppelinError::Serialization(format!(
                                    "creating namespace {name} manifest incarnation does not match metadata"
                                )));
                            }
                        }
                        (1 | 2, Some(actual)) if actual == expected_incarnation => {}
                        (_, Some(actual)) if actual != expected_incarnation => {
                            return Err(ZeppelinError::Serialization(format!(
                                "creating namespace {name} manifest incarnation does not match metadata"
                            )));
                        }
                        _ => {
                            return Err(ZeppelinError::Serialization(format!(
                                "creating namespace {name} has non-bootstrap manifest generation {}",
                                manifest.version()
                            )));
                        }
                    }
                    bootstrap_ready = true;
                    break;
                }
                None => {
                    let mut manifest = crate::wal::Manifest::new_at(meta.created_at);
                    manifest.bind_namespace_incarnation(
                        meta.incarnation_id
                            .as_ref()
                            .ok_or_else(|| {
                                ZeppelinError::Serialization(format!(
                                    "creating namespace {name} is missing its incarnation identity"
                                ))
                            })?
                            .as_uuid(),
                    )?;
                    match manifest
                        .publish_initial_create_only(&self.store, &name)
                        .await
                    {
                        Ok(_) => {
                            bootstrap_ready = true;
                            break;
                        }
                        Err(ZeppelinError::ManifestConflict { .. }) => continue,
                        Err(error) => return Err(error),
                    }
                }
            }
        }

        if !bootstrap_ready {
            return Err(ZeppelinError::ManifestConflict { namespace: name });
        }

        let recovered = self.activate_created_namespace(&name).await?;
        info!(
            namespace = %name,
            state = recovered.state.as_str(),
            "recovered interrupted namespace creation"
        );
        Ok(recovered)
    }

    /// CAS-publishes an initialized namespace as active.
    ///
    /// The live manifest must already exist before this transition is called.
    /// Re-reading metadata on every retry preserves a concurrent deletion, and
    /// observing `active` makes the operation idempotent after a lost response.
    async fn activate_created_namespace(&self, name: &str) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..10 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            match meta.state {
                NamespaceState::Active => return Ok(meta),
                NamespaceState::Deleting => return Ok(meta),
                NamespaceState::Creating => {}
            }

            if meta.deletion_intent.is_some() {
                return Err(ZeppelinError::NamespaceDeleting {
                    namespace: name.to_string(),
                });
            }

            if !matches!(meta.creation_kind, NamespaceCreationKind::Root) {
                return Err(BranchError::CreatingRecoveryRequired {
                    target: NamespaceId::parse(name.to_string()).map_err(|_| {
                        ZeppelinError::Validation(format!(
                            "invalid namespace name for activation: {name}"
                        ))
                    })?,
                }
                .into());
            }

            meta.state = NamespaceState::Active;
            meta.updated_at = self.clock.now();
            let etag = etag.unwrap_or_default();
            match self.put_metadata_if_match(&key, &meta, &etag, name).await {
                Ok(_) => {
                    self.insert_registry(meta.clone());
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// Persist the exact stale-worker fence before installing a policy guard.
    ///
    /// Only a fully prepared, non-visible fork can begin activation. A retry
    /// carrying the same nonce is idempotent; a different pending nonce or any
    /// visible state is a conflict.
    pub(crate) async fn begin_branch_activation(
        &self,
        target: &NamespaceId,
        expected_identity: &ForkIdentity,
        nonce: ActivationNonce,
    ) -> Result<NamespaceMetadata> {
        let name = target.as_str();
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..10 {
            let (mut metadata, etag) = self.read_metadata_versioned(name).await?;
            let changed = begin_branch_activation_metadata(
                &mut metadata,
                target,
                expected_identity,
                nonce,
                self.clock.now(),
            )?;
            if !changed {
                return Ok(metadata);
            }
            let etag = etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "authoritative branch metadata {key} has no non-empty ETag required for activation"
                ))
            })?;
            match self
                .put_metadata_if_match(&key, &metadata, &etag, name)
                .await
            {
                Ok(_) => {
                    self.insert_registry(metadata.clone());
                    return Ok(metadata);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(branch_activation_conflict(target))
    }

    /// Publish the sole branch-visibility boundary with immutable evidence.
    ///
    /// The evidence nonce must exactly equal the persisted pending nonce. The
    /// successful CAS installs the evidence and clears transient preparation
    /// state in one whole-object metadata replacement.
    pub(crate) async fn commit_branch_activation(
        &self,
        target: &NamespaceId,
        expected_identity: &ForkIdentity,
        evidence: BranchActivationEvidence,
    ) -> Result<NamespaceMetadata> {
        let name = target.as_str();
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..10 {
            let (mut metadata, etag) = self.read_metadata_versioned(name).await?;
            let changed = commit_branch_activation_metadata(
                &mut metadata,
                target,
                expected_identity,
                &evidence,
                self.clock.now(),
            )?;
            if !changed {
                return Ok(metadata);
            }
            let etag = etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "authoritative branch metadata {key} has no non-empty ETag required for activation"
                ))
            })?;
            match self
                .put_metadata_if_match(&key, &metadata, &etag, name)
                .await
            {
                Ok(_) => {
                    self.insert_registry(metadata.clone());
                    return Ok(metadata);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(branch_activation_conflict(target))
    }

    /// Revoke one exact pending nonce before releasing its policy-head guard.
    ///
    /// A cancellation winner restores `ManifestPublished` and remains
    /// invisible. If the exact activation already committed, the typed outcome
    /// lets guard recovery finalize without attempting to hide an active fork.
    pub(crate) async fn revoke_branch_activation(
        &self,
        target: &NamespaceId,
        expected_identity: &ForkIdentity,
        nonce: ActivationNonce,
    ) -> Result<BranchActivationRevocationOutcome> {
        let name = target.as_str();
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..10 {
            let (mut metadata, etag) = self.read_metadata_versioned(name).await?;
            match revoke_branch_activation_metadata(
                &mut metadata,
                target,
                expected_identity,
                nonce,
                self.clock.now(),
            )? {
                BranchActivationRevocationPlan::Outcome(outcome) => return Ok(outcome),
                BranchActivationRevocationPlan::PublishPrepared => {}
            }
            let etag = etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "authoritative branch metadata {key} has no non-empty ETag required for activation revocation"
                ))
            })?;
            match self
                .put_metadata_if_match(&key, &metadata, &etag, name)
                .await
            {
                Ok(_) => {
                    self.insert_registry(metadata);
                    return Ok(BranchActivationRevocationOutcome::Revoked);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(branch_activation_conflict(target))
    }

    /// Create-only reserve one complete `creating` metadata object.
    pub(crate) async fn reserve_metadata_creating(
        &self,
        meta: NamespaceMetadata,
    ) -> Result<ReserveMetadataOutcome> {
        if meta.state != NamespaceState::Creating {
            return Err(ZeppelinError::Serialization(format!(
                "namespace reservation {} is not creating",
                meta.name
            )));
        }
        meta.validate_creation_lifecycle()?;
        let key = NamespaceMetadata::s3_key(&meta.name);
        let user_metadata = meta.user_metadata();
        match self
            .store
            .put_if_not_exists_with_user_metadata(
                &key,
                meta.to_bytes()?,
                &meta.name,
                &user_metadata,
            )
            .await
        {
            Ok(()) => {
                self.insert_registry(meta.clone());
                Ok(ReserveMetadataOutcome::Reserved(meta))
            }
            Err(ZeppelinError::NamespaceAlreadyExists { .. }) => Ok(
                ReserveMetadataOutcome::Existing(self.read_metadata_versioned(&meta.name).await?.0),
            ),
            Err(error) => Err(error),
        }
    }

    /// Strongly read one exact creating fork intent and its non-empty CAS ETag.
    pub(crate) async fn read_creating_intent_strong(
        &self,
        name: &str,
    ) -> Result<(NamespaceMetadata, String)> {
        let (meta, etag) = self.read_metadata_versioned(name).await?;
        if meta.state != NamespaceState::Creating
            || !matches!(meta.creation_kind, NamespaceCreationKind::Fork(_))
        {
            let target = NamespaceId::parse(name.to_string()).map_err(|_| {
                ZeppelinError::Validation(format!("invalid branch target name: {name}"))
            })?;
            return Err(BranchError::TargetAlreadyExists { target }.into());
        }
        let etag = etag.filter(|etag| !etag.is_empty()).ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "authoritative creating metadata {name} has no non-empty ETag"
            ))
        })?;
        Ok((meta, etag))
    }

    /// CAS-publish one monotonic update to an existing creating fork intent.
    pub(crate) async fn cas_update_creating_intent(
        &self,
        meta: &NamespaceMetadata,
        etag: &str,
    ) -> Result<Option<String>> {
        if meta.state != NamespaceState::Creating
            || !matches!(meta.creation_kind, NamespaceCreationKind::Fork(_))
        {
            return Err(ZeppelinError::Serialization(format!(
                "namespace {} is not a creating fork intent",
                meta.name
            )));
        }
        meta.validate_creation_lifecycle()?;
        let key = NamespaceMetadata::s3_key(&meta.name);
        let next = self
            .put_metadata_if_match(&key, meta, etag, &meta.name)
            .await?;
        self.insert_registry(meta.clone());
        Ok(next)
    }

    /// Idempotently creates a named namespace with optional FTS settings.
    ///
    /// Same name plus identical immutable configuration returns the existing
    /// S3 metadata; same name plus different configuration remains a conflict.
    /// This keeps create-by-name useful for multi-process clients without
    /// silently changing an existing namespace's shape.
    ///
    /// # Parameters
    ///
    /// - `name`: Stable client-selected namespace identifier.
    /// - `dimensions`: Requested vector dimensionality.
    /// - `distance_metric`: Requested vector distance metric.
    /// - `full_text_search`: Requested per-field FTS configuration.
    ///
    /// # Returns
    ///
    /// Returns [`CreateNamespaceOutcome::Created`] for the winning create or
    /// [`CreateNamespaceOutcome::Existing`] when authoritative metadata already
    /// has the same immutable configuration.
    ///
    /// # Errors
    ///
    /// Propagates validation and storage errors. A same-name namespace with a
    /// different configuration remains a conflict; a deletion tombstone remains
    /// unavailable.
    ///
    /// # Examples
    ///
    /// Two deployment processes can both request the same 384-dimensional
    /// `catalog` namespace. One receives `Created`; the other receives
    /// `Existing` if every immutable setting matches.
    #[instrument(skip(self, full_text_search), fields(namespace = name))]
    pub async fn create_idempotent_with_fts(
        &self,
        name: &str,
        dimensions: usize,
        distance_metric: DistanceMetric,
        full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
    ) -> Result<CreateNamespaceOutcome> {
        self.create_idempotent_with_fts_and_index_config(
            name,
            dimensions,
            distance_metric,
            full_text_search,
            None,
        )
        .await
    }

    /// Idempotently creates or verifies a namespace including index settings.
    ///
    /// This method first attempts the same atomic creation as the non-idempotent
    /// API. On a name conflict it reloads S3, rejects deletion tombstones, and
    /// compares every immutable request field with the persisted record.
    ///
    /// # Parameters
    ///
    /// - `name`: Stable client-selected namespace identifier.
    /// - `dimensions`: Requested positive vector dimensionality.
    /// - `distance_metric`: Requested vector metric.
    /// - `full_text_search`: Requested analyzer configuration.
    /// - `index_config`: Requested optional per-namespace index configuration.
    ///
    /// # Returns
    ///
    /// Returns whether this request created the namespace or matched an existing
    /// authoritative record.
    ///
    /// # Errors
    ///
    /// Returns validation errors for incompatible index settings, a deleting
    /// error for a tombstone, or an already-exists conflict when any immutable
    /// field differs. Storage and serialization failures propagate.
    ///
    /// # Side Effects
    ///
    /// May perform the two-write create flow. The existing path performs an S3
    /// metadata GET and refreshes the registry but does not rewrite metadata.
    ///
    /// # Consistency
    ///
    /// Equality is checked against freshly loaded S3 metadata, never only the
    /// process-local registry.
    ///
    /// # Examples
    ///
    /// Repeating a create with the same FTS analyzers and `nlist` returns
    /// `Existing`. Changing `nlist` under the same name returns a conflict.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The method clones the request maps only because the first creation
    /// attempt consumes them; a conflict still needs the original logical values
    /// for comparison. Java references would remain usable automatically. Rust
    /// makes this ownership cost explicit.
    #[instrument(skip(self, full_text_search), fields(namespace = name))]
    pub async fn create_idempotent_with_fts_and_index_config(
        &self,
        name: &str,
        dimensions: usize,
        distance_metric: DistanceMetric,
        full_text_search: std::collections::HashMap<String, FtsFieldConfig>,
        index_config: Option<NamespaceIndexConfig>,
    ) -> Result<CreateNamespaceOutcome> {
        if let Some(index_config) = index_config.as_ref() {
            index_config.validate(dimensions)?;
        }
        match self
            .create_with_fts_and_index_config(
                name,
                dimensions,
                distance_metric,
                full_text_search.clone(),
                index_config.clone(),
            )
            .await
        {
            Ok(meta) => Ok(CreateNamespaceOutcome::Created(meta)),
            Err(ZeppelinError::NamespaceAlreadyExists { .. }) => {
                let existing = self.read_metadata_from_s3(name).await?;
                if existing.state == NamespaceState::Deleting {
                    return Err(ZeppelinError::NamespaceDeleting {
                        namespace: name.to_string(),
                    });
                }
                if !namespace_config_matches(
                    &existing,
                    dimensions,
                    distance_metric,
                    &full_text_search,
                    &index_config,
                )? {
                    return Err(ZeppelinError::NamespaceAlreadyExists {
                        namespace: name.to_string(),
                    });
                }
                let existing = self.recover_creating_namespace(existing).await?;
                let existing = self.ensure_active(existing)?;
                Ok(CreateNamespaceOutcome::Existing(existing))
            }
            Err(e) => Err(e),
        }
    }

    /// Returns metadata for an active namespace.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose current metadata is required.
    ///
    /// # Returns
    ///
    /// Returns an owned metadata snapshot from a fresh-enough registry entry or
    /// an S3 reload.
    ///
    /// # Errors
    ///
    /// Returns namespace-not-found for an absent metadata object,
    /// namespace-deleting for a tombstone, or the underlying storage/JSON error.
    ///
    /// # Consistency
    ///
    /// Read-only lookup may reuse metadata for at most `registry_ttl`. S3
    /// remains authoritative, and mutation paths do not use this cached value as
    /// a write precondition.
    ///
    /// # Examples
    ///
    /// A normal query receives active metadata. A namespace already marked for
    /// deletion is rejected instead of being queried during cleanup.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn get(&self, name: &str) -> Result<NamespaceMetadata> {
        let meta = self.get_including_deleting(name).await?;
        self.ensure_active(meta)
    }

    /// Returns authoritative active metadata for a guarded write, migrating a
    /// legacy `meta.json` object when its user metadata has no incarnation.
    ///
    /// This is an explicit mutation-path seam for guarded writes. Ordinary
    /// [`Self::get`] and [`Self::list`] calls remain read-only so their object-
    /// store operation contracts do not change.
    ///
    /// Returning the complete metadata snapshot keeps dimensions and other
    /// validation fields paired with the same namespace lifetime as the
    /// incarnation. The result never combines an ordinary cached metadata body
    /// with an incarnation loaded after a delete/recreate race.
    ///
    /// # Errors
    ///
    /// Returns namespace lifecycle, storage, and decoding errors unchanged.
    /// Missing or empty authoritative ETags fail before any write because an
    /// unconditional legacy migration could overwrite concurrent metadata.
    /// Exhausting the bounded CAS retries returns a manifest conflict.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn get_active_metadata_for_guarded_write(
        &self,
        name: &str,
    ) -> Result<NamespaceMetadata> {
        let meta = self.read_or_migrate_namespace_incarnation(name).await?;
        self.ensure_active(meta)
    }

    /// Loads metadata and establishes its incarnation without using the
    /// process registry as a precondition.
    ///
    /// Active and creating records are accepted. Deleting records fail before
    /// any metadata or manifest write. This narrower helper lets interrupted
    /// namespace creation recover a legacy record without turning ordinary
    /// active reads into migration writes.
    async fn read_or_migrate_namespace_incarnation(&self, name: &str) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);

        for _ in 0..MAX_NAMESPACE_INCARNATION_MIGRATION_ATTEMPTS {
            let (body, object_metadata) = match self.store.get_with_object_metadata(&key).await {
                Ok(value) => value,
                Err(ZeppelinError::NotFound { .. }) => {
                    return Err(ZeppelinError::NamespaceNotFound {
                        namespace: name.to_string(),
                    });
                }
                Err(error) => return Err(error),
            };
            let mut meta = NamespaceMetadata::from_bytes(&body)?
                .attach_user_metadata(&object_metadata.user_metadata)?;
            if meta.state == NamespaceState::Deleting {
                return Err(ZeppelinError::NamespaceDeleting {
                    namespace: meta.name,
                });
            }

            if meta.incarnation_id.is_some() {
                self.insert_registry(meta.clone());
                return Ok(meta);
            }

            let etag = object_metadata
                .e_tag
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    ZeppelinError::Serialization(format!(
                        "authoritative namespace metadata {key} has no non-empty ETag required for incarnation migration"
                    ))
                })?;
            // Mixed-version deployments may already have bound the manifest
            // before metadata received its header. Adopt that identity rather
            // than minting a conflicting lifetime. An active namespace without
            // any live manifest is an integrity error; an interrupted create is
            // allowed to establish a new identity before creating its first
            // manifest.
            let incarnation_id = match crate::wal::Manifest::read(&self.store, name).await? {
                Some(manifest) => manifest
                    .namespace_incarnation()
                    .map(NamespaceIncarnationId::from_uuid)
                    .unwrap_or_else(NamespaceIncarnationId::new),
                None if meta.state == NamespaceState::Creating => NamespaceIncarnationId::new(),
                None => {
                    return Err(ZeppelinError::Serialization(format!(
                        "active namespace {name} is missing its live manifest during incarnation migration"
                    )));
                }
            };
            let mut user_metadata = object_metadata.user_metadata;
            user_metadata.insert(
                NAMESPACE_INCARNATION_METADATA_KEY,
                incarnation_id.as_string(),
            );

            match self
                .store
                .put_if_match_with_user_metadata(&key, body, &etag, name, &user_metadata)
                .await
            {
                Ok(_) => {
                    meta.incarnation_id = Some(incarnation_id);
                    self.insert_registry(meta.clone());
                    info!(namespace = name, "migrated legacy namespace incarnation");
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// Returns namespace metadata without rejecting the deletion tombstone.
    ///
    /// This lifecycle-aware form supports deletion status and cleanup workers;
    /// ordinary request paths should prefer [`Self::get`].
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose metadata snapshot is required.
    ///
    /// # Returns
    ///
    /// Returns active or deleting metadata from the TTL registry when fresh,
    /// otherwise from S3.
    ///
    /// # Errors
    ///
    /// Returns namespace-not-found, storage, or JSON decoding errors.
    ///
    /// # Performance
    ///
    /// A fresh registry hit clones metadata without remote I/O. A miss performs
    /// one full metadata GET.
    ///
    /// # Examples
    ///
    /// A status endpoint can report `deleting` while a background worker resumes
    /// prefix cleanup.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn get_including_deleting(&self, name: &str) -> Result<NamespaceMetadata> {
        let meta = match self.fresh_registry_meta(name) {
            Some(meta) => meta,
            None => self.read_metadata_from_s3(name).await?,
        };
        self.recover_creating_namespace(meta).await
    }

    /// Loads authoritative metadata from S3 and refreshes the registry.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose `meta.json` object should be read.
    ///
    /// # Returns
    ///
    /// Returns decoded owned metadata and caches the same snapshot locally.
    ///
    /// # Errors
    ///
    /// Translates a missing object into namespace-not-found. Other storage and
    /// JSON failures propagate without inserting a replacement cache entry.
    ///
    /// # Side Effects
    ///
    /// Performs one object-store GET and updates the process-local registry.
    ///
    /// # Examples
    ///
    /// Listing discovers a `catalog/` prefix, loads `catalog/meta.json`, and
    /// registers the decoded namespace for later maintenance scans.
    async fn read_metadata_from_s3(&self, name: &str) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        match self.store.get_with_object_metadata(&key).await {
            Ok((data, object_metadata)) => {
                let meta = NamespaceMetadata::from_bytes(&data)?
                    .attach_user_metadata(&object_metadata.user_metadata)?;
                self.insert_registry(meta.clone());
                Ok(meta)
            }
            Err(ZeppelinError::NotFound { .. }) => Err(ZeppelinError::NamespaceNotFound {
                namespace: name.to_string(),
            }),
            Err(e) => Err(e),
        }
    }

    /// Loads metadata together with the object version required for CAS.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose authoritative metadata will be mutated.
    ///
    /// # Returns
    ///
    /// Returns decoded metadata and the optional ETag supplied by the backend,
    /// while refreshing the registry snapshot.
    ///
    /// # Errors
    ///
    /// Translates a missing object into namespace-not-found and propagates
    /// storage or decoding failures.
    ///
    /// # Consistency
    ///
    /// Callers must pass the returned ETag to a conditional PUT; loading a value
    /// alone does not authorize an unconditional overwrite.
    ///
    /// # Examples
    ///
    /// An index-config update reads metadata at ETag `v12`, edits an owned clone,
    /// and publishes only if `v12` remains current.
    pub(crate) async fn read_metadata_versioned(
        &self,
        name: &str,
    ) -> Result<(NamespaceMetadata, Option<String>)> {
        let key = NamespaceMetadata::s3_key(name);
        match self.store.get_with_object_metadata(&key).await {
            Ok((data, object_metadata)) => {
                let meta = NamespaceMetadata::from_bytes(&data)?
                    .attach_user_metadata(&object_metadata.user_metadata)?;
                self.insert_registry(meta.clone());
                Ok((meta, object_metadata.e_tag))
            }
            Err(ZeppelinError::NotFound { .. }) => Err(ZeppelinError::NamespaceNotFound {
                namespace: name.to_string(),
            }),
            Err(e) => Err(e),
        }
    }

    /// CAS-record the durable branch visibility marker and grace deadline.
    pub(crate) async fn record_visibility_removal(
        &self,
        name: &str,
        visibility: VisibilityRemoval,
    ) -> Result<()> {
        for _attempt in 0..8 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            let etag = etag.ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "namespace {name} metadata has no ETag for visibility CAS"
                ))
            })?;
            let intent = meta.deletion_intent.as_mut().ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "namespace {name} has no deletion intent for visibility removal"
                ))
            })?;
            if meta.state != NamespaceState::Deleting || intent.fenced_generation.is_none() {
                return Err(ZeppelinError::Validation(format!(
                    "namespace {name} visibility removal requires a fenced deletion tombstone"
                )));
            }
            if let Some(existing) = &intent.visibility {
                if existing != &visibility {
                    return Err(ZeppelinError::Validation(format!(
                        "namespace {name} visibility removal conflicts with durable intent"
                    )));
                }
                return Ok(());
            }
            intent.visibility = Some(visibility.clone());
            meta.updated_at = self.clock.now();
            match self
                .put_metadata_if_match(&NamespaceMetadata::s3_key(name), &meta, &etag, name)
                .await
            {
                Ok(_) => return Ok(()),
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ZeppelinError::Validation(format!(
            "namespace {name} visibility metadata CAS exceeded retry budget"
        )))
    }

    /// CAS-record the durable acknowledgement for an exact parent-root release.
    pub(crate) async fn record_root_release(
        &self,
        name: &str,
        expected_metadata: &NamespaceMetadata,
        expected_etag: &str,
        release: RootReleaseState,
    ) -> Result<()> {
        let namespace = NamespaceId::new(name.to_string())?;
        if expected_metadata.name != name {
            return Err(ZeppelinError::Validation(format!(
                "root-release metadata snapshot names {} instead of {name}",
                expected_metadata.name
            )));
        }
        if expected_etag.is_empty() {
            return Err(ZeppelinError::Serialization(format!(
                "namespace {name} metadata has no ETag for root-release CAS"
            )));
        }

        let mut updated = expected_metadata.clone();
        let expected_intent = expected_metadata.deletion_intent.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "namespace {name} has no deletion intent for root release"
            ))
        })?;
        if updated.state != NamespaceState::Deleting
            || expected_intent.fenced_generation.is_none()
            || expected_intent.visibility.is_none()
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} root release requires durable fenced visibility removal"
            )));
        }
        let visibility = expected_intent.visibility.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "namespace {name} root release requires durable visibility removal"
            ))
        })?;
        let release_time = match &release {
            RootReleaseState::Released { acked_at } => *acked_at,
            RootReleaseState::Converged { observed_at } => *observed_at,
            RootReleaseState::Pending => {
                return Err(ZeppelinError::Validation(format!(
                    "namespace {name} root-release acknowledgement cannot remain pending"
                )))
            }
        };
        if release_time < visibility.not_before {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} root-release acknowledgement predates its reader-safety deadline"
            )));
        }
        if matches!(
            expected_intent.root_release,
            Some(RootReleaseState::Released { .. } | RootReleaseState::Converged { .. })
        ) {
            return Ok(());
        }

        let updated_intent = updated.deletion_intent.as_mut().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "namespace {name} has no deletion intent for root release"
            ))
        })?;
        updated_intent.root_release = Some(release);
        updated.updated_at = self.clock.now();
        match self
            .put_metadata_if_match(
                &NamespaceMetadata::s3_key(name),
                &updated,
                expected_etag,
                name,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(ZeppelinError::ManifestConflict { .. }) => {
                let (current, _) = self.read_metadata_versioned(name).await?;
                let current_intent = current.deletion_intent.as_ref().ok_or_else(|| {
                    BranchError::RootReleaseIntentChanged {
                        namespace: namespace.clone(),
                    }
                })?;
                let current_release_is_final_and_safe = match current_intent.root_release.as_ref() {
                    Some(RootReleaseState::Released { acked_at }) => {
                        *acked_at >= visibility.not_before
                    }
                    Some(RootReleaseState::Converged { observed_at }) => {
                        *observed_at >= visibility.not_before
                    }
                    Some(RootReleaseState::Pending) | None => false,
                };
                if current.state == NamespaceState::Deleting
                    && current.name == expected_metadata.name
                    && current.incarnation_id == expected_metadata.incarnation_id
                    && current.creation_kind == expected_metadata.creation_kind
                    && current.branch_identity == expected_metadata.branch_identity
                    && current.destruction_record_key == expected_metadata.destruction_record_key
                    && expected_intent.has_same_root_release_identity(current_intent)
                    && current_release_is_final_and_safe
                {
                    Ok(())
                } else {
                    Err(BranchError::RootReleaseIntentChanged { namespace }.into())
                }
            }
            Err(error) => Err(error),
        }
    }

    /// CAS-publishes metadata while preserving its namespace incarnation ID.
    async fn put_metadata_if_match(
        &self,
        key: &str,
        meta: &NamespaceMetadata,
        etag: &str,
        namespace: &str,
    ) -> Result<Option<String>> {
        let user_metadata = meta.user_metadata();
        self.store
            .put_if_match_with_user_metadata(key, meta.to_bytes()?, etag, namespace, &user_metadata)
            .await
    }

    /// Returns an owned registry snapshot when it remains inside the TTL.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace key in the process-local registry.
    ///
    /// # Returns
    ///
    /// Returns `Some` cloned metadata for a fresh entry, or `None` when missing
    /// or expired. Expired entries remain stored until a later refresh or list
    /// reconciliation.
    ///
    /// # Performance
    ///
    /// Acquires one DashMap shard guard and clones the metadata. The guard is
    /// dropped before the caller can perform async work.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `and_then` keeps the map guard scoped to the closure. Returning an owned
    /// clone avoids exposing a reference whose validity depends on a concurrent
    /// map entry and lock guard.
    fn fresh_registry_meta(&self, name: &str) -> Option<NamespaceMetadata> {
        self.registry.get(name).and_then(|entry| {
            (entry.fetched_at.elapsed() < self.registry_ttl).then(|| entry.meta.clone())
        })
    }

    /// Inserts an owned metadata snapshot with a fresh local timestamp.
    ///
    /// # Parameters
    ///
    /// - `meta`: Metadata confirmed by a successful read or write.
    ///
    /// # Returns
    ///
    /// Returns unit after replacing any prior entry for the same name.
    ///
    /// # Side Effects
    ///
    /// Mutates only the disposable in-memory registry.
    fn insert_registry(&self, meta: NamespaceMetadata) {
        self.registry.insert(
            meta.name.clone(),
            RegistryEntry {
                meta,
                fetched_at: Instant::now(),
            },
        );
    }

    /// Rejects non-active lifecycle states while preserving active metadata.
    ///
    /// # Parameters
    ///
    /// - `meta`: Owned lifecycle metadata snapshot.
    ///
    /// # Returns
    ///
    /// Returns the same owned value when active.
    ///
    /// # Errors
    ///
    /// Returns namespace-deleting when cleanup has begun. A still-creating
    /// namespace becomes a retryable manifest conflict; ordinary lookup paths
    /// first attempt explicit recovery and should not normally expose it.
    ///
    /// # Examples
    ///
    /// Query setup can pass loaded metadata through this helper and fail before
    /// reading a manifest for a tombstoned namespace.
    fn ensure_active(&self, meta: NamespaceMetadata) -> Result<NamespaceMetadata> {
        match meta.state {
            NamespaceState::Active => Ok(meta),
            NamespaceState::Creating => Err(ZeppelinError::ManifestConflict {
                namespace: meta.name,
            }),
            NamespaceState::Deleting => Err(ZeppelinError::NamespaceDeleting {
                namespace: meta.name,
            }),
        }
    }

    /// Lists authoritative namespace metadata, optionally filtered by name prefix.
    ///
    /// The method performs delimiter-based discovery of top-level namespace
    /// prefixes so it does not recursively walk WAL fragments or segment
    /// objects. Each discovered prefix is confirmed by loading `meta.json`.
    /// Missing metadata is treated as a concurrently removed namespace.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Optional lexical namespace-name prefix, not an S3 directory
    ///   boundary. `None` lists every top-level namespace.
    ///
    /// # Returns
    ///
    /// Returns decoded metadata for discovered namespaces. Ordering follows the
    /// backend prefix listing and is not guaranteed.
    ///
    /// # Errors
    ///
    /// Returns listing, metadata-read, or decoding errors other than a metadata
    /// object that disappeared during the scan.
    ///
    /// # Side Effects
    ///
    /// Performs one delimiter LIST plus one metadata GET per unique prefix,
    /// refreshes found registry entries, and removes cached entries proven absent
    /// within the requested scope.
    ///
    /// # Consistency
    ///
    /// The result is a sequence of S3 observations, not a transactional bucket
    /// snapshot. Concurrent creates or deletes may appear in a later scan.
    ///
    /// # Performance
    ///
    /// Cost scales with namespace count rather than every artifact in the
    /// bucket. Metadata GETs currently run sequentially.
    ///
    /// # Examples
    ///
    /// Prefix `tenant-a` can match `tenant-a-prod` and `tenant-a-test`; the
    /// filter is applied to namespace names after top-level discovery.
    #[instrument(skip(self))]
    pub async fn list(&self, prefix: Option<&str>) -> Result<Vec<NamespaceMetadata>> {
        // List immediate namespace prefixes that have meta.json. This must use
        // delimiter listing: a recursive bucket walk would visit every WAL,
        // segment, cluster, and nested meta.json object under every namespace.
        //
        // Namespace names are top-level path components, so a test prefix such
        // as `test-<uuid>` is a partial component, not a directory. Some object
        // store backends support delimiter listing with that partial prefix,
        // while the memory backend does not. List top-level namespace prefixes
        // and apply the namespace-name prefix before reading metadata.
        let mut namespace_prefixes = self.store.list_common_prefixes("").await?;
        if let Some(prefix) = prefix {
            namespace_prefixes.retain(|namespace_prefix| {
                namespace_prefix.trim_end_matches('/').starts_with(prefix)
            });
        }

        let mut namespaces = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut found = std::collections::HashSet::new();

        for prefix in &namespace_prefixes {
            let ns_name = prefix.trim_end_matches('/');
            // Reserved control-plane roots such as `_audit/` are not namespace
            // candidates. Filtering with the same creation grammar avoids an
            // unnecessary `<reserved>/meta.json` GET on every stateless boot.
            if !is_valid_namespace_name(ns_name) {
                continue;
            }
            if seen.insert(ns_name.to_string()) {
                match self.read_metadata_from_s3(ns_name).await {
                    Ok(meta) => {
                        let meta = self.recover_creating_namespace(meta).await?;
                        // Keep non-visible fork reservations discoverable by
                        // graph maintenance without returning them from the
                        // active namespace listing.
                        found.insert(meta.name.clone());
                        if meta.state == NamespaceState::Creating
                            && matches!(meta.creation_kind, NamespaceCreationKind::Fork(_))
                        {
                            continue;
                        }
                        namespaces.push(meta);
                    }
                    Err(ZeppelinError::NamespaceNotFound { .. }) => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        self.registry.retain(|name, _| match prefix {
            Some(scope) if name.starts_with(scope) => found.contains(name),
            Some(_) => true,
            None => found.contains(name),
        });

        Ok(namespaces)
    }

    /// Synchronously deletes a namespace through the resumable tombstone protocol.
    ///
    /// On an explicitly ungoverned manager, synchronous direct delete flips
    /// `meta.json` to `deleting`, purges all namespace data while keeping the
    /// tombstone, then deletes `meta.json` last. A production manager with the
    /// preservation service attached rejects this path before any mutation;
    /// production deletion is coordinated by `NamespaceGraph` instead.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace to tombstone and purge.
    ///
    /// # Returns
    ///
    /// Returns unit only after cleanup reports complete.
    ///
    /// # Errors
    ///
    /// Returns validation before mutation when preservation governance is
    /// attached. Otherwise propagates tombstone, manifest-delete, listing,
    /// object-delete, and final metadata-delete errors. Returns
    /// namespace-delete-incomplete if an unbounded pass unexpectedly stops
    /// early. Partial cleanup remains resumable because `meta.json` stays in
    /// `deleting` state until the end.
    ///
    /// # Side Effects
    ///
    /// On an ungoverned manager, conditionally updates metadata, deletes the
    /// manifest and namespace objects, removes registry state, and finally
    /// deletes the tombstone. A governed-manager rejection has no side effects.
    ///
    /// # Examples
    ///
    /// Administrative or test cleanup can await this method when it needs the
    /// namespace fully absent before continuing.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn delete(&self, name: &str) -> Result<()> {
        self.start_delete(name).await?;
        let outcome = self.finish_delete(name, Duration::MAX).await?;
        if !outcome.complete {
            return Err(ZeppelinError::NamespaceDeleteIncomplete {
                namespace: name.to_string(),
                remaining_keys: 1,
            });
        }

        Ok(())
    }

    /// Marks a namespace deleting and removes its manifest visibility root.
    ///
    /// This lower-level administrative/test helper predates governed destruction.
    /// An explicitly ungoverned manager writes the durable metadata tombstone
    /// before removing the manifest, so ordinary namespace access is rejected
    /// before data cleanup begins. A production manager with preservation
    /// attached rejects this path before tombstoning. Production deletion instead
    /// enters the graph-owned intent/fence/evidence state machine.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose deletion should begin or resume.
    ///
    /// # Returns
    ///
    /// Returns the persisted deleting metadata. Repeating the call for an
    /// existing tombstone is idempotent.
    ///
    /// # Errors
    ///
    /// Returns validation before mutation when preservation governance is
    /// attached. Otherwise returns namespace-not-found, CAS conflict after
    /// bounded retries, serialization, or storage errors. This ungoverned helper
    /// relies on the caller honoring Zeppelin's single-writer-per-namespace
    /// contract; it does not coordinate a concurrent fork. If manifest deletion
    /// fails, the tombstone remains authoritative and cleanup can be retried.
    ///
    /// # Side Effects
    ///
    /// On an ungoverned manager, CAS-updates `meta.json`, evicts the registry
    /// entry, deletes the manifest if present, and logs the lifecycle transition.
    /// A governed-manager rejection has no side effects.
    ///
    /// # Consistency
    ///
    /// The tombstone precedes destructive cleanup. A missing manifest is accepted
    /// as already removed; no stale cached metadata is used for publication.
    /// Production callers, including security-disabled HTTP requests, enter
    /// `NamespaceGraph`, whose intent/fence protocol linearizes against forks.
    ///
    /// # Examples
    ///
    /// Tests or explicitly ungoverned administrative callers may use this helper.
    /// Governed HTTP deletion must enter `NamespaceGraph`.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn start_delete(&self, name: &str) -> Result<NamespaceMetadata> {
        if self.preservation.is_some() {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} requires governed deletion"
            )));
        }
        if let Some(manifest) = crate::wal::Manifest::read(&self.store, name).await? {
            if !manifest.branch_roots().is_empty() {
                return Err(BranchError::NamespaceHasLiveBranches {
                    namespace: name.to_string(),
                    visible_children: Vec::new(),
                    has_additional_children: true,
                }
                .into());
            }
        }
        debug_assert!(
            self.preservation.is_none(),
            "governed namespace deletion must enter NamespaceGraph"
        );

        let meta = self.mark_deleting(name).await?;
        self.registry.remove(name);
        self.remove_live_manifest(name).await?;
        info!(
            namespace = name,
            state = NamespaceState::Deleting.as_str(),
            "namespace marked deleting"
        );
        Ok(meta)
    }

    /// CAS-install a governed deletion intent while leaving live metadata active.
    pub(crate) async fn install_deletion_intent(
        &self,
        name: &str,
        decision_evidence_ref: String,
        parent_root: Option<BranchRoot>,
    ) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..8 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            if meta.incarnation_id.is_none() {
                self.read_or_migrate_namespace_incarnation(name).await?;
                continue;
            }
            if meta.state != NamespaceState::Active {
                return Err(ZeppelinError::NamespaceDeleting {
                    namespace: name.to_string(),
                });
            }
            let incarnation = meta.incarnation_id.clone().ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "authoritative namespace metadata {key} omitted its incarnation"
                ))
            })?;
            if let Some(existing) = &meta.deletion_intent {
                if existing.incarnation != incarnation || existing.parent_root != parent_root {
                    return Err(ZeppelinError::Validation(format!(
                        "namespace {name} carries a conflicting deletion intent"
                    )));
                }
                return Ok(meta);
            }
            meta.deletion_intent = Some(NamespaceDeletionIntent {
                incarnation,
                destruction_record_key: format!(
                    "_audit/destruction/{}.json",
                    meta.incarnation_id
                        .as_ref()
                        .ok_or_else(|| ZeppelinError::Serialization(format!(
                            "authoritative namespace metadata {key} omitted its incarnation"
                        )))?
                        .as_uuid()
                        .simple()
                ),
                decision_evidence_ref: decision_evidence_ref.clone(),
                branch_activation_nonce: None,
                parent_root: parent_root.clone(),
                fenced_generation: None,
                visibility: None,
                root_release: None,
            });
            meta.updated_at = self.clock.now();
            let etag = etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "authoritative namespace metadata {key} has no ETag for deletion intent"
                ))
            })?;
            match self.put_metadata_if_match(&key, &meta, &etag, name).await {
                Ok(_) => {
                    self.insert_registry(meta.clone());
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// Clear only the exact trailing intent that has not published a fence.
    pub(crate) async fn clear_unfenced_deletion_intent(
        &self,
        name: &str,
        decision_evidence_ref: &str,
    ) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..8 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            let Some(intent) = meta.deletion_intent.as_ref() else {
                return Ok(meta);
            };
            if intent.decision_evidence_ref != decision_evidence_ref {
                return Err(ZeppelinError::Validation(format!(
                    "namespace {name} deletion intent changed before clear"
                )));
            }
            if meta.state != NamespaceState::Active || intent.fenced_generation.is_some() {
                return Err(ZeppelinError::Validation(format!(
                    "namespace {name} deletion intent is no longer clearable"
                )));
            }
            let (_, manifest_version) =
                crate::wal::Manifest::read_versioned_required(&self.store, name).await?;
            if manifest_version.is_deletion_fenced() {
                return Err(ZeppelinError::Validation(format!(
                    "namespace {name} deletion intent has a published fence"
                )));
            }
            meta.deletion_intent = None;
            meta.destruction_record_key = None;
            meta.updated_at = self.clock.now();
            let etag = etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "authoritative namespace metadata {key} has no ETag for deletion intent clear"
                ))
            })?;
            match self.put_metadata_if_match(&key, &meta, &etag, name).await {
                Ok(_) => {
                    self.insert_registry(meta.clone());
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// Record the exact manifest generation won by the deletion fence.
    pub(crate) async fn record_fenced_generation(
        &self,
        name: &str,
        decision_evidence_ref: &str,
        generation: u64,
    ) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..8 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            let intent = meta.deletion_intent.as_mut().ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "namespace {name} has no deletion intent to bind a fence"
                ))
            })?;
            if intent.decision_evidence_ref != decision_evidence_ref {
                return Err(ZeppelinError::Validation(format!(
                    "namespace {name} deletion intent changed before fence acknowledgement"
                )));
            }
            match intent.fenced_generation {
                Some(existing) if existing == generation => return Ok(meta),
                Some(_) => {
                    return Err(ZeppelinError::Validation(format!(
                        "namespace {name} deletion fence generation changed"
                    )))
                }
                None => intent.fenced_generation = Some(generation),
            }
            meta.updated_at = self.clock.now();
            let etag = etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "authoritative namespace metadata {key} has no ETag for fence acknowledgement"
                ))
            })?;
            match self.put_metadata_if_match(&key, &meta, &etag, name).await {
                Ok(_) => {
                    self.insert_registry(meta.clone());
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// CAS-transition an active, fenced intent to the durable deleting state.
    pub(crate) async fn tombstone_with_intent(
        &self,
        name: &str,
        decision_evidence_ref: &str,
    ) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..8 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            let intent = meta.deletion_intent.as_ref().ok_or_else(|| {
                ZeppelinError::Validation(format!(
                    "namespace {name} has no deletion intent to tombstone"
                ))
            })?;
            if intent.decision_evidence_ref != decision_evidence_ref
                || intent.fenced_generation.is_none()
            {
                return Err(ZeppelinError::Validation(format!(
                    "namespace {name} deletion intent is not fenced"
                )));
            }
            if meta.state == NamespaceState::Deleting {
                return Ok(meta);
            }
            if meta.state != NamespaceState::Active {
                return Err(ZeppelinError::Validation(format!(
                    "namespace {name} cannot enter deleting from {}",
                    meta.state.as_str()
                )));
            }
            meta.state = NamespaceState::Deleting;
            meta.destruction_record_key = Some(intent.destruction_record_key.clone());
            meta.updated_at = self.clock.now();
            let etag = etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "authoritative namespace metadata {key} has no ETag for tombstone"
                ))
            })?;
            match self.put_metadata_if_match(&key, &meta, &etag, name).await {
                Ok(_) => {
                    self.insert_registry(meta.clone());
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// Remove a deletion-fenced live manifest after graph governance settles.
    pub(crate) async fn remove_governed_live_manifest(&self, name: &str) -> Result<()> {
        self.registry.remove(name);
        self.remove_live_manifest(name).await
    }

    async fn remove_live_manifest(&self, name: &str) -> Result<()> {
        let manifest_key = crate::wal::Manifest::s3_key(name);
        match self.store.delete(&manifest_key).await {
            Ok(()) | Err(ZeppelinError::NotFound { .. }) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Resumes bounded object cleanup and deletes the tombstone only when safe.
    ///
    /// ```text
    /// require meta.json state = deleting
    ///                 |
    ///                 v
    /// refuse graph-governed intent/evidence state
    ///                 |
    ///                 v
    /// delete prefix except meta.json ---- budget ends --> incomplete outcome
    ///                 |
    ///                 v
    /// relist and require no non-meta keys
    ///                 |
    ///                 v
    /// delete meta.json last and evict cache
    /// ```
    ///
    /// # Parameters
    ///
    /// - `name`: Tombstoned namespace whose cleanup should continue.
    /// - `budget`: Approximate time budget forwarded to paged prefix deletion.
    ///
    /// # Returns
    ///
    /// Returns the paged deletion outcome. `complete = false` means the
    /// tombstone remains and a later call must resume. A complete result means
    /// all non-metadata objects were verified absent and `meta.json` was removed.
    ///
    /// # Errors
    ///
    /// Returns namespace-not-found if no tombstone exists, validation if the
    /// namespace is still active or carries graph-governed intent/evidence,
    /// namespace-delete-incomplete if verification finds remaining objects, or
    /// storage/listing failures. Partial deletion may already have occurred.
    ///
    /// # Side Effects
    ///
    /// Lists and deletes object-store keys, deletes the metadata tombstone last,
    /// evicts local registry state, and logs completed deletion.
    ///
    /// # Consistency
    ///
    /// Prefix deletion alone is not trusted as proof of completion. This public
    /// helper accepts only explicitly ungoverned legacy tombstones; every intent-
    /// or evidence-bound deletion must resume through `NamespaceGraph`. The
    /// method relists authoritative storage and preserves the tombstone whenever
    /// any non-metadata key remains.
    ///
    /// # Performance
    ///
    /// Performs paged LIST/DELETE work. A completed pass adds a full verification
    /// LIST and one final metadata DELETE.
    ///
    /// # Examples
    ///
    /// A 25-second background pass may return incomplete after deleting a chunk;
    /// the next maintenance iteration calls this method again using the same
    /// durable tombstone.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn finish_delete(
        &self,
        name: &str,
        budget: Duration,
    ) -> Result<crate::storage::DeletePrefixOutcome> {
        let meta = self.read_metadata_from_s3(name).await?;
        if meta.state != NamespaceState::Deleting {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} is not marked deleting"
            )));
        }
        if self.preservation.is_some()
            || meta.destruction_record_key.is_some()
            || meta.deletion_intent.is_some()
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} governed deletion must resume through NamespaceGraph"
            )));
        }

        let outcome = self
            .cleanup_legacy_delete_batch(name, meta.incarnation_id.as_ref(), budget)
            .await?;

        if !outcome.complete {
            return Ok(outcome);
        }

        self.remove_legacy_deletion_metadata(name, meta.incarnation_id.as_ref())
            .await?;

        info!(
            namespace = name,
            objects_deleted = outcome.deleted + 1,
            "deleted namespace"
        );
        Ok(outcome)
    }

    /// Delete one bounded batch of namespace-owned objects while retaining the
    /// durable deletion tombstone.
    ///
    /// `NamespaceGraph` calls this only after a fresh `CleanupBatch`
    /// preservation boundary. The method deliberately does not delete
    /// `meta.json`; metadata removal is a separate graph-governed mutation.
    pub(crate) async fn cleanup_governed_delete_batch(
        &self,
        name: &str,
        identity: &GovernedDeletionIdentity,
        budget: Duration,
    ) -> Result<crate::storage::DeletePrefixOutcome> {
        let meta = self.read_metadata_from_s3(name).await?;
        self.require_cleanup_ready(name, &meta, identity).await?;
        self.delete_cleanup_batch(name, budget).await
    }

    /// Delete the durable tombstone only after authoritative storage proves
    /// every other target-owned object is absent.
    ///
    /// `NamespaceGraph` calls this only after a fresh `MetadataRemoval`
    /// preservation boundary. A relist at this seam makes metadata-last an
    /// invariant of the primitive rather than a caller convention.
    pub(crate) async fn remove_deletion_metadata(
        &self,
        name: &str,
        identity: &GovernedDeletionIdentity,
    ) -> Result<()> {
        let meta = self.read_metadata_from_s3(name).await?;
        self.require_cleanup_ready(name, &meta, identity).await?;
        self.remove_metadata_after_cleanup(name).await
    }

    /// Delete one bounded batch of a cancelled, never-active fork while
    /// retaining its exact creating metadata as the recovery handle.
    pub(crate) async fn cleanup_creating_cancellation_batch(
        &self,
        name: &str,
        expected_intent: &NamespaceDeletionIntent,
        budget: Duration,
    ) -> Result<DeletePrefixOutcome> {
        let meta = self.read_metadata_from_s3(name).await?;
        self.require_creating_cancellation_cleanup_ready(name, &meta, expected_intent)
            .await?;
        self.delete_cleanup_batch(name, budget).await
    }

    /// Remove a cancelled fork's creating metadata only after every other
    /// target-owned object is authoritatively absent.
    pub(crate) async fn remove_creating_cancellation_metadata(
        &self,
        name: &str,
        expected_intent: &NamespaceDeletionIntent,
    ) -> Result<()> {
        let meta = self.read_metadata_from_s3(name).await?;
        self.require_creating_cancellation_cleanup_ready(name, &meta, expected_intent)
            .await?;
        self.remove_metadata_after_cleanup(name).await
    }

    async fn delete_cleanup_batch(
        &self,
        name: &str,
        budget: Duration,
    ) -> Result<crate::storage::DeletePrefixOutcome> {
        self.store
            .delete_namespace_objects_paged(name, budget)
            .await
    }

    async fn remove_metadata_after_cleanup(&self, name: &str) -> Result<()> {
        let meta_key = NamespaceMetadata::s3_key(name);
        let remaining = self.store.list_namespace_objects(name).await?;
        let non_meta_remaining = remaining
            .iter()
            .filter(|key| !key.family().is_metadata())
            .count();
        if non_meta_remaining != 0 {
            return Err(ZeppelinError::NamespaceDeleteIncomplete {
                namespace: name.to_string(),
                remaining_keys: non_meta_remaining,
            });
        }
        match self.store.delete(&meta_key).await {
            Ok(()) | Err(ZeppelinError::NotFound { .. }) => {}
            Err(delete_error) => match self.read_metadata_from_s3(name).await {
                Err(ZeppelinError::NamespaceNotFound { .. }) => {}
                Ok(_) => return Err(delete_error),
                Err(read_error) => return Err(read_error),
            },
        }
        self.registry.remove(name);
        Ok(())
    }

    async fn require_creating_cancellation_cleanup_ready(
        &self,
        name: &str,
        meta: &NamespaceMetadata,
        expected_intent: &NamespaceDeletionIntent,
    ) -> Result<()> {
        let NamespaceCreationKind::Fork(reservation) = &meta.creation_kind else {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} cancellation cleanup requires a creating fork"
            )));
        };
        if meta.state != NamespaceState::Creating
            || meta.deletion_intent.as_ref() != Some(expected_intent)
            || meta.incarnation_id.as_ref() != Some(&reservation.target_incarnation)
            || expected_intent.incarnation != reservation.target_incarnation
            || expected_intent.fenced_generation.is_some()
            || expected_intent.visibility.is_some()
            || expected_intent.root_release.is_some()
            || meta.destruction_record_key.is_some()
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} cancellation cleanup identity changed"
            )));
        }
        if crate::wal::Manifest::read(&self.store, name)
            .await?
            .is_some()
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} cancellation cleanup requires manifest removal"
            )));
        }

        let evidence = NamespaceDestructionRecord::from_bytes(
            &self
                .store
                .get(&expected_intent.destruction_record_key)
                .await?,
        )?;
        let namespace = NamespaceId::new(name.to_string())?;
        if evidence.namespace != namespace
            || evidence.incarnation.as_ref() != Some(&expected_intent.incarnation)
            || evidence.parent_root != expected_intent.parent_root
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} cancellation evidence does not match its durable intent"
            )));
        }
        Ok(())
    }

    async fn require_cleanup_ready(
        &self,
        name: &str,
        meta: &NamespaceMetadata,
        identity: &GovernedDeletionIdentity,
    ) -> Result<()> {
        if meta.state != NamespaceState::Deleting {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} is not marked deleting"
            )));
        }
        if crate::wal::Manifest::read(&self.store, name)
            .await?
            .is_some()
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} cleanup requires governed manifest removal"
            )));
        }
        let incarnation = meta.incarnation_id.as_ref().ok_or_else(|| {
            ZeppelinError::Serialization(format!(
                "namespace {name} deletion metadata omitted its incarnation"
            ))
        })?;
        let intent = meta.deletion_intent.as_ref().ok_or_else(|| {
            ZeppelinError::Validation(format!(
                "namespace {name} deletion tombstone omitted its durable intent"
            ))
        })?;
        if incarnation != &identity.incarnation
            || intent.incarnation != identity.incarnation
            || intent.destruction_record_key != identity.destruction_record_key
            || intent.decision_evidence_ref != identity.decision_evidence_ref
            || intent.fenced_generation != Some(identity.fenced_generation)
            || meta.destruction_record_key.as_deref()
                != Some(identity.destruction_record_key.as_str())
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} cleanup identity does not match its durable intent"
            )));
        }
        let evidence = NamespaceDestructionRecord::from_bytes(
            &self.store.get(&identity.destruction_record_key).await?,
        )?;
        let namespace = NamespaceId::new(name.to_string())?;
        if evidence.namespace != namespace
            || evidence.manifest_version_destroyed != identity.fenced_generation
            || evidence.parent_root != intent.parent_root
            || !evidence.protocol_fields_match(intent)
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} cleanup evidence does not match its durable intent"
            )));
        }
        if matches!(meta.creation_kind, NamespaceCreationKind::Fork(_)) {
            let released = matches!(
                intent.root_release,
                Some(RootReleaseState::Released { .. } | RootReleaseState::Converged { .. })
            );
            if !released {
                return Err(ZeppelinError::Validation(format!(
                    "branch namespace {name} cleanup requires durable parent-root release"
                )));
            }
        }
        Ok(())
    }

    /// Continue a legacy ordinary tombstone that predates governed intents.
    pub(crate) async fn cleanup_legacy_delete_batch(
        &self,
        name: &str,
        expected_incarnation: Option<&NamespaceIncarnationId>,
        budget: Duration,
    ) -> Result<crate::storage::DeletePrefixOutcome> {
        let meta = self.read_metadata_from_s3(name).await?;
        self.require_legacy_cleanup_ready(name, &meta, expected_incarnation)
            .await?;
        self.delete_cleanup_batch(name, budget).await
    }

    /// Remove metadata last for a legacy ordinary tombstone.
    pub(crate) async fn remove_legacy_deletion_metadata(
        &self,
        name: &str,
        expected_incarnation: Option<&NamespaceIncarnationId>,
    ) -> Result<()> {
        let meta = self.read_metadata_from_s3(name).await?;
        self.require_legacy_cleanup_ready(name, &meta, expected_incarnation)
            .await?;
        self.remove_metadata_after_cleanup(name).await
    }

    async fn require_legacy_cleanup_ready(
        &self,
        name: &str,
        meta: &NamespaceMetadata,
        expected_incarnation: Option<&NamespaceIncarnationId>,
    ) -> Result<()> {
        if meta.state != NamespaceState::Deleting {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} is not marked deleting"
            )));
        }
        if matches!(meta.creation_kind, NamespaceCreationKind::Fork(_)) {
            return Err(ZeppelinError::Validation(format!(
                "branch namespace {name} cannot use legacy ungoverned cleanup"
            )));
        }
        if meta.incarnation_id.as_ref() != expected_incarnation {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} legacy deletion incarnation changed"
            )));
        }
        if crate::wal::Manifest::read(&self.store, name)
            .await?
            .is_some()
        {
            return Err(ZeppelinError::Validation(format!(
                "namespace {name} cleanup requires manifest removal"
            )));
        }
        Ok(())
    }

    /// Publishes new per-namespace index settings for future compactions.
    ///
    /// Existing immutable segments are not rewritten. The new settings become
    /// inputs to later compactions after the metadata CAS succeeds.
    ///
    /// # Parameters
    ///
    /// - `lease_manager`: The same namespace-writer lease domain used by WAL,
    ///   compaction, and branch-root publication.
    /// - `name`: Active namespace whose desired build settings should change.
    /// - `index_config`: Fully resolved replacement configuration.
    ///
    /// # Returns
    ///
    /// Returns the newly published metadata snapshot.
    ///
    /// # Errors
    ///
    /// Returns validation for incompatible namespace dimensions, deleting for a
    /// tombstone, storage/serialization errors, or manifest-conflict after ten
    /// CAS attempts. No segment artifact is modified by this method.
    ///
    /// # Side Effects
    ///
    /// Acquires and renews the namespace writer lease, performs at least one
    /// metadata GET and conditional PUT, updates `updated_at`, refreshes the
    /// registry on success, and releases the lease best-effort.
    ///
    /// # Consistency
    ///
    /// The writer lease is acquired before the first authoritative metadata
    /// read and renewed immediately before each metadata CAS. Every CAS retry
    /// reloads metadata and its ETag. This linearizes mutable index config with
    /// fork preparation while preserving ETag exclusion against lifecycle and
    /// health updates.
    ///
    /// # Examples
    ///
    /// Changing `nlist` from 128 to 256 affects the next segment build; segments
    /// already referenced by the manifest retain their original layout.
    #[instrument(skip(self, lease_manager, index_config), fields(namespace = name))]
    pub async fn update_index_config(
        &self,
        lease_manager: &LeaseManager,
        name: &str,
        index_config: NamespaceIndexConfig,
    ) -> Result<NamespaceMetadata> {
        let mut lease = lease_manager.acquire(name).await?;
        let result = async {
            let key = NamespaceMetadata::s3_key(name);
            for _ in 0..10 {
                let (mut meta, etag) = self.read_metadata_versioned(name).await?;
                let meta_name = meta.name.clone();
                self.ensure_active(meta.clone())?;
                index_config.validate(meta.dimensions)?;

                meta.index_config = Some(index_config.clone());
                meta.updated_at = self.clock.now();

                let renewed = lease_manager.renew(name, &lease).await?;
                if !lease_manager.validate(&renewed) {
                    return Err(ZeppelinError::LeaseExpired {
                        namespace: name.to_string(),
                    });
                }
                lease = renewed;

                let etag = etag.unwrap_or_default();
                match self
                    .put_metadata_if_match(&key, &meta, &etag, &meta_name)
                    .await
                {
                    Ok(_) => {
                        self.insert_registry(meta.clone());
                        return Ok(meta);
                    }
                    Err(ZeppelinError::ManifestConflict { .. }) => continue,
                    Err(e) => return Err(e),
                }
            }

            Err(ZeppelinError::ManifestConflict {
                namespace: name.to_string(),
            })
        }
        .await;

        if let Err(error) = lease_manager.release(name, &lease).await {
            warn!(
                namespace = name,
                error = %error,
                "index-config update lease release failed (best-effort)"
            );
        }
        result
    }

    /// Records a successful compaction and clears accumulated failure state.
    ///
    /// # Parameters
    ///
    /// - `name`: Active namespace whose latest outcome should be stored.
    ///
    /// # Returns
    ///
    /// Returns metadata with a fresh completion time, `success` status, no error
    /// message, and zero consecutive failures.
    ///
    /// # Errors
    ///
    /// Propagates namespace, CAS, serialization, and storage failures from the
    /// shared health updater.
    ///
    /// # Examples
    ///
    /// A successful run after six failures resets both the health counter and
    /// the degraded Prometheus gauge.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn record_compaction_success(&self, name: &str) -> Result<NamespaceMetadata> {
        let now = self.clock.now();
        self.update_compaction_health(name, |health| {
            health.last_compaction_at = Some(now);
            health.last_compaction_status = CompactionStatus::Success;
            health.last_compaction_error = None;
            health.consecutive_failures = 0;
        })
        .await
    }

    /// Records a failed compaction and increments consecutive failure health.
    ///
    /// # Parameters
    ///
    /// - `name`: Active namespace whose latest outcome should be stored.
    /// - `error`: Borrowed internal failure whose display text is persisted for
    ///   operator diagnostics.
    ///
    /// # Returns
    ///
    /// Returns metadata containing the failure time, status, message, and
    /// saturating consecutive-failure count.
    ///
    /// # Errors
    ///
    /// Propagates namespace, CAS, serialization, and storage failures from the
    /// shared health updater. The original compaction error is not returned by
    /// this metadata operation.
    ///
    /// # Examples
    ///
    /// The fifth consecutive failure publishes a count of five and marks the
    /// namespace degraded in metrics after the CAS succeeds.
    #[instrument(skip(self), fields(namespace = name))]
    pub async fn record_compaction_failure(
        &self,
        name: &str,
        error: &ZeppelinError,
    ) -> Result<NamespaceMetadata> {
        let message = error.to_string();
        let now = self.clock.now();
        self.update_compaction_health(name, |health| {
            health.last_compaction_at = Some(now);
            health.last_compaction_status = CompactionStatus::Failure;
            health.last_compaction_error = Some(message.clone());
            health.consecutive_failures = health.consecutive_failures.saturating_add(1);
        })
        .await
    }

    /// Applies and publishes one retry-safe compaction-health transformation.
    ///
    /// The closure may run more than once. Each CAS conflict reloads the latest
    /// metadata, reapplies the transformation, and attempts a new conditional
    /// PUT so concurrent metadata changes are preserved.
    ///
    /// # Parameters
    ///
    /// - `name`: Active namespace whose health record should change.
    /// - `update`: Reusable transformation applied to each freshly loaded health
    ///   record.
    ///
    /// # Returns
    ///
    /// Returns the metadata version whose conditional PUT succeeded.
    ///
    /// # Errors
    ///
    /// Returns namespace/deleting, serialization, storage, or final CAS conflict
    /// errors after ten attempts.
    ///
    /// # Side Effects
    ///
    /// Performs versioned metadata reads and conditional writes, refreshes the
    /// registry, and updates the degraded gauge only after publication succeeds.
    ///
    /// # Consistency
    ///
    /// ETag CAS prevents a health update from overwriting a concurrent index
    /// setting or deletion transition. The metric mirrors only committed
    /// metadata.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The parameter is `impl Fn`, not `FnOnce`, because a CAS retry may invoke
    /// it repeatedly. Rust encodes that reuse requirement in the type system;
    /// Java would rely on a reusable functional object, while C would pass a
    /// function pointer plus explicit context.
    async fn update_compaction_health(
        &self,
        name: &str,
        update: impl Fn(&mut CompactionHealth),
    ) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..10 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            let meta_name = meta.name.clone();
            self.ensure_active(meta.clone())?;

            update(&mut meta.compaction_health);
            meta.updated_at = self.clock.now();
            let degraded = meta.compaction_health.consecutive_failures
                >= COMPACTION_DEGRADED_FAILURE_THRESHOLD;
            let etag = etag.unwrap_or_default();
            match self
                .put_metadata_if_match(&key, &meta, &etag, &meta_name)
                .await
            {
                Ok(_) => {
                    self.insert_registry(meta.clone());
                    crate::metrics::COMPACTION_NAMESPACE_DEGRADED
                        .with_label_values(&[name])
                        .set(i64::from(degraded));
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// CAS-transitions active metadata to the durable deleting state.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace whose tombstone should be created or observed.
    ///
    /// # Returns
    ///
    /// Returns existing deleting metadata idempotently, or the newly published
    /// tombstone after a successful conditional PUT.
    ///
    /// # Errors
    ///
    /// Returns namespace-not-found, storage/serialization errors, or conflict
    /// after two CAS attempts.
    ///
    /// # Consistency
    ///
    /// Each retry reloads S3 and its ETag. The process registry is refreshed
    /// only with metadata read from or successfully written to S3.
    async fn mark_deleting(&self, name: &str) -> Result<NamespaceMetadata> {
        let key = NamespaceMetadata::s3_key(name);
        for _ in 0..8 {
            let (mut meta, etag) = self.read_metadata_versioned(name).await?;
            if let Some(manifest) = crate::wal::Manifest::read(&self.store, name).await? {
                if !manifest.branch_roots().is_empty() {
                    return Err(BranchError::NamespaceHasLiveBranches {
                        namespace: name.to_string(),
                        visible_children: Vec::new(),
                        has_additional_children: true,
                    }
                    .into());
                }
            }
            if meta.destruction_record_key.is_some() || meta.deletion_intent.is_some() {
                return Err(ZeppelinError::Validation(format!(
                    "namespace {name} requires governed deletion"
                )));
            }
            if meta.state == NamespaceState::Deleting {
                return Ok(meta);
            }

            meta.state = NamespaceState::Deleting;

            meta.updated_at = self.clock.now();
            let etag = etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                ZeppelinError::Serialization(format!(
                    "authoritative namespace metadata {key} has no non-empty ETag required for deletion"
                ))
            })?;
            match self.put_metadata_if_match(&key, &meta, &etag, name).await {
                Ok(_) => {
                    self.insert_registry(meta.clone());
                    return Ok(meta);
                }
                Err(ZeppelinError::ManifestConflict { .. }) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(ZeppelinError::ManifestConflict {
            namespace: name.to_string(),
        })
    }

    /// Scans S3 for namespaces and seeds the process-local registry.
    ///
    /// Startup uses this to discover pre-existing data on a stateless node.
    ///
    /// # Returns
    ///
    /// Returns the number of namespace metadata records found by
    /// [`Self::list`].
    ///
    /// # Errors
    ///
    /// Propagates list or metadata-load errors. Successfully loaded entries may
    /// already be present in the registry when a later entry fails.
    ///
    /// # Side Effects
    ///
    /// Performs the full namespace list flow, refreshes the registry, and logs
    /// the discovered count.
    ///
    /// # Examples
    ///
    /// A replacement compute node starts with an empty registry, scans the same
    /// S3 bucket, and discovers the namespaces created by earlier nodes.
    #[instrument(skip(self))]
    pub async fn scan_and_register(&self) -> Result<usize> {
        let namespaces = self.list(None).await?;
        let count = namespaces.len();

        info!(namespaces = count, "scanned and registered namespaces");
        Ok(count)
    }

    /// Checks only whether a name is present in the local registry.
    ///
    /// # Parameters
    ///
    /// - `name`: Namespace key to inspect.
    ///
    /// # Returns
    ///
    /// Returns `true` for any cached entry, including expired or deleting
    /// metadata. It does not prove current S3 existence.
    ///
    /// # Examples
    ///
    /// Tests can confirm that a successful create seeded the registry, but
    /// production authorization must use [`Self::get`] rather than this helper.
    pub fn exists_in_registry(&self, name: &str) -> bool {
        self.registry.contains_key(name)
    }

    /// Snapshots cached namespaces for background maintenance loops.
    ///
    /// This deliberately does not enforce registry TTL. Maintenance scans use
    /// the registry as a work-discovery hint and revalidate authoritative state
    /// inside the operation they run.
    ///
    /// # Parameters
    ///
    /// - `prefix`: Optional lexical namespace-name prefix.
    ///
    /// # Returns
    ///
    /// Returns owned metadata clones in unspecified map iteration order.
    ///
    /// # Performance
    ///
    /// Iterates the process-local DashMap and clones every matching metadata
    /// record; it performs no object-store I/O.
    ///
    /// # Examples
    ///
    /// The compaction scheduler snapshots cached `tenant-a` namespaces, then
    /// each compaction path verifies current leases, metadata, and manifests.
    #[must_use]
    pub fn cached_namespaces(&self, prefix: Option<&str>) -> Vec<NamespaceMetadata> {
        self.registry
            .iter()
            .filter_map(|entry| {
                let meta = &entry.value().meta;
                if meta.state == NamespaceState::Creating {
                    return None;
                }
                match prefix {
                    Some(prefix) if !meta.name.starts_with(prefix) => None,
                    _ => Some(meta.clone()),
                }
            })
            .collect()
    }
}

/// Compares an idempotent create request with persisted immutable settings.
///
/// # Parameters
///
/// - `existing`: Fresh authoritative namespace metadata.
/// - `dimensions`: Requested vector dimensionality.
/// - `distance_metric`: Requested vector metric.
/// - `full_text_search`: Requested FTS field configuration.
/// - `index_config`: Requested optional index settings.
///
/// # Returns
///
/// Returns `true` only when every immutable setting matches. FTS maps are
/// compared through their serialized JSON value representation.
///
/// # Errors
///
/// Returns a serialization error if either FTS configuration cannot be
/// represented as JSON.
///
/// # Examples
///
/// Map insertion order does not create a false conflict, but changing one
/// analyzer or the vector dimensions returns `false`.
fn namespace_config_matches(
    existing: &NamespaceMetadata,
    dimensions: usize,
    distance_metric: DistanceMetric,
    full_text_search: &std::collections::HashMap<String, FtsFieldConfig>,
    index_config: &Option<NamespaceIndexConfig>,
) -> Result<bool> {
    Ok(existing.dimensions == dimensions
        && existing.distance_metric == distance_metric
        && fts_config_value(&existing.full_text_search)? == fts_config_value(full_text_search)?
        && existing.index_config == *index_config)
}

/// Converts an FTS configuration map into its order-independent JSON value.
///
/// # Parameters
///
/// - `full_text_search`: Borrowed field configuration map.
///
/// # Returns
///
/// Returns an owned JSON object used for semantic equality comparison.
///
/// # Errors
///
/// Returns the shared JSON serialization error if a nested configuration cannot
/// be represented.
///
/// # Rust Notes for Java/C Engineers
///
/// The borrowed map is not consumed. Serde constructs an owned value tree,
/// comparable to a Java JSON DOM; in C both allocation and recursive cleanup
/// would need explicit management.
fn fts_config_value(
    full_text_search: &std::collections::HashMap<String, FtsFieldConfig>,
) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(full_text_search)?)
}

#[cfg(test)]
mod tests {
    //! Unit tests for the namespace-name boundary shared by S3 keys and routes.

    use super::*;
    use crate::namespace::branching::{
        BranchActivationEvidence, ForkReservationIdentity, PolicyHeadIdentity,
    };
    use crate::namespace::{
        ForkViewDigest, ManifestDigest, ManifestGeneration, SourceDataPlaneConfigDigest,
    };

    fn prepared_fork_metadata() -> Result<(NamespaceMetadata, NamespaceId, ForkIdentity)> {
        let now = Utc::now();
        let source_namespace = NamespaceId::parse("activation-source")
            .map_err(|_| ZeppelinError::Validation("invalid test source".to_string()))?;
        let target_namespace = NamespaceId::parse("activation-target")
            .map_err(|_| ZeppelinError::Validation("invalid test target".to_string()))?;
        let source_incarnation = NamespaceIncarnationId::new();
        let target_incarnation = NamespaceIncarnationId::new();
        let branch_id = crate::namespace::BranchId::new();
        let reservation = ForkReservationIdentity {
            branch_id,
            source_namespace: source_namespace.clone(),
            source_incarnation: source_incarnation.clone(),
            target_namespace: target_namespace.clone(),
            target_incarnation: target_incarnation.clone(),
            created_at: now,
            depth: 1,
        };
        let identity = ForkIdentity {
            branch_id,
            source_namespace,
            source_incarnation,
            target_namespace: target_namespace.clone(),
            target_incarnation: target_incarnation.clone(),
            created_at: now,
            depth: 1,
            source_generation: ManifestGeneration::new(7)?,
            source_manifest_sha256: ManifestDigest::new([7; 32]),
            fork_view_sha256: ForkViewDigest::new([8; 32]),
            source_config_sha256: SourceDataPlaneConfigDigest::new([9; 32]),
            target_generation: ManifestGeneration::new(1)?,
            target_manifest_sha256: ManifestDigest::new([10; 32]),
        };
        let metadata = NamespaceMetadata {
            name: target_namespace.as_str().to_string(),
            dimensions: 4,
            distance_metric: DistanceMetric::Euclidean,
            index_type: IndexType::IvfFlat,
            vector_count: 0,
            created_at: now,
            updated_at: now,
            state: NamespaceState::Creating,
            destruction_record_key: None,
            deletion_intent: None,
            full_text_search: std::collections::HashMap::new(),
            index_config: None,
            compaction_health: CompactionHealth::default(),
            creation_kind: NamespaceCreationKind::Fork(reservation),
            branch_identity: Some(identity.clone()),
            branch_activation: None,
            branch_prepare: Some(ForkPrepareIntent {
                branch_id,
                target_incarnation: target_incarnation.clone(),
                stage: BranchPrepareStage::ManifestPublished,
                provisional: None,
            }),
            incarnation_id: Some(target_incarnation),
        };
        Ok((metadata, target_namespace, identity))
    }

    fn boot_activation_evidence(
        identity: &ForkIdentity,
        nonce: ActivationNonce,
    ) -> Result<BranchActivationEvidence> {
        Ok(BranchActivationEvidence {
            branch_id: identity.branch_id,
            target_namespace: identity.target_namespace.clone(),
            target_incarnation: identity.target_incarnation.clone(),
            policy_head: PolicyHeadIdentity::Boot {
                activation_nonce: nonce,
            },
            decision_id: DecisionId::new(),
            approver: Some(PrincipalId::new("activation-approver")?),
            audit_evidence_ref: "audit://branch-activation/request-1".to_string(),
            activated_at: Utc::now(),
        })
    }

    fn activation_cancellation_intent(
        identity: &ForkIdentity,
        nonce: ActivationNonce,
    ) -> NamespaceDeletionIntent {
        NamespaceDeletionIntent {
            incarnation: identity.target_incarnation.clone(),
            destruction_record_key: format!(
                "_audit/destruction/{}.json",
                identity.target_incarnation.as_uuid().simple()
            ),
            decision_evidence_ref: "_audit/deletion-decisions/cancel-activation.json".to_string(),
            branch_activation_nonce: Some(nonce),
            parent_root: Some(BranchRoot {
                branch_id: identity.branch_id,
                source_generation: identity.source_generation,
                source_manifest_sha256: identity.source_manifest_sha256,
                fork_view_sha256: identity.fork_view_sha256,
                source_config_sha256: identity.source_config_sha256,
                target_namespace: identity.target_namespace.clone(),
                target_incarnation: identity.target_incarnation.clone(),
                created_at: identity.created_at,
            }),
            fenced_generation: None,
            visibility: None,
            root_release: None,
        }
    }

    #[test]
    fn cancellation_winning_exact_nonce_fences_the_stale_activator() -> Result<()> {
        let (mut metadata, target, identity) = prepared_fork_metadata()?;
        let nonce = ActivationNonce::new();
        let evidence = boot_activation_evidence(&identity, nonce)?;
        assert!(begin_branch_activation_metadata(
            &mut metadata,
            &target,
            &identity,
            nonce,
            Utc::now(),
        )?);

        assert_eq!(
            revoke_branch_activation_metadata(
                &mut metadata,
                &target,
                &identity,
                nonce,
                Utc::now(),
            )?,
            BranchActivationRevocationPlan::PublishPrepared
        );
        assert_eq!(metadata.state, NamespaceState::Creating);
        assert_eq!(
            metadata
                .branch_prepare
                .as_ref()
                .map(|prepare| prepare.stage),
            Some(BranchPrepareStage::ManifestPublished)
        );
        metadata.validate_creation_lifecycle()?;

        let error = commit_branch_activation_metadata(
            &mut metadata,
            &target,
            &identity,
            &evidence,
            Utc::now(),
        )
        .expect_err("a stale activator must not cross the visibility boundary");
        assert!(matches!(error, ZeppelinError::ManifestConflict { .. }));
        assert_eq!(metadata.state, NamespaceState::Creating);
        assert!(metadata.branch_activation.is_none());

        assert_eq!(
            revoke_branch_activation_metadata(
                &mut metadata,
                &target,
                &identity,
                nonce,
                Utc::now(),
            )?,
            BranchActivationRevocationPlan::Outcome(
                BranchActivationRevocationOutcome::AlreadyPrepared
            )
        );
        Ok(())
    }

    #[test]
    fn activation_winning_exact_nonce_is_not_hidden_by_stale_cancellation() -> Result<()> {
        let (mut metadata, target, identity) = prepared_fork_metadata()?;
        let nonce = ActivationNonce::new();
        let evidence = boot_activation_evidence(&identity, nonce)?;
        assert!(begin_branch_activation_metadata(
            &mut metadata,
            &target,
            &identity,
            nonce,
            Utc::now(),
        )?);
        assert!(commit_branch_activation_metadata(
            &mut metadata,
            &target,
            &identity,
            &evidence,
            Utc::now(),
        )?);
        metadata.validate_creation_lifecycle()?;
        assert_eq!(metadata.state, NamespaceState::Active);
        assert_eq!(metadata.branch_activation.as_ref(), Some(&evidence));

        assert!(
            !commit_branch_activation_metadata(
                &mut metadata,
                &target,
                &identity,
                &evidence,
                Utc::now(),
            )?,
            "an exact lost-response retry must observe the committed evidence without a write"
        );

        let mut conflicting_evidence = evidence.clone();
        conflicting_evidence.decision_id = DecisionId::new();
        assert!(matches!(
            commit_branch_activation_metadata(
                &mut metadata,
                &target,
                &identity,
                &conflicting_evidence,
                Utc::now(),
            ),
            Err(ZeppelinError::ManifestConflict { .. })
        ));

        assert_eq!(
            revoke_branch_activation_metadata(
                &mut metadata,
                &target,
                &identity,
                nonce,
                Utc::now(),
            )?,
            BranchActivationRevocationPlan::Outcome(
                BranchActivationRevocationOutcome::ActivationCommitted
            )
        );
        assert_eq!(metadata.state, NamespaceState::Active);
        assert_eq!(metadata.branch_activation.as_ref(), Some(&evidence));

        metadata.state = NamespaceState::Deleting;
        metadata.validate_creation_lifecycle()?;
        assert_eq!(
            revoke_branch_activation_metadata(
                &mut metadata,
                &target,
                &identity,
                nonce,
                Utc::now(),
            )?,
            BranchActivationRevocationPlan::Outcome(
                BranchActivationRevocationOutcome::ActivationCommitted
            )
        );
        metadata.branch_activation = None;
        assert!(metadata.validate_creation_lifecycle().is_err());
        Ok(())
    }

    #[test]
    fn exact_begin_retry_is_idempotent_and_different_nonce_conflicts() -> Result<()> {
        let (mut metadata, target, identity) = prepared_fork_metadata()?;
        let nonce = ActivationNonce::new();
        let first_update = Utc::now();
        assert!(begin_branch_activation_metadata(
            &mut metadata,
            &target,
            &identity,
            nonce,
            first_update,
        )?);
        assert_eq!(metadata.updated_at, first_update);

        assert!(
            !begin_branch_activation_metadata(
                &mut metadata,
                &target,
                &identity,
                nonce,
                first_update + chrono::Duration::seconds(1),
            )?,
            "the same nonce must observe the pending attempt without a metadata write"
        );
        assert_eq!(metadata.updated_at, first_update);
        assert!(matches!(
            begin_branch_activation_metadata(
                &mut metadata,
                &target,
                &identity,
                ActivationNonce::new(),
                first_update + chrono::Duration::seconds(2),
            ),
            Err(ZeppelinError::ManifestConflict { .. })
        ));
        assert_eq!(
            metadata
                .branch_prepare
                .as_ref()
                .map(|prepare| prepare.stage),
            Some(BranchPrepareStage::ActivationPending { nonce })
        );
        metadata.validate_creation_lifecycle()?;
        Ok(())
    }

    #[test]
    fn stale_nonce_cannot_revoke_or_commit_a_newer_attempt() -> Result<()> {
        let (mut metadata, target, identity) = prepared_fork_metadata()?;
        let current = ActivationNonce::new();
        let stale = ActivationNonce::new();
        assert!(begin_branch_activation_metadata(
            &mut metadata,
            &target,
            &identity,
            current,
            Utc::now(),
        )?);
        let stale_revoke =
            revoke_branch_activation_metadata(&mut metadata, &target, &identity, stale, Utc::now());
        assert!(matches!(
            stale_revoke,
            Err(ZeppelinError::ManifestConflict { .. })
        ));
        let stale_evidence = boot_activation_evidence(&identity, stale)?;
        assert!(matches!(
            commit_branch_activation_metadata(
                &mut metadata,
                &target,
                &identity,
                &stale_evidence,
                Utc::now(),
            ),
            Err(ZeppelinError::ManifestConflict { .. })
        ));
        assert_eq!(
            metadata
                .branch_prepare
                .as_ref()
                .map(|prepare| prepare.stage),
            Some(BranchPrepareStage::ActivationPending { nonce: current })
        );
        let current_evidence = boot_activation_evidence(&identity, current)?;
        assert!(commit_branch_activation_metadata(
            &mut metadata,
            &target,
            &identity,
            &current_evidence,
            Utc::now(),
        )?);
        let stale_revoke =
            revoke_branch_activation_metadata(&mut metadata, &target, &identity, stale, Utc::now());
        assert!(matches!(
            stale_revoke,
            Err(ZeppelinError::ManifestConflict { .. })
        ));
        assert_eq!(metadata.state, NamespaceState::Active);
        assert_eq!(metadata.branch_activation.as_ref(), Some(&current_evidence));
        Ok(())
    }

    #[test]
    fn branch_activation_evidence_is_required_only_after_visibility() -> Result<()> {
        let (mut metadata, _, identity) = prepared_fork_metadata()?;
        metadata.validate_creation_lifecycle()?;

        let nonce = ActivationNonce::new();
        metadata.branch_activation = Some(boot_activation_evidence(&identity, nonce)?);
        assert!(metadata.validate_creation_lifecycle().is_err());

        metadata.branch_prepare = None;
        metadata.state = NamespaceState::Active;
        metadata.branch_activation = None;
        assert!(metadata.validate_creation_lifecycle().is_err());

        let mut mismatched = boot_activation_evidence(&identity, nonce)?;
        mismatched.target_incarnation = NamespaceIncarnationId::new();
        metadata.branch_activation = Some(mismatched);
        assert!(metadata.validate_creation_lifecycle().is_err());

        let mut unaudited = boot_activation_evidence(&identity, nonce)?;
        unaudited.audit_evidence_ref = " \t".to_string();
        metadata.branch_activation = Some(unaudited);
        assert!(metadata.validate_creation_lifecycle().is_err());

        metadata.branch_activation = Some(boot_activation_evidence(&identity, nonce)?);
        metadata.validate_creation_lifecycle()?;
        let encoded = metadata.to_bytes()?;
        let decoded = NamespaceMetadata::from_bytes(&encoded)?;
        assert_eq!(decoded.branch_activation, metadata.branch_activation);

        let mut with_unknown: serde_json::Value = serde_json::from_slice(&encoded)?;
        with_unknown["branch_activation"]["bearer_credential"] =
            serde_json::Value::String("must-not-persist".to_string());
        assert!(NamespaceMetadata::from_bytes(&serde_json::to_vec(&with_unknown)?).is_err());
        Ok(())
    }

    #[test]
    fn metadata_without_branch_activation_field_remains_compatible_for_legacy_roots() -> Result<()>
    {
        let now = Utc::now();
        let encoded = serde_json::to_vec(&serde_json::json!({
            "name": "legacy-root",
            "dimensions": 4,
            "distance_metric": "euclidean",
            "index_type": "ivf_flat",
            "vector_count": 0,
            "created_at": now,
            "updated_at": now
        }))?;
        let metadata = NamespaceMetadata::from_bytes(&encoded)?;
        assert_eq!(metadata.state, NamespaceState::Active);
        assert!(matches!(
            metadata.creation_kind,
            NamespaceCreationKind::Root
        ));
        assert!(metadata.branch_activation.is_none());
        Ok(())
    }

    #[test]
    fn malformed_incarnation_metadata_fails_loudly() {
        let now = Utc::now();
        let metadata = NamespaceMetadata {
            name: "invalid-incarnation".to_string(),
            dimensions: 16,
            distance_metric: DistanceMetric::Euclidean,
            index_type: IndexType::IvfFlat,
            vector_count: 0,
            created_at: now,
            updated_at: now,
            state: NamespaceState::Active,
            destruction_record_key: None,
            deletion_intent: None,
            full_text_search: std::collections::HashMap::new(),
            index_config: None,
            compaction_health: CompactionHealth::default(),
            creation_kind: NamespaceCreationKind::Root,
            branch_identity: None,
            branch_activation: None,
            branch_prepare: None,
            incarnation_id: None,
        };
        let mut user_metadata = ObjectUserMetadata::new();
        user_metadata.insert(NAMESPACE_INCARNATION_METADATA_KEY, "not-a-uuid");

        let error = match metadata.attach_user_metadata(&user_metadata) {
            Ok(_) => panic!("malformed incarnation metadata unexpectedly decoded"),
            Err(error) => error,
        };

        assert!(matches!(error, ZeppelinError::Serialization(_)));
    }

    #[test]
    fn deletion_intent_round_trips_with_exact_identity() {
        let nonce = ActivationNonce::new();
        let intent = NamespaceDeletionIntent {
            incarnation: NamespaceIncarnationId::new(),
            destruction_record_key: "_audit/destruction/example.json".to_string(),
            decision_evidence_ref: "decision-123".to_string(),
            branch_activation_nonce: Some(nonce),
            parent_root: None,
            fenced_generation: None,
            visibility: None,
            root_release: None,
        };
        let encoded = serde_json::to_vec(&intent).expect("intent serializes");
        let decoded: NamespaceDeletionIntent =
            serde_json::from_slice(&encoded).expect("intent decodes");
        assert_eq!(decoded, intent);
        assert_eq!(decoded.branch_activation_nonce, Some(nonce));
    }

    #[test]
    fn legacy_deletion_intent_defaults_branch_activation_nonce_to_none() {
        let incarnation = NamespaceIncarnationId::new();
        let encoded = serde_json::to_vec(&serde_json::json!({
            "incarnation": incarnation,
            "destruction_record_key": "_audit/destruction/legacy.json",
            "decision_evidence_ref": "_audit/deletion-decisions/legacy.json"
        }))
        .expect("legacy intent serializes");
        let decoded: NamespaceDeletionIntent =
            serde_json::from_slice(&encoded).expect("legacy intent decodes");
        assert!(decoded.branch_activation_nonce.is_none());
    }

    #[test]
    fn activation_cancellation_marker_requires_revoked_prepared_fork() -> Result<()> {
        let (mut metadata, target, identity) = prepared_fork_metadata()?;
        let nonce = ActivationNonce::new();
        assert!(begin_branch_activation_metadata(
            &mut metadata,
            &target,
            &identity,
            nonce,
            Utc::now(),
        )?);
        assert_eq!(
            revoke_branch_activation_metadata(
                &mut metadata,
                &target,
                &identity,
                nonce,
                Utc::now(),
            )?,
            BranchActivationRevocationPlan::PublishPrepared
        );
        metadata.deletion_intent = Some(activation_cancellation_intent(&identity, nonce));
        metadata.validate_creation_lifecycle()?;

        let encoded = metadata.to_bytes()?;
        let decoded = NamespaceMetadata::from_bytes(&encoded)?;
        assert_eq!(decoded.deletion_intent, metadata.deletion_intent);
        assert_eq!(
            decoded
                .deletion_intent
                .as_ref()
                .and_then(|intent| intent.branch_activation_nonce),
            Some(nonce)
        );

        let intent = metadata
            .deletion_intent
            .as_mut()
            .expect("cancellation intent remains present");
        intent.decision_evidence_ref = " \t".to_string();
        assert!(metadata.validate_creation_lifecycle().is_err());
        Ok(())
    }

    #[test]
    fn activation_cancellation_marker_is_rejected_before_nonce_revocation() -> Result<()> {
        let (mut metadata, target, identity) = prepared_fork_metadata()?;
        let nonce = ActivationNonce::new();
        assert!(begin_branch_activation_metadata(
            &mut metadata,
            &target,
            &identity,
            nonce,
            Utc::now(),
        )?);
        metadata.deletion_intent = Some(activation_cancellation_intent(&identity, nonce));
        assert!(metadata.validate_creation_lifecycle().is_err());
        Ok(())
    }

    #[test]
    fn activation_cancellation_marker_is_never_valid_for_visible_deletion() -> Result<()> {
        let (mut active, target, identity) = prepared_fork_metadata()?;
        let nonce = ActivationNonce::new();
        assert!(begin_branch_activation_metadata(
            &mut active,
            &target,
            &identity,
            nonce,
            Utc::now(),
        )?);
        let evidence = boot_activation_evidence(&identity, nonce)?;
        assert!(commit_branch_activation_metadata(
            &mut active,
            &target,
            &identity,
            &evidence,
            Utc::now(),
        )?);
        active.deletion_intent = Some(activation_cancellation_intent(&identity, nonce));
        assert!(active.validate_creation_lifecycle().is_err());

        active.state = NamespaceState::Deleting;
        assert!(active.validate_creation_lifecycle().is_err());
        Ok(())
    }

    #[test]
    fn activation_cancellation_marker_is_never_valid_for_root_deletion() -> Result<()> {
        let (mut root, _, identity) = prepared_fork_metadata()?;
        let nonce = ActivationNonce::new();
        root.creation_kind = NamespaceCreationKind::Root;
        root.branch_identity = None;
        root.branch_prepare = None;
        root.state = NamespaceState::Active;
        root.deletion_intent = Some(activation_cancellation_intent(&identity, nonce));
        assert!(root.validate_creation_lifecycle().is_err());
        Ok(())
    }

    #[test]
    fn legacy_direct_evidence_binding_requires_the_incarnation_derived_key() {
        let incarnation = NamespaceIncarnationId::new();
        let canonical_key = format!("_audit/destruction/{}.json", incarnation.as_uuid().simple());
        let mut intent = NamespaceDeletionIntent {
            incarnation,
            destruction_record_key: canonical_key.clone(),
            decision_evidence_ref: canonical_key,
            branch_activation_nonce: None,
            parent_root: None,
            fenced_generation: None,
            visibility: None,
            root_release: None,
        };
        assert!(intent.is_legacy_direct_evidence_binding());

        intent.decision_evidence_ref = "_audit/deletion-decisions/current.json".to_string();
        assert!(!intent.is_legacy_direct_evidence_binding());
        intent.decision_evidence_ref = intent.destruction_record_key.clone();
        intent.destruction_record_key = "_audit/destruction/replayed.json".to_string();
        intent.decision_evidence_ref = intent.destruction_record_key.clone();
        assert!(!intent.is_legacy_direct_evidence_binding());
    }

    #[test]
    fn evidence_extensions_are_relaxed_only_for_exact_legacy_binding() -> Result<()> {
        let incarnation = NamespaceIncarnationId::new();
        let canonical_key = format!("_audit/destruction/{}.json", incarnation.as_uuid().simple());
        let mut intent = NamespaceDeletionIntent {
            incarnation: incarnation.clone(),
            destruction_record_key: canonical_key.clone(),
            decision_evidence_ref: canonical_key,
            branch_activation_nonce: None,
            parent_root: None,
            fenced_generation: None,
            visibility: None,
            root_release: None,
        };
        let mut evidence = NamespaceDestructionRecord {
            namespace: NamespaceId::new("legacy-evidence")?,
            manifest_version_destroyed: 7,
            object_count: 0,
            byte_count: 0,
            actor: PrincipalId::new("legacy-actor")?,
            approver: None,
            decision_id: DecisionId::new(),
            parent_root: None,
            incarnation: None,
            preservation_head: None,
            ts: Utc::now(),
        };
        assert!(evidence.protocol_fields_match(&intent));

        evidence.incarnation = Some(NamespaceIncarnationId::new());
        assert!(!evidence.protocol_fields_match(&intent));
        evidence.incarnation = None;
        intent.decision_evidence_ref = "_audit/deletion-decisions/current.json".to_string();
        assert!(!evidence.protocol_fields_match(&intent));

        evidence.incarnation = Some(incarnation);
        assert!(
            !evidence.protocol_fields_match(&intent),
            "current evidence must not omit its preservation head"
        );
        Ok(())
    }

    #[test]
    fn root_release_identity_ignores_only_the_acknowledgement() {
        let now = Utc::now();
        let intent = NamespaceDeletionIntent {
            incarnation: NamespaceIncarnationId::new(),
            destruction_record_key: "_audit/destruction/example.json".to_string(),
            decision_evidence_ref: "decision-123".to_string(),
            branch_activation_nonce: None,
            parent_root: None,
            fenced_generation: Some(7),
            visibility: Some(VisibilityRemoval {
                marker_key: "target/_lifecycle/branch_visibility_removed.json".to_string(),
                observed_at: now,
                not_before: now + chrono::Duration::seconds(31),
            }),
            root_release: None,
        };
        let mut acknowledged = intent.clone();
        acknowledged.root_release = Some(RootReleaseState::Released { acked_at: now });
        assert!(intent.has_same_root_release_identity(&acknowledged));

        acknowledged.decision_evidence_ref = "decision-456".to_string();
        assert!(!intent.has_same_root_release_identity(&acknowledged));
    }

    /// Verifies the accepted grammar covers the intended portable name forms.
    #[test]
    fn namespace_name_validator_accepts_s3_and_url_safe_names() {
        for name in ["tenant-a", "tenant_a", "tenant.a", "TenantA-123"] {
            assert!(
                is_valid_namespace_name(name),
                "expected namespace name to be valid: {name}"
            );
        }
    }

    /// Verifies unsafe paths, encodings, leading punctuation, and length fail.
    #[test]
    fn namespace_name_validator_rejects_unsafe_names_at_creation_boundary() {
        let overlong = format!("a{}", "b".repeat(255));
        let invalid = [
            "",
            "tenant/a",
            "tenant a",
            "tenant%2Fa",
            "../tenant",
            "-tenant",
            "tenant?",
            "tenant#fragment",
            overlong.as_str(),
        ];

        for name in invalid {
            assert!(
                !is_valid_namespace_name(name),
                "expected namespace name to be invalid: {name:?}"
            );
        }
    }
}
