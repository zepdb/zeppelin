//! Persisted namespace-fork lifecycle identities and prepare outcomes.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::namespace::manager::NamespaceIndexConfig;
use crate::namespace::{
    BranchId, BranchRoot, ForkViewDigest, ManifestDigest, ManifestGeneration, NamespaceId,
    NamespaceIncarnationId, SourceDataPlaneConfigDigest,
};
use crate::types::{DistanceMetric, IndexType};

/// Stable create-only reservation for one direct parent-to-child edge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForkReservationIdentity {
    /// Stable branch-edge identifier minted once at target reservation.
    pub branch_id: BranchId,
    /// Direct parent namespace selected by the request.
    pub source_namespace: NamespaceId,
    /// Exact parent namespace lifetime observed before reservation.
    pub source_incarnation: NamespaceIncarnationId,
    /// Reserved target namespace name.
    pub target_namespace: NamespaceId,
    /// Exact target lifetime stored in object user metadata.
    pub target_incarnation: NamespaceIncarnationId,
    /// Audit timestamp only; never an ordering clock.
    pub created_at: DateTime<Utc>,
    /// One-based ancestry depth of the target.
    pub depth: u16,
}

/// Final immutable creation proof retained for the target lifetime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForkIdentity {
    /// Stable branch-edge identifier.
    pub branch_id: BranchId,
    /// Direct parent namespace.
    pub source_namespace: NamespaceId,
    /// Exact parent lifetime.
    pub source_incarnation: NamespaceIncarnationId,
    /// Target namespace.
    pub target_namespace: NamespaceId,
    /// Exact target lifetime.
    pub target_incarnation: NamespaceIncarnationId,
    /// Reservation timestamp.
    pub created_at: DateTime<Utc>,
    /// Target ancestry depth.
    pub depth: u16,
    /// Exact parent generation selected by the winning root CAS.
    pub source_generation: ManifestGeneration,
    /// SHA-256 over the exact stored parent-generation bytes.
    pub source_manifest_sha256: ManifestDigest,
    /// Canonical digest of the initial inherited target view.
    pub fork_view_sha256: ForkViewDigest,
    /// Canonical source data-plane configuration digest.
    pub source_config_sha256: SourceDataPlaneConfigDigest,
    /// Create-only target manifest generation; always one in v1.
    pub target_generation: ManifestGeneration,
    /// SHA-256 over the exact prepared target generation-one bytes.
    pub target_manifest_sha256: ManifestDigest,
}

impl ForkIdentity {
    /// Returns whether every create-only field matches one reservation.
    #[must_use]
    pub fn matches_reservation(&self, reservation: &ForkReservationIdentity) -> bool {
        self.branch_id == reservation.branch_id
            && self.source_namespace == reservation.source_namespace
            && self.source_incarnation == reservation.source_incarnation
            && self.target_namespace == reservation.target_namespace
            && self.target_incarnation == reservation.target_incarnation
            && self.created_at == reservation.created_at
            && self.depth == reservation.depth
    }
}

/// Immutable ancestry proof signed by every manifest in a branch lifetime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BranchLineage {
    /// Stable direct-edge identifier.
    pub branch_id: BranchId,
    /// Direct parent namespace.
    pub parent_namespace: NamespaceId,
    /// Exact direct-parent lifetime.
    pub parent_incarnation: NamespaceIncarnationId,
    /// Parent generation retained by the root.
    pub fork_generation: ManifestGeneration,
    /// Exact digest of the retained parent bytes.
    pub fork_manifest_sha256: ManifestDigest,
    /// Canonical initial inherited-view digest.
    pub fork_view_sha256: ForkViewDigest,
    /// Canonical source data-plane configuration digest.
    pub source_config_sha256: SourceDataPlaneConfigDigest,
    /// One-based target ancestry depth.
    pub depth: u16,
    /// Reservation timestamp.
    pub created_at: DateTime<Utc>,
}

/// Canonical provisional target data-plane snapshot while no parent root exists.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForkDataPlaneConfig {
    /// Vector dimensionality inherited from the parent.
    pub dimensions: usize,
    /// Distance metric inherited from the parent.
    pub distance_metric: DistanceMetric,
    /// Index algorithm inherited from the parent.
    pub index_type: IndexType,
    /// Canonically ordered analyzer JSON, independent of source HashMap order.
    pub full_text_search: BTreeMap<String, serde_json::Value>,
    /// Fully resolved per-namespace indexing configuration.
    pub index_config: NamespaceIndexConfig,
}

/// Creation family controlling recovery of a `creating` metadata record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NamespaceCreationKind {
    /// Ordinary empty-root namespace creation.
    #[default]
    Root,
    /// Zero-copy fork reservation owned only by [`crate::namespace::graph::NamespaceGraph`].
    Fork(ForkReservationIdentity),
}

/// Monotonic non-visible preparation milestone.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BranchPrepareStage {
    /// Target metadata exists but no final identity has been installed.
    Reserved,
    /// Direct-parent root and immutable branch identity are durable.
    Rooted,
    /// Exact target generation-one manifest and history are durable.
    ManifestPublished,
}

/// Transient preparation state valid only while target metadata is `creating`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForkPrepareIntent {
    /// Stable edge identifier repeated for corruption-local validation.
    pub branch_id: BranchId,
    /// Stable target lifetime repeated for corruption-local validation.
    pub target_incarnation: NamespaceIncarnationId,
    /// Current monotonic prepare milestone.
    pub stage: BranchPrepareStage,
    /// Mutable provisional configuration only while `stage = reserved`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provisional: Option<ForkDataPlaneConfig>,
}

/// Typed request accepted by the prepare-only namespace graph seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareForkRequest {
    /// Authoritative active parent namespace.
    pub source: NamespaceId,
    /// Name to reserve for the non-visible target.
    pub target: NamespaceId,
}

/// Graph-owned deletion request. Authorization proof plumbing is added by the
/// HTTP security phase; the graph remains the only lifecycle entry point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceDeleteRequest {
    /// Namespace selected for graph-owned deletion.
    pub namespace: NamespaceId,
    /// Opaque authorization/decision evidence reference supplied by the
    /// security adapter; never a bearer credential.
    pub decision_evidence_ref: Option<String>,
    /// Exact parent root binding when deleting a branch target.
    pub parent_root: Option<BranchRoot>,
}

/// Result of graph-owned deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceDeleteOutcome {
    /// Cleanup completed through the namespace manager.
    Deleted,
    /// The namespace was already in the deleting lifecycle state.
    AlreadyDeleting,
}

/// Direct-child listing request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchListRequest {
    /// Source namespace whose direct root map is listed.
    pub source: NamespaceId,
}

/// Redacted direct-child descriptor returned by the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchDescriptor {
    /// Direct child target namespace.
    pub target: NamespaceId,
    /// Stable branch edge identity.
    pub branch_id: BranchId,
    /// Redacted lifecycle state.
    pub state: &'static str,
}

/// Complete proof returned after prepare reaches `manifest_published`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedBranch {
    /// Immutable metadata identity.
    pub identity: ForkIdentity,
    /// Manifest ancestry proof.
    pub lineage: BranchLineage,
    /// Exact direct-parent retention root.
    pub root: BranchRoot,
}

/// Idempotent prepare result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareForkOutcome {
    /// This call advanced at least one durable preparation milestone.
    Prepared(PreparedBranch),
    /// Target was already prepared and passed full consistency verification.
    ExistingPrepared(PreparedBranch),
}

/// Bounded maintenance observations for non-visible branch control state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BranchMaintenanceReport {
    /// Creating fork reservations inspected within the budget.
    pub inspected: usize,
    /// Existing roots reconciled into rooted metadata.
    pub rooted_repaired: usize,
    /// Exact target generation-one manifests published or confirmed.
    pub manifests_published: usize,
    /// Fully prepared reservations verified.
    pub prepared_verified: usize,
    /// Active branches whose parent root and generation-one lineage verified.
    pub active_verified: usize,
    /// Reserved targets deliberately left for a fresh authenticated retry.
    pub awaiting_authenticated_retry: usize,
    /// Reserved targets whose parent is absent or deleting.
    pub awaiting_authorized_cancellation: usize,
}
