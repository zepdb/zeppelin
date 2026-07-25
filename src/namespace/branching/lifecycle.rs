//! Persisted namespace-fork lifecycle identities and prepare outcomes.
//!
//! Everything a fork durably records is declared here as a strong type: the
//! create-only reservation written into the target's `meta.json`, the ancestry
//! proof signed into the target's manifest, and the evidence installed by the
//! single visibility compare-and-swap that turns a hidden reservation into a
//! usable namespace. This file owns the *vocabulary* and the cross-record
//! equality rules that bind those three independent objects to one fork. It
//! performs no I/O, holds no lease, takes no CAS, and makes no authorization
//! decision.
//!
//! ## Where this sits
//!
//! The crate-private namespace graph (`NamespaceGraph`, `src/namespace/graph.rs`)
//! is the only coordinator that advances these values. It reads and writes them
//! through three persistence owners, none of which live here:
//!
//! - [`NamespaceMetadata`](crate::namespace::manager::NamespaceMetadata) persists the reservation,
//!   the prepare intent, the final identity, and the activation evidence in
//!   `meta.json` (JSON).
//! - [`Manifest`](crate::wal::Manifest) persists [`BranchLineage`] in the
//!   target's manifest and [`BranchRoot`] in the *parent's*
//!   manifest (MessagePack).
//! - `src/namespace/branch_root.rs` performs the lease-fenced root mutation.
//!
//! Above the graph, `crate::server::handlers::namespace::create_branch` and
//! `list_branches` project a redacted subset onto the wire types in
//! [`http`](crate::namespace::branching::http). Below it, `src/storage/` is reached
//! only by those persistence owners — never from this module.
//!
//! Branching is fork-only. There is no merge, rebase, diff, or promote state in
//! this vocabulary because those operations do not exist in the product; that is
//! a locked decision, not a missing variant.
//!
//! ## Reading map
//!
//! 1. [`NamespaceCreationKind`] — which lifecycle family a `meta.json` record
//!    belongs to. `Root` is the pre-branching default; `Fork` carries the
//!    reservation.
//! 2. [`ForkReservationIdentity`] — the create-only edge minted when the target
//!    name is claimed.
//! 3. [`BranchPrepareStage`] and [`ForkPrepareIntent`] — the monotonic,
//!    never-visible preparation milestone.
//! 4. [`ForkIdentity`] — the final immutable creation proof, plus the three
//!    predicates that reconcile it against the other persisted records:
//!    [`ForkIdentity::matches_reservation`], [`ForkIdentity::matches_root`],
//!    and [`ForkIdentity::matches_lineage`].
//! 5. [`BranchLineage`] — the manifest-side half of the same proof.
//! 6. [`ActivationNonce`], [`PolicyHeadIdentity`], and
//!    [`BranchActivationEvidence`] — the fencing and proof material for the sole
//!    visibility CAS.
//! 7. [`PrepareForkRequest`], [`PrepareForkOutcome`], [`PreparedBranch`],
//!    [`BranchDescriptor`], and [`BranchLifecycleState`] — in-process request
//!    and result shapes that are never persisted.
//! 8. [`NamespaceDeleteRequest`], [`NamespaceDeleteOutcome`], and
//!    [`BranchMaintenanceReport`] — the deletion and background-maintenance
//!    vocabulary.
//!
//! ## Fork lifecycle state machine
//!
//! Each arrow is a durable boundary. A crash between any two of them leaves a
//! record that background maintenance can resume; nothing is visible to readers
//! until the final CAS.
//!
//! ```text
//! reserve target name (meta.json, state = creating)
//!   creation_kind  = Fork(ForkReservationIdentity)
//!   branch_prepare = ForkPrepareIntent {
//!       stage       = Reserved,
//!       provisional = Some(ForkDataPlaneConfig) }   <- copied from parent
//!   branch_identity = None
//!         |
//!         | publish BranchRoot into the PARENT manifest
//!         | (lease-fenced + ETag CAS); parent now pins the forked generation
//!         v
//!   stage           = Rooted
//!   branch_identity = Some(ForkIdentity)   <- digests are now frozen
//!   provisional     = None
//!         |
//!         | create-only PUT of the target's generation-1 manifest,
//!         | signed with BranchLineage; still zero artifact bytes copied
//!         v
//!   stage = ManifestPublished
//!         |
//!         | mint ActivationNonce; install the policy-head guard
//!         v
//!   stage = ActivationPending { nonce }
//!         |                         \
//!         |                          \ crash / guard expiry
//!         |                           v
//!         |                    guard recovery proves the target outcome:
//!         |                    finalize (evidence exists) or abort (nonce
//!         |                    durably revoked). Never "assume success".
//!         |
//!         | sole target-visibility CAS on meta.json
//!         v
//! state = active
//!   branch_prepare    = None              <- preparation state is cleared
//!   branch_identity   = Some(ForkIdentity)
//!   branch_activation = Some(BranchActivationEvidence)
//! ```
//!
//! ## Invariants
//!
//! - **Visibility is the CAS, not the artifacts.** A rooted, manifest-published
//!   fork is fully durable and still invisible. Only the metadata CAS that sets
//!   `state = active` and installs [`BranchActivationEvidence`] exposes it.
//! - **Preparation is monotonic.** [`BranchPrepareStage`] only advances.
//!   Recovery re-derives the current stage from persisted state; it never
//!   rewinds a milestone or infers one from a timestamp.
//! - **Timestamps are audit, not order.** `created_at` on
//!   [`ForkReservationIdentity`], [`ForkIdentity`], and [`BranchLineage`] is
//!   reporting material. Ordering authority is
//!   [`ManifestGeneration`] plus ETag CAS.
//! - **Identity is a lifetime, not a name.** Every cross-record predicate
//!   compares `NamespaceIncarnationId` as well as `NamespaceId`, so a
//!   deleted-and-recreated namespace can never silently satisfy a stale proof.
//! - **A reservation is create-only.** `matches_reservation` requires every
//!   reservation field, including `created_at` and `depth`, to be byte-identical
//!   in the final identity. A retry that differs is a conflict, not an update.
//! - **The nonce fences workers, it does not authorize them.**
//!   [`ActivationNonce`] is an ordering identity; authority comes from the
//!   freshly re-checked policy head carried by [`PolicyHeadIdentity`].
//! - **BOOT is never dressed up as a fenced publication.**
//!   [`PolicyHeadIdentity::Persisted`] is rejected by
//!   [`PolicyHeadIdentity::is_valid_for_activation`] when it names the BOOT
//!   policy version or an unadvanced control revision.
//! - **Cleared state is part of the contract.** Once active, `branch_prepare`
//!   must be absent and both the identity and the evidence present; the
//!   validator in `NamespaceMetadata::validate_creation_lifecycle` rejects any
//!   other combination rather than repairing it.
//!
//! ## Serialization compatibility
//!
//! Two different persisted formats read these types, and the rules differ.
//!
//! - `meta.json` is JSON. `#[serde(default)]` lets pre-branching records decode
//!   as [`NamespaceCreationKind::Root`], and `skip_serializing_if` is safe on
//!   optional fields such as `BranchActivationEvidence::approver` and
//!   `ForkPrepareIntent::provisional`.
//! - The manifest is MessagePack. [`BranchLineage`] is the only type here that
//!   is persisted there, and it deliberately uses `#[serde(default)]` alone.
//!   **Do not add `skip_serializing_if` to it** — MessagePack is not
//!   self-describing enough for an omitted field in that position.
//! - `#[serde(deny_unknown_fields)]` on the persisted records makes a foreign or
//!   corrupted field a loud decode failure rather than a silent drop.
//! - [`BranchPrepareStage::ActivationPending`] is an intentional forward
//!   incompatibility: an older reader that predates the nonce fails closed on it
//!   instead of mistaking a fenced attempt for a plain `manifest_published`
//!   record. A unit test in this file pins that behavior.
//!
//! ## Rust concepts used here
//!
//! **Newtypes over raw values.** [`ActivationNonce`] wraps a `Ulid` and is
//! `#[serde(transparent)]`, so it costs nothing on the wire but cannot be
//! confused with a `BranchId` or a policy token at a call site. In Java this
//! would be a wrapper class you would be tempted to skip for performance; in
//! Rust the wrapper is erased at runtime. It is `Copy` because a ULID is 16
//! plain bytes — passing one moves nothing and allocates nothing.
//!
//! **Enums that make invalid states unrepresentable.** [`PolicyHeadIdentity`]
//! is the clearest case. A struct with nullable lease fields would let a BOOT
//! deployment be described with a fabricated fencing token; the two-variant enum
//! makes that shape impossible to construct. Likewise
//! [`BranchPrepareStage::ActivationPending`] carries its nonce *inside* the
//! variant, so "pending activation without a fence" cannot be spelled. A C
//! tagged union offers the same layout but no compiler-checked exhaustiveness;
//! Rust's `match` forces every reader to handle a newly added stage.
//!
//! **Predicates instead of `PartialEq` on whole records.** `matches_root` and
//! `matches_lineage` deliberately compare *subsets* of [`ForkIdentity`], because
//! the parent root and the child lineage each legitimately omit fields the other
//! carries. Deriving equality across records would have forced those differences
//! into the type layout.
//!
//! **In-process versus persisted types.** Only the `Serialize`/`Deserialize`
//! types in this file reach object storage. [`PrepareForkOutcome`],
//! [`BranchDescriptor`], [`NamespaceDeleteOutcome`], and
//! [`BranchMaintenanceReport`] have no serde derives at all — that absence is
//! the signal that changing them cannot break an existing `meta.json`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::namespace::manager::NamespaceIndexConfig;
use crate::namespace::{
    BranchId, BranchRoot, ForkViewDigest, ManifestDigest, ManifestGeneration, NamespaceId,
    NamespaceIncarnationId, SourceDataPlaneConfigDigest,
};
use crate::security::{
    DecisionId, PolicyControlRevision, PolicyHeadDigest, PolicyLeaseFencingToken, PolicyVersion,
    PrincipalId,
};
use crate::types::{DistanceMetric, IndexType};

/// Collision-resistant fence for one branch-activation attempt.
///
/// The nonce is persisted in target metadata before any policy-head guard is
/// installed. It is an ordering identity only and never bearer authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActivationNonce(Ulid);

impl ActivationNonce {
    /// Mint a fresh activation-attempt identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// Wrap an already validated ULID.
    #[must_use]
    pub const fn from_ulid(value: Ulid) -> Self {
        Self(value)
    }

    /// Return the stable ULID value.
    #[must_use]
    pub const fn get(self) -> Ulid {
        self.0
    }
}

impl Default for ActivationNonce {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ActivationNonce {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, formatter)
    }
}

/// Exact policy publication boundary linearized with branch activation.
///
/// BOOT mode is represented explicitly because it has no persisted policy
/// head and no fencing lease. It must never be made to look equivalent to a
/// fenced persisted policy publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolicyHeadIdentity {
    /// Immutable boot-policy proof for deployments without an S3 policy head.
    Boot {
        /// Exact target activation attempt bound by the metadata CAS.
        activation_nonce: ActivationNonce,
    },
    /// Persisted policy-head proof guarded by the publication lease.
    Persisted {
        /// Semantic policy version reauthorized at activation time.
        policy_version: PolicyVersion,
        /// Canonical digest of the guarded authoritative policy head.
        policy_head_digest: PolicyHeadDigest,
        /// Non-semantic policy-head control revision containing the guard.
        control_revision: PolicyControlRevision,
        /// Fencing token of the policy-publication lease holder.
        lease_fencing_token: PolicyLeaseFencingToken,
        /// Exact target activation attempt named by the policy-head guard.
        activation_nonce: ActivationNonce,
    },
}

impl PolicyHeadIdentity {
    /// Return the exact target activation attempt carried by either authority.
    #[must_use]
    pub const fn activation_nonce(&self) -> ActivationNonce {
        match self {
            Self::Boot { activation_nonce }
            | Self::Persisted {
                activation_nonce, ..
            } => *activation_nonce,
        }
    }

    /// Return whether this identity can represent a completed activation proof.
    ///
    /// A persisted proof must name a semantic (non-BOOT) policy and the
    /// post-guard control revision. BOOT is valid only through its explicit
    /// variant, which deliberately carries no fictional lease fields.
    #[must_use]
    pub const fn is_valid_for_activation(&self) -> bool {
        match self {
            Self::Boot { .. } => true,
            Self::Persisted {
                policy_version,
                control_revision,
                ..
            } => policy_version.get() != PolicyVersion::BOOT.get() && control_revision.get() > 0,
        }
    }
}

/// Immutable evidence installed by the sole target-visibility CAS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchActivationEvidence {
    /// Stable direct-edge identity that became visible.
    pub branch_id: BranchId,
    /// Exact target namespace made visible by this activation.
    pub target_namespace: NamespaceId,
    /// Exact target lifetime made visible by this activation.
    pub target_incarnation: NamespaceIncarnationId,
    /// Exact policy publication boundary proved immediately before activation.
    pub policy_head: PolicyHeadIdentity,
    /// Fresh authorization decision admitted by the activation attempt.
    pub decision_id: DecisionId,
    /// Independent approver when the policy required two-person approval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approver: Option<PrincipalId>,
    /// Opaque durable audit linkage settled before the visibility CAS.
    pub audit_evidence_ref: String,
    /// Trusted activation timestamp used only for audit reporting.
    pub activated_at: DateTime<Utc>,
}

impl BranchActivationEvidence {
    /// Return whether this evidence is bound to one exact immutable fork.
    #[must_use]
    pub fn matches_identity(&self, identity: &ForkIdentity) -> bool {
        self.branch_id == identity.branch_id
            && self.target_namespace == identity.target_namespace
            && self.target_incarnation == identity.target_incarnation
            && !self.audit_evidence_ref.trim().is_empty()
            && self.policy_head.is_valid_for_activation()
    }

    /// Return the exact activation-attempt fence committed by this evidence.
    #[must_use]
    pub fn activation_nonce(&self) -> ActivationNonce {
        self.policy_head.activation_nonce()
    }
}

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

    /// Returns whether every parent-root field matches this immutable branch
    /// identity. The target generation and target-manifest digest are not root
    /// fields and are therefore deliberately excluded.
    #[must_use]
    pub fn matches_root(&self, root: &BranchRoot) -> bool {
        self.branch_id == root.branch_id
            && self.source_generation == root.source_generation
            && self.source_manifest_sha256 == root.source_manifest_sha256
            && self.fork_view_sha256 == root.fork_view_sha256
            && self.source_config_sha256 == root.source_config_sha256
            && self.target_namespace == root.target_namespace
            && self.target_incarnation == root.target_incarnation
            && self.created_at == root.created_at
    }

    /// Returns whether every immutable ancestry field matches the lineage
    /// carried by a live manifest in this branch lifetime. Target binding is
    /// established by the incarnation-bound manifest read; generation-one
    /// bytes and their digest are deliberately not lineage fields.
    #[must_use]
    pub fn matches_lineage(&self, lineage: &BranchLineage) -> bool {
        self.branch_id == lineage.branch_id
            && self.source_namespace == lineage.parent_namespace
            && self.source_incarnation == lineage.parent_incarnation
            && self.source_generation == lineage.fork_generation
            && self.source_manifest_sha256 == lineage.fork_manifest_sha256
            && self.fork_view_sha256 == lineage.fork_view_sha256
            && self.source_config_sha256 == lineage.source_config_sha256
            && self.depth == lineage.depth
            && self.created_at == lineage.created_at
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
    /// Zero-copy fork reservation owned only by `NamespaceGraph`.
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
    /// One exact authenticated activation attempt owns the visibility race.
    ActivationPending {
        /// Stale-worker fence minted once for this activation attempt.
        nonce: ActivationNonce,
    },
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
}

/// Result of graph-owned deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceDeleteOutcome {
    /// Cleanup completed through the namespace manager.
    Deleted,
    /// The namespace was already in the deleting lifecycle state.
    AlreadyDeleting,
    /// Branch visibility is gone, but its parent root remains pinned through
    /// the persisted reader-safety deadline.
    BranchGraceWait {
        /// Earliest authoritative wall-clock instant at which root release may
        /// begin. Derived from the marker object's S3 `last_modified` value.
        not_before: DateTime<Utc>,
    },
}

/// Closed lifecycle vocabulary disclosed for a direct child branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchLifecycleState {
    /// The target reservation has not completed activation.
    Preparing,
    /// The target is active and ready for branch reads and writes.
    Active,
    /// The target is fenced and its deletion protocol is in progress.
    Deleting,
}

/// Redacted direct-child descriptor returned by the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchDescriptor {
    /// Direct child target namespace.
    pub target: NamespaceId,
    /// Stable branch edge identity.
    pub branch_id: BranchId,
    /// Exact child lifetime already disclosed by the parent root.
    pub target_incarnation: NamespaceIncarnationId,
    /// One-based ancestry depth copied from the verified target reservation.
    pub depth: u16,
    /// Stable branch reservation timestamp.
    pub created_at: DateTime<Utc>,
    /// Redacted lifecycle state.
    pub state: BranchLifecycleState,
    /// Whether the current active view contains only target-owned artifacts.
    pub materialized: bool,
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

/// Bounded maintenance observations for deletion and branch control state.
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
    /// Active branches whose parent root and current immutable lineage verified.
    pub active_verified: usize,
    /// Reserved targets deliberately left for a fresh authenticated retry.
    pub awaiting_authenticated_retry: usize,
    /// Reserved targets whose parent is absent or deleting.
    pub awaiting_authorized_cancellation: usize,
    /// Governed deletion intents or tombstones inspected within the budget.
    pub deletions_inspected: usize,
    /// Governed deletions that reached metadata-last completion.
    pub deletions_completed: usize,
    /// Governed deletions that made bounded progress but remain durable.
    pub deletions_in_progress: usize,
    /// Branch deletions still retaining their root through reader-safety grace.
    pub branch_grace_waiting: usize,
    /// Expired policy-head activation guards inspected under publication fencing.
    pub activation_guards_inspected: usize,
    /// Guards finalized after exact matching Active evidence was proved.
    pub activation_guards_finalized: usize,
    /// Guards aborted after the target nonce was proved durably non-visible.
    pub activation_guards_aborted: usize,
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::{ActivationNonce, BranchPrepareStage, PolicyHeadIdentity};
    use crate::security::{
        PolicyControlRevision, PolicyHeadDigest, PolicyLeaseFencingToken, PolicyVersion,
    };

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    #[serde(rename_all = "snake_case")]
    enum LegacyBranchPrepareStage {
        Reserved,
        Rooted,
        ManifestPublished,
    }

    #[test]
    fn activation_pending_is_nonce_bound_without_changing_legacy_stage_encoding() {
        for (encoded, expected) in [
            ("\"reserved\"", BranchPrepareStage::Reserved),
            ("\"rooted\"", BranchPrepareStage::Rooted),
            (
                "\"manifest_published\"",
                BranchPrepareStage::ManifestPublished,
            ),
        ] {
            assert_eq!(
                serde_json::from_str::<BranchPrepareStage>(encoded).expect("legacy stage decodes"),
                expected
            );
            assert_eq!(
                serde_json::to_string(&expected).expect("legacy stage encodes"),
                encoded
            );
        }

        let nonce = ActivationNonce::new();
        let encoded = serde_json::to_value(BranchPrepareStage::ActivationPending { nonce })
            .expect("activation stage encodes");
        assert_eq!(
            encoded,
            serde_json::json!({"activation_pending": {"nonce": nonce}})
        );
        assert_eq!(
            serde_json::from_value::<BranchPrepareStage>(encoded)
                .expect("activation stage decodes"),
            BranchPrepareStage::ActivationPending { nonce }
        );
        assert!(
            serde_json::from_value::<LegacyBranchPrepareStage>(
                serde_json::json!({"activation_pending": {"nonce": nonce}})
            )
            .is_err(),
            "an old reader must fail closed on the nonce-bearing state"
        );
    }

    #[test]
    fn policy_head_identity_distinguishes_boot_from_fenced_publication() {
        let boot_nonce = ActivationNonce::new();
        let boot = PolicyHeadIdentity::Boot {
            activation_nonce: boot_nonce,
        };
        let boot_json = serde_json::to_value(&boot).expect("boot identity encodes");
        assert_eq!(
            boot_json,
            serde_json::json!({
                "kind": "boot",
                "activation_nonce": boot_nonce,
            })
        );
        assert_eq!(boot.activation_nonce(), boot_nonce);
        assert!(boot.is_valid_for_activation());

        let persisted_nonce = ActivationNonce::new();
        let persisted = PolicyHeadIdentity::Persisted {
            policy_version: PolicyVersion::persisted(7).expect("nonzero policy version"),
            policy_head_digest: PolicyHeadDigest::new([0xab; 32]),
            control_revision: PolicyControlRevision::new(3),
            lease_fencing_token: PolicyLeaseFencingToken::new(4)
                .expect("nonzero lease fencing token"),
            activation_nonce: persisted_nonce,
        };
        let persisted_json = serde_json::to_value(&persisted).expect("persisted identity encodes");
        assert_eq!(
            persisted_json,
            serde_json::json!({
                "kind": "persisted",
                "policy_version": 7,
                "policy_head_digest": "abababababababababababababababababababababababababababababababab",
                "control_revision": 3,
                "lease_fencing_token": 4,
                "activation_nonce": persisted_nonce,
            })
        );
        assert_eq!(persisted.activation_nonce(), persisted_nonce);
        assert!(persisted.is_valid_for_activation());
        assert_eq!(
            serde_json::from_value::<PolicyHeadIdentity>(persisted_json)
                .expect("persisted identity decodes"),
            persisted
        );

        assert!(!PolicyHeadIdentity::Persisted {
            policy_version: PolicyVersion::BOOT,
            policy_head_digest: PolicyHeadDigest::new([0xcd; 32]),
            control_revision: PolicyControlRevision::new(1),
            lease_fencing_token: PolicyLeaseFencingToken::INITIAL,
            activation_nonce: persisted_nonce,
        }
        .is_valid_for_activation());
        assert!(!PolicyHeadIdentity::Persisted {
            policy_version: PolicyVersion::persisted(8).expect("nonzero policy version"),
            policy_head_digest: PolicyHeadDigest::new([0xef; 32]),
            control_revision: PolicyControlRevision::INITIAL,
            lease_fencing_token: PolicyLeaseFencingToken::INITIAL,
            activation_nonce: persisted_nonce,
        }
        .is_valid_for_activation());

        let with_unknown = serde_json::json!({
            "kind": "boot",
            "activation_nonce": boot_nonce,
            "lease_fencing_token": 1,
        });
        assert!(serde_json::from_value::<PolicyHeadIdentity>(with_unknown).is_err());
    }
}
