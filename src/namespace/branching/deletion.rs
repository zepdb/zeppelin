//! Security-to-graph deletion contract.
//!
//! This module contains the typed envelope and callbacks consumed by the
//! authoritative namespace graph. It deliberately carries decisions and
//! governance hooks, never bearer credentials or caller-supplied roots.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::Result;
use crate::namespace::NamespaceId;
use crate::namespace::{BranchId, NamespaceIncarnationId};
use crate::security::{
    DecisionId, PolicyVersion, PreservationGuard, PreservationHeadProof, PrincipalId,
};
use serde::{Deserialize, Serialize};

/// One authorization decision passed from the security adapter to the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeletionDecision {
    /// Principal that requested deletion.
    pub actor: PrincipalId,
    /// Optional approving principal.
    pub approver: Option<PrincipalId>,
    /// Stable authorization decision identity.
    pub decision_id: DecisionId,
    /// Policy version used for the decision.
    pub policy_version: PolicyVersion,
    /// Opaque durable audit linkage.
    pub decision_evidence_ref: String,
}

/// Governance hooks required at every destructive boundary.
#[async_trait]
pub(crate) trait DeletionGovernance: Send + Sync {
    /// Read fresh preservation authority for the next mutation boundary.
    async fn preservation_boundary(
        &self,
        namespace: &NamespaceId,
        boundary: DeletionBoundary,
    ) -> Result<(PreservationGuard, PreservationHeadProof)>;

    /// Decide whether a child may be disclosed to the current caller.
    fn disclose_child(&self, target: &NamespaceId) -> Result<bool>;

    /// Persist lifecycle audit evidence.
    async fn settle_lifecycle_audit(&self, event: DeletionLifecycleEvent) -> Result<()>;
}

/// Boundary at which preservation authority must be freshly observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeletionBoundary {
    /// Before publishing the manifest fence.
    Fence,
    /// Before tombstoning metadata.
    Tombstone,
    /// Before removing live visibility.
    VisibilityRemoval,
    /// Before releasing a parent branch root.
    RootRelease,
    /// Before one bounded cleanup batch.
    CleanupBatch,
    /// Before deleting metadata last.
    MetadataRemoval,
}

/// Redacted lifecycle event supplied to the durable audit adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeletionLifecycleEvent {
    /// Parent-root release advanced or converged.
    RootRelease {
        /// Branch target whose root changed.
        namespace: NamespaceId,
        /// Whether the root was newly removed or already absent safely.
        converged: bool,
    },
    /// Cleanup could not yet finish and must be retried.
    CleanupIncomplete {
        /// Namespace retaining its durable deletion intent.
        namespace: NamespaceId,
        /// Approximate remaining object count, never a caller disclosure.
        remaining: usize,
    },
}

/// Kernel-minted authorization envelope consumed by `NamespaceGraph::delete`.
pub(crate) struct AuthorizedNamespaceDelete {
    /// Namespace selected for deletion.
    pub namespace: NamespaceId,
    /// Typed authorization decision.
    pub decision: DeletionDecision,
    /// Strong governance hooks for the destructive lifecycle.
    pub governance: Arc<dyn DeletionGovernance>,
    /// Cleanup budget for this invocation.
    pub budget: Duration,
}

/// Stable lifecycle marker proving that a branch target's live visibility was
/// removed. The body intentionally contains no process timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BranchVisibilityRemovalMarker {
    /// Schema discriminator for future marker evolution.
    pub domain: String,
    /// Target namespace whose visibility was removed.
    pub target_namespace: NamespaceId,
    /// Exact branch edge being retired.
    pub branch_id: BranchId,
    /// Target lifetime bound by the marker.
    pub target_incarnation: NamespaceIncarnationId,
}

impl BranchVisibilityRemovalMarker {
    /// Marker schema discriminator.
    pub const DOMAIN: &'static str = "zeppelin.branch-visibility-removed.v1";

    /// Deterministic marker key under the target-owned lifecycle prefix.
    #[must_use]
    pub(crate) fn key(
        target: &NamespaceId,
        branch_id: BranchId,
        incarnation: NamespaceIncarnationId,
    ) -> String {
        format!(
            "{target}/_lifecycle/branch_visibility_removed/{}.{}.json",
            branch_id.get(),
            incarnation.as_uuid().simple()
        )
    }

    /// Construct the canonical marker body for one branch lifetime.
    #[must_use]
    pub(crate) fn new(
        target: NamespaceId,
        branch_id: BranchId,
        target_incarnation: NamespaceIncarnationId,
    ) -> Self {
        Self {
            domain: Self::DOMAIN.to_string(),
            target_namespace: target,
            branch_id,
            target_incarnation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BranchVisibilityRemovalMarker;
    use crate::namespace::{BranchId, NamespaceId, NamespaceIncarnationId};

    #[test]
    fn visibility_marker_is_deterministic_and_strict() {
        let target = NamespaceId::new("branch-target").expect("valid namespace");
        let branch = BranchId::new();
        let incarnation = NamespaceIncarnationId::new();
        let marker =
            BranchVisibilityRemovalMarker::new(target.clone(), branch, incarnation.clone());
        let encoded = serde_json::to_vec(&marker).expect("marker encodes");
        let decoded: BranchVisibilityRemovalMarker =
            serde_json::from_slice(&encoded).expect("marker decodes");
        assert_eq!(decoded, marker);
        assert_eq!(marker.domain, BranchVisibilityRemovalMarker::DOMAIN);
        assert_eq!(
            BranchVisibilityRemovalMarker::key(&target, branch, incarnation.clone()),
            format!(
                "branch-target/_lifecycle/branch_visibility_removed/{}.{}.json",
                branch.get(),
                incarnation.as_uuid().simple()
            )
        );
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        value["unexpected"] = serde_json::Value::Bool(true);
        assert!(serde_json::from_value::<BranchVisibilityRemovalMarker>(value).is_err());
    }
}
