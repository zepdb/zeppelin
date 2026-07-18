//! Security-to-graph deletion contract.
//!
//! This module contains the typed envelope and callbacks consumed by the
//! authoritative namespace graph. It deliberately carries decisions and
//! governance hooks, never bearer credentials or caller-supplied roots.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::error::Result;
use crate::namespace::NamespaceId;
use crate::namespace::{BranchId, NamespaceIncarnationId};
use crate::security::{
    DecisionId, PolicyVersion, PreservationGuard, PreservationHeadProof, PrincipalId,
};
use crate::storage::{CreateOnlyOutcome, ZeppelinStore};
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

type PreservationCallback = dyn Fn(
        NamespaceId,
        DeletionBoundary,
    )
        -> Pin<Box<dyn Future<Output = Result<(PreservationGuard, PreservationHeadProof)>> + Send>>
    + Send
    + Sync;
type AuditCallback = dyn Fn(DeletionLifecycleEvent) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
    + Send
    + Sync;
type DisclosureCallback = dyn Fn(&NamespaceId) -> Result<bool> + Send + Sync;

/// Callback-backed governance adapter used by the security/server boundary.
pub(crate) struct CallbackDeletionGovernance {
    preservation: Arc<PreservationCallback>,
    disclose: Arc<DisclosureCallback>,
    audit: Arc<AuditCallback>,
}

impl CallbackDeletionGovernance {
    /// Assemble callbacks without carrying bearer credentials into the graph.
    #[must_use]
    pub(crate) fn new(
        preservation: Arc<PreservationCallback>,
        disclose: Arc<DisclosureCallback>,
        audit: Arc<AuditCallback>,
    ) -> Self {
        Self {
            preservation,
            disclose,
            audit,
        }
    }
}

#[async_trait]
impl DeletionGovernance for CallbackDeletionGovernance {
    async fn preservation_boundary(
        &self,
        namespace: &NamespaceId,
        boundary: DeletionBoundary,
    ) -> Result<(PreservationGuard, PreservationHeadProof)> {
        (self.preservation)(namespace.clone(), boundary).await
    }

    fn disclose_child(&self, target: &NamespaceId) -> Result<bool> {
        (self.disclose)(target)
    }

    async fn settle_lifecycle_audit(&self, event: DeletionLifecycleEvent) -> Result<()> {
        (self.audit)(event).await
    }
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

impl AuthorizedNamespaceDelete {
    /// Construct an envelope after the security layer has completed
    /// authorization and assembled its governance callbacks.
    #[must_use]
    pub(crate) fn new(
        namespace: NamespaceId,
        decision: DeletionDecision,
        governance: Arc<dyn DeletionGovernance>,
        budget: Duration,
    ) -> Self {
        Self {
            namespace,
            decision,
            governance,
            budget,
        }
    }
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

/// Persist or adopt the exact branch visibility marker and derive its grace
/// deadline from the authoritative S3 object timestamp.
pub(crate) async fn persist_branch_visibility_removal(
    store: &ZeppelinStore,
    target: &NamespaceId,
    branch_id: BranchId,
    incarnation: NamespaceIncarnationId,
    grace_floor: Duration,
) -> Result<super::super::manager::VisibilityRemoval> {
    let marker = BranchVisibilityRemovalMarker::new(target.clone(), branch_id, incarnation.clone());
    let key = BranchVisibilityRemovalMarker::key(target, branch_id, incarnation);
    let body = serde_json::to_vec(&marker).map_err(|error| {
        crate::error::ZeppelinError::Serialization(format!("visibility marker encode: {error}"))
    })?;
    match store
        .put_create_outcome(&key, Bytes::from(body.clone()))
        .await?
    {
        CreateOnlyOutcome::Created { .. } => {}
        CreateOnlyOutcome::AlreadyExists => {
            let existing = store.get(&key).await?;
            if existing.as_ref() != body.as_slice() {
                return Err(crate::error::ZeppelinError::Validation(format!(
                    "branch visibility marker {key} has conflicting bytes"
                )));
            }
        }
    }
    let observed_at = store.head(&key).await?.last_modified;
    let rounded = observed_at
        .checked_add_signed(ChronoDuration::seconds(1))
        .ok_or_else(|| {
            crate::error::ZeppelinError::Validation("marker timestamp overflow".to_string())
        })?
        .timestamp();
    let floor = ChronoDuration::from_std(grace_floor).map_err(|_| {
        crate::error::ZeppelinError::Validation(
            "branch grace floor exceeds chrono range".to_string(),
        )
    })?;
    let not_before = DateTime::<Utc>::from_timestamp(rounded, 0)
        .ok_or_else(|| {
            crate::error::ZeppelinError::Validation("invalid marker timestamp".to_string())
        })?
        .checked_add_signed(floor)
        .ok_or_else(|| {
            crate::error::ZeppelinError::Validation("branch grace deadline overflow".to_string())
        })?;
    Ok(super::super::manager::VisibilityRemoval {
        marker_key: key,
        observed_at,
        not_before,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        BranchVisibilityRemovalMarker, CallbackDeletionGovernance, DeletionBoundary,
        DeletionGovernance, DeletionLifecycleEvent,
    };
    use crate::namespace::{BranchId, NamespaceId, NamespaceIncarnationId};
    use crate::security::{PreservationGuard, PreservationHeadProof};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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

    #[tokio::test]
    async fn callback_governance_forwards_all_hooks() {
        let preservation_calls = Arc::new(AtomicUsize::new(0));
        let audit_calls = Arc::new(AtomicUsize::new(0));
        let disclose_calls = Arc::new(AtomicUsize::new(0));
        let preservation_calls_for_cb = Arc::clone(&preservation_calls);
        let audit_calls_for_cb = Arc::clone(&audit_calls);
        let disclose_calls_for_cb = Arc::clone(&disclose_calls);
        let adapter = CallbackDeletionGovernance::new(
            Arc::new(move |_namespace, _boundary| {
                let calls = Arc::clone(&preservation_calls_for_cb);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok((
                        PreservationGuard::unlocked(),
                        PreservationHeadProof {
                            head_sha256: [0; 32],
                            e_tag: None,
                        },
                    ))
                })
            }),
            Arc::new(move |_target| {
                disclose_calls_for_cb.fetch_add(1, Ordering::SeqCst);
                Ok(true)
            }),
            Arc::new(move |_event| {
                let calls = Arc::clone(&audit_calls_for_cb);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
        );
        let target = NamespaceId::new("adapter-target").unwrap();
        adapter
            .preservation_boundary(&target, DeletionBoundary::Fence)
            .await
            .unwrap();
        assert!(adapter.disclose_child(&target).unwrap());
        adapter
            .settle_lifecycle_audit(DeletionLifecycleEvent::CleanupIncomplete {
                namespace: target,
                remaining: 1,
            })
            .await
            .unwrap();
        assert_eq!(preservation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(disclose_calls.load(Ordering::SeqCst), 1);
        assert_eq!(audit_calls.load(Ordering::SeqCst), 1);
    }
}
