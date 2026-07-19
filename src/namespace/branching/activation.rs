//! Security-to-graph namespace-fork activation contract.
//!
//! The security kernel mints [`AuthorizedForkNamespace`] after request
//! admission. The graph owns preparation and the sole target-visibility CAS;
//! governance owns fresh authorization, policy-head fencing, and durable audit.
//! Neither side receives bearer credentials from the other.

use async_trait::async_trait;

use crate::error::Result;
use crate::namespace::{BranchId, NamespaceId, NamespaceIncarnationId};

use super::{ActivationNonce, BranchActivationEvidence, ForkIdentity, PreparedBranch};

/// Stable branch identity used to locate policy guards without trusting a
/// stale activation nonce supplied by one crashed worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BranchActivationTarget {
    branch_id: BranchId,
    target_namespace: NamespaceId,
    target_incarnation: NamespaceIncarnationId,
}

impl BranchActivationTarget {
    /// Assemble the exact branch edge and target lifetime named by recovery.
    #[must_use]
    pub(crate) fn new(
        branch_id: BranchId,
        target_namespace: NamespaceId,
        target_incarnation: NamespaceIncarnationId,
    ) -> Self {
        Self {
            branch_id,
            target_namespace,
            target_incarnation,
        }
    }

    /// Derive the recovery identity from one validated fork identity.
    #[must_use]
    pub(crate) fn from_identity(identity: &ForkIdentity) -> Self {
        Self::new(
            identity.branch_id,
            identity.target_namespace.clone(),
            identity.target_incarnation.clone(),
        )
    }

    /// Stable parent-to-child branch edge.
    #[must_use]
    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    /// Exact target namespace named by the branch edge.
    #[must_use]
    pub(crate) fn target_namespace(&self) -> &NamespaceId {
        &self.target_namespace
    }

    /// Exact target lifetime named by the branch edge.
    #[must_use]
    pub(crate) fn target_incarnation(&self) -> &NamespaceIncarnationId {
        &self.target_incarnation
    }
}

/// Exact persisted activation attempt returned by guard recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BranchActivationAttempt {
    target: BranchActivationTarget,
    nonce: ActivationNonce,
}

impl BranchActivationAttempt {
    /// Bind one target lifetime to its one-shot metadata nonce.
    #[must_use]
    pub(crate) fn new(target: BranchActivationTarget, nonce: ActivationNonce) -> Self {
        Self { target, nonce }
    }

    /// Branch target whose metadata must decide the recovery outcome.
    #[must_use]
    pub(crate) const fn target(&self) -> &BranchActivationTarget {
        &self.target
    }

    /// One-shot target metadata fence carried by the policy guard.
    #[must_use]
    pub(crate) const fn nonce(&self) -> ActivationNonce {
        self.nonce
    }
}

/// Exact retained policy guard used only for crash recovery or cancellation.
///
/// The graph must prove the target outcome first: `finalize` follows matching
/// Active evidence, while `abort` follows a durable exact-nonce revocation.
#[async_trait]
pub(crate) trait BranchActivationGuard: Send {
    /// Exact persisted target/nonce identity covered by this retained guard.
    fn attempt(&self) -> &BranchActivationAttempt;

    /// Remove the guard after the matching activation is already visible.
    async fn finalize(self: Box<Self>) -> Result<()>;

    /// Remove the guard after the matching target nonce is durably revoked.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Restart-safe guard lookup used by cancellation and background recovery.
#[async_trait]
pub(crate) trait BranchActivationRecovery: Send + Sync {
    /// Fence older policy writers and retain the guard for this exact target
    /// lifetime. `None` proves the claimed head has no entry for its branch ID.
    async fn retain_branch(
        &self,
        target: &BranchActivationTarget,
    ) -> Result<Option<Box<dyn BranchActivationGuard>>>;

    /// Retain the deterministic next expired guard from one claimed head.
    async fn retain_next_expired(&self) -> Result<Option<Box<dyn BranchActivationGuard>>>;
}

/// One policy/audit permit retained across the target visibility CAS.
///
/// Implementations must install any mutable policy-head guard before returning
/// from governance `begin`. The graph settles audit, revalidates the retained
/// permit, and only then attempts the exact nonce-bound metadata CAS.
#[async_trait]
pub(crate) trait BranchActivationPermit: Send {
    /// Immutable evidence that the target visibility CAS must retain.
    fn evidence(&self) -> &BranchActivationEvidence;

    /// Durably settle the pre-activation audit and approval linkage.
    async fn settle_audit(&mut self) -> Result<()>;

    /// Re-prove time-bound authority and the exact guard immediately before
    /// the visibility CAS. This must not replace the audited decision.
    async fn revalidate(&mut self) -> Result<()>;

    /// Remove the exact policy guard after matching Active evidence exists.
    async fn finalize(self: Box<Self>) -> Result<()>;

    /// Remove the exact policy guard only after the graph revoked its target
    /// nonce and proved that the target remains non-visible.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Fresh governance required to turn a prepared fork into a visible target.
#[async_trait]
pub(crate) trait BranchActivationGovernance: Send + Sync {
    /// Reauthorize against current authority and install the activation guard.
    async fn begin(
        &self,
        prepared: &PreparedBranch,
        nonce: ActivationNonce,
    ) -> Result<Box<dyn BranchActivationPermit>>;

    /// Fence older policy writers and retain this branch target's guard. An
    /// expected nonce requests exact retry; `None` performs branch recovery.
    async fn retain_guard(
        &self,
        prepared: &PreparedBranch,
        expected_nonce: Option<ActivationNonce>,
    ) -> Result<Option<Box<dyn BranchActivationGuard>>>;
}

/// Kernel-minted request consumed by [`crate::namespace::graph::NamespaceGraph`].
pub(crate) struct AuthorizedForkNamespace {
    source: NamespaceId,
    target: NamespaceId,
    governance: Box<dyn BranchActivationGovernance>,
}

impl AuthorizedForkNamespace {
    /// Assemble the opaque graph request after initial HTTP admission.
    #[must_use]
    pub(crate) fn new(
        source: NamespaceId,
        target: NamespaceId,
        governance: Box<dyn BranchActivationGovernance>,
    ) -> Self {
        Self {
            source,
            target,
            governance,
        }
    }

    /// Consume the envelope exactly once at the graph seam.
    pub(crate) fn into_parts(
        self,
    ) -> (
        NamespaceId,
        NamespaceId,
        Box<dyn BranchActivationGovernance>,
    ) {
        (self.source, self.target, self.governance)
    }
}

/// Publicly observable result of one complete prepare-and-activate operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ForkOutcome {
    /// This request won the durable target reservation and activated it.
    Created(PreparedBranch),
    /// A prior exact request reserved or activated the same branch edge.
    Existing(PreparedBranch),
}

impl ForkOutcome {
    /// Borrow the verified branch description returned to the HTTP adapter.
    #[must_use]
    pub(crate) const fn branch(&self) -> &PreparedBranch {
        match self {
            Self::Created(branch) | Self::Existing(branch) => branch,
        }
    }

    /// Whether this request created the durable target reservation.
    #[must_use]
    pub(crate) const fn created(&self) -> bool {
        matches!(self, Self::Created(_))
    }
}
