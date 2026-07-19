//! Security-to-graph namespace-fork activation contract.
//!
//! The security kernel mints [`AuthorizedForkNamespace`] after request
//! admission. The graph owns preparation and the sole target-visibility CAS;
//! governance owns fresh authorization, policy-head fencing, and durable audit.
//! Neither side receives bearer credentials from the other.

use async_trait::async_trait;

use crate::error::Result;
use crate::namespace::{BranchId, NamespaceId};

use super::{ActivationNonce, BranchActivationEvidence, PreparedBranch};

/// Exact retained policy guard used only for crash recovery or cancellation.
///
/// The graph must prove the target outcome first: `finalize` follows matching
/// Active evidence, while `abort` follows a durable exact-nonce revocation.
#[async_trait]
pub(crate) trait BranchActivationGuard: Send {
    /// Remove the guard after the matching activation is already visible.
    async fn finalize(self: Box<Self>) -> Result<()>;

    /// Remove the guard after the matching target nonce is durably revoked.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// Restart-safe guard lookup used by cancellation and background recovery.
#[async_trait]
pub(crate) trait BranchActivationRecovery: Send + Sync {
    /// Fence older policy writers and retain the exact guard when one exists.
    /// `None` proves the newly claimed authoritative head has no guards.
    async fn retain_guard(
        &self,
        branch_id: BranchId,
        nonce: ActivationNonce,
    ) -> Result<Option<Box<dyn BranchActivationGuard>>>;
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

    /// Fence older policy writers and retain a matching crash-stalled guard.
    /// `None` is authoritative only when the claimed head contains no guards.
    async fn retain_guard(
        &self,
        prepared: &PreparedBranch,
        nonce: ActivationNonce,
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
