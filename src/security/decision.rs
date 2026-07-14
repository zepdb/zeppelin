//! Full-shaped allow and deny values returned before domain work.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::types::{AttributeValue, Filter};

use super::Action;

/// Collision-resistant identity shared by authorization and audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DecisionId(Ulid);

impl DecisionId {
    /// Generate a fresh decision identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// Return the underlying ULID.
    #[must_use]
    pub const fn get(self) -> Ulid {
        self.0
    }
}

impl Default for DecisionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Monotonic security-policy version attached to every decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PolicyVersion(u64);

impl PolicyVersion {
    /// Boot-config policy version used until S3 policy lands in phase 3.
    pub const BOOT: Self = Self(0);

    /// Return the numeric policy version.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Stable reason why an authenticated request was denied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DenyReason {
    /// No authenticated identity was available.
    Unauthenticated,
    /// The presented credential is past its configured expiry.
    CredentialExpired,
    /// The credential or principal is not known to current policy.
    CredentialUnknown,
    /// The principal lacks the requested action.
    ActionNotGranted,
    /// The action is granted, but not for this namespace.
    NamespaceNotGranted,
    /// Authoritative policy freshness cannot be proven.
    SecurityStale,
    /// A required audit, approval, or other obligation was not satisfied.
    ObligationUnsatisfied,
}

impl DenyReason {
    /// Stable lowercase wire and audit reason code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::CredentialExpired => "credential_expired",
            Self::CredentialUnknown => "credential_unknown",
            Self::ActionNotGranted => "action_not_granted",
            Self::NamespaceNotGranted => "namespace_not_granted",
            Self::SecurityStale => "security_stale",
            Self::ObligationUnsatisfied => "obligation_unsatisfied",
        }
    }
}

/// Attribute names denied from response projection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldMask {
    /// Attribute keys removed from every observation surface.
    pub deny: BTreeSet<String>,
}

impl FieldMask {
    /// Return whether this mask removes no fields.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.deny.is_empty()
    }
}

/// Server-owned attribute stamps and caller-forbidden fields.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WriteConstraints {
    /// Attributes stamped by the server, overriding caller values.
    pub stamp: BTreeMap<String, AttributeValue>,
    /// Attributes ordinary callers may not set or change.
    pub forbid_set: BTreeSet<String>,
}

impl WriteConstraints {
    /// Construct the phase-1 empty constraint set.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Return whether this value constrains no attributes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.stamp.is_empty() && self.forbid_set.is_empty()
    }
}

/// Additional work that must complete before an allowed operation succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Obligation {
    /// Persist an audit record before returning success.
    DurableAudit,
    /// Obtain a distinct authorized approver.
    Approval,
}

/// Authorization result that callers must handle explicitly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Decision {
    /// The operation may proceed with all attached constraints and obligations.
    Allow(AllowDecision),
    /// The operation must not reach domain logic.
    Deny(DenyDecision),
}

/// Full set of server-owned constraints for one allowed operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowDecision {
    /// Identity shared with audit evidence.
    pub decision_id: DecisionId,
    /// Authoritative policy version used for the evaluation.
    pub policy_version: PolicyVersion,
    /// Server-owned filter ANDed with any caller filter.
    pub mandatory_filter: Option<Filter>,
    /// Server-owned response projection restrictions.
    pub field_mask: Option<FieldMask>,
    /// Server-owned write attribute constraints.
    pub write_constraints: WriteConstraints,
    /// Required work that must settle before success.
    pub obligations: Vec<Obligation>,
}

impl AllowDecision {
    /// Construct the full-shaped boot-policy allow value for one action.
    ///
    /// Phase 2 attaches the baseline durable-audit obligation here so the
    /// kernel's decision remains the single source of pre-success work.
    #[must_use]
    pub fn boot(action: Action) -> Self {
        let obligations = if matches!(
            action,
            Action::RuntimeConfigWrite
                | Action::NamespaceDelete
                | Action::SnapshotDelete
                | Action::IndexConfigWrite
                | Action::VectorDelete
        ) {
            vec![Obligation::DurableAudit]
        } else {
            Vec::new()
        };
        Self {
            decision_id: DecisionId::new(),
            policy_version: PolicyVersion::BOOT,
            mandatory_filter: None,
            field_mask: None,
            write_constraints: WriteConstraints::none(),
            obligations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AllowDecision, Obligation};
    use crate::security::Action;

    #[test]
    fn phase_two_durable_audit_obligation_inventory_is_exact() {
        let durable = Action::ALL
            .into_iter()
            .filter(|action| {
                AllowDecision::boot(*action)
                    .obligations
                    .contains(&Obligation::DurableAudit)
            })
            .collect::<Vec<_>>();

        assert_eq!(
            durable,
            vec![
                Action::RuntimeConfigWrite,
                Action::NamespaceDelete,
                Action::SnapshotDelete,
                Action::IndexConfigWrite,
                Action::VectorDelete,
            ]
        );
    }
}

/// Explicit failed authorization decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DenyDecision {
    /// Identity shared with audit evidence.
    pub decision_id: DecisionId,
    /// Authoritative policy version used for the evaluation.
    pub policy_version: PolicyVersion,
    /// Stable reason the operation was rejected.
    pub reason: DenyReason,
}

impl DenyDecision {
    /// Construct a boot-policy denial with a fresh decision identity.
    #[must_use]
    pub fn boot(reason: DenyReason) -> Self {
        Self {
            decision_id: DecisionId::new(),
            policy_version: PolicyVersion::BOOT,
            reason,
        }
    }
}
