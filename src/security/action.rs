//! Exhaustive operation inventory used by central authorization.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::SecurityError;

/// One independently grantable Zeppelin operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Action {
    /// Inspect readiness and other protected system state.
    SystemRead,
    /// Read Prometheus metrics or profiling output.
    MetricsRead,
    /// Read the live runtime query configuration.
    RuntimeConfigRead,
    /// Replace or patch the live runtime query configuration.
    RuntimeConfigWrite,
    /// Create a namespace.
    NamespaceCreate,
    /// Read namespace metadata.
    NamespaceRead,
    /// Delete a namespace and its data artifacts.
    NamespaceDelete,
    /// List or inspect named snapshots.
    SnapshotRead,
    /// Create a named snapshot.
    SnapshotWrite,
    /// Delete a named snapshot.
    SnapshotDelete,
    /// Clone a namespace into a new namespace.
    NamespaceClone,
    /// Fork a namespace through the branching lifecycle.
    NamespaceFork,
    /// Change namespace index configuration.
    IndexConfigWrite,
    /// Trigger namespace compaction.
    CompactionTrigger,
    /// Inspect compaction status.
    CompactionStatusRead,
    /// Trigger cache hydration.
    HydrationTrigger,
    /// Fetch stored vectors by identifier.
    VectorFetch,
    /// Insert or replace vectors.
    VectorUpsert,
    /// Delete vectors.
    VectorDelete,
    /// Execute a single or batched retrieval query.
    Query,
    /// Verify a signed structural retrieval receipt.
    ReceiptVerify,
    /// Inspect security principals, credentials, grants, and active policy metadata.
    SecurityAdminRead,
    /// Create or change security principals, credentials, grants, and policy.
    SecurityAdminWrite,
    /// Override caller-owned protected attributes while retaining server stamps.
    ///
    /// This capability is evaluated by the kernel during vector-write
    /// authorization and is deliberately not mapped to an HTTP route.
    AttributeAdmin,
    /// Mint one strictly narrowed, short-lived delegated credential.
    CredentialDelegate,
    /// Create and inspect generic preservation locks.
    PreservationAdmin,
    /// Release one preservation lock under two-person approval.
    PreservationRelease,
}

impl Action {
    /// Every action in declaration order for completeness tests and parsing.
    pub const ALL: [Self; 27] = [
        Self::SystemRead,
        Self::MetricsRead,
        Self::RuntimeConfigRead,
        Self::RuntimeConfigWrite,
        Self::NamespaceCreate,
        Self::NamespaceRead,
        Self::NamespaceDelete,
        Self::SnapshotRead,
        Self::SnapshotWrite,
        Self::SnapshotDelete,
        Self::NamespaceClone,
        Self::NamespaceFork,
        Self::IndexConfigWrite,
        Self::CompactionTrigger,
        Self::CompactionStatusRead,
        Self::HydrationTrigger,
        Self::VectorFetch,
        Self::VectorUpsert,
        Self::VectorDelete,
        Self::Query,
        Self::ReceiptVerify,
        Self::SecurityAdminRead,
        Self::SecurityAdminWrite,
        Self::AttributeAdmin,
        Self::CredentialDelegate,
        Self::PreservationAdmin,
        Self::PreservationRelease,
    ];

    /// Frozen Phase 3 wildcard expansion used by persisted `GrantActions::All`.
    ///
    /// Adding a new action must never silently widen an immutable policy grant.
    /// `AttributeAdmin` therefore requires an explicit selected grant.
    pub(crate) const POLICY_ALL_V1: [Self; 21] = [
        Self::SystemRead,
        Self::MetricsRead,
        Self::RuntimeConfigRead,
        Self::RuntimeConfigWrite,
        Self::NamespaceCreate,
        Self::NamespaceRead,
        Self::NamespaceDelete,
        Self::SnapshotRead,
        Self::SnapshotWrite,
        Self::SnapshotDelete,
        Self::NamespaceClone,
        Self::IndexConfigWrite,
        Self::CompactionTrigger,
        Self::CompactionStatusRead,
        Self::HydrationTrigger,
        Self::VectorFetch,
        Self::VectorUpsert,
        Self::VectorDelete,
        Self::Query,
        Self::SecurityAdminRead,
        Self::SecurityAdminWrite,
    ];

    /// Safe expansion used when publishing a new Phase 7 wildcard request.
    ///
    /// Persisted Phase 3 `All` grants must continue to compile through
    /// [`Self::POLICY_ALL_V1`]. New wildcard requests are normalized to this
    /// explicit set before publication so they do not gain security-policy
    /// mutation or later privileged capabilities.
    pub(crate) const POLICY_SAFE_ALL_V2: [Self; 20] = [
        Self::SystemRead,
        Self::MetricsRead,
        Self::RuntimeConfigRead,
        Self::RuntimeConfigWrite,
        Self::NamespaceCreate,
        Self::NamespaceRead,
        Self::NamespaceDelete,
        Self::SnapshotRead,
        Self::SnapshotWrite,
        Self::SnapshotDelete,
        Self::NamespaceClone,
        Self::IndexConfigWrite,
        Self::CompactionTrigger,
        Self::CompactionStatusRead,
        Self::HydrationTrigger,
        Self::VectorFetch,
        Self::VectorUpsert,
        Self::VectorDelete,
        Self::Query,
        Self::SecurityAdminRead,
    ];

    /// Explicit administrator expansion used only while converting boot config.
    ///
    /// This is deliberately distinct from persisted `GrantActions::All`: a
    /// bootstrap `actions = ["*"]` must retain policy-administration authority,
    /// while a policy wildcard must never acquire privileged security actions.
    pub(crate) const BOOTSTRAP_ADMIN_V1: [Self; 23] = [
        Self::SystemRead,
        Self::MetricsRead,
        Self::RuntimeConfigRead,
        Self::RuntimeConfigWrite,
        Self::NamespaceCreate,
        Self::NamespaceRead,
        Self::NamespaceDelete,
        Self::SnapshotRead,
        Self::SnapshotWrite,
        Self::SnapshotDelete,
        Self::NamespaceClone,
        Self::NamespaceFork,
        Self::IndexConfigWrite,
        Self::CompactionTrigger,
        Self::CompactionStatusRead,
        Self::HydrationTrigger,
        Self::VectorFetch,
        Self::VectorUpsert,
        Self::VectorDelete,
        Self::Query,
        Self::ReceiptVerify,
        Self::SecurityAdminRead,
        Self::SecurityAdminWrite,
    ];

    /// Stable configuration and audit spelling for this action.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SystemRead => "SystemRead",
            Self::MetricsRead => "MetricsRead",
            Self::RuntimeConfigRead => "RuntimeConfigRead",
            Self::RuntimeConfigWrite => "RuntimeConfigWrite",
            Self::NamespaceCreate => "NamespaceCreate",
            Self::NamespaceRead => "NamespaceRead",
            Self::NamespaceDelete => "NamespaceDelete",
            Self::SnapshotRead => "SnapshotRead",
            Self::SnapshotWrite => "SnapshotWrite",
            Self::SnapshotDelete => "SnapshotDelete",
            Self::NamespaceClone => "NamespaceClone",
            Self::NamespaceFork => "NamespaceFork",
            Self::IndexConfigWrite => "IndexConfigWrite",
            Self::CompactionTrigger => "CompactionTrigger",
            Self::CompactionStatusRead => "CompactionStatusRead",
            Self::HydrationTrigger => "HydrationTrigger",
            Self::VectorFetch => "VectorFetch",
            Self::VectorUpsert => "VectorUpsert",
            Self::VectorDelete => "VectorDelete",
            Self::Query => "Query",
            Self::ReceiptVerify => "ReceiptVerify",
            Self::SecurityAdminRead => "SecurityAdminRead",
            Self::SecurityAdminWrite => "SecurityAdminWrite",
            Self::AttributeAdmin => "AttributeAdmin",
            Self::CredentialDelegate => "CredentialDelegate",
            Self::PreservationAdmin => "PreservationAdmin",
            Self::PreservationRelease => "PreservationRelease",
        }
    }

    /// Return whether execution destroys authoritative or user-addressable state.
    #[must_use]
    pub const fn is_destructive(self) -> bool {
        matches!(
            self,
            Self::NamespaceDelete
                | Self::NamespaceFork
                | Self::SnapshotDelete
                | Self::VectorDelete
                | Self::SecurityAdminWrite
                | Self::PreservationRelease
        )
    }

    /// Return whether a delegated token can bind this action to its namespace list.
    ///
    /// Global/control-plane actions and compound namespace creation are excluded:
    /// their real authorization resource cannot be represented by Phase 7's
    /// namespace-only narrowing shape.
    #[must_use]
    pub const fn is_delegatable(self) -> bool {
        matches!(
            self,
            Self::NamespaceRead
                | Self::NamespaceDelete
                | Self::NamespaceFork
                | Self::SnapshotRead
                | Self::SnapshotWrite
                | Self::SnapshotDelete
                | Self::IndexConfigWrite
                | Self::CompactionTrigger
                | Self::CompactionStatusRead
                | Self::HydrationTrigger
                | Self::VectorFetch
                | Self::VectorUpsert
                | Self::VectorDelete
                | Self::Query
                | Self::AttributeAdmin
        )
    }
}

impl FromStr for Action {
    type Err = SecurityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|action| action.as_str() == value)
            .ok_or_else(|| SecurityError::UnknownAction(value.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::Action;

    #[test]
    fn action_inventory_has_exact_phase_ten_variants() {
        let names = Action::ALL.map(Action::as_str).to_vec();

        assert_eq!(
            names,
            vec![
                "SystemRead",
                "MetricsRead",
                "RuntimeConfigRead",
                "RuntimeConfigWrite",
                "NamespaceCreate",
                "NamespaceRead",
                "NamespaceDelete",
                "SnapshotRead",
                "SnapshotWrite",
                "SnapshotDelete",
                "NamespaceClone",
                "NamespaceFork",
                "IndexConfigWrite",
                "CompactionTrigger",
                "CompactionStatusRead",
                "HydrationTrigger",
                "VectorFetch",
                "VectorUpsert",
                "VectorDelete",
                "Query",
                "ReceiptVerify",
                "SecurityAdminRead",
                "SecurityAdminWrite",
                "AttributeAdmin",
                "CredentialDelegate",
                "PreservationAdmin",
                "PreservationRelease",
            ]
        );
    }

    #[test]
    fn namespace_fork_is_delegatable() {
        assert!(Action::NamespaceFork.is_delegatable());
        assert!(Action::NamespaceFork.is_destructive());
        assert!(!Action::NamespaceClone.is_delegatable());
    }

    #[test]
    fn attribute_admin_is_parseable_but_not_in_the_frozen_policy_all_set() {
        assert!(matches!(
            "AttributeAdmin".parse::<Action>(),
            Ok(Action::AttributeAdmin)
        ));
        assert!(Action::ALL.contains(&Action::AttributeAdmin));
        assert!(!Action::POLICY_ALL_V1.contains(&Action::AttributeAdmin));
        assert_eq!(Action::POLICY_ALL_V1.len(), 21);
    }

    #[test]
    fn phase_seven_safe_all_excludes_privileged_security_actions_without_rewriting_legacy_all() {
        assert!(matches!(
            "CredentialDelegate".parse::<Action>(),
            Ok(Action::CredentialDelegate)
        ));
        assert!(Action::ALL.contains(&Action::CredentialDelegate));
        assert!(!Action::POLICY_ALL_V1.contains(&Action::CredentialDelegate));
        assert!(Action::ALL.contains(&Action::SecurityAdminWrite));
        assert!(Action::POLICY_ALL_V1.contains(&Action::SecurityAdminWrite));
        assert!(!Action::POLICY_SAFE_ALL_V2.contains(&Action::CredentialDelegate));
        assert!(!Action::POLICY_SAFE_ALL_V2.contains(&Action::SecurityAdminWrite));
        assert!(!Action::POLICY_ALL_V1.contains(&Action::PreservationAdmin));
        assert!(!Action::POLICY_ALL_V1.contains(&Action::PreservationRelease));
        assert!(!Action::POLICY_SAFE_ALL_V2.contains(&Action::PreservationAdmin));
        assert!(!Action::POLICY_SAFE_ALL_V2.contains(&Action::PreservationRelease));
        assert_eq!(Action::POLICY_ALL_V1.len(), 21);
        assert_eq!(Action::POLICY_SAFE_ALL_V2.len(), 20);
    }
}
