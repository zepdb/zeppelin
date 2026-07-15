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
    /// Inspect security principals, credentials, grants, and active policy metadata.
    SecurityAdminRead,
    /// Create or change security principals, credentials, grants, and policy.
    SecurityAdminWrite,
    /// Override caller-owned protected attributes while retaining server stamps.
    ///
    /// This capability is evaluated by the kernel during vector-write
    /// authorization and is deliberately not mapped to an HTTP route.
    AttributeAdmin,
}

impl Action {
    /// Every action in declaration order for completeness tests and parsing.
    pub const ALL: [Self; 22] = [
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
        Self::AttributeAdmin,
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
            Self::IndexConfigWrite => "IndexConfigWrite",
            Self::CompactionTrigger => "CompactionTrigger",
            Self::CompactionStatusRead => "CompactionStatusRead",
            Self::HydrationTrigger => "HydrationTrigger",
            Self::VectorFetch => "VectorFetch",
            Self::VectorUpsert => "VectorUpsert",
            Self::VectorDelete => "VectorDelete",
            Self::Query => "Query",
            Self::SecurityAdminRead => "SecurityAdminRead",
            Self::SecurityAdminWrite => "SecurityAdminWrite",
            Self::AttributeAdmin => "AttributeAdmin",
        }
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
    fn action_inventory_has_exact_phase_four_variants() {
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
                "IndexConfigWrite",
                "CompactionTrigger",
                "CompactionStatusRead",
                "HydrationTrigger",
                "VectorFetch",
                "VectorUpsert",
                "VectorDelete",
                "Query",
                "SecurityAdminRead",
                "SecurityAdminWrite",
                "AttributeAdmin",
            ]
        );
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
}
