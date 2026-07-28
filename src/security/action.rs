//! Exhaustive operation inventory used by central authorization.
//!
//! This file owns the closed vocabulary of *what a caller can ask Zeppelin to
//! do*: one [`Action`] variant per independently grantable operation, the
//! stable string spelling each variant carries into persisted policy documents
//! and audit records, and the small predicates other security modules use to
//! classify an action. It is the leaf of the security subsystem — it depends on
//! nothing but [`SecurityError`], and everything else in `src/security/` names
//! it.
//!
//! It deliberately does **not** own:
//!
//! - *who* may perform an action — that is `policy.rs` (grants) and `kernel.rs`
//!   (evaluation), reached through [`SecurityKernel`](crate::security::SecurityKernel);
//! - *which HTTP route* requires an action — that is
//!   [`ROUTE_ACTIONS`](crate::security::ROUTE_ACTIONS);
//! - *what must happen* once an action is allowed — obligations, mandatory
//!   filters, field masks, and write constraints live on
//!   [`AllowDecision`](crate::security::AllowDecision);
//! - *whether the feature is licensed* — entitlement checks against
//!   [`Feature`](crate::security::Feature) happen in the kernel, not here.
//!
//! ## Reading map
//!
//! 1. [`Action`] — the inventory itself. The variant list is the contract.
//! 2. [`Action::ALL`] — every variant in declaration order, used for parsing
//!    and for completeness tests in this and other modules.
//! 3. The three frozen expansion sets (`POLICY_ALL_V1`, `POLICY_SAFE_ALL_V2`,
//!    `BOOTSTRAP_ADMIN_V1`) — the reason a wildcard grant does not silently
//!    widen when a variant is appended.
//! 4. [`Action::is_destructive`] and [`Action::is_delegatable`] — the
//!    classification predicates consumed by grant validation, approval
//!    obligations, and delegated-token narrowing.
//!
//! ## The four action sets
//!
//! Four distinct subsets exist because "all actions" means something different
//! depending on who wrote the word. Widening any of them is a privilege
//! escalation, so each is a fixed-length array whose length is asserted by
//! tests in this file.
//!
//! ```text
//! Action::ALL (26)                       every variant; parsing + completeness
//!  |
//!  +-- BOOTSTRAP_ADMIN_V1 (22)           boot-config `actions = ["*"]`
//!  |     keeps SecurityAdminWrite so a bootstrap operator can publish policy
//!  |     at all; excludes AttributeAdmin, CredentialDelegate, Preservation*
//!  |
//!  +-- POLICY_ALL_V1 (21)                FROZEN: persisted GrantActions::All
//!  |     what an already-stored wildcard grant expands to, forever
//!  |
//!  +-- POLICY_SAFE_ALL_V2 (20)           normalization target for NEW wildcards
//!        drops SecurityAdminWrite as well, so a wildcard published today
//!        cannot mutate the policy that governs it
//! ```
//!
//! `POLICY_ALL_V1` is the immutability rule made concrete: an operator who
//! stored `"*"` under Phase 3 consented to those 21 actions and nothing more.
//! Appending `NamespaceFork`, `CredentialDelegate`, `PreservationRelease`, or
//! `AttributeAdmin` to [`Action`] must never retroactively grant them, so the
//! expansion set is copied and pinned rather than derived from `ALL`.
//! `POLICY_SAFE_ALL_V2` is the forward-looking version: `policy.rs` rewrites an
//! incoming `GrantActions::All` request into this explicit list before
//! publication, and separately re-issues a narrow `SecurityAdminWrite` grant
//! only for global-scope wildcards, so wildcard authority is visible in the
//! stored document instead of hiding behind a symbol.
//!
//! ## State and persisted artifacts
//!
//! This module reads and writes no object-store state, but its spellings are
//! persisted by others and are therefore a compatibility surface:
//!
//! - [`Action::as_str`] values appear inside the authoritative policy document
//!   under `_security/` and inside every durable audit record. Renaming a
//!   variant's string invalidates stored grants and breaks audit history.
//! - The `Serialize`/`Deserialize` derives use the Rust variant names, which
//!   currently match [`Action::as_str`]. There is no catch-all variant: an
//!   unrecognized name fails loudly, either as a serde error or as
//!   [`SecurityError::UnknownAction`] through [`FromStr`].
//! - `Ord` follows declaration order. `GrantDefinition` sorts and dedupes its
//!   action list, so declaration order is also the canonical order actions take
//!   inside a stored grant. Append new variants at the end; inserting one in the
//!   middle reorders existing documents and shifts the frozen sets' meaning.
//!
//! ## Invariants
//!
//! - [`Action::ALL`] contains every variant exactly once, in declaration order.
//!   `action_inventory_has_exact_phase_ten_variants` pins the full list, so
//!   adding a variant without updating `ALL` fails the test suite rather than
//!   silently producing an unparseable action.
//! - The frozen expansion sets are append-hostile by design. Their lengths are
//!   asserted; growing one requires a deliberate edit and a security review.
//! - `AttributeAdmin` is intentionally absent from every wildcard set and from
//!   [`ROUTE_ACTIONS`](crate::security::ROUTE_ACTIONS). It is a kernel-evaluated capability
//!   that lets a caller override server-owned protected attributes on a vector
//!   write; it can only be held through an explicitly selected grant.
//! - [`Action::is_destructive`] drives two independent controls: grant
//!   validation rejects a `require_approval` entry that names a non-destructive
//!   action, and delegated-parent authorization forces an approval obligation on
//!   destructive actions. `NamespaceFork` is classed destructive because a fork
//!   participates in the branch lifecycle and its deletion semantics.
//! - [`Action::is_delegatable`] gates what a short-lived delegated credential
//!   may carry. Control-plane actions (`SystemRead`, `MetricsRead`,
//!   `RuntimeConfig*`, `SecurityAdmin*`, `CredentialDelegate`, `Preservation*`,
//!   `Preservation*`) and `NamespaceCreate`/`NamespaceClone` are excluded
//!   because their real authorization resource is not a single existing
//!   namespace, and the delegation shape narrows by namespace list only.
//!
//! ## Rust concepts used here
//!
//! [`Action`] is a fieldless enum that is `Copy`, so passing it to the kernel,
//! the route map, and audit costs nothing and never allocates. A Java engineer
//! can read it as an `enum` constant, but with two differences that matter here:
//! the `match` in [`Action::as_str`] is *exhaustive*, so adding a variant is a
//! compile error at every classification site rather than a runtime
//! `IllegalArgumentException`; and there is no `values()` reflection, which is
//! why [`Action::ALL`] is written out by hand and test-pinned. In C terms this
//! is an `enum` plus a hand-maintained name table, with the compiler checking
//! that the table is total.
//!
//! The frozen sets are `const` arrays with explicit lengths (`[Self; 21]`).
//! Changing the number of elements without changing the declared length does not
//! compile — the type system, not a code review, is what stops a wildcard grant
//! from quietly gaining a variant.
//!
//! [`FromStr`] returns `Result<Self, SecurityError>` rather than an `Option`, so
//! an unknown action name propagates a typed, reportable failure instead of
//! degrading to a permissive default. That is the fail-closed rule expressed in
//! the type signature.

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
    pub const ALL: [Self; 26] = [
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
    pub(crate) const BOOTSTRAP_ADMIN_V1: [Self; 22] = [
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
