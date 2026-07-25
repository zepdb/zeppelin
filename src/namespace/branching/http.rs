//! Stable, redacted HTTP contract vocabulary for namespace branches.
//!
//! This module owns the JSON shapes that cross the branch API boundary, and
//! nothing else. There is no lifecycle logic, no authorization, no storage
//! access, and no conversion code here — the types are pure data, and the
//! handlers in `crate::server::handlers::namespace` do the projection. Keeping
//! the wire vocabulary in its own leaf module is what lets the internal
//! identities in `super::lifecycle` change shape without breaking a published
//! contract.
//!
//! ## Where this sits
//!
//! ```text
//! NamespaceGraph  ->  BranchDescriptor        (internal, rich, unredacted)
//!                          |
//!                          | handlers/namespace.rs: list_branches
//!                          | drops incarnation-internal detail, splits the
//!                          | internal state into lifecycle + health
//!                          v
//!                     BranchDescriptorResponse ->  BranchListResponse
//!                                                        |
//!                                                        v
//!                                            GET /v1/namespaces/:ns/branches
//! ```
//!
//! Three entry points use these types:
//!
//! - `crate::server::handlers::namespace::create_branch` — accepts a fork
//!   request body and answers with the handler-local `ForkResponse`, which
//!   embeds [`BranchMode`] from this module.
//! - `crate::server::handlers::namespace::list_branches` — answers with
//!   [`BranchListResponse`], mapping each
//!   [`branching::BranchLifecycleState`](crate::namespace::branching::BranchLifecycleState) onto the
//!   [`BranchLifecycle`] / [`BranchHealth`] pair.
//! - The namespace status response embeds [`BranchStatusDescriptor`] when the
//!   namespace is itself a branch.
//!
//! [`ForkNamespaceRequest`] mirrors the `ForkNamespaceRequest` schema published
//! in `api/zeppelin-api.yaml`. Note that the live handler currently binds its
//! own structurally identical `ForkRequest` type rather than this one; both
//! accept exactly `{"target": "..."}` and both reject unknown fields, so the
//! wire contract is the same, but this type is not on the request path today.
//!
//! ## Gating
//!
//! Branching is off by default and both routes require **two** independent
//! things, checked in two different layers:
//!
//! 1. `config.branching.enabled` — controls whether the branch route is
//!    registered at all, and is re-checked inside each handler as defense in
//!    depth. When false, the handler returns
//!    `BranchError::BranchingNotReady`.
//! 2. A valid `crate::security::Feature::Branching` entitlement — enforced by
//!    the security kernel in `authorize_namespace_fork` and
//!    `authorize_branch_list`, and again on each fresh re-authorization path.
//!
//! Both gates apply to both create and list. A deployment with the config flag
//! on but no entitlement gets a licensing failure, not a branch.
//!
//! ## Redaction and contract invariants
//!
//! - **Fork-only, and the wire says so.** [`BranchMode`] has exactly one
//!   variant, `CopyOnWrite`. There is no merge, rebase, diff, or promote in the
//!   product, so there is no vocabulary for one here. The enum exists rather
//!   than a bare string so that adding a future mode is a deliberate, exhaustive
//!   change.
//! - **Lifecycle is disclosed as a coarse pair, not as internal state.**
//!   [`BranchLifecycle`] answers "what phase" and [`BranchHealth`] answers "what
//!   should the client do", and both are closed enums. Preparation milestones,
//!   activation nonces, policy-head identities, manifest generations, and digest
//!   proofs are all deliberately absent from every type in this module.
//! - **No enumeration oracle.** [`BranchListResponse`] carries only the
//!   `branches` array — no total count and no pagination cursor — so a caller
//!   cannot infer the existence of children it is not authorized to see. The
//!   handler sorts the array by target namespace then branch id, so the response
//!   is stable across calls rather than reflecting storage iteration order.
//! - **Identities are strings on the wire.** [`BranchTargetIdentity`] holds
//!   `String` fields rather than `NamespaceId` / `NamespaceIncarnationId`. That
//!   is intentional: the internal newtypes carry validation rules and may gain
//!   fields, while the published contract must stay a plain, stable JSON shape.
//! - **`materialized` is a cost signal, not a status.** It reports whether every
//!   visible ref in the branch's manifest is target-owned. A freshly created
//!   copy-on-write branch is `false`; it flips to `true` only after the first
//!   compaction fully materializes the branch, which is a full-corpus operation.
//! - **Unknown request fields fail loudly.** [`ForkNamespaceRequest`] is
//!   `#[serde(deny_unknown_fields)]`, so a misspelled key is a 4xx rather than a
//!   silently ignored option.
//! - **Response fields are never omitted.** No type here uses
//!   `skip_serializing_if`, so every documented field is always present and a
//!   client can rely on a fixed shape.
//!
//! ## Rust concepts used here
//!
//! **Closed enums as the wire contract.** `#[serde(rename_all = "snake_case")]`
//! on [`BranchLifecycle`], [`BranchHealth`], and [`BranchMode`] fixes the JSON
//! spelling (`awaiting_authenticated_retry`) independently of the Rust
//! spelling, so a rename refactor cannot quietly change the API. Because the
//! handler matches exhaustively when projecting the internal state, adding an
//! internal lifecycle state fails to compile until someone decides what clients
//! should be told — which is the property a `String` status field would lose.
//!
//! **`Deserialize` and `Serialize` on the same types.** These derive both
//! directions even where only one is used at runtime; that is what allows tests
//! to round-trip a response and assert the exact JSON contract instead of
//! hand-writing it.
//!
//! **Owned `String` and `DateTime<Utc>` rather than borrows.** A response type
//! is built and then handed to `axum` for serialization after the borrowed
//! domain values are gone, so every field is owned. In Java this distinction is
//! invisible because everything is a reference; here it is the compiler
//! preventing a response that outlives the manifest it was derived from.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Strict create-fork request; live-head only in v1.
#[allow(missing_docs)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForkNamespaceRequest {
    /// Target namespace name and idempotency key.
    pub target: String,
}

/// Stable v1 branch mode.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BranchMode {
    CopyOnWrite,
}

/// Public branch lifecycle.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BranchLifecycle {
    Preparing,
    Active,
    Deleting,
}

/// Public branch health.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BranchHealth {
    AwaitingAuthenticatedRetry,
    Ready,
    DeletionInProgress,
}

/// Redacted target identity.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchTargetIdentity {
    pub namespace: String,
    pub incarnation: String,
}

/// Public direct-child descriptor.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchDescriptorResponse {
    pub branch_id: String,
    pub target: BranchTargetIdentity,
    pub mode: BranchMode,
    pub depth: u16,
    pub lifecycle: BranchLifecycle,
    pub health: BranchHealth,
    pub materialized: bool,
    pub created_at: DateTime<Utc>,
}

/// Redacted branch status embedded in target namespace metadata.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchStatusDescriptor {
    pub branch_id: String,
    pub mode: BranchMode,
    pub depth: u16,
    pub lifecycle: BranchLifecycle,
    pub health: BranchHealth,
    pub materialized: bool,
    pub created_at: DateTime<Utc>,
}

/// Direct-child list response without count or pagination oracle fields.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchListResponse {
    pub branches: Vec<BranchDescriptorResponse>,
}
