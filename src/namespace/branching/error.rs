//! Typed namespace-branching failures.
//!
//! This module owns the closed set of conditions that abort a branch operation,
//! and the redaction rules those conditions must obey when they travel back to a
//! caller. It owns nothing else: no HTTP status, no retry policy, no
//! authorization decision, and no repair action. Every variant here means *stop
//! and surface the failure* — none of them describes a fallback or a degraded
//! mode.
//!
//! ## Where this sits
//!
//! [`BranchError`] is produced by the layers that manipulate branch state and is
//! consumed one level up as [`ZeppelinError::Branch`](crate::error::ZeppelinError::Branch):
//!
//! ```text
//! src/wal/manifest.rs        origin-table and root/lineage validation
//! src/namespace/branch_root.rs   lease-fenced parent-root mutation
//! src/namespace/graph.rs     fork, activation, delete state machines
//! src/namespace/manager.rs   meta.json lifecycle validation
//! src/compaction/{mod,gc}.rs materialization and owned-key cleanup
//! src/security/kernel.rs     branch admission
//!            |
//!            |  From<BranchError> for ZeppelinError  (boxed)
//!            v
//!    ZeppelinError::Branch
//!            |
//!            |  status_code() / code() in src/error.rs
//!            v
//!    src/server/handlers/  ->  HTTP response envelope
//! ```
//!
//! This module does not depend on `axum` or on the security kernel; it names
//! only namespace identities and the artifact-origin types from
//! [`branching`](crate::namespace::branching).
//!
//! ## Failure families
//!
//! The variants group into five concerns worth recognizing on sight:
//!
//! - **Reservation and retry conflicts** — [`BranchError::TargetAlreadyExists`],
//!   [`BranchError::IntentMismatch`], [`BranchError::CreatingRecoveryRequired`],
//!   [`BranchError::CancellationInProgress`]. A fork retry that does not match
//!   the persisted reservation byte-for-byte is a conflict, never an update.
//! - **Lifetime and liveness races** —
//!   [`BranchError::SourceIncarnationChanged`], [`BranchError::SourceDeleting`],
//!   [`BranchError::RootReleaseIntentChanged`]. These fire when the namespace
//!   name still resolves but now names a different lifetime or a fenced one.
//! - **Configured bounds** — [`BranchError::BranchDepthExceeded`],
//!   [`BranchError::BranchLimitExceeded`],
//!   [`BranchError::BranchRootLimitExceeded`]. Ancestry depth and direct-child
//!   count are capped by configuration; exceeding a cap is refused, not clamped.
//! - **Persisted-state integrity** — [`BranchError::BranchRootInvalid`],
//!   [`BranchError::BranchRootMismatch`], [`BranchError::BranchRootConflict`],
//!   [`BranchError::BranchRootMissing`], [`BranchError::OrphanBranchRoot`],
//!   [`BranchError::ManifestDigestMismatch`],
//!   [`BranchError::ArtifactOriginInvalid`], [`BranchError::BranchIntegrity`].
//!   These describe object-storage state that violates an invariant. The
//!   authoritative object is never rewritten to make the error go away.
//! - **Deletion blockers and gating** —
//!   [`BranchError::NamespaceHasLiveBranches`],
//!   [`BranchError::BranchHasLiveChildren`], [`BranchError::SecurityWouldWiden`],
//!   [`BranchError::BranchingNotReady`].
//!
//! ## Invariants
//!
//! - **A live child pins its parent.** A source namespace that still has a live
//!   direct child root cannot be deleted; the graph raises
//!   [`BranchError::NamespaceHasLiveBranches`], which `src/error.rs` maps to HTTP
//!   409. The documented alternative for the operator is a copy-clone, not a
//!   forced delete. Deletion is never partially applied to work around this.
//! - **Child listings in errors are authorized, not exhaustive.** Both
//!   liveness blockers carry `visible_children` — only the children the current
//!   principal already passed a disclosure check for — plus a
//!   `has_additional_children` boolean. There is deliberately **no count**, so
//!   the error cannot be used as an enumeration oracle for namespaces the caller
//!   may not read. [`DisclosedBranchChild`] is the only shape allowed through
//!   that boundary.
//! - **Diagnostics are secret-free and low-cardinality.**
//!   [`BranchError::ArtifactOriginInvalid`] carries a structural `reason` string,
//!   a `descriptor_kind` restricted to `&'static str` values (`manifest`,
//!   `fragment`, `segment`), and the offending index or key. It never carries
//!   object bytes, credentials, or policy content.
//! - **Repair identities are explicit.** [`BranchError::OrphanBranchRoot`]
//!   returns the whole [`BranchRoot`] because an operator
//!   needs the exact digest proof to reconcile a parent that outlived its child.
//!   This is the one variant that deliberately widens disclosure. It is raised
//!   to a caller only through the graph; the readiness path reports the same
//!   condition through a separate operator-safe projection, which
//!   `security.readyz_public` withholds from an unauthenticated `/readyz` body.
//!
//! ## Status mapping lives in `src/error.rs`, and is narrow
//!
//! Only four variants currently receive a non-500 status and a stable machine
//! code: [`BranchError::TargetAlreadyExists`],
//! [`BranchError::NamespaceHasLiveBranches`],
//! [`BranchError::BranchHasLiveChildren`], and
//! [`BranchError::CancellationInProgress`] map to 409.
//! [`BranchError::BranchIntegrity`] gets the `branch_integrity_error` code but
//! keeps a 500. Every other variant — including
//! [`BranchError::BranchingNotReady`], which is what the handlers return when
//! `config.branching.enabled` is false — falls through to HTTP 500 with the
//! generic `INTERNAL_ERROR` code. Read `ZeppelinError::status_code` before
//! assuming a variant is client-visible as anything other than a server error.
//!
//! ## Rust concepts used here
//!
//! **`thiserror` instead of a message string.** `#[derive(Error)]` generates the
//! `Display` implementation from the `#[error("...")]` attributes, so the
//! human-readable text lives beside the structured fields it interpolates and
//! cannot drift from them. The nearest Java analogue is a checked-exception
//! hierarchy; the difference is that a Rust `match` over this enum is
//! exhaustively checked, so adding a variant breaks every incomplete handler at
//! compile time rather than at runtime.
//!
//! **`Clone + PartialEq + Eq` on an error type.** This is unusual and
//! deliberate: it lets tests assert an exact expected failure, and lets the
//! graph compare a persisted failure against a freshly derived one. The cost is
//! that no variant may hold a non-comparable source such as `std::io::Error`, so
//! this enum has no `#[source]` chain — storage failures stay in their own
//! [`ZeppelinError`](crate::error::ZeppelinError) variants instead of being wrapped here.
//!
//! **Boxed at the crate boundary.** [`BranchError`] is a large enum (several
//! variants own `String`s and a `Vec`), so `ZeppelinError::Branch` stores it as
//! `Box<BranchError>`. Without that, every `Result` in the crate would grow to
//! this enum's size, which is why the origin builder in `super::types` carries an
//! explicit `clippy::result_large_err` allowance where it returns the unboxed
//! form.

use thiserror::Error;

use super::types::{ArtifactOrigin, ArtifactOriginIndex};
use crate::namespace::{
    BranchId, BranchRoot, ManifestGeneration, NamespaceId, NamespaceIncarnationId,
};

/// One direct child identity the current caller may safely receive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisclosedBranchChild {
    /// Target namespace authorized for current `NamespaceRead`.
    pub namespace: NamespaceId,
    /// Stable direct-edge identity from the authoritative parent root.
    pub branch_id: BranchId,
}

/// Failures produced while validating or coordinating namespace branches.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum BranchError {
    /// The target name belongs to another namespace creation intent.
    #[error("branch target {target} already exists")]
    TargetAlreadyExists {
        /// Conflicting target namespace.
        target: NamespaceId,
    },

    /// Persisted reservation fields do not match the retry request.
    #[error("branch intent for target {target} does not match this request")]
    IntentMismatch {
        /// Reserved target namespace.
        target: NamespaceId,
    },

    /// The source name now identifies a different namespace lifetime.
    #[error("branch source {namespace} incarnation changed")]
    SourceIncarnationChanged {
        /// Source namespace whose lifetime changed.
        namespace: NamespaceId,
    },

    /// Root release raced with a different target deletion intent.
    #[error("branch root-release intent for namespace {namespace} changed")]
    RootReleaseIntentChanged {
        /// Target namespace whose exact deletion intent no longer matches.
        namespace: NamespaceId,
    },

    /// The source entered deletion before a first root could be published.
    #[error("branch source {namespace} is deleting")]
    SourceDeleting {
        /// Source namespace being deleted or fenced.
        namespace: NamespaceId,
    },

    /// A fork would exceed the configured ancestry bound.
    #[error("branch depth {depth} exceeds configured limit {limit}")]
    BranchDepthExceeded {
        /// Proposed target depth.
        depth: u16,
        /// Configured maximum depth.
        limit: u16,
    },

    /// The source already has the configured number of direct children.
    #[error("branch child limit {limit} would be exceeded")]
    BranchLimitExceeded {
        /// Configured maximum direct children.
        limit: usize,
    },

    /// A persisted root exists for the branch ID but not the exact final identity.
    #[error("branch root {branch_id} does not match the prepared branch identity")]
    BranchRootMismatch {
        /// Mismatched direct-edge identity.
        branch_id: BranchId,
    },

    /// A non-visible reservation requires graph recovery rather than root recovery.
    #[error("creating branch target {target} requires namespace-graph recovery")]
    CreatingRecoveryRequired {
        /// Reserved branch target.
        target: NamespaceId,
    },

    /// A governed cancellation intent won before the target became active.
    #[error("creating branch target {target} is being cancelled")]
    CancellationInProgress {
        /// Never-active target protected from further publication.
        target: NamespaceId,
    },

    /// Authorization would make the target less restrictive than its source.
    #[error("branch security policy would widen source access")]
    SecurityWouldWiden,

    /// A persisted descriptor cannot resolve to one valid physical owner.
    #[error(
        "artifact origin invalid for {manifest_namespace}/{manifest_incarnation:?} \
         {descriptor_kind} {descriptor_id}: {reason}"
    )]
    ArtifactOriginInvalid {
        /// Logical namespace bound to the containing manifest.
        manifest_namespace: String,
        /// Namespace-lifetime identity bound to the containing manifest.
        manifest_incarnation: Option<NamespaceIncarnationId>,
        /// Low-cardinality descriptor family (`manifest`, `fragment`, or `segment`).
        descriptor_kind: &'static str,
        /// Stable descriptor ID, or the manifest field name for table errors.
        descriptor_id: String,
        /// Invalid persisted origin-table index when one was present.
        offending_index: Option<ArtifactOriginIndex>,
        /// Invalid explicit object key when one was present.
        offending_key: Option<String>,
        /// Origin the descriptor was required to use when known.
        expected_origin: Option<ArtifactOrigin>,
        /// Secret-free structural diagnostic.
        reason: String,
    },

    /// A valid future branch shape reached production before its safety phase.
    #[error("branching is not ready for {feature}")]
    BranchingNotReady {
        /// Reserved feature or binding projection that is not yet admitted.
        feature: &'static str,
    },

    /// A persisted or proposed root violates structural domain invariants.
    #[error("branch root {branch_id:?} is invalid: {reason}")]
    BranchRootInvalid {
        /// Branch identity when decoding reached it successfully.
        branch_id: Option<BranchId>,
        /// Secret-free structural diagnostic.
        reason: String,
    },

    /// The branch ID already names a different exact root identity.
    #[error("branch root {branch_id} conflicts with the authoritative manifest")]
    BranchRootConflict {
        /// Conflicting branch identity.
        branch_id: BranchId,
    },

    /// An exact root required for lifecycle progress is absent.
    #[error("branch root {branch_id} is missing")]
    BranchRootMissing {
        /// Missing branch identity.
        branch_id: BranchId,
    },

    /// A parent still retains an exact root after its target lifetime vanished.
    #[error("source namespace {source_namespace} retains an orphan branch root {root:?}")]
    OrphanBranchRoot {
        /// Parent namespace whose authoritative manifest retains the root.
        source_namespace: NamespaceId,
        /// Exact bounded operator-repair identity and digest proof.
        root: BranchRoot,
    },

    /// Publishing another root would exceed the configured manifest bound.
    #[error("branch root limit {limit} would be exceeded")]
    BranchRootLimitExceeded {
        /// Configured maximum direct-child root count.
        limit: usize,
    },

    /// Governed deletion cannot fence a namespace with current live children.
    #[error("namespace {namespace} has live branches")]
    NamespaceHasLiveBranches {
        /// Source namespace; never used as an exact-child-count oracle.
        namespace: String,
        /// Bounded direct children authorized for caller disclosure.
        visible_children: Vec<DisclosedBranchChild>,
        /// Whether denied children also block deletion.
        has_additional_children: bool,
    },

    /// A branch cannot be deleted while it has direct child roots.
    #[error("branch {branch_id} has live child branches")]
    BranchHasLiveChildren {
        /// Branch edge whose direct children block deletion.
        branch_id: BranchId,
        /// Bounded direct children authorized for caller disclosure.
        visible_children: Vec<DisclosedBranchChild>,
        /// Whether denied children also block deletion.
        has_additional_children: bool,
    },

    /// An authorized direct child failed exact root/metadata identity checks.
    #[error("authorized branch state failed an integrity check")]
    BranchIntegrity,

    /// Retained bytes do not match the exact digest named by a current root.
    #[error("manifest history generation {generation:?} does not match its branch root")]
    ManifestDigestMismatch {
        /// Rooted source generation whose history failed exact verification.
        generation: ManifestGeneration,
    },
}
