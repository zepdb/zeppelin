//! Typed namespace-branching failures.

use thiserror::Error;

use super::types::{ArtifactOrigin, ArtifactOriginIndex};
use crate::namespace::{
    BranchId, BranchRoot, ManifestGeneration, NamespaceId, NamespaceIncarnationId,
};

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
        /// Source namespace; the child count is intentionally not disclosed.
        namespace: String,
    },

    /// A branch cannot be deleted while it has direct child roots.
    #[error("branch {branch_id} has live child branches")]
    BranchHasLiveChildren {
        /// Branch edge whose direct children block deletion.
        branch_id: BranchId,
    },

    /// Retained bytes do not match the exact digest named by a current root.
    #[error("manifest history generation {generation:?} does not match its branch root")]
    ManifestDigestMismatch {
        /// Rooted source generation whose history failed exact verification.
        generation: ManifestGeneration,
    },
}
