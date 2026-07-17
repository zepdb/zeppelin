//! Typed namespace-branching failures.

use thiserror::Error;

use super::types::{ArtifactOrigin, ArtifactOriginIndex};
use crate::namespace::{BranchId, ManifestGeneration, NamespaceIncarnationId};

/// Failures produced while validating or coordinating namespace branches.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum BranchError {
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

    /// Retained bytes do not match the exact digest named by a current root.
    #[error("manifest history generation {generation:?} does not match its branch root")]
    ManifestDigestMismatch {
        /// Rooted source generation whose history failed exact verification.
        generation: ManifestGeneration,
    },
}
