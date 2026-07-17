//! Typed namespace-branching failures.

use thiserror::Error;

use super::types::{ArtifactOrigin, ArtifactOriginIndex};
use crate::namespace::NamespaceIncarnationId;

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
}
