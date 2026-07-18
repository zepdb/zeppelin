//! Persisted identities and validation errors for namespace branching.
//!
//! This module owns the physical-origin and branch-root vocabulary shared by
//! manifests and the later namespace graph. Phase 04 still exposes no public
//! fork lifecycle coordinator; its source-root mutation primitive remains
//! crate-private.

pub(crate) mod deletion;
mod error;
pub mod http;
mod lifecycle;
mod types;

pub use crate::namespace::types::{
    BranchId, BranchRoot, ForkViewDigest, ManifestDigest, ManifestGeneration,
    SourceDataPlaneConfigDigest,
};
pub use error::{BranchError, DisclosedBranchChild};
pub use lifecycle::{
    BranchDescriptor, BranchLifecycleState, BranchLineage, BranchMaintenanceReport,
    BranchPrepareStage, ForkDataPlaneConfig, ForkIdentity, ForkPrepareIntent,
    ForkReservationIdentity, NamespaceCreationKind, NamespaceDeleteOutcome, NamespaceDeleteRequest,
    PrepareForkOutcome, PrepareForkRequest, PreparedBranch,
};
pub(crate) use types::ArtifactOriginSetBuilder;
pub use types::{ArtifactOrigin, ArtifactOriginIndex};

#[cfg(feature = "branching-test-support")]
#[doc(hidden)]
pub mod test_support;
