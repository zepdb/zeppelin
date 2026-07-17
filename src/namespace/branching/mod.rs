//! Persisted identities and validation errors for namespace branching.
//!
//! Phase 02 intentionally exposes no lifecycle coordinator. This module only
//! owns the strong physical-origin vocabulary shared by manifests and the
//! later namespace graph.

mod error;
mod types;

pub use error::BranchError;
pub(crate) use types::ArtifactOriginSetBuilder;
pub use types::{ArtifactOrigin, ArtifactOriginIndex};

#[cfg(feature = "branching-test-support")]
#[doc(hidden)]
pub mod test_support {
    //! Narrow compile-time marker for later external branching fixtures.
    //!
    //! Phase 03 adds storage-backed fixture adapters here. Keeping the module
    //! feature-gated now proves default and server builds cannot select the
    //! temporary test-support surface at runtime.
}
