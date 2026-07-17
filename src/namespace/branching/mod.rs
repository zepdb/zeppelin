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
pub mod test_support;
