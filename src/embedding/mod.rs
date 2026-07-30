//! Typed source inputs and semantic-coverage vocabulary.
//!
//! Retrieval-unit records are durable caller input. They remain separate from
//! dense vectors and from derived encoder artifacts so the original source can
//! be retained and re-embedded without changing dense write semantics.

/// Durable retrieval-unit and semantic-coverage types.
pub mod types;

pub use types::{
    ArtifactChecksum, ContentHash, EmbeddingProfileId, EncoderInputRef, FdeGenerationId,
    ImageObjectRef, InputModality, LateInteractionNamespaceConfig, ModalityCounts,
    MultiVectorEpochId, RetrievalUnitRecord, SemanticCoverageState, SemanticState, TextContentRef,
};
