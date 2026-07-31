//! Typed source inputs and semantic-coverage vocabulary.
//!
//! Retrieval-unit records are durable caller input. They remain separate from
//! dense vectors and from derived encoder artifacts so the original source can
//! be retained and re-embedded without changing dense write semantics.

/// Deterministic codecs for immutable matrix, FDE, and centering artifacts.
pub mod artifact;
/// Bounded background enrichment and fenced overlay publication.
pub mod coordinator;
/// Explicitly gated deterministic adapter for development and tests.
pub mod dev;
/// Encoder-neutral validated values and asynchronous adapter contract.
pub mod encoder;
/// Lazy configuration-backed encoder session ownership.
pub mod provider;
/// Durable retrieval-unit and semantic-coverage types.
pub mod types;
/// Digest-pinned external encoder-worker adapter.
pub mod worker;

pub use artifact::{
    CenteringArtifact, EmbeddingArtifactError, FdeArtifact, FdeArtifactRow, ImmutableArtifactBytes,
    MatrixArtifact, MatrixArtifactRow, CENTERING_ARTIFACT_FORMAT_VERSION,
    FDE_ARTIFACT_FORMAT_VERSION, MATRIX_ARTIFACT_FORMAT_VERSION,
};
pub use coordinator::{
    EnrichmentAdmissionReport, EnrichmentCheckpoint, EnrichmentCoordinator,
    EnrichmentCoordinatorOptions, EnrichmentWorkId, MultiVectorEncoderProvider,
    MultiVectorEncoderRegistry,
};
pub use dev::{DeterministicDev, DETERMINISTIC_DEV_IMPLEMENTATION, DETERMINISTIC_DEV_VERSION};
pub use encoder::{
    EncoderDocumentInput, EncoderQueryInput, MultiVectorEmbedding, MultiVectorEmbeddingBatch,
    MultiVectorEncoder,
};
pub use provider::ConfiguredEncoderProvider;
pub use types::{
    ArtifactChecksum, CandidateDocumentPooling, ContentHash, EmbeddingProfileId,
    EmbeddingProfileRef, EncoderExecutionRef, EncoderInputRef, ExactScorerVersion, FdeFragmentRef,
    FdeGenerationId, FdeRecipe, FdeTransformArtifactRef, ImageObjectRef, InputModality,
    Int8QualificationStamp, LateInteractionNamespaceConfig, MatrixDtype, MeanVectorRef,
    ModalityCounts, MultiVectorEmbeddingFragmentRef, MultiVectorEpoch, MultiVectorEpochId,
    NormalizationRecipe, PhysicalInputFragmentIdentity, RecordVersionCoverage, RecordVersionRef,
    RetrievalUnitRecord, SemanticCoverageState, SemanticOverlayRef, SemanticState, TextContentRef,
    VectorTransformRecipe, INT8_QUALIFICATION_STAMP_VERSION,
};
pub use worker::{materialize_bundle_from_s3, PinnedWorker, PinnedWorkerConfig};
