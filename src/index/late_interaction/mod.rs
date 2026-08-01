//! Late-interaction kernels and exact manifest-selected overlay search.
//!
//! The module validates ragged multi-vector matrices, builds deterministic
//! MUVERA fixed-dimensional encodings, provides the scalar MaxSim truth scorer,
//! and owns exhaustive exact search over one immutable root-plus-section pair.

mod attribute_artifact;
mod candidate;
mod fde;
mod flat_candidate;
mod matrix;
mod matrix_artifact;
mod maxsim;
#[cfg(test)]
mod phase10_flat_pq_bench;
#[cfg(test)]
mod phase9_flat_sq8_bench;
#[cfg(test)]
mod phase9_routing_diagnostic;
mod search;
mod segment_build;
mod segment_search;

pub use attribute_artifact::AttributeBlockRef;
pub use candidate::{
    LateCandidateBootstrapRef, LateCandidateClusterRef, LateCandidateIndexRef, LateCandidateRecipe,
    LateRoutingMetric,
};
pub use fde::{FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection};
pub use flat_candidate::{LateFlatCandidateRecipe, LateFlatCandidateRef};
pub use matrix::MultiVectorMatrixRef;
pub use matrix_artifact::{MatrixBlockLocator, MatrixBlockRef};
pub use maxsim::max_sim;
pub use search::{
    search, LateInteractionCoverage, LateInteractionProvenance, LateInteractionRankedResult,
    LateInteractionReadTrace, LateInteractionSearchOutput, LateInteractionSearchRequest,
    LateInteractionWaveTrace, ManifestRefresh,
};

pub(crate) use attribute_artifact::decode_attribute_block;
pub(crate) use candidate::{
    decode_all_candidate_rows, CandidateFdeSource, FetchedLateCandidateCluster,
};
pub(crate) use flat_candidate::{
    FlatCalibrationSource, LateFlatCandidateBuildConfig, ResidentFlatCandidateIndex,
};
pub(crate) use matrix_artifact::decode_matrix_block;
pub(crate) use segment_build::{
    build_late_interaction_segment, LateCandidateBuild, LateRowMatrixSource,
    LateSegmentBuildConfig, LateSegmentBuildRow, PrebuiltLateFtsArtifact,
};

use thiserror::Error;

/// Typed failures from pure late-interaction primitives.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LateInteractionError {
    /// A multi-vector matrix contained no rows.
    #[error("multi-vector matrix must contain at least one vector")]
    EmptyMatrix,

    /// A multi-vector matrix declared zero columns.
    #[error("multi-vector matrix dimension must be positive")]
    ZeroDimension,

    /// Matrix shape multiplication overflowed `usize`.
    #[error("multi-vector matrix shape overflows: {vector_count} x {vector_dimension}")]
    MatrixShapeOverflow {
        /// Declared row count.
        vector_count: usize,
        /// Declared column count.
        vector_dimension: usize,
    },

    /// The backing slice length disagreed with the declared shape.
    #[error(
        "multi-vector matrix length mismatch: {vector_count} x {vector_dimension} requires \
         {expected}, got {actual}"
    )]
    MatrixLengthMismatch {
        /// Declared row count.
        vector_count: usize,
        /// Declared column count.
        vector_dimension: usize,
        /// Required scalar count.
        expected: usize,
        /// Supplied scalar count.
        actual: usize,
    },

    /// The caller-provided row cap was exceeded.
    #[error("multi-vector matrix has {actual} vectors, maximum is {maximum}")]
    TooManyVectors {
        /// Supplied row count.
        actual: usize,
        /// Caller-supplied limit.
        maximum: usize,
    },

    /// A matrix scalar was NaN or infinite.
    #[error("multi-vector matrix value at scalar index {index} is not finite")]
    NonFiniteValue {
        /// Row-major scalar index.
        index: usize,
    },

    /// Two matrices or a matrix and transform used different dimensions.
    #[error("late-interaction dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch {
        /// Required dimension.
        expected: usize,
        /// Supplied dimension.
        actual: usize,
    },

    /// Document FDE construction received an all-zero row.
    #[error("document multi-vector row {row} is the zero vector")]
    ZeroDocumentVector {
        /// Zero row ordinal.
        row: usize,
    },

    /// An FDE parameter combination was invalid.
    #[error("invalid FDE parameters: {reason}")]
    InvalidFdeParams {
        /// Stable validation reason.
        reason: &'static str,
    },

    /// Persisted transform bytes were malformed or inconsistent.
    #[error("invalid FDE transform: {reason}")]
    InvalidFdeTransform {
        /// Decode or integrity diagnostic.
        reason: String,
    },

    /// The selected root did not reference late-interaction state.
    #[error("late-interaction search requires a manifest-selected late-state section")]
    MissingLateState,

    /// The selected late-state section had no active semantic profile.
    #[error("late-interaction search requires an active embedding profile")]
    MissingActiveProfile,

    /// Root coverage metadata did not describe the root-plus-section snapshot.
    #[error("semantic coverage metadata disagrees with the selected snapshot")]
    CoverageMetadataMismatch,

    /// One source version was published more than once for the active recipe.
    #[error("semantic coverage contains duplicate output for one source version")]
    DuplicateVersionCoverage,

    /// An exact matrix artifact disagreed with its overlay coverage descriptor.
    #[error("semantic overlay matrix metadata disagrees with covered versions")]
    MatrixCoverageMismatch,

    /// Checked addition overflowed while sizing selected matrix objects.
    #[error("semantic overlay payload byte count overflows u64")]
    OverlayPayloadSizeOverflow,

    /// Coverage replay exceeded a persisted integer bound.
    #[error("semantic coverage replay arithmetic overflow")]
    CoverageArithmeticOverflow,

    /// The resolved query encoder disagreed with the active profile.
    #[error("late-interaction query encoder identity or dimension mismatch")]
    EncoderIdentityMismatch,

    /// Loading selected exact matrices would exceed the configured query cap.
    #[error("semantic overlay payload is {requested_bytes} bytes, maximum is {max_bytes}")]
    OverlayPayloadBudgetExceeded {
        /// Aggregate immutable matrix bytes selected by this query.
        requested_bytes: u64,
        /// Configured maximum overlay bytes per query.
        max_bytes: u64,
    },

    /// Strong semantics could not find a fully covered snapshot in time.
    #[error(
        "semantic index lag at requested generation {requested_generation}: covered through \
         sequence {covered_sequence}, {pending_records} pending, {failed_records} failed"
    )]
    SemanticIndexLag {
        /// Root generation selected when the query began.
        requested_generation: u64,
        /// Highest contiguous mutation sequence represented by derived state.
        covered_sequence: u64,
        /// Live source versions without applicable output.
        pending_records: u64,
        /// Live source versions with deterministic enrichment failure.
        failed_records: u64,
    },
}
