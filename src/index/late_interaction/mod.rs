//! Pure late-interaction primitives.
//!
//! The module validates ragged multi-vector matrices, builds deterministic
//! MUVERA fixed-dimensional encodings, and provides the scalar MaxSim truth
//! scorer. It owns no storage, manifest, configuration, or query orchestration.

mod fde;
mod matrix;
mod maxsim;

pub use fde::{FdeAlgorithmVersion, FdeParams, FdeTransform, FinalProjection, InnerProjection};
pub use matrix::MultiVectorMatrixRef;
pub use maxsim::max_sim;

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
}
