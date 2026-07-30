use crate::error::Result;

use super::LateInteractionError;

/// Borrowed, checked row-major multi-vector matrix.
#[derive(Clone, Copy, Debug)]
pub struct MultiVectorMatrixRef<'a> {
    values: &'a [f32],
    vector_count: usize,
    vector_dimension: usize,
}

impl<'a> MultiVectorMatrixRef<'a> {
    /// Validates a row-major matrix before it reaches an encoder or scorer.
    ///
    /// The caller-provided `max_vectors` limit is checked before any downstream
    /// allocation. Values must be finite and the declared shape must exactly
    /// cover `values`.
    #[must_use = "matrix validation errors must be handled"]
    pub fn new(
        values: &'a [f32],
        vector_count: usize,
        vector_dimension: usize,
        max_vectors: usize,
    ) -> Result<Self> {
        if vector_count == 0 {
            return Err(LateInteractionError::EmptyMatrix.into());
        }
        if vector_dimension == 0 {
            return Err(LateInteractionError::ZeroDimension.into());
        }
        if vector_count > max_vectors {
            return Err(LateInteractionError::TooManyVectors {
                actual: vector_count,
                maximum: max_vectors,
            }
            .into());
        }
        let expected = vector_count.checked_mul(vector_dimension).ok_or(
            LateInteractionError::MatrixShapeOverflow {
                vector_count,
                vector_dimension,
            },
        )?;
        if expected != values.len() {
            return Err(LateInteractionError::MatrixLengthMismatch {
                vector_count,
                vector_dimension,
                expected,
                actual: values.len(),
            }
            .into());
        }
        if let Some(index) = values.iter().position(|value| !value.is_finite()) {
            return Err(LateInteractionError::NonFiniteValue { index }.into());
        }
        Ok(Self {
            values,
            vector_count,
            vector_dimension,
        })
    }

    /// Returns row `index`.
    ///
    /// # Panics
    ///
    /// Panics when `index >= vector_count`.
    #[must_use]
    pub fn row(&self, index: usize) -> &'a [f32] {
        let start = index * self.vector_dimension;
        &self.values[start..start + self.vector_dimension]
    }

    /// Returns the number of matrix rows.
    #[must_use]
    pub const fn vector_count(&self) -> usize {
        self.vector_count
    }

    /// Returns the number of scalars in each row.
    #[must_use]
    pub const fn vector_dimension(&self) -> usize {
        self.vector_dimension
    }

    /// Returns the complete row-major scalar slice.
    #[must_use]
    pub const fn values(&self) -> &'a [f32] {
        self.values
    }
}
