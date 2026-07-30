use crate::error::Result;

use super::{LateInteractionError, MultiVectorMatrixRef};

/// Computes scalar MaxSim/Chamfer similarity.
///
/// For every query row, the scorer keeps the first document row attaining the
/// maximum dot product, then sums those maxima. Scores are never negated:
/// higher is better.
pub fn max_sim(
    query: &MultiVectorMatrixRef<'_>,
    document: &MultiVectorMatrixRef<'_>,
) -> Result<f32> {
    if query.vector_dimension() != document.vector_dimension() {
        return Err(LateInteractionError::DimensionMismatch {
            expected: query.vector_dimension(),
            actual: document.vector_dimension(),
        }
        .into());
    }

    let mut score = 0.0_f32;
    for query_index in 0..query.vector_count() {
        let query_row = query.row(query_index);
        let mut best = f32::NEG_INFINITY;
        for document_index in 0..document.vector_count() {
            let dot = query_row
                .iter()
                .zip(document.row(document_index))
                .map(|(query_value, document_value)| query_value * document_value)
                .sum::<f32>();
            if dot > best {
                best = dot;
            }
        }
        score += best;
    }
    Ok(score)
}

#[cfg(test)]
mod tests {
    use crate::error::ZeppelinError;

    use super::*;

    const MAX_VECTORS: usize = 16;

    #[test]
    fn maxsim_matches_hand_computed_truth_and_negative_scores() {
        let query_values = [1.0, 0.0, -1.0, 0.0, 1.0, 1.0];
        let document_values = [0.5, 0.0, -1.0, 1.0, 0.0, -2.0, -0.5, -0.5, -0.5];
        let query =
            MultiVectorMatrixRef::new(&query_values, 2, 3, MAX_VECTORS).expect("valid query");
        let document =
            MultiVectorMatrixRef::new(&document_values, 3, 3, MAX_VECTORS).expect("valid document");

        // max([1.5, 3.0, 0.0]) + max([-1.0, -2.0, -1.0]) = 2.0
        assert_eq!(max_sim(&query, &document).expect("score"), 2.0);

        let negative_query_values = [1.0, 0.0];
        let negative_document_values = [-3.0, 0.0, -2.0, 0.0];
        let negative_query = MultiVectorMatrixRef::new(&negative_query_values, 1, 2, MAX_VECTORS)
            .expect("valid query");
        let negative_document =
            MultiVectorMatrixRef::new(&negative_document_values, 2, 2, MAX_VECTORS)
                .expect("valid document");
        assert_eq!(
            max_sim(&negative_query, &negative_document).expect("negative score"),
            -2.0
        );
    }

    #[test]
    fn maxsim_and_matrix_validation_reject_invalid_inputs() {
        assert!(matches!(
            MultiVectorMatrixRef::new(&[], 0, 3, MAX_VECTORS),
            Err(ZeppelinError::LateInteraction(
                LateInteractionError::EmptyMatrix
            ))
        ));
        assert!(matches!(
            MultiVectorMatrixRef::new(&[1.0], 1, 0, MAX_VECTORS),
            Err(ZeppelinError::LateInteraction(
                LateInteractionError::ZeroDimension
            ))
        ));
        assert!(matches!(
            MultiVectorMatrixRef::new(&[f32::NAN], 1, 1, MAX_VECTORS),
            Err(ZeppelinError::LateInteraction(
                LateInteractionError::NonFiniteValue { .. }
            ))
        ));
        assert!(matches!(
            MultiVectorMatrixRef::new(&[1.0, 2.0], 2, 1, 1),
            Err(ZeppelinError::LateInteraction(
                LateInteractionError::TooManyVectors { .. }
            ))
        ));

        let query_values = [1.0, 2.0];
        let document_values = [1.0, 2.0, 3.0];
        let query =
            MultiVectorMatrixRef::new(&query_values, 1, 2, MAX_VECTORS).expect("valid query");
        let document =
            MultiVectorMatrixRef::new(&document_values, 1, 3, MAX_VECTORS).expect("valid document");
        assert!(matches!(
            max_sim(&query, &document),
            Err(ZeppelinError::LateInteraction(
                LateInteractionError::DimensionMismatch { .. }
            ))
        ));
    }
}
