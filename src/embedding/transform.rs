//! Shared loading and application of profile-pinned row transforms.

use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;

use super::{CenteringArtifact, MultiVectorEmbedding, VectorTransformRecipe};

/// Load and verify the immutable mean selected by a transform recipe.
pub(crate) async fn load_vector_transform_mean(
    store: &ZeppelinStore,
    recipe: &VectorTransformRecipe,
    dimension: usize,
) -> Result<Option<Vec<f32>>> {
    let Some(reference) = recipe.mean() else {
        return Ok(None);
    };
    let bytes = store.get(&reference.key).await?;
    if u64::try_from(bytes.len()).ok() != Some(reference.size_bytes) {
        return Err(ZeppelinError::Serialization(
            "centering artifact size mismatch".to_string(),
        ));
    }
    CenteringArtifact::from_bytes(&bytes, reference.checksum, dimension)
        .map(|artifact| Some(artifact.values().to_vec()))
}

/// Apply one exact profile-pinned transform to every matrix row.
pub(crate) fn apply_vector_transform(
    embedding: &MultiVectorEmbedding,
    recipe: &VectorTransformRecipe,
    mean: Option<&[f32]>,
    max_vectors: usize,
) -> Result<MultiVectorEmbedding> {
    let mut values = embedding.values().to_vec();
    match recipe {
        VectorTransformRecipe::Identity => {}
        VectorTransformRecipe::SubtractMean { renormalize, .. } => {
            let mean = mean.ok_or_else(|| {
                ZeppelinError::Validation(
                    "centering transform is missing its loaded mean".to_string(),
                )
            })?;
            if mean.len() != embedding.vector_dimension() {
                return Err(ZeppelinError::DimensionMismatch {
                    expected: embedding.vector_dimension(),
                    actual: mean.len(),
                });
            }
            for row in values.chunks_exact_mut(embedding.vector_dimension()) {
                for (value, center) in row.iter_mut().zip(mean) {
                    *value -= center;
                }
                if *renormalize {
                    let norm = row
                        .iter()
                        .map(|value| f64::from(*value) * f64::from(*value))
                        .sum::<f64>()
                        .sqrt();
                    if !norm.is_finite() || norm == 0.0 {
                        return Err(ZeppelinError::Validation(
                            "centering produced a zero or non-finite row".to_string(),
                        ));
                    }
                    let inverse = norm.recip() as f32;
                    for value in row {
                        *value *= inverse;
                    }
                }
            }
        }
    }
    MultiVectorEmbedding::new(
        values,
        embedding.vector_count(),
        embedding.vector_dimension(),
        max_vectors,
    )
}
