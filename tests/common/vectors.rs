use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use zeppelin::types::{AttributeValue, VectorEntry};

/// Generate `n` random vectors of dimension `dims` with uniform f32 values in [-1, 1].
pub fn random_vectors(n: usize, dims: usize) -> Vec<VectorEntry> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..n)
        .map(|i| VectorEntry {
            id: format!("vec_{i}"),
            values: (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect(),
            attributes: None,
        })
        .collect()
}

/// Generate vectors clustered around `n_clusters` known centroids.
/// Returns (vectors, centroids) — centroids are the ground truth for recall testing.
///
/// Each centroid is a random unit vector. Vectors in each cluster are the centroid
/// plus small gaussian noise (stddev = `noise`).
pub fn clustered_vectors(
    n_clusters: usize,
    n_per_cluster: usize,
    dims: usize,
    noise: f32,
) -> (Vec<VectorEntry>, Vec<Vec<f32>>) {
    let mut rng = StdRng::seed_from_u64(123);

    // Generate random centroids
    let centroids: Vec<Vec<f32>> = (0..n_clusters)
        .map(|_| {
            let v: Vec<f32> = (0..dims).map(|_| rng.gen_range(-1.0..1.0)).collect();
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            v.iter().map(|x| x / norm).collect()
        })
        .collect();

    let mut vectors = Vec::with_capacity(n_clusters * n_per_cluster);

    for (ci, centroid) in centroids.iter().enumerate() {
        for vi in 0..n_per_cluster {
            let values: Vec<f32> = centroid
                .iter()
                .map(|&c| c + rng.gen_range(-noise..noise))
                .collect();
            vectors.push(VectorEntry {
                id: format!("cluster_{ci}_vec_{vi}"),
                values,
                attributes: None,
            });
        }
    }

    (vectors, centroids)
}

/// Attach attributes to vectors using a generator function.
/// The generator receives the vector index and returns attributes.
pub fn with_attributes<F>(mut vectors: Vec<VectorEntry>, attr_gen: F) -> Vec<VectorEntry>
where
    F: Fn(usize) -> HashMap<String, AttributeValue>,
{
    for (i, vec) in vectors.iter_mut().enumerate() {
        vec.attributes = Some(attr_gen(i));
    }
    vectors
}

/// Simple attribute generator: assigns "category" (a/b/c round-robin) and "score" (0..n).
pub fn simple_attributes(index: usize) -> HashMap<String, AttributeValue> {
    let mut attrs = HashMap::new();
    let categories = ["a", "b", "c"];
    attrs.insert(
        "category".to_string(),
        AttributeValue::String(categories[index % 3].to_string()),
    );
    attrs.insert("score".to_string(), AttributeValue::Integer(index as i64));
    attrs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_vectors() {
        let vecs = random_vectors(10, 128);
        assert_eq!(vecs.len(), 10);
        assert_eq!(vecs[0].values.len(), 128);
        assert_eq!(vecs[0].id, "vec_0");
        assert_eq!(vecs[9].id, "vec_9");
    }

    #[test]
    fn test_clustered_vectors() {
        let (vecs, centroids) = clustered_vectors(3, 5, 64, 0.05);
        assert_eq!(vecs.len(), 15);
        assert_eq!(centroids.len(), 3);
        assert_eq!(vecs[0].values.len(), 64);
        assert!(vecs[0].id.starts_with("cluster_0"));
    }

    #[test]
    fn test_with_attributes() {
        let vecs = random_vectors(6, 4);
        let vecs = with_attributes(vecs, simple_attributes);
        assert!(vecs[0].attributes.is_some());
        let attrs = vecs[0].attributes.as_ref().unwrap();
        assert_eq!(
            attrs.get("category"),
            Some(&AttributeValue::String("a".to_string()))
        );
        assert_eq!(attrs.get("score"), Some(&AttributeValue::Integer(0)));
    }
}
