//! Dataset generation and loading for benchmarks.

use rand::Rng;
use std::collections::HashMap;

use crate::client::Vector;

/// Generate random vectors with optional attributes.
pub fn random_vectors(
    n: usize,
    dim: usize,
    with_attributes: bool,
) -> Vec<Vector> {
    let mut rng = rand::thread_rng();
    let categories = ["electronics", "books", "clothing", "sports", "food"];

    (0..n)
        .map(|i| {
            let values: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();

            let attributes = if with_attributes {
                let mut attrs = HashMap::new();
                attrs.insert(
                    "category".to_string(),
                    serde_json::Value::String(
                        categories[rng.gen_range(0..categories.len())].to_string(),
                    ),
                );
                attrs.insert(
                    "price".to_string(),
                    serde_json::json!(rng.gen_range(1.0..1000.0_f64)),
                );
                attrs.insert(
                    "rating".to_string(),
                    serde_json::json!(rng.gen_range(1_i64..6)),
                );
                Some(attrs)
            } else {
                None
            };

            Vector {
                id: format!("vec-{i}"),
                values,
                attributes,
            }
        })
        .collect()
}

/// Generate a single random query vector.
pub fn random_query(dim: usize) -> Vec<f32> {
    let mut rng = rand::thread_rng();
    (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

/// Generate text documents for BM25 benchmarks.
///
/// Documents include random vectors (required by dimension validation) alongside
/// text content for FTS indexing.
pub fn random_documents(n: usize, dim: usize) -> Vec<Vector> {
    let mut rng = rand::thread_rng();

    let words = [
        "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
        "vector", "search", "engine", "database", "index", "query", "fast",
        "performance", "benchmark", "storage", "cloud", "native", "distributed",
        "system", "machine", "learning", "neural", "network", "embedding",
        "cluster", "retrieval", "ranking", "relevance", "similarity", "algorithm",
        "optimization", "parallel", "concurrent", "memory", "cache", "scalable",
        "efficient", "real", "time", "streaming", "batch", "processing",
    ];

    (0..n)
        .map(|i| {
            let doc_len = rng.gen_range(20..200);
            let text: String = (0..doc_len)
                .map(|_| words[rng.gen_range(0..words.len())])
                .collect::<Vec<_>>()
                .join(" ");

            let mut attrs = HashMap::new();
            attrs.insert("content".to_string(), serde_json::Value::String(text));

            let values: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();

            Vector {
                id: format!("doc-{i}"),
                values,
                attributes: Some(attrs),
            }
        })
        .collect()
}

/// Load SIFT1M dataset from fvecs format.
///
/// File format: for each vector, [dim: u32][f32 * dim].
#[allow(dead_code)]
pub fn load_fvecs(path: &str, max_vectors: usize) -> Result<Vec<Vec<f32>>, String> {
    use std::io::Read;

    let mut file =
        std::fs::File::open(path).map_err(|e| format!("failed to open {path}: {e}"))?;

    let mut vectors = Vec::new();
    let mut dim_buf = [0u8; 4];

    while vectors.len() < max_vectors {
        if file.read_exact(&mut dim_buf).is_err() {
            break; // EOF
        }
        let dim = u32::from_le_bytes(dim_buf) as usize;
        let mut float_buf = vec![0u8; dim * 4];
        file.read_exact(&mut float_buf)
            .map_err(|e| format!("failed to read vector data: {e}"))?;

        let vector: Vec<f32> = float_buf
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        vectors.push(vector);
    }

    Ok(vectors)
}
