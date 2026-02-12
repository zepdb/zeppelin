//! Product Quantization (PQ): divides vectors into M subvectors and
//! quantizes each independently using a trained codebook.
//!
//! For a D-dimensional vector with M=8 subquantizers and K=256 centroids:
//! - Each subvector has D/M dimensions
//! - Each subvector is encoded as 1 byte (index into codebook of 256 entries)
//! - Total: M bytes per vector (vs D*4 bytes for f32)
//! - Compression ratio: D*4 / M (e.g., 128*4/8 = 64x for 128-dim)
//!
//! ## Search
//!
//! Asymmetric Distance Computation (ADC):
//! 1. For each subquantizer m, precompute distance from query subvector
//!    to all K centroids → lookup table of M × K entries.
//! 2. For each encoded vector, sum M table lookups → approximate distance.
//!    This makes per-vector distance computation O(M) regardless of D.
//!
//! ## Binary format
//!
//! Codebook blob:
//! ```text
//! [M: u32][K: u32][sub_dim: u32]
//! For m in 0..M:
//!   For k in 0..K:
//!     [f32 * sub_dim]
//! ```
//!
//! PQ-encoded cluster blob:
//! ```text
//! [num_vectors: u32][M: u32]
//! For each vector: [id_len: u32][id_bytes...][u8 * M]
//! ```

use bytes::Bytes;

use crate::error::{Result, ZeppelinError};
use crate::types::DistanceMetric;

/// Number of centroids per subquantizer (always 256 for 1-byte codes).
const PQ_K: usize = 256;

/// Trained PQ codebook for encoding and distance computation.
#[derive(Debug, Clone)]
pub struct PqCodebook {
    /// Number of subquantizers.
    pub m: usize,
    /// Dimension of each subvector (= dim / m).
    pub sub_dim: usize,
    /// Full vector dimension.
    pub dim: usize,
    /// Codebook: `codebook[m][k]` is a `Vec<f32>` of length `sub_dim`.
    /// Stored as flat array: `codebook[m * PQ_K + k][..]`.
    centroids: Vec<Vec<f32>>,
}

impl PqCodebook {
    /// Train PQ codebooks from a set of training vectors.
    ///
    /// # Arguments
    /// * `vectors` — Training data, each of length `dim`.
    /// * `dim` — Vector dimensionality.
    /// * `m` — Number of subquantizers. Must divide `dim` evenly.
    /// * `kmeans_iters` — Max k-means iterations per subquantizer.
    pub fn train(vectors: &[&[f32]], dim: usize, m: usize, kmeans_iters: usize) -> Result<Self> {
        if dim == 0 || m == 0 {
            return Err(ZeppelinError::Index("PQ: dim and m must be > 0".into()));
        }
        if dim % m != 0 {
            return Err(ZeppelinError::Index(format!(
                "PQ: dim ({dim}) must be divisible by m ({m})"
            )));
        }
        if vectors.is_empty() {
            return Err(ZeppelinError::Index(
                "PQ: cannot train on empty dataset".into(),
            ));
        }

        let sub_dim = dim / m;
        let mut centroids = Vec::with_capacity(m * PQ_K);

        for sub_idx in 0..m {
            let offset = sub_idx * sub_dim;

            // Extract subvectors for this subquantizer.
            let sub_vectors: Vec<Vec<f32>> = vectors
                .iter()
                .map(|v| v[offset..offset + sub_dim].to_vec())
                .collect();
            let sub_refs: Vec<&[f32]> = sub_vectors.iter().map(|v| v.as_slice()).collect();

            // Train k-means on the subvectors.
            let k = PQ_K.min(vectors.len());
            let sub_centroids = crate::index::ivf_flat::kmeans::train_kmeans(
                &sub_refs,
                sub_dim,
                k,
                kmeans_iters,
                1e-4,
            )?;

            // Pad to PQ_K if we have fewer than 256 training points.
            let mut padded = sub_centroids;
            while padded.len() < PQ_K {
                padded.push(padded.last().unwrap().clone());
            }

            centroids.extend(padded);
        }

        Ok(Self {
            m,
            sub_dim,
            dim,
            centroids,
        })
    }

    /// Encode a single vector to M-byte PQ code.
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        debug_assert_eq!(vector.len(), self.dim);
        let mut codes = Vec::with_capacity(self.m);

        for sub_idx in 0..self.m {
            let offset = sub_idx * self.sub_dim;
            let sub_vec = &vector[offset..offset + self.sub_dim];

            // Find nearest centroid.
            let base = sub_idx * PQ_K;
            let mut best_dist = f32::MAX;
            let mut best_k = 0u8;

            for k in 0..PQ_K {
                let centroid = &self.centroids[base + k];
                let dist = sq_l2(sub_vec, centroid);
                if dist < best_dist {
                    best_dist = dist;
                    best_k = k as u8;
                }
            }

            codes.push(best_k);
        }

        codes
    }

    /// Encode a batch of vectors.
    pub fn encode_batch(&self, vectors: &[&[f32]]) -> Vec<Vec<u8>> {
        vectors.iter().map(|v| self.encode(v)).collect()
    }

    /// Decode a PQ code back to an approximate f32 vector.
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        debug_assert_eq!(codes.len(), self.m);
        let mut vector = Vec::with_capacity(self.dim);

        for (sub_idx, &code) in codes.iter().enumerate() {
            let base = sub_idx * PQ_K;
            let centroid = &self.centroids[base + code as usize];
            vector.extend_from_slice(centroid);
        }

        vector
    }

    /// Build an ADC (Asymmetric Distance Computation) lookup table for a query.
    ///
    /// Returns a table of shape `[M][K]` stored as flat `Vec<f32>`.
    /// `table[m * K + k]` = distance from query subvector m to centroid k.
    pub fn build_adc_table(&self, query: &[f32], metric: DistanceMetric) -> Vec<f32> {
        debug_assert_eq!(query.len(), self.dim);
        let mut table = Vec::with_capacity(self.m * PQ_K);

        for sub_idx in 0..self.m {
            let q_offset = sub_idx * self.sub_dim;
            let q_sub = &query[q_offset..q_offset + self.sub_dim];
            let c_base = sub_idx * PQ_K;

            for k in 0..PQ_K {
                let centroid = &self.centroids[c_base + k];
                let dist = match metric {
                    DistanceMetric::Euclidean => sq_l2(q_sub, centroid),
                    DistanceMetric::DotProduct => -dot(q_sub, centroid),
                    DistanceMetric::Cosine => {
                        // For cosine with ADC, we use L2 on normalized subvectors
                        // as an approximation. Full cosine requires global norms.
                        sq_l2(q_sub, centroid)
                    }
                };
                table.push(dist);
            }
        }

        table
    }

    /// Compute approximate distance from a query to an encoded vector using
    /// a precomputed ADC table. O(M) per vector.
    #[inline]
    pub fn adc_distance(&self, table: &[f32], codes: &[u8]) -> f32 {
        debug_assert_eq!(codes.len(), self.m);
        let mut sum = 0.0f32;
        for (sub_idx, &code) in codes.iter().enumerate() {
            sum += table[sub_idx * PQ_K + code as usize];
        }
        sum
    }

    /// Serialize the codebook to binary format.
    pub fn to_bytes(&self) -> Bytes {
        let total = 12 + self.m * PQ_K * self.sub_dim * 4;
        let mut buf = Vec::with_capacity(total);

        buf.extend_from_slice(&(self.m as u32).to_le_bytes());
        buf.extend_from_slice(&(PQ_K as u32).to_le_bytes());
        buf.extend_from_slice(&(self.sub_dim as u32).to_le_bytes());

        for centroid in &self.centroids {
            for &val in centroid {
                buf.extend_from_slice(&val.to_le_bytes());
            }
        }

        debug_assert_eq!(buf.len(), total);
        Bytes::from(buf)
    }

    /// Deserialize a codebook from binary format.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 12 {
            return Err(ZeppelinError::Index(
                "PQ codebook blob too small for header".into(),
            ));
        }

        let m = u32::from_le_bytes(
            data[0..4]
                .try_into()
                .map_err(|_| ZeppelinError::Index("PQ header parse error".into()))?,
        ) as usize;
        let k = u32::from_le_bytes(
            data[4..8]
                .try_into()
                .map_err(|_| ZeppelinError::Index("PQ header parse error".into()))?,
        ) as usize;
        let sub_dim = u32::from_le_bytes(
            data[8..12]
                .try_into()
                .map_err(|_| ZeppelinError::Index("PQ header parse error".into()))?,
        ) as usize;

        if k != PQ_K {
            return Err(ZeppelinError::Index(format!(
                "PQ: expected K={PQ_K}, got {k}"
            )));
        }

        let expected = 12 + m * PQ_K * sub_dim * 4;
        if data.len() < expected {
            return Err(ZeppelinError::Index(format!(
                "PQ codebook blob size mismatch: expected {expected}, got {}",
                data.len()
            )));
        }

        let mut centroids = Vec::with_capacity(m * PQ_K);
        let mut offset = 12;
        for _ in 0..m * PQ_K {
            let mut c = Vec::with_capacity(sub_dim);
            for _ in 0..sub_dim {
                let val = f32::from_le_bytes(
                    data[offset..offset + 4]
                        .try_into()
                        .map_err(|_| ZeppelinError::Index("PQ centroid parse error".into()))?,
                );
                c.push(val);
                offset += 4;
            }
            centroids.push(c);
        }

        let dim = m * sub_dim;
        Ok(Self {
            m,
            sub_dim,
            dim,
            centroids,
        })
    }
}

/// Serialize PQ-encoded cluster data (PQ codes + IDs).
///
/// Layout: `[num_vectors: u32][M: u32]`
/// then for each vector: `[id_len: u32][id_bytes...][u8 * M]`
pub fn serialize_pq_cluster(ids: &[String], codes: &[Vec<u8>], m: usize) -> Result<Bytes> {
    let n = ids.len() as u32;

    let mut buf = Vec::new();
    buf.extend_from_slice(&n.to_le_bytes());
    buf.extend_from_slice(&(m as u32).to_le_bytes());

    for (id, code) in ids.iter().zip(codes.iter()) {
        let id_bytes = id.as_bytes();
        buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(id_bytes);
        buf.extend_from_slice(code);
    }

    Ok(Bytes::from(buf))
}

/// Deserialized PQ cluster data.
#[derive(Debug)]
pub struct PqClusterData {
    pub ids: Vec<String>,
    pub codes: Vec<Vec<u8>>,
}

/// Deserialize PQ-encoded cluster data.
pub fn deserialize_pq_cluster(data: &[u8]) -> Result<PqClusterData> {
    if data.len() < 8 {
        return Err(ZeppelinError::Index(
            "PQ cluster blob too small for header".into(),
        ));
    }

    let n = u32::from_le_bytes(
        data[0..4]
            .try_into()
            .map_err(|_| ZeppelinError::Index("PQ cluster header parse error".into()))?,
    ) as usize;
    let m = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("PQ cluster header parse error".into()))?,
    ) as usize;

    let mut ids = Vec::with_capacity(n);
    let mut codes = Vec::with_capacity(n);
    let mut offset = 8;

    for _ in 0..n {
        if offset + 4 > data.len() {
            return Err(ZeppelinError::Index(
                "PQ cluster blob truncated at id_len".into(),
            ));
        }
        let id_len = u32::from_le_bytes(
            data[offset..offset + 4]
                .try_into()
                .map_err(|_| ZeppelinError::Index("PQ cluster id_len parse error".into()))?,
        ) as usize;
        offset += 4;

        if offset + id_len > data.len() {
            return Err(ZeppelinError::Index(
                "PQ cluster blob truncated at id".into(),
            ));
        }
        let id = String::from_utf8_lossy(&data[offset..offset + id_len]).into_owned();
        offset += id_len;

        if offset + m > data.len() {
            return Err(ZeppelinError::Index(
                "PQ cluster blob truncated at codes".into(),
            ));
        }
        let code = data[offset..offset + m].to_vec();
        offset += m;

        ids.push(id);
        codes.push(code);
    }

    Ok(PqClusterData { ids, codes })
}

/// S3 key for the PQ codebook blob.
pub fn pq_codebook_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/pq_codebook.bin")
}

/// S3 key for PQ-encoded cluster data.
pub fn pq_cluster_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/pq_cluster_{cluster_idx}.bin")
}

// ---------------------------------------------------------------------------
// Helper math
// ---------------------------------------------------------------------------

#[inline]
fn sq_l2(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

#[inline]
fn dot(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        sum += a[i] * b[i];
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    fn training_data(n: usize, dim: usize) -> Vec<Vec<f32>> {
        // Generate deterministic training data.
        let mut vecs = Vec::with_capacity(n);
        for i in 0..n {
            let mut v = Vec::with_capacity(dim);
            for d in 0..dim {
                v.push(((i * dim + d) as f32 * 0.1) % 10.0);
            }
            vecs.push(v);
        }
        vecs
    }

    #[test]
    fn test_pq_train_encode_decode() {
        let data = training_data(100, 8);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();

        let codebook = PqCodebook::train(&refs, 8, 4, 10).unwrap();
        assert_eq!(codebook.m, 4);
        assert_eq!(codebook.sub_dim, 2);
        assert_eq!(codebook.dim, 8);

        // Encode and decode should produce reasonable approximation.
        let codes = codebook.encode(&data[0]);
        assert_eq!(codes.len(), 4);
        let decoded = codebook.decode(&codes);
        assert_eq!(decoded.len(), 8);
    }

    #[test]
    fn test_pq_adc_distance() {
        let data = training_data(100, 8);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();

        let codebook = PqCodebook::train(&refs, 8, 4, 10).unwrap();
        let codes = codebook.encode(&data[0]);

        let table = codebook.build_adc_table(&data[0], DistanceMetric::Euclidean);
        let dist_self = codebook.adc_distance(&table, &codes);

        let codes_far = codebook.encode(&data[50]);
        let dist_far = codebook.adc_distance(&table, &codes_far);

        // Distance to self should be smaller (or equal) to distance to a different vector.
        assert!(dist_self <= dist_far + 0.01);
    }

    #[test]
    fn test_pq_codebook_serde_roundtrip() {
        let data = training_data(100, 8);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();

        let codebook = PqCodebook::train(&refs, 8, 4, 10).unwrap();
        let bytes = codebook.to_bytes();
        let decoded = PqCodebook::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.m, codebook.m);
        assert_eq!(decoded.sub_dim, codebook.sub_dim);
        assert_eq!(decoded.dim, codebook.dim);
        assert_eq!(decoded.centroids.len(), codebook.centroids.len());
    }

    #[test]
    fn test_pq_cluster_serde_roundtrip() {
        let ids = vec!["a".into(), "b".into(), "c".into()];
        let codes = vec![vec![0u8, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11]];
        let data = serialize_pq_cluster(&ids, &codes, 4).unwrap();
        let decoded = deserialize_pq_cluster(&data).unwrap();
        assert_eq!(decoded.ids, ids);
        assert_eq!(decoded.codes, codes);
    }

    #[test]
    fn test_pq_dim_not_divisible() {
        let data = training_data(100, 7);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        // 7 is not divisible by 4.
        let result = PqCodebook::train(&refs, 7, 4, 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_pq_empty_dataset() {
        let refs: Vec<&[f32]> = vec![];
        let result = PqCodebook::train(&refs, 8, 4, 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_pq_codebook_from_bytes_too_small() {
        assert!(PqCodebook::from_bytes(&[0u8; 8]).is_err());
    }

    #[test]
    fn test_pq_batch_encode() {
        let data = training_data(50, 8);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();

        let codebook = PqCodebook::train(&refs, 8, 4, 10).unwrap();
        let batch = codebook.encode_batch(&refs);
        assert_eq!(batch.len(), 50);
        for codes in &batch {
            assert_eq!(codes.len(), 4);
        }
    }
}
