//! Scalar Quantization (SQ8): maps each f32 dimension to u8.
//!
//! For each dimension, we store `min` and `max` values observed during
//! calibration.  Encoding maps `[min, max]` to `[0, 255]`.  Decoding
//! maps back to approximate f32 values.
//!
//! **Compression**: 4x (f32 → u8 per component).
//!
//! **Accuracy**: Typically <1% recall loss at 256-dim+ with reranking.
//!
//! ## Binary format
//!
//! Calibration blob:
//! ```text
//! [dimension: u32]
//! [min_0: f32][max_0: f32] ... [min_{dim-1}: f32][max_{dim-1}: f32]
//! ```
//!
//! Quantized cluster blob:
//! ```text
//! [num_vectors: u32][dimension: u32]
//! For each vector: [id_len: u32][id_bytes...][u8 * dim]
//! ```

use bytes::Bytes;

use crate::error::{Result, ZeppelinError};

/// Per-dimension calibration parameters for scalar quantization.
#[derive(Debug, Clone)]
pub struct SqCalibration {
    /// Dimensionality of the vectors.
    pub dim: usize,
    /// Minimum value per dimension.
    pub mins: Vec<f32>,
    /// Maximum value per dimension.
    pub maxs: Vec<f32>,
    /// Precomputed `(max - min) / 255.0` per dimension (for decoding).
    scales: Vec<f32>,
    /// Precomputed `255.0 / (max - min)` per dimension (for encoding).
    inv_scales: Vec<f32>,
}

impl SqCalibration {
    /// Calibrate from a set of vectors by computing per-dimension min/max.
    pub fn calibrate(vectors: &[&[f32]], dim: usize) -> Self {
        let mut mins = vec![f32::MAX; dim];
        let mut maxs = vec![f32::MIN; dim];

        for vec in vectors {
            for (d, &val) in vec.iter().enumerate() {
                if val < mins[d] {
                    mins[d] = val;
                }
                if val > maxs[d] {
                    maxs[d] = val;
                }
            }
        }

        // Handle degenerate case where min == max (constant dimension).
        let mut scales = Vec::with_capacity(dim);
        let mut inv_scales = Vec::with_capacity(dim);
        for d in 0..dim {
            let range = maxs[d] - mins[d];
            if range < f32::EPSILON {
                scales.push(0.0);
                inv_scales.push(0.0);
            } else {
                scales.push(range / 255.0);
                inv_scales.push(255.0 / range);
            }
        }

        Self {
            dim,
            mins,
            maxs,
            scales,
            inv_scales,
        }
    }

    /// Encode a single f32 vector to u8 codes.
    #[inline]
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        debug_assert_eq!(vector.len(), self.dim);
        vector
            .iter()
            .enumerate()
            .map(|(d, &v)| {
                let val = (v - self.mins[d]) * self.inv_scales[d];
                val.clamp(0.0, 255.0) as u8
            })
            .collect()
    }

    /// Decode u8 codes back to approximate f32 vector.
    #[inline]
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        debug_assert_eq!(codes.len(), self.dim);
        codes
            .iter()
            .enumerate()
            .map(|(d, &c)| self.mins[d] + c as f32 * self.scales[d])
            .collect()
    }

    /// Encode a batch of vectors.
    pub fn encode_batch(&self, vectors: &[&[f32]]) -> Vec<Vec<u8>> {
        vectors.iter().map(|v| self.encode(v)).collect()
    }

    /// Compute approximate squared L2 distance between a query (f32) and
    /// a quantized vector (u8) without full decoding.
    ///
    /// Uses a precomputed lookup table for each dimension to avoid
    /// per-element decode overhead.
    #[inline]
    pub fn asymmetric_l2_squared(&self, query: &[f32], codes: &[u8]) -> f32 {
        debug_assert_eq!(query.len(), self.dim);
        debug_assert_eq!(codes.len(), self.dim);
        query
            .iter()
            .zip(codes.iter())
            .enumerate()
            .map(|(d, (&q, &c))| {
                let reconstructed = self.mins[d] + c as f32 * self.scales[d];
                let diff = q - reconstructed;
                diff * diff
            })
            .sum()
    }

    /// Compute approximate dot product distance between a query (f32) and
    /// a quantized vector (u8).
    ///
    /// Returns negated dot product (lower = more similar).
    #[inline]
    pub fn asymmetric_dot_product(&self, query: &[f32], codes: &[u8]) -> f32 {
        debug_assert_eq!(query.len(), self.dim);
        debug_assert_eq!(codes.len(), self.dim);
        let dot: f32 = query
            .iter()
            .zip(codes.iter())
            .enumerate()
            .map(|(d, (&q, &c))| {
                let reconstructed = self.mins[d] + c as f32 * self.scales[d];
                q * reconstructed
            })
            .sum();
        -dot
    }

    /// Compute approximate cosine distance between a query (f32) and
    /// a quantized vector (u8).
    #[inline]
    pub fn asymmetric_cosine(&self, query: &[f32], codes: &[u8]) -> f32 {
        debug_assert_eq!(query.len(), self.dim);
        debug_assert_eq!(codes.len(), self.dim);
        let (dot, norm_q, norm_c) = query.iter().zip(codes.iter()).enumerate().fold(
            (0.0f32, 0.0f32, 0.0f32),
            |(dot, nq, nc), (d, (&q, &c))| {
                let reconstructed = self.mins[d] + c as f32 * self.scales[d];
                (
                    dot + q * reconstructed,
                    nq + q * q,
                    nc + reconstructed * reconstructed,
                )
            },
        );
        let denom = (norm_q * norm_c).sqrt();
        if denom < f32::EPSILON {
            return 1.0;
        }
        1.0 - (dot / denom).clamp(-1.0, 1.0)
    }

    /// Compute asymmetric distance using the specified metric.
    #[inline]
    pub fn asymmetric_distance(
        &self,
        query: &[f32],
        codes: &[u8],
        metric: crate::types::DistanceMetric,
    ) -> f32 {
        match metric {
            crate::types::DistanceMetric::Euclidean => self.asymmetric_l2_squared(query, codes),
            crate::types::DistanceMetric::DotProduct => self.asymmetric_dot_product(query, codes),
            crate::types::DistanceMetric::Cosine => self.asymmetric_cosine(query, codes),
        }
    }

    /// Serialize calibration parameters to binary format.
    pub fn to_bytes(&self) -> Bytes {
        let total = 4 + self.dim * 8; // u32 + dim * (f32 min + f32 max)
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&(self.dim as u32).to_le_bytes());
        for d in 0..self.dim {
            buf.extend_from_slice(&self.mins[d].to_le_bytes());
            buf.extend_from_slice(&self.maxs[d].to_le_bytes());
        }
        debug_assert_eq!(buf.len(), total);
        Bytes::from(buf)
    }

    /// Deserialize calibration parameters from binary format.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(ZeppelinError::Index("SQ calibration blob too small".into()));
        }
        let dim = u32::from_le_bytes(
            data[0..4]
                .try_into()
                .map_err(|_| ZeppelinError::Index("SQ calibration parse error".into()))?,
        ) as usize;

        let expected = 4 + dim * 8;
        if data.len() < expected {
            return Err(ZeppelinError::Index(format!(
                "SQ calibration blob size mismatch: expected {expected}, got {}",
                data.len()
            )));
        }

        let mut mins = Vec::with_capacity(dim);
        let mut maxs = Vec::with_capacity(dim);
        let mut offset = 4;
        for _ in 0..dim {
            let min_val = f32::from_le_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .map_err(|_| ZeppelinError::Index("SQ min parse error".into()))?,
            );
            offset += 4;
            let max_val = f32::from_le_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .map_err(|_| ZeppelinError::Index("SQ max parse error".into()))?,
            );
            offset += 4;
            mins.push(min_val);
            maxs.push(max_val);
        }

        let mut scales = Vec::with_capacity(dim);
        let mut inv_scales = Vec::with_capacity(dim);
        for d in 0..dim {
            let range = maxs[d] - mins[d];
            if range < f32::EPSILON {
                scales.push(0.0);
                inv_scales.push(0.0);
            } else {
                scales.push(range / 255.0);
                inv_scales.push(255.0 / range);
            }
        }

        Ok(Self {
            dim,
            mins,
            maxs,
            scales,
            inv_scales,
        })
    }
}

/// Serialize quantized cluster data (SQ8 codes + IDs).
///
/// Layout: `[num_vectors: u32][dimension: u32]`
/// then for each vector: `[id_len: u32][id_bytes...][u8 * dim]`
pub fn serialize_sq_cluster(ids: &[String], codes: &[Vec<u8>], dim: usize) -> Result<Bytes> {
    let n = ids.len() as u32;
    let dimension = dim as u32;

    let mut buf = Vec::new();
    buf.extend_from_slice(&n.to_le_bytes());
    buf.extend_from_slice(&dimension.to_le_bytes());

    for (id, code) in ids.iter().zip(codes.iter()) {
        let id_bytes = id.as_bytes();
        buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(id_bytes);
        buf.extend_from_slice(code);
    }

    Ok(Bytes::from(buf))
}

/// Deserialized SQ8 cluster data.
#[derive(Debug)]
pub struct SqClusterData {
    /// Vector IDs in cluster order.
    pub ids: Vec<String>,
    /// SQ8 encoded codes for each vector.
    pub codes: Vec<Vec<u8>>,
}

/// Deserialize quantized cluster data.
pub fn deserialize_sq_cluster(data: &[u8]) -> Result<SqClusterData> {
    if data.len() < 8 {
        return Err(ZeppelinError::Index(
            "SQ cluster blob too small for header".into(),
        ));
    }

    let n = u32::from_le_bytes(
        data[0..4]
            .try_into()
            .map_err(|_| ZeppelinError::Index("SQ cluster header parse error".into()))?,
    ) as usize;
    let dim = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("SQ cluster header parse error".into()))?,
    ) as usize;

    let mut ids = Vec::with_capacity(n);
    let mut codes = Vec::with_capacity(n);
    let mut offset = 8;

    for _ in 0..n {
        if offset + 4 > data.len() {
            return Err(ZeppelinError::Index(
                "SQ cluster blob truncated at id_len".into(),
            ));
        }
        let id_len = u32::from_le_bytes(
            data[offset..offset + 4]
                .try_into()
                .map_err(|_| ZeppelinError::Index("SQ cluster id_len parse error".into()))?,
        ) as usize;
        offset += 4;

        if offset + id_len > data.len() {
            return Err(ZeppelinError::Index(
                "SQ cluster blob truncated at id".into(),
            ));
        }
        let id = String::from_utf8_lossy(&data[offset..offset + id_len]).into_owned();
        offset += id_len;

        if offset + dim > data.len() {
            return Err(ZeppelinError::Index(
                "SQ cluster blob truncated at codes".into(),
            ));
        }
        let code = data[offset..offset + dim].to_vec();
        offset += dim;

        ids.push(id);
        codes.push(code);
    }

    Ok(SqClusterData { ids, codes })
}

/// S3 key for the SQ calibration blob.
pub fn sq_calibration_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/sq_calibration.bin")
}

/// S3 key for the quantized cluster data.
pub fn sq_cluster_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/sq_cluster_{cluster_idx}.bin")
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DistanceMetric;

    fn sample_vectors() -> Vec<Vec<f32>> {
        vec![
            vec![0.0, 1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0, 7.0],
            vec![1.0, 3.0, 5.0, 7.0],
            vec![2.0, 2.0, 2.0, 2.0],
        ]
    }

    #[test]
    fn test_calibration() {
        let vecs = sample_vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let cal = SqCalibration::calibrate(&refs, 4);

        assert_eq!(cal.dim, 4);
        assert_eq!(cal.mins, vec![0.0, 1.0, 2.0, 2.0]);
        assert_eq!(cal.maxs, vec![4.0, 5.0, 6.0, 7.0]);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let vecs = sample_vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let cal = SqCalibration::calibrate(&refs, 4);

        for vec in &vecs {
            let codes = cal.encode(vec);
            assert_eq!(codes.len(), 4);
            let decoded = cal.decode(&codes);
            assert_eq!(decoded.len(), 4);
            // Should be close to original (within quantization error).
            for (orig, dec) in vec.iter().zip(decoded.iter()) {
                let error = (orig - dec).abs();
                let range = cal.maxs[0]
                    .max(cal.maxs[1])
                    .max(cal.maxs[2])
                    .max(cal.maxs[3]);
                // Max quantization error is range / 255 per dimension.
                assert!(
                    error < range / 255.0 + 0.01,
                    "quantization error too large: orig={orig}, decoded={dec}, error={error}"
                );
            }
        }
    }

    #[test]
    fn test_encode_boundary_values() {
        let vecs = sample_vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let cal = SqCalibration::calibrate(&refs, 4);

        // Min values should encode to 0.
        let min_vec = vec![0.0, 1.0, 2.0, 2.0];
        let codes = cal.encode(&min_vec);
        assert_eq!(codes, vec![0, 0, 0, 0]);

        // Max values should encode to 255.
        let max_vec = vec![4.0, 5.0, 6.0, 7.0];
        let codes = cal.encode(&max_vec);
        assert_eq!(codes, vec![255, 255, 255, 255]);
    }

    #[test]
    fn test_constant_dimension() {
        // When all values in a dimension are the same, encoding should still work.
        let vecs = [vec![1.0, 5.0], vec![1.0, 3.0], vec![1.0, 7.0]];
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let cal = SqCalibration::calibrate(&refs, 2);

        // Dim 0 is constant (1.0), should encode to 0.
        let codes = cal.encode(&[1.0, 5.0]);
        assert_eq!(codes[0], 0);
        let decoded = cal.decode(&codes);
        assert!((decoded[0] - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn test_asymmetric_distance_ordering() {
        let vecs = sample_vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let cal = SqCalibration::calibrate(&refs, 4);

        let query = vec![0.0, 1.0, 2.0, 3.0]; // Same as vecs[0].
        let codes: Vec<Vec<u8>> = vecs.iter().map(|v| cal.encode(v)).collect();

        // Distance to self should be smallest.
        let dist_self = cal.asymmetric_l2_squared(&query, &codes[0]);
        let dist_far = cal.asymmetric_l2_squared(&query, &codes[1]);
        assert!(dist_self < dist_far, "self distance should be smallest");
    }

    #[test]
    fn test_asymmetric_metrics() {
        let vecs = sample_vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let cal = SqCalibration::calibrate(&refs, 4);

        let query = vec![1.0, 2.0, 3.0, 4.0];
        let codes = cal.encode(&vecs[0]);

        // All metrics should produce finite values.
        let d_l2 = cal.asymmetric_distance(&query, &codes, DistanceMetric::Euclidean);
        let d_dot = cal.asymmetric_distance(&query, &codes, DistanceMetric::DotProduct);
        let d_cos = cal.asymmetric_distance(&query, &codes, DistanceMetric::Cosine);
        assert!(d_l2.is_finite());
        assert!(d_dot.is_finite());
        assert!(d_cos.is_finite());
    }

    #[test]
    fn test_calibration_serde_roundtrip() {
        let vecs = sample_vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let cal = SqCalibration::calibrate(&refs, 4);

        let bytes = cal.to_bytes();
        let decoded = SqCalibration::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.dim, cal.dim);
        assert_eq!(decoded.mins, cal.mins);
        assert_eq!(decoded.maxs, cal.maxs);
    }

    #[test]
    fn test_calibration_from_bytes_too_small() {
        assert!(SqCalibration::from_bytes(&[0u8; 2]).is_err());
    }

    #[test]
    fn test_calibration_from_bytes_truncated() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&4u32.to_le_bytes()); // dim = 4
                                                    // Only provide 2 min/max pairs instead of 4.
        for _ in 0..2 {
            buf.extend_from_slice(&0.0f32.to_le_bytes());
            buf.extend_from_slice(&1.0f32.to_le_bytes());
        }
        assert!(SqCalibration::from_bytes(&buf).is_err());
    }

    #[test]
    fn test_sq_cluster_serde_roundtrip() {
        let ids = vec!["v1".to_string(), "v2".to_string()];
        let codes = vec![vec![0u8, 128, 255, 64], vec![10, 20, 30, 40]];
        let data = serialize_sq_cluster(&ids, &codes, 4).unwrap();
        let decoded = deserialize_sq_cluster(&data).unwrap();
        assert_eq!(decoded.ids, ids);
        assert_eq!(decoded.codes, codes);
    }

    #[test]
    fn test_sq_cluster_from_bytes_too_small() {
        assert!(deserialize_sq_cluster(&[0u8; 4]).is_err());
    }

    #[test]
    fn test_encode_batch() {
        let vecs = sample_vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let cal = SqCalibration::calibrate(&refs, 4);

        let batch = cal.encode_batch(&refs);
        assert_eq!(batch.len(), 4);
        for codes in &batch {
            assert_eq!(codes.len(), 4);
        }
    }
}
