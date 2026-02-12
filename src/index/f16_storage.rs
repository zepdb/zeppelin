//! Half-precision (f16) vector storage utilities.
//!
//! Provides conversion between f32 and f16 vectors for 50% storage savings.
//! Distance computation is always done in f32 — vectors are promoted on read.
//!
//! ## Binary format
//!
//! f16 cluster blob:
//! ```text
//! [num_vectors: u32][dimension: u32]
//! For each vector: [id_len: u32][id_bytes...][f16 * dim]
//! ```
//! Each f16 value is 2 bytes (IEEE 754 half-precision), stored little-endian.

use bytes::Bytes;
use half::f16;

use crate::error::{Result, ZeppelinError};

/// Convert an f32 vector to f16.
#[inline]
pub fn f32_to_f16(values: &[f32]) -> Vec<f16> {
    values.iter().map(|&v| f16::from_f32(v)).collect()
}

/// Convert an f16 vector back to f32.
#[inline]
pub fn f16_to_f32(values: &[f16]) -> Vec<f32> {
    values.iter().map(|v| v.to_f32()).collect()
}

/// Serialize a cluster of vectors in f16 format.
///
/// Layout: `[num_vectors: u32][dimension: u32]`
/// then for each vector: `[id_len: u32][id_bytes...][f16_le * dim]`
pub fn serialize_f16_cluster(ids: &[String], vectors: &[Vec<f32>], dim: usize) -> Result<Bytes> {
    let n = ids.len() as u32;
    let dimension = dim as u32;

    let mut buf = Vec::new();
    buf.extend_from_slice(&n.to_le_bytes());
    buf.extend_from_slice(&dimension.to_le_bytes());

    for (id, vec) in ids.iter().zip(vectors.iter()) {
        let id_bytes = id.as_bytes();
        buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(id_bytes);
        for &val in vec {
            let half_val = f16::from_f32(val);
            buf.extend_from_slice(&half_val.to_le_bytes());
        }
    }

    Ok(Bytes::from(buf))
}

/// Deserialized f16 cluster data with vectors promoted to f32.
#[derive(Debug)]
pub struct F16ClusterData {
    pub ids: Vec<String>,
    /// Vectors have been promoted to f32 for distance computation.
    pub vectors: Vec<Vec<f32>>,
}

/// Deserialize an f16 cluster blob. Vectors are promoted to f32.
pub fn deserialize_f16_cluster(data: &[u8]) -> Result<F16ClusterData> {
    if data.len() < 8 {
        return Err(ZeppelinError::Index(
            "f16 cluster blob too small for header".into(),
        ));
    }

    let n = u32::from_le_bytes(
        data[0..4]
            .try_into()
            .map_err(|_| ZeppelinError::Index("f16 cluster header parse error".into()))?,
    ) as usize;
    let dim = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("f16 cluster header parse error".into()))?,
    ) as usize;

    let mut ids = Vec::with_capacity(n);
    let mut vectors = Vec::with_capacity(n);
    let mut offset = 8;

    for _ in 0..n {
        if offset + 4 > data.len() {
            return Err(ZeppelinError::Index(
                "f16 cluster blob truncated at id_len".into(),
            ));
        }
        let id_len = u32::from_le_bytes(
            data[offset..offset + 4]
                .try_into()
                .map_err(|_| ZeppelinError::Index("f16 cluster id_len parse error".into()))?,
        ) as usize;
        offset += 4;

        if offset + id_len > data.len() {
            return Err(ZeppelinError::Index(
                "f16 cluster blob truncated at id".into(),
            ));
        }
        let id = String::from_utf8_lossy(&data[offset..offset + id_len]).into_owned();
        offset += id_len;

        let half_bytes = dim * 2; // 2 bytes per f16
        if offset + half_bytes > data.len() {
            return Err(ZeppelinError::Index(
                "f16 cluster blob truncated at vector data".into(),
            ));
        }

        let mut vec = Vec::with_capacity(dim);
        for _ in 0..dim {
            let half_val = f16::from_le_bytes(
                data[offset..offset + 2]
                    .try_into()
                    .map_err(|_| ZeppelinError::Index("f16 value parse error".into()))?,
            );
            vec.push(half_val.to_f32());
            offset += 2;
        }

        ids.push(id);
        vectors.push(vec);
    }

    Ok(F16ClusterData { ids, vectors })
}

/// S3 key for f16 cluster data.
pub fn f16_cluster_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/f16_cluster_{cluster_idx}.bin")
}

/// Compute the storage savings for f16 vs f32.
///
/// Returns the ratio of f16 size to f32 size (should be ~0.5).
pub fn compression_ratio(dim: usize, num_vectors: usize) -> f64 {
    let f32_bytes = num_vectors * dim * 4;
    let f16_bytes = num_vectors * dim * 2;
    f16_bytes as f64 / f32_bytes as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_f32_to_f16_roundtrip() {
        let values = vec![1.0f32, 2.5, -3.0, 0.0, 100.0];
        let f16_vals = f32_to_f16(&values);
        let back = f16_to_f32(&f16_vals);

        for (orig, decoded) in values.iter().zip(back.iter()) {
            // f16 has ~3 decimal digits of precision.
            let error = (orig - decoded).abs();
            let tolerance = orig.abs() * 0.01 + 0.001; // 1% relative + small absolute
            assert!(
                error < tolerance,
                "f16 roundtrip error too large: orig={orig}, decoded={decoded}, error={error}"
            );
        }
    }

    #[test]
    fn test_f16_cluster_serde_roundtrip() {
        let ids = vec!["v1".to_string(), "v2".to_string()];
        let vectors = vec![vec![1.0f32, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        let dim = 3;

        let data = serialize_f16_cluster(&ids, &vectors, dim).unwrap();
        let decoded = deserialize_f16_cluster(&data).unwrap();

        assert_eq!(decoded.ids, ids);
        assert_eq!(decoded.vectors.len(), 2);
        // Check approximate equality (f16 precision loss).
        for (orig, decoded) in vectors.iter().zip(decoded.vectors.iter()) {
            for (o, d) in orig.iter().zip(decoded.iter()) {
                assert!((o - d).abs() < 0.01, "orig={o}, decoded={d}");
            }
        }
    }

    #[test]
    fn test_f16_cluster_from_bytes_too_small() {
        assert!(deserialize_f16_cluster(&[0u8; 4]).is_err());
    }

    #[test]
    fn test_f16_cluster_truncated_vector() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // n = 1
        buf.extend_from_slice(&4u32.to_le_bytes()); // dim = 4
        let id = b"v1";
        buf.extend_from_slice(&(id.len() as u32).to_le_bytes());
        buf.extend_from_slice(id);
        // Only 2 f16 values (4 bytes) instead of 4 (8 bytes).
        buf.extend_from_slice(&f16::from_f32(1.0).to_le_bytes());
        buf.extend_from_slice(&f16::from_f32(2.0).to_le_bytes());

        assert!(deserialize_f16_cluster(&buf).is_err());
    }

    #[test]
    fn test_compression_ratio() {
        let ratio = compression_ratio(128, 1000);
        assert!((ratio - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_f16_precision_high_dim() {
        // Test with a realistic embedding dimension.
        let dim = 384;
        let values: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.001) - 0.192).collect();
        let f16_vals = f32_to_f16(&values);
        let back = f16_to_f32(&f16_vals);

        let mut max_error = 0.0f32;
        for (o, d) in values.iter().zip(back.iter()) {
            let error = (o - d).abs();
            if error > max_error {
                max_error = error;
            }
        }
        // f16 has ~3 decimal digits of precision for numbers in [-1, 1].
        assert!(max_error < 0.001, "max f16 roundtrip error: {max_error}");
    }
}
