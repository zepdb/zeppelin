//! Resident coarse sketch for IVF-Flat segments.
//!
//! The sketch is a small immutable segment artifact built during compaction
//! from corpus vectors only. It stores packed 4-bit product-quantization codes
//! for every vector, ordered by IVF cluster. Query-time code scans this
//! resident data inside the requested `nprobe` centroid set, selects a small
//! cluster set, and the normal cluster reader performs exact rerank.

use std::collections::HashMap;

use bytes::Bytes;

use crate::error::{Result, ZeppelinError};
use crate::index::ivf_flat::kmeans::train_kmeans;
use crate::types::{AttributeValue, DistanceMetric};
use crate::wal::manifest::SketchRef;

const SKETCH_MAGIC: &[u8; 4] = b"ZSK1";
const SKETCH_VERSION: u32 = 2;
const SKETCH_MAX_SUBQUANTIZERS: usize = 96;
const SKETCH_K: usize = 16;
const SKETCH_TRAIN_SAMPLE: usize = 4096;
const SKETCH_TRAIN_ITERS: usize = 6;

/// S3 key for the resident coarse sketch.
#[must_use]
pub fn sketch_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/coarse_sketch.bin")
}

/// In-memory resident sketch loaded from the immutable segment artifact.
#[derive(Debug, Clone)]
pub(crate) struct ResidentSketch {
    dim: usize,
    subquantizers: usize,
    cluster_count: usize,
    codebook: Vec<f32>,
    cluster_offsets: Vec<(usize, usize)>,
    codes: Bytes,
    cluster_has_attrs: Vec<bool>,
    packed_code_bytes: usize,
}

impl ResidentSketch {
    /// Deserialize and validate a sketch artifact.
    pub(crate) fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 28 {
            return Err(ZeppelinError::Index(
                "coarse sketch blob too small for header".into(),
            ));
        }
        if !data.starts_with(SKETCH_MAGIC) {
            return Err(ZeppelinError::Index("coarse sketch magic mismatch".into()));
        }

        let version = read_u32(data, 4, "coarse sketch version")?;
        if version != SKETCH_VERSION {
            return Err(ZeppelinError::Index(format!(
                "unsupported coarse sketch version: {version}"
            )));
        }
        let dim = read_u32(data, 8, "coarse sketch dim")? as usize;
        let subquantizers = read_u32(data, 12, "coarse sketch subquantizers")? as usize;
        let cluster_count = read_u32(data, 16, "coarse sketch cluster_count")? as usize;
        let vector_count = read_u64(data, 20, "coarse sketch vector_count")? as usize;

        if dim == 0 {
            return Err(ZeppelinError::Index("coarse sketch dim is zero".into()));
        }
        if subquantizers == 0 || subquantizers > dim {
            return Err(ZeppelinError::Index(format!(
                "coarse sketch invalid subquantizer count: dim={dim}, subquantizers={subquantizers}"
            )));
        }
        if cluster_count == 0 {
            return Err(ZeppelinError::Index(
                "coarse sketch cluster_count is zero".into(),
            ));
        }

        let codebook_floats = dim.checked_mul(SKETCH_K).ok_or_else(|| {
            ZeppelinError::Index(format!("coarse sketch codebook overflows: dim={dim}"))
        })?;
        let codebook_bytes = codebook_floats.checked_mul(4).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "coarse sketch codebook byte size overflows: floats={codebook_floats}"
            ))
        })?;
        let attr_bitset_len = bitset_len(cluster_count);
        let counts_bytes = cluster_count.checked_mul(4).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "coarse sketch cluster counts overflows: clusters={cluster_count}"
            ))
        })?;
        let packed_code_bytes = packed_code_bytes(subquantizers);
        let all_codes_bytes = vector_count.checked_mul(packed_code_bytes).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "coarse sketch code section overflows: vectors={vector_count}, bytes_per_code={packed_code_bytes}"
            ))
        })?;

        let codebook_offset = 28usize;
        let attr_offset = codebook_offset
            .checked_add(codebook_bytes)
            .ok_or_else(|| ZeppelinError::Index("coarse sketch attr offset overflows".into()))?;
        let counts_offset = attr_offset
            .checked_add(attr_bitset_len)
            .ok_or_else(|| ZeppelinError::Index("coarse sketch counts offset overflows".into()))?;
        let codes_offset = counts_offset
            .checked_add(counts_bytes)
            .ok_or_else(|| ZeppelinError::Index("coarse sketch code offset overflows".into()))?;
        let expected = codes_offset
            .checked_add(all_codes_bytes)
            .ok_or_else(|| ZeppelinError::Index("coarse sketch total size overflows".into()))?;
        if data.len() != expected {
            return Err(ZeppelinError::Index(format!(
                "coarse sketch blob size mismatch: expected {expected}, got {}",
                data.len()
            )));
        }

        let mut codebook = Vec::with_capacity(codebook_floats);
        for i in 0..codebook_floats {
            codebook.push(read_f32(
                data,
                codebook_offset + i * 4,
                "coarse sketch codebook",
            )?);
        }

        let mut cluster_has_attrs = vec![false; cluster_count];
        for (cluster_idx, has_attrs) in cluster_has_attrs.iter_mut().enumerate() {
            *has_attrs = bit_is_set(
                &data[attr_offset..attr_offset + attr_bitset_len],
                cluster_idx,
            );
        }

        let mut cluster_offsets = Vec::with_capacity(cluster_count);
        let mut row_offset = 0usize;
        for cluster_idx in 0..cluster_count {
            let count = read_u32(
                data,
                counts_offset + cluster_idx * 4,
                "coarse sketch cluster count",
            )? as usize;
            let next = row_offset.checked_add(count).ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "coarse sketch cluster offsets overflow at cluster {cluster_idx}"
                ))
            })?;
            cluster_offsets.push((row_offset, next));
            row_offset = next;
        }
        if row_offset != vector_count {
            return Err(ZeppelinError::Index(format!(
                "coarse sketch cluster counts sum mismatch: expected {vector_count}, got {row_offset}"
            )));
        }

        Ok(Self {
            dim,
            subquantizers,
            cluster_count,
            codebook,
            cluster_offsets,
            codes: Bytes::copy_from_slice(&data[codes_offset..expected]),
            cluster_has_attrs,
            packed_code_bytes,
        })
    }

    /// Whether a cluster may contain any non-null attributes.
    #[must_use]
    pub(crate) fn cluster_has_attrs(&self, cluster_idx: usize) -> bool {
        self.cluster_has_attrs
            .get(cluster_idx)
            .copied()
            .unwrap_or(true)
    }

    /// Select clusters for exact rerank from the requested centroid-probe set.
    pub(crate) fn select_clusters(
        &self,
        query: &[f32],
        distance_metric: DistanceMetric,
        probe_clusters: &[usize],
        cluster_budget: usize,
    ) -> Result<Vec<usize>> {
        if query.len() != self.dim {
            return Err(ZeppelinError::DimensionMismatch {
                expected: self.dim,
                actual: query.len(),
            });
        }
        if cluster_budget == 0 {
            return Err(ZeppelinError::Index(
                "coarse sketch cluster budget is zero".into(),
            ));
        }
        if probe_clusters.is_empty() {
            return Err(ZeppelinError::Index(
                "coarse sketch received an empty probe set".into(),
            ));
        }
        for &cluster_idx in probe_clusters {
            if cluster_idx >= self.cluster_count {
                return Err(ZeppelinError::Index(format!(
                    "probe cluster {cluster_idx} outside sketch cluster_count {}",
                    self.cluster_count
                )));
            }
        }

        let adc_table = self.build_adc_table(query, distance_metric);
        let centroid_rank: HashMap<usize, usize> = probe_clusters
            .iter()
            .copied()
            .enumerate()
            .map(|(rank, cluster_idx)| (cluster_idx, rank))
            .collect();
        let mut ranked_clusters = Vec::new();
        for &cluster_idx in probe_clusters {
            let (start, end) = self.cluster_offsets[cluster_idx];
            if start == end {
                continue;
            }

            let mut best_score = f32::INFINITY;
            for row in start..end {
                let code_offset = row * self.packed_code_bytes;
                let codes = &self.codes[code_offset..code_offset + self.packed_code_bytes];
                let score = self.adc_score(&adc_table, codes);
                if score < best_score {
                    best_score = score;
                }
            }
            ranked_clusters.push(ClusterScore {
                cluster_idx,
                best_score,
            });
        }

        ranked_clusters.sort_by(|a, b| {
            a.best_score
                .partial_cmp(&b.best_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    centroid_rank
                        .get(&a.cluster_idx)
                        .copied()
                        .unwrap_or(usize::MAX)
                        .cmp(
                            &centroid_rank
                                .get(&b.cluster_idx)
                                .copied()
                                .unwrap_or(usize::MAX),
                        )
                })
        });

        let mut selected: Vec<usize> = ranked_clusters
            .into_iter()
            .take(cluster_budget.min(probe_clusters.len()))
            .map(|score| score.cluster_idx)
            .collect();

        for &cluster_idx in probe_clusters {
            if selected.len() >= cluster_budget || selected.len() >= probe_clusters.len() {
                break;
            }
            if !selected.contains(&cluster_idx) {
                selected.push(cluster_idx);
            }
        }
        Ok(selected)
    }

    fn build_adc_table(&self, query: &[f32], distance_metric: DistanceMetric) -> Vec<f32> {
        let mut table = vec![0.0f32; self.subquantizers * SKETCH_K];
        for subq in 0..self.subquantizers {
            let (start, end) = chunk_range(self.dim, self.subquantizers, subq);
            for code in 0..SKETCH_K {
                let centroid_offset = codebook_offset(self.dim, self.subquantizers, subq, code);
                let centroid = &self.codebook[centroid_offset..centroid_offset + (end - start)];
                let q = &query[start..end];
                table[subq * SKETCH_K + code] = match distance_metric {
                    DistanceMetric::DotProduct => -dot(q, centroid),
                    DistanceMetric::Cosine | DistanceMetric::Euclidean => sq_l2(q, centroid),
                };
            }
        }
        table
    }

    #[inline]
    fn adc_score(&self, adc_table: &[f32], packed_codes: &[u8]) -> f32 {
        let mut score = 0.0;
        for subq in 0..self.subquantizers {
            let code = unpack_nibble(packed_codes, subq) as usize;
            score += adc_table[subq * SKETCH_K + code];
        }
        score
    }
}

/// Build and serialize a resident coarse sketch from clustered corpus vectors.
#[allow(clippy::type_complexity)]
pub(crate) fn build_resident_sketch(
    namespace: &str,
    segment_id: &str,
    dim: usize,
    cluster_vecs: &[Vec<Vec<f32>>],
    cluster_attrs: &[Vec<Option<HashMap<String, AttributeValue>>>],
) -> Result<(SketchRef, Bytes, ResidentSketch)> {
    let cluster_count = cluster_vecs.len();
    if cluster_count == 0 {
        return Err(ZeppelinError::Index(
            "cannot build coarse sketch with zero clusters".into(),
        ));
    }
    if cluster_attrs.len() != cluster_count {
        return Err(ZeppelinError::Index(format!(
            "coarse sketch cluster_attrs length mismatch: expected {cluster_count}, got {}",
            cluster_attrs.len()
        )));
    }
    let vector_count: usize = cluster_vecs.iter().map(Vec::len).sum();
    if vector_count == 0 {
        return Err(ZeppelinError::Index(
            "cannot build coarse sketch from empty vector set".into(),
        ));
    }

    let subquantizers = SKETCH_MAX_SUBQUANTIZERS.min(dim).max(1);
    let packed_code_bytes = packed_code_bytes(subquantizers);
    let all_vectors: Vec<&[f32]> = cluster_vecs
        .iter()
        .flat_map(|cluster| cluster.iter().map(Vec::as_slice))
        .collect();
    debug_assert_eq!(all_vectors.len(), vector_count);
    let sample_indices = sample_indices(vector_count, SKETCH_TRAIN_SAMPLE.min(vector_count));

    let mut codebook = Vec::with_capacity(dim * SKETCH_K);
    for subq in 0..subquantizers {
        let (start, end) = chunk_range(dim, subquantizers, subq);
        let sub_dim = end - start;
        let samples: Vec<Vec<f32>> = sample_indices
            .iter()
            .map(|&idx| all_vectors[idx][start..end].to_vec())
            .collect();
        let sample_refs: Vec<&[f32]> = samples.iter().map(Vec::as_slice).collect();
        let k = SKETCH_K.min(sample_refs.len());
        let mut centroids = train_kmeans(&sample_refs, sub_dim, k, SKETCH_TRAIN_ITERS, 1e-4)?;
        while centroids.len() < SKETCH_K {
            let fill = centroids
                .last()
                .cloned()
                .ok_or_else(|| ZeppelinError::Index("empty sketch PQ centroids".into()))?;
            centroids.push(fill);
        }
        for centroid in centroids {
            codebook.extend_from_slice(&centroid);
        }
    }

    let mut attr_bits = vec![0u8; bitset_len(cluster_count)];
    for (cluster_idx, attrs) in cluster_attrs.iter().enumerate() {
        if attrs.iter().any(Option::is_some) {
            set_bit(&mut attr_bits, cluster_idx);
        }
    }

    let mut codes = Vec::with_capacity(vector_count * packed_code_bytes);
    for cluster in cluster_vecs {
        for vector in cluster {
            if vector.len() != dim {
                return Err(ZeppelinError::DimensionMismatch {
                    expected: dim,
                    actual: vector.len(),
                });
            }
            let mut packed = vec![0u8; packed_code_bytes];
            for subq in 0..subquantizers {
                let (start, end) = chunk_range(dim, subquantizers, subq);
                let code =
                    encode_subvector(&codebook, dim, subquantizers, subq, &vector[start..end]);
                pack_nibble(&mut packed, subq, code);
            }
            codes.extend_from_slice(&packed);
        }
    }

    let attr_bitset_len = attr_bits.len();
    let counts_bytes = cluster_count * 4;
    let total = 28 + codebook.len() * 4 + attr_bitset_len + counts_bytes + codes.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(SKETCH_MAGIC);
    buf.extend_from_slice(&SKETCH_VERSION.to_le_bytes());
    buf.extend_from_slice(&(dim as u32).to_le_bytes());
    buf.extend_from_slice(&(subquantizers as u32).to_le_bytes());
    buf.extend_from_slice(&(cluster_count as u32).to_le_bytes());
    buf.extend_from_slice(&(vector_count as u64).to_le_bytes());
    for value in &codebook {
        buf.extend_from_slice(&value.to_le_bytes());
    }
    buf.extend_from_slice(&attr_bits);
    for cluster in cluster_vecs {
        let count = u32::try_from(cluster.len()).map_err(|_| {
            ZeppelinError::Index(format!(
                "coarse sketch cluster has too many vectors: {}",
                cluster.len()
            ))
        })?;
        buf.extend_from_slice(&count.to_le_bytes());
    }
    buf.extend_from_slice(&codes);
    debug_assert_eq!(buf.len(), total);

    let key = sketch_key(namespace, segment_id);
    let bytes = Bytes::from(buf);
    let sketch_ref = SketchRef {
        key,
        version: SKETCH_VERSION,
        code_dims: subquantizers,
        bytes_per_vector: packed_code_bytes,
        size_bytes: bytes.len() as u64,
    };
    let resident = ResidentSketch::from_bytes(&bytes)?;
    Ok((sketch_ref, bytes, resident))
}

#[derive(Clone, Copy)]
struct ClusterScore {
    cluster_idx: usize,
    best_score: f32,
}

fn sample_indices(vector_count: usize, sample_count: usize) -> Vec<usize> {
    if sample_count >= vector_count {
        return (0..vector_count).collect();
    }
    (0..sample_count)
        .map(|i| i * vector_count / sample_count)
        .collect()
}

#[inline]
fn encode_subvector(
    codebook: &[f32],
    dim: usize,
    subquantizers: usize,
    subq: usize,
    vector: &[f32],
) -> u8 {
    let (start, end) = chunk_range(dim, subquantizers, subq);
    debug_assert_eq!(vector.len(), end - start);
    let mut best_code = 0u8;
    let mut best_dist = f32::INFINITY;
    for code in 0..SKETCH_K {
        let offset = codebook_offset(dim, subquantizers, subq, code);
        let centroid = &codebook[offset..offset + vector.len()];
        let dist = sq_l2(vector, centroid);
        if dist < best_dist {
            best_dist = dist;
            best_code = code as u8;
        }
    }
    best_code
}

fn codebook_offset(dim: usize, subquantizers: usize, subq: usize, code: usize) -> usize {
    let mut offset = 0usize;
    for prev in 0..subq {
        let (start, end) = chunk_range(dim, subquantizers, prev);
        offset += SKETCH_K * (end - start);
    }
    let (start, end) = chunk_range(dim, subquantizers, subq);
    offset + code * (end - start)
}

fn chunk_range(dim: usize, chunks: usize, chunk: usize) -> (usize, usize) {
    let start = chunk * dim / chunks;
    let end = (chunk + 1) * dim / chunks;
    (start, end)
}

fn packed_code_bytes(subquantizers: usize) -> usize {
    subquantizers.div_ceil(2)
}

fn pack_nibble(bytes: &mut [u8], index: usize, value: u8) {
    debug_assert!(value < 16);
    let slot = &mut bytes[index / 2];
    if index % 2 == 0 {
        *slot = (*slot & 0xF0) | value;
    } else {
        *slot = (*slot & 0x0F) | (value << 4);
    }
}

fn unpack_nibble(bytes: &[u8], index: usize) -> u8 {
    let value = bytes[index / 2];
    if index % 2 == 0 {
        value & 0x0F
    } else {
        value >> 4
    }
}

#[inline]
fn sq_l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| {
            let diff = x - y;
            diff * diff
        })
        .sum()
}

#[inline]
fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum()
}

fn bitset_len(bits: usize) -> usize {
    bits.div_ceil(8)
}

fn set_bit(bytes: &mut [u8], bit: usize) {
    bytes[bit / 8] |= 1 << (bit % 8);
}

fn bit_is_set(bytes: &[u8], bit: usize) -> bool {
    bytes
        .get(bit / 8)
        .map(|byte| byte & (1 << (bit % 8)) != 0)
        .unwrap_or(false)
}

fn read_u32(data: &[u8], offset: usize, label: &str) -> Result<u32> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| ZeppelinError::Index(format!("{label} truncated")))?;
    Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
        ZeppelinError::Index(format!("{label} parse error"))
    })?))
}

fn read_u64(data: &[u8], offset: usize, label: &str) -> Result<u64> {
    let bytes = data
        .get(offset..offset + 8)
        .ok_or_else(|| ZeppelinError::Index(format!("{label} truncated")))?;
    Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
        ZeppelinError::Index(format!("{label} parse error"))
    })?))
}

fn read_f32(data: &[u8], offset: usize, label: &str) -> Result<f32> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| ZeppelinError::Index(format!("{label} truncated")))?;
    Ok(f32::from_le_bytes(bytes.try_into().map_err(|_| {
        ZeppelinError::Index(format!("{label} parse error"))
    })?))
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sketch_roundtrip_and_attr_bits() {
        let attrs = vec![
            vec![None, None],
            vec![Some(HashMap::from([(
                "kind".to_string(),
                AttributeValue::String("x".to_string()),
            )]))],
        ];
        let clusters = vec![
            vec![vec![1.0, 0.0, 0.0], vec![0.9, 0.1, 0.0]],
            vec![vec![0.0, 1.0, 0.0]],
        ];

        let (sketch_ref, bytes, sketch) =
            build_resident_sketch("ns", "seg", 3, &clusters, &attrs).unwrap();
        assert_eq!(sketch_ref.bytes_per_vector, packed_code_bytes(3));
        assert_eq!(sketch_ref.code_dims, 3);
        assert!(!sketch.cluster_has_attrs(0));
        assert!(sketch.cluster_has_attrs(1));

        let decoded = ResidentSketch::from_bytes(&bytes).unwrap();
        let selected = decoded
            .select_clusters(&[1.0, 0.0, 0.0], DistanceMetric::Cosine, &[0, 1], 1)
            .unwrap();
        assert_eq!(selected.len(), 1);
    }
}
