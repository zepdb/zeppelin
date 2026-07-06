//! Resident coarse sketch for IVF-Flat segments.
//!
//! The sketch is a small immutable segment artifact built during compaction
//! from corpus vectors only. It stores product-quantization codes for every
//! vector, ordered by IVF cluster. Query-time code scans this resident data
//! inside the requested `nprobe` centroid set, selects a small cluster set,
//! and the normal cluster reader performs exact rerank.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use bytes::Bytes;

use crate::error::{Result, ZeppelinError};
use crate::index::ivf_flat::kmeans::train_kmeans;
use crate::types::{AttributeValue, DistanceMetric};
use crate::wal::manifest::SketchRef;

const SKETCH_MAGIC: &[u8; 4] = b"ZSK1";
const SKETCH_VERSION: u32 = 3;
const SKETCH_V2_VERSION: u32 = 2;
const SKETCH_V2_K: usize = 16;
const SKETCH_V3_K: usize = 256;
const SKETCH_MAX_SUBQUANTIZERS: usize = 64;
const SKETCH_K: usize = SKETCH_V3_K;
const SKETCH_TRAIN_SAMPLE: usize = 4096;
const SKETCH_TRAIN_ITERS: usize = 6;
const SKETCH_CLUSTER_SCORE_TOP_M: usize = 2;
const SKETCH_CODE_WIDTH: SketchCodeWidth = SketchCodeWidth::EightBit;

/// S3 key for the resident coarse sketch.
#[must_use]
pub fn sketch_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/coarse_sketch.bin")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SketchCodeWidth {
    FourBit,
    EightBit,
}

impl SketchCodeWidth {
    fn packed_code_bytes(self, subquantizers: usize) -> usize {
        match self {
            Self::FourBit => subquantizers.div_ceil(2),
            Self::EightBit => subquantizers,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SketchFormat {
    codebook_size: usize,
    code_width: SketchCodeWidth,
}

/// In-memory resident sketch loaded from the immutable segment artifact.
#[derive(Debug, Clone)]
pub(crate) struct ResidentSketch {
    dim: usize,
    subquantizers: usize,
    cluster_count: usize,
    codebook: Vec<f32>,
    codebook_size: usize,
    cluster_offsets: Vec<(usize, usize)>,
    codes: Bytes,
    cluster_has_attrs: Vec<bool>,
    packed_code_bytes: usize,
    code_width: SketchCodeWidth,
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
        let format = sketch_format(version)?;
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

        let codebook_floats = dim.checked_mul(format.codebook_size).ok_or_else(|| {
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
        let packed_code_bytes = format.code_width.packed_code_bytes(subquantizers);
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
            codebook_size: format.codebook_size,
            cluster_offsets,
            codes: Bytes::copy_from_slice(&data[codes_offset..expected]),
            cluster_has_attrs,
            packed_code_bytes,
            code_width: format.code_width,
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
        budget: AdaptiveClusterBudget,
        mass_top_k: usize,
    ) -> Result<Vec<usize>> {
        budget.validate()?;
        let ranked_clusters =
            self.rank_clusters(query, distance_metric, probe_clusters, mass_top_k)?;
        if budget.max_clusters >= probe_clusters.len() {
            return Ok(probe_clusters.to_vec());
        }
        let target_count = adaptive_cluster_count(&ranked_clusters, budget);
        Ok(ranked_clusters
            .into_iter()
            .take(target_count)
            .map(|score| score.cluster_idx)
            .collect())
    }

    /// Rank clusters inside the requested centroid-probe set by sketch score.
    pub(crate) fn rank_clusters(
        &self,
        query: &[f32],
        distance_metric: DistanceMetric,
        probe_clusters: &[usize],
        mass_top_k: usize,
    ) -> Result<Vec<ClusterScore>> {
        if query.len() != self.dim {
            return Err(ZeppelinError::DimensionMismatch {
                expected: self.dim,
                actual: query.len(),
            });
        }
        if probe_clusters.is_empty() {
            return Err(ZeppelinError::Index(
                "coarse sketch received an empty probe set".into(),
            ));
        }
        if mass_top_k == 0 {
            return Err(ZeppelinError::Index(
                "coarse sketch mass top-k is zero".into(),
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
        let mut top_rows = BinaryHeap::with_capacity(mass_top_k);
        for &cluster_idx in probe_clusters {
            let (start, end) = self.cluster_offsets[cluster_idx];
            if start == end {
                continue;
            }

            let mut top_scores = TopSketchScores::new();
            for row in start..end {
                let code_offset = row * self.packed_code_bytes;
                let codes = &self.codes[code_offset..code_offset + self.packed_code_bytes];
                let score = self.adc_score(&adc_table, codes);
                top_scores.insert(score);
                insert_top_mass_row(
                    &mut top_rows,
                    mass_top_k,
                    SketchRowScore { score, cluster_idx },
                );
            }
            ranked_clusters.push(ClusterScore {
                cluster_idx,
                aggregate_score: top_scores.mean(),
                mass_count: 0,
            });
        }

        let mut mass_counts = vec![0usize; self.cluster_count];
        for row in top_rows {
            mass_counts[row.cluster_idx] += 1;
        }
        for score in &mut ranked_clusters {
            score.mass_count = mass_counts[score.cluster_idx];
        }

        ranked_clusters.sort_by(|a, b| {
            b.mass_count
                .cmp(&a.mass_count)
                .then_with(|| {
                    a.aggregate_score
                        .partial_cmp(&b.aggregate_score)
                        .unwrap_or(Ordering::Equal)
                })
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

        Ok(ranked_clusters)
    }

    fn build_adc_table(&self, query: &[f32], distance_metric: DistanceMetric) -> Vec<f32> {
        let mut table = vec![0.0f32; self.subquantizers * self.codebook_size];
        for subq in 0..self.subquantizers {
            let (start, end) = chunk_range(self.dim, self.subquantizers, subq);
            for code in 0..self.codebook_size {
                let centroid_offset =
                    codebook_offset(self.dim, self.subquantizers, self.codebook_size, subq, code);
                let centroid = &self.codebook[centroid_offset..centroid_offset + (end - start)];
                let q = &query[start..end];
                table[subq * self.codebook_size + code] = match distance_metric {
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
            let code = unpack_code(packed_codes, subq, self.code_width) as usize;
            score += adc_table[subq * self.codebook_size + code];
        }
        score
    }
}

/// Query-local cluster budget for resident-sketch selection.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AdaptiveClusterBudget {
    floor_clusters: usize,
    max_clusters: usize,
    relative_score_margin: f32,
}

impl AdaptiveClusterBudget {
    #[must_use]
    pub(crate) fn new(
        floor_clusters: usize,
        max_clusters: usize,
        relative_score_margin: f32,
    ) -> Self {
        Self {
            floor_clusters,
            max_clusters,
            relative_score_margin,
        }
    }

    #[must_use]
    pub(crate) fn max_clusters(self) -> usize {
        self.max_clusters
    }

    fn validate(self) -> Result<()> {
        if self.max_clusters == 0 {
            return Err(ZeppelinError::Index(
                "coarse sketch max cluster budget is zero".into(),
            ));
        }
        if self.floor_clusters == 0 {
            return Err(ZeppelinError::Index(
                "coarse sketch floor cluster budget is zero".into(),
            ));
        }
        if self.floor_clusters > self.max_clusters {
            return Err(ZeppelinError::Index(format!(
                "coarse sketch floor budget {} exceeds max budget {}",
                self.floor_clusters, self.max_clusters
            )));
        }
        if !self.relative_score_margin.is_finite() || self.relative_score_margin < 0.0 {
            return Err(ZeppelinError::Index(format!(
                "coarse sketch invalid relative score margin: {}",
                self.relative_score_margin
            )));
        }
        Ok(())
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
    let code_width = SKETCH_CODE_WIDTH;
    let packed_code_bytes = code_width.packed_code_bytes(subquantizers);
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
                let code = encode_subvector(
                    &codebook,
                    dim,
                    subquantizers,
                    SKETCH_K,
                    subq,
                    &vector[start..end],
                );
                pack_code(&mut packed, subq, code, code_width);
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

/// Result of attempting to stitch a resident sketch from a previous segment.
pub(crate) enum ResidentSketchStitch {
    /// The old sketch was compatible and the new sketch was stitched.
    Stitched(SketchRef, Bytes, Box<ResidentSketch>),
    /// The old sketch could not be reused; caller should rebuild the sketch.
    Unavailable(&'static str),
}

/// Build a new resident sketch by reusing the old codebook and carried codes.
///
/// Untouched clusters copy their old code sections byte-for-byte. Touched
/// clusters are re-encoded against the same old codebook, so the result is
/// identical to a full re-encode of all surviving vectors against that
/// codebook without paying to encode carried clusters again.
#[must_use = "callers must use the stitched sketch or intentionally rebuild"]
#[allow(clippy::type_complexity)]
pub(crate) fn stitch_resident_sketch(
    namespace: &str,
    segment_id: &str,
    dim: usize,
    old: &ResidentSketch,
    touched: &[bool],
    cluster_vecs: &[Vec<Vec<f32>>],
    cluster_attrs: &[Vec<Option<HashMap<String, AttributeValue>>>],
) -> Result<ResidentSketchStitch> {
    let cluster_count = cluster_vecs.len();
    if cluster_count == 0 {
        return Err(ZeppelinError::Index(
            "cannot stitch coarse sketch with zero clusters".into(),
        ));
    }
    if touched.len() != cluster_count {
        return Err(ZeppelinError::Index(format!(
            "coarse sketch touched length mismatch: expected {cluster_count}, got {}",
            touched.len()
        )));
    }
    if cluster_attrs.len() != cluster_count {
        return Err(ZeppelinError::Index(format!(
            "coarse sketch cluster_attrs length mismatch: expected {cluster_count}, got {}",
            cluster_attrs.len()
        )));
    }

    let expected_subquantizers = SKETCH_MAX_SUBQUANTIZERS.min(dim).max(1);
    if old.dim != dim {
        return Ok(ResidentSketchStitch::Unavailable("dim_mismatch"));
    }
    if old.subquantizers != expected_subquantizers {
        return Ok(ResidentSketchStitch::Unavailable("subquantizer_mismatch"));
    }
    if old.cluster_count != cluster_count {
        return Ok(ResidentSketchStitch::Unavailable("cluster_count_mismatch"));
    }
    if old.codebook_size != SKETCH_K || old.code_width != SKETCH_CODE_WIDTH {
        return Ok(ResidentSketchStitch::Unavailable("format_mismatch"));
    }

    let mut counts = Vec::with_capacity(cluster_count);
    let mut vector_count = 0usize;
    let mut attr_bits = vec![0u8; bitset_len(cluster_count)];
    let mut codes = Vec::new();
    for cluster_idx in 0..cluster_count {
        if touched[cluster_idx] {
            let count = cluster_vecs[cluster_idx].len();
            counts.push(count);
            vector_count = vector_count.checked_add(count).ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "coarse sketch vector count overflows at cluster {cluster_idx}"
                ))
            })?;
            if cluster_attrs[cluster_idx].iter().any(Option::is_some) {
                set_bit(&mut attr_bits, cluster_idx);
            }
            for vector in &cluster_vecs[cluster_idx] {
                if vector.len() != dim {
                    return Err(ZeppelinError::DimensionMismatch {
                        expected: dim,
                        actual: vector.len(),
                    });
                }
                let mut packed = vec![0u8; old.packed_code_bytes];
                for subq in 0..old.subquantizers {
                    let (start, end) = chunk_range(dim, old.subquantizers, subq);
                    let code = encode_subvector(
                        &old.codebook,
                        dim,
                        old.subquantizers,
                        old.codebook_size,
                        subq,
                        &vector[start..end],
                    );
                    pack_code(&mut packed, subq, code, old.code_width);
                }
                codes.extend_from_slice(&packed);
            }
            continue;
        }

        let (row_start, row_end) = old.cluster_offsets[cluster_idx];
        let count = row_end.checked_sub(row_start).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "coarse sketch invalid old offsets for cluster {cluster_idx}"
            ))
        })?;
        if cluster_vecs[cluster_idx].len() != count {
            return Err(ZeppelinError::Index(format!(
                "untouched cluster {cluster_idx} vector count changed: old={count}, new={}",
                cluster_vecs[cluster_idx].len()
            )));
        }
        counts.push(count);
        vector_count = vector_count.checked_add(count).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "coarse sketch vector count overflows at carried cluster {cluster_idx}"
            ))
        })?;
        if old.cluster_has_attrs[cluster_idx] {
            set_bit(&mut attr_bits, cluster_idx);
        }
        let byte_start = row_start
            .checked_mul(old.packed_code_bytes)
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "coarse sketch carried code start overflows at cluster {cluster_idx}"
                ))
            })?;
        let byte_end = row_end.checked_mul(old.packed_code_bytes).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "coarse sketch carried code end overflows at cluster {cluster_idx}"
            ))
        })?;
        codes.extend_from_slice(&old.codes[byte_start..byte_end]);
    }

    if vector_count == 0 {
        return Err(ZeppelinError::Index(
            "cannot stitch coarse sketch from empty vector set".into(),
        ));
    }

    let total = 28usize
        .checked_add(old.codebook.len().checked_mul(4).ok_or_else(|| {
            ZeppelinError::Index("coarse sketch codebook byte size overflows".into())
        })?)
        .and_then(|size| size.checked_add(attr_bits.len()))
        .and_then(|size| size.checked_add(cluster_count.checked_mul(4)?))
        .and_then(|size| size.checked_add(codes.len()))
        .ok_or_else(|| ZeppelinError::Index("coarse sketch total size overflows".into()))?;
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(SKETCH_MAGIC);
    buf.extend_from_slice(&SKETCH_VERSION.to_le_bytes());
    buf.extend_from_slice(&(dim as u32).to_le_bytes());
    buf.extend_from_slice(&(old.subquantizers as u32).to_le_bytes());
    buf.extend_from_slice(&(cluster_count as u32).to_le_bytes());
    buf.extend_from_slice(&(vector_count as u64).to_le_bytes());
    for value in &old.codebook {
        buf.extend_from_slice(&value.to_le_bytes());
    }
    buf.extend_from_slice(&attr_bits);
    for count in counts {
        let count = u32::try_from(count).map_err(|_| {
            ZeppelinError::Index(format!(
                "coarse sketch cluster has too many vectors: {count}"
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
        code_dims: old.subquantizers,
        bytes_per_vector: old.packed_code_bytes,
        size_bytes: bytes.len() as u64,
    };
    let resident = ResidentSketch::from_bytes(&bytes)?;
    Ok(ResidentSketchStitch::Stitched(
        sketch_ref,
        bytes,
        Box::new(resident),
    ))
}

#[derive(Clone, Copy)]
pub(crate) struct ClusterScore {
    pub(crate) cluster_idx: usize,
    pub(crate) aggregate_score: f32,
    pub(crate) mass_count: usize,
}

#[derive(Clone, Copy, Debug)]
struct SketchRowScore {
    score: f32,
    cluster_idx: usize,
}

impl PartialEq for SketchRowScore {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.cluster_idx == other.cluster_idx
    }
}

impl Eq for SketchRowScore {}

impl PartialOrd for SketchRowScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SketchRowScore {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.cluster_idx.cmp(&other.cluster_idx))
    }
}

fn insert_top_mass_row(
    top_rows: &mut BinaryHeap<SketchRowScore>,
    mass_top_k: usize,
    row: SketchRowScore,
) {
    if top_rows.len() < mass_top_k {
        top_rows.push(row);
        return;
    }

    let Some(&worst) = top_rows.peek() else {
        top_rows.push(row);
        return;
    };
    if row.score.total_cmp(&worst.score).is_lt() {
        top_rows.pop();
        top_rows.push(row);
    }
}

struct TopSketchScores {
    scores: [f32; SKETCH_CLUSTER_SCORE_TOP_M],
    len: usize,
}

impl TopSketchScores {
    fn new() -> Self {
        Self {
            scores: [f32::INFINITY; SKETCH_CLUSTER_SCORE_TOP_M],
            len: 0,
        }
    }

    fn insert(&mut self, score: f32) {
        if self.len < SKETCH_CLUSTER_SCORE_TOP_M {
            self.scores[self.len] = score;
            self.len += 1;
            self.bubble_up(self.len - 1);
            return;
        }

        if score >= self.scores[SKETCH_CLUSTER_SCORE_TOP_M - 1] {
            return;
        }

        self.scores[SKETCH_CLUSTER_SCORE_TOP_M - 1] = score;
        self.bubble_up(SKETCH_CLUSTER_SCORE_TOP_M - 1);
    }

    fn mean(&self) -> f32 {
        debug_assert!(self.len > 0);
        self.scores[..self.len].iter().sum::<f32>() / self.len as f32
    }

    fn bubble_up(&mut self, mut idx: usize) {
        while idx > 0 && self.scores[idx] < self.scores[idx - 1] {
            self.scores.swap(idx, idx - 1);
            idx -= 1;
        }
    }
}

fn adaptive_cluster_count(
    ranked_clusters: &[ClusterScore],
    budget: AdaptiveClusterBudget,
) -> usize {
    debug_assert!(budget.validate().is_ok());
    let max_clusters = budget.max_clusters.min(ranked_clusters.len());
    let floor_clusters = budget.floor_clusters.min(max_clusters);
    if floor_clusters == max_clusters || ranked_clusters.is_empty() {
        return max_clusters;
    }

    let mass_cutoff =
        mass_score_cutoff(ranked_clusters[0].mass_count, budget.relative_score_margin);
    let mut count = floor_clusters;
    while count < max_clusters && ranked_clusters[count].mass_count >= mass_cutoff {
        count += 1;
    }
    count
}

pub(crate) fn mass_score_cutoff(best_mass: usize, relative_margin: f32) -> usize {
    if best_mass == 0 {
        return 0;
    }
    if relative_margin >= 1.0 {
        return 1;
    }
    ((best_mass as f32) * (1.0 - relative_margin))
        .ceil()
        .max(1.0) as usize
}

fn sample_indices(vector_count: usize, sample_count: usize) -> Vec<usize> {
    if sample_count >= vector_count {
        return (0..vector_count).collect();
    }
    (0..sample_count)
        .map(|i| i * vector_count / sample_count)
        .collect()
}

fn sketch_format(version: u32) -> Result<SketchFormat> {
    match version {
        SKETCH_V2_VERSION => Ok(SketchFormat {
            codebook_size: SKETCH_V2_K,
            code_width: SketchCodeWidth::FourBit,
        }),
        SKETCH_VERSION => Ok(SketchFormat {
            codebook_size: SKETCH_V3_K,
            code_width: SketchCodeWidth::EightBit,
        }),
        _ => Err(ZeppelinError::Index(format!(
            "unsupported coarse sketch version: {version}"
        ))),
    }
}

#[inline]
fn encode_subvector(
    codebook: &[f32],
    dim: usize,
    subquantizers: usize,
    codebook_size: usize,
    subq: usize,
    vector: &[f32],
) -> u8 {
    let (start, end) = chunk_range(dim, subquantizers, subq);
    debug_assert_eq!(vector.len(), end - start);
    debug_assert!(codebook_size <= u8::MAX as usize + 1);
    let mut best_code = 0u8;
    let mut best_dist = f32::INFINITY;
    for code in 0..codebook_size {
        let offset = codebook_offset(dim, subquantizers, codebook_size, subq, code);
        let centroid = &codebook[offset..offset + vector.len()];
        let dist = sq_l2(vector, centroid);
        if dist < best_dist {
            best_dist = dist;
            best_code = code as u8;
        }
    }
    best_code
}

fn codebook_offset(
    dim: usize,
    subquantizers: usize,
    codebook_size: usize,
    subq: usize,
    code: usize,
) -> usize {
    let mut offset = 0usize;
    for prev in 0..subq {
        let (start, end) = chunk_range(dim, subquantizers, prev);
        offset += codebook_size * (end - start);
    }
    let (start, end) = chunk_range(dim, subquantizers, subq);
    offset + code * (end - start)
}

fn chunk_range(dim: usize, chunks: usize, chunk: usize) -> (usize, usize) {
    let start = chunk * dim / chunks;
    let end = (chunk + 1) * dim / chunks;
    (start, end)
}

#[cfg(test)]
fn packed_code_bytes(subquantizers: usize) -> usize {
    SKETCH_CODE_WIDTH.packed_code_bytes(subquantizers)
}

fn pack_code(bytes: &mut [u8], index: usize, value: u8, code_width: SketchCodeWidth) {
    match code_width {
        SketchCodeWidth::FourBit => pack_nibble(bytes, index, value),
        SketchCodeWidth::EightBit => {
            bytes[index] = value;
        }
    }
}

fn unpack_code(bytes: &[u8], index: usize, code_width: SketchCodeWidth) -> u8 {
    match code_width {
        SketchCodeWidth::FourBit => unpack_nibble(bytes, index),
        SketchCodeWidth::EightBit => bytes[index],
    }
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

    fn fixed_budget(cluster_budget: usize) -> AdaptiveClusterBudget {
        AdaptiveClusterBudget::new(cluster_budget, cluster_budget, 0.0)
    }

    fn one_dim_sketch_with_scores(scores: &[f32]) -> ResidentSketch {
        assert!(scores.len() <= SKETCH_K);
        let mut codebook = vec![0.0; SKETCH_K];
        let mut code_bytes = Vec::new();
        for (code, &score) in scores.iter().enumerate() {
            codebook[code] = score.sqrt();
            let mut packed = vec![0u8; 1];
            pack_code(&mut packed, 0, code as u8, SKETCH_CODE_WIDTH);
            code_bytes.extend_from_slice(&packed);
        }

        ResidentSketch {
            dim: 1,
            subquantizers: 1,
            cluster_count: scores.len(),
            codebook,
            codebook_size: SKETCH_K,
            cluster_offsets: (0..scores.len()).map(|row| (row, row + 1)).collect(),
            codes: Bytes::from(code_bytes),
            cluster_has_attrs: vec![false; scores.len()],
            packed_code_bytes: 1,
            code_width: SKETCH_CODE_WIDTH,
        }
    }

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
            .select_clusters(
                &[1.0, 0.0, 0.0],
                DistanceMetric::Cosine,
                &[0, 1],
                fixed_budget(1),
                1,
            )
            .unwrap();
        assert_eq!(selected.len(), 1);
    }

    #[test]
    fn stitch_reports_incompatible_dim_without_reencoding() {
        let attrs = vec![vec![None], vec![None]];
        let clusters = vec![vec![vec![1.0, 0.0, 0.0]], vec![vec![0.0, 1.0, 0.0]]];
        let (_sketch_ref, _bytes, old) =
            build_resident_sketch("ns", "old", 3, &clusters, &attrs).unwrap();

        let new_attrs = vec![vec![None], vec![None]];
        let new_clusters = vec![
            vec![vec![1.0, 0.0, 0.0, 0.0]],
            vec![vec![0.0, 1.0, 0.0, 0.0]],
        ];
        let result = stitch_resident_sketch(
            "ns",
            "new",
            4,
            &old,
            &[true, false],
            &new_clusters,
            &new_attrs,
        )
        .unwrap();

        match result {
            ResidentSketchStitch::Unavailable(reason) => assert_eq!(reason, "dim_mismatch"),
            ResidentSketchStitch::Stitched(_, _, _) => {
                panic!("dim-mismatched old sketch must not be stitched")
            }
        }
    }

    #[test]
    fn cluster_selection_prefers_global_top_k_mass() {
        let mut codebook = Vec::with_capacity(SKETCH_K);
        for value in 0..SKETCH_K {
            codebook.push(value as f32);
        }

        let mut code_bytes = Vec::new();
        for code in [0u8, 4, 4, 4, 1, 1, 1, 1] {
            let mut packed = vec![0u8; 1];
            pack_code(&mut packed, 0, code, SKETCH_CODE_WIDTH);
            code_bytes.extend_from_slice(&packed);
        }

        let sketch = ResidentSketch {
            dim: 1,
            subquantizers: 1,
            cluster_count: 2,
            codebook,
            codebook_size: SKETCH_K,
            cluster_offsets: vec![(0, 4), (4, 8)],
            codes: Bytes::from(code_bytes),
            cluster_has_attrs: vec![false, false],
            packed_code_bytes: 1,
            code_width: SKETCH_CODE_WIDTH,
        };

        let selected = sketch
            .select_clusters(
                &[0.0],
                DistanceMetric::Euclidean,
                &[0, 1],
                fixed_budget(1),
                4,
            )
            .unwrap();

        assert_eq!(selected, vec![1]);
    }

    #[test]
    fn cluster_selection_ties_mass_by_top_m_mean() {
        let mut codebook = Vec::with_capacity(SKETCH_K);
        for value in 0..SKETCH_K {
            codebook.push(value as f32);
        }

        let mut code_bytes = Vec::new();
        for code in [0u8, 4, 1, 1] {
            let mut packed = vec![0u8; 1];
            pack_code(&mut packed, 0, code, SKETCH_CODE_WIDTH);
            code_bytes.extend_from_slice(&packed);
        }

        let sketch = ResidentSketch {
            dim: 1,
            subquantizers: 1,
            cluster_count: 2,
            codebook,
            codebook_size: SKETCH_K,
            cluster_offsets: vec![(0, 2), (2, 4)],
            codes: Bytes::from(code_bytes),
            cluster_has_attrs: vec![false, false],
            packed_code_bytes: 1,
            code_width: SKETCH_CODE_WIDTH,
        };

        let selected = sketch
            .select_clusters(
                &[0.0],
                DistanceMetric::Euclidean,
                &[0, 1],
                fixed_budget(1),
                2,
            )
            .unwrap();

        assert_eq!(selected, vec![1]);
    }

    #[test]
    fn sketch_code_bytes_stay_within_resident_fence() {
        assert_eq!(packed_code_bytes(SKETCH_MAX_SUBQUANTIZERS), 64);
    }

    #[test]
    fn adaptive_cluster_selection_uses_floor_and_score_gap() {
        let sketch = one_dim_sketch_with_scores(&[1.0, 1.03, 1.07, 1.20, 1.21, 1.22]);

        let selected = sketch
            .select_clusters(
                &[0.0],
                DistanceMetric::Euclidean,
                &[0, 1, 2, 3, 4, 5],
                AdaptiveClusterBudget::new(1, 5, 0.08),
                3,
            )
            .unwrap();

        assert_eq!(selected, vec![0, 1, 2]);

        let floor_selected = sketch
            .select_clusters(
                &[0.0],
                DistanceMetric::Euclidean,
                &[0, 1, 2, 3, 4, 5],
                AdaptiveClusterBudget::new(4, 5, 0.01),
                4,
            )
            .unwrap();
        assert_eq!(floor_selected, vec![0, 1, 2, 3]);
    }

    #[test]
    fn adaptive_cluster_selection_respects_cap() {
        let sketch = one_dim_sketch_with_scores(&[1.0, 1.01, 1.02, 1.03]);

        let selected = sketch
            .select_clusters(
                &[0.0],
                DistanceMetric::Euclidean,
                &[0, 1, 2, 3],
                AdaptiveClusterBudget::new(1, 2, 0.50),
                4,
            )
            .unwrap();

        assert_eq!(selected, vec![0, 1]);
    }

    #[test]
    fn adaptive_cluster_selection_no_ops_when_cap_covers_probe_set() {
        let sketch = one_dim_sketch_with_scores(&[8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]);
        let probe_clusters = vec![7, 6, 5, 4, 3, 2, 1, 0];

        let selected = sketch
            .select_clusters(
                &[0.0],
                DistanceMetric::Euclidean,
                &probe_clusters,
                AdaptiveClusterBudget::new(4, 8, 0.01),
                8,
            )
            .unwrap();

        assert_eq!(selected, probe_clusters);
    }

    #[test]
    fn v2_four_bit_sketch_still_decodes() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SKETCH_MAGIC);
        bytes.extend_from_slice(&SKETCH_V2_VERSION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes()); // dim
        bytes.extend_from_slice(&1u32.to_le_bytes()); // subquantizers
        bytes.extend_from_slice(&1u32.to_le_bytes()); // cluster_count
        bytes.extend_from_slice(&1u64.to_le_bytes()); // vector_count

        for value in 0..SKETCH_V2_K {
            bytes.extend_from_slice(&(value as f32).to_le_bytes());
        }
        bytes.push(0); // attr bits
        bytes.extend_from_slice(&1u32.to_le_bytes()); // cluster 0 count
        let mut code = vec![0u8; 1];
        pack_nibble(&mut code, 0, 1);
        bytes.extend_from_slice(&code);

        let sketch = ResidentSketch::from_bytes(&bytes).unwrap();
        assert_eq!(sketch.codebook_size, SKETCH_V2_K);
        assert_eq!(sketch.code_width, SketchCodeWidth::FourBit);
        assert_eq!(sketch.packed_code_bytes, 1);

        let selected = sketch
            .select_clusters(&[0.0], DistanceMetric::Euclidean, &[0], fixed_budget(1), 1)
            .unwrap();
        assert_eq!(selected, vec![0]);
    }
}
