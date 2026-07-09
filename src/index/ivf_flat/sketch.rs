//! Builds and searches the resident coarse sketch for an IVF-Flat segment.
//!
//! IVF first chooses `nprobe` nearby centroid clusters; larger `nprobe` usually
//! improves recall—the chance of finding the true nearest neighbors—at the cost
//! of more object reads and exact distance work. This module adds a smaller,
//! memory-resident selection stage inside that probe set. It stores one product
//! quantization (PQ) code per corpus vector, ordered by IVF cluster. PQ divides a
//! vector into contiguous subvectors called subquantizers, learns a centroid
//! codebook for each part, and replaces each part with a small integer code
//! naming its nearest learned centroid. Manifest metadata calls these
//! projection/code dimensions; in this implementation each dimension is one
//! contiguous subvector slot, not a learned matrix projection.
//!
//! At query time, asymmetric distance computation (ADC) compares the full-
//! precision query with codebook centroids once, then scores compact row codes
//! by table lookup. The sketch ranks clusters; the normal IVF reader still
//! fetches selected full-precision cluster data and performs the authoritative
//! exact rerank. The sketch is therefore a recall/latency optimization, not a
//! replacement source of vector truth.
//!
//! ```text
//! compaction/build                         query
//! ----------------                        -----
//! clustered full vectors                  nprobe centroid set
//!          |                                        |
//!          v                                        v
//! train PQ codebooks                    build query-to-codebook table
//!          |                                        |
//!          v                                        v
//! encode one compact code per row        scan resident codes in probe set
//!          |                                        |
//!          v                                        v
//! upload immutable sketch object         adaptive cluster/object budget
//!          |                                        |
//!          v                                        v
//! manifest publishes SketchRef           fetch full vectors + exact rerank
//! ```
//!
//! This module performs no S3 operations. `build_resident_sketch` and
//! `stitch_resident_sketch` return bytes and a [`SketchRef`]; their callers
//! upload immutable objects and later expose them through the authoritative
//! manifest. `ResidentSketch::from_bytes` validates bytes loaded either from
//! the sketch object or a segment bootstrap object.
//!
//! ## Reading map
//!
//! 1. `ResidentSketch` describes the decoded resident representation and
//!    `ResidentSketch::from_bytes` defines format compatibility.
//! 2. `build_resident_sketch` trains codebooks and encodes a new artifact.
//! 3. `ResidentSketch::rank_clusters` shows ADC scoring and cluster ranking;
//!    `ResidentSketch::select_clusters` applies `AdaptiveClusterBudget`.
//! 4. `stitch_resident_sketch` reuses an old codebook and unchanged code spans
//!    during bounded incremental compaction.
//! 5. The small helpers after `ClusterScore` define deterministic ranking,
//!    packing, chunk layout, and guarded binary reads.
//!
//! ## Invariants and compatibility
//!
//! - Rows and packed codes remain grouped in logical cluster order; cluster
//!   count metadata must partition exactly the declared vector count.
//! - New artifacts use version 3 with 256 one-byte codes per subquantizer.
//!   Version 2's 16 four-bit codes remain readable, but only current version-3
//!   artifacts are eligible for incremental stitching.
//! - A stitched artifact may copy an untouched cluster's old code bytes only
//!   when vector count, dimension, cluster layout, codebook, and code width are
//!   compatible. Otherwise the caller receives an explicit unavailable reason.
//! - Existence in object storage does not make a sketch visible. The containing
//!   segment's manifest entry remains authoritative.
//! - Lower ADC scores are better. Dot product is negated so all supported
//!   metrics share that ordering.
//!
//! ## Rust concepts used here
//!
//! Borrowed slices let training and scoring inspect caller-owned vectors
//! without copying them. Owned `Vec` buffers hold codebooks and offsets, while
//! [`Bytes`] holds immutable encoded bytes. In Java these all look broadly like
//! references, and in C like pointer/length pairs, but Rust distinguishes who
//! owns the allocation and proves borrowed views cannot outlive it. The
//! `ResidentSketchStitch` also makes “reuse succeeded” and “rebuild is
//! required” explicit states that callers must exhaustively match.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use bytes::Bytes;

use crate::error::{Result, ZeppelinError};
use crate::index::ivf_flat::kmeans::train_kmeans;
use crate::types::{AttributeValue, DistanceMetric};
use crate::wal::manifest::SketchRef;

/// Four-byte signature for coarse-sketch objects.
const SKETCH_MAGIC: &[u8; 4] = b"ZSK1";
/// Current write format: 256 codewords represented by one byte per subvector.
const SKETCH_VERSION: u32 = 3;
/// Legacy readable format using packed four-bit codes.
const SKETCH_V2_VERSION: u32 = 2;
/// Number of codewords in each version-2 subquantizer codebook.
const SKETCH_V2_K: usize = 16;
/// Number of codewords in each version-3 subquantizer codebook.
const SKETCH_V3_K: usize = 256;
/// Resident-memory fence on the number of codes stored for one vector.
const SKETCH_MAX_SUBQUANTIZERS: usize = 64;
/// Codebook size used when writing new sketches.
const SKETCH_K: usize = SKETCH_V3_K;
/// Maximum corpus rows sampled to train each subquantizer codebook.
const SKETCH_TRAIN_SAMPLE: usize = 4096;
/// K-means refinement passes used for each coarse-sketch codebook.
const SKETCH_TRAIN_ITERS: usize = 6;
/// Best row scores averaged to break equal global-mass cluster ranks.
const SKETCH_CLUSTER_SCORE_TOP_M: usize = 2;
/// Packed-code representation used by newly written sketches.
const SKETCH_CODE_WIDTH: SketchCodeWidth = SketchCodeWidth::EightBit;

/// Constructs the object-store key for a segment's resident coarse sketch.
///
/// # Parameters
///
/// - `namespace`: Validated namespace key prefix.
/// - `segment_id`: Immutable segment identifier.
///
/// # Returns
///
/// An owned key ending in `segments/{segment_id}/coarse_sketch.bin`.
///
/// # Examples
///
/// `sketch_key("catalog", "seg-7")` names
/// `catalog/segments/seg-7/coarse_sketch.bin`. It constructs a key only; it
/// performs no object-store request.
#[must_use]
pub fn sketch_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/coarse_sketch.bin")
}

/// Persisted width of one subquantizer code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SketchCodeWidth {
    /// Two codes share one byte; used by readable version-2 artifacts.
    FourBit,
    /// Each code occupies one byte; used by current version-3 artifacts.
    EightBit,
}

impl SketchCodeWidth {
    /// Calculates bytes required for one vector's packed subquantizer codes.
    ///
    /// Four-bit layouts round odd code counts up to the next byte; eight-bit
    /// layouts use one byte per code. For example, three four-bit codes need two
    /// bytes, while three eight-bit codes need three.
    fn packed_code_bytes(self, subquantizers: usize) -> usize {
        match self {
            Self::FourBit => subquantizers.div_ceil(2),
            Self::EightBit => subquantizers,
        }
    }
}

/// Decoding parameters selected from an artifact's persisted version.
#[derive(Debug, Clone, Copy)]
struct SketchFormat {
    /// Number of learned centroid choices for each subquantizer.
    codebook_size: usize,
    /// On-disk width used to pack each centroid code.
    code_width: SketchCodeWidth,
}

/// Validated in-memory coarse index loaded from an immutable segment artifact.
///
/// Rows are represented only by compact PQ codes and cluster boundaries; full
/// vectors and IDs remain in normal segment data. Cloning this type clones the
/// codebook and offset vectors and cheaply clones the reference-counted
/// [`Bytes`] code buffer.
#[derive(Debug, Clone)]
pub(crate) struct ResidentSketch {
    /// Full vector dimension expected from every query.
    dim: usize,
    /// Number of contiguous vector chunks encoded independently.
    subquantizers: usize,
    /// Number of logical IVF clusters represented by offset ranges.
    cluster_count: usize,
    /// Flattened subquantizer codebooks in chunk-major, code-major order.
    codebook: Vec<f32>,
    /// Number of centroid choices available in each subquantizer.
    codebook_size: usize,
    /// Half-open row range for each cluster in the packed code stream.
    cluster_offsets: Vec<(usize, usize)>,
    /// Immutable cluster-ordered packed PQ codes.
    codes: Bytes,
    /// Conservative bit per cluster indicating any non-null row attributes.
    cluster_has_attrs: Vec<bool>,
    /// Encoded byte stride for one vector row.
    packed_code_bytes: usize,
    /// Version-selected interpretation of the packed code bytes.
    code_width: SketchCodeWidth,
}

impl ResidentSketch {
    /// Decodes and validates one complete version-2 or version-3 sketch.
    ///
    /// Validation derives every section size with checked arithmetic, requires
    /// the object length to match exactly, and proves cluster row counts sum to
    /// the declared vector count before constructing a resident view.
    ///
    /// # Parameters
    ///
    /// - `data`: Complete borrowed sketch bytes loaded by the caller.
    ///
    /// # Returns
    ///
    /// An owned, validated [`ResidentSketch`]. The codebook is decoded into
    /// floats and the packed-code section is copied into immutable [`Bytes`], so
    /// the result does not borrow `data`.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Index`] for a short object, wrong magic,
    /// unsupported version, zero/invalid dimensions or cluster counts, size
    /// arithmetic overflow, exact-size mismatch, truncated scalar fields, or
    /// cluster counts inconsistent with the declared row count. No partial
    /// sketch escapes.
    ///
    /// # Performance
    ///
    /// Performs one linear decode. It allocates the float codebook, cluster
    /// metadata, attribute flags, and a copy of the packed-code section. It
    /// performs no object-store I/O.
    ///
    /// # Examples
    ///
    /// A version-2 object is decoded with 16 four-bit codewords, while a
    /// version-3 object uses 256 eight-bit codewords. Appending one unexplained
    /// byte to either object causes an exact-size error instead of being ignored.
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

    /// Reports whether a cluster may contain at least one non-null attribute map.
    ///
    /// `false` proves that the encoded cluster had no non-null attributes when
    /// built. `true` is conservative: it can mean the bit was set or the caller
    /// supplied an out-of-range cluster index, in which case query planning must
    /// not skip attribute work based on missing sketch metadata.
    ///
    /// # Parameters
    ///
    /// - `cluster_idx`: Logical cluster index to inspect.
    ///
    /// # Returns
    ///
    /// `false` only for a known cluster whose bit is clear; otherwise `true`.
    ///
    /// # Examples
    ///
    /// If every row in cluster 2 stored `None`, this returns `false` for 2. An
    /// index beyond the cluster count returns `true` to preserve correctness.
    #[must_use]
    pub(crate) fn cluster_has_attrs(&self, cluster_idx: usize) -> bool {
        self.cluster_has_attrs
            .get(cluster_idx)
            .copied()
            .unwrap_or(true)
    }

    /// Selects an adaptively bounded subset of probed clusters for exact rerank.
    ///
    /// All candidates stay inside the caller's centroid-selected probe set.
    /// Ranking uses global top-row mass first, the mean of each cluster's best
    /// approximate scores second, and original centroid rank last. If the budget
    /// cap covers the entire probe set, validation and ranking still run but the
    /// original probe ordering is returned unchanged.
    ///
    /// # Parameters
    ///
    /// - `query`: Full-precision query with exactly the sketch dimension.
    /// - `distance_metric`: Metric whose lower-is-better ADC form scores codes.
    /// - `probe_clusters`: Non-empty, in-range centroid-selected cluster indexes.
    /// - `budget`: Validated floor, cap, and mass-margin policy.
    /// - `mass_top_k`: Number of globally best approximate rows used to measure
    ///   how much result mass falls in each cluster; must be positive.
    ///
    /// # Returns
    ///
    /// Cluster indexes ordered by coarse preference, except the no-pruning case
    /// preserves `probe_clusters`. Empty clusters do not enter a pruned result.
    ///
    /// # Errors
    ///
    /// Returns an index or dimension error for an invalid budget, query shape,
    /// empty probe set, zero `mass_top_k`, or out-of-range cluster index.
    ///
    /// # Performance
    ///
    /// Builds one ADC table and scans every compact code row in the requested
    /// clusters. It avoids S3 reads here; downstream exact rerank reads only the
    /// selected full cluster objects.
    ///
    /// # Examples
    ///
    /// With eight probed clusters and a floor of two, cap of four, and a narrow
    /// margin, the sketch may return three clusters whose rows dominate the
    /// global approximate top results. A cap of eight returns all probes.
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

    /// Ranks non-empty probe clusters by approximate result evidence.
    ///
    /// “Mass” counts how many of the globally best `mass_top_k` encoded rows
    /// belong to each cluster. Ties use the mean of that cluster's best two ADC
    /// row scores, then the cluster's incoming centroid-probe order.
    ///
    /// # Parameters
    ///
    /// - `query`: Full-precision query with `self.dim` components.
    /// - `distance_metric`: Euclidean, cosine, or dot-product score policy.
    /// - `probe_clusters`: Non-empty in-range logical clusters to consider.
    /// - `mass_top_k`: Positive size of the global approximate-result window.
    ///
    /// # Returns
    ///
    /// Owned [`ClusterScore`] values sorted best first. Empty probed clusters
    /// are omitted because they contribute no encoded rows.
    ///
    /// # Errors
    ///
    /// Returns a dimension or index error for invalid query/probe inputs.
    ///
    /// # Performance
    ///
    /// ADC-table construction costs `O(dim * codebook_size)`. Row scoring costs
    /// `O(rows_in_probes * subquantizers)` and retains at most `mass_top_k` rows
    /// in a heap plus one score per cluster.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The iterator chain that builds `centroid_rank` consumes copied integer
    /// indexes, not the borrowed probe slice. `BinaryHeap` owns only compact
    /// [`SketchRowScore`] values. Rust's borrows keep the query, codes, and probe
    /// storage read-only throughout without reference counting or locks.
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

    /// Precomputes query distance to every codeword for constant-time code lookup.
    ///
    /// The returned table is indexed by `(subquantizer, code)`. Cosine and
    /// Euclidean use squared L2 between chunks; dot product stores its negation
    /// so lower table sums consistently rank better.
    ///
    /// # Performance
    ///
    /// Allocates `subquantizers * codebook_size` floats and performs `dim *
    /// codebook_size` component operations.
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

    /// Sums precomputed ADC entries for one vector's packed PQ code.
    ///
    /// `adc_table` must match this sketch's layout and `packed_codes` must hold
    /// one complete row. The caller guarantees both by slicing validated
    /// resident buffers. The operation allocates nothing.
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

/// Query-local policy balancing minimum recall work against a hard read cap.
///
/// The floor guarantees some clusters survive, the cap bounds downstream work,
/// and the relative margin admits clusters whose global top-row mass stays
/// close enough to the best cluster. This is a `Copy` value: passing it copies
/// three machine-scale fields and performs no heap allocation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AdaptiveClusterBudget {
    /// Minimum clusters to retain, capped by available ranked clusters.
    floor_clusters: usize,
    /// Maximum clusters allowed to reach exact rerank.
    max_clusters: usize,
    /// Allowed fractional drop from the best cluster's global mass.
    relative_score_margin: f32,
}

impl AdaptiveClusterBudget {
    /// Creates a budget value; [`AdaptiveClusterBudget::validate`] enforces it at use.
    ///
    /// For example, `(2, 5, 0.10)` always retains at least two clusters, never
    /// more than five, and may retain extra clusters with at least 90% of the
    /// best cluster's mass.
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

    /// Returns the hard cluster cap without consuming owned resources.
    ///
    /// The receiver is copied because [`AdaptiveClusterBudget`] implements
    /// `Copy`; Java would pass these fields by value only if represented as
    /// primitives, while C has the same cheap struct-copy model.
    #[must_use]
    pub(crate) fn max_clusters(self) -> usize {
        self.max_clusters
    }

    /// Rejects budgets that cannot define a safe adaptive selection.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Index`] when the floor or cap is zero, the floor
    /// exceeds the cap, or the margin is negative, NaN, or infinite.
    ///
    /// # Examples
    ///
    /// `(1, 4, 0.08)` is valid; `(5, 4, 0.08)` and `(1, 4, NaN)` are not.
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

/// Trains PQ codebooks and serializes a new resident coarse sketch.
///
/// Training samples at most 4,096 rows at deterministic, evenly spaced corpus
/// positions. Each vector is split into at most 64 contiguous chunks; each
/// chunk receives a 256-centroid codebook and one byte code. If fewer than 256
/// samples exist, the last learned centroid is duplicated so every persisted
/// codebook retains the fixed current layout.
///
/// # Parameters
///
/// - `namespace`: Namespace used to derive the future immutable object key.
/// - `segment_id`: Segment identifier that will own the artifact.
/// - `dim`: Required full vector dimension.
/// - `cluster_vecs`: Full-precision corpus vectors grouped in logical cluster
///   order. All rows must have exactly `dim` finite values.
/// - `cluster_attrs`: Attribute maps grouped in the same outer cluster order;
///   each cluster contributes one conservative “has attributes” bit.
///
/// # Returns
///
/// A [`SketchRef`], complete immutable object bytes, and a decoded resident
/// sketch ready for the building node to use. Nothing has been uploaded or
/// published yet.
///
/// # Errors
///
/// Returns an index or dimension error for zero clusters, mismatched outer
/// attribute cluster count, no vectors, invalid row dimensions detected during
/// encoding, k-means failure, oversized cluster counts, invalid serialized
/// output, or arithmetic failures handled by called operations.
///
/// # Panics
///
/// Callers must validate dimensions and finite values before entry. A malformed
/// sampled row can panic while slicing, and [`train_kmeans`] debug-asserts
/// finite components. Practical dimensions/counts must also fit persisted
/// `u32`/`u64` fields.
///
/// # Consistency
///
/// The returned object is not visible merely because a caller uploads it. The
/// manifest must publish the returned [`SketchRef`] as part of the segment.
///
/// # Performance
///
/// Allocates sampled subvector copies for training, a `dim * 256` float
/// codebook, and up to 64 code bytes per row. Encoding compares every subvector
/// with 256 codewords. No object-store I/O occurs here.
///
/// # Examples
///
/// For three-dimensional rows, the builder uses three one-dimensional
/// subquantizers and stores three code bytes per vector. Two clusters produce
/// two contiguous code ranges plus two attribute-presence bits.
///
/// # Rust Notes for Java/C Engineers
///
/// `all_vectors` contains borrowed slices pointing into `cluster_vecs`; it does
/// not duplicate the corpus. Training samples do allocate owned subvector
/// copies because k-means needs a compact stable input. The returned tuple moves
/// three owned results to the caller with no implicit deep copies.
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

/// Outcome of attempting codebook-preserving incremental sketch construction.
///
/// Incompatibility is distinct from corruption: it tells the compactor that a
/// full rebuild is required and names the reason, while a `Result::Err` from
/// [`stitch_resident_sketch`] means the requested construction itself was
/// invalid or corrupt.
pub(crate) enum ResidentSketchStitch {
    /// New manifest metadata, complete bytes, and decoded resident state built
    /// with the compatible old codebook.
    Stitched(SketchRef, Bytes, Box<ResidentSketch>),
    /// Stable reason code explaining why the caller must rebuild from vectors.
    Unavailable(&'static str),
}

/// Build a new resident sketch by reusing the old codebook and carried codes.
///
/// Untouched clusters copy their old code sections byte-for-byte. Touched
/// clusters are re-encoded against the same old codebook, so the result is
/// identical to a full re-encode of all surviving vectors against that
/// codebook without paying to encode carried clusters again.
///
/// ```text
/// old validated sketch + touched[] + current clustered rows
///                       |
///          +------------+-------------+
///          |                          |
///   compatible layout          incompatible layout
///          |                          |
///          v                          v
/// touched: re-encode          Unavailable(reason)
/// untouched: copy bytes       caller decides whether rebuild is allowed
///          |
///          v
/// new immutable version-3 sketch bytes
/// ```
///
/// # Parameters
///
/// - `namespace`: Namespace prefix for the new sketch key.
/// - `segment_id`: New immutable segment identifier.
/// - `dim`: Current vector dimension.
/// - `old`: Validated resident sketch considered for reuse.
/// - `touched`: One flag per cluster; `true` means current vectors must be
///   re-encoded, `false` permits byte-for-byte carry-over.
/// - `cluster_vecs`: Current full vectors for every cluster. Untouched cluster
///   row counts must equal the old code ranges.
/// - `cluster_attrs`: Current attributes for touched clusters; untouched
///   presence bits come from `old`.
///
/// # Returns
///
/// [`ResidentSketchStitch::Stitched`] with fully owned metadata, bytes, and
/// resident state, or [`ResidentSketchStitch::Unavailable`] when dimensions,
/// subquantizers, cluster count, or persisted format prevent safe reuse.
///
/// # Errors
///
/// Returns an index or dimension error for empty/misaligned cluster state,
/// vector-count or byte-offset overflow, changed row count in an allegedly
/// untouched cluster, invalid touched-row dimension, an empty result, oversized
/// persisted counts, or failure to decode the newly assembled artifact.
///
/// # Consistency
///
/// No object is written here. Successful bytes still require immutable upload
/// and authoritative manifest publication. Returning `Unavailable` creates no
/// partial artifact and gives the compactor an explicit rebuild decision.
///
/// # Performance
///
/// Copies packed bytes for untouched rows and performs `O(touched_rows * dim *
/// 256)` codeword comparisons for changed rows. It reuses the old codebook
/// values rather than retraining them, then allocates a complete new artifact.
///
/// # Examples
///
/// If only cluster 3 changed, its rows are re-encoded against the old codebook
/// while all other cluster code spans are copied. If the new segment dimension
/// differs, the result is `Unavailable("dim_mismatch")`; no misleading mixed-
/// format sketch is produced.
///
/// # Rust Notes for Java/C Engineers
///
/// The function borrows every input but returns owned output. Boxing the
/// resident sketch keeps the successful enum variant's inline size smaller;
/// unlike a raw C pointer it has automatic destruction, and unlike an ordinary
/// Java reference its unique ownership can be moved without garbage collection.
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

/// Query-local evidence used to rank one non-empty logical cluster.
///
/// This type is `Copy`, so sorting and passing scores duplicates three scalar
/// fields without allocation or shared ownership.
#[derive(Clone, Copy)]
pub(crate) struct ClusterScore {
    /// Logical cluster index understood by the surrounding IVF segment.
    pub(crate) cluster_idx: usize,
    /// Mean ADC score of the cluster's best few rows; lower is better.
    pub(crate) aggregate_score: f32,
    /// Rows this cluster contributes to the global approximate top window.
    pub(crate) mass_count: usize,
}

/// Heap entry for one approximate row participating in global mass counting.
#[derive(Clone, Copy, Debug)]
struct SketchRowScore {
    /// Lower-is-better ADC score.
    score: f32,
    /// Cluster that owns the encoded row.
    cluster_idx: usize,
}

impl PartialEq for SketchRowScore {
    /// Compares exact float bit patterns and cluster indexes for heap consistency.
    ///
    /// Bit equality distinguishes representations such as positive and negative
    /// zero. Together with [`Ord::cmp`]'s total float order, this avoids the
    /// undefined NaN behavior of ordinary partial float comparison.
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.cluster_idx == other.cluster_idx
    }
}

impl Eq for SketchRowScore {}

impl PartialOrd for SketchRowScore {
    /// Adapts the type's total ordering to APIs expecting partial ordering.
    ///
    /// This always returns `Some` because [`SketchRowScore::cmp`] orders every
    /// IEEE-754 bit pattern, including NaN.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SketchRowScore {
    /// Orders worse (larger) scores first, then uses cluster index as a tie-break.
    ///
    /// Rust's `f32::total_cmp` resembles Java's `Float.compare`; C callers would
    /// need to define an explicit total order before using floats in a heap.
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.cluster_idx.cmp(&other.cluster_idx))
    }
}

/// Retains one row in a bounded max-heap of globally best approximate scores.
///
/// # Parameters
///
/// - `top_rows`: Heap whose root is the current worst retained row.
/// - `mass_top_k`: Maximum rows to retain; callers require a positive value.
/// - `row`: Newly scored row considered for the global window.
///
/// # Side Effects
///
/// Mutates the heap, inserting while under capacity or replacing the current
/// worst row when `row` is better. Equal scores keep the existing row.
///
/// # Performance
///
/// Costs `O(log mass_top_k)` only when the heap changes.
///
/// # Examples
///
/// With capacity two and retained scores `1.0` and `3.0`, inserting `2.0`
/// evicts `3.0`; inserting `4.0` changes nothing.
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

/// Fixed-size accumulator for a cluster's best row scores.
///
/// The array avoids heap allocation because the tie-break needs only
/// [`SKETCH_CLUSTER_SCORE_TOP_M`] values.
struct TopSketchScores {
    /// Ascending best scores, with unused slots initialized to infinity.
    scores: [f32; SKETCH_CLUSTER_SCORE_TOP_M],
    /// Number of initialized values in `scores`.
    len: usize,
}

impl TopSketchScores {
    /// Creates an empty best-score accumulator without allocation.
    fn new() -> Self {
        Self {
            scores: [f32::INFINITY; SKETCH_CLUSTER_SCORE_TOP_M],
            len: 0,
        }
    }

    /// Retains `score` when it belongs to the fixed-size best set.
    ///
    /// Once full, scores no better than the current worst retained value are
    /// ignored. For best-two storage, inserting `1`, `4`, then `2` leaves
    /// `[1, 2]`.
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

    /// Returns the arithmetic mean of all initialized best scores.
    ///
    /// # Panics
    ///
    /// Debug builds assert that at least one row was inserted. Production
    /// callers create this accumulator only for non-empty cluster ranges.
    fn mean(&self) -> f32 {
        debug_assert!(self.len > 0);
        self.scores[..self.len].iter().sum::<f32>() / self.len as f32
    }

    /// Restores ascending order after a value is appended or replaces the tail.
    ///
    /// `idx` must name an initialized array slot. The fixed two-element bound
    /// makes this effectively constant work.
    fn bubble_up(&mut self, mut idx: usize) {
        while idx > 0 && self.scores[idx] < self.scores[idx - 1] {
            self.scores.swap(idx, idx - 1);
            idx -= 1;
        }
    }
}

/// Chooses how many already-ranked clusters fit an adaptive budget.
///
/// The floor is always honored up to the available/capped cluster count. Beyond
/// it, consecutive clusters survive while their mass meets the cutoff derived
/// from the best rank.
///
/// # Parameters
///
/// - `ranked_clusters`: Best-first cluster scores from
///   [`ResidentSketch::rank_clusters`].
/// - `budget`: Previously validated adaptive policy.
///
/// # Returns
///
/// A prefix length no greater than either the ranked length or budget cap.
///
/// # Examples
///
/// A floor of two always retains the first two available scores. If the third
/// cluster remains within the mass margin and the fourth does not, returns 3.
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

/// Converts a relative mass margin into the minimum integer mass to retain.
///
/// # Parameters
///
/// - `best_mass`: Global top-row count owned by the highest-ranked cluster.
/// - `relative_margin`: Allowed fractional drop from that count. Normal callers
///   provide a finite non-negative value through [`AdaptiveClusterBudget`].
///
/// # Returns
///
/// A ceiling-rounded cutoff of at least one when `best_mass` is positive. Zero
/// best mass yields zero; margins of one or greater accept any positive mass.
///
/// # Examples
///
/// Best mass `10` with margin `0.20` returns `8`, so clusters contributing at
/// least eight global top rows remain eligible.
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

/// Selects deterministic, approximately even training positions across a corpus.
///
/// # Parameters
///
/// - `vector_count`: Total cluster-ordered rows.
/// - `sample_count`: Maximum positions requested.
///
/// # Returns
///
/// Every position when the request covers the corpus; otherwise exactly
/// `sample_count` monotonically increasing indexes spread by integer division.
///
/// # Examples
///
/// Sampling two positions from ten rows returns `[0, 5]`. No randomness or
/// allocation of vector values occurs here.
fn sample_indices(vector_count: usize, sample_count: usize) -> Vec<usize> {
    if sample_count >= vector_count {
        return (0..vector_count).collect();
    }
    (0..sample_count)
        .map(|i| i * vector_count / sample_count)
        .collect()
}

/// Maps a persisted sketch version to its codebook and packing interpretation.
///
/// # Returns
///
/// Version 2 maps to 16 four-bit codes; version 3 maps to 256 eight-bit codes.
///
/// # Errors
///
/// Returns an index error for every unsupported version so new formats cannot
/// be accidentally decoded with an old layout.
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

/// Encodes one vector chunk as the nearest codebook-centroid index.
///
/// # Parameters
///
/// - `codebook`: Flattened codebooks for all subquantizers.
/// - `dim`: Full vector dimension used to derive chunk widths.
/// - `subquantizers`: Number of chunks covering the full dimension.
/// - `codebook_size`: Centroids available for each chunk, at most 256.
/// - `subq`: Chunk whose codebook should be searched.
/// - `vector`: Borrowed values for exactly that chunk.
///
/// # Returns
///
/// One byte naming the nearest centroid by squared L2 distance. Equal distances
/// retain the lower code index.
///
/// # Panics
///
/// Debug builds assert chunk width and byte-sized codebook constraints. Invalid
/// flattened layouts can panic while slicing; callers construct layouts through
/// validated build/decode paths.
///
/// # Performance
///
/// Scans every codeword and allocates nothing.
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

/// Locates one codeword in the flattened variable-width codebook.
///
/// # Returns
///
/// Float offset for `(subq, code)`. Earlier chunk widths are accumulated because
/// dimensions that do not divide evenly can give chunks different sizes.
///
/// # Panics
///
/// Callers must provide in-range chunk and code indexes matching the codebook.
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

/// Divides a dimension into deterministic contiguous, near-equal chunks.
///
/// # Parameters
///
/// - `dim`: Full component count.
/// - `chunks`: Positive number of partitions, normally no greater than `dim`.
/// - `chunk`: Zero-based partition index smaller than `chunks`.
///
/// # Returns
///
/// Half-open `(start, end)` component indexes. Across all valid chunk indexes,
/// ranges are contiguous and cover `0..dim` exactly.
///
/// # Panics
///
/// `chunks == 0` divides by zero; callers enforce a positive subquantizer count.
fn chunk_range(dim: usize, chunks: usize, chunk: usize) -> (usize, usize) {
    let start = chunk * dim / chunks;
    let end = (chunk + 1) * dim / chunks;
    (start, end)
}

/// Returns the current write format's row stride for resident-memory tests.
///
/// For 64 current eight-bit subquantizers the result is 64 bytes.
#[cfg(test)]
fn packed_code_bytes(subquantizers: usize) -> usize {
    SKETCH_CODE_WIDTH.packed_code_bytes(subquantizers)
}

/// Writes one code into a caller-provided packed row according to its format.
///
/// # Panics
///
/// Panics when `bytes` is too short for `index`; four-bit values must also be
/// below 16 in debug builds. Valid layouts derive both from [`SketchFormat`].
fn pack_code(bytes: &mut [u8], index: usize, value: u8, code_width: SketchCodeWidth) {
    match code_width {
        SketchCodeWidth::FourBit => pack_nibble(bytes, index, value),
        SketchCodeWidth::EightBit => {
            bytes[index] = value;
        }
    }
}

/// Reads one code from a packed row according to its persisted format.
///
/// # Returns
///
/// A value in `0..16` for four-bit data or the stored byte for eight-bit data.
///
/// # Panics
///
/// Panics if the packed row is too short for `index`.
fn unpack_code(bytes: &[u8], index: usize, code_width: SketchCodeWidth) -> u8 {
    match code_width {
        SketchCodeWidth::FourBit => unpack_nibble(bytes, index),
        SketchCodeWidth::EightBit => bytes[index],
    }
}

/// Stores a four-bit value in the low or high half of its target byte.
///
/// Neighboring nibbles are preserved. Index 0 uses the low nibble, index 1 the
/// high nibble, and index 2 begins the next byte.
///
/// # Panics
///
/// Debug builds reject values above 15; any build panics for a short buffer.
fn pack_nibble(bytes: &mut [u8], index: usize, value: u8) {
    debug_assert!(value < 16);
    let slot = &mut bytes[index / 2];
    if index % 2 == 0 {
        *slot = (*slot & 0xF0) | value;
    } else {
        *slot = (*slot & 0x0F) | (value << 4);
    }
}

/// Extracts one low-then-high ordered four-bit code from packed bytes.
///
/// # Panics
///
/// Panics when the buffer does not contain the byte for `index`.
fn unpack_nibble(bytes: &[u8], index: usize) -> u8 {
    let value = bytes[index / 2];
    if index % 2 == 0 {
        value & 0x0F
    } else {
        value >> 4
    }
}

/// Computes squared L2 distance for equal-length vector chunks.
///
/// The implementation uses iterator zipping and therefore considers only the
/// shared prefix if lengths differ. Internal callers guarantee equal chunk
/// widths. The function allocates nothing.
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

/// Computes the dot product of equal-length vector chunks.
///
/// Internal callers guarantee equal widths; iterator zipping otherwise ignores
/// unmatched tails. The function allocates nothing.
#[inline]
fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum()
}

/// Returns bytes required to store one bit per item.
///
/// Eight bits require one byte and nine require two. Zero bits require no bytes.
fn bitset_len(bits: usize) -> usize {
    bits.div_ceil(8)
}

/// Marks one in-range bit in a mutable compact bitset.
///
/// # Panics
///
/// Panics when `bytes` does not contain the requested bit. Builders size the
/// buffer with [`bitset_len`] first.
fn set_bit(bytes: &mut [u8], bit: usize) {
    bytes[bit / 8] |= 1 << (bit % 8);
}

/// Tests a bit, returning `false` when the backing byte is absent.
///
/// The conservative out-of-range behavior exposed to query planning is handled
/// by [`ResidentSketch::cluster_has_attrs`], which returns `true` before calling
/// this helper on an absent cluster.
fn bit_is_set(bytes: &[u8], bit: usize) -> bool {
    bytes
        .get(bit / 8)
        .map(|byte| byte & (1 << (bit % 8)) != 0)
        .unwrap_or(false)
}

/// Reads a little-endian `u32` from a known fixed-field offset.
///
/// # Errors
///
/// Returns an index error labeled with `label` when four bytes are unavailable
/// or cannot be converted. The input is borrowed and no allocation occurs apart
/// from an error message on failure.
fn read_u32(data: &[u8], offset: usize, label: &str) -> Result<u32> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| ZeppelinError::Index(format!("{label} truncated")))?;
    Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
        ZeppelinError::Index(format!("{label} parse error"))
    })?))
}

/// Reads a little-endian `u64` from a known fixed-field offset.
///
/// # Errors
///
/// Returns an index error labeled with `label` when eight bytes are unavailable
/// or cannot be converted.
fn read_u64(data: &[u8], offset: usize, label: &str) -> Result<u64> {
    let bytes = data
        .get(offset..offset + 8)
        .ok_or_else(|| ZeppelinError::Index(format!("{label} truncated")))?;
    Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
        ZeppelinError::Index(format!("{label} parse error"))
    })?))
}

/// Reads one little-endian IEEE-754 `f32` from a fixed-field offset.
///
/// # Errors
///
/// Returns an index error labeled with `label` when four bytes are unavailable
/// or cannot be converted. Floating-point finiteness is a build-time input
/// invariant, not revalidated by this format reader.
fn read_f32(data: &[u8], offset: usize, label: &str) -> Result<f32> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| ZeppelinError::Index(format!("{label} truncated")))?;
    Ok(f32::from_le_bytes(bytes.try_into().map_err(|_| {
        ZeppelinError::Index(format!("{label} parse error"))
    })?))
}

/// Unit tests for format compatibility, stitching, ranking, and adaptive budgets.
#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a policy whose floor equals its cap, forcing an exact count.
    fn fixed_budget(cluster_budget: usize) -> AdaptiveClusterBudget {
        AdaptiveClusterBudget::new(cluster_budget, cluster_budget, 0.0)
    }

    /// Builds a one-row-per-cluster sketch with controlled squared-L2 scores.
    ///
    /// The helper turns each desired score into a one-dimensional codeword so
    /// selection tests can exercise ranking without invoking training.
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

    /// Build/decode preserves row layout and conservative attribute-presence bits.
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

    /// A changed dimension reports incompatibility instead of mixing codebooks.
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

    /// Cluster ranking prioritizes ownership of the global approximate top rows.
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

    /// Equal mass is resolved by the mean of each cluster's best row scores.
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

    /// The maximum current subquantizer count stays within the 64-byte row fence.
    #[test]
    fn sketch_code_bytes_stay_within_resident_fence() {
        assert_eq!(packed_code_bytes(SKETCH_MAX_SUBQUANTIZERS), 64);
    }

    /// Adaptive selection honors its floor and extends through the mass margin.
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

    /// Adaptive selection never exceeds the configured hard cap.
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

    /// A cap covering every probe preserves the caller's original cluster order.
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

    /// Legacy version-2 four-bit artifacts remain decodable and searchable.
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
