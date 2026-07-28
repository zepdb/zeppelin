//! Builds and searches the resident coarse sketch for an IVF-Flat segment.
//!
//! IVF first chooses `nprobe` nearby centroid clusters; larger `nprobe` usually
//! improves recall—the chance of finding the true nearest neighbors—at the cost
//! of more object reads and exact distance work. This module adds a smaller,
//! memory-resident selection stage inside that probe set. New artifacts store
//! one two-bit Extended-RaBitQ residual code per corpus vector, ordered by IVF
//! cluster. The residual is computed against the authoritative IVF centroid,
//! zero-padded to a 256-dimension block boundary, and transformed by one
//! persisted deterministic structured rotation shared by the artifact.
//!
//! At query time, asymmetric distance computation (ADC) compares the full-
//! precision query with compact two-plane row codes using popcount operations.
//! The sketch ranks clusters; the normal IVF reader still
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
//! rotate residuals once                 rotate query once
//!          |                                        |
//!          v                                        v
//! encode two bit planes + factors        popcount ADC in probe set
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
//! manifest. `decode_resident_sketch` validates bytes loaded either from the
//! sketch object or a segment bootstrap object against manifest metadata and
//! authoritative centroids.
//!
//! ## Reading map
//!
//! 1. `ResidentSketch` describes the decoded resident representation and
//!    its decoders define format compatibility.
//! 2. `build_resident_sketch` encodes a new v4 artifact.
//! 3. `ResidentSketch::rank_clusters` shows ADC scoring and cluster ranking;
//!    `ResidentSketch::select_clusters` applies `AdaptiveClusterBudget`.
//! 4. `stitch_resident_sketch` reuses compatible unchanged v4 row spans
//!    during bounded incremental compaction.
//! 5. The small helpers after `ClusterScore` define deterministic ranking,
//!    packing, chunk layout, and guarded binary reads.
//!
//! ## Invariants and compatibility
//!
//! - Rows and packed codes remain grouped in logical cluster order; cluster
//!   count metadata must partition exactly the declared vector count.
//! - New artifacts use version 4 with exactly two bit planes and two `f32`
//!   correction factors per residual row. Legacy PQ versions 2 and 3 remain
//!   readable but are never eligible for incremental stitching.
//! - A stitched artifact may copy an untouched cluster's old code bytes only
//!   when vector count, dimension, cluster layout, centroid, rotation seed,
//!   rotation scheme, and two-bit row width are compatible. Otherwise the
//!   caller receives an explicit unavailable reason.
//! - Existence in object storage does not make a sketch visible. The containing
//!   segment's manifest entry remains authoritative.
//! - Lower ADC scores are better. Dot product is negated so all supported
//!   metrics share that ordering.
//!
//! ## Rust concepts used here
//!
//! Borrowed slices let encoding and scoring inspect caller-owned vectors
//! without copying them. Owned `Vec` buffers hold reusable rotation/ADC scratch
//! and offsets, while [`Bytes`] holds immutable encoded rows. In Java these look broadly like
//! references, and in C like pointer/length pairs, but Rust distinguishes who
//! owns the allocation and proves borrowed views cannot outlive it. The
//! `ResidentSketchStitch` also makes “reuse succeeded” and “rebuild is
//! required” explicit states that callers must exhaustively match.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Result, ZeppelinError};
use crate::index::quantization::rabitq::{
    self, QueryAdc4, StructuredRotation, TwoBitFactors, BLOCK_DIM,
};
use crate::types::{AttributeValue, DistanceMetric};
use crate::wal::manifest::SketchRef;

/// Four-byte signature for coarse-sketch objects.
const SKETCH_MAGIC: &[u8; 4] = b"ZSK1";
/// Current write format: two-bit Extended-RaBitQ residual codes.
const SKETCH_VERSION: u32 = 4;
/// Legacy readable format using 256 one-byte PQ codes per subquantizer.
const SKETCH_V3_VERSION: u32 = 3;
/// Legacy readable format using packed four-bit codes.
const SKETCH_V2_VERSION: u32 = 2;
/// Number of codewords in each version-2 subquantizer codebook.
const SKETCH_V2_K: usize = 16;
/// Number of codewords in each version-3 subquantizer codebook.
const SKETCH_V3_K: usize = 256;
/// Best row scores averaged to break equal global-mass cluster ranks.
const SKETCH_CLUSTER_SCORE_TOP_M: usize = 2;
/// Bytes in the common v2/v3 binary header.
const LEGACY_SKETCH_HEADER_LEN: usize = 28;
/// Bytes in the v4 binary header, including rotation metadata.
const SKETCH_V4_HEADER_LEN: usize = 44;
/// Phase-1 structured-rotation seed promoted into the production format.
const SKETCH_ROTATION_SEED: u64 = 0x5a45_5050_454c_494e;
/// Structured sign/FHT/permutation rotation with zero-padding to 256 blocks.
const SKETCH_ROTATION_SCHEME_VERSION: u32 = 1;
/// ZSK1 v4 supports exactly the Phase-1 winning two-bit representation.
const SKETCH_BIT_WIDTH: u32 = 2;

/// Returns the global seed used by newly encoded resident sketches.
#[must_use]
pub(crate) const fn sketch_rotation_seed() -> u64 {
    SKETCH_ROTATION_SEED
}

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
    /// Each code occupies one byte; used by frozen version-3 artifacts.
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

/// Decoding parameters selected from a legacy artifact's persisted version.
#[derive(Debug, Clone, Copy)]
struct LegacySketchFormat {
    /// Number of learned centroid choices for each subquantizer.
    codebook_size: usize,
    /// On-disk width used to pack each centroid code.
    code_width: SketchCodeWidth,
}

/// Format-specific resident state hidden behind the sketch interface.
#[derive(Debug, Clone)]
enum ResidentEncoding {
    /// Read-only compatibility state for ZSK1 versions 2 and 3.
    LegacyPq {
        /// Number of contiguous chunks encoded independently.
        subquantizers: usize,
        /// Flattened subquantizer codebooks in chunk-major order.
        codebook: Vec<f32>,
        /// Number of centroid choices in each subquantizer.
        codebook_size: usize,
        /// Version-selected interpretation of packed codes.
        code_width: SketchCodeWidth,
    },
    /// Production ZSK1 v4 two-bit Extended-RaBitQ state.
    Rabitq2 {
        /// Padded rotation dimension stored in the header.
        code_dims: usize,
        /// Global deterministic rotation seed stored in header and manifest.
        rotation_seed: u64,
        /// Prepared structured rotation shared by query calls.
        rotation: Arc<StructuredRotation>,
        /// Centroids rotated once when the sketch is attached to an index.
        rotated_centroids: Option<Arc<Vec<Vec<f32>>>>,
    },
}

/// Validated in-memory coarse index loaded from an immutable segment artifact.
///
/// Rows are represented only by compact quantized codes and cluster boundaries; full
/// vectors and IDs remain in normal segment data. Cloning this type clones the
/// metadata and offsets and cheaply clones the reference-counted [`Bytes`] row
/// buffer.
#[derive(Debug, Clone)]
pub(crate) struct ResidentSketch {
    /// Persisted ZSK1 version that selected this encoding variant.
    version: u32,
    /// Full vector dimension expected from every query.
    dim: usize,
    /// Number of logical IVF clusters represented by offset ranges.
    cluster_count: usize,
    /// Half-open row range for each cluster in the packed code stream.
    cluster_offsets: Vec<(usize, usize)>,
    /// Immutable cluster-ordered row payloads.
    codes: Bytes,
    /// Conservative bit per cluster indicating any non-null row attributes.
    cluster_has_attrs: Vec<bool>,
    /// Encoded byte stride for one vector row, including v4 correction scalars.
    packed_code_bytes: usize,
    /// Exact complete-object size retained for warm-cache manifest validation.
    serialized_size: usize,
    /// Format-specific legacy-PQ or production-RaBitQ state.
    encoding: ResidentEncoding,
}

/// Query-local scorer selected once from a validated resident encoding.
enum ResidentQueryScorer<'a> {
    /// Legacy PQ lookup-table scoring for v2/v3 artifacts.
    Legacy {
        subquantizers: usize,
        codebook_size: usize,
        code_width: SketchCodeWidth,
        adc_table: Vec<f32>,
    },
    /// Production two-bit popcount ADC for v4 artifacts.
    Rabitq2(Box<Rabitq2QueryScorer<'a>>),
}

/// Reusable query and row scratch for allocation-free v4 inner-loop scoring.
struct Rabitq2QueryScorer<'a> {
    distance_metric: DistanceMetric,
    rotation_seed: u64,
    query_hash: u64,
    rotated_query: Vec<f32>,
    rotated_centroids: &'a [Vec<f32>],
    rotated_query_residual: Vec<f32>,
    low_plane: Vec<u64>,
    high_plane: Vec<u64>,
    query_adc: Option<QueryAdc4>,
    query_residual_norm_sq: f32,
    centroid_dot_query: f32,
}

impl ResidentQueryScorer<'_> {
    /// Prepares any cluster-specific ADC state before its contiguous row span.
    fn prepare_cluster(&mut self, cluster_idx: usize) -> Result<()> {
        let Self::Rabitq2(scorer) = self else {
            return Ok(());
        };
        scorer.prepare_cluster(cluster_idx)
    }

    /// Scores one validated packed row with the selected persisted encoding.
    fn score(&mut self, packed_codes: &[u8]) -> Result<f32> {
        let score = match self {
            Self::Legacy {
                subquantizers,
                codebook_size,
                code_width,
                adc_table,
            } => {
                let mut score = 0.0;
                for subq in 0..*subquantizers {
                    let code = unpack_code(packed_codes, subq, *code_width) as usize;
                    score += adc_table[subq * *codebook_size + code];
                }
                score
            }
            Self::Rabitq2(scorer) => scorer.score(packed_codes)?,
        };
        if !score.is_finite() {
            return Err(ZeppelinError::Index(format!(
                "coarse sketch produced a non-finite row score: {score}"
            )));
        }
        Ok(score)
    }
}

impl Rabitq2QueryScorer<'_> {
    /// Prepares deterministic popcount ADC state for one logical cluster.
    fn prepare_cluster(&mut self, cluster_idx: usize) -> Result<()> {
        let centroid = self.rotated_centroids.get(cluster_idx).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "coarse sketch missing rotated centroid {cluster_idx}"
            ))
        })?;
        self.centroid_dot_query = dot(&self.rotated_query, centroid);
        let query_seed = sketch_query_adc_seed(
            self.rotation_seed,
            self.query_hash,
            cluster_idx,
            0x5155_4552_595f_4144,
        );

        match self.distance_metric {
            DistanceMetric::Cosine | DistanceMetric::Euclidean => {
                for ((residual, query), center) in self
                    .rotated_query_residual
                    .iter_mut()
                    .zip(&self.rotated_query)
                    .zip(centroid)
                {
                    *residual = *query - *center;
                }
                self.query_residual_norm_sq = squared_norm(&self.rotated_query_residual);
                self.query_adc = Some(rabitq::prepare_query_adc4(
                    &self.rotated_query_residual,
                    query_seed,
                )?);
            }
            DistanceMetric::DotProduct => {
                self.query_residual_norm_sq = 0.0;
                self.query_adc = Some(rabitq::prepare_query_adc4(&self.rotated_query, query_seed)?);
            }
        }
        Ok(())
    }

    /// Decodes one row into scratch planes and evaluates its metric score.
    fn score(&mut self, packed_codes: &[u8]) -> Result<f32> {
        let words_per_code = self.low_plane.len();
        let plane_bytes = words_per_code * std::mem::size_of::<u64>();
        let expected = plane_bytes * 2 + 2 * std::mem::size_of::<f32>();
        if packed_codes.len() != expected {
            return Err(ZeppelinError::Index(format!(
                "coarse sketch row size mismatch: expected {expected}, got {}",
                packed_codes.len()
            )));
        }
        decode_plane_words(&packed_codes[..plane_bytes], &mut self.low_plane, "low")?;
        decode_plane_words(
            &packed_codes[plane_bytes..plane_bytes * 2],
            &mut self.high_plane,
            "high",
        )?;
        let factors = TwoBitFactors {
            residual_norm: f32::from_le_bytes(
                packed_codes[plane_bytes * 2..plane_bytes * 2 + 4]
                    .try_into()
                    .map_err(|_| ZeppelinError::Index("truncated residual norm".into()))?,
            ),
            bar_dot_residual: f32::from_le_bytes(
                packed_codes[plane_bytes * 2 + 4..expected]
                    .try_into()
                    .map_err(|_| ZeppelinError::Index("truncated correction scalar".into()))?,
            ),
        };
        let query_adc = self.query_adc.as_ref().ok_or_else(|| {
            ZeppelinError::Index("coarse sketch row scored before cluster preparation".into())
        })?;

        match self.distance_metric {
            DistanceMetric::Cosine | DistanceMetric::Euclidean => {
                Ok(rabitq::estimate_l2_two_bit_parts(
                    &self.low_plane,
                    &self.high_plane,
                    factors,
                    query_adc,
                    self.query_residual_norm_sq,
                )?)
            }
            DistanceMetric::DotProduct => {
                let residual_dot = rabitq::estimate_residual_dot_two_bit_parts(
                    &self.low_plane,
                    &self.high_plane,
                    factors,
                    query_adc,
                )?;
                Ok(-(self.centroid_dot_query + residual_dot))
            }
        }
    }
}

impl ResidentSketch {
    /// Reports whether row scores come from attached production v4 state.
    #[must_use]
    pub(crate) fn supports_row_frontier(&self) -> bool {
        self.version == SKETCH_VERSION
            && matches!(
                &self.encoding,
                ResidentEncoding::Rabitq2 {
                    rotated_centroids: Some(_),
                    ..
                }
            )
    }

    /// Decodes and validates one complete supported sketch for unit tests.
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
    /// An owned, validated [`ResidentSketch`]. Legacy codebooks are decoded into
    /// floats and row bytes are copied into immutable [`Bytes`], so the result
    /// does not borrow `data`.
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
    /// Performs one linear decode. It allocates cluster metadata, attribute
    /// flags, and a copy of the row section. Legacy artifacts also allocate a
    /// float codebook. It performs no object-store I/O.
    ///
    /// # Examples
    ///
    /// Versions 2 and 3 select their frozen PQ layouts; version 4 selects the
    /// two-plane RaBitQ layout. Appending one unexplained byte to any object
    /// causes an exact-size error instead of being ignored.
    #[cfg(test)]
    pub(crate) fn from_bytes(data: &[u8]) -> Result<Self> {
        Self::decode(data, None)
    }

    /// Decodes an owned object while sharing its immutable row payload bytes.
    pub(crate) fn from_owned_bytes(data: Bytes) -> Result<Self> {
        Self::decode(&data, Some(&data))
    }

    /// Dispatches one borrowed or reference-counted object by persisted version.
    fn decode(data: &[u8], owned: Option<&Bytes>) -> Result<Self> {
        if data.len() < 8 {
            return Err(ZeppelinError::Index(
                "coarse sketch blob too small for header".into(),
            ));
        }
        if !data.starts_with(SKETCH_MAGIC) {
            return Err(ZeppelinError::Index("coarse sketch magic mismatch".into()));
        }

        let version = read_u32(data, 4, "coarse sketch version")?;
        match version {
            SKETCH_V2_VERSION | SKETCH_V3_VERSION => Self::decode_legacy(data, owned, version),
            SKETCH_VERSION => Self::decode_v4(data, owned),
            _ => Err(ZeppelinError::Index(format!(
                "unsupported coarse sketch version: {version}"
            ))),
        }
    }

    /// Decodes the frozen version-2/version-3 PQ layouts.
    fn decode_legacy(data: &[u8], owned: Option<&Bytes>, version: u32) -> Result<Self> {
        if data.len() < LEGACY_SKETCH_HEADER_LEN {
            return Err(ZeppelinError::Index(
                "coarse sketch blob too small for legacy header".into(),
            ));
        }
        let format = legacy_sketch_format(version)?;
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

        let codebook_offset = LEGACY_SKETCH_HEADER_LEN;
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
            version,
            dim,
            cluster_count,
            cluster_offsets,
            codes: owned.map_or_else(
                || Bytes::copy_from_slice(&data[codes_offset..expected]),
                |bytes| bytes.slice(codes_offset..expected),
            ),
            cluster_has_attrs,
            packed_code_bytes,
            serialized_size: data.len(),
            encoding: ResidentEncoding::LegacyPq {
                subquantizers,
                codebook,
                codebook_size: format.codebook_size,
                code_width: format.code_width,
            },
        })
    }

    /// Decodes and validates the production version-4 two-bit layout.
    fn decode_v4(data: &[u8], owned: Option<&Bytes>) -> Result<Self> {
        if data.len() < SKETCH_V4_HEADER_LEN {
            return Err(ZeppelinError::Index(
                "coarse sketch blob too small for v4 header".into(),
            ));
        }
        let dim = read_u32(data, 8, "coarse sketch dim")? as usize;
        let code_dims = read_u32(data, 12, "coarse sketch code_dims")? as usize;
        let cluster_count = read_u32(data, 16, "coarse sketch cluster_count")? as usize;
        let vector_count = read_u64(data, 20, "coarse sketch vector_count")? as usize;
        let rotation_seed = read_u64(data, 28, "coarse sketch rotation seed")?;
        let rotation_scheme = read_u32(data, 36, "coarse sketch rotation scheme")?;
        let bit_width = read_u32(data, 40, "coarse sketch bit width")?;

        if dim == 0 {
            return Err(ZeppelinError::Index("coarse sketch dim is zero".into()));
        }
        let expected_code_dims = padded_code_dims(dim)?;
        if code_dims != expected_code_dims {
            return Err(ZeppelinError::Index(format!(
                "coarse sketch v4 code_dims mismatch: dim={dim}, expected={expected_code_dims}, got={code_dims}"
            )));
        }
        if cluster_count == 0 {
            return Err(ZeppelinError::Index(
                "coarse sketch cluster_count is zero".into(),
            ));
        }
        if rotation_scheme != SKETCH_ROTATION_SCHEME_VERSION {
            return Err(ZeppelinError::Index(format!(
                "unsupported coarse sketch rotation scheme: {rotation_scheme}"
            )));
        }
        if bit_width != SKETCH_BIT_WIDTH {
            return Err(ZeppelinError::Index(format!(
                "unsupported coarse sketch bit width: {bit_width}; expected {SKETCH_BIT_WIDTH}"
            )));
        }

        let attr_bitset_len = bitset_len(cluster_count);
        let counts_bytes = cluster_count.checked_mul(4).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "coarse sketch cluster counts overflows: clusters={cluster_count}"
            ))
        })?;
        let packed_code_bytes = rabitq_row_bytes(code_dims)?;
        let all_codes_bytes = vector_count.checked_mul(packed_code_bytes).ok_or_else(|| {
            ZeppelinError::Index(format!(
                "coarse sketch code section overflows: vectors={vector_count}, bytes_per_code={packed_code_bytes}"
            ))
        })?;
        let attr_offset = SKETCH_V4_HEADER_LEN;
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

        let mut cluster_has_attrs = vec![false; cluster_count];
        for (cluster_idx, has_attrs) in cluster_has_attrs.iter_mut().enumerate() {
            *has_attrs = bit_is_set(
                &data[attr_offset..attr_offset + attr_bitset_len],
                cluster_idx,
            );
        }
        let cluster_offsets =
            decode_cluster_offsets(data, counts_offset, cluster_count, vector_count)?;
        for row in 0..vector_count {
            let row_offset = codes_offset + row * packed_code_bytes;
            let residual_norm = read_f32(
                data,
                row_offset + packed_code_bytes - 8,
                "coarse sketch residual norm",
            )?;
            let bar_dot_residual = read_f32(
                data,
                row_offset + packed_code_bytes - 4,
                "coarse sketch correction scalar",
            )?;
            if !residual_norm.is_finite()
                || residual_norm < 0.0
                || !bar_dot_residual.is_finite()
                || bar_dot_residual < 0.0
                || ((residual_norm == 0.0) != (bar_dot_residual == 0.0))
            {
                return Err(ZeppelinError::Index(format!(
                    "coarse sketch row {row} has invalid correction scalars: residual_norm={residual_norm}, bar_dot_residual={bar_dot_residual}"
                )));
            }
        }

        let rotation = Arc::new(StructuredRotation::new(code_dims, rotation_seed)?);
        Ok(Self {
            version: SKETCH_VERSION,
            dim,
            cluster_count,
            cluster_offsets,
            codes: owned.map_or_else(
                || Bytes::copy_from_slice(&data[codes_offset..expected]),
                |bytes| bytes.slice(codes_offset..expected),
            ),
            cluster_has_attrs,
            packed_code_bytes,
            serialized_size: data.len(),
            encoding: ResidentEncoding::Rabitq2 {
                code_dims,
                rotation_seed,
                rotation,
                rotated_centroids: None,
            },
        })
    }

    /// Prepares immutable rotated centroids needed by v4 query scoring.
    pub(crate) fn with_centroids(mut self, centroids: &[Vec<f32>]) -> Result<Self> {
        self.validate_centroid_shape(centroids)?;
        let ResidentEncoding::Rabitq2 {
            code_dims,
            rotation,
            rotated_centroids,
            ..
        } = &mut self.encoding
        else {
            return Ok(self);
        };

        let mut prepared = Vec::with_capacity(centroids.len());
        let mut scratch = vec![0.0; *code_dims];
        for centroid in centroids {
            let mut padded = vec![0.0; *code_dims];
            padded[..self.dim].copy_from_slice(centroid);
            rotation.rotate_in_place(&mut padded, &mut scratch)?;
            prepared.push(padded);
        }
        *rotated_centroids = Some(Arc::new(prepared));
        Ok(self)
    }

    /// Validates centroid count and logical dimension without mutating state.
    pub(crate) fn validate_centroid_shape(&self, centroids: &[Vec<f32>]) -> Result<()> {
        if centroids.len() != self.cluster_count {
            return Err(ZeppelinError::Index(format!(
                "coarse sketch centroid count mismatch: expected {}, got {}",
                self.cluster_count,
                centroids.len()
            )));
        }
        for (cluster_idx, centroid) in centroids.iter().enumerate() {
            if centroid.len() != self.dim {
                return Err(ZeppelinError::Index(format!(
                    "coarse sketch centroid {cluster_idx} dimension mismatch: expected {}, got {}",
                    self.dim,
                    centroid.len()
                )));
            }
            if let Some((coordinate, value)) = centroid
                .iter()
                .enumerate()
                .find(|(_, value)| !value.is_finite())
            {
                return Err(ZeppelinError::Index(format!(
                    "coarse sketch centroid {cluster_idx} has non-finite coordinate {coordinate}: {value}"
                )));
            }
        }
        Ok(())
    }

    /// Validates manifest metadata against the decoded immutable artifact.
    pub(crate) fn validate_reference(&self, sketch_ref: &SketchRef) -> Result<()> {
        let (code_dims, rotation_seed) = match &self.encoding {
            ResidentEncoding::LegacyPq { subquantizers, .. } => (*subquantizers, None),
            ResidentEncoding::Rabitq2 {
                code_dims,
                rotation_seed,
                ..
            } => (*code_dims, Some(*rotation_seed)),
        };
        let serialized_size = u64::try_from(self.serialized_size).map_err(|_| {
            ZeppelinError::Index(format!(
                "coarse sketch serialized size does not fit u64: {}",
                self.serialized_size
            ))
        })?;
        if sketch_ref.version != self.version
            || sketch_ref.code_dims != code_dims
            || sketch_ref.bytes_per_vector != self.packed_code_bytes
            || sketch_ref.size_bytes != serialized_size
            || sketch_ref.rotation_seed != rotation_seed
        {
            return Err(ZeppelinError::Index(format!(
                "coarse sketch reference mismatch: manifest version/code_dims/bytes_per_vector/size_bytes/rotation_seed={}/{}/{}/{}/{:?}, object={}/{}/{}/{}/{:?}",
                sketch_ref.version,
                sketch_ref.code_dims,
                sketch_ref.bytes_per_vector,
                sketch_ref.size_bytes,
                sketch_ref.rotation_seed,
                self.version,
                code_dims,
                self.packed_code_bytes,
                serialized_size,
                rotation_seed
            )));
        }
        Ok(())
    }

    /// Validates the logical row count supplied by the authoritative manifest.
    pub(crate) fn validate_vector_count(&self, expected: usize) -> Result<()> {
        let actual = self
            .cluster_offsets
            .last()
            .map(|(_, end)| *end)
            .unwrap_or(0);
        if actual != expected {
            return Err(ZeppelinError::Index(format!(
                "coarse sketch vector count mismatch: manifest={expected}, object={actual}"
            )));
        }
        Ok(())
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
    /// V4 rotates the query once, prepares cluster-local popcount ADC state, and
    /// scans two bit planes per row. Legacy formats build a PQ ADC table. Both
    /// retain at most `mass_top_k` rows in a heap plus one score per cluster.
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
        Ok(self
            .rank_clusters_with_frontier(query, distance_metric, probe_clusters, mass_top_k, 0)?
            .ranked_clusters)
    }

    /// Scores one probe set once for cluster mass and an optional row frontier.
    ///
    /// Existing cluster selection consumes `ranked_clusters`; the fixed-stride
    /// rerank path consumes `row_frontier`. Both outputs share the same prepared
    /// query scorer and per-row ADC pass, so requesting row coordinates never
    /// scans the resident sketch a second time.
    pub(crate) fn rank_clusters_with_frontier(
        &self,
        query: &[f32],
        distance_metric: DistanceMetric,
        probe_clusters: &[usize],
        mass_top_k: usize,
        row_frontier_top_k: usize,
    ) -> Result<ResidentSketchScores> {
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

        let mut scorer = self.prepare_query_scorer(query, distance_metric)?;
        let centroid_rank: HashMap<usize, usize> = probe_clusters
            .iter()
            .copied()
            .enumerate()
            .map(|(rank, cluster_idx)| (cluster_idx, rank))
            .collect();
        let mut ranked_clusters = Vec::new();
        let mut row_scores = SketchRowAccumulator::new(mass_top_k, row_frontier_top_k);
        for &cluster_idx in probe_clusters {
            let (start, end) = self.cluster_offsets[cluster_idx];
            if start == end {
                continue;
            }

            scorer.prepare_cluster(cluster_idx)?;
            let mut top_scores = TopSketchScores::new();
            for row in start..end {
                let code_offset = row * self.packed_code_bytes;
                let codes = &self.codes[code_offset..code_offset + self.packed_code_bytes];
                let score = scorer.score(codes)?;
                top_scores.insert(score);
                row_scores.insert(ResidentRowScore {
                    approximate_score: score,
                    cluster_idx,
                    row_idx: row - start,
                });
            }
            ranked_clusters.push(ClusterScore {
                cluster_idx,
                aggregate_score: top_scores.mean(),
                mass_count: 0,
            });
        }

        let mut mass_counts = vec![0usize; self.cluster_count];
        for row in row_scores.mass_rows {
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

        let mut row_frontier = row_scores.frontier_rows.into_vec();
        row_frontier.sort();
        Ok(ResidentSketchScores {
            ranked_clusters,
            row_frontier,
        })
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
    fn prepare_query_scorer<'a>(
        &'a self,
        query: &[f32],
        distance_metric: DistanceMetric,
    ) -> Result<ResidentQueryScorer<'a>> {
        match &self.encoding {
            ResidentEncoding::LegacyPq {
                subquantizers,
                codebook,
                codebook_size,
                code_width,
            } => {
                let mut adc_table = vec![0.0f32; *subquantizers * *codebook_size];
                for subq in 0..*subquantizers {
                    let (start, end) = chunk_range(self.dim, *subquantizers, subq);
                    for code in 0..*codebook_size {
                        let centroid_offset =
                            codebook_offset(self.dim, *subquantizers, *codebook_size, subq, code);
                        let centroid = &codebook[centroid_offset..centroid_offset + (end - start)];
                        let query_chunk = &query[start..end];
                        adc_table[subq * *codebook_size + code] = match distance_metric {
                            DistanceMetric::DotProduct => -dot(query_chunk, centroid),
                            DistanceMetric::Cosine | DistanceMetric::Euclidean => {
                                sq_l2(query_chunk, centroid)
                            }
                        };
                    }
                }
                Ok(ResidentQueryScorer::Legacy {
                    subquantizers: *subquantizers,
                    codebook_size: *codebook_size,
                    code_width: *code_width,
                    adc_table,
                })
            }
            ResidentEncoding::Rabitq2 {
                code_dims,
                rotation_seed,
                rotation,
                rotated_centroids,
            } => {
                let rotated_centroids = rotated_centroids.as_deref().ok_or_else(|| {
                    ZeppelinError::Index(
                        "coarse sketch v4 is not attached to authoritative centroids".into(),
                    )
                })?;
                let mut rotated_query = vec![0.0; *code_dims];
                rotated_query[..self.dim].copy_from_slice(query);
                let mut scratch = vec![0.0; *code_dims];
                rotation.rotate_in_place(&mut rotated_query, &mut scratch)?;
                let words_per_code = rabitq::words_per_code(*code_dims)?;
                Ok(ResidentQueryScorer::Rabitq2(Box::new(Rabitq2QueryScorer {
                    distance_metric,
                    rotation_seed: *rotation_seed,
                    query_hash: stable_query_hash(query),
                    rotated_query,
                    rotated_centroids,
                    rotated_query_residual: vec![0.0; *code_dims],
                    low_plane: vec![0; words_per_code],
                    high_plane: vec![0; words_per_code],
                    query_adc: None,
                    query_residual_norm_sq: 0.0,
                    centroid_dot_query: 0.0,
                })))
            }
        }
    }
}

/// Decodes one manifest-selected sketch and cross-validates all external shape.
pub(crate) fn decode_resident_sketch(
    data: Bytes,
    sketch_ref: &SketchRef,
    centroids: &[Vec<f32>],
    expected_vector_count: usize,
) -> Result<ResidentSketch> {
    if sketch_ref.size_bytes != data.len() as u64 {
        return Err(ZeppelinError::Index(format!(
            "coarse sketch size mismatch for {}: manifest={}, object={}",
            sketch_ref.key,
            sketch_ref.size_bytes,
            data.len()
        )));
    }
    let sketch = ResidentSketch::from_owned_bytes(data)?;
    sketch.validate_reference(sketch_ref)?;
    sketch.validate_vector_count(expected_vector_count)?;
    sketch.with_centroids(centroids)
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

/// Encodes and serializes a new version-4 resident coarse sketch.
///
/// Each row is converted to a residual against its authoritative IVF centroid,
/// zero-padded to a 256-dimension boundary, transformed by the artifact's fixed
/// structured rotation, and encoded as two bit planes plus two correction
/// factors. There is no user-selectable production bit width.
///
/// # Parameters
///
/// - `namespace`: Namespace used to derive the future immutable object key.
/// - `segment_id`: Segment identifier that will own the artifact.
/// - `dim`: Required full vector dimension.
/// - `centroids`: Authoritative IVF centroid for every logical cluster.
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
/// Returns an index, quantization, or dimension error for zero clusters,
/// mismatched centroid/attribute shapes, no vectors, invalid row dimensions,
/// oversized persisted counts, invalid encoded output, or checked-arithmetic
/// failure.
///
/// # Consistency
///
/// The returned object is not visible merely because a caller uploads it. The
/// manifest must publish the returned [`SketchRef`] as part of the segment.
///
/// # Performance
///
/// Reuses one padded residual, one rotation scratch buffer, two bit planes, and
/// one order scratch vector across all rows. Encoded rows are written directly
/// into the final artifact allocation. No object-store I/O occurs here.
///
/// # Examples
///
/// A 768-dimensional row stores 192 bytes of bit planes and eight bytes of
/// factors, for a 200-byte row. Two clusters produce two contiguous row ranges
/// plus two attribute-presence bits.
///
/// # Rust Notes for Java/C Engineers
///
/// The encoder borrows vectors and centroids rather than duplicating the corpus.
/// The returned tuple moves three owned results to the caller; cloning the
/// resident row bytes only increments the [`Bytes`] reference count.
#[allow(clippy::type_complexity)]
pub(crate) fn build_resident_sketch(
    namespace: &str,
    segment_id: &str,
    dim: usize,
    centroids: &[Vec<f32>],
    cluster_vecs: &[Vec<Vec<f32>>],
    cluster_attrs: &[Vec<Option<HashMap<String, AttributeValue>>>],
) -> Result<(SketchRef, Bytes, ResidentSketch)> {
    let cluster_count = cluster_vecs.len();
    if cluster_count == 0 {
        return Err(ZeppelinError::Index(
            "cannot build coarse sketch with zero clusters".into(),
        ));
    }
    if centroids.len() != cluster_count {
        return Err(ZeppelinError::Index(format!(
            "coarse sketch centroid count mismatch: expected {cluster_count}, got {}",
            centroids.len()
        )));
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

    let code_dims = padded_code_dims(dim)?;
    let words_per_code = rabitq::words_per_code(code_dims)?;
    let packed_code_bytes = rabitq_row_bytes(code_dims)?;
    let dim_u32 = u32::try_from(dim)
        .map_err(|_| ZeppelinError::Index(format!("coarse sketch dim exceeds u32: {dim}")))?;
    let code_dims_u32 = u32::try_from(code_dims).map_err(|_| {
        ZeppelinError::Index(format!("coarse sketch code_dims exceeds u32: {code_dims}"))
    })?;
    let cluster_count_u32 = u32::try_from(cluster_count).map_err(|_| {
        ZeppelinError::Index(format!(
            "coarse sketch cluster count exceeds u32: {cluster_count}"
        ))
    })?;
    let vector_count_u64 = u64::try_from(vector_count).map_err(|_| {
        ZeppelinError::Index(format!(
            "coarse sketch vector count exceeds u64: {vector_count}"
        ))
    })?;

    let mut attr_bits = vec![0u8; bitset_len(cluster_count)];
    for (cluster_idx, attrs) in cluster_attrs.iter().enumerate() {
        if attrs.iter().any(Option::is_some) {
            set_bit(&mut attr_bits, cluster_idx);
        }
    }

    let counts_bytes = cluster_count
        .checked_mul(4)
        .ok_or_else(|| ZeppelinError::Index("coarse sketch cluster-count bytes overflow".into()))?;
    let code_bytes = vector_count
        .checked_mul(packed_code_bytes)
        .ok_or_else(|| ZeppelinError::Index("coarse sketch row payload bytes overflow".into()))?;
    let total = SKETCH_V4_HEADER_LEN
        .checked_add(attr_bits.len())
        .and_then(|size| size.checked_add(counts_bytes))
        .and_then(|size| size.checked_add(code_bytes))
        .ok_or_else(|| ZeppelinError::Index("coarse sketch total size overflows".into()))?;
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(SKETCH_MAGIC);
    buf.extend_from_slice(&SKETCH_VERSION.to_le_bytes());
    buf.extend_from_slice(&dim_u32.to_le_bytes());
    buf.extend_from_slice(&code_dims_u32.to_le_bytes());
    buf.extend_from_slice(&cluster_count_u32.to_le_bytes());
    buf.extend_from_slice(&vector_count_u64.to_le_bytes());
    buf.extend_from_slice(&SKETCH_ROTATION_SEED.to_le_bytes());
    buf.extend_from_slice(&SKETCH_ROTATION_SCHEME_VERSION.to_le_bytes());
    buf.extend_from_slice(&SKETCH_BIT_WIDTH.to_le_bytes());
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

    let rotation = StructuredRotation::new(code_dims, SKETCH_ROTATION_SEED)?;
    let mut rotated = vec![0.0; code_dims];
    let mut rotation_scratch = vec![0.0; code_dims];
    let mut low_plane = vec![0u64; words_per_code];
    let mut high_plane = vec![0u64; words_per_code];
    let mut order_scratch = vec![0usize; code_dims];
    for (cluster_idx, cluster) in cluster_vecs.iter().enumerate() {
        let centroid = &centroids[cluster_idx];
        if centroid.len() != dim {
            return Err(ZeppelinError::DimensionMismatch {
                expected: dim,
                actual: centroid.len(),
            });
        }
        for vector in cluster {
            if vector.len() != dim {
                return Err(ZeppelinError::DimensionMismatch {
                    expected: dim,
                    actual: vector.len(),
                });
            }
            rotated.fill(0.0);
            for ((output, value), center) in rotated[..dim].iter_mut().zip(vector).zip(centroid) {
                *output = *value - *center;
            }
            rotation.rotate_in_place(&mut rotated, &mut rotation_scratch)?;
            let factors = rabitq::encode_two_bit_into(
                &rotated,
                &mut low_plane,
                &mut high_plane,
                &mut order_scratch,
            )?;
            for word in &low_plane {
                buf.extend_from_slice(&word.to_le_bytes());
            }
            for word in &high_plane {
                buf.extend_from_slice(&word.to_le_bytes());
            }
            buf.extend_from_slice(&factors.residual_norm.to_le_bytes());
            buf.extend_from_slice(&factors.bar_dot_residual.to_le_bytes());
        }
    }
    debug_assert_eq!(buf.len(), total);

    let key = sketch_key(namespace, segment_id);
    let bytes = Bytes::from(buf);
    let sketch_ref = SketchRef {
        key,
        version: SKETCH_VERSION,
        code_dims,
        bytes_per_vector: packed_code_bytes,
        size_bytes: bytes.len() as u64,
        rotation_seed: Some(SKETCH_ROTATION_SEED),
    };
    let resident = decode_resident_sketch(bytes.clone(), &sketch_ref, centroids, vector_count)?;
    Ok((sketch_ref, bytes, resident))
}

/// Outcome of attempting rotation-compatible incremental sketch construction.
///
/// Incompatibility is distinct from corruption: it tells the compactor that a
/// full rebuild is required and names the reason, while a `Result::Err` from
/// [`stitch_resident_sketch`] means the requested construction itself was
/// invalid or corrupt.
pub(crate) enum ResidentSketchStitch {
    /// New manifest metadata, complete bytes, and decoded resident state built
    /// with compatible old rotation metadata and centroids.
    Stitched(SketchRef, Bytes, Box<ResidentSketch>),
    /// Stable reason code explaining why the caller must rebuild from vectors.
    Unavailable(&'static str),
}

/// Build a new resident sketch by reusing compatible carried v4 rows.
///
/// Untouched clusters copy their old code sections byte-for-byte. Touched
/// clusters are re-encoded against their current authoritative centroids using
/// the same rotation scheme and seed. Legacy artifacts and incompatible v4
/// metadata require an explicit full rebuild.
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
/// new immutable version-4 sketch bytes
/// ```
///
/// # Parameters
///
/// - `namespace`: Namespace prefix for the new sketch key.
/// - `segment_id`: New immutable segment identifier.
/// - `dim`: Current vector dimension.
/// - `centroids`: Current authoritative IVF centroids. Untouched centroids must
///   rotate bit-for-bit identically to those bound to `old`.
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
/// centroids, cluster count, seed, scheme, row width, or persisted format
/// prevent safe reuse.
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
/// Copies complete row spans for untouched rows and rotates/encodes only touched
/// rows, then allocates a complete new immutable artifact.
///
/// # Examples
///
/// If only cluster 3 changed, its rows are re-encoded against its authoritative
/// centroid while all other full row spans are copied. If the segment dimension
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
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub(crate) fn stitch_resident_sketch(
    namespace: &str,
    segment_id: &str,
    dim: usize,
    centroids: &[Vec<f32>],
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
    if centroids.len() != cluster_count {
        return Err(ZeppelinError::Index(format!(
            "coarse sketch centroid count mismatch: expected {cluster_count}, got {}",
            centroids.len()
        )));
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

    if old.dim != dim {
        return Ok(ResidentSketchStitch::Unavailable("dim_mismatch"));
    }
    if old.cluster_count != cluster_count {
        return Ok(ResidentSketchStitch::Unavailable("cluster_count_mismatch"));
    }
    let expected_code_dims = padded_code_dims(dim)?;
    let (old_code_dims, old_rotation_seed, old_rotation, old_rotated_centroids) =
        match &old.encoding {
            ResidentEncoding::LegacyPq { .. } => {
                return Ok(ResidentSketchStitch::Unavailable("format_mismatch"));
            }
            ResidentEncoding::Rabitq2 {
                code_dims,
                rotation_seed,
                rotation,
                rotated_centroids,
                ..
            } => (
                *code_dims,
                *rotation_seed,
                Arc::clone(rotation),
                rotated_centroids.as_ref().map(Arc::clone),
            ),
        };
    if old_code_dims != expected_code_dims {
        return Ok(ResidentSketchStitch::Unavailable("code_dims_mismatch"));
    }
    if old_rotation_seed != SKETCH_ROTATION_SEED {
        return Ok(ResidentSketchStitch::Unavailable("rotation_seed_mismatch"));
    }
    let expected_row_bytes = rabitq_row_bytes(old_code_dims)?;
    if old.packed_code_bytes != expected_row_bytes {
        return Ok(ResidentSketchStitch::Unavailable("row_width_mismatch"));
    }
    let Some(old_rotated_centroids) = old_rotated_centroids else {
        return Ok(ResidentSketchStitch::Unavailable("centroids_unprepared"));
    };
    let mut centroid_scratch = vec![0.0; old_code_dims];
    for (cluster_idx, centroid) in centroids.iter().enumerate() {
        if centroid.len() != dim {
            return Err(ZeppelinError::DimensionMismatch {
                expected: dim,
                actual: centroid.len(),
            });
        }
        if touched[cluster_idx] {
            continue;
        }
        let mut rotated = vec![0.0; old_code_dims];
        rotated[..dim].copy_from_slice(centroid);
        old_rotation.rotate_in_place(&mut rotated, &mut centroid_scratch)?;
        let unchanged = old_rotated_centroids[cluster_idx]
            .iter()
            .zip(&rotated)
            .all(|(old, current)| old.to_bits() == current.to_bits());
        if !unchanged {
            return Ok(ResidentSketchStitch::Unavailable("centroid_mismatch"));
        }
    }

    let mut counts = Vec::with_capacity(cluster_count);
    let mut vector_count = 0usize;
    let mut attr_bits = vec![0u8; bitset_len(cluster_count)];
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
    }

    if vector_count == 0 {
        return Err(ZeppelinError::Index(
            "cannot stitch coarse sketch from empty vector set".into(),
        ));
    }

    let codes_len = vector_count
        .checked_mul(old.packed_code_bytes)
        .ok_or_else(|| ZeppelinError::Index("coarse sketch code bytes overflow".into()))?;
    let total = SKETCH_V4_HEADER_LEN
        .checked_add(attr_bits.len())
        .and_then(|size| size.checked_add(cluster_count.checked_mul(4)?))
        .and_then(|size| size.checked_add(codes_len))
        .ok_or_else(|| ZeppelinError::Index("coarse sketch total size overflows".into()))?;
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(SKETCH_MAGIC);
    buf.extend_from_slice(&SKETCH_VERSION.to_le_bytes());
    buf.extend_from_slice(
        &u32::try_from(dim)
            .map_err(|_| ZeppelinError::Index("coarse sketch dim exceeds u32".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(
        &u32::try_from(old_code_dims)
            .map_err(|_| ZeppelinError::Index("coarse sketch code_dims exceeds u32".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(
        &u32::try_from(cluster_count)
            .map_err(|_| ZeppelinError::Index("coarse sketch clusters exceed u32".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(
        &u64::try_from(vector_count)
            .map_err(|_| ZeppelinError::Index("coarse sketch rows exceed u64".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(&SKETCH_ROTATION_SEED.to_le_bytes());
    buf.extend_from_slice(&SKETCH_ROTATION_SCHEME_VERSION.to_le_bytes());
    buf.extend_from_slice(&SKETCH_BIT_WIDTH.to_le_bytes());
    buf.extend_from_slice(&attr_bits);
    for &count in &counts {
        let persisted = u32::try_from(count).map_err(|_| {
            ZeppelinError::Index(format!(
                "coarse sketch cluster has too many vectors: {count}"
            ))
        })?;
        buf.extend_from_slice(&persisted.to_le_bytes());
    }

    let rotation = StructuredRotation::new(old_code_dims, SKETCH_ROTATION_SEED)?;
    let words_per_code = rabitq::words_per_code(old_code_dims)?;
    let mut rotated = vec![0.0; old_code_dims];
    let mut rotation_scratch = vec![0.0; old_code_dims];
    let mut low_plane = vec![0u64; words_per_code];
    let mut high_plane = vec![0u64; words_per_code];
    let mut order_scratch = vec![0usize; old_code_dims];
    for cluster_idx in 0..cluster_count {
        if !touched[cluster_idx] {
            let (row_start, row_end) = old.cluster_offsets[cluster_idx];
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
            buf.extend_from_slice(&old.codes[byte_start..byte_end]);
            continue;
        }

        let centroid = &centroids[cluster_idx];
        if centroid.len() != dim {
            return Err(ZeppelinError::DimensionMismatch {
                expected: dim,
                actual: centroid.len(),
            });
        }
        for vector in &cluster_vecs[cluster_idx] {
            if vector.len() != dim {
                return Err(ZeppelinError::DimensionMismatch {
                    expected: dim,
                    actual: vector.len(),
                });
            }
            rotated.fill(0.0);
            for ((output, value), center) in rotated[..dim].iter_mut().zip(vector).zip(centroid) {
                *output = *value - *center;
            }
            rotation.rotate_in_place(&mut rotated, &mut rotation_scratch)?;
            let factors = rabitq::encode_two_bit_into(
                &rotated,
                &mut low_plane,
                &mut high_plane,
                &mut order_scratch,
            )?;
            for word in &low_plane {
                buf.extend_from_slice(&word.to_le_bytes());
            }
            for word in &high_plane {
                buf.extend_from_slice(&word.to_le_bytes());
            }
            buf.extend_from_slice(&factors.residual_norm.to_le_bytes());
            buf.extend_from_slice(&factors.bar_dot_residual.to_le_bytes());
        }
    }
    debug_assert_eq!(buf.len(), total);

    let key = sketch_key(namespace, segment_id);
    let bytes = Bytes::from(buf);
    let sketch_ref = SketchRef {
        key,
        version: SKETCH_VERSION,
        code_dims: old_code_dims,
        bytes_per_vector: old.packed_code_bytes,
        size_bytes: bytes.len() as u64,
        rotation_seed: Some(SKETCH_ROTATION_SEED),
    };
    let resident = decode_resident_sketch(bytes.clone(), &sketch_ref, centroids, vector_count)?;
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

/// Outputs derived from one shared resident-sketch row scan.
pub(crate) struct ResidentSketchScores {
    /// Existing cluster-mass ranking, sorted by coarse preference.
    pub(crate) ranked_clusters: Vec<ClusterScore>,
    /// Globally best requested rows, sorted by deterministic coarse preference.
    pub(crate) row_frontier: Vec<ResidentRowScore>,
}

/// Owned approximate row coordinate retained from the resident sketch.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ResidentRowScore {
    /// Lower-is-better ADC score.
    pub(crate) approximate_score: f32,
    /// Cluster that owns the encoded row.
    pub(crate) cluster_idx: usize,
    /// Zero-based row position within that cluster.
    pub(crate) row_idx: usize,
}

impl PartialEq for ResidentRowScore {
    /// Compares exact float bit patterns and cluster indexes for heap consistency.
    ///
    /// Bit equality distinguishes representations such as positive and negative
    /// zero. Together with [`Ord::cmp`]'s total float order, this avoids the
    /// undefined NaN behavior of ordinary partial float comparison.
    fn eq(&self, other: &Self) -> bool {
        self.approximate_score.to_bits() == other.approximate_score.to_bits()
            && self.cluster_idx == other.cluster_idx
            && self.row_idx == other.row_idx
    }
}

impl Eq for ResidentRowScore {}

impl PartialOrd for ResidentRowScore {
    /// Adapts the type's total ordering to APIs expecting partial ordering.
    ///
    /// This always returns `Some` because [`ResidentRowScore::cmp`] orders every
    /// IEEE-754 bit pattern, including NaN.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ResidentRowScore {
    /// Orders worse scores first, then uses stable row coordinates as tie-breaks.
    ///
    /// Rust's `f32::total_cmp` resembles Java's `Float.compare`; C callers would
    /// need to define an explicit total order before using floats in a heap.
    fn cmp(&self, other: &Self) -> Ordering {
        self.approximate_score
            .total_cmp(&other.approximate_score)
            .then_with(|| self.cluster_idx.cmp(&other.cluster_idx))
            .then_with(|| self.row_idx.cmp(&other.row_idx))
    }
}

/// Shared accumulator fed by the single resident-sketch ADC row loop.
struct SketchRowAccumulator {
    /// Historical mass window used by cluster ranking.
    mass_rows: BinaryHeap<ResidentRowScore>,
    /// Optional larger row window used by fixed-stride exact rerank.
    frontier_rows: BinaryHeap<ResidentRowScore>,
    /// Maximum number of rows retained for cluster mass.
    mass_top_k: usize,
    /// Maximum number of explicit row coordinates returned to search.
    frontier_top_k: usize,
}

impl SketchRowAccumulator {
    /// Allocates the two bounded views populated from each score exactly once.
    fn new(mass_top_k: usize, frontier_top_k: usize) -> Self {
        Self {
            mass_rows: BinaryHeap::with_capacity(mass_top_k),
            frontier_rows: BinaryHeap::with_capacity(frontier_top_k),
            mass_top_k,
            frontier_top_k,
        }
    }

    /// Feeds one row into cluster-mass and frontier views without rescoring it.
    fn insert(&mut self, row: ResidentRowScore) {
        insert_top_mass_row(&mut self.mass_rows, self.mass_top_k, row);
        insert_top_frontier_row(&mut self.frontier_rows, self.frontier_top_k, row);
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
    top_rows: &mut BinaryHeap<ResidentRowScore>,
    mass_top_k: usize,
    row: ResidentRowScore,
) {
    if top_rows.len() < mass_top_k {
        top_rows.push(row);
        return;
    }

    let Some(&worst) = top_rows.peek() else {
        top_rows.push(row);
        return;
    };
    if row
        .approximate_score
        .total_cmp(&worst.approximate_score)
        .is_lt()
    {
        top_rows.pop();
        top_rows.push(row);
    }
}

/// Retains a deterministic globally best row frontier.
fn insert_top_frontier_row(
    top_rows: &mut BinaryHeap<ResidentRowScore>,
    frontier_top_k: usize,
    row: ResidentRowScore,
) {
    if frontier_top_k == 0 {
        return;
    }
    if top_rows.len() < frontier_top_k {
        top_rows.push(row);
        return;
    }
    let Some(&worst) = top_rows.peek() else {
        top_rows.push(row);
        return;
    };
    if row.cmp(&worst).is_lt() {
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
fn legacy_sketch_format(version: u32) -> Result<LegacySketchFormat> {
    match version {
        SKETCH_V2_VERSION => Ok(LegacySketchFormat {
            codebook_size: SKETCH_V2_K,
            code_width: SketchCodeWidth::FourBit,
        }),
        SKETCH_V3_VERSION => Ok(LegacySketchFormat {
            codebook_size: SKETCH_V3_K,
            code_width: SketchCodeWidth::EightBit,
        }),
        _ => Err(ZeppelinError::Index(format!(
            "unsupported coarse sketch version: {version}"
        ))),
    }
}

/// Rounds a logical dimension up to complete structured-rotation blocks.
fn padded_code_dims(dim: usize) -> Result<usize> {
    if dim == 0 {
        return Err(ZeppelinError::Index("coarse sketch dim is zero".into()));
    }
    dim.checked_add(BLOCK_DIM - 1)
        .map(|value| value / BLOCK_DIM)
        .and_then(|blocks| blocks.checked_mul(BLOCK_DIM))
        .ok_or_else(|| ZeppelinError::Index(format!("coarse sketch dim overflows: {dim}")))
}

/// Returns both bit planes plus two f32 correction scalars per v4 row.
fn rabitq_row_bytes(code_dims: usize) -> Result<usize> {
    let plane_bytes = rabitq::words_per_code(code_dims)?
        .checked_mul(std::mem::size_of::<u64>())
        .ok_or_else(|| ZeppelinError::Index("coarse sketch bit plane size overflows".into()))?;
    plane_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<f32>()))
        .ok_or_else(|| ZeppelinError::Index("coarse sketch row size overflows".into()))
}

/// Decodes cluster counts and proves they partition the declared rows exactly.
fn decode_cluster_offsets(
    data: &[u8],
    counts_offset: usize,
    cluster_count: usize,
    vector_count: usize,
) -> Result<Vec<(usize, usize)>> {
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
    Ok(cluster_offsets)
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

/// Writes one code into a caller-provided packed row according to its format.
///
/// # Panics
///
/// Panics when `bytes` is too short for `index`; four-bit values must also be
/// below 16 in debug builds. Valid test layouts derive both from the selected
/// [`SketchCodeWidth`].
#[cfg(test)]
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
#[cfg(test)]
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

/// Computes a finite vector's squared Euclidean norm.
fn squared_norm(values: &[f32]) -> f32 {
    values.iter().map(|value| value * value).sum()
}

/// Decodes one little-endian persisted bit plane into reusable word scratch.
fn decode_plane_words(data: &[u8], output: &mut [u64], label: &str) -> Result<()> {
    let expected = output
        .len()
        .checked_mul(std::mem::size_of::<u64>())
        .ok_or_else(|| ZeppelinError::Index(format!("coarse sketch {label} plane overflows")))?;
    if data.len() != expected {
        return Err(ZeppelinError::Index(format!(
            "coarse sketch {label} plane size mismatch: expected {expected}, got {}",
            data.len()
        )));
    }
    for (word, bytes) in output.iter_mut().zip(data.chunks_exact(8)) {
        *word = u64::from_le_bytes(bytes.try_into().map_err(|_| {
            ZeppelinError::Index(format!("coarse sketch {label} plane word is truncated"))
        })?);
    }
    Ok(())
}

/// Hashes exact query f32 bits without process-randomized hash state.
fn stable_query_hash(query: &[f32]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for value in query {
        hash ^= u64::from(value.to_bits());
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Derives deterministic ADC dithering from persisted and query-local state.
fn sketch_query_adc_seed(
    rotation_seed: u64,
    query_hash: u64,
    cluster_idx: usize,
    purpose: u64,
) -> u64 {
    mix64(
        rotation_seed
            ^ query_hash
            ^ (cluster_idx as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
            ^ purpose,
    )
}

/// SplitMix64 finalizer used only for deterministic query seed derivation.
fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
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
/// invariant for legacy layouts; the v4 decoder revalidates persisted factors.
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

    /// Newly built sketches pin the production two-bit RaBitQ format contract.
    #[test]
    fn new_sketches_are_v4_two_bit_rabitq() {
        let mut left = vec![0.0; 256];
        left[0] = 1.0;
        let mut right = vec![0.0; 256];
        right[1] = 1.0;
        let clusters = vec![vec![left], vec![right]];
        let mut left_centroid = vec![0.0; 256];
        left_centroid[0] = 0.25;
        let mut right_centroid = vec![0.0; 256];
        right_centroid[1] = 0.25;
        let centroids = vec![left_centroid, right_centroid];
        let attrs = vec![vec![None], vec![None]];

        let (sketch_ref, bytes, _) =
            build_resident_sketch("ns", "seg", 256, &centroids, &clusters, &attrs).unwrap();

        assert_eq!(sketch_ref.version, 4);
        assert_eq!(sketch_ref.code_dims, 256);
        assert_eq!(sketch_ref.bytes_per_vector, 72);
        assert_eq!(sketch_ref.rotation_seed, Some(0x5a45_5050_454c_494e));
        assert_eq!(
            u64::from_le_bytes(bytes[28..36].try_into().unwrap()),
            0x5a45_5050_454c_494e
        );
        assert_eq!(u32::from_le_bytes(bytes[36..40].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(bytes[40..44].try_into().unwrap()), 2);
    }

    /// Identical inputs and the fixed seed produce byte-identical v4 objects.
    #[test]
    fn v4_build_is_byte_deterministic() {
        let centroids = vec![vec![0.25, 0.0, 0.0], vec![0.0, 0.25, 0.0]];
        let clusters = vec![
            vec![vec![1.0, 0.0, 0.0], vec![0.9, 0.1, 0.0]],
            vec![vec![0.0, 1.0, 0.0]],
        ];
        let attrs = vec![vec![None, None], vec![None]];

        let (_, first, _) =
            build_resident_sketch("ns", "seg-a", 3, &centroids, &clusters, &attrs).unwrap();
        let (_, second, _) =
            build_resident_sketch("ns", "seg-b", 3, &centroids, &clusters, &attrs).unwrap();

        assert_eq!(first, second);
    }

    /// V4's bit-width field is descriptive, not a user-selectable decoder knob.
    #[test]
    fn v4_loader_rejects_non_two_bit_width() {
        let centroids = vec![vec![0.0, 0.0]];
        let clusters = vec![vec![vec![1.0, 0.0]]];
        let attrs = vec![vec![None]];
        let (_, bytes, _) =
            build_resident_sketch("ns", "seg", 2, &centroids, &clusters, &attrs).unwrap();
        let mut corrupt = bytes.to_vec();
        corrupt[40..44].copy_from_slice(&1u32.to_le_bytes());

        let error = ResidentSketch::from_bytes(&corrupt).unwrap_err();
        assert!(error.to_string().contains("bit width"));
    }

    /// Unknown rotation schemes fail instead of reinterpreting persisted rows.
    #[test]
    fn v4_loader_rejects_unknown_rotation_scheme() {
        let centroids = vec![vec![0.0, 0.0]];
        let clusters = vec![vec![vec![1.0, 0.0]]];
        let attrs = vec![vec![None]];
        let (_, bytes, _) =
            build_resident_sketch("ns", "seg", 2, &centroids, &clusters, &attrs).unwrap();
        let mut corrupt = bytes.to_vec();
        corrupt[36..40].copy_from_slice(&2u32.to_le_bytes());

        let error = ResidentSketch::from_bytes(&corrupt).unwrap_err();
        assert!(error.to_string().contains("rotation scheme"));
    }

    /// Non-finite v4 correction factors are corruption, never scan fallbacks.
    #[test]
    fn v4_loader_rejects_nonfinite_correction_scalar() {
        let centroids = vec![vec![0.0, 0.0]];
        let clusters = vec![vec![vec![1.0, 0.0]]];
        let attrs = vec![vec![None]];
        let (_, bytes, _) =
            build_resident_sketch("ns", "seg", 2, &centroids, &clusters, &attrs).unwrap();
        let mut corrupt = bytes.to_vec();
        let row_offset = SKETCH_V4_HEADER_LEN + bitset_len(1) + 4;
        let scalar_offset = row_offset + rabitq_row_bytes(256).unwrap() - 4;
        corrupt[scalar_offset..scalar_offset + 4].copy_from_slice(&f32::NAN.to_le_bytes());

        let error = ResidentSketch::from_bytes(&corrupt).unwrap_err();
        assert!(error.to_string().contains("correction scalars"));
    }

    /// Zero residual norm and correction scalar must occur together.
    #[test]
    fn v4_loader_rejects_inconsistent_zero_correction_factors() {
        let centroids = vec![vec![0.0, 0.0]];
        let clusters = vec![vec![vec![1.0, 0.0]]];
        let attrs = vec![vec![None]];
        let (_, bytes, _) =
            build_resident_sketch("ns", "seg", 2, &centroids, &clusters, &attrs).unwrap();
        let mut corrupt = bytes.to_vec();
        let row_offset = SKETCH_V4_HEADER_LEN + bitset_len(1) + 4;
        let norm_offset = row_offset + rabitq_row_bytes(256).unwrap() - 8;
        corrupt[norm_offset..norm_offset + 4].copy_from_slice(&0.0f32.to_le_bytes());

        let error = ResidentSketch::from_bytes(&corrupt).unwrap_err();
        assert!(error.to_string().contains("correction scalars"));
    }

    /// Exact-size framing rejects incomplete v4 rows instead of scanning around them.
    #[test]
    fn v4_loader_rejects_truncated_row() {
        let centroids = vec![vec![0.0, 0.0]];
        let clusters = vec![vec![vec![1.0, 0.0]]];
        let attrs = vec![vec![None]];
        let (_, bytes, _) =
            build_resident_sketch("ns", "seg", 2, &centroids, &clusters, &attrs).unwrap();
        let mut truncated = bytes.to_vec();
        truncated.pop();

        let error = ResidentSketch::from_bytes(&truncated).unwrap_err();
        assert!(error.to_string().contains("size mismatch"));
    }

    /// Manifest/object disagreements are format errors, never implicit defaults.
    #[test]
    fn v4_decode_rejects_mismatched_manifest_reference_fields() {
        let centroids = vec![vec![0.0, 0.0]];
        let clusters = vec![vec![vec![1.0, 0.0]]];
        let attrs = vec![vec![None]];
        let (sketch_ref, bytes, resident) =
            build_resident_sketch("ns", "seg", 2, &centroids, &clusters, &attrs).unwrap();

        let mut mismatches = Vec::new();
        let mut wrong_version = sketch_ref.clone();
        wrong_version.version = SKETCH_V3_VERSION;
        mismatches.push(wrong_version);
        let mut wrong_dims = sketch_ref.clone();
        wrong_dims.code_dims += BLOCK_DIM;
        mismatches.push(wrong_dims);
        let mut wrong_stride = sketch_ref.clone();
        wrong_stride.bytes_per_vector += 1;
        mismatches.push(wrong_stride);
        let mut missing_seed = sketch_ref.clone();
        missing_seed.rotation_seed = None;
        mismatches.push(missing_seed);

        for mismatched in mismatches {
            let warm_error = resident.validate_reference(&mismatched).unwrap_err();
            assert!(warm_error.to_string().contains("reference mismatch"));
            let error =
                decode_resident_sketch(bytes.clone(), &mismatched, &centroids, clusters[0].len())
                    .unwrap_err();
            assert!(error.to_string().contains("reference mismatch"));
        }

        let mut wrong_size = sketch_ref;
        wrong_size.size_bytes += 1;
        let warm_error = resident.validate_reference(&wrong_size).unwrap_err();
        assert!(warm_error.to_string().contains("reference mismatch"));
        let error =
            decode_resident_sketch(bytes, &wrong_size, &centroids, clusters[0].len()).unwrap_err();
        assert!(error.to_string().contains("size mismatch"));
    }

    /// Corrupt authoritative centroids fail while the resident sketch is bound.
    #[test]
    fn v4_binding_rejects_nonfinite_centroid() {
        let centroids = vec![vec![0.0, 0.0]];
        let clusters = vec![vec![vec![1.0, 0.0]]];
        let attrs = vec![vec![None]];
        let (_, bytes, _) =
            build_resident_sketch("ns", "seg", 2, &centroids, &clusters, &attrs).unwrap();
        let corrupt_centroids = vec![vec![f32::NAN, 0.0]];

        let error = ResidentSketch::from_bytes(&bytes)
            .unwrap()
            .with_centroids(&corrupt_centroids)
            .unwrap_err();

        assert!(error.to_string().contains("non-finite coordinate"));
    }

    /// A frozen current-main v3 layout remains decodable and searchable.
    #[test]
    fn v3_eight_bit_sketch_still_decodes_and_ranks() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SKETCH_MAGIC);
        bytes.extend_from_slice(&SKETCH_V3_VERSION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&2u64.to_le_bytes());
        for value in 0..SKETCH_V3_K {
            bytes.extend_from_slice(&(value as f32).to_le_bytes());
        }
        bytes.push(0);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&[0, 2]);

        let sketch = ResidentSketch::from_bytes(&bytes).unwrap();
        let selected = sketch
            .select_clusters(
                &[0.0],
                DistanceMetric::Euclidean,
                &[0, 1],
                fixed_budget(1),
                1,
            )
            .unwrap();
        assert_eq!(selected, vec![0]);
    }

    /// Builds a one-row-per-cluster sketch with controlled squared-L2 scores.
    ///
    /// The helper turns each desired score into a one-dimensional codeword so
    /// selection tests can exercise ranking without invoking training.
    fn one_dim_sketch_with_scores(scores: &[f32]) -> ResidentSketch {
        assert!(scores.len() <= SKETCH_V3_K);
        let mut codebook = vec![0.0; SKETCH_V3_K];
        let mut code_bytes = Vec::new();
        for (code, &score) in scores.iter().enumerate() {
            codebook[code] = score.sqrt();
            let mut packed = vec![0u8; 1];
            pack_code(&mut packed, 0, code as u8, SketchCodeWidth::EightBit);
            code_bytes.extend_from_slice(&packed);
        }
        let serialized_size = LEGACY_SKETCH_HEADER_LEN
            + SKETCH_V3_K * std::mem::size_of::<f32>()
            + bitset_len(scores.len())
            + scores.len() * std::mem::size_of::<u32>()
            + code_bytes.len();

        ResidentSketch {
            version: SKETCH_V3_VERSION,
            dim: 1,
            cluster_count: scores.len(),
            cluster_offsets: (0..scores.len()).map(|row| (row, row + 1)).collect(),
            codes: Bytes::from(code_bytes),
            cluster_has_attrs: vec![false; scores.len()],
            packed_code_bytes: 1,
            serialized_size,
            encoding: ResidentEncoding::LegacyPq {
                subquantizers: 1,
                codebook,
                codebook_size: SKETCH_V3_K,
                code_width: SketchCodeWidth::EightBit,
            },
        }
    }

    /// Row-frontier scoring is deterministic and never escapes the probe set.
    #[test]
    fn resident_row_frontier_is_deterministic_and_probe_bounded() {
        let cluster_scores = [vec![4.0_f32, 1.0], vec![1.0, 9.0], vec![0.25, 1.0]];
        let mut codebook = vec![0.0; SKETCH_V3_K];
        let mut codes = Vec::new();
        let mut cluster_offsets = Vec::new();
        for scores in &cluster_scores {
            let start = codes.len();
            for &score in scores {
                let code = codes.len();
                codebook[code] = score.sqrt();
                codes.push(code as u8);
            }
            cluster_offsets.push((start, codes.len()));
        }
        let sketch = ResidentSketch {
            version: SKETCH_V3_VERSION,
            dim: 1,
            cluster_count: cluster_scores.len(),
            cluster_offsets,
            codes: Bytes::from(codes),
            cluster_has_attrs: vec![false; cluster_scores.len()],
            packed_code_bytes: 1,
            serialized_size: 0,
            encoding: ResidentEncoding::LegacyPq {
                subquantizers: 1,
                codebook,
                codebook_size: SKETCH_V3_K,
                code_width: SketchCodeWidth::EightBit,
            },
        };

        let first = sketch
            .rank_clusters_with_frontier(&[0.0], DistanceMetric::Euclidean, &[2, 0], 2, 4)
            .unwrap()
            .row_frontier;
        let second = sketch
            .rank_clusters_with_frontier(&[0.0], DistanceMetric::Euclidean, &[2, 0], 2, 4)
            .unwrap()
            .row_frontier;

        let coordinates = |rows: &[ResidentRowScore]| {
            rows.iter()
                .map(|row| (row.cluster_idx, row.row_idx))
                .collect::<Vec<_>>()
        };
        assert_eq!(coordinates(&first), vec![(2, 0), (0, 1), (2, 1), (0, 0)]);
        assert_eq!(coordinates(&second), coordinates(&first));
        assert!(first.iter().all(|row| row.cluster_idx != 1));
        for (left, right) in first.iter().zip(second) {
            assert_eq!(
                left.approximate_score.to_bits(),
                right.approximate_score.to_bits()
            );
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

        let centroids = vec![vec![0.95, 0.05, 0.0], vec![0.0, 1.0, 0.0]];
        let (sketch_ref, bytes, sketch) =
            build_resident_sketch("ns", "seg", 3, &centroids, &clusters, &attrs).unwrap();
        assert_eq!(sketch_ref.bytes_per_vector, 72);
        assert_eq!(sketch_ref.code_dims, 256);
        assert!(!sketch.cluster_has_attrs(0));
        assert!(sketch.cluster_has_attrs(1));

        let decoded = ResidentSketch::from_bytes(&bytes)
            .unwrap()
            .with_centroids(&centroids)
            .unwrap();
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
        let centroids = vec![vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 0.0]];
        let (_sketch_ref, _bytes, old) =
            build_resident_sketch("ns", "old", 3, &centroids, &clusters, &attrs).unwrap();

        let new_attrs = vec![vec![None], vec![None]];
        let new_clusters = vec![
            vec![vec![1.0, 0.0, 0.0, 0.0]],
            vec![vec![0.0, 1.0, 0.0, 0.0]],
        ];
        let new_centroids = vec![vec![1.0, 0.0, 0.0, 0.0], vec![0.0, 1.0, 0.0, 0.0]];
        let result = stitch_resident_sketch(
            "ns",
            "new",
            4,
            &new_centroids,
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

    /// Untouched v4 rows cannot survive a centroid change under the same seed.
    #[test]
    fn stitch_rejects_untouched_centroid_change() {
        let attrs = vec![vec![None], vec![None]];
        let clusters = vec![vec![vec![1.0, 0.0, 0.0]], vec![vec![0.0, 1.0, 0.0]]];
        let old_centroids = vec![vec![0.5, 0.0, 0.0], vec![0.0, 0.5, 0.0]];
        let (_, _, old) =
            build_resident_sketch("ns", "old", 3, &old_centroids, &clusters, &attrs).unwrap();
        let changed_centroids = vec![vec![0.5, 0.0, 0.0], vec![0.0, 0.75, 0.0]];

        let result = stitch_resident_sketch(
            "ns",
            "new",
            3,
            &changed_centroids,
            &old,
            &[false, false],
            &clusters,
            &attrs,
        )
        .unwrap();

        match result {
            ResidentSketchStitch::Unavailable(reason) => {
                assert_eq!(reason, "centroid_mismatch")
            }
            ResidentSketchStitch::Stitched(_, _, _) => {
                panic!("untouched rows must not survive a centroid change")
            }
        }
    }

    /// Stitching copies every byte of an untouched row, including both factors.
    #[test]
    fn stitch_copies_complete_untouched_v4_row_spans() {
        let centroids = vec![vec![0.25, 0.0, 0.0], vec![0.0, 0.25, 0.0]];
        let old_clusters = vec![
            vec![vec![1.0, 0.0, 0.0], vec![0.75, 0.25, 0.0]],
            vec![vec![0.0, 1.0, 0.0]],
        ];
        let old_attrs = vec![vec![None, None], vec![None]];
        let (_, _, old) =
            build_resident_sketch("ns", "old", 3, &centroids, &old_clusters, &old_attrs).unwrap();
        let new_clusters = vec![
            old_clusters[0].clone(),
            vec![vec![0.0, 0.5, 0.5], vec![0.0, 0.25, 0.75]],
        ];
        let new_attrs = vec![vec![None, None], vec![None, None]];

        let result = stitch_resident_sketch(
            "ns",
            "new",
            3,
            &centroids,
            &old,
            &[false, true],
            &new_clusters,
            &new_attrs,
        )
        .unwrap();
        let ResidentSketchStitch::Stitched(_, _, stitched) = result else {
            panic!("compatible v4 rows must stitch");
        };

        let (old_start, old_end) = old.cluster_offsets[0];
        let (new_start, new_end) = stitched.cluster_offsets[0];
        assert_eq!(old_end - old_start, new_end - new_start);
        assert_eq!(
            &old.codes[old_start * old.packed_code_bytes..old_end * old.packed_code_bytes],
            &stitched.codes
                [new_start * stitched.packed_code_bytes..new_end * stitched.packed_code_bytes]
        );
    }

    /// A decoded v4 artifact with another persisted seed requires rebuilding.
    #[test]
    fn stitch_rejects_rotation_seed_change() {
        let centroids = vec![vec![0.0, 0.0, 0.0]];
        let clusters = vec![vec![vec![1.0, 0.0, 0.0]]];
        let attrs = vec![vec![None]];
        let (_, bytes, _) =
            build_resident_sketch("ns", "old", 3, &centroids, &clusters, &attrs).unwrap();
        let mut changed_seed = bytes.to_vec();
        changed_seed[28..36].copy_from_slice(&123_u64.to_le_bytes());
        let old = ResidentSketch::from_bytes(&changed_seed)
            .unwrap()
            .with_centroids(&centroids)
            .unwrap();

        let result = stitch_resident_sketch(
            "ns",
            "new",
            3,
            &centroids,
            &old,
            &[false],
            &clusters,
            &attrs,
        )
        .unwrap();

        match result {
            ResidentSketchStitch::Unavailable(reason) => {
                assert_eq!(reason, "rotation_seed_mismatch")
            }
            ResidentSketchStitch::Stitched(_, _, _) => {
                panic!("a different rotation seed must not stitch")
            }
        }
    }

    /// Frozen legacy PQ artifacts are readable but never copied into v4 output.
    #[test]
    fn stitch_requires_rebuild_for_v3_artifact() {
        let old = one_dim_sketch_with_scores(&[1.0]);
        let centroids = vec![vec![0.0]];
        let clusters = vec![vec![vec![1.0]]];
        let attrs = vec![vec![None]];

        let result = stitch_resident_sketch(
            "ns",
            "new",
            1,
            &centroids,
            &old,
            &[false],
            &clusters,
            &attrs,
        )
        .unwrap();

        match result {
            ResidentSketchStitch::Unavailable(reason) => {
                assert_eq!(reason, "format_mismatch")
            }
            ResidentSketchStitch::Stitched(_, _, _) => {
                panic!("legacy rows must trigger a full v4 rebuild")
            }
        }
    }

    /// V4 popcount ADC is deterministic and keeps the nearest Euclidean cluster.
    #[test]
    fn v4_query_scoring_is_deterministic() {
        let centroids = vec![vec![0.0, 0.0, 0.0], vec![10.0, 0.0, 0.0]];
        let clusters = vec![
            vec![vec![0.0, 0.0, 0.0], vec![0.2, 0.0, 0.0]],
            vec![vec![10.0, 0.0, 0.0], vec![9.8, 0.0, 0.0]],
        ];
        let attrs = vec![vec![None, None], vec![None, None]];
        let (_, _, sketch) =
            build_resident_sketch("ns", "seg", 3, &centroids, &clusters, &attrs).unwrap();
        let query = [0.1, 0.0, 0.0];

        let first = sketch
            .rank_clusters(&query, DistanceMetric::Euclidean, &[0, 1], 2)
            .unwrap();
        let second = sketch
            .rank_clusters(&query, DistanceMetric::Euclidean, &[0, 1], 2)
            .unwrap();

        assert_eq!(first[0].cluster_idx, 0);
        assert_eq!(first.len(), second.len());
        for (left, right) in first.iter().zip(second) {
            assert_eq!(left.cluster_idx, right.cluster_idx);
            assert_eq!(left.mass_count, right.mass_count);
            assert_eq!(
                left.aggregate_score.to_bits(),
                right.aggregate_score.to_bits()
            );
        }
    }

    /// Wire decoding and query preparation match the Phase-1 two-bit ADC math.
    #[test]
    fn v4_euclidean_row_score_matches_direct_rabitq_adc() {
        let centroid = vec![0.25, -0.5, 0.75];
        let vector = vec![1.0, -0.25, 0.0];
        let query = vec![0.5, 0.25, -0.5];
        let centroids = vec![centroid.clone()];
        let clusters = vec![vec![vector.clone()]];
        let attrs = vec![vec![None]];
        let (_, _, sketch) =
            build_resident_sketch("ns", "seg", 3, &centroids, &clusters, &attrs).unwrap();

        let actual = sketch
            .rank_clusters(&query, DistanceMetric::Euclidean, &[0], 1)
            .unwrap()[0]
            .aggregate_score;

        let code_dims = padded_code_dims(query.len()).unwrap();
        let rotation = StructuredRotation::new(code_dims, SKETCH_ROTATION_SEED).unwrap();
        let mut residual = vec![0.0; code_dims];
        let mut query_residual = vec![0.0; code_dims];
        for index in 0..query.len() {
            residual[index] = vector[index] - centroid[index];
            query_residual[index] = query[index] - centroid[index];
        }
        let mut scratch = vec![0.0; code_dims];
        rotation
            .rotate_in_place(&mut residual, &mut scratch)
            .unwrap();
        rotation
            .rotate_in_place(&mut query_residual, &mut scratch)
            .unwrap();
        let code = rabitq::encode_two_bit(&residual).unwrap();
        let seed = sketch_query_adc_seed(
            SKETCH_ROTATION_SEED,
            stable_query_hash(&query),
            0,
            0x5155_4552_595f_4144,
        );
        let adc = rabitq::prepare_query_adc4(&query_residual, seed).unwrap();
        let expected =
            rabitq::estimate_l2_two_bit(&code, &adc, squared_norm(&query_residual)).unwrap();

        assert_eq!(actual.to_bits(), expected.to_bits());
    }

    /// Dot-product v4 ranking combines residual ADC with the exact centroid term.
    #[test]
    fn v4_dot_product_ranking_accounts_for_centroid() {
        let centroids = vec![vec![4.0, 0.0, 0.0], vec![0.0, 2.0, 0.0]];
        let clusters = vec![vec![vec![5.0, 0.0, 0.0]], vec![vec![0.0, 3.0, 0.0]]];
        let attrs = vec![vec![None], vec![None]];
        let (_, _, sketch) =
            build_resident_sketch("ns", "seg", 3, &centroids, &clusters, &attrs).unwrap();

        let selected = sketch
            .select_clusters(
                &[1.0, 0.0, 0.0],
                DistanceMetric::DotProduct,
                &[0, 1],
                fixed_budget(1),
                1,
            )
            .unwrap();

        assert_eq!(selected, vec![0]);
    }

    /// Unit-normalized cosine uses the residual-L2 geometry measured in Phase 1.
    #[test]
    fn v4_cosine_uses_bakeoff_residual_l2_geometry() {
        let centroids = vec![vec![0.8, 0.0, 0.0], vec![0.0, 0.8, 0.0]];
        let clusters = vec![vec![vec![1.0, 0.0, 0.0]], vec![vec![0.0, 1.0, 0.0]]];
        let attrs = vec![vec![None], vec![None]];
        let (_, _, sketch) =
            build_resident_sketch("ns", "seg", 3, &centroids, &clusters, &attrs).unwrap();
        let query = [1.0, 0.0, 0.0];

        let cosine = sketch
            .select_clusters(&query, DistanceMetric::Cosine, &[0, 1], fixed_budget(1), 1)
            .unwrap();
        let euclidean = sketch
            .select_clusters(
                &query,
                DistanceMetric::Euclidean,
                &[0, 1],
                fixed_budget(1),
                1,
            )
            .unwrap();

        assert_eq!(cosine, vec![0]);
        assert_eq!(cosine, euclidean);
    }

    /// Cluster ranking prioritizes ownership of the global approximate top rows.
    #[test]
    fn cluster_selection_prefers_global_top_k_mass() {
        let mut codebook = Vec::with_capacity(SKETCH_V3_K);
        for value in 0..SKETCH_V3_K {
            codebook.push(value as f32);
        }

        let mut code_bytes = Vec::new();
        for code in [0u8, 4, 4, 4, 1, 1, 1, 1] {
            let mut packed = vec![0u8; 1];
            pack_code(&mut packed, 0, code, SketchCodeWidth::EightBit);
            code_bytes.extend_from_slice(&packed);
        }
        let serialized_size = LEGACY_SKETCH_HEADER_LEN
            + SKETCH_V3_K * std::mem::size_of::<f32>()
            + bitset_len(2)
            + 2 * std::mem::size_of::<u32>()
            + code_bytes.len();

        let sketch = ResidentSketch {
            version: SKETCH_V3_VERSION,
            dim: 1,
            cluster_count: 2,
            cluster_offsets: vec![(0, 4), (4, 8)],
            codes: Bytes::from(code_bytes),
            cluster_has_attrs: vec![false, false],
            packed_code_bytes: 1,
            serialized_size,
            encoding: ResidentEncoding::LegacyPq {
                subquantizers: 1,
                codebook,
                codebook_size: SKETCH_V3_K,
                code_width: SketchCodeWidth::EightBit,
            },
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
        let mut codebook = Vec::with_capacity(SKETCH_V3_K);
        for value in 0..SKETCH_V3_K {
            codebook.push(value as f32);
        }

        let mut code_bytes = Vec::new();
        for code in [0u8, 4, 1, 1] {
            let mut packed = vec![0u8; 1];
            pack_code(&mut packed, 0, code, SketchCodeWidth::EightBit);
            code_bytes.extend_from_slice(&packed);
        }
        let serialized_size = LEGACY_SKETCH_HEADER_LEN
            + SKETCH_V3_K * std::mem::size_of::<f32>()
            + bitset_len(2)
            + 2 * std::mem::size_of::<u32>()
            + code_bytes.len();

        let sketch = ResidentSketch {
            version: SKETCH_V3_VERSION,
            dim: 1,
            cluster_count: 2,
            cluster_offsets: vec![(0, 2), (2, 4)],
            codes: Bytes::from(code_bytes),
            cluster_has_attrs: vec![false, false],
            packed_code_bytes: 1,
            serialized_size,
            encoding: ResidentEncoding::LegacyPq {
                subquantizers: 1,
                codebook,
                codebook_size: SKETCH_V3_K,
                code_width: SketchCodeWidth::EightBit,
            },
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

    /// The accepted two-bit row arithmetic stays exact at target dimensions.
    #[test]
    fn sketch_code_bytes_match_two_bit_decision() {
        assert_eq!(rabitq_row_bytes(768).unwrap(), 200);
        assert_eq!(rabitq_row_bytes(1536).unwrap(), 392);
    }

    /// Target dimensions persist exact header metadata and one-row arithmetic.
    #[test]
    fn v4_target_dimension_objects_have_exact_size() {
        for (dim, row_bytes) in [(768usize, 200usize), (1536, 392)] {
            let centroid = vec![0.0; dim];
            let mut vector = vec![0.0; dim];
            vector[0] = 1.0;
            let centroids = vec![centroid];
            let clusters = vec![vec![vector]];
            let attrs = vec![vec![None]];

            let (sketch_ref, bytes, _) = build_resident_sketch(
                "ns",
                &format!("seg-{dim}"),
                dim,
                &centroids,
                &clusters,
                &attrs,
            )
            .unwrap();

            assert_eq!(sketch_ref.version, SKETCH_VERSION);
            assert_eq!(sketch_ref.code_dims, dim);
            assert_eq!(sketch_ref.bytes_per_vector, row_bytes);
            assert_eq!(bytes.len(), SKETCH_V4_HEADER_LEN + 1 + 4 + row_bytes);
            assert_eq!(
                u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
                dim as u32
            );
            assert_eq!(
                u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
                dim as u32
            );
        }
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
        assert!(matches!(
            &sketch.encoding,
            ResidentEncoding::LegacyPq {
                codebook_size: SKETCH_V2_K,
                code_width: SketchCodeWidth::FourBit,
                ..
            }
        ));
        assert_eq!(sketch.packed_code_bytes, 1);

        let selected = sketch
            .select_clusters(&[0.0], DistanceMetric::Euclidean, &[0], fixed_budget(1), 1)
            .unwrap();
        assert_eq!(selected, vec![0]);
    }
}
