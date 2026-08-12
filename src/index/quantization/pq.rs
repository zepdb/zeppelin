//! Implements product quantization (PQ) for compact, approximate vector search.
//!
//! PQ divides a `D`-dimensional vector into `M` equal contiguous subvectors and
//! trains an independent 256-centroid k-means codebook for each part. Encoding
//! stores the nearest centroid number for every subvector. Because 256 choices
//! fit in one byte, the complete code uses `M` bytes instead of `D * 4` bytes.
//! For example, `D = 128` and `M = 8` compress 512 vector bytes to eight code
//! bytes, excluding IDs, codebooks, and enclosing object overhead.
//!
//! Search uses asymmetric distance computation (ADC): the query remains full
//! precision, while each stored row remains encoded. One query-specific lookup
//! table records the distance from each query subvector to all 256 centroids;
//! scoring a candidate then sums only `M` table entries.
//!
//! ```text
//! segment vectors
//!      |
//!      | split each D-value row into M equal parts
//!      v
//! train 256 centroids per part -----> persisted PqCodebook
//!      |                                      |
//!      | choose nearest centroid              | query builds M x 256 table
//!      v                                      v
//! M-byte cluster codes ----------------> sum M selected distances
//!                                             |
//!                                             v
//!                                  coarse candidate ranking
//!                                             |
//!                                             v
//!                               full-vector reranking elsewhere
//! ```
//!
//! This file owns training, encoding, ADC math, binary conversion, and object
//! key construction. [`crate::index::ivf_flat`] and
//! [`crate::index::hierarchical`] decide when to use it,
//! [`crate::storage::ZeppelinStore`] performs S3/MinIO I/O, and the
//! authoritative [`crate::wal::manifest::Manifest`] controls when a completed
//! immutable segment becomes visible. Creating a codebook or cluster payload
//! here does not write or publish remote state.
//!
//! ## Reading map
//!
//! 1. Start with [`PqCodebook`] and [`PqCodebook::train`] for the learned model.
//! 2. Follow [`PqCodebook::encode`] through
//!    [`PqCodebook::build_adc_table`] and [`PqCodebook::adc_distance`] for the
//!    build-to-search lifecycle.
//! 3. Read [`PqCodebook::to_bytes`] and [`PqCodebook::from_bytes`] for codebook
//!    compatibility.
//! 4. Read [`serialize_pq_cluster`] and [`deserialize_pq_cluster`] for the
//!    positional ID/code row payload.
//!
//! ## Persisted formats
//!
//! All numeric fields are little-endian. These formats have no magic prefix,
//! version, checksum, or embedded namespace/segment identity, and the readers
//! currently ignore trailing bytes. The surrounding manifest metadata and key
//! therefore determine which parser and codebook belong to a cluster.
//!
//! Codebook blob:
//! ```text
//! [M: u32][K = 256: u32][sub_dim: u32]
//! repeat M times:
//!   repeat 256 times:
//!     [centroid coordinate: f32 * sub_dim]
//! ```
//!
//! PQ cluster blob:
//! ```text
//! [num_vectors: u32][M: u32]
//! repeat num_vectors times:
//!   [id_len: u32][UTF-8 id bytes][centroid index: u8 * M]
//! ```
//!
//! ## Invariants
//!
//! - `D > 0`, `M > 0`, and `D` is exactly divisible by `M`.
//! - Every training, encoded, and query vector uses the same `D`.
//! - Every code has exactly `M` bytes and is interpreted with its own segment's
//!   codebook.
//! - Cluster IDs and codes have equal counts and identical row order.
//! - Codebook and cluster objects are immutable segment artifacts; a manifest
//!   publication, not object existence alone, makes the segment visible.
//!
//! ## Rust concepts used here
//!
//! Training accepts `&[&[f32]]`, a borrowed slice of borrowed vector rows, so it
//! can read caller-owned buffers without copying the whole training set at the
//! API boundary. Internally it intentionally creates owned subvector matrices
//! for k-means. A C analogy is an array of pointer/length pairs; Java would use
//! an array or list of arrays. Rust additionally proves the borrowed rows remain
//! valid and cannot be mutated through these shared references during use.

use bytes::Bytes;

use crate::error::{Result, ZeppelinError};
use crate::types::DistanceMetric;

/// Number of centroids in every subquantizer codebook.
///
/// `256` is part of the persisted format: it lets each selected centroid fit in
/// one `u8`. [`PqCodebook::from_bytes`] rejects a different stored value.
const PQ_K: usize = 256;

/// Owns the `M` learned subvector codebooks used for encoding and ADC scoring.
///
/// Centroids are flattened in subquantizer-major, then centroid-major order:
/// `centroids[subquantizer * 256 + code]`. Each inner vector owns exactly
/// [`Self::sub_dim`] `f32` values. Cloning this type deeply allocates and copies
/// every centroid; it is not a shared reference-count operation.
///
/// # Examples
///
/// With `dim = 8` and `m = 4`, every vector is split into four two-dimensional
/// parts. The codebook stores 256 possible two-dimensional reconstructions per
/// part, and an encoded vector contains four bytes.
#[derive(Debug, Clone)]
pub struct PqCodebook {
    /// Number of equal subvector partitions and bytes in every encoded row.
    pub m: usize,
    /// Number of coordinates in one subvector, equal to `dim / m`.
    pub sub_dim: usize,
    /// Number of coordinates expected in every training and query vector.
    pub dim: usize,
    /// Flattened, owned centroid vectors in subquantizer-major order.
    centroids: Vec<Vec<f32>>,
}

impl PqCodebook {
    /// Trains one k-means codebook for every contiguous subvector partition.
    ///
    /// When fewer than 256 training rows are available, k-means uses that
    /// smaller row count and the last learned centroid is duplicated until all
    /// byte values have a valid lookup target. The duplicates preserve the
    /// fixed one-byte persisted format; they do not add information.
    ///
    /// # Parameters
    ///
    /// - `vectors`: Borrowed training rows, each exactly `dim` coordinates.
    /// - `dim`: Full vector dimension `D`; must be nonzero.
    /// - `m`: Number of subquantizers and output code bytes; must be nonzero and
    ///   divide `dim` evenly.
    /// - `kmeans_iters`: Maximum iterations passed to each subquantizer's
    ///   k-means training.
    ///
    /// # Returns
    ///
    /// Returns an owned codebook containing `m * 256` centroids. It borrows no
    /// training data after return.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Index`] for zero dimensions, zero
    /// subquantizers, a non-divisible dimension, empty training data, an empty
    /// sub-codebook, or an error from
    /// [`crate::index::ivf_flat::kmeans::train_kmeans`]. No object-store state
    /// is written before failure.
    ///
    /// # Panics
    ///
    /// Panics while slicing if any training vector contains fewer than `dim`
    /// values. Longer rows are accepted but coordinates beyond `dim` are
    /// ignored. Callers should validate exact dimensions before training.
    ///
    /// # Performance
    ///
    /// For each of `m` partitions, this method copies every subvector into a
    /// temporary owned matrix and runs k-means with up to 256 centroids. A rough
    /// upper bound is `O(m * iterations * training rows * 256 * sub_dim)` CPU,
    /// plus temporary `O(training rows * sub_dim)` storage per partition and
    /// persistent `O(256 * dim)` centroid storage.
    ///
    /// # Examples
    ///
    /// Training `dim = 8, m = 4` creates four two-dimensional codebooks. Asking
    /// for `dim = 7, m = 4` returns an error because equal subvector boundaries
    /// cannot be formed.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The training rows are borrowed, then each partition uses `to_vec()` to
    /// make an intentional owned copy required by the k-means API. `?`
    /// propagates training failure while automatically dropping those temporary
    /// vectors. Padding uses `.clone()` on `Vec<f32>`, which allocates and copies
    /// centroid coordinates; it is unlike cloning [`Bytes`] or `Arc`, which can
    /// share underlying storage.
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
                padded.push(
                    padded
                        .last()
                        .ok_or_else(|| ZeppelinError::Index("empty PQ sub-centroids".into()))?
                        .clone(),
                );
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

    /// Encodes one vector as the nearest centroid index for each subquantizer.
    ///
    /// # Parameters
    ///
    /// - `vector`: Borrowed full-precision row expected to contain exactly
    ///   [`Self::dim`] coordinates.
    ///
    /// # Returns
    ///
    /// Returns an owned `Vec<u8>` of length [`Self::m`]. Byte `i` names the
    /// nearest centroid in subquantizer `i` under squared Euclidean distance.
    ///
    /// # Panics
    ///
    /// Debug builds reject a dimension mismatch. Optimized builds panic if the
    /// vector is too short; extra trailing coordinates are ignored. Callers
    /// must validate the dimension before this hot path.
    ///
    /// # Performance
    ///
    /// Compares each subvector with all 256 centroids: `O(256 * dim)` CPU and
    /// one `m`-byte allocation, with no I/O.
    ///
    /// # Examples
    ///
    /// An eight-dimensional vector encoded by an `m = 4` codebook yields four
    /// bytes, one for each consecutive pair of coordinates.
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

    /// Encodes a borrowed batch while preserving its row order.
    ///
    /// # Parameters
    ///
    /// - `vectors`: Borrowed rows, each exactly [`Self::dim`] coordinates.
    ///
    /// # Returns
    ///
    /// Returns one owned `m`-byte code per input row in identical order; empty
    /// input returns an empty outer vector.
    ///
    /// # Panics
    ///
    /// Inherits [`Self::encode`]'s dimension precondition for every row.
    ///
    /// # Performance
    ///
    /// Performs `O(number of rows * 256 * dim)` work and allocates an outer
    /// vector plus one code vector per row.
    ///
    /// # Examples
    ///
    /// Encoding all rows assigned to one IVF cluster produces codes that can be
    /// paired positionally with those rows' IDs in [`serialize_pq_cluster`].
    pub fn encode_batch(&self, vectors: &[&[f32]]) -> Vec<Vec<u8>> {
        vectors.iter().map(|v| self.encode(v)).collect()
    }

    /// Reconstructs an approximate full vector by concatenating selected
    /// centroids.
    ///
    /// # Parameters
    ///
    /// - `codes`: Borrowed code expected to contain exactly [`Self::m`] bytes
    ///   produced with this codebook.
    ///
    /// # Returns
    ///
    /// Returns a newly allocated `Vec<f32>` of length [`Self::dim`]. Each
    /// subvector equals its selected centroid, so the reconstruction is
    /// generally approximate.
    ///
    /// # Panics
    ///
    /// Debug builds reject a code-width mismatch. Optimized builds accept a
    /// shorter code and return a short reconstruction, while a longer code can
    /// index beyond the flattened codebook. Callers must preserve `m`.
    ///
    /// # Performance
    ///
    /// Copies `dim` floats into one allocation in `O(dim)` time.
    ///
    /// # Examples
    ///
    /// A four-byte code under a codebook with two-dimensional subvectors
    /// concatenates four selected centroids into an eight-value approximation.
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

    /// Builds the query-specific lookup table used by asymmetric distance
    /// computation (ADC).
    ///
    /// The table is flattened in subquantizer-major order:
    /// `table[subquantizer * 256 + centroid_code]`. Euclidean stores squared L2
    /// distance, dot product stores a negated score, and cosine currently uses
    /// squared L2 between query and centroid subvectors as an approximation;
    /// full cosine would require global reconstructed norms.
    ///
    /// # Parameters
    ///
    /// - `query`: Borrowed full-precision query of exactly [`Self::dim`]
    ///   coordinates.
    /// - `metric`: Namespace distance metric that determines each table entry's
    ///   scoring rule.
    ///
    /// # Returns
    ///
    /// Returns an owned table containing [`Self::m`] times 256 `f32` scores.
    /// Reuse it for every PQ row compared with the same query and codebook.
    ///
    /// # Panics
    ///
    /// Debug builds reject a query dimension mismatch. Optimized builds panic
    /// for a short query and ignore extra coordinates beyond `self.dim`.
    ///
    /// # Performance
    ///
    /// Performs `O(256 * dim)` arithmetic and allocates `m * 256` floats once
    /// per query/codebook, moving that repeated work out of the candidate loop.
    ///
    /// # Examples
    ///
    /// For `m = 4`, one query creates 1,024 table entries. A cluster containing
    /// ten thousand codes can then score every row using four indexed additions
    /// rather than revisiting all query coordinates.
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

    /// Scores one PQ row by summing its selected ADC table entries.
    ///
    /// # Parameters
    ///
    /// - `table`: Borrowed table from [`Self::build_adc_table`] for the same
    ///   query, metric, and codebook. It must contain at least `m * 256` entries.
    /// - `codes`: Borrowed candidate code containing exactly [`Self::m`] bytes.
    ///
    /// # Returns
    ///
    /// Returns the candidate's approximate lower-is-better distance.
    ///
    /// # Panics
    ///
    /// Debug builds reject a code-width mismatch. Any build panics if a selected
    /// table index is missing; optimized builds can silently under-score a short
    /// code because only the supplied bytes are summed.
    ///
    /// # Performance
    ///
    /// Performs exactly one table lookup and addition per supplied code byte:
    /// `O(m)` time with no allocation or I/O.
    ///
    /// # Examples
    ///
    /// A four-byte code reads four entries from a query table and sums them.
    /// Search repeats this cheap step for each coarse candidate, then reranks a
    /// smaller set against full vectors.
    #[inline]
    pub fn adc_distance(&self, table: &[f32], codes: &[u8]) -> f32 {
        debug_assert_eq!(codes.len(), self.m);
        let mut sum = 0.0f32;
        for (sub_idx, &code) in codes.iter().enumerate() {
            sum += table[sub_idx * PQ_K + code as usize];
        }
        sum
    }

    /// Serializes this codebook into its fixed-256-centroid little-endian format.
    ///
    /// # Returns
    ///
    /// Returns immutable bytes containing a 12-byte `m`, `k`, and `sub_dim`
    /// header followed by every centroid coordinate. The exact length is
    /// `12 + m * 256 * sub_dim * 4` bytes.
    ///
    /// # Panics
    ///
    /// May panic or fail its debug assertion if internal centroid counts or
    /// widths violate [`PqCodebook`]'s invariants. Values larger than `u32::MAX`
    /// are outside the persisted header's representable contract.
    ///
    /// # Consistency
    ///
    /// This method constructs bytes only. The builder must store the codebook
    /// and all matching cluster codes as immutable artifacts before a manifest
    /// publication makes the segment visible.
    ///
    /// # Performance
    ///
    /// Allocates one exactly sized buffer and copies `256 * dim` floats. Turning
    /// the completed `Vec<u8>` into [`Bytes`] transfers its allocation rather
    /// than copying each byte again.
    ///
    /// # Examples
    ///
    /// A codebook with `dim = 8, m = 4, sub_dim = 2` writes 8,204 bytes: 12
    /// header bytes plus 2,048 centroid floats.
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

    /// Parses a persisted PQ codebook and reconstructs its full dimension.
    ///
    /// # Parameters
    ///
    /// - `data`: Borrowed bytes in the module-level codebook format. Trailing
    ///   bytes are currently ignored.
    ///
    /// # Returns
    ///
    /// Returns an owned [`PqCodebook`] with `dim = m * sub_dim` and all
    /// centroids copied from the input.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Index`] for a truncated header, a stored `k`
    /// other than 256, zero `m` or `sub_dim`, arithmetic overflow in the
    /// declared payload size, a truncated centroid body, or a float parse
    /// failure. No remote or shared state changes before failure. The parser has
    /// no magic, version, checksum, finiteness validation, or upper allocation
    /// limit beyond arithmetic and input-length checks.
    ///
    /// # Performance
    ///
    /// Allocates `m * 256` inner vectors and copies `256 * dim` floats. Size
    /// arithmetic is checked before allocation; callers should still enforce
    /// object-size bounds because valid large headers can request large memory.
    ///
    /// # Examples
    ///
    /// [`Self::to_bytes`] output round-trips `m`, `sub_dim`, `dim`, and centroid
    /// values. A blob that claims `k = 128` is rejected because its byte codes
    /// would not match Zeppelin's fixed 256-centroid interpretation.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The chained [`usize::checked_mul`] calls turn integer overflow into
    /// `None`, and `ok_or_else(...)?` converts that absence into an index error.
    /// Unlike unchecked C size arithmetic or Java's wrapping primitive
    /// multiplication, the allocation size cannot wrap to a misleading smaller
    /// value along this path. Previously allocated vectors are released
    /// automatically on every early return.
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

        if m == 0 || sub_dim == 0 {
            return Err(ZeppelinError::Index(format!(
                "PQ codebook header has zero dimension: m={m}, sub_dim={sub_dim}"
            )));
        }

        let expected = m
            .checked_mul(PQ_K)
            .and_then(|v| v.checked_mul(sub_dim))
            .and_then(|v| v.checked_mul(4))
            .and_then(|v| v.checked_add(12))
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "PQ codebook size overflows: m={m}, sub_dim={sub_dim}"
                ))
            })?;
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

/// Serializes ordered vector IDs and PQ codes into one immutable cluster
/// payload.
///
/// Row position is the only association between an ID and its code. The
/// codebook is deliberately stored separately and must come from the same
/// physical segment owner.
///
/// # Parameters
///
/// - `ids`: Borrowed UTF-8 vector IDs in cluster row order.
/// - `codes`: Borrowed PQ rows in exactly the same count and order as `ids`;
///   every row must contain exactly `m` bytes.
/// - `m`: Persisted code width, which must match [`PqCodebook::m`].
///
/// # Returns
///
/// Returns immutable bytes containing the row count, `m`, and each
/// length-prefixed ID followed by its fixed-width code.
///
/// # Errors
///
/// The current implementation always returns `Ok`; [`Result`] matches other
/// index serializers. It does **not** validate row counts, code widths, or
/// conversion of counts and ID lengths into the format's `u32` fields.
///
/// # Consistency
///
/// `ids.iter().zip(codes)` stops at the shorter input. A mismatch therefore
/// produces a header count that may not match the body, and a wrong code width
/// shifts subsequent row boundaries. Callers must validate these invariants.
/// Constructing bytes does not write or publish an object; the matching
/// codebook and cluster object become readable only with their segment's
/// authoritative manifest publication.
///
/// # Performance
///
/// Copies every ID and code byte into one growing allocation. Work and output
/// size are `O(total ID bytes + number of rows * m)`; conversion to [`Bytes`]
/// transfers the completed buffer.
///
/// # Examples
///
/// Three IDs and three four-byte codes produce a cluster declaring `n = 3` and
/// `m = 4`. Supplying only two codes would still declare three rows but emit
/// two, so builders must reject that mismatch before calling this helper.
pub fn serialize_pq_cluster(ids: &[String], codes: &[Vec<u8>], m: usize) -> Result<Bytes> {
    // A silent `as u32` truncation would write a header that disagrees with
    // the payload, making the decoder drop rows without any error. Fail loud
    // instead; the Result channel already exists.
    let n = u32::try_from(ids.len())
        .map_err(|_| ZeppelinError::Index("PQ cluster row count exceeds u32".into()))?;
    let m_u32 = u32::try_from(m)
        .map_err(|_| ZeppelinError::Index("PQ cluster code width exceeds u32".into()))?;

    let mut buf = Vec::new();
    buf.extend_from_slice(&n.to_le_bytes());
    buf.extend_from_slice(&m_u32.to_le_bytes());

    for (id, code) in ids.iter().zip(codes.iter()) {
        let id_bytes = id.as_bytes();
        let id_len = u32::try_from(id_bytes.len())
            .map_err(|_| ZeppelinError::Index("vector id length exceeds u32".into()))?;
        buf.extend_from_slice(&id_len.to_le_bytes());
        buf.extend_from_slice(id_bytes);
        buf.extend_from_slice(code);
    }

    Ok(Bytes::from(buf))
}

/// Owns the vector IDs and PQ codes parsed from one cluster artifact.
///
/// Corresponding positions in [`Self::ids`] and [`Self::codes`] name and encode
/// the same candidate. Filters, approximate scores, and reranking depend on
/// preserving that alignment.
///
/// # Examples
///
/// `ids[7]` belongs to `codes[7]`. Reordering either collection independently
/// would attach the approximate distance to the wrong vector.
#[derive(Debug)]
pub struct PqClusterData {
    /// Owned vector identifiers in persisted cluster row order.
    pub ids: Vec<String>,
    /// Owned `m`-byte codes aligned positionally with [`Self::ids`].
    pub codes: Vec<Vec<u8>>,
}

/// Parses one PQ cluster payload into owned IDs and code rows.
///
/// # Parameters
///
/// - `data`: Borrowed bytes in the module-level cluster format. The parser uses
///   the stored row count and `m` and currently ignores trailing bytes.
///
/// # Returns
///
/// Returns [`PqClusterData`] with exactly the declared number of IDs and codes,
/// in persisted order. The payload's `m` controls code widths but is not
/// retained in the result; the caller must pair it with the matching codebook.
///
/// # Errors
///
/// Returns [`ZeppelinError::Index`] if the header, an ID length, an ID payload,
/// or an `m`-byte code is truncated. Invalid UTF-8 is not an error:
/// [`String::from_utf8_lossy`] replaces malformed sequences with the Unicode
/// replacement character, which is the current compatibility behavior.
///
/// # Performance
///
/// Allocates both outer vectors, one `String`, and one code vector per row, then
/// copies all consumed ID and code bytes. The header controls capacity, so
/// callers should bound object size before parsing object-store data.
///
/// # Examples
///
/// A payload declaring two four-byte rows returns two aligned IDs and codes. If
/// the final code has only three bytes, parsing returns an error and drops the
/// partially built result.
///
/// # Rust Notes for Java/C Engineers
///
/// Although `data` is borrowed, the returned cluster owns every ID and code, so
/// it remains valid after the source buffer is released. Bounds-checked slices
/// replace manual C pointer arithmetic, and the `?` operator unwinds ordinary
/// control flow—not exceptions—while RAII drops partial allocations.
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

    // Cap the reservation by what the payload could possibly hold: each row
    // carries at least a 4-byte id_len prefix, so a valid n never exceeds
    // data.len() / 4. A hostile or corrupt header otherwise requests gigabytes
    // before any per-row validation runs.
    let cap = n.min(data.len() / 4);
    let mut ids = Vec::with_capacity(cap);
    let mut codes = Vec::with_capacity(cap);
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

/// Builds the object-store key for one immutable segment's PQ codebook.
///
/// # Parameters
///
/// - `namespace`: Already validated namespace key prefix.
/// - `segment_id`: Immutable segment identifier and key path component.
///
/// # Returns
///
/// Returns `<namespace>/segments/<segment_id>/pq_codebook.bin` without a
/// leading slash.
///
/// # Consistency
///
/// This pure formatter does not access S3/MinIO, verify the object, or publish
/// the segment. It performs no escaping or validation; callers must use the
/// namespace and physical segment owner from authoritative metadata.
///
/// # Examples
///
/// Namespace `catalog` and segment `01ABC` produce
/// `catalog/segments/01ABC/pq_codebook.bin`.
pub fn pq_codebook_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/pq_codebook.bin")
}

/// Builds the object-store key for one immutable PQ cluster artifact.
///
/// # Parameters
///
/// - `namespace`: Already validated namespace key prefix.
/// - `segment_id`: Segment that physically owns this cluster; incremental
///   compaction may leave a carried cluster under an older owner.
/// - `cluster_idx`: Zero-based cluster number in the logical index.
///
/// # Returns
///
/// Returns `<namespace>/segments/<segment_id>/pq_cluster_<cluster_idx>.bin`.
///
/// # Consistency
///
/// This pure formatter performs no I/O or existence check. Search and garbage
/// collection must derive the physical owner from manifest metadata so carried
/// clusters are read from and retained under the correct immutable segment.
///
/// # Examples
///
/// Cluster `3` in `catalog` segment `01ABC` maps to
/// `catalog/segments/01ABC/pq_cluster_3.bin`.
pub fn pq_cluster_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/pq_cluster_{cluster_idx}.bin")
}

// ---------------------------------------------------------------------------
// Helper math
// ---------------------------------------------------------------------------

/// Computes squared Euclidean distance between equally sized float slices.
///
/// `a` and `b` are borrowed and no allocation occurs. The caller must provide
/// `b.len() >= a.len()`; a shorter `b` panics, while extra `b` values are
/// ignored. For example, `[1, 2]` and `[4, 6]` return `25`.
#[inline]
fn sq_l2(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

/// Computes the dot product of equally sized float slices.
///
/// `a` and `b` are borrowed and no allocation occurs. The caller must provide
/// `b.len() >= a.len()`; a shorter `b` panics, while extra `b` values are
/// ignored. For example, `[1, 2]` and `[4, 6]` return `16`.
#[inline]
fn dot(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        sum += a[i] * b[i];
    }
    sum
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Exercises PQ training, ADC ranking, binary compatibility, allocation
    //! guards, and caller-facing dimension failures with deterministic data.

    use super::*;

    /// Verifies a hostile row count in a tiny payload is rejected without
    /// reserving row-count-proportional memory.
    ///
    /// Before the reservation cap, an 8-byte blob claiming `u32::MAX` rows
    /// requested a multi-gigabyte allocation (aborting the process) before any
    /// per-row validation ran.
    #[test]
    fn test_deserialize_pq_cluster_rejects_hostile_row_count() {
        let mut data = Vec::new();
        data.extend_from_slice(&u32::MAX.to_le_bytes());
        data.extend_from_slice(&8u32.to_le_bytes());
        assert!(deserialize_pq_cluster(&data).is_err());
    }

    /// Builds reproducible rows whose values vary across both vector and
    /// dimension indexes, avoiding random input in codebook tests.
    ///
    /// `n` controls the number of owned rows and `dim` their width. The result is
    /// used only in memory and preserves generation order.
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

    /// Verifies the learned partition shape and the widths of one encoded and
    /// reconstructed vector.
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

    /// Verifies that a query's own PQ row ranks no worse than a distant row under
    /// the query-specific Euclidean ADC table.
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

    /// Verifies that codebook serialization preserves partition dimensions and
    /// the complete centroid count.
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

    /// Verifies that cluster serialization preserves the positional ID/code
    /// relationship across a round trip.
    #[test]
    fn test_pq_cluster_serde_roundtrip() {
        let ids = vec!["a".into(), "b".into(), "c".into()];
        let codes = vec![vec![0u8, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11]];
        let data = serialize_pq_cluster(&ids, &codes, 4).unwrap();
        let decoded = deserialize_pq_cluster(&data).unwrap();
        assert_eq!(decoded.ids, ids);
        assert_eq!(decoded.codes, codes);
    }

    /// Verifies that training rejects a full dimension that cannot be divided
    /// into equal subvector partitions.
    #[test]
    fn test_pq_dim_not_divisible() {
        let data = training_data(100, 7);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        // 7 is not divisible by 4.
        let result = PqCodebook::train(&refs, 7, 4, 10);
        assert!(result.is_err());
    }

    /// Verifies that PQ cannot train a codebook without at least one row.
    #[test]
    fn test_pq_empty_dataset() {
        let refs: Vec<&[f32]> = vec![];
        let result = PqCodebook::train(&refs, 8, 4, 10);
        assert!(result.is_err());
    }

    /// Verifies that a codebook payload shorter than its 12-byte header is
    /// rejected.
    #[test]
    fn test_pq_codebook_from_bytes_too_small() {
        assert!(PqCodebook::from_bytes(&[0u8; 8]).is_err());
    }

    /// Guards a fuzz-discovered case where a huge `m` and zero `sub_dim` could
    /// previously trigger a multi-gigabyte centroid allocation.
    #[test]
    fn test_pq_codebook_from_bytes_zero_sub_dim_huge_m() {
        // Fuzz regression: sub_dim=0 makes the expected-size check pass with a
        // huge m, which then drove a ~45 GB Vec::with_capacity.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0x2bdf00e6u32.to_le_bytes()); // m (huge)
        buf.extend_from_slice(&(PQ_K as u32).to_le_bytes()); // k
        buf.extend_from_slice(&0u32.to_le_bytes()); // sub_dim = 0
        buf.extend_from_slice(&[14, 223]);
        assert!(PqCodebook::from_bytes(&buf).is_err());
    }

    /// Verifies that overflowing persisted size arithmetic returns an error
    /// instead of wrapping to a smaller allocation.
    #[test]
    fn test_pq_codebook_from_bytes_size_overflow() {
        // m * PQ_K * sub_dim * 4 overflows usize; must error, not wrap.
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // m
        buf.extend_from_slice(&(PQ_K as u32).to_le_bytes()); // k
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // sub_dim
        assert!(PqCodebook::from_bytes(&buf).is_err());
    }

    /// Verifies that batch encoding returns one `m`-byte code for every input
    /// row.
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
