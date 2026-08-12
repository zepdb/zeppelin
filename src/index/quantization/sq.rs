//! Implements eight-bit scalar quantization (SQ8) for coarse vector search.
//!
//! Scalar quantization treats every vector coordinate independently. During
//! segment construction, [`SqCalibration::calibrate`] records the smallest and
//! largest observed value for each dimension. [`SqCalibration::encode`] then
//! maps that interval onto the integers `0..=255`; decoding chooses the
//! corresponding point in the interval. A code therefore uses one byte per
//! dimension instead of the four bytes used by an `f32`, giving a four-to-one
//! vector compression ratio before ID and object-format overhead.
//!
//! ```text
//! segment vectors                 one query + stored SQ codes
//!      |                                      |
//!      | find min/max per dimension           | reconstruct each coordinate
//!      v                                      v
//! SqCalibration ------ encode ------> one byte per dimension
//!      |                                      |
//!      | persisted with the segment           | approximate coarse distance
//!      +--------------------------------------+
//!                                             |
//!                                             v
//!                                  full-vector reranking elsewhere
//! ```
//!
//! This file performs CPU-side calibration, encoding, distance calculation,
//! and byte-format conversion. Index builders decide where the returned
//! [`Bytes`] are placed, [`crate::storage::ZeppelinStore`] performs the actual
//! S3/MinIO I/O, and the authoritative [`crate::wal::manifest::Manifest`]
//! controls segment visibility. New IVF and hierarchical segments co-locate SQ
//! payloads with their full vectors; [`sq_calibration_key`] and
//! [`sq_cluster_key`] remain part of the legacy sidecar layout read by older
//! segments. None of these helpers publishes or mutates remote state.
//!
//! ## Reading map
//!
//! 1. Read [`SqCalibration`] and [`SqCalibration::calibrate`] for the learned
//!    per-dimension model.
//! 2. Follow [`SqCalibration::encode`] through
//!    [`SqCalibration::asymmetric_distance`] for the write-to-search flow.
//! 3. Read [`SqCalibration::to_bytes`] and [`SqCalibration::from_bytes`] for
//!    persisted calibration compatibility.
//! 4. Read [`serialize_sq_cluster`] and [`deserialize_sq_cluster`] for the row
//!    payload shared by legacy sidecars and newer co-located objects.
//!
//! ## Persisted formats
//!
//! All integers and floats are little-endian. Neither format has a magic
//! prefix, version, checksum, or embedded namespace/segment identity, and the
//! readers currently ignore trailing bytes. Callers must therefore choose the
//! correct parser from manifest metadata and enclosing object format.
//!
//! Calibration blob:
//! ```text
//! [dimension: u32]
//! [min_0: f32][max_0: f32] ... [min_{dim-1}: f32][max_{dim-1}: f32]
//! ```
//!
//! Quantized cluster payload:
//! ```text
//! [num_vectors: u32][dimension: u32]
//! repeat num_vectors times:
//!   [id_len: u32][UTF-8 id bytes][u8 code * dimension]
//! ```
//!
//! Codes-only cluster payload (coarse block of `ZBP5` grouped objects; row IDs
//! live in a separate ID block joined by position):
//! ```text
//! [num_vectors: u32][dimension: u32][u8 code * dimension * num_vectors]
//! ```
//! Unlike the other two formats, the codes-only reader requires an exact
//! length: trailing bytes are rejected.
//!
//! ## Invariants
//!
//! - Calibration and every encoded row use the segment's declared dimension.
//! - Cluster IDs and codes have equal counts and remain in identical row order.
//! - A constant dimension has zero scale, encodes to `0`, and decodes to its
//!   single calibrated value.
//! - Values outside the calibrated interval clamp to its nearest endpoint.
//! - Quantized scores rank candidates approximately; full vectors remain the
//!   source for exact reranking.
//!
//! ## Rust concepts used here
//!
//! Public APIs borrow vectors as slices (`&[f32]` and `&[&[f32]]`) so callers
//! retain ownership of their buffers while calibration and encoding read them.
//! A C analogy is a pointer plus length, but Rust slices are non-null views whose
//! lifetimes and bounds are checked. A Java analogy is a read-only view over an
//! array, except Java's type system does not enforce the borrow lifetime. The
//! returned [`Vec`] and [`Bytes`] values are owned: `Vec` allocates element
//! storage, while converting it into `Bytes` transfers the completed buffer
//! into an immutable, cheaply shareable byte owner.

use bytes::Bytes;

use crate::error::{Result, ZeppelinError};

/// Holds the segment-wide ranges required to encode, reconstruct, and score SQ8
/// vectors.
///
/// `dim`, `mins`, and `maxs` are the persisted model. `scales` and
/// `inv_scales` are derived lookup arrays reconstructed after deserialization so
/// hot paths avoid division. A cloned calibration owns independent copies of
/// all five vectors; cloning is therefore proportional to the dimension and is
/// not a cheap reference-count increment.
///
/// # Examples
///
/// For observations `[-2.0]` and `[2.0]`, the single dimension has a range of
/// four. Encoding `-2.0` produces `0`, encoding `2.0` produces `255`, and an
/// interior value is rounded down to one of the 256 reconstructable levels.
#[derive(Debug, Clone)]
pub struct SqCalibration {
    /// Number of coordinates expected in every vector and SQ code.
    pub dim: usize,
    /// Lowest observed calibration value at each dimension index.
    pub mins: Vec<f32>,
    /// Highest observed calibration value at each dimension index.
    pub maxs: Vec<f32>,
    /// Precomputed `(max - min) / 255.0` decoding step for each dimension.
    scales: Vec<f32>,
    /// Precomputed `255.0 / (max - min)` encoding multiplier for each dimension.
    inv_scales: Vec<f32>,
}

impl SqCalibration {
    /// Learns the minimum, maximum, and scale for every vector dimension.
    ///
    /// # Parameters
    ///
    /// - `vectors`: Borrowed training rows. Callers must provide at least one
    ///   row, and every row must contain exactly `dim` values.
    /// - `dim`: Declared number of coordinates in every training and future
    ///   encoded vector.
    ///
    /// # Returns
    ///
    /// Returns an owned calibration whose arrays all contain `dim` entries.
    /// No input vectors are retained.
    ///
    /// # Panics
    ///
    /// Panics if a training vector is longer than `dim`, because its dimension
    /// index is used to address the fixed-size range arrays. Empty input or
    /// shorter rows do not currently panic but violate the caller contract and
    /// can leave unusable sentinel ranges.
    ///
    /// # Performance
    ///
    /// Scans each supplied coordinate once: `O(number of vectors * dim)` for
    /// valid rows. It allocates four `dim`-element `Vec<f32>` arrays in the
    /// returned value and performs no object-store I/O.
    ///
    /// # Examples
    ///
    /// Given rows `[0.0, 10.0]` and `[4.0, 20.0]`, dimension zero is calibrated
    /// to `[0, 4]` and dimension one to `[10, 20]`. A dimension that is always
    /// `7.0` receives a zero scale so it decodes exactly to `7.0`.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `&[&[f32]]` is a borrowed slice of borrowed rows. It avoids copying the
    /// training matrix and prevents this calibration from outliving any row.
    /// Java array references and C `float **` do not encode those lifetime and
    /// non-null guarantees in their types. The function returns `Self` by
    /// value, moving the newly allocated arrays to the caller without copying
    /// their contents.
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

    /// Compresses one full-precision vector into one byte per dimension.
    ///
    /// # Parameters
    ///
    /// - `vector`: Borrowed vector expected to have exactly [`Self::dim`]
    ///   coordinates. Values outside the calibrated interval are clamped.
    ///
    /// # Returns
    ///
    /// Returns an owned code vector in the same dimension order as the input.
    /// Each byte selects one of 256 evenly spaced reconstruction levels.
    ///
    /// # Panics
    ///
    /// Debug builds panic when `vector.len() != self.dim`. In optimized builds,
    /// a longer vector eventually indexes past the calibration arrays; a shorter
    /// vector instead produces a shorter, invalid code. Callers must validate
    /// dimension before entering this hot path.
    ///
    /// # Performance
    ///
    /// Performs `O(dim)` arithmetic, allocates exactly one result `Vec<u8>`, and
    /// performs no decoding or I/O.
    ///
    /// # Examples
    ///
    /// With a calibrated range `[0.0, 10.0]`, values below zero encode as `0`,
    /// values above ten encode as `255`, and `5.0` encodes near the midpoint.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The iterator pipeline borrows the input, carries each safe index beside
    /// its value with `enumerate`, and `collect`s newly owned bytes. It has the
    /// same asymptotic work as an indexed Java or C loop; Rust can inline the
    /// adapters without allocating intermediate collections.
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

    /// Reconstructs an approximate full-precision vector from SQ8 codes.
    ///
    /// # Parameters
    ///
    /// - `codes`: Borrowed bytes expected to contain exactly [`Self::dim`]
    ///   values produced under this calibration.
    ///
    /// # Returns
    ///
    /// Returns a newly allocated `Vec<f32>`. The result is approximate except
    /// for calibrated endpoints and constant dimensions.
    ///
    /// # Panics
    ///
    /// Debug builds panic when the code width differs from `self.dim`.
    /// Optimized builds may panic on an overlong code or return an invalid short
    /// result for an underlong code, so callers must preserve the format width.
    ///
    /// # Performance
    ///
    /// Performs `O(dim)` arithmetic and one `dim`-element allocation.
    ///
    /// # Examples
    ///
    /// For a dimension calibrated from `2.0` through `6.0`, codes `0` and `255`
    /// reconstruct the endpoints; code `127` reconstructs an interior value
    /// close to `4.0`.
    #[inline]
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        debug_assert_eq!(codes.len(), self.dim);
        codes
            .iter()
            .enumerate()
            .map(|(d, &c)| self.mins[d] + c as f32 * self.scales[d])
            .collect()
    }

    /// Encodes a borrowed batch while preserving its row order.
    ///
    /// # Parameters
    ///
    /// - `vectors`: Borrowed vector rows, each exactly [`Self::dim`] values.
    ///
    /// # Returns
    ///
    /// Returns one owned `Vec<u8>` per input row in identical order; an empty
    /// input produces an empty outer vector.
    ///
    /// # Panics
    ///
    /// Has the same dimension precondition and debug/release behavior as
    /// [`Self::encode`] for every row.
    ///
    /// # Performance
    ///
    /// Performs `O(number of vectors * dim)` work and allocates an outer vector
    /// plus one code vector per row.
    ///
    /// # Examples
    ///
    /// A four-row cluster produces four code rows, which can then be passed with
    /// the same four IDs to [`serialize_sq_cluster`].
    pub fn encode_batch(&self, vectors: &[&[f32]]) -> Vec<Vec<u8>> {
        vectors.iter().map(|v| self.encode(v)).collect()
    }

    /// Computes approximate squared Euclidean distance without allocating a
    /// reconstructed vector.
    ///
    /// "Asymmetric" means the query remains full precision while the stored
    /// candidate is reconstructed one coordinate at a time from its SQ code.
    /// Keeping the squared value preserves result ordering while avoiding a
    /// square root.
    ///
    /// # Parameters
    ///
    /// - `query`: Borrowed full-precision query of [`Self::dim`] coordinates.
    /// - `codes`: Borrowed SQ8 candidate of [`Self::dim`] bytes, encoded with
    ///   this calibration.
    ///
    /// # Returns
    ///
    /// Returns the sum of squared coordinate differences. Lower scores mean
    /// closer candidates.
    ///
    /// # Panics
    ///
    /// Debug builds reject either slice when its length differs from
    /// `self.dim`. In optimized builds, unequal widths can be truncated by
    /// `zip` or can index outside calibration arrays; callers must validate the
    /// query and artifact dimensions earlier.
    ///
    /// # Performance
    ///
    /// Performs `O(dim)` arithmetic with no heap allocation and no object-store
    /// I/O, making it suitable for the per-candidate coarse scan.
    ///
    /// # Examples
    ///
    /// A candidate encoded from the query itself should receive a smaller
    /// approximate distance than a candidate near the opposite calibration
    /// endpoints, subject to quantization error.
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

    /// Computes negated approximate dot-product similarity against an SQ8 row.
    ///
    /// # Parameters
    ///
    /// - `query`: Borrowed full-precision query of [`Self::dim`] coordinates.
    /// - `codes`: Borrowed SQ8 candidate of [`Self::dim`] bytes, encoded with
    ///   this calibration.
    ///
    /// # Returns
    ///
    /// Returns the negative dot product so all Zeppelin distance metrics share
    /// the convention that a lower score is a better match.
    ///
    /// # Panics
    ///
    /// Has the same dimension precondition and debug/release behavior as
    /// [`Self::asymmetric_l2_squared`].
    ///
    /// # Performance
    ///
    /// Performs `O(dim)` arithmetic without allocation or I/O.
    ///
    /// # Examples
    ///
    /// If two candidates point in the query's direction, the one with the
    /// larger reconstructed dot product receives the more negative, and thus
    /// better, distance.
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

    /// Computes approximate cosine distance against an SQ8 row.
    ///
    /// # Parameters
    ///
    /// - `query`: Borrowed full-precision query of [`Self::dim`] coordinates.
    /// - `codes`: Borrowed SQ8 candidate of [`Self::dim`] bytes, encoded with
    ///   this calibration.
    ///
    /// # Returns
    ///
    /// Returns `1 - cosine_similarity`, clamped through similarity to the
    /// normal range. If either vector has a near-zero norm, returns `1.0`
    /// because their direction is not useful for cosine matching.
    ///
    /// # Panics
    ///
    /// Has the same dimension precondition and debug/release behavior as
    /// [`Self::asymmetric_l2_squared`].
    ///
    /// # Performance
    ///
    /// Performs one `O(dim)` pass and a final square root, with no allocation.
    ///
    /// # Examples
    ///
    /// A reconstructed candidate parallel to a nonzero query scores near
    /// `0.0`; a perpendicular candidate scores near `1.0`.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `fold` carries the three accumulators as a tuple value. Each closure call
    /// returns the next tuple; the compiler normally keeps these `f32` values in
    /// registers, so this functional style does not imply heap allocation.
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

    /// Dispatches SQ8 scoring to the selected Zeppelin distance metric.
    ///
    /// # Parameters
    ///
    /// - `query`: Borrowed full-precision query of [`Self::dim`] coordinates.
    /// - `codes`: Borrowed SQ8 candidate of [`Self::dim`] bytes.
    /// - `metric`: Metric chosen by namespace configuration. See
    ///   [`crate::types::DistanceMetric`].
    ///
    /// # Returns
    ///
    /// Returns an approximate score with the shared lower-is-better ordering:
    /// squared L2, negated dot product, or cosine distance.
    ///
    /// # Panics
    ///
    /// Inherits the dimension preconditions of the selected scoring method.
    ///
    /// # Performance
    ///
    /// Dispatch is an exhaustive enum match followed by one `O(dim)` scan.
    ///
    /// # Examples
    ///
    /// The search path can pass a namespace's `DistanceMetric::Cosine` here
    /// rather than duplicating metric-specific branches around the cluster
    /// scan.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// Matching an enum is comparable to a Java `switch` or C `switch`, but
    /// Rust proves that every [`crate::types::DistanceMetric`] variant is
    /// handled. Adding a variant makes this code fail to compile until its
    /// scoring rule is supplied.
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

    /// Serializes the persistent part of this calibration into its legacy
    /// little-endian binary format.
    ///
    /// Derived `scales` and `inv_scales` are deliberately omitted because
    /// [`Self::from_bytes`] can reconstruct them from minima and maxima.
    ///
    /// # Returns
    ///
    /// Returns owned immutable bytes containing `4 + dim * 8` bytes: a `u32`
    /// dimension followed by one little-endian minimum/maximum `f32` pair per
    /// coordinate.
    ///
    /// # Panics
    ///
    /// May panic if this value violates its internal invariant that `mins` and
    /// `maxs` each contain `dim` entries. Dimensions larger than `u32::MAX` are
    /// also outside the persisted format's representable contract.
    ///
    /// # Consistency
    ///
    /// This method only constructs bytes. A builder must store them under the
    /// correct immutable segment and the manifest must publish that segment
    /// before readers may treat the calibration as visible.
    ///
    /// # Performance
    ///
    /// Allocates one exactly sized buffer and writes `O(dim)` values.
    /// Converting the `Vec<u8>` to [`Bytes`] transfers ownership of its storage
    /// without copying every byte.
    ///
    /// # Examples
    ///
    /// A two-dimensional calibration emits a four-byte dimension and four
    /// floats, for a total of 20 bytes. Deserializing those bytes recreates the
    /// derived scale arrays.
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

    /// Parses persisted SQ calibration bytes and rebuilds its derived scales.
    ///
    /// # Parameters
    ///
    /// - `data`: Borrowed bytes expected to begin with the calibration format
    ///   documented at module level. Trailing bytes are currently ignored.
    ///
    /// # Returns
    ///
    /// Returns an owned calibration with reconstructed scale arrays.
    ///
    /// # Errors
    ///
    /// Returns [`ZeppelinError::Index`] when the input lacks the four-byte
    /// header or the declared minimum/maximum pairs. Parsing fails before any
    /// remote or shared state changes. The current parser has no magic,
    /// version, checksum, or semantic validation of finite/ordered ranges.
    ///
    /// # Performance
    ///
    /// Allocates four arrays of `dim` floats and performs `O(dim)` parsing and
    /// scale calculation. The dimension header controls these allocations, so
    /// callers should only pass bytes from trusted, size-bounded segment
    /// artifacts.
    ///
    /// # Examples
    ///
    /// Bytes produced by [`Self::to_bytes`] round-trip the dimension, minima,
    /// and maxima. A blob declaring four dimensions but containing only two
    /// range pairs returns an error rather than a partial calibration.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `try_into()` converts a four-byte slice to `[u8; 4]` with a checked
    /// length, and `?` immediately returns the mapped error. This resembles a
    /// checked Java parser or explicit C error branch, but Rust's [`Result`]
    /// requires the caller to handle or propagate failure. All output arrays
    /// remain local and are dropped automatically on an early return.
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

/// Serializes ordered vector IDs and SQ8 rows into one cluster payload.
///
/// The row position joins each ID to its code; there is no separate row index
/// in the format. This payload may be stored as a legacy SQ sidecar or embedded
/// within a newer co-located cluster object.
///
/// # Parameters
///
/// - `ids`: Borrowed UTF-8 vector identifiers in cluster row order.
/// - `codes`: Borrowed SQ8 rows in exactly the same order and count as `ids`.
///   Every row must contain exactly `dim` bytes.
/// - `dim`: Persisted code width and vector dimension.
///
/// # Returns
///
/// Returns immutable bytes beginning with the row count and dimension, followed
/// by length-prefixed IDs and fixed-width code rows.
///
/// # Errors
///
/// The current implementation always returns `Ok`; the [`Result`] keeps the
/// serializer consistent with other index-format builders. In particular, it
/// does **not** validate counts, widths, or conversion to the format's `u32`
/// fields.
///
/// # Consistency
///
/// `ids.iter().zip(codes)` stops at the shorter input. A mismatch therefore
/// creates bytes whose header count may not match the emitted rows, and a wrong
/// code width shifts every following row. Callers must validate both invariants
/// before serialization. Constructing bytes neither writes nor publishes an
/// object; visibility still requires an immutable segment and manifest update.
///
/// # Performance
///
/// Copies every ID and code byte into one growing `Vec<u8>`, then transfers that
/// allocation into [`Bytes`]. Work and output size are `O(total ID bytes +
/// number of rows * dim)`.
///
/// # Examples
///
/// IDs `["a", "b"]` and two four-byte codes produce a two-row payload. Passing
/// only one code would still declare two rows but write one, so such mismatched
/// input must be rejected by the caller before this helper.
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

/// Owns the IDs and SQ8 rows parsed from one cluster payload.
///
/// Corresponding positions in [`Self::ids`] and [`Self::codes`] describe the
/// same candidate. Search code relies on that alignment when applying filters
/// and attaching approximate scores.
///
/// # Examples
///
/// `ids[3]` names the candidate encoded by `codes[3]`; reordering only one
/// vector would silently associate scores with the wrong ID.
#[derive(Debug)]
pub struct SqClusterData {
    /// Owned vector identifiers in the cluster's persisted row order.
    pub ids: Vec<String>,
    /// Owned one-byte-per-dimension codes aligned positionally with [`Self::ids`].
    pub codes: Vec<Vec<u8>>,
}

/// Parses one SQ8 cluster payload into owned IDs and code rows.
///
/// # Parameters
///
/// - `data`: Borrowed bytes in the module-level cluster format. The parser uses
///   the header's row count and dimension and currently ignores trailing bytes.
///
/// # Returns
///
/// Returns [`SqClusterData`] with exactly the declared number of IDs and code
/// rows, in persisted order.
///
/// # Errors
///
/// Returns [`ZeppelinError::Index`] if the header, an ID length, an ID payload,
/// or a fixed-width code row is truncated. Invalid UTF-8 is not an error:
/// [`String::from_utf8_lossy`] replaces malformed sequences with the Unicode
/// replacement character, which is the current compatibility behavior.
///
/// # Performance
///
/// Allocates both outer vectors, one `String` per row, and one `Vec<u8>` per
/// code. Parsing copies all IDs and codes and is linear in consumed bytes. The
/// untrusted header controls capacity, so callers should enforce artifact size
/// limits before parsing object-store data.
///
/// # Examples
///
/// A payload declaring two four-dimensional rows yields two IDs and two
/// four-byte codes. If the last code contains only three bytes, parsing returns
/// an error and no partial cluster escapes.
///
/// # Rust Notes for Java/C Engineers
///
/// The input is borrowed, but the result owns its strings and code buffers, so
/// callers can drop the source [`Bytes`] after this function returns. Rust's
/// bounds-checked slices replace the manual pointer arithmetic a C parser would
/// require; each early `Err` automatically drops already allocated rows.
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

/// Owns SQ8 code rows parsed from a codes-only cluster payload.
///
/// The codes-only payload carries no row IDs; callers join each row to the
/// separately persisted ID block by position. [`Self::codes`] retains the
/// persisted row order, so `codes[r]` belongs to the `r`-th ID of the matching
/// ID block.
///
/// # Examples
///
/// A three-row payload yields exactly three code rows; attaching scores to IDs
/// requires fetching the sibling ID block for the same cluster.
#[derive(Debug)]
pub struct SqCodesOnlyData {
    /// Persisted code width and vector dimension.
    pub dim: usize,
    /// Owned one-byte-per-dimension codes in persisted row order.
    pub codes: Vec<Vec<u8>>,
}

/// Serializes SQ8 code rows without IDs into one fixed-stride payload.
///
/// This is the coarse-block payload for `ZBP5` grouped objects. Row position
/// joins each code to the separately persisted ID block, so the format stores
/// only `[row_count: u32][dimension: u32]` followed by exactly
/// `row_count * dim` code bytes.
///
/// # Parameters
///
/// - `codes`: Borrowed SQ8 rows in cluster row order. Every row must contain
///   exactly `dim` bytes.
/// - `dim`: Persisted code width and vector dimension.
///
/// # Returns
///
/// Returns immutable bytes with the row count, dimension, and concatenated code
/// rows. No IDs are emitted.
///
/// # Errors
///
/// Returns [`ZeppelinError::Index`] when a row's width differs from `dim`, when
/// the row count or dimension does not fit the format's `u32` fields, or when
/// the total size arithmetic overflows. Unlike [`serialize_sq_cluster`], this
/// serializer validates row widths because a shifted fixed-stride row corrupts
/// every following row silently.
///
/// # Consistency
///
/// Constructing bytes neither writes nor publishes an object; visibility still
/// requires an immutable segment and manifest update.
///
/// # Performance
///
/// Allocates one exactly sized buffer and copies every code byte once.
///
/// # Examples
///
/// Three eight-byte codes produce `8 + 3 * 8 = 32` bytes. Deserializing them
/// with [`deserialize_sq_codes_only`] recovers the same three rows.
pub fn serialize_sq_codes_only(codes: &[Vec<u8>], dim: usize) -> Result<Bytes> {
    let row_count = u32::try_from(codes.len()).map_err(|_| {
        ZeppelinError::Index(format!(
            "SQ codes-only row count does not fit in u32: {}",
            codes.len()
        ))
    })?;
    let dimension = u32::try_from(dim).map_err(|_| {
        ZeppelinError::Index(format!(
            "SQ codes-only dimension does not fit in u32: {dim}"
        ))
    })?;
    let total = codes
        .len()
        .checked_mul(dim)
        .and_then(|payload| payload.checked_add(8))
        .ok_or_else(|| ZeppelinError::Index("SQ codes-only payload size overflows".into()))?;

    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&row_count.to_le_bytes());
    buf.extend_from_slice(&dimension.to_le_bytes());
    for (row, code) in codes.iter().enumerate() {
        if code.len() != dim {
            return Err(ZeppelinError::Index(format!(
                "SQ codes-only row {row} width mismatch: expected {dim}, got {}",
                code.len()
            )));
        }
        buf.extend_from_slice(code);
    }
    debug_assert_eq!(buf.len(), total);

    Ok(Bytes::from(buf))
}

/// Parses one codes-only SQ8 payload into owned code rows.
///
/// # Parameters
///
/// - `data`: Borrowed bytes in the codes-only format produced by
///   [`serialize_sq_codes_only`].
///
/// # Returns
///
/// Returns [`SqCodesOnlyData`] with exactly the declared number of fixed-width
/// code rows, in persisted order. No IDs are present to decode.
///
/// # Errors
///
/// Returns [`ZeppelinError::Index`] when the header is truncated, the size
/// arithmetic overflows, or the payload length differs from exactly
/// `8 + row_count * dim`. Trailing and missing bytes are both rejected: a
/// fixed-stride artifact has an exact length.
///
/// # Performance
///
/// Allocates one outer vector and one `Vec<u8>` per row; parsing is linear in
/// the payload size.
///
/// # Examples
///
/// Bytes from [`serialize_sq_codes_only`] round-trip the code rows. Appending
/// one byte is an error, not an ignored suffix.
pub fn deserialize_sq_codes_only(data: &[u8]) -> Result<SqCodesOnlyData> {
    if data.len() < 8 {
        return Err(ZeppelinError::Index(
            "SQ codes-only blob too small for header".into(),
        ));
    }
    let row_count = u32::from_le_bytes(
        data[0..4]
            .try_into()
            .map_err(|_| ZeppelinError::Index("SQ codes-only header parse error".into()))?,
    ) as usize;
    let dim = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("SQ codes-only header parse error".into()))?,
    ) as usize;

    let expected = row_count
        .checked_mul(dim)
        .and_then(|payload| payload.checked_add(8))
        .ok_or_else(|| ZeppelinError::Index("SQ codes-only payload size overflows".into()))?;
    if data.len() != expected {
        return Err(ZeppelinError::Index(format!(
            "SQ codes-only blob size mismatch: expected {expected}, got {}",
            data.len()
        )));
    }

    let mut codes = Vec::with_capacity(row_count);
    let mut offset = 8;
    for _ in 0..row_count {
        codes.push(data[offset..offset + dim].to_vec());
        offset += dim;
    }

    Ok(SqCodesOnlyData { dim, codes })
}

/// Builds the legacy object-store key for one segment's SQ calibration.
///
/// # Parameters
///
/// - `namespace`: Already validated namespace key prefix.
/// - `segment_id`: Immutable segment identifier and key path component.
///
/// # Returns
///
/// Returns `<namespace>/segments/<segment_id>/sq_calibration.bin` without a
/// leading slash.
///
/// # Consistency
///
/// This pure formatter performs no S3/MinIO request and does not make an
/// artifact visible. It does not escape or validate either component; callers
/// must use the namespace and segment identifiers associated with the
/// authoritative manifest. New co-located layouts may embed calibration rather
/// than write this legacy key.
///
/// # Examples
///
/// Namespace `catalog` and segment `01ABC` produce
/// `catalog/segments/01ABC/sq_calibration.bin`.
pub fn sq_calibration_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/sq_calibration.bin")
}

/// Builds the legacy object-store key for one SQ8 cluster sidecar.
///
/// # Parameters
///
/// - `namespace`: Already validated namespace key prefix.
/// - `segment_id`: Segment that physically owns the cluster; carried clusters
///   may be owned by an older segment than the active manifest entry.
/// - `cluster_idx`: Zero-based cluster number within the segment index.
///
/// # Returns
///
/// Returns `<namespace>/segments/<segment_id>/sq_cluster_<cluster_idx>.bin`.
///
/// # Consistency
///
/// This pure formatter neither checks object existence nor performs I/O.
/// Callers must derive physical ownership from manifest metadata and use this
/// key only for legacy layouts; current co-located layouts embed SQ rows in the
/// normal cluster object.
///
/// # Examples
///
/// Cluster `3` in `catalog` segment `01ABC` maps to
/// `catalog/segments/01ABC/sq_cluster_3.bin`.
pub fn sq_cluster_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/sq_cluster_{cluster_idx}.bin")
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Exercises SQ calibration, approximate scoring, persisted formats, and
    //! malformed-input rejection with deterministic in-memory vectors.

    use super::*;
    use crate::types::DistanceMetric;

    /// Verifies a hostile row count in a tiny payload is rejected without
    /// reserving row-count-proportional memory.
    ///
    /// Before the reservation cap, an 8-byte blob claiming `u32::MAX` rows
    /// requested a multi-gigabyte allocation (aborting the process) before any
    /// per-row validation ran.
    #[test]
    fn test_deserialize_sq_cluster_rejects_hostile_row_count() {
        let mut data = Vec::new();
        data.extend_from_slice(&u32::MAX.to_le_bytes());
        data.extend_from_slice(&4u32.to_le_bytes());
        assert!(deserialize_sq_cluster(&data).is_err());
    }

    /// Returns four small rows whose per-dimension extrema are easy to verify by
    /// hand across calibration and round-trip tests.
    fn sample_vectors() -> Vec<Vec<f32>> {
        vec![
            vec![0.0, 1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0, 7.0],
            vec![1.0, 3.0, 5.0, 7.0],
            vec![2.0, 2.0, 2.0, 2.0],
        ]
    }

    /// Verifies that calibration records independent minima and maxima for all
    /// four dimensions.
    #[test]
    fn test_calibration() {
        let vecs = sample_vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let cal = SqCalibration::calibrate(&refs, 4);

        assert_eq!(cal.dim, 4);
        assert_eq!(cal.mins, vec![0.0, 1.0, 2.0, 2.0]);
        assert_eq!(cal.maxs, vec![4.0, 5.0, 6.0, 7.0]);
    }

    /// Verifies that each sample reconstructs within one quantization step after
    /// encoding and decoding.
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

    /// Verifies that calibrated minima and maxima select byte codes `0` and
    /// `255`, respectively.
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

    /// Verifies the zero-scale rule for a dimension whose observed value never
    /// changes.
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

    /// Verifies that approximate Euclidean scoring ranks the query's own encoded
    /// row ahead of a distant sample.
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

    /// Verifies that all configured metric dispatch branches return finite
    /// scores for an ordinary query and SQ row.
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

    /// Verifies that persisted calibration bytes preserve the dimension and
    /// extrema needed to rebuild derived scales.
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

    /// Verifies that a calibration payload shorter than its header fails loudly.
    #[test]
    fn test_calibration_from_bytes_too_small() {
        assert!(SqCalibration::from_bytes(&[0u8; 2]).is_err());
    }

    /// Verifies that a declared dimension cannot be satisfied by a truncated
    /// list of minimum/maximum pairs.
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

    /// Verifies that cluster serialization preserves positional ID/code row
    /// alignment across a round trip.
    #[test]
    fn test_sq_cluster_serde_roundtrip() {
        let ids = vec!["v1".to_string(), "v2".to_string()];
        let codes = vec![vec![0u8, 128, 255, 64], vec![10, 20, 30, 40]];
        let data = serialize_sq_cluster(&ids, &codes, 4).unwrap();
        let decoded = deserialize_sq_cluster(&data).unwrap();
        assert_eq!(decoded.ids, ids);
        assert_eq!(decoded.codes, codes);
    }

    /// Verifies that a cluster payload without its complete header is rejected.
    #[test]
    fn test_sq_cluster_from_bytes_too_small() {
        assert!(deserialize_sq_cluster(&[0u8; 4]).is_err());
    }

    /// Verifies that batch encoding returns one correctly sized code row per
    /// borrowed input vector.
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
