//! Implements RaBitQ bit-level residual quantization, the estimator behind the
//! resident coarse sketch.
//!
//! ## What RaBitQ is
//!
//! Product and scalar quantization keep a small *approximation of the vector*
//! and score candidates by reconstructing it. RaBitQ keeps something much
//! smaller and reconstructs nothing: for each corpus vector it stores only the
//! **sign** of every coordinate of the vector's residual against its IVF
//! centroid, plus two `f32` correction scalars. A "residual" here is
//! `v - c`, the offset from the cluster centroid `c` that IVF already chose for
//! the row; centering on the centroid removes the bulk of the shared magnitude
//! so the remaining direction is what actually discriminates neighbors.
//!
//! One sign bit per dimension is a brutal amount of information loss, and it
//! only works because of two ideas:
//!
//! 1. **Random rotation.** Before encoding, the residual is passed through a
//!    fixed random orthogonal transform ([`StructuredRotation`]). Rotation
//!    preserves every norm and inner product, so no geometry is lost, but it
//!    spreads the residual's energy evenly across coordinates. That makes the
//!    sign pattern an unbiased, well-conditioned sketch of the direction
//!    instead of a sketch of whatever axes the embedding model happened to
//!    favor.
//! 2. **An unbiased estimator, not a reconstruction.** The stored sign vector
//!    `h` (with `h_i` in `{-1, +1}`) is treated as a unit vector `h / sqrt(D)`.
//!    Combined with the two stored scalars — the residual's length
//!    `||r||` and its alignment `<h / sqrt(D), r>` — the inner product between
//!    a stored row and a query is estimated as
//!    `||r||^2 * <h/sqrt(D), q_hat> / <h/sqrt(D), r>`. The error of that
//!    estimate has mean zero and shrinks like `1/sqrt(D)`; the tests in this
//!    file pin both properties.
//!
//! The query side is *not* quantized to one bit. Scoring is asymmetric
//! (ADC — asymmetric distance computation): the stored row stays at 1 or 2 bits
//! while the query is quantized to 4 bits per dimension
//! ([`prepare_query_adc4`]) and packed into four bit planes. Every dot product
//! then reduces to `AND` plus `count_ones` over `u64` words, which is why a
//! sketch scan is cheap enough to run over an entire probe set in memory.
//!
//! **Extended RaBitQ (two bits)** replaces the sign grid `{-1, +1}` with the
//! four-level grid `{-1.5, -0.5, +0.5, +1.5}` per coordinate
//! ([`encode_two_bit_into`]) and searches for the grid direction best aligned
//! with the true residual. It costs one extra bit plane per row and is what the
//! production sketch format writes.
//!
//! ### Size and recall tradeoff
//!
//! For a 768-dimensional embedding, one full-precision row is 3,072 bytes. A
//! one-bit code is 96 bytes of planes plus 8 bytes of scalars (104 total, ~30x
//! smaller); a two-bit code is 192 plus 8 (200 total, ~15x smaller). Those
//! savings buy a *ranking* signal only. Scores produced here are approximate
//! and are used to choose which clusters and rows deserve a full-precision
//! read; the authoritative distance is always recomputed from full vectors
//! afterwards.
//!
//! ## What this file owns, and what it does not
//!
//! This file owns the rotation, the encoders, the query quantizer, the
//! popcount estimators, and a deterministic offline oracle that proves the
//! cheap structured rotation is as good as a dense random one. It owns no I/O,
//! no object layout, no cluster selection, and no visibility decision.
//!
//! - `crate::index::ivf_flat::sketch` calls into this file. It owns the ZSK1
//!   binary format, zero-pads embedding dimensions up to a [`BLOCK_DIM`]
//!   multiple, persists the rotation seed, groups rows by cluster, and turns
//!   row scores into cluster rankings.
//! - `crate::compaction` and `crate::index::ivf_flat::build` decide when a
//!   sketch is built or stitched; `crate::storage` performs every S3/MinIO
//!   request.
//! - **The index module never decides visibility.** Compaction publishes and
//!   query selects. An encoded row exists only inside an immutable artifact,
//!   and only a manifest publication makes that artifact readable.
//! - `src/bin/quant_bakeoff.rs` includes this exact file a second time through
//!   `#[path]`. That is why the file imports nothing from `crate::` and
//!   defines its own [`RabitqError`] instead of using `ZeppelinError`;
//!   `src/error.rs` provides the `From` conversion for the library build.
//!   **Keep this file free of `crate::` dependencies** or the offline bake-off
//!   binary stops compiling.
//!
//! Every entry point returns `Result`. Dimension, length, finiteness, and
//! query-norm violations are reported as errors rather than clamped, padded, or
//! silently ignored — a mis-sized code is a corrupted artifact, not a
//! degraded one.
//!
//! ## Encode and query paths
//!
//! ```text
//! build / compaction                          query
//! ------------------                          -----
//! vector v, IVF centroid c                    query q, same centroid c
//!         |                                             |
//!         | r = v - c                                   | r_q = q - c
//!         v                                             v
//! StructuredRotation::rotate_residual         StructuredRotation::rotate_in_place
//!   3 rounds of:                                (same seed => same transform)
//!     Rademacher sign flip                              |
//!     normalized 256-wide FHT                           v
//!     whole-vector permutation                  prepare_query_adc4
//!         |                                       4-bit stochastic rounding
//!         v                                       -> 4 packed bit planes
//! encode_one_bit_into  (1 plane)                        |
//! encode_two_bit_into  (2 planes)                       |
//!   + residual_norm, bar_dot_residual                   |
//!         |                                             |
//!         | stored in an immutable sketch object        |
//!         +---------------------+-----------------------+
//!                               |
//!                               v
//!               estimate_residual_dot_*_parts
//!               AND + count_ones over u64 words
//!                               |
//!                               v
//!               estimate_l2_*_parts  ->  approximate squared L2
//!                               |
//!                               v
//!               candidate ordering only; the exact rerank
//!               against full-precision vectors happens
//!               outside this module
//! ```
//!
//! ## Reading map
//!
//! 1. [`BLOCK_DIM`] and [`StructuredRotation`] for the no-padding orthogonal
//!    transform every other entry point assumes has already been applied.
//! 2. [`encode_one_bit_into`] and [`encode_two_bit_into`] for the write path,
//!    including the `O(D log D)` grid-direction search that makes the two-bit
//!    code exact rather than a rounding.
//! 3. [`prepare_query_adc4`] for the 4-bit query representation, then
//!    `QueryAdc4::signed_dot` and `QueryAdc4::two_bit_grid_dot` for the
//!    popcount kernels.
//! 4. [`estimate_residual_dot_one_bit_parts`] and
//!    [`estimate_l2_two_bit_parts`] for the estimator the search path actually
//!    calls; the owned-code wrappers above them are convenience shells.
//! 5. [`compare_structured_dense_quality_768`] and [`RotationQuality`] for the
//!    offline oracle. This is reporting/bake-off machinery, never a serving
//!    path — it allocates a dense 768x768 matrix.
//!
//! ## Measured findings
//!
//! These are results from the July 2026 quantization bake-off recorded in
//! `src/index/CLAUDE.md`, not general claims about the RaBitQ algorithm. They
//! are stated with their conditions because they do not transfer across
//! embedding models or dimensionalities:
//!
//! - On `wiki_dpr_e5` (768-dimensional E5 embeddings), the **one-bit** code
//!   retains only about **95%** of the exact in-probe recall ceiling. That is
//!   **insufficient** for the recall gate. Do not re-propose one-bit codes for
//!   768-dimensional embeddings without new measured evidence.
//! - The **two-bit** Extended-RaBitQ code retains **at least 99.3%** on the
//!   same data and passes the gate at `nprobe` 32. `nprobe` is the number of
//!   IVF clusters a query probes; recall is the fraction of true nearest
//!   neighbors returned.
//! - `tests/ivf_recall_gate.rs` is the quality authority. Unit tests in this
//!   file pin the estimator's statistical properties (unbiasedness,
//!   `1/sqrt(D)` error decay, two-bit RMSE improvement), but they do not
//!   establish retrieval quality. Any change to the numerics here must be
//!   re-measured against that gate on both `wikidpr1m` and `wikidpr2m`.
//!
//! ## Invariants
//!
//! - Every dimension reaching this file is a non-zero multiple of
//!   [`BLOCK_DIM`]. Callers zero-pad before encoding. This is not cosmetic: it
//!   makes `dim / 64` exact, so every bit of every packed `u64` word is a real
//!   coordinate and whole-word popcounts need no tail masking.
//! - A stored code and the query it is scored against must come from the same
//!   rotation (same seed and scheme) and the same centroid. Mixing them
//!   produces a plausible number with no meaning.
//! - Encoding and estimation are pure and deterministic given their inputs and
//!   seeds. Nothing here mutates shared state, performs I/O, or spawns work.
//! - Both encoders take a rotated residual. Passing an unrotated vector is not
//!   detectable here and silently degrades the estimator.
//!
//! ## Rust concepts used here
//!
//! **Bit packing.** Codes are `Vec<u64>` bit planes rather than `Vec<bool>` or
//! byte arrays. Coordinate `i` lives at bit `i % 64` of word `i / 64`, so a
//! 768-dimensional sign vector is twelve `u64` values. The estimators never
//! unpack them: `(stored & query).count_ones()` compiles to a hardware
//! `popcnt`, so a dot product over 768 dimensions is a dozen AND/POPCNT pairs.
//! A C engineer will recognize the layout as a manual bitset; Java's `long[]`
//! plus `Long.bitCount` is the closest analogue. Rust adds no per-element
//! bounds cost here because the loops are iterator `zip` pipelines over slices
//! whose lengths were validated once at the top of the function.
//!
//! **Slices and caller-owned scratch.** Every hot entry point comes in two
//! flavors: an owned one ([`encode_two_bit`]) that allocates its result, and a
//! `_into` / `_parts` one ([`encode_two_bit_into`],
//! [`estimate_l2_two_bit_parts`]) that borrows `&mut [u64]` output buffers and
//! `&[u64]` inputs. Encoding runs once per corpus row, so the borrowed form
//! lets a builder allocate one flat buffer for a whole cluster and write
//! sub-slices of it, avoiding a `Vec` allocation per vector. In C this is the
//! familiar "caller supplies the output buffer" convention; the difference is
//! that Rust proves the buffer outlives the call and that the `&mut` slice is
//! not aliased by any of the `&` inputs, so the compiler is free to keep values
//! in registers across the loop.
//!
//! **Alignment.** Nothing in this file transmutes or reinterprets bytes. The
//! planes are real `u64` slices with natural alignment, and the conversion to
//! and from the on-disk little-endian byte form happens in
//! `crate::index::ivf_flat::sketch`, which reads and writes each word
//! explicitly. That keeps this file portable and free of `unsafe`.
//!
//! **No `unsafe`, no explicit SIMD.** The performance comes from `count_ones`
//! intrinsics and from `chunks_exact_mut`, which yields fixed-size blocks the
//! optimizer can vectorize without the bounds checks a manual index loop would
//! carry. `f32::mul_add` is used where a fused multiply-add both saves an
//! instruction and avoids one rounding step.
//!
//! **Errors as data.** [`RabitqError`] is a `thiserror` enum whose variants
//! carry the offending lengths, indices, and measured values. A Java engineer
//! can read it as a small closed hierarchy of exceptions; unlike an exception
//! it is an ordinary value that must be handled or propagated with `?`, and the
//! compiler enforces that a caller cannot ignore it.

use thiserror::Error;

/// The power-of-two block used by the no-padding structured rotation.
///
/// A fast Hadamard transform needs a power-of-two length, but common embedding
/// dimensions such as 768 and 1,536 are not powers of two. Padding a 768-wide
/// vector out to 1,024 would waste a third of every code. Instead the transform
/// is applied to independent 256-coordinate blocks and cross-block mixing is
/// supplied by a whole-vector permutation, so any multiple of 256 is handled
/// with no padding at all.
///
/// Every dimension accepted by this module must therefore be a non-zero
/// multiple of this constant. Callers such as `crate::index::ivf_flat::sketch`
/// zero-pad the embedding dimension up to the next multiple before encoding.
/// Because 256 is also a multiple of 64, `dim / 64` is exact and every bit of
/// every packed word corresponds to a real coordinate.
pub const BLOCK_DIM: usize = 256;

/// Three rounds provide cross-block mixing while keeping the transform cheap.
///
/// One round mixes coordinates only within a 256-wide block; the permutation at
/// the end of the round is what moves information between blocks, so a single
/// round leaves the transform effectively block-diagonal. Three rounds give
/// every coordinate a path to every other coordinate while keeping the cost at
/// roughly thirty arithmetic operations per coordinate, against `D` operations
/// per coordinate for a dense random rotation.
///
/// [`compare_structured_dense_quality_768`] is the offline evidence that this
/// count is sufficient: it measures the resulting estimator error against a
/// dense Haar-like rotation on a deliberately anisotropic, non-rotationally
/// invariant distribution.
pub const ROTATION_ROUNDS: usize = 3;

/// Reports a violated precondition or a failed offline rotation-quality check.
///
/// Every variant describes a caller mistake or a measurement that did not meet
/// its pinned bound; none describes a recoverable condition. In the library
/// build `src/error.rs` converts this into `ZeppelinError::Rabitq`, and in the
/// offline `quant_bakeoff` binary it is converted into that binary's own error
/// type. The variants carry the offending values so a failure is diagnosable
/// from the message alone without re-running with a debugger.
///
/// The type is `Clone` so a caller batching many rows can retain a failure
/// alongside partial results; cloning copies only small scalars and `&'static
/// str` labels, never a heap buffer.
#[derive(Debug, Clone, Error)]
pub enum RabitqError {
    /// The vector dimension is zero or is not a multiple of [`BLOCK_DIM`].
    ///
    /// Raised by [`StructuredRotation::new`], [`words_per_code`], and both
    /// encoders. The caller is responsible for zero-padding embeddings up to a
    /// block boundary before reaching this module.
    #[error("RaBitQ dimension {dim} must be a non-zero multiple of {BLOCK_DIM}")]
    InvalidDimension {
        /// The rejected dimension, in coordinates.
        dim: usize,
    },
    /// A buffer did not have the exact length its role requires.
    ///
    /// This is the guard that keeps a stored code, a query representation, a
    /// scratch buffer, and a centroid from being combined at mismatched
    /// dimensions. It is an exact-equality check, never a "large enough"
    /// check, because a longer buffer would silently change what a whole-word
    /// popcount counts.
    #[error("{name} length mismatch: expected {expected}, got {actual}")]
    LengthMismatch {
        /// Static label identifying which buffer was wrong, for example
        /// `"rotation scratch"` or `"two-bit high plane"`.
        name: &'static str,
        /// Length the operation requires, in elements of that buffer's type.
        expected: usize,
        /// Length the caller actually supplied.
        actual: usize,
    },
    /// An input vector contained a `NaN` or an infinity.
    ///
    /// Non-finite coordinates would propagate through the norm and correction
    /// scalars and poison every score derived from the row, so encoding
    /// refuses them rather than producing a code that ranks arbitrarily.
    #[error("{name} contains a non-finite value at coordinate {index}")]
    NonFiniteValue {
        /// Static label identifying the rejected input.
        name: &'static str,
        /// Zero-based index of the first non-finite coordinate found.
        index: usize,
    },
    /// The caller-supplied squared query residual norm was negative or
    /// non-finite.
    ///
    /// The squared-L2 estimators add this term directly, so an invalid value
    /// would produce a distance that is not comparable with any other
    /// candidate's.
    #[error("query residual norm squared must be finite and non-negative, got {value}")]
    InvalidQueryNorm {
        /// The rejected `||q - c||^2` value.
        value: f32,
    },
    /// The offline oracle found the structured rotation statistically worse
    /// than a dense random rotation.
    ///
    /// Returned by [`compare_structured_dense_quality_768`] when the paired
    /// mean squared-error difference exceeds five standard errors, which is the
    /// evidence that the cheap blocked transform is no longer an acceptable
    /// substitute for an `O(D^2)` one.
    #[error(
        "structured rotation MSE delta {mse_delta} exceeds five standard errors ({standard_error}); structured RMSE {structured_rmse}, dense RMSE {dense_rmse}"
    )]
    StructuredDenseQualityMismatch {
        /// Mean of the paired `structured MSE - dense MSE` samples.
        mse_delta: f64,
        /// Standard error of that paired mean over the sample count.
        standard_error: f64,
        /// Root mean squared estimator error under the structured rotation.
        structured_rmse: f64,
        /// Root mean squared estimator error under the dense rotation.
        dense_rmse: f64,
    },
    /// The offline oracle found the structured rotation no better than applying
    /// no rotation at all.
    ///
    /// Returned by [`compare_structured_dense_quality_768`] when structured
    /// RMSE is not at most [`ROTATION_IDENTITY_MAX_RMSE_RATIO`] of identity
    /// RMSE. This is the check that catches a transform whose cross-block
    /// mixing has been broken: without it, a rotation that does nothing useful
    /// could still pass the dense comparison by being equally bad.
    #[error(
        "structured rotation RMSE {structured_rmse} is not at most {maximum_ratio} of identity RMSE {identity_rmse}"
    )]
    StructuredIdentityQualityMismatch {
        /// Root mean squared estimator error under the structured rotation.
        structured_rmse: f64,
        /// Root mean squared estimator error with no rotation applied.
        identity_rmse: f64,
        /// The pinned bound, [`ROTATION_IDENTITY_MAX_RMSE_RATIO`].
        maximum_ratio: f64,
    },
}

/// Holds the expanded randomness for one of the [`ROTATION_ROUNDS`] rounds.
///
/// The three stages of a round are each individually orthogonal, so their
/// composition is orthogonal and preserves norms and inner products exactly (up
/// to `f32` rounding). Both fields are derived deterministically from the
/// rotation seed and are never persisted: an artifact stores the seed, and a
/// reader rebuilds identical rounds from it.
#[derive(Debug, Clone)]
struct RotationRound {
    /// Rademacher diagonal packed one sign per bit: a set bit at position `i`
    /// negates coordinate `i`. `dim.div_ceil(64)` words, which is exact
    /// because `dim` is a multiple of [`BLOCK_DIM`].
    sign_words: Vec<u64>,
    /// Scatter convention: input coordinate `i` is written to `permutation[i]`.
    permutation: Vec<usize>,
}

/// An orthogonal, no-padding rotation for dimensions divisible by 256.
///
/// Each round applies an independently seeded Rademacher diagonal, a
/// normalized FHT to every 256-coordinate block, and a full-coordinate
/// permutation. All randomness is expanded deterministically from `seed`.
///
/// RaBitQ's error guarantees assume the residual direction is generic with
/// respect to the coding grid. Real embeddings are not: energy concentrates on
/// a minority of coordinates. This transform removes that structure without
/// changing any distance, because an orthogonal map preserves every norm and
/// inner product. It costs roughly thirty operations per coordinate, against
/// `D` per coordinate for the dense random rotation the technique is usually
/// described with.
///
/// The value is cheap to share but not free to clone: a clone allocates fresh
/// `sign_words` and `permutation` vectors for every round, so a builder should
/// construct one rotation per artifact and pass it by reference.
///
/// # Examples
///
/// A segment of 768-dimensional embeddings persists only the seed. Both the
/// compaction path that encoded the rows and the query path that scores them
/// call `StructuredRotation::new(768, seed)` and obtain byte-identical
/// transforms; a code encoded under one seed and scored under another yields a
/// meaningless number, which is why the sketch format records the seed and
/// scheme version alongside the rows.
#[derive(Debug, Clone)]
pub struct StructuredRotation {
    /// Vector length this rotation is defined for, in coordinates. Always a
    /// non-zero multiple of [`BLOCK_DIM`].
    dim: usize,
    /// The [`ROTATION_ROUNDS`] rounds, applied in order.
    rounds: Vec<RotationRound>,
}

impl StructuredRotation {
    /// Builds the deterministic rotation for `dim` coordinates from `seed`.
    ///
    /// All randomness — the Rademacher signs and the Fisher-Yates permutation
    /// of every round — is expanded from `seed` with SplitMix64, so two
    /// processes that agree on `dim` and `seed` produce identical transforms
    /// with no shared state and no persisted matrix.
    ///
    /// # Parameters
    ///
    /// - `dim`: Vector length in coordinates. Must be a non-zero multiple of
    ///   [`BLOCK_DIM`]; callers zero-pad shorter embeddings themselves.
    /// - `seed`: Value recorded in the artifact that consumes the codes. Any
    ///   `u64` is valid; changing it invalidates every code already written.
    ///
    /// # Returns
    ///
    /// An owned rotation holding [`ROTATION_ROUNDS`] rounds of expanded
    /// randomness, sized for `dim`.
    ///
    /// # Errors
    ///
    /// Returns [`RabitqError::InvalidDimension`] when `dim` is zero or not a
    /// multiple of [`BLOCK_DIM`]. No partial state exists on failure.
    ///
    /// # Performance
    ///
    /// Allocates `ROTATION_ROUNDS` pairs of vectors — about `dim / 64` words
    /// and `dim` indices per round — and draws `O(dim)` random numbers. This is
    /// setup work performed once per artifact or per query, never per row.
    ///
    /// # Examples
    ///
    /// Constructing with `dim = 768` succeeds because 768 is three blocks of
    /// 256. Constructing with `dim = 700` fails with
    /// [`RabitqError::InvalidDimension`] rather than rounding up internally,
    /// so a caller that forgot to pad learns about it at build time instead of
    /// producing codes that no reader can interpret.
    #[allow(clippy::manual_is_multiple_of)] // `usize::is_multiple_of` is newer than the MSRV.
    pub fn new(dim: usize, seed: u64) -> Result<Self, RabitqError> {
        if dim == 0 || dim % BLOCK_DIM != 0 {
            return Err(RabitqError::InvalidDimension { dim });
        }

        let mut rng = SplitMix64::new(seed);
        let mut rounds = Vec::with_capacity(ROTATION_ROUNDS);
        for _ in 0..ROTATION_ROUNDS {
            let sign_words = (0..dim.div_ceil(64)).map(|_| rng.next_u64()).collect();
            let mut permutation: Vec<usize> = (0..dim).collect();
            for i in (1..dim).rev() {
                let j = rng.uniform_below(i + 1);
                permutation.swap(i, j);
            }
            rounds.push(RotationRound {
                sign_words,
                permutation,
            });
        }

        Ok(Self { dim, rounds })
    }

    /// Returns the vector length, in coordinates, this rotation applies to.
    ///
    /// Callers use it to size scratch buffers and to check a rotation against
    /// artifact metadata before scoring rows encoded under it.
    #[must_use]
    pub const fn dim(&self) -> usize {
        self.dim
    }

    /// Rotate `values` in place using caller-owned scratch storage.
    ///
    /// Applies all [`ROTATION_ROUNDS`] rounds. Within a round the coordinates
    /// are sign-flipped according to the round's Rademacher diagonal, each
    /// 256-coordinate block is passed through a normalized fast Hadamard
    /// transform, and the whole vector is permuted — the permutation being the
    /// only stage that moves information between blocks.
    ///
    /// The transform is orthogonal, so `||rotate(x)|| == ||x||` and
    /// `<rotate(x), rotate(y)> == <x, y>` to within `f32` rounding. This is
    /// what makes rotating both a stored residual and a query legitimate: the
    /// geometry being estimated is unchanged, only the coordinate system moved.
    ///
    /// # Parameters
    ///
    /// - `values`: The vector to transform, overwritten with the result. Must
    ///   have exactly [`dim`][Self::dim] elements.
    /// - `scratch`: Caller-owned working buffer of exactly
    ///   [`dim`][Self::dim] elements. Its prior contents are ignored and its
    ///   contents after the call are unspecified. It exists so a build loop can
    ///   rotate thousands of rows without one allocation per row.
    ///
    /// # Errors
    ///
    /// Returns [`RabitqError::LengthMismatch`] when either slice length differs
    /// from [`dim`][Self::dim]. Lengths are validated before any coordinate is
    /// touched, so `values` is unmodified on error.
    ///
    /// Non-finite inputs are *not* rejected here; the encoders
    /// ([`encode_one_bit_into`], [`encode_two_bit_into`]) perform that check on
    /// the rotated result.
    ///
    /// # Performance
    ///
    /// `O(dim * ROTATION_ROUNDS * log2(BLOCK_DIM))`: about eight butterfly
    /// operations per coordinate per round from the Hadamard transform, plus a
    /// sign flip, a scatter, and a copy. No allocation. The `chunks_exact_mut`
    /// block loop gives the optimizer fixed-size, bounds-check-free work.
    ///
    /// # Examples
    ///
    /// Rotating a 768-dimensional residual and the corresponding query residual
    /// with the same rotation leaves their inner product unchanged, so the
    /// popcount estimator downstream is measuring the same quantity it would
    /// have measured in the original basis — but over coordinates whose energy
    /// is now evenly spread, which is what the one-bit sign code requires.
    pub fn rotate_in_place(
        &self,
        values: &mut [f32],
        scratch: &mut [f32],
    ) -> Result<(), RabitqError> {
        check_len("rotation input", values.len(), self.dim)?;
        check_len("rotation scratch", scratch.len(), self.dim)?;

        for round in &self.rounds {
            for (index, value) in values.iter_mut().enumerate() {
                if ((round.sign_words[index / 64] >> (index % 64)) & 1) != 0 {
                    *value = -*value;
                }
            }
            for block in values.chunks_exact_mut(BLOCK_DIM) {
                normalized_fht_256(block);
            }
            for (source, &destination) in round.permutation.iter().enumerate() {
                scratch[destination] = values[source];
            }
            values.copy_from_slice(scratch);
        }
        Ok(())
    }

    /// Subtract a cluster centroid and rotate the residual.
    ///
    /// This is the encoder-side entry point: it computes `vector - centroid`
    /// into `output` and then applies [`rotate_in_place`][Self::rotate_in_place]
    /// to it. Centering on the IVF centroid the row already belongs to is what
    /// makes a one- or two-bit code informative — it removes the magnitude
    /// shared by every member of the cluster and leaves the offset that
    /// distinguishes them.
    ///
    /// # Parameters
    ///
    /// - `vector`: Full-precision vector, borrowed and not modified.
    /// - `centroid`: The authoritative IVF centroid for this row's cluster. A
    ///   query must later be centered on the *same* centroid, or the estimate
    ///   is meaningless.
    /// - `output`: Receives the rotated residual. Fully overwritten.
    /// - `scratch`: Working buffer for the rotation; contents after the call
    ///   are unspecified.
    ///
    /// All four slices must have exactly [`dim`][Self::dim] elements.
    ///
    /// # Errors
    ///
    /// Returns [`RabitqError::LengthMismatch`] naming the first slice whose
    /// length is wrong. All four lengths are checked before any write, so
    /// `output` is untouched on error.
    ///
    /// # Performance
    ///
    /// One `dim`-length subtraction plus the cost of
    /// [`rotate_in_place`][Self::rotate_in_place]. No allocation.
    ///
    /// # Examples
    ///
    /// During compaction, a builder holds one `output` and one `scratch`
    /// buffer and calls this once per corpus row, then hands `output` to
    /// [`encode_two_bit_into`] which writes into a sub-slice of a
    /// cluster-wide plane buffer. Neither step allocates per row.
    pub fn rotate_residual(
        &self,
        vector: &[f32],
        centroid: &[f32],
        output: &mut [f32],
        scratch: &mut [f32],
    ) -> Result<(), RabitqError> {
        check_len("vector", vector.len(), self.dim)?;
        check_len("centroid", centroid.len(), self.dim)?;
        check_len("rotated residual output", output.len(), self.dim)?;
        check_len("rotation scratch", scratch.len(), self.dim)?;
        for ((out, value), center) in output.iter_mut().zip(vector).zip(centroid) {
            *out = *value - *center;
        }
        self.rotate_in_place(output, scratch)
    }
}

/// Number of bit planes used for asymmetric query quantization.
///
/// The stored side of the comparison is squeezed to one or two bits per
/// coordinate because it is multiplied by the corpus size. The query side is
/// materialized once per cluster, so it can afford four bits — sixteen levels
/// across the query residual's observed range — which keeps the query's own
/// quantization error far below the stored code's and preserves the
/// estimator's unbiasedness.
pub const QUERY_BITS: usize = 4;

/// Largest query code value, `2^QUERY_BITS - 1`.
///
/// Query codes are integers in `0..=QUERY_LEVELS`, so the quantization step is
/// the observed range divided by this value and the reconstructed value is
/// `lower + step * code`.
const QUERY_LEVELS: u8 = (1 << QUERY_BITS) - 1;

/// A one-bit RaBitQ code over a rotated cluster residual.
///
/// This is the owned form returned by [`encode_one_bit`]: the packed sign bits
/// together with the two scalars the estimator needs. Search paths generally
/// use the borrowed form instead ([`OneBitFactors`] plus a `&[u64]` slice into
/// a cluster-wide buffer) to avoid one allocation per row.
///
/// Cloning allocates a fresh `words` vector; this is not a reference-count
/// bump.
///
/// Per `src/index/CLAUDE.md`, one-bit codes retain only about 95% of the exact
/// in-probe recall ceiling on 768-dimensional E5 embeddings, which is
/// insufficient for the recall gate. The production sketch format writes
/// [`TwoBitCode`]; the one-bit path remains for the offline bake-off and for
/// the estimator tests that pin its statistical behavior.
#[derive(Debug, Clone)]
pub struct OneBitCode {
    /// Sign plane, `dim / 64` words. Bit `i % 64` of word `i / 64` is set when
    /// rotated residual coordinate `i` was strictly positive, and clear
    /// otherwise (including for exactly zero). Read as `h_i = +1` for a set bit
    /// and `h_i = -1` for a clear one.
    pub words: Vec<u64>,
    /// Euclidean length `||r||` of the rotated residual, accumulated in `f64`
    /// and narrowed once. Zero means the vector coincided with its centroid,
    /// and the estimators short-circuit to a zero inner product.
    pub residual_norm: f32,
    /// `<h / sqrt(D), residual>` from the RaBitQ estimator.
    pub bar_dot_residual: f32,
    /// Dimension the code was produced at, a non-zero multiple of
    /// [`BLOCK_DIM`]. Kept so a caller can reject a query built at another
    /// dimension.
    dim: usize,
}

impl OneBitCode {
    /// Returns the coordinate count this code was encoded at.
    ///
    /// Used to reject a [`QueryAdc4`] built at a different dimension before any
    /// popcount is performed.
    #[must_use]
    pub const fn dim(&self) -> usize {
        self.dim
    }
}

/// Allocation-free scalar output from one-bit encoding.
///
/// [`encode_one_bit_into`] writes the bit plane into a caller-owned buffer and
/// returns these two scalars by value. Together with a `&[u64]` view of that
/// buffer they carry everything
/// [`estimate_l2_one_bit_parts`] needs, so a search path can hold one flat
/// allocation for a whole cluster instead of a `Vec` per row.
///
/// The type is [`Copy`]: it is eight bytes of plain data, so passing it to an
/// estimator copies rather than borrows and imposes no lifetime on the caller.
#[derive(Debug, Clone, Copy)]
pub struct OneBitFactors {
    /// Euclidean length `||r||` of the rotated residual.
    pub residual_norm: f32,
    /// `<h / sqrt(D), residual>`, the alignment between the unit sign vector
    /// and the true residual. Non-zero whenever `residual_norm` is non-zero,
    /// which is what makes it safe as the estimator's divisor.
    pub bar_dot_residual: f32,
}

/// A two-bit Extended-RaBitQ code. Plane 0 is the low bit and plane 1
/// is the sign/high bit of the unsigned grid code in `[0, 3]`.
///
/// Each coordinate is assigned one of four grid values `{-1.5, -0.5, +0.5,
/// +1.5}`, stored as the unsigned code `u = y + 1.5` in `0..=3` and split
/// across the two planes. Unlike a plain rounding, [`encode_two_bit_into`]
/// searches for the grid *direction* best aligned with the true residual, which
/// is why the type is called exact rather than approximate.
///
/// This is the representation the production ZSK1 v4 sketch writes: on
/// 768-dimensional E5 embeddings it retains at least 99.3% of the exact
/// in-probe recall ceiling and passes the recall gate at `nprobe` 32
/// (`src/index/CLAUDE.md`).
///
/// Cloning allocates two fresh word vectors.
#[derive(Debug, Clone)]
pub struct TwoBitCode {
    /// `[low, high]` bit planes of `dim / 64` words each. Coordinate `i`'s
    /// unsigned grid code is `bit(low, i) | (bit(high, i) << 1)`, so `0` is
    /// `-1.5`, `1` is `-0.5`, `2` is `+0.5`, and `3` is `+1.5`.
    pub planes: [Vec<u64>; 2],
    /// Euclidean length `||r||` of the rotated residual.
    pub residual_norm: f32,
    /// `<y / ||y||, residual>` for the selected half-integer grid vector.
    pub bar_dot_residual: f32,
    /// Dimension the code was produced at, a non-zero multiple of
    /// [`BLOCK_DIM`].
    dim: usize,
}

impl TwoBitCode {
    /// Returns the coordinate count this code was encoded at.
    ///
    /// Used to reject a [`QueryAdc4`] built at a different dimension before any
    /// popcount is performed.
    #[must_use]
    pub const fn dim(&self) -> usize {
        self.dim
    }
}

/// Allocation-free scalar output from exact two-bit encoding.
///
/// The borrowed counterpart to [`TwoBitCode`], returned by
/// [`encode_two_bit_into`] and consumed by
/// [`estimate_l2_two_bit_parts`]. The resident sketch stores exactly these two
/// `f32` values after each row's bit planes, so this type is also the in-memory
/// shape of that persisted row tail.
#[derive(Debug, Clone, Copy)]
pub struct TwoBitFactors {
    /// Euclidean length `||r||` of the rotated residual.
    pub residual_norm: f32,
    /// `<y / ||y||, residual>` for the grid vector chosen by the encoder's
    /// direction search.
    pub bar_dot_residual: f32,
}

/// Four bit-plane query representation used by popcount ADC.
///
/// Built once per probed cluster by [`prepare_query_adc4`] from the rotated
/// query residual, then reused for every row in that cluster. Holding the query
/// as bit planes rather than as `f32` values is what lets the estimators score
/// a row with a handful of `AND` and `count_ones` instructions instead of a
/// `dim`-length floating-point loop.
///
/// The quantization is affine and shared by all coordinates: coordinate `i`'s
/// reconstructed value is `lower + step * code_i`, with `code_i` recovered from
/// the planes. The estimators never reconstruct it explicitly; they rearrange
/// the sum so the per-coordinate codes appear only inside popcounts.
#[derive(Debug, Clone)]
pub struct QueryAdc4 {
    /// Bit plane `p` holds bit `p` of every coordinate's code, `dim / 64` words
    /// each. Plane 0 is the least significant bit.
    planes: [Vec<u64>; QUERY_BITS],
    /// Smallest observed coordinate of the rotated query residual; the value
    /// code `0` reconstructs to.
    lower: f32,
    /// Width of one quantization level, `(upper - lower) / QUERY_LEVELS`. Zero
    /// when every coordinate was identical, in which case all codes are `0`.
    step: f32,
    /// Sum of all `dim` codes, precomputed so the estimators can center the
    /// popcount sums without re-scanning the planes.
    code_sum: u32,
    /// Coordinate count, a non-zero multiple of [`BLOCK_DIM`].
    dim: usize,
}

impl QueryAdc4 {
    /// Returns the coordinate count this query representation was built at.
    ///
    /// Codes scored against it must have been encoded at the same dimension.
    #[must_use]
    pub const fn dim(&self) -> usize {
        self.dim
    }

    /// Return `sum_i h_i * q_hat_i`, where `h_i` is the stored sign.
    ///
    /// `q_hat_i = lower + step * code_i` is the dequantized query coordinate,
    /// so the sum expands to
    /// `step * sum_i h_i * code_i + lower * sum_i h_i`. Neither term needs the
    /// individual codes:
    ///
    /// - `sum_i h_i * code_i` equals `2 * S - code_sum`, where `S` is the total
    ///   code value over the coordinates whose sign bit is set. `S` is
    ///   recovered by popcounting `sign_words & plane_p` and weighting by
    ///   `2^p`.
    /// - `sum_i h_i` equals `2 * ones - dim`, where `ones` is the popcount of
    ///   the whole sign plane.
    ///
    /// Whole-word popcounts are only valid because `dim` is a multiple of 64;
    /// there are no padding bits to mask off.
    ///
    /// # Parameters
    ///
    /// - `sign_words`: Borrowed one-bit code plane for the row being scored.
    ///
    /// # Returns
    ///
    /// The exact dot product between the `+/-1` sign vector and the
    /// *dequantized* query. The result is exact with respect to the query's
    /// 4-bit representation; the approximation lives in that representation and
    /// in the sign code, not in this computation.
    ///
    /// # Errors
    ///
    /// Returns [`RabitqError::LengthMismatch`] when `sign_words` is not
    /// `dim / 64` words long.
    ///
    /// # Performance
    ///
    /// `QUERY_BITS + 1` popcount passes over `dim / 64` words: for 768
    /// dimensions that is sixty `u64` operations, against 768 multiply-adds for
    /// the equivalent floating-point dot product.
    fn signed_dot(&self, sign_words: &[u64]) -> Result<f32, RabitqError> {
        check_len(
            "one-bit code words",
            sign_words.len(),
            self.dim.div_ceil(64),
        )?;

        let ones = sign_words.iter().map(|word| word.count_ones()).sum::<u32>();
        let mut selected_code_sum = 0_u32;
        for (plane_index, plane) in self.planes.iter().enumerate() {
            let selected = sign_words
                .iter()
                .zip(plane)
                .map(|(signs, query_bits)| (signs & query_bits).count_ones())
                .sum::<u32>();
            selected_code_sum += selected << plane_index;
        }

        let centered_codes = 2_i64 * i64::from(selected_code_sum) - i64::from(self.code_sum);
        let centered_ones = 2_i64 * i64::from(ones) - self.dim as i64;
        Ok(self.step * centered_codes as f32 + self.lower * centered_ones as f32)
    }

    /// Return `<y, q_hat>` for unsigned two-bit code `u = y + 1.5`.
    ///
    /// The stored grid value `y_i` in `{-1.5, -0.5, +0.5, +1.5}` is held as the
    /// unsigned code `u_i = y_i + 1.5` in `0..=3`, so
    /// `<y, q_hat> = <u, q_hat> - 1.5 * sum_i q_hat_i`. Expanding
    /// `q_hat_i = lower + step * code_i` again removes the need to decode
    /// anything:
    ///
    /// - `sum_i u_i` is `popcount(low) + 2 * popcount(high)`.
    /// - `sum_i u_i * code_i` is accumulated over all four stored-plane by
    ///   query-plane pairs, each contributing
    ///   `popcount(stored_plane & query_plane) << (stored_index +
    ///   query_index)`.
    /// - `sum_i q_hat_i` is `step * code_sum + lower * dim`.
    ///
    /// # Parameters
    ///
    /// - `planes`: `[low, high]` borrowed bit planes of the row being scored.
    ///
    /// # Returns
    ///
    /// The dot product between the row's grid vector and the dequantized query.
    /// This is not yet an estimate of the residual inner product; the caller
    /// normalizes it by the grid vector's own length.
    ///
    /// # Errors
    ///
    /// Returns [`RabitqError::LengthMismatch`] when either plane is not
    /// `dim / 64` words long, naming which plane was wrong.
    ///
    /// # Performance
    ///
    /// `2 * QUERY_BITS + 2` popcount passes over `dim / 64` words — roughly
    /// double the one-bit kernel, for the recall the extra bit buys.
    fn two_bit_grid_dot(&self, planes: [&[u64]; 2]) -> Result<f32, RabitqError> {
        for (index, plane) in planes.iter().enumerate() {
            check_len(
                if index == 0 {
                    "two-bit low plane"
                } else {
                    "two-bit high plane"
                },
                plane.len(),
                self.dim / 64,
            )?;
        }

        let stored_code_sum = planes[0].iter().map(|word| word.count_ones()).sum::<u32>()
            + 2 * planes[1].iter().map(|word| word.count_ones()).sum::<u32>();
        let mut selected_product_sum = 0_u32;
        for (stored_plane_index, stored_plane) in planes.iter().enumerate() {
            for (query_plane_index, query_plane) in self.planes.iter().enumerate() {
                let selected = stored_plane
                    .iter()
                    .zip(query_plane)
                    .map(|(stored, query)| (stored & query).count_ones())
                    .sum::<u32>();
                selected_product_sum += selected << (stored_plane_index + query_plane_index);
            }
        }

        let unsigned_dot =
            self.step * selected_product_sum as f32 + self.lower * stored_code_sum as f32;
        let query_sum = self.step * self.code_sum as f32 + self.lower * self.dim as f32;
        Ok(unsigned_dot - 1.5 * query_sum)
    }
}

/// Encode signs plus the two RaBitQ correction scalars.
///
/// The owned convenience wrapper around [`encode_one_bit_into`]: it allocates
/// the bit plane, encodes into it, and packages the result with its dimension.
/// Build loops that encode many rows should call the `_into` form and write
/// into sub-slices of one cluster-wide buffer instead.
///
/// # Parameters
///
/// - `rotated_residual`: The residual `v - c` **after**
///   [`StructuredRotation::rotate_residual`]. Length must be a non-zero
///   multiple of [`BLOCK_DIM`]. Passing an unrotated vector cannot be detected
///   and silently degrades the estimator.
///
/// # Returns
///
/// An owned [`OneBitCode`] holding the sign plane and the two correction
/// scalars.
///
/// # Errors
///
/// Returns [`RabitqError::InvalidDimension`] for a length that is zero or not a
/// multiple of [`BLOCK_DIM`], and [`RabitqError::NonFiniteValue`] for the first
/// `NaN` or infinity found.
///
/// # Performance
///
/// One `Vec<u64>` allocation of `dim / 64` words plus a single pass over the
/// residual.
pub fn encode_one_bit(rotated_residual: &[f32]) -> Result<OneBitCode, RabitqError> {
    let dim = rotated_residual.len();
    let mut words = vec![0_u64; words_per_code(dim)?];
    let factors = encode_one_bit_into(rotated_residual, &mut words)?;

    Ok(OneBitCode {
        words,
        residual_norm: factors.residual_norm,
        bar_dot_residual: factors.bar_dot_residual,
        dim,
    })
}

/// Number of packed u64 words in one bit plane.
///
/// This is the sizing primitive callers use to lay out storage: a one-bit row
/// needs one plane of this many words, a two-bit row needs two. The resident
/// sketch multiplies it by eight to get plane bytes and adds two `f32` scalars
/// to get its row stride.
///
/// # Parameters
///
/// - `dim`: Coordinate count, which must be a non-zero multiple of
///   [`BLOCK_DIM`].
///
/// # Returns
///
/// `dim / 64`, exactly — never rounded up, because a valid `dim` is always a
/// multiple of 64. Every bit of every returned word therefore corresponds to a
/// real coordinate, which is the precondition that makes whole-word popcounts
/// correct in the estimators.
///
/// # Errors
///
/// Returns [`RabitqError::InvalidDimension`] when `dim` is zero or not a
/// multiple of [`BLOCK_DIM`].
///
/// # Examples
///
/// `words_per_code(768)` is 12, so a two-bit row occupies `12 * 8 * 2 = 192`
/// bytes of planes plus 8 bytes of scalars — 200 bytes against 3,072 for the
/// full-precision vector.
pub fn words_per_code(dim: usize) -> Result<usize, RabitqError> {
    validate_quantized_dim(dim)?;
    Ok(dim / 64)
}

/// Encode into caller-owned flat storage, avoiding one allocation per row.
///
/// Sets bit `i` when rotated residual coordinate `i` is strictly positive, so
/// the implied sign is `h_i = +1` for a set bit and `h_i = -1` otherwise. A
/// coordinate of exactly zero produces a clear bit and contributes nothing to
/// either scalar, so the choice is immaterial to the estimate.
///
/// The two returned scalars are what turn a sign pattern into a usable
/// estimate:
///
/// - `residual_norm` is `||r||`, accumulated in `f64` and narrowed once so a
///   768-term sum of squares does not lose precision.
/// - `bar_dot_residual` is `sum |r_i| / sqrt(D)`, which is exactly
///   `<h / sqrt(D), r>` because `h_i` is the sign of `r_i`. It is the cosine
///   between the unit sign vector and the true residual, scaled by `||r||`, and
///   it appears as the estimator's divisor. It is zero only when the residual
///   itself is zero, which the estimators check before dividing.
///
/// # Parameters
///
/// - `rotated_residual`: Residual after [`StructuredRotation`], length a
///   non-zero multiple of [`BLOCK_DIM`].
/// - `words`: Caller-owned output plane of exactly `dim / 64` words. Fully
///   zeroed before use, so a reused or freshly allocated buffer behaves
///   identically.
///
/// # Returns
///
/// The two correction scalars, by value. The bit plane is left in `words`.
///
/// # Errors
///
/// Returns [`RabitqError::InvalidDimension`] for an invalid residual length,
/// [`RabitqError::NonFiniteValue`] for a `NaN` or infinity, and
/// [`RabitqError::LengthMismatch`] when `words` is not `dim / 64` long. All
/// checks precede the `words.fill(0)`, so a rejected call leaves the caller's
/// buffer untouched.
///
/// # Performance
///
/// One pass over the residual with no allocation. A compaction loop encoding a
/// cluster allocates a single `Vec<u64>` for all rows and passes successive
/// sub-slices here.
///
/// # Rust Notes for Java/C Engineers
///
/// `words: &mut [u64]` is a unique borrow: while it is held, the compiler
/// guarantees no other reference — including `rotated_residual` — can alias it.
/// C's equivalent `uint64_t *out` carries no such guarantee, which is why a C
/// compiler must reload through the pointer inside the loop unless told
/// otherwise. Java has no way to express the guarantee at all.
pub fn encode_one_bit_into(
    rotated_residual: &[f32],
    words: &mut [u64],
) -> Result<OneBitFactors, RabitqError> {
    validate_quantized_dim(rotated_residual.len())?;
    validate_finite("rotated residual", rotated_residual)?;
    check_len(
        "one-bit output words",
        words.len(),
        rotated_residual.len() / 64,
    )?;
    words.fill(0);

    let mut norm_sq = 0.0_f64;
    let mut absolute_sum = 0.0_f64;
    for (index, &value) in rotated_residual.iter().enumerate() {
        let value = f64::from(value);
        norm_sq += value * value;
        absolute_sum += value.abs();
        if value > 0.0 {
            words[index / 64] |= 1_u64 << (index % 64);
        }
    }

    let residual_norm = norm_sq.sqrt() as f32;
    let bar_dot_residual = (absolute_sum / (rotated_residual.len() as f64).sqrt()) as f32;

    Ok(OneBitFactors {
        residual_norm,
        bar_dot_residual,
    })
}

/// Exact two-bit Extended-RaBitQ encoding with owned storage.
///
/// The owned convenience wrapper around [`encode_two_bit_into`]: it allocates
/// both planes and the sort scratch, encodes, and packages the result. Build
/// loops should use the `_into` form and hoist those three allocations out of
/// the loop.
///
/// # Parameters
///
/// - `rotated_residual`: Residual after [`StructuredRotation`], length a
///   non-zero multiple of [`BLOCK_DIM`].
///
/// # Returns
///
/// An owned [`TwoBitCode`] holding both planes and the two correction scalars.
///
/// # Errors
///
/// Returns [`RabitqError::InvalidDimension`] for an invalid length and
/// [`RabitqError::NonFiniteValue`] for a `NaN` or infinity.
///
/// # Performance
///
/// Three allocations (two `dim / 64` word planes and a `dim`-length index
/// buffer) plus the `O(D log D)` direction search described on
/// [`encode_two_bit_into`].
pub fn encode_two_bit(rotated_residual: &[f32]) -> Result<TwoBitCode, RabitqError> {
    let dim = rotated_residual.len();
    let words = words_per_code(dim)?;
    let mut planes = std::array::from_fn(|_| vec![0_u64; words]);
    let mut order = vec![0_usize; dim];
    let [low_plane, high_plane] = &mut planes;
    let factors = encode_two_bit_into(rotated_residual, low_plane, high_plane, &mut order)?;
    Ok(TwoBitCode {
        planes,
        residual_norm: factors.residual_norm,
        bar_dot_residual: factors.bar_dot_residual,
        dim,
    })
}

/// Encode exact two-bit Extended-RaBitQ codes into caller-owned bit planes.
///
/// For total bit width two, the grid is `{-1.5, -0.5, 0.5, 1.5}`.
/// For a fixed number of outer coordinates, the optimal grid direction uses
/// `1.5` on the coordinates with the largest absolute residuals. Enumerating
/// all sorted prefixes is exactly Algorithm 1's rescale search, in O(D log D).
///
/// The word "exact" refers to that search. A naive encoder would pick each
/// coordinate's grid level independently by rounding, which optimizes each
/// coordinate but not the quantity the estimator actually depends on — the
/// angle between the grid vector `y` and the true residual `r`. Because the
/// estimator normalizes by `||y||`, only `y`'s *direction* matters, so the
/// encoder maximizes `<y, r>^2 / ||y||^2` over all admissible `y`. Fixing each
/// coordinate's sign to match `r` and observing that promoting a coordinate
/// from `+/-0.5` to `+/-1.5` is always most profitable for the largest `|r_i|`
/// reduces the search to choosing how many of the sorted-by-magnitude
/// coordinates get the outer level. Every prefix is evaluated:
///
/// ```text
/// coordinates sorted by |r_i| descending
///   [ largest ................................ smallest ]
///     ^^^^^^^^^^^^^^^ prefix -> +/-1.5   rest -> +/-0.5
///
/// prefix = 0        every |y_i| = 0.5     ||y||^2 = 0.25 * D
/// prefix + 1        one more outer level  ||y||^2 += 1.5^2 - 0.5^2 = 2
///                                         <y, r>  += |r_i|
/// keep the prefix maximizing <y, r>^2 / ||y||^2
/// ```
///
/// Both running quantities update in `O(1)` per step, so the sort dominates.
/// Ties in `|r_i|` are broken by ascending coordinate index, making the chosen
/// code deterministic for identical input.
///
/// Encoding writes the inner levels first (`+0.5` as unsigned code `2`, `-0.5`
/// as `1`) and then toggles the low bit of the winning prefix, which moves `2`
/// to `3` (`+1.5`) and `1` to `0` (`-1.5`).
///
/// # Parameters
///
/// - `rotated_residual`: Residual after [`StructuredRotation`], length a
///   non-zero multiple of [`BLOCK_DIM`].
/// - `low_plane`: Caller-owned low bit plane, exactly `dim / 64` words. Zeroed
///   before use.
/// - `high_plane`: Caller-owned high bit plane, exactly `dim / 64` words.
///   Zeroed before use.
/// - `order_scratch`: Caller-owned index buffer of exactly `dim` elements. Its
///   prior contents are ignored; on return it holds the coordinate indices
///   sorted by descending `|r_i|`. It exists purely to keep the sort out of the
///   allocator on a per-row path.
///
/// # Returns
///
/// The residual norm `||r||` and `<y / ||y||, r>` for the winning grid vector.
/// The planes are left in `low_plane` and `high_plane`.
///
/// # Errors
///
/// Returns [`RabitqError::InvalidDimension`] for an invalid residual length,
/// [`RabitqError::NonFiniteValue`] for a `NaN` or infinity, and
/// [`RabitqError::LengthMismatch`] naming the first mis-sized buffer. All
/// validation precedes any write, so a rejected call leaves every caller buffer
/// untouched.
///
/// # Performance
///
/// `O(D log D)`, dominated by one unstable sort of `dim` indices; everything
/// else is a linear pass. No allocation. This runs once per corpus row during
/// compaction, never on the query path.
///
/// # Examples
///
/// For a residual that is `1.0` in one coordinate and zero elsewhere at
/// `dim = 256`, the search promotes exactly that one coordinate: it receives
/// unsigned code `3` (`+1.5`) and the remaining 255 zero coordinates stay at
/// code `2` (`+0.5`). The reported alignment is `1.5 / sqrt(66)`, since
/// `||y||^2 = 0.25 * 256 + 2 = 66`.
pub fn encode_two_bit_into(
    rotated_residual: &[f32],
    low_plane: &mut [u64],
    high_plane: &mut [u64],
    order_scratch: &mut [usize],
) -> Result<TwoBitFactors, RabitqError> {
    validate_quantized_dim(rotated_residual.len())?;
    validate_finite("rotated residual", rotated_residual)?;
    let words = rotated_residual.len() / 64;
    check_len("two-bit low output plane", low_plane.len(), words)?;
    check_len("two-bit high output plane", high_plane.len(), words)?;
    check_len(
        "two-bit order scratch",
        order_scratch.len(),
        rotated_residual.len(),
    )?;
    low_plane.fill(0);
    high_plane.fill(0);

    let mut norm_sq = 0.0_f64;
    let mut absolute_sum = 0.0_f64;
    for (index, &value) in rotated_residual.iter().enumerate() {
        let value_f64 = f64::from(value);
        norm_sq += value_f64 * value_f64;
        absolute_sum += value_f64.abs();
        order_scratch[index] = index;

        if value >= 0.0 {
            // Inner positive grid value: unsigned code 2 (`10`).
            high_plane[index / 64] |= 1_u64 << (index % 64);
        } else {
            // Inner negative grid value: unsigned code 1 (`01`).
            low_plane[index / 64] |= 1_u64 << (index % 64);
        }
    }
    order_scratch.sort_unstable_by(|&left, &right| {
        rotated_residual[right]
            .abs()
            .total_cmp(&rotated_residual[left].abs())
            .then_with(|| left.cmp(&right))
    });

    let mut numerator = 0.5 * absolute_sum;
    let mut grid_norm_sq = 0.25 * rotated_residual.len() as f64;
    let mut best_numerator = numerator;
    let mut best_grid_norm_sq = grid_norm_sq;
    let mut best_prefix = 0_usize;
    let mut best_cosine_sq = if grid_norm_sq == 0.0 {
        0.0
    } else {
        numerator * numerator / grid_norm_sq
    };

    for (prefix, &coordinate) in order_scratch.iter().enumerate() {
        numerator += f64::from(rotated_residual[coordinate].abs());
        // Changing |y_i| from 0.5 to 1.5 adds 1.5^2 - 0.5^2 = 2.
        grid_norm_sq += 2.0;
        let cosine_sq = numerator * numerator / grid_norm_sq;
        if cosine_sq > best_cosine_sq {
            best_cosine_sq = cosine_sq;
            best_numerator = numerator;
            best_grid_norm_sq = grid_norm_sq;
            best_prefix = prefix + 1;
        }
    }

    // Toggle the low bit to move inner +/-0.5 to outer +/-1.5.
    for &coordinate in &order_scratch[..best_prefix] {
        low_plane[coordinate / 64] ^= 1_u64 << (coordinate % 64);
    }

    Ok(TwoBitFactors {
        residual_norm: norm_sq.sqrt() as f32,
        bar_dot_residual: (best_numerator / best_grid_norm_sq.sqrt()) as f32,
    })
}

/// Quantize a rotated query residual with deterministic randomized rounding.
///
/// Builds the four-bit-plane query representation the popcount estimators
/// consume. The quantizer is affine over the query's own observed range: the
/// minimum coordinate becomes code `0`, the maximum becomes code
/// `QUERY_LEVELS`, and the step is their difference divided by
/// `QUERY_LEVELS`.
///
/// Rounding is **stochastic, not nearest**. Each coordinate's exact position
/// `(value - lower) / step` has a uniform `(0, 1)` dither added before the
/// floor, so a value 30% of the way between two levels lands on the upper level
/// 30% of the time. Nearest rounding would introduce a fixed per-coordinate
/// bias that survives the sum over `dim` coordinates and shifts every score in
/// the same direction; stochastic rounding has expectation equal to the exact
/// value, which is what keeps the overall estimator unbiased. The dither comes
/// from SplitMix64 seeded from `seed`, so the result is reproducible rather
/// than genuinely random.
///
/// A degenerate query whose coordinates are all identical gives `step == 0`;
/// every code is then `0` and the representation reduces to the constant
/// `lower`, which the estimators handle without a special case.
///
/// # Parameters
///
/// - `rotated_query_residual`: The query residual `q - c` after the **same**
///   [`StructuredRotation`] used to encode the rows being scored. Length must
///   be a non-zero multiple of [`BLOCK_DIM`].
/// - `seed`: Determines the dither sequence. Callers derive it from the
///   artifact's rotation seed, the query, and the cluster index so that
///   repeating a query reproduces its scores exactly. The value is mixed with a
///   fixed constant internally so it cannot collide with the rotation's own use
///   of the same seed.
///
/// # Returns
///
/// An owned [`QueryAdc4`] holding four bit planes plus the affine parameters
/// and the precomputed code sum.
///
/// # Errors
///
/// Returns [`RabitqError::InvalidDimension`] for an invalid length and
/// [`RabitqError::NonFiniteValue`] for a `NaN` or infinity. A non-finite
/// coordinate would corrupt `lower`, `upper`, and therefore every score in the
/// cluster.
///
/// # Performance
///
/// Allocates `QUERY_BITS` planes of `dim / 64` words — four small vectors — and
/// makes three passes over the query. This is per-cluster setup amortized over
/// every row in that cluster, not per-row work.
///
/// # Examples
///
/// A search probing 32 clusters calls this once per cluster, because the query
/// residual is taken against each cluster's own centroid. Each call produces a
/// representation reused across all rows of that cluster; scoring a row is then
/// a fixed number of `AND`/`count_ones` pairs.
pub fn prepare_query_adc4(
    rotated_query_residual: &[f32],
    seed: u64,
) -> Result<QueryAdc4, RabitqError> {
    validate_quantized_dim(rotated_query_residual.len())?;
    validate_finite("rotated query residual", rotated_query_residual)?;

    let dim = rotated_query_residual.len();
    let lower = rotated_query_residual
        .iter()
        .copied()
        .fold(f32::INFINITY, f32::min);
    let upper = rotated_query_residual
        .iter()
        .copied()
        .fold(f32::NEG_INFINITY, f32::max);
    let step = (upper - lower) / f32::from(QUERY_LEVELS);
    let mut planes: [Vec<u64>; QUERY_BITS] = std::array::from_fn(|_| vec![0_u64; dim.div_ceil(64)]);
    let mut code_sum = 0_u32;
    let mut rng = SplitMix64::new(seed ^ 0x5141_4443_345f_524e);

    for (index, &value) in rotated_query_residual.iter().enumerate() {
        let code = if step == 0.0 {
            0_u8
        } else {
            let scaled = f64::from((value - lower) / step);
            let dither = rng.next_open_unit_f64();
            (scaled + dither)
                .floor()
                .clamp(0.0, f64::from(QUERY_LEVELS)) as u8
        };
        code_sum += u32::from(code);
        for (plane_index, plane) in planes.iter_mut().enumerate() {
            if ((code >> plane_index) & 1) != 0 {
                plane[index / 64] |= 1_u64 << (index % 64);
            }
        }
    }

    Ok(QueryAdc4 {
        planes,
        lower,
        step,
        code_sum,
        dim,
    })
}

/// Estimate the residual inner product `<v-c, q-c>`.
///
/// Owned-code wrapper over [`estimate_residual_dot_one_bit_parts`]; see that
/// function for the estimator itself.
///
/// # Parameters
///
/// - `code`: Row code produced by [`encode_one_bit`] under the same rotation
///   and centroid as `query`.
/// - `query`: Query representation from [`prepare_query_adc4`].
///
/// # Returns
///
/// An approximation of `<v - c, q - c>` in the original (unrotated) geometry,
/// since the rotation preserves inner products.
///
/// # Errors
///
/// Returns [`RabitqError::LengthMismatch`] when the code and query dimensions
/// disagree.
pub fn estimate_residual_dot_one_bit(
    code: &OneBitCode,
    query: &QueryAdc4,
) -> Result<f32, RabitqError> {
    estimate_residual_dot_one_bit_parts(
        &code.words,
        OneBitFactors {
            residual_norm: code.residual_norm,
            bar_dot_residual: code.bar_dot_residual,
        },
        query,
    )
}

/// Allocation-free one-bit residual-dot estimator over a borrowed code slice.
///
/// This is the RaBitQ estimator itself. Writing `h_bar` for the unit sign
/// vector `h / sqrt(D)`, the value returned is
///
/// ```text
///   ||r||^2 * <h_bar, q_hat> / <h_bar, r>
/// ```
///
/// where `<h_bar, r>` is the stored `bar_dot_residual` and `<h_bar, q_hat>` is
/// computed by popcount from the bit planes. The division by `<h_bar, r>`
/// rescales the sign vector's projection back onto the true residual direction:
/// the sign code only knows which orthant `r` lies in, and this factor converts
/// "how much of the query lies along the code" into "how much lies along the
/// residual". Its error has mean zero and standard deviation falling like
/// `1 / sqrt(D)`, both pinned by tests in this file.
///
/// # Parameters
///
/// - `words`: Borrowed sign plane, typically a sub-slice of a cluster-wide
///   buffer. Must be `query.dim / 64` words.
/// - `factors`: The two scalars returned when this row was encoded, passed by
///   value.
/// - `query`: Query representation for the row's cluster.
///
/// # Returns
///
/// The estimated residual inner product. Returns exactly `0.0` when
/// `residual_norm` is zero — the vector coincided with its centroid, so the
/// residual inner product is zero and the sign code carries no direction. That
/// early return also guards the division, since `bar_dot_residual` is non-zero
/// whenever `residual_norm` is.
///
/// # Errors
///
/// Returns [`RabitqError::LengthMismatch`] when `words` is not `query.dim / 64`
/// long.
///
/// # Performance
///
/// One popcount kernel pass (see `QueryAdc4::signed_dot`), a square root, and
/// three floating-point operations. No allocation, no branch per coordinate.
pub fn estimate_residual_dot_one_bit_parts(
    words: &[u64],
    factors: OneBitFactors,
    query: &QueryAdc4,
) -> Result<f32, RabitqError> {
    check_len("one-bit code words", words.len(), query.dim / 64)?;
    if factors.residual_norm == 0.0 {
        return Ok(0.0);
    }
    let signed_dot = query.signed_dot(words)? / (query.dim as f32).sqrt();
    Ok(factors.residual_norm * factors.residual_norm * signed_dot / factors.bar_dot_residual)
}

/// Estimate squared L2 distance using per-cluster residual bookkeeping.
///
/// Owned-code wrapper over [`estimate_l2_one_bit_parts`].
///
/// # Parameters
///
/// - `code`: Row code encoded under the same rotation and centroid as `query`.
/// - `query`: Query representation for that cluster.
/// - `query_residual_norm_sq`: `||q - c||^2` for the same centroid, computed
///   once per cluster by the caller.
///
/// # Returns
///
/// An approximation of `||v - q||^2`. Lower is better; the value is comparable
/// across clusters because the centroid terms cancel.
///
/// # Errors
///
/// Returns [`RabitqError::LengthMismatch`] when the code and query dimensions
/// disagree, or [`RabitqError::InvalidQueryNorm`] for a negative or non-finite
/// norm.
pub fn estimate_l2_one_bit(
    code: &OneBitCode,
    query: &QueryAdc4,
    query_residual_norm_sq: f32,
) -> Result<f32, RabitqError> {
    check_len("query ADC dimension", query.dim, code.dim)?;
    estimate_l2_one_bit_parts(
        &code.words,
        OneBitFactors {
            residual_norm: code.residual_norm,
            bar_dot_residual: code.bar_dot_residual,
        },
        query,
        query_residual_norm_sq,
    )
}

/// Allocation-free squared-L2 estimator over a borrowed one-bit code.
///
/// Both vectors are expressed relative to the same centroid `c`, so the
/// centroid cancels out of the difference:
///
/// ```text
///   ||v - q||^2 = ||(v - c) - (q - c)||^2
///               = ||v - c||^2 + ||q - c||^2 - 2 * <v - c, q - c>
/// ```
///
/// The first term is the stored `residual_norm` squared, the second is supplied
/// by the caller once per cluster, and only the cross term is estimated. That
/// is why a one-bit code can produce a usable distance: the two magnitudes are
/// known exactly and the approximation is confined to the inner product.
///
/// # Parameters
///
/// - `words`: Borrowed sign plane for the row.
/// - `factors`: The row's stored scalars.
/// - `query`: Query representation for the row's cluster.
/// - `query_residual_norm_sq`: `||q - c||^2` against the same centroid the row
///   was encoded against. Supplying a norm from a different centroid produces a
///   finite, wrong, silently mis-ranked distance.
///
/// # Returns
///
/// The estimated squared L2 distance. It can be slightly negative for a very
/// close match, because the estimated cross term carries error; callers rank on
/// it rather than reporting it.
///
/// # Errors
///
/// Returns [`RabitqError::InvalidQueryNorm`] when `query_residual_norm_sq` is
/// negative or non-finite, and [`RabitqError::LengthMismatch`] when `words` is
/// mis-sized.
///
/// # Performance
///
/// One popcount kernel pass plus a fused multiply-add. No allocation.
pub fn estimate_l2_one_bit_parts(
    words: &[u64],
    factors: OneBitFactors,
    query: &QueryAdc4,
    query_residual_norm_sq: f32,
) -> Result<f32, RabitqError> {
    validate_query_norm(query_residual_norm_sq)?;
    let cross = estimate_residual_dot_one_bit_parts(words, factors, query)?;
    Ok(factors
        .residual_norm
        .mul_add(factors.residual_norm, query_residual_norm_sq)
        - 2.0 * cross)
}

/// Estimate `<v-c, q-c>` from an owned two-bit code.
///
/// Owned-code wrapper over [`estimate_residual_dot_two_bit_parts`].
///
/// # Parameters
///
/// - `code`: Row code from [`encode_two_bit`], under the same rotation and
///   centroid as `query`.
/// - `query`: Query representation from [`prepare_query_adc4`].
///
/// # Returns
///
/// An approximation of `<v - c, q - c>`, more accurate than the one-bit
/// estimate for the cost of one extra bit plane per row.
///
/// # Errors
///
/// Returns [`RabitqError::LengthMismatch`] when the code and query dimensions
/// disagree.
pub fn estimate_residual_dot_two_bit(
    code: &TwoBitCode,
    query: &QueryAdc4,
) -> Result<f32, RabitqError> {
    check_len("query ADC dimension", query.dim, code.dim)?;
    estimate_residual_dot_two_bit_parts(
        &code.planes[0],
        &code.planes[1],
        TwoBitFactors {
            residual_norm: code.residual_norm,
            bar_dot_residual: code.bar_dot_residual,
        },
        query,
    )
}

/// Allocation-free two-bit residual-dot estimator over borrowed planes.
///
/// The same estimator shape as the one-bit path, with the unit sign vector
/// replaced by the normalized grid vector `y / ||y||` the encoder chose:
///
/// ```text
///   ||r||^2 * <y/||y||, q_hat> / <y/||y||, r>
/// ```
///
/// `<y/||y||, r>` is the stored `bar_dot_residual`. The numerator is recovered
/// without decoding any coordinate: `<y, q_hat>` comes from the popcount kernel
/// and `||y||` is reconstructed from the planes, because a coordinate sits at
/// `+/-1.5` exactly when its low and high bits agree (unsigned code `0` or
/// `3`). Counting those agreements gives
/// `||y||^2 = 0.25 * D + 2 * outer`, matching the quantity the encoder
/// maximized.
///
/// # Parameters
///
/// - `low_plane`, `high_plane`: Borrowed bit planes for the row, each
///   `query.dim / 64` words.
/// - `factors`: The row's stored scalars.
/// - `query`: Query representation for the row's cluster.
///
/// # Returns
///
/// The estimated residual inner product, or exactly `0.0` when `residual_norm`
/// is zero. That early return also guards the division by
/// `bar_dot_residual`.
///
/// # Errors
///
/// Returns [`RabitqError::LengthMismatch`] when either plane is not
/// `query.dim / 64` words long. Note that a zero-norm row returns before the
/// planes are inspected, so a mis-sized plane on such a row is not reported.
///
/// # Performance
///
/// One two-bit popcount kernel pass plus one extra popcount pass to recover
/// `||y||`, a square root, and a handful of scalar operations. No allocation.
pub fn estimate_residual_dot_two_bit_parts(
    low_plane: &[u64],
    high_plane: &[u64],
    factors: TwoBitFactors,
    query: &QueryAdc4,
) -> Result<f32, RabitqError> {
    if factors.residual_norm == 0.0 {
        return Ok(0.0);
    }
    let grid_dot = query.two_bit_grid_dot([low_plane, high_plane])?;
    let outer = low_plane
        .iter()
        .zip(high_plane)
        .map(|(low, high)| (!(low ^ high)).count_ones())
        .sum::<u32>();
    let grid_norm = (0.25 * query.dim as f32 + 2.0 * outer as f32).sqrt();
    let bar_dot_query = grid_dot / grid_norm;
    Ok(factors.residual_norm * factors.residual_norm * bar_dot_query / factors.bar_dot_residual)
}

/// Estimate squared L2 from an owned two-bit code.
///
/// Owned-code wrapper over [`estimate_l2_two_bit_parts`].
///
/// # Parameters
///
/// - `code`: Row code encoded under the same rotation and centroid as `query`.
/// - `query`: Query representation for that cluster.
/// - `query_residual_norm_sq`: `||q - c||^2` for the same centroid.
///
/// # Returns
///
/// An approximation of `||v - q||^2`; lower is better.
///
/// # Errors
///
/// Returns [`RabitqError::LengthMismatch`] on a dimension disagreement or
/// [`RabitqError::InvalidQueryNorm`] for a negative or non-finite norm.
pub fn estimate_l2_two_bit(
    code: &TwoBitCode,
    query: &QueryAdc4,
    query_residual_norm_sq: f32,
) -> Result<f32, RabitqError> {
    check_len("query ADC dimension", query.dim, code.dim)?;
    estimate_l2_two_bit_parts(
        &code.planes[0],
        &code.planes[1],
        TwoBitFactors {
            residual_norm: code.residual_norm,
            bar_dot_residual: code.bar_dot_residual,
        },
        query,
        query_residual_norm_sq,
    )
}

/// Allocation-free squared-L2 estimator over borrowed two-bit planes.
///
/// This is the estimator the production coarse-sketch search path calls for
/// cosine and Euclidean metrics. It applies the same centroid-cancelling
/// identity documented on [`estimate_l2_one_bit_parts`], using the two-bit
/// cross-term estimate.
///
/// # Parameters
///
/// - `low_plane`, `high_plane`: Borrowed bit planes for the row, typically
///   sub-slices of a cluster-wide buffer.
/// - `factors`: The row's stored scalars, read from the persisted row tail.
/// - `query`: Query representation for the row's cluster.
/// - `query_residual_norm_sq`: `||q - c||^2` against the same centroid the row
///   was encoded against.
///
/// # Returns
///
/// The estimated squared L2 distance, usable for ordering candidates. For unit
/// vectors [`estimate_cosine_from_unit_l2`] converts it to a cosine score.
///
/// # Errors
///
/// Returns [`RabitqError::InvalidQueryNorm`] for a negative or non-finite norm
/// and [`RabitqError::LengthMismatch`] for a mis-sized plane.
///
/// # Performance
///
/// One two-bit popcount kernel pass plus a fused multiply-add. No allocation,
/// so a probe set of tens of thousands of rows costs no allocator traffic.
///
/// # Examples
///
/// The coarse sketch scores every row of a probed cluster with this function,
/// ranks clusters by the resulting scores, and then reads full-precision
/// vectors only for the selected clusters. The values returned here never reach
/// a caller of the search API — they choose what to read, and the exact
/// distance is recomputed from full vectors afterwards.
pub fn estimate_l2_two_bit_parts(
    low_plane: &[u64],
    high_plane: &[u64],
    factors: TwoBitFactors,
    query: &QueryAdc4,
    query_residual_norm_sq: f32,
) -> Result<f32, RabitqError> {
    validate_query_norm(query_residual_norm_sq)?;
    let cross = estimate_residual_dot_two_bit_parts(low_plane, high_plane, factors, query)?;
    Ok(factors
        .residual_norm
        .mul_add(factors.residual_norm, query_residual_norm_sq)
        - 2.0 * cross)
}

/// Convert squared L2 to cosine for inputs known to be unit-normalized.
///
/// For unit vectors `||a - b||^2 = 2 - 2 * <a, b>`, so the cosine similarity is
/// `1 - l2 / 2`. This is an identity, not an approximation: whatever error the
/// squared-L2 estimate carries is carried through unchanged and merely halved.
///
/// # Parameters
///
/// - `estimated_l2`: A squared-L2 estimate from one of the `estimate_l2_*`
///   functions, for vectors the caller has already established are unit
///   length. Applying it to unnormalized vectors yields a number with no
///   meaning; nothing here can detect that.
///
/// # Returns
///
/// The corresponding cosine similarity estimate, where larger is more similar —
/// the opposite ordering from the squared-L2 input.
#[must_use]
pub fn estimate_cosine_from_unit_l2(estimated_l2: f32) -> f32 {
    1.0 - 0.5 * estimated_l2
}

/// Rejects any dimension the packed-word layout cannot represent exactly.
///
/// Guards every encoder and sizing helper. The multiple-of-[`BLOCK_DIM`] rule
/// is doing two jobs at once: it lets the blocked Hadamard transform run
/// without padding, and it makes `dim / 64` exact so no packed word contains
/// bits that do not correspond to coordinates. Whole-word popcounts in the
/// estimators depend on the second property.
///
/// # Parameters
///
/// - `dim`: Candidate coordinate count.
///
/// # Errors
///
/// Returns [`RabitqError::InvalidDimension`] for zero or for any value not
/// divisible by [`BLOCK_DIM`]. Callers pad; this function never rounds.
#[allow(clippy::manual_is_multiple_of)] // `usize::is_multiple_of` is newer than the MSRV.
fn validate_quantized_dim(dim: usize) -> Result<(), RabitqError> {
    if dim == 0 || dim % BLOCK_DIM != 0 {
        Err(RabitqError::InvalidDimension { dim })
    } else {
        Ok(())
    }
}

/// Rejects an input vector containing a `NaN` or an infinity.
///
/// A non-finite coordinate would propagate into the norm and correction
/// scalars and from there into every score derived from the row or the
/// cluster, ranking arbitrarily rather than failing. Encoding refuses it
/// instead.
///
/// # Parameters
///
/// - `name`: Static label used in the error so the caller learns which input
///   was bad.
/// - `values`: Borrowed vector to scan.
///
/// # Errors
///
/// Returns [`RabitqError::NonFiniteValue`] carrying the index of the first
/// offending coordinate.
///
/// # Performance
///
/// One short-circuiting linear scan.
fn validate_finite(name: &'static str, values: &[f32]) -> Result<(), RabitqError> {
    if let Some((index, _)) = values
        .iter()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        Err(RabitqError::NonFiniteValue { name, index })
    } else {
        Ok(())
    }
}

/// Rejects a caller-supplied `||q - c||^2` that cannot be a squared norm.
///
/// The squared-L2 estimators add this term directly, so a negative or
/// non-finite value would make the resulting distance incomparable with every
/// other candidate's rather than merely inaccurate.
///
/// # Parameters
///
/// - `value`: The squared query residual norm to check.
///
/// # Errors
///
/// Returns [`RabitqError::InvalidQueryNorm`] when the value is negative,
/// infinite, or `NaN`. Zero is accepted: it means the query coincides with the
/// centroid.
fn validate_query_norm(value: f32) -> Result<(), RabitqError> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(RabitqError::InvalidQueryNorm { value })
    }
}

/// Reads one packed bit, used only by tests that decode a code by hand.
///
/// Production code never unpacks individual bits; it works on whole words
/// through popcount. This helper exists so a test can assert what a specific
/// coordinate was encoded as.
///
/// # Parameters
///
/// - `words`: Packed bit plane.
/// - `index`: Coordinate index.
///
/// # Returns
///
/// `true` when the bit for `index` is set.
///
/// # Panics
///
/// Panics on out-of-bounds indexing when `index / 64` is not a valid word.
#[cfg(test)]
fn bit_at(words: &[u64], index: usize) -> bool {
    ((words[index / 64] >> (index % 64)) & 1) != 0
}

/// Enforces an exact length, the single guard behind every buffer contract in
/// this file.
///
/// Deliberately an equality check rather than a minimum. A longer buffer is not
/// harmlessly large here: whole-word popcounts would count bits that are not
/// coordinates, and a scatter would write outside the intended region.
///
/// # Parameters
///
/// - `name`: Static label naming the buffer, surfaced in the error.
/// - `actual`: Length the caller supplied.
/// - `expected`: Length the operation requires.
///
/// # Errors
///
/// Returns [`RabitqError::LengthMismatch`] carrying all three values.
fn check_len(name: &'static str, actual: usize, expected: usize) -> Result<(), RabitqError> {
    if actual == expected {
        Ok(())
    } else {
        Err(RabitqError::LengthMismatch {
            name,
            expected,
            actual,
        })
    }
}

/// Applies an orthonormal fast Hadamard transform to one 256-coordinate block.
///
/// The Hadamard transform is the real-valued cousin of the FFT: eight stages of
/// butterflies, each replacing a pair `(a, b)` with `(a + b, a - b)`, compute
/// the full 256-point transform in `256 * 8` operations instead of `256^2`.
/// Scaling the result by `1 / sqrt(256) = 0.0625` makes the transform
/// orthonormal, so it preserves norms and inner products exactly rather than
/// scaling them by 16.
///
/// This is the stage that actually mixes coordinates. Applied to a block whose
/// energy sits on a few coordinates, it produces a block whose energy is spread
/// across all 256 — which is the condition the sign code needs.
///
/// # Parameters
///
/// - `values`: Exactly [`BLOCK_DIM`] coordinates, transformed in place.
///
/// # Panics
///
/// Debug builds assert the length is [`BLOCK_DIM`]. In release builds a shorter
/// slice would panic on indexing instead; callers reach this only through
/// `chunks_exact_mut(BLOCK_DIM)`, which cannot yield a short block.
///
/// # Performance
///
/// `BLOCK_DIM * log2(BLOCK_DIM)` butterflies — eight operations per coordinate
/// — plus one scaling pass. All accesses are sequential within the block.
fn normalized_fht_256(values: &mut [f32]) {
    debug_assert_eq!(values.len(), BLOCK_DIM);
    let mut half = 1;
    while half < BLOCK_DIM {
        for start in (0..BLOCK_DIM).step_by(half * 2) {
            for offset in 0..half {
                let left = values[start + offset];
                let right = values[start + offset + half];
                values[start + offset] = left + right;
                values[start + offset + half] = left - right;
            }
        }
        half *= 2;
    }
    // sqrt(256) = 16.
    for value in values {
        *value *= 0.0625;
    }
}

/// A tiny, fully deterministic pseudorandom generator.
///
/// Zeppelin needs the rotation's signs and permutations, and the query
/// quantizer's dither, to be reproducible from a `u64` seed alone: an artifact
/// persists the seed, and any reader must rebuild the identical transform. A
/// crate-provided generator would tie that reproducibility to a dependency
/// version, so SplitMix64 is implemented here — it is eight bytes of state and
/// a handful of multiplies and shifts, with no cross-version drift.
///
/// This is not a cryptographic generator and does not need to be. Nothing here
/// protects a secret; the seed is stored in plain text beside the codes.
#[derive(Debug, Clone, Copy)]
struct SplitMix64 {
    /// The full generator state, advanced by a fixed odd increment per draw.
    state: u64,
}

impl SplitMix64 {
    /// Creates a generator positioned by `seed`.
    ///
    /// Every seed is valid, including zero: the state is advanced before it is
    /// mixed, so a zero seed does not produce a degenerate stream.
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Draws the next 64 random bits and advances the state.
    ///
    /// # Returns
    ///
    /// A uniformly distributed `u64`. The stream is fully determined by the
    /// seed and the number of prior draws.
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    /// Draws a uniform integer in `0..bound` with no modulo bias.
    ///
    /// Taking `next_u64() % bound` directly would favor the low residues
    /// whenever `bound` does not divide `2^64`. This rejects any draw at or
    /// above the largest multiple of `bound` that fits in 64 bits, so every
    /// outcome is equally likely. Bias matters here because the Fisher-Yates
    /// shuffle that builds each rotation permutation calls this `dim - 1`
    /// times; a skewed shuffle would leave structure in the transform.
    ///
    /// # Parameters
    ///
    /// - `bound`: Exclusive upper bound. Must be greater than zero.
    ///
    /// # Returns
    ///
    /// A uniformly distributed value in `0..bound`.
    ///
    /// # Panics
    ///
    /// Debug builds assert `bound > 0`. In release builds a zero bound would
    /// divide by zero; the only caller passes `i + 1` with `i >= 1`.
    ///
    /// # Performance
    ///
    /// One draw in the overwhelming majority of calls; the rejection loop
    /// repeats with probability below `bound / 2^64`.
    fn uniform_below(&mut self, bound: usize) -> usize {
        debug_assert!(bound > 0);
        let bound = bound as u128;
        let range = 1_u128 << 64;
        let limit = range - (range % bound);
        loop {
            let value = self.next_u64() as u128;
            if value < limit {
                return (value % bound) as usize;
            }
        }
    }

    /// Draws a uniform `f64` strictly inside `(0, 1)`.
    ///
    /// Used as the dither in [`prepare_query_adc4`] and as the uniform input to
    /// the Box-Muller transform in the offline oracle's Gaussian source.
    ///
    /// # Returns
    ///
    /// A value in the open interval `(0, 1)`; neither endpoint is attainable.
    fn next_open_unit_f64(&mut self) -> f64 {
        // The half-unit offset excludes both endpoints and avoids log(0) when
        // the same generator is used by the dense Gaussian test oracle.
        ((self.next_u64() >> 11) as f64 + 0.5) / ((1_u64 << 53) as f64)
    }
}

/// Number of paired samples in the deterministic 768-dimensional rotation
/// quality oracle.
pub const ROTATION_QUALITY_PAIRS: usize = 256;

/// Largest acceptable structured-to-identity RMSE ratio in the offline
/// rotation quality oracle.
pub const ROTATION_IDENTITY_MAX_RMSE_RATIO: f64 = 0.8;

/// Deterministic offline comparison of structured, dense, and identity
/// rotations on a sparse anisotropic distribution.
///
/// Returned by [`compare_structured_dense_quality_768`] for the bake-off report.
/// The three RMSE fields are directly comparable because all three rotations
/// were measured on the same sample pairs with the same seeds; the two `mse_*`
/// fields express the structured-versus-dense comparison as a paired
/// significance test, which is far more sensitive than comparing two
/// independently noisy RMSE values.
///
/// The type is [`Copy`] and holds only scalars, so it can be returned and
/// forwarded to a report writer without ownership concerns.
#[derive(Debug, Clone, Copy)]
pub struct RotationQuality {
    /// Number of sample pairs measured, always [`ROTATION_QUALITY_PAIRS`].
    pub pairs: usize,
    /// Root mean squared estimator error under [`StructuredRotation`] — the
    /// production transform.
    pub structured_rmse: f64,
    /// Root mean squared estimator error under the dense Gram-Schmidt
    /// reference rotation. The number the structured transform must match.
    pub dense_rmse: f64,
    /// Root mean squared estimator error with no rotation applied. The
    /// baseline the structured transform must beat, proving it does real work
    /// rather than passing by being equally bad as the reference.
    pub identity_rmse: f64,
    /// Paired `structured MSE - dense MSE` mean.
    pub mse_delta: f64,
    /// Standard error of `mse_delta` over `pairs` samples. The gate accepts
    /// `|mse_delta|` up to five of these, so a structured transform that is
    /// genuinely equivalent passes while a real regression does not.
    pub mse_delta_standard_error: f64,
}

/// Run the deterministic 768-dimensional offline rotation quality oracle.
///
/// The input pairs have non-zero mean, coordinate-dependent variance, and
/// support only in a small subset of the first 256-coordinate block. This is
/// deliberately not rotationally invariant: identity, and structured
/// rotations with broken cross-block mixing, cannot pass accidentally. The
/// function returns an error unless structured and dense MSEs agree within
/// five paired standard errors and structured RMSE is at least 20% lower than
/// identity RMSE.
///
/// This allocates a dense 768-by-768 matrix and is intended only for explicit
/// offline bakeoff/reporting paths, never request serving or production
/// encoding.
///
/// This oracle answers one narrow question: is the cheap blocked transform a
/// legitimate substitute for an `O(D^2)` random rotation? It says nothing about
/// retrieval quality. `tests/ivf_recall_gate.rs` remains the quality authority
/// for that, and no result here can substitute for it.
///
/// # Returns
///
/// A [`RotationQuality`] summarizing all three rotations over
/// [`ROTATION_QUALITY_PAIRS`] pairs. Both gates passed if this returns `Ok`.
///
/// # Errors
///
/// Returns [`RabitqError::StructuredDenseQualityMismatch`] when the structured
/// transform is statistically distinguishable from the dense one, and
/// [`RabitqError::StructuredIdentityQualityMismatch`] when it fails to improve
/// on doing nothing by the margin in [`ROTATION_IDENTITY_MAX_RMSE_RATIO`]. The
/// dimension and finiteness errors from the underlying encoders can also
/// propagate, though the fixed inputs here do not trigger them.
///
/// # Performance
///
/// Allocates and Gram-Schmidt-orthogonalizes a 768-by-768 `f64` matrix (about
/// 4.7 MB) and applies it twice per pair at `O(D^2)` each. Seconds of CPU, and
/// entirely unsuitable for any request path.
///
/// # Examples
///
/// The bake-off binary calls this once and embeds the returned RMSE figures in
/// its report. A regression that broke the cross-block permutation would leave
/// each round mixing only within its own 256-coordinate block; the sparse input
/// distribution concentrates support in the first block, so identity-like
/// behavior would surface as structured RMSE approaching identity RMSE and the
/// second gate would reject it.
pub fn compare_structured_dense_quality_768() -> Result<RotationQuality, RabitqError> {
    const DIM: usize = 768;
    const EXACT_DOT: f32 = 0.3;
    const STRUCTURED_SEED: u64 = 0x057a_c7ed;
    const DENSE_SEED: u64 = 0xd00d_5eed;
    const PAIR_SEED: u64 = 0x5151_5151;

    let structured = StructuredRotation::new(DIM, STRUCTURED_SEED)?;
    let dense = DenseRotation::new(DIM, DENSE_SEED);
    let mut gaussian = StandardNormal::new(PAIR_SEED);
    let mut residual = vec![0.0; DIM];
    let mut orthogonal = vec![0.0; DIM];
    let mut query = vec![0.0; DIM];
    let mut structured_residual = vec![0.0; DIM];
    let mut structured_query = vec![0.0; DIM];
    let mut dense_residual = vec![0.0; DIM];
    let mut dense_query = vec![0.0; DIM];
    let mut scratch = vec![0.0; DIM];
    let mut structured_words = vec![0_u64; DIM / 64];
    let mut dense_words = vec![0_u64; DIM / 64];
    let mut identity_words = vec![0_u64; DIM / 64];
    let mut difference_sum = 0.0_f64;
    let mut difference_square_sum = 0.0_f64;
    let mut structured_mse = 0.0_f64;
    let mut dense_mse = 0.0_f64;
    let mut identity_mse = 0.0_f64;

    for pair in 0..ROTATION_QUALITY_PAIRS {
        fill_anisotropic_sparse_pair(
            &mut gaussian,
            EXACT_DOT,
            &mut residual,
            &mut orthogonal,
            &mut query,
        );
        structured_residual.copy_from_slice(&residual);
        structured_query.copy_from_slice(&query);
        structured.rotate_in_place(&mut structured_residual, &mut scratch)?;
        structured.rotate_in_place(&mut structured_query, &mut scratch)?;
        dense.rotate(&residual, &mut dense_residual);
        dense.rotate(&query, &mut dense_query);

        let structured_factors = encode_one_bit_into(&structured_residual, &mut structured_words)?;
        let dense_factors = encode_one_bit_into(&dense_residual, &mut dense_words)?;
        let identity_factors = encode_one_bit_into(&residual, &mut identity_words)?;
        let adc_seed = pair as u64 ^ 0x51;
        let structured_adc = prepare_query_adc4(&structured_query, adc_seed)?;
        let dense_adc = prepare_query_adc4(&dense_query, adc_seed)?;
        let identity_adc = prepare_query_adc4(&query, adc_seed)?;
        let structured_estimate = estimate_residual_dot_one_bit_parts(
            &structured_words,
            structured_factors,
            &structured_adc,
        )?;
        let dense_estimate =
            estimate_residual_dot_one_bit_parts(&dense_words, dense_factors, &dense_adc)?;
        let identity_estimate =
            estimate_residual_dot_one_bit_parts(&identity_words, identity_factors, &identity_adc)?;
        let exact_dot = offline_dot(&residual, &query) as f32;
        let structured_squared = f64::from(structured_estimate - exact_dot).powi(2);
        let dense_squared = f64::from(dense_estimate - exact_dot).powi(2);
        let identity_squared = f64::from(identity_estimate - exact_dot).powi(2);
        let difference = structured_squared - dense_squared;
        structured_mse += structured_squared;
        dense_mse += dense_squared;
        identity_mse += identity_squared;
        difference_sum += difference;
        difference_square_sum += difference * difference;
    }

    let pairs = ROTATION_QUALITY_PAIRS as f64;
    let mse_delta = difference_sum / pairs;
    let difference_variance = (difference_square_sum / pairs - mse_delta * mse_delta).max(0.0);
    let quality = RotationQuality {
        pairs: ROTATION_QUALITY_PAIRS,
        structured_rmse: (structured_mse / pairs).sqrt(),
        dense_rmse: (dense_mse / pairs).sqrt(),
        identity_rmse: (identity_mse / pairs).sqrt(),
        mse_delta,
        mse_delta_standard_error: (difference_variance / pairs).sqrt(),
    };

    if quality.mse_delta.abs() > 5.0 * quality.mse_delta_standard_error {
        return Err(RabitqError::StructuredDenseQualityMismatch {
            mse_delta: quality.mse_delta,
            standard_error: quality.mse_delta_standard_error,
            structured_rmse: quality.structured_rmse,
            dense_rmse: quality.dense_rmse,
        });
    }
    if quality.structured_rmse > ROTATION_IDENTITY_MAX_RMSE_RATIO * quality.identity_rmse {
        return Err(RabitqError::StructuredIdentityQualityMismatch {
            structured_rmse: quality.structured_rmse,
            identity_rmse: quality.identity_rmse,
            maximum_ratio: ROTATION_IDENTITY_MAX_RMSE_RATIO,
        });
    }

    Ok(quality)
}

/// First coordinate carrying support in the oracle's sparse input distribution.
///
/// Deliberately not zero and not block-aligned, so a transform that happens to
/// treat block boundaries specially cannot pass by coincidence.
const ANISOTROPIC_ACTIVE_OFFSET: usize = 13;

/// Number of consecutive coordinates carrying support, starting at
/// [`ANISOTROPIC_ACTIVE_OFFSET`].
///
/// Twenty-four active coordinates out of 768 is far from rotationally
/// invariant: all support lies inside the first [`BLOCK_DIM`] block. A
/// structured rotation whose cross-block mixing were broken would leave most of
/// the vector at zero and score like identity, which the oracle's second gate
/// rejects.
const ANISOTROPIC_ACTIVE_DIM: usize = 24;

/// Draws one correlated unit-vector pair from the oracle's sparse anisotropic
/// distribution.
///
/// The pair is constructed so the exact answer is known in advance: `residual`
/// is a unit vector, `orthogonal` is a unit vector perpendicular to it, and
/// `query` is `exact_dot * residual + sqrt(1 - exact_dot^2) * orthogonal`,
/// which is a unit vector whose inner product with `residual` is exactly
/// `exact_dot`. Any deviation an estimator reports is therefore pure estimator
/// error.
///
/// The distribution is adversarial on purpose: support is confined to
/// [`ANISOTROPIC_ACTIVE_DIM`] consecutive coordinates, the samples have non-zero
/// mean, and per-coordinate variance decays across the active window. Gaussian
/// isotropic inputs would flatter any rotation, including one that does
/// nothing.
///
/// # Parameters
///
/// - `gaussian`: Deterministic standard-normal source, advanced by this call.
/// - `exact_dot`: Target inner product between `residual` and `query`, in
///   `[-1, 1]`.
/// - `residual`, `orthogonal`, `query`: Equal-length output buffers, fully
///   overwritten.
///
/// # Panics
///
/// Debug builds assert the three buffers have equal length and are long enough
/// to hold the active window.
fn fill_anisotropic_sparse_pair(
    gaussian: &mut StandardNormal,
    exact_dot: f32,
    residual: &mut [f32],
    orthogonal: &mut [f32],
    query: &mut [f32],
) {
    debug_assert_eq!(residual.len(), orthogonal.len());
    debug_assert_eq!(residual.len(), query.len());
    debug_assert!(residual.len() >= ANISOTROPIC_ACTIVE_OFFSET + ANISOTROPIC_ACTIVE_DIM);
    residual.fill(0.0);
    orthogonal.fill(0.0);
    query.fill(0.0);

    for local_index in 0..ANISOTROPIC_ACTIVE_DIM {
        let coordinate = ANISOTROPIC_ACTIVE_OFFSET + local_index;
        let scale = 1.0 / (1.0 + 0.18 * local_index as f32);
        residual[coordinate] = scale * (gaussian.next() as f32 + 1.25);
        orthogonal[coordinate] = scale * (gaussian.next() as f32 - 0.65);
    }
    offline_normalize(residual);
    let projection = offline_dot(residual, orthogonal) as f32;
    for (value, axis) in orthogonal.iter_mut().zip(residual.iter()) {
        *value -= projection * axis;
    }
    offline_normalize(orthogonal);

    let perpendicular_scale = (1.0 - exact_dot * exact_dot).sqrt();
    for ((output, axis), perpendicular) in
        query.iter_mut().zip(residual.iter()).zip(orthogonal.iter())
    {
        *output = exact_dot.mul_add(*axis, perpendicular_scale * perpendicular);
    }
}

/// Computes an inner product in `f64` for the offline oracle's ground truth.
///
/// Accumulating in `f64` matters here even though the inputs are `f32`: this
/// value is the reference the estimator's error is measured against, so its own
/// rounding must be far below the error being measured.
///
/// # Parameters
///
/// - `left`, `right`: Borrowed vectors. Excess elements of the longer slice are
///   ignored, since `zip` stops at the shorter one.
///
/// # Returns
///
/// The inner product as `f64`.
fn offline_dot(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| f64::from(*left) * f64::from(*right))
        .sum()
}

/// Scales a vector to unit length in place for the offline oracle.
///
/// # Parameters
///
/// - `values`: Vector to normalize, modified in place. Must have non-zero
///   length.
///
/// # Panics
///
/// Debug builds assert the norm is positive. In release builds a zero vector
/// would yield `NaN` coordinates rather than panicking; the oracle's
/// construction never produces one.
fn offline_normalize(values: &mut [f32]) {
    let norm = offline_dot(values, values).sqrt() as f32;
    debug_assert!(norm > 0.0);
    for value in values {
        *value /= norm;
    }
}

/// Dense Haar-like Gaussian/Gram-Schmidt oracle. It is private to the explicit
/// offline quality path because its O(D^2) apply cost is unsuitable for
/// production encoding.
#[derive(Debug, Clone)]
struct DenseRotation {
    /// Side length of the square matrix, in coordinates.
    dim: usize,
    /// Row-major `dim * dim` orthonormal basis. Row `i` occupies
    /// `rows[i * dim..(i + 1) * dim]`; a flat `Vec` avoids `dim` separate
    /// allocations and keeps each row contiguous for the dot products in
    /// `rotate`.
    rows: Vec<f64>,
}

impl DenseRotation {
    /// Builds a random orthonormal matrix by Gram-Schmidt on Gaussian rows.
    ///
    /// Each candidate row is drawn from a standard normal, projected off every
    /// previously accepted row, and normalized. Drawing from an isotropic
    /// Gaussian before orthogonalizing is what makes the resulting basis
    /// Haar-like — uniformly distributed over rotations — rather than biased
    /// toward the coordinate axes. A candidate whose residual norm collapses
    /// below `1e-12` (near-linear dependence) is discarded and redrawn.
    ///
    /// # Parameters
    ///
    /// - `dim`: Matrix side length. Unlike [`StructuredRotation`] this has no
    ///   block constraint, because no Hadamard transform is involved.
    /// - `seed`: Determines the entire matrix; mixed with a fixed constant so
    ///   it does not collide with other uses of the same seed value.
    ///
    /// # Returns
    ///
    /// An owned rotation holding a `dim * dim` `f64` matrix.
    ///
    /// # Performance
    ///
    /// `O(dim^3)` to build and `dim * dim * 8` bytes to hold — about 4.7 MB at
    /// `dim = 768`. This is why the type is private to the offline oracle.
    fn new(dim: usize, seed: u64) -> Self {
        let mut gaussian = StandardNormal::new(seed ^ 0x4445_4e53_455f_5152);
        let mut rows = vec![0.0_f64; dim * dim];
        let mut candidate = vec![0.0_f64; dim];

        for row_index in 0..dim {
            loop {
                for value in &mut candidate {
                    *value = gaussian.next();
                }
                for previous_index in 0..row_index {
                    let previous = &rows[previous_index * dim..(previous_index + 1) * dim];
                    let projection = candidate
                        .iter()
                        .zip(previous)
                        .map(|(left, right)| left * right)
                        .sum::<f64>();
                    for (value, basis) in candidate.iter_mut().zip(previous) {
                        *value -= projection * basis;
                    }
                }
                let norm = candidate
                    .iter()
                    .map(|value| value * value)
                    .sum::<f64>()
                    .sqrt();
                if norm > 1.0e-12 {
                    let row = &mut rows[row_index * dim..(row_index + 1) * dim];
                    for (output, value) in row.iter_mut().zip(&candidate) {
                        *output = *value / norm;
                    }
                    break;
                }
            }
        }
        Self { dim, rows }
    }

    /// Applies the dense matrix, producing one output coordinate per row.
    ///
    /// Accumulation is in `f64` so the reference transform contributes
    /// negligible error to the comparison it anchors.
    ///
    /// # Parameters
    ///
    /// - `input`: Vector to rotate, borrowed and unmodified. Must have exactly
    ///   `dim` elements.
    /// - `output`: Receives the rotated vector; fully overwritten. Must have
    ///   exactly `dim` elements.
    ///
    /// # Panics
    ///
    /// Panics in all build profiles when either slice length differs from
    /// `dim`. This is offline-only code, so an assertion is the appropriate
    /// response to a programming error rather than a [`RabitqError`].
    ///
    /// # Performance
    ///
    /// `O(dim^2)` multiply-adds — roughly 590,000 at `dim = 768`, against about
    /// 23,000 for [`StructuredRotation::rotate_in_place`] at the same
    /// dimension. That gap is the reason the structured transform exists.
    fn rotate(&self, input: &[f32], output: &mut [f32]) {
        assert_eq!(input.len(), self.dim);
        assert_eq!(output.len(), self.dim);
        for (row, output) in self.rows.chunks_exact(self.dim).zip(output) {
            *output = row
                .iter()
                .zip(input)
                .map(|(basis, value)| basis * f64::from(*value))
                .sum::<f64>() as f32;
        }
    }
}

/// Deterministic standard-normal source built on Box-Muller.
///
/// The offline oracle needs Gaussian samples for the dense reference matrix and
/// for its input pairs. Box-Muller converts two uniform draws into two
/// independent normal values; keeping the second in `spare` halves the
/// generator calls.
///
/// The type is [`Copy`] because it is a `u64` plus an `Option<f64>`. Copying it
/// forks the stream rather than sharing it, which is fine for the single-owner
/// use here but would be a trap if it were ever passed by value into a helper
/// expecting to advance the caller's stream.
#[derive(Debug, Clone, Copy)]
struct StandardNormal {
    /// Underlying uniform generator.
    rng: SplitMix64,
    /// The second value from the previous Box-Muller pair, consumed by the next
    /// call before any new uniforms are drawn.
    spare: Option<f64>,
}

impl StandardNormal {
    /// Creates a normal source positioned by `seed`.
    const fn new(seed: u64) -> Self {
        Self {
            rng: SplitMix64::new(seed),
            spare: None,
        }
    }

    /// Draws the next standard-normal sample.
    ///
    /// # Returns
    ///
    /// A value from `N(0, 1)`. Alternate calls consume the cached second half
    /// of the previous Box-Muller pair rather than drawing new uniforms, so the
    /// stream depends on the exact call sequence — reordering calls changes the
    /// samples and therefore the oracle's reported numbers.
    fn next(&mut self) -> f64 {
        if let Some(value) = self.spare.take() {
            return value;
        }
        let radius = (-2.0 * self.rng.next_open_unit_f64().ln()).sqrt();
        let angle = std::f64::consts::TAU * self.rng.next_open_unit_f64();
        let (sin, cos) = angle.sin_cos();
        self.spare = Some(radius * sin);
        radius * cos
    }
}

/// Pins the statistical properties the RaBitQ estimator depends on.
///
/// These tests protect claims that no compilation error and no length check can
/// protect: that the rotation is orthogonal, that the estimator is unbiased,
/// that its error decays like `1 / sqrt(D)`, that the two-bit grid search
/// genuinely improves on the one-bit code, and that the cheap structured
/// rotation is not measurably worse than a dense one. A numerics change that
/// still compiles and still produces finite scores will be caught here.
///
/// They are **not** a retrieval-quality gate. Per `src/index/CLAUDE.md`,
/// `tests/ivf_recall_gate.rs` is the quality authority, and every change to
/// these numerics must be re-measured against it on both `wikidpr1m` and
/// `wikidpr2m`.
///
/// Statistical assertions use a five-standard-error band rather than a fixed
/// tolerance, so they stay stable as sample counts change while still rejecting
/// a real bias. All inputs come from fixed seeds, so a failure reproduces
/// exactly.
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        /// Proves the structured rotation is orthogonal for arbitrary seeds.
        ///
        /// Norms and inner products must survive the transform, because the
        /// whole justification for rotating before encoding is that no geometry
        /// is lost. A broken Hadamard normalization or a permutation that
        /// duplicated a coordinate would show up here as a changed norm.
        #[test]
        fn structured_rotation_property_preserves_geometry(
            rotation_seed in any::<u64>(),
            left_seed in any::<u64>(),
            right_seed in any::<u64>(),
        ) {
            let rotation = StructuredRotation::new(256, rotation_seed)
                .expect("property dimensions are valid");
            let mut left = deterministic_vector(256, left_seed);
            let mut right = deterministic_vector(256, right_seed);
            let expected_norm = dot(&left, &left);
            let expected_ip = dot(&left, &right);
            let mut scratch = vec![0.0; 256];
            rotation.rotate_in_place(&mut left, &mut scratch)
                .expect("matching dimensions");
            rotation.rotate_in_place(&mut right, &mut scratch)
                .expect("matching dimensions");

            prop_assert!((dot(&left, &left) - expected_norm).abs()
                <= 2.0e-5 * (1.0 + expected_norm.abs()));
            prop_assert!((dot(&left, &right) - expected_ip).abs()
                <= 2.0e-5 * (1.0 + expected_ip.abs()));
        }
    }

    /// Checks orthogonality at the production dimension of 768.
    ///
    /// The property test above runs at 256, a single block, where the
    /// permutation stage has nothing to mix between. This case spans three
    /// blocks and so exercises the cross-block path the property test cannot
    /// reach.
    #[test]
    fn structured_rotation_preserves_norms_and_inner_products() {
        let rotation = StructuredRotation::new(768, 0x5eed).expect("valid rotation");
        let mut left = deterministic_vector(768, 11);
        let mut right = deterministic_vector(768, 29);
        let original_left_norm = dot(&left, &left);
        let original_ip = dot(&left, &right);
        let mut scratch = vec![0.0; 768];

        rotation
            .rotate_in_place(&mut left, &mut scratch)
            .expect("matching dimensions");
        rotation
            .rotate_in_place(&mut right, &mut scratch)
            .expect("matching dimensions");

        assert_close(dot(&left, &left), original_left_norm, 2.0e-4);
        assert_close(dot(&left, &right), original_ip, 2.0e-4);
    }

    /// Proves the popcount kernel equals the explicit decode-and-multiply sum.
    ///
    /// `QueryAdc4::signed_dot` computes a dot product without ever
    /// reconstructing a query coordinate, using an algebraic rearrangement into
    /// centered popcounts. This test recomputes the same value the obvious way
    /// — unpack each bit, rebuild `lower + step * code`, multiply by the sign —
    /// and requires they agree. It is the guard against a sign, shift, or
    /// centering error in that rearrangement, which would otherwise produce
    /// plausible-looking but wrong scores.
    #[test]
    fn four_bit_planes_match_decoded_one_bit_dot_product() {
        let data = deterministic_vector(256, 41);
        let query = deterministic_vector(256, 97);
        let code = encode_one_bit(&data).expect("non-empty residual");
        let adc = prepare_query_adc4(&query, 0xadc4).expect("matching dimension");

        let actual = adc
            .signed_dot(&code.words)
            .expect("matching code dimension");
        let mut expected = 0.0_f32;
        for index in 0..query.len() {
            let sign = if bit_at(&code.words, index) {
                1.0
            } else {
                -1.0
            };
            let quantized = (0..QUERY_BITS)
                .map(|plane| usize::from(bit_at(&adc.planes[plane], index)) << plane)
                .sum::<usize>();
            let decoded = adc.lower + adc.step * quantized as f32;
            expected += sign * decoded;
        }

        assert_close(actual, expected, 2.0e-4);
    }

    /// Proves the borrowed and owned one-bit APIs are bit-for-bit equivalent.
    ///
    /// Production paths use the `_into` and `_parts` forms for their allocation
    /// behavior, while tests and the bake-off often use the owned wrappers. If
    /// the two ever diverged, every measurement taken through one would stop
    /// describing the other. The output buffer is pre-filled with `u64::MAX` so
    /// a missing `fill(0)` in the encoder would fail rather than pass by luck.
    #[test]
    fn allocation_free_one_bit_api_matches_owned_wrapper() {
        let residual = deterministic_vector(256, 53);
        let query_values = deterministic_vector(256, 59);
        let owned = encode_one_bit(&residual).expect("valid residual");
        let mut words = vec![u64::MAX; words_per_code(256).expect("valid dimension")];
        let factors = encode_one_bit_into(&residual, &mut words).expect("sized output");
        let query = prepare_query_adc4(&query_values, 61).expect("valid query");
        let query_norm_sq = dot(&query_values, &query_values);

        assert_eq!(owned.words, words);
        assert_close(owned.residual_norm, factors.residual_norm, 0.0);
        assert_close(owned.bar_dot_residual, factors.bar_dot_residual, 0.0);
        let owned_distance =
            estimate_l2_one_bit(&owned, &query, query_norm_sq).expect("matching dimensions");
        let borrowed_distance = estimate_l2_one_bit_parts(&words, factors, &query, query_norm_sq)
            .expect("matching dimensions");
        assert_close(owned_distance, borrowed_distance, 0.0);
    }

    /// Pins the two-bit grid-direction search on a case with a known answer.
    ///
    /// For a residual that is `1.0` in one coordinate and zero elsewhere, the
    /// optimal grid vector promotes exactly that coordinate to `+1.5` and
    /// leaves all 255 others at `+0.5`; the resulting alignment is
    /// `1.5 / sqrt(66)`. A prefix search that stopped early, ran one step too
    /// far, or sorted ascending would land on a different code.
    #[test]
    fn exact_two_bit_search_selects_the_best_grid_direction() {
        let mut residual = vec![0.0_f32; 256];
        residual[0] = 1.0;
        let code = encode_two_bit(&residual).expect("valid residual");

        assert_eq!(
            two_bit_value(&code.planes, 0),
            3,
            "largest positive is +1.5"
        );
        for index in 1..256 {
            assert_eq!(two_bit_value(&code.planes, index), 2, "zeros stay +0.5");
        }
        assert_close(code.residual_norm, 1.0, 0.0);
        assert_close(code.bar_dot_residual, 1.5 / 66.0_f32.sqrt(), 1.0e-7);
    }

    /// Validates the offline oracle's own reference rotation.
    ///
    /// `DenseRotation` is the yardstick the structured transform is judged
    /// against, so a Gram-Schmidt bug there would silently move the bar. This
    /// checks that the generated matrix really is orthonormal before any
    /// comparison relies on it.
    #[test]
    fn dense_768_oracle_preserves_norms_and_inner_products() {
        let rotation = DenseRotation::new(768, 0xd00d_5eed);
        let left = deterministic_vector(768, 71);
        let right = deterministic_vector(768, 73);
        let mut rotated_left = vec![0.0; 768];
        let mut rotated_right = vec![0.0; 768];
        rotation.rotate(&left, &mut rotated_left);
        rotation.rotate(&right, &mut rotated_right);

        assert_close(dot(&rotated_left, &rotated_left), dot(&left, &left), 3.0e-4);
        assert_close(
            dot(&rotated_left, &rotated_right),
            dot(&left, &right),
            3.0e-4,
        );
    }

    /// Proves the one-bit estimator has no systematic bias.
    ///
    /// Unbiasedness is the property that makes a one-bit code usable at all: a
    /// small random error per row averages out across a candidate set, while a
    /// constant offset would shift every score and distort the ranking. Ten
    /// thousand pairs with a known exact inner product of 0.35 must have mean
    /// error within five standard errors of zero. Losing the stochastic dither
    /// in [`prepare_query_adc4`], for instance, would fail here.
    #[test]
    fn one_bit_estimator_is_unbiased_over_ten_thousand_pairs() {
        const DIM: usize = 768;
        const PAIRS: usize = 10_000;
        const EXACT_DOT: f32 = 0.35;
        let mut gaussian = StandardNormal::new(0x1bad_c0de);
        let mut residual = vec![0.0; DIM];
        let mut orthogonal = vec![0.0; DIM];
        let mut query = vec![0.0; DIM];
        let mut words = vec![0_u64; DIM / 64];
        let mut error_sum = 0.0_f64;
        let mut error_square_sum = 0.0_f64;

        for pair in 0..PAIRS {
            fill_correlated_unit_pair(
                &mut gaussian,
                EXACT_DOT,
                &mut residual,
                &mut orthogonal,
                &mut query,
            );
            let factors = encode_one_bit_into(&residual, &mut words).expect("valid residual code");
            let adc =
                prepare_query_adc4(&query, pair as u64 ^ 0xa11c_e5ed).expect("valid query ADC");
            let estimate = estimate_residual_dot_one_bit_parts(&words, factors, &adc)
                .expect("matching dimensions");
            let error = f64::from(estimate - EXACT_DOT);
            error_sum += error;
            error_square_sum += error * error;
        }

        let mean = error_sum / PAIRS as f64;
        let variance = (error_square_sum / PAIRS as f64 - mean * mean).max(0.0);
        let standard_error = (variance / PAIRS as f64).sqrt();
        assert!(
            mean.abs() <= 5.0 * standard_error + 2.0e-4,
            "mean error {mean} exceeds five standard errors {standard_error}"
        );
    }

    /// Proves the two-bit estimator is unbiased on the full production path.
    ///
    /// Unlike the one-bit case above, this runs the adversarial sparse
    /// anisotropic distribution through the real structured rotation before
    /// encoding — the exact sequence a production sketch performs. It therefore
    /// covers the interaction between the rotation, the grid-direction search,
    /// and the two-bit popcount kernel, any of which could introduce a bias the
    /// others hide.
    #[test]
    fn two_bit_estimator_is_unbiased_after_structured_rotation_over_ten_thousand_pairs() {
        let (mean_error, standard_error) = structured_two_bit_anisotropic_error_stats(10_000);
        assert!(
            mean_error.abs() <= 5.0 * standard_error,
            "mean error {mean_error} exceeds five standard errors {standard_error}"
        );
    }

    /// Pins the estimator's `1 / sqrt(D)` error-decay law.
    ///
    /// RaBitQ's guarantee is not just that the error is unbiased but that it
    /// shrinks as the dimension grows — which is why one bit per coordinate is
    /// viable at 768 dimensions and would not be at 32. Measuring RMSE at 256,
    /// 512, and 768 and requiring `RMSE * sqrt(D)` to stay within a 1.45x band
    /// checks the shape of the decay, not just its direction.
    #[test]
    fn one_bit_error_decreases_at_inverse_sqrt_dimension() {
        let (rmse_256, _) = estimator_rmse(256, 2_000, 0x256);
        let (rmse_512, _) = estimator_rmse(512, 2_000, 0x512);
        let (rmse_768, _) = estimator_rmse(768, 2_000, 0x768);
        let scaled = [
            rmse_256 * 256.0_f64.sqrt(),
            rmse_512 * 512.0_f64.sqrt(),
            rmse_768 * 768.0_f64.sqrt(),
        ];
        let min_scaled = scaled.iter().copied().fold(f64::INFINITY, f64::min);
        let max_scaled = scaled.iter().copied().fold(f64::NEG_INFINITY, f64::max);

        assert!(
            rmse_768 < rmse_512 && rmse_512 < rmse_256,
            "RMSE did not decrease with D: {rmse_256}, {rmse_512}, {rmse_768}"
        );
        assert!(
            max_scaled / min_scaled < 1.45,
            "RMSE*sqrt(D) not stable enough: {scaled:?}"
        );
    }

    /// Proves the second bit plane earns its storage cost.
    ///
    /// A two-bit row is roughly twice the bytes of a one-bit row, so the
    /// estimator error must fall materially — here, below 70% of the one-bit
    /// RMSE at 768 dimensions. This is the unit-level counterpart to the
    /// measured recall finding in `src/index/CLAUDE.md` that one-bit retains
    /// only about 95% of the exact ceiling on E5-768 while two-bit retains at
    /// least 99.3%.
    #[test]
    fn exact_two_bit_search_improves_estimator_rmse() {
        let (one_bit_rmse, two_bit_rmse) = estimator_rmse(768, 3_000, 0x2b17);
        assert!(
            two_bit_rmse < one_bit_rmse * 0.7,
            "2-bit RMSE {two_bit_rmse} did not improve enough over 1-bit {one_bit_rmse}"
        );
    }

    /// Exercises the full centered-residual bookkeeping end to end.
    ///
    /// This is the closest test to real search: unit vectors, a centroid that
    /// is neither the origin nor any candidate, residuals with genuinely
    /// varying norms (asserted to span below 0.75 and above 1.0, so the
    /// centering is not accidentally trivial), and 384 candidates spanning
    /// cosines from -0.2 to 0.95. It verifies the algebraic identity that
    /// centering both sides on the same centroid preserves the squared
    /// distance, that [`estimate_cosine_from_unit_l2`] is consistent with its
    /// input, and that estimated and exact squared-L2 correlate above 0.97 —
    /// the property that makes the estimate usable for ranking.
    #[test]
    fn centered_unit_vectors_keep_correct_cosine_and_l2_bookkeeping() {
        const DIM: usize = 768;
        const CANDIDATES: usize = 384;
        let rotation = StructuredRotation::new(DIM, 0xc05e).expect("valid rotation");
        let mut gaussian = StandardNormal::new(0xce17_3eed);
        let mut query = vec![0.0; DIM];
        let mut centroid_axis = vec![0.0; DIM];
        for value in &mut query {
            *value = gaussian.next() as f32;
        }
        normalize(&mut query);
        for value in &mut centroid_axis {
            *value = gaussian.next() as f32;
        }
        remove_projection_and_normalize(&query, &mut centroid_axis);
        let centroid: Vec<f32> = query
            .iter()
            .zip(&centroid_axis)
            .map(|(query_value, axis)| 0.35 * query_value + 0.12 * axis)
            .collect();

        let mut rotated_query_residual = vec![0.0; DIM];
        let mut scratch = vec![0.0; DIM];
        rotation
            .rotate_residual(&query, &centroid, &mut rotated_query_residual, &mut scratch)
            .expect("matching dimensions");
        let query_residual_norm_sq = query
            .iter()
            .zip(&centroid)
            .map(|(value, center)| (value - center) * (value - center))
            .sum::<f32>();
        let adc =
            prepare_query_adc4(&rotated_query_residual, 0xc05e_adc4).expect("valid query ADC");

        let mut candidate_axis = vec![0.0; DIM];
        let mut candidate = vec![0.0; DIM];
        let mut rotated_residual = vec![0.0; DIM];
        let mut words = vec![0_u64; DIM / 64];
        let mut exact_distances = Vec::with_capacity(CANDIDATES);
        let mut estimated_distances = Vec::with_capacity(CANDIDATES);
        let mut residual_norm_min = f32::INFINITY;
        let mut residual_norm_max = f32::NEG_INFINITY;

        for candidate_index in 0..CANDIDATES {
            for value in &mut candidate_axis {
                *value = gaussian.next() as f32;
            }
            remove_projection_and_normalize(&query, &mut candidate_axis);
            let exact_cosine = -0.2 + 1.15 * candidate_index as f32 / (CANDIDATES - 1) as f32;
            let perpendicular = (1.0 - exact_cosine * exact_cosine).sqrt();
            for ((value, query_value), axis) in
                candidate.iter_mut().zip(&query).zip(&candidate_axis)
            {
                *value = exact_cosine.mul_add(*query_value, perpendicular * axis);
            }

            let exact_l2 = candidate
                .iter()
                .zip(&query)
                .map(|(left, right)| (left - right) * (left - right))
                .sum::<f32>();
            let residual_l2 = candidate
                .iter()
                .zip(&centroid)
                .map(|(left, center)| left - center)
                .zip(
                    query
                        .iter()
                        .zip(&centroid)
                        .map(|(right, center)| right - center),
                )
                .map(|(left, right)| (left - right) * (left - right))
                .sum::<f32>();
            assert_close(residual_l2, exact_l2, 2.0e-5);

            rotation
                .rotate_residual(&candidate, &centroid, &mut rotated_residual, &mut scratch)
                .expect("matching dimensions");
            let factors =
                encode_one_bit_into(&rotated_residual, &mut words).expect("valid one-bit code");
            residual_norm_min = residual_norm_min.min(factors.residual_norm);
            residual_norm_max = residual_norm_max.max(factors.residual_norm);
            let estimated_l2 =
                estimate_l2_one_bit_parts(&words, factors, &adc, query_residual_norm_sq)
                    .expect("matching dimensions");
            let estimated_cosine = estimate_cosine_from_unit_l2(estimated_l2);
            assert_close(estimated_cosine, 1.0 - 0.5 * estimated_l2, 0.0);
            exact_distances.push(exact_l2);
            estimated_distances.push(estimated_l2);
        }

        assert!(
            residual_norm_min < 0.75 && residual_norm_max > 1.0,
            "centering should create non-unit, varying norms: min={residual_norm_min}, max={residual_norm_max}"
        );
        let correlation = pearson(&exact_distances, &estimated_distances);
        assert!(
            correlation > 0.97,
            "estimated/exact squared-L2 correlation too low: {correlation}"
        );
    }

    /// Protects the claim that the cheap structured rotation is as good as a
    /// dense random one for 768-dimensional residuals.
    ///
    /// The structured rotation exists only because it is `O(D log D)` instead
    /// of `O(D^2)`. That trade is worth taking only if it costs no accuracy, so
    /// this pins two separate properties: the structured/dense MSE difference
    /// stays inside five standard errors of the measured noise, and the
    /// structured rotation still beats doing nothing by the pinned ratio.
    ///
    /// Without the second assertion the first would pass trivially if both
    /// rotations silently degenerated toward the identity.
    #[test]
    fn structured_and_dense_rotations_have_quality_within_noise() {
        let quality = compare_structured_dense_quality_768()
            .expect("structured rotation should stay within five standard errors of dense");
        assert_eq!(quality.pairs, 256);
        assert!(
            quality.mse_delta.abs() <= 5.0 * quality.mse_delta_standard_error,
            "MSE delta {} exceeds noise SE {}; structured={}, dense={}",
            quality.mse_delta,
            quality.mse_delta_standard_error,
            quality.structured_rmse,
            quality.dense_rmse,
        );
        assert!(
            quality.structured_rmse <= ROTATION_IDENTITY_MAX_RMSE_RATIO * quality.identity_rmse,
            "structured RMSE {} did not materially beat identity RMSE {}",
            quality.structured_rmse,
            quality.identity_rmse,
        );
    }

    /// Builds a reproducible pseudo-random vector with components in `[-1, 1]`.
    ///
    /// Uses an inline LCG rather than a seeded RNG type so a failing case can
    /// be reproduced from the seed alone, with no dependency on any crate's
    /// generator staying bit-stable across versions.
    ///
    /// # Parameters
    ///
    /// - `dim`: Component count. Callers needing an encodable vector must pass
    ///   a multiple of [`BLOCK_DIM`].
    /// - `seed`: Chooses the stream; distinct seeds give independent vectors.
    fn deterministic_vector(dim: usize, seed: u64) -> Vec<f32> {
        let mut state = seed;
        (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ((state >> 40) as f32 / (1_u32 << 24) as f32) * 2.0 - 1.0
            })
            .collect()
    }

    /// Writes a residual/query pair of unit vectors whose inner product is
    /// exactly `exact_dot`.
    ///
    /// Estimator tests need a known ground truth. Rather than sampling pairs
    /// and measuring their dot product, this constructs the pair to hit the
    /// target exactly: it draws a random unit `residual`, builds a unit
    /// `orthogonal` direction perpendicular to it via Gram-Schmidt, then sets
    /// `query = exact_dot * residual + sqrt(1 - exact_dot^2) * orthogonal`.
    /// The result is a unit vector at the requested angle, so any deviation an
    /// estimator reports is estimator error and nothing else.
    ///
    /// # Parameters
    ///
    /// - `gaussian`: Source of standard normal samples; advanced in place.
    /// - `exact_dot`: Target inner product, in `[-1, 1]`.
    /// - `residual`, `orthogonal`, `query`: Caller-owned buffers, all the same
    ///   length, overwritten in full. `orthogonal` is scratch.
    fn fill_correlated_unit_pair(
        gaussian: &mut StandardNormal,
        exact_dot: f32,
        residual: &mut [f32],
        orthogonal: &mut [f32],
        query: &mut [f32],
    ) {
        for value in residual.iter_mut() {
            *value = gaussian.next() as f32;
        }
        normalize(residual);
        for value in orthogonal.iter_mut() {
            *value = gaussian.next() as f32;
        }
        let projection = dot(residual, orthogonal);
        for (value, axis) in orthogonal.iter_mut().zip(residual.iter()) {
            *value -= projection * axis;
        }
        normalize(orthogonal);
        let perpendicular_scale = (1.0 - exact_dot * exact_dot).sqrt();
        for ((output, axis), perpendicular) in
            query.iter_mut().zip(residual.iter()).zip(orthogonal.iter())
        {
            *output = exact_dot.mul_add(*axis, perpendicular_scale * perpendicular);
        }
    }

    /// Measures one-bit and two-bit estimator RMSE over synthetic pairs at a
    /// fixed known inner product.
    ///
    /// Drives the full production path per pair — encode, prepare the 4-bit
    /// query ADC, estimate — so the number reflects the real kernels rather
    /// than an idealized model of them.
    ///
    /// # Parameters
    ///
    /// - `dim`: Vector dimension; must be a multiple of [`BLOCK_DIM`].
    /// - `pairs`: Sample count. RMSE noise falls as `1/sqrt(pairs)`.
    /// - `seed`: Makes the whole measurement reproducible.
    ///
    /// # Returns
    ///
    /// `(one_bit_rmse, two_bit_rmse)` against the known dot product.
    fn estimator_rmse(dim: usize, pairs: usize, seed: u64) -> (f64, f64) {
        const EXACT_DOT: f32 = 0.25;
        let mut gaussian = StandardNormal::new(seed);
        let mut residual = vec![0.0; dim];
        let mut orthogonal = vec![0.0; dim];
        let mut query = vec![0.0; dim];
        let mut one_bit_words = vec![0_u64; dim / 64];
        let mut two_bit_low = vec![0_u64; dim / 64];
        let mut two_bit_high = vec![0_u64; dim / 64];
        let mut order = vec![0_usize; dim];
        let mut one_bit_squared_error = 0.0_f64;
        let mut two_bit_squared_error = 0.0_f64;

        for pair in 0..pairs {
            fill_correlated_unit_pair(
                &mut gaussian,
                EXACT_DOT,
                &mut residual,
                &mut orthogonal,
                &mut query,
            );
            let one_bit =
                encode_one_bit_into(&residual, &mut one_bit_words).expect("valid one-bit code");
            let two_bit =
                encode_two_bit_into(&residual, &mut two_bit_low, &mut two_bit_high, &mut order)
                    .expect("valid two-bit code");
            let adc = prepare_query_adc4(&query, seed ^ pair as u64).expect("valid query ADC");
            let one_bit_estimate =
                estimate_residual_dot_one_bit_parts(&one_bit_words, one_bit, &adc)
                    .expect("matching one-bit code");
            let two_bit_estimate =
                estimate_residual_dot_two_bit_parts(&two_bit_low, &two_bit_high, two_bit, &adc)
                    .expect("matching two-bit code");
            one_bit_squared_error += f64::from(one_bit_estimate - EXACT_DOT).powi(2);
            two_bit_squared_error += f64::from(two_bit_estimate - EXACT_DOT).powi(2);
        }

        (
            (one_bit_squared_error / pairs as f64).sqrt(),
            (two_bit_squared_error / pairs as f64).sqrt(),
        )
    }

    /// Measures two-bit estimator bias on deliberately anisotropic 768-dim
    /// input, after the structured rotation.
    ///
    /// The estimator is unbiased for isotropic residuals. Real embeddings are
    /// not isotropic, and the structured rotation is what is supposed to
    /// restore that assumption. This concentrates energy in a narrow
    /// coordinate band — the adversarial case for a blockwise rotation — and
    /// reports the mean error with its standard error so a caller can test
    /// whether bias is distinguishable from zero rather than merely small.
    ///
    /// # Parameters
    ///
    /// - `pairs`: Sample count; the standard error scales as `1/sqrt(pairs)`.
    ///
    /// # Returns
    ///
    /// `(mean_error, standard_error_of_the_mean)`.
    fn structured_two_bit_anisotropic_error_stats(pairs: usize) -> (f64, f64) {
        const DIM: usize = 768;
        const EXACT_DOT: f32 = 0.25;
        let rotation = StructuredRotation::new(DIM, 0x2b17_5eed).expect("valid rotation");
        let mut gaussian = StandardNormal::new(0x2b17_a115);
        let mut residual = vec![0.0; DIM];
        let mut orthogonal = vec![0.0; DIM];
        let mut query = vec![0.0; DIM];
        let mut rotated_residual = vec![0.0; DIM];
        let mut rotated_query = vec![0.0; DIM];
        let mut scratch = vec![0.0; DIM];
        let mut low_plane = vec![0_u64; DIM / 64];
        let mut high_plane = vec![0_u64; DIM / 64];
        let mut order = vec![0_usize; DIM];
        let mut error_sum = 0.0_f64;
        let mut error_square_sum = 0.0_f64;

        for pair in 0..pairs {
            fill_anisotropic_sparse_pair(
                &mut gaussian,
                EXACT_DOT,
                &mut residual,
                &mut orthogonal,
                &mut query,
            );
            rotated_residual.copy_from_slice(&residual);
            rotated_query.copy_from_slice(&query);
            rotation
                .rotate_in_place(&mut rotated_residual, &mut scratch)
                .expect("matching residual dimension");
            rotation
                .rotate_in_place(&mut rotated_query, &mut scratch)
                .expect("matching query dimension");
            let factors = encode_two_bit_into(
                &rotated_residual,
                &mut low_plane,
                &mut high_plane,
                &mut order,
            )
            .expect("valid two-bit code");
            let adc = prepare_query_adc4(&rotated_query, pair as u64 ^ 0x2b17_adc4)
                .expect("valid query ADC");
            let estimate =
                estimate_residual_dot_two_bit_parts(&low_plane, &high_plane, factors, &adc)
                    .expect("matching two-bit code");
            let exact_dot = offline_dot(&residual, &query) as f32;
            let error = f64::from(estimate - exact_dot);
            error_sum += error;
            error_square_sum += error * error;
        }

        let mean_error = error_sum / pairs as f64;
        let variance = (error_square_sum / pairs as f64 - mean_error * mean_error).max(0.0);
        (mean_error, (variance / pairs as f64).sqrt())
    }

    /// Scales a vector in place to unit L2 norm.
    ///
    /// # Panics
    ///
    /// Does not panic, but a zero vector yields non-finite components. Callers
    /// here always pass a freshly drawn Gaussian vector, which is non-zero with
    /// probability one.
    fn normalize(values: &mut [f32]) {
        let norm = dot(values, values).sqrt();
        for value in values {
            *value /= norm;
        }
    }

    /// Projects `values` onto the hyperplane orthogonal to `axis`, then
    /// normalizes it.
    ///
    /// One Gram-Schmidt step. Used to build a unit direction perpendicular to
    /// a chosen axis, which is what lets a test place a query at an exact angle
    /// from a residual.
    ///
    /// # Parameters
    ///
    /// - `axis`: Unit vector to remove. Must already be normalized.
    /// - `values`: Buffer modified in place.
    fn remove_projection_and_normalize(axis: &[f32], values: &mut [f32]) {
        let projection = dot(axis, values);
        for (value, axis_value) in values.iter_mut().zip(axis) {
            *value -= projection * axis_value;
        }
        normalize(values);
    }

    /// Returns the Pearson correlation coefficient between two equal-length
    /// samples.
    ///
    /// Used where a test cares that estimated and exact distances *rank*
    /// consistently, which is what drives recall, rather than that they agree
    /// in absolute value.
    ///
    /// # Panics
    ///
    /// Panics when the two slices differ in length.
    fn pearson(left: &[f32], right: &[f32]) -> f32 {
        assert_eq!(left.len(), right.len());
        let left_mean = left.iter().sum::<f32>() / left.len() as f32;
        let right_mean = right.iter().sum::<f32>() / right.len() as f32;
        let mut covariance = 0.0_f32;
        let mut left_variance = 0.0_f32;
        let mut right_variance = 0.0_f32;
        for (&left, &right) in left.iter().zip(right) {
            let centered_left = left - left_mean;
            let centered_right = right - right_mean;
            covariance += centered_left * centered_right;
            left_variance += centered_left * centered_left;
            right_variance += centered_right * centered_right;
        }
        covariance / (left_variance * right_variance).sqrt()
    }

    /// Computes a plain `f32` inner product over the overlapping prefix.
    ///
    /// Deliberately the naive scalar form: it is the independent reference the
    /// optimized popcount kernels are checked against, so it must not share
    /// their implementation.
    fn dot(left: &[f32], right: &[f32]) -> f32 {
        left.iter().zip(right).map(|(l, r)| l * r).sum()
    }

    /// Asserts two floats agree within an absolute tolerance, reporting all
    /// three values on failure.
    ///
    /// # Panics
    ///
    /// Panics when `|actual - expected|` exceeds `tolerance`.
    fn assert_close(actual: f32, expected: f32, tolerance: f32) {
        assert!(
            (actual - expected).abs() <= tolerance,
            "actual={actual}, expected={expected}, tolerance={tolerance}"
        );
    }

    /// Reads back the 2-bit code stored for one coordinate.
    ///
    /// Reassembles the value from the bit-plane layout — low plane in bit 0,
    /// high plane in bit 1 — so a test can assert on the stored code without
    /// duplicating the packing rule.
    ///
    /// # Returns
    ///
    /// The quantization level in `0..=3`.
    fn two_bit_value(planes: &[Vec<u64>; 2], index: usize) -> u8 {
        u8::from(bit_at(&planes[0], index)) | (u8::from(bit_at(&planes[1], index)) << 1)
    }
}
