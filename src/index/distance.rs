//! Shared vector-distance kernels for indexing, query search, and reranking.
//!
//! Every index implementation enters through
//! [`crate::index::distance::compute_distance`] or a metric-specific helper.
//! The central contract is that **smaller values always rank as closer**:
//! Euclidean distance is squared, cosine similarity becomes `1 - similarity`,
//! and dot-product similarity is negated. This lets top-k code use one ascending
//! ordering regardless of the namespace metric.
//!
//! ```text
//! equal-length f32 slices + DistanceMetric
//!                    |
//!                    v
//!             compute_distance
//!          / cosine | euclidean \ dot
//!         v         v             v
//! compile-time scalar, AVX2, or NEON kernel
//!                    |
//!                    v
//!       one f32 distance; lower ranks first
//! ```
//!
//! These functions are pure CPU operations: they perform no allocation, object
//! storage I/O, cache access, or manifest lookup. Callers must already have
//! validated equal vector dimensions and finite values at the API boundary.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::index::distance::compute_distance`] for metric
//!    dispatch and the shared ordering rule.
//! 2. Read the three public metric functions for mathematical behavior.
//! 3. Read the `*_inner` and `cosine_components` variants to see how `cfg`
//!    selects scalar or architecture-specific kernels at compile time.
//! 4. Finish with [`crate::index::distance::normalize`] for in-place unit-vector
//!    preparation.
//!
//! ## Rust concepts used here
//!
//! Borrowed slices (`&[f32]`) are non-owning pointer-and-length views. They are
//! similar to Java array ranges or C pointer/length pairs, but Rust guarantees
//! that the backing allocation remains alive and cannot be mutated through the
//! shared borrow. `#[cfg]` compiles exactly one kernel per target. The AVX2 and
//! NEON paths contain `unsafe` pointer loads; their safety depends on the
//! equal-length precondition already established by callers.

use crate::types::DistanceMetric;

/// Dispatches to the metric selected for a namespace.
///
/// All distance functions return a *distance* (lower is closer) so that
/// callers can sort ascending and take the first `k` results.
///
/// # Parameters
///
/// - `a`: First borrowed vector coordinate slice.
/// - `b`: Second borrowed vector coordinate slice with the same length as `a`.
/// - `metric`: Mathematical comparison selected by namespace metadata.
///
/// # Returns
///
/// Returns one distance where a smaller value ranks first. Dot-product results
/// may be negative because similarity is negated.
///
/// # Panics
///
/// Metric helpers debug-assert equal dimensions. Scalar kernels may also panic
/// on indexing if `b` is shorter in a release build. Callers must validate
/// dimensions before entering this hot path.
///
/// # Performance
///
/// Performs one linear pass over the coordinates with no allocation. The
/// selected target may use scalar, AVX2, or NEON instructions.
///
/// # Examples
///
/// For orthogonal unit vectors, cosine distance is `1`, squared Euclidean
/// distance is `2`, and dot-product distance is `0`.
#[inline]
pub fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::Euclidean => euclidean_distance(a, b),
        DistanceMetric::DotProduct => dot_product_distance(a, b),
    }
}

/// Computes cosine distance as `1 - cosine_similarity(a, b)`.
///
/// Returns 0.0 for identical directions and 2.0 for opposite directions.
/// If either vector has zero magnitude, returns 1.0 (orthogonal).
///
/// # Parameters
///
/// - `a`: First borrowed vector.
/// - `b`: Equal-length second vector.
///
/// # Returns
///
/// Returns a clamped value from `0` for the same direction through `2` for the
/// opposite direction. A zero-magnitude input returns `1`.
///
/// # Panics
///
/// Panics when the lengths differ. The SIMD kernels rely on that equality for
/// safe pointer loads, so it is enforced in release builds as well: a
/// mismatched pair must fail loud here rather than read out of bounds in the
/// NEON or AVX2 path.
///
/// # Performance
///
/// Accumulates dot product and both squared norms in one linear pass, then
/// performs one square root.
///
/// # Examples
///
/// `[1, 0]` compared with `[1, 0]` returns `0`; compared with `[-1, 0]` it
/// returns `2`.
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "vector dimensions must match");

    let (dot, norm_a, norm_b) = cosine_components(a, b);

    let denom = (norm_a * norm_b).sqrt();
    if denom < f32::EPSILON {
        return 1.0;
    }

    let similarity = dot / denom;
    // Clamp to [-1, 1] to handle floating-point drift.
    1.0 - similarity.clamp(-1.0, 1.0)
}

/// Computes squared Euclidean distance, `sum((a_i - b_i)^2)`.
///
/// We return squared distance to avoid the sqrt cost. This preserves
/// ordering for top-k selection.
///
/// # Parameters
///
/// - `a`: First borrowed vector.
/// - `b`: Equal-length second vector.
///
/// # Returns
///
/// Returns a non-negative squared distance for finite coordinates.
///
/// # Panics
///
/// Panics when the lengths differ, in release builds as well; the SIMD kernels
/// rely on that equality for safe pointer loads.
///
/// # Performance
///
/// Performs a linear pass and deliberately avoids a square root because
/// squaring preserves nearest-neighbor order.
///
/// # Examples
///
/// `[0, 0]` and `[3, 4]` return `25`, not `5`.
#[inline]
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "vector dimensions must match");
    euclidean_squared_inner(a, b)
}

/// Computes dot-product distance as the negated inner product.
///
/// Negated so that higher similarity (larger dot product) yields a
/// smaller distance value, keeping the "lower is closer" invariant.
///
/// # Parameters
///
/// - `a`: First borrowed vector.
/// - `b`: Equal-length second vector.
///
/// # Returns
///
/// Returns the negative sum of pairwise products. More similar vectors therefore
/// produce smaller values.
///
/// # Panics
///
/// Panics when the lengths differ, in release builds as well; the SIMD kernels
/// rely on that equality for safe pointer loads.
///
/// # Performance
///
/// Performs one linear pass without allocation.
///
/// # Examples
///
/// `[1, 2]` compared with itself has similarity `5` and distance `-5`.
#[inline]
pub fn dot_product_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "vector dimensions must match");
    -dot_product_inner(a, b)
}

// ---------------------------------------------------------------------------
// Scalar kernels written to encourage auto-vectorization.
//
// The loops are structured so LLVM can emit SIMD instructions when compiling
// with `-C target-cpu=native` or appropriate target features.  We also
// provide explicit SIMD paths behind `target_arch` gates for x86-64 and
// aarch64 in the future.
// ---------------------------------------------------------------------------

/// Accumulates dot product and both squared norms with the scalar kernel.
///
/// # Parameters
///
/// - `a`: First vector.
/// - `b`: Equal-length second vector.
///
/// # Returns
///
/// Returns `(a · b, ||a||², ||b||²)`.
///
/// # Performance
///
/// Processes eight coordinates per outer iteration to encourage LLVM
/// auto-vectorization, then handles the remainder.
#[cfg(not(any(
    all(target_arch = "x86_64", target_feature = "avx2"),
    all(target_arch = "aarch64", target_feature = "neon"),
)))]
#[inline]
fn cosine_components(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    let mut dot: f32 = 0.0;
    let mut norm_a: f32 = 0.0;
    let mut norm_b: f32 = 0.0;

    // Process in chunks of 8 to hint at vectorization.
    let chunks = a.len() / 8;
    let remainder = a.len() % 8;

    for i in 0..chunks {
        let base = i * 8;
        let mut d = [0.0f32; 8];
        let mut na = [0.0f32; 8];
        let mut nb = [0.0f32; 8];
        for j in 0..8 {
            let ai = a[base + j];
            let bi = b[base + j];
            d[j] = ai * bi;
            na[j] = ai * ai;
            nb[j] = bi * bi;
        }
        for j in 0..8 {
            dot += d[j];
            norm_a += na[j];
            norm_b += nb[j];
        }
    }

    let base = chunks * 8;
    for i in 0..remainder {
        let ai = a[base + i];
        let bi = b[base + i];
        dot += ai * bi;
        norm_a += ai * ai;
        norm_b += bi * bi;
    }

    (dot, norm_a, norm_b)
}

/// Accumulates cosine components with AVX2 fused multiply-add instructions.
///
/// This definition exists only when the target enables x86-64 AVX2. It loads
/// unaligned groups of eight coordinates and handles the tail scalarly.
///
/// # Parameters
///
/// - `a`: First vector.
/// - `b`: Equal-length second vector.
///
/// # Returns
///
/// Returns `(a · b, ||a||², ||b||²)`.
///
/// # Rust Notes for Java/C Engineers
///
/// The intrinsics require `unsafe` because raw pointer arithmetic is not bounds
/// checked. Slice lifetimes keep the allocations alive, while the caller's
/// equal-length invariant ensures every eight-float load is inside both slices.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn cosine_components(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let chunks = a.len() / 8;
    let remainder = a.len() % 8;

    // SAFETY: the public wrappers assert `a.len() == b.len()`, and `chunks`
    // is derived from that common length, so every unaligned eight-float load
    // at `base` is in bounds for both slices.
    let (mut dot_sum, mut norm_a, mut norm_b) = unsafe {
        let mut dot_acc = _mm256_setzero_ps();
        let mut norm_a_acc = _mm256_setzero_ps();
        let mut norm_b_acc = _mm256_setzero_ps();

        for i in 0..chunks {
            let base = i * 8;
            let va = _mm256_loadu_ps(a.as_ptr().add(base));
            let vb = _mm256_loadu_ps(b.as_ptr().add(base));
            dot_acc = _mm256_fmadd_ps(va, vb, dot_acc);
            norm_a_acc = _mm256_fmadd_ps(va, va, norm_a_acc);
            norm_b_acc = _mm256_fmadd_ps(vb, vb, norm_b_acc);
        }

        (
            hsum_avx(dot_acc),
            hsum_avx(norm_a_acc),
            hsum_avx(norm_b_acc),
        )
    };

    // Handle the remainder with safe bounds-checked indexing.
    let base = chunks * 8;
    for i in 0..remainder {
        let ai = a[base + i];
        let bi = b[base + i];
        dot_sum += ai * bi;
        norm_a += ai * ai;
        norm_b += bi * bi;
    }

    (dot_sum, norm_a, norm_b)
}

/// Accumulates cosine components with AArch64 NEON instructions.
///
/// This compile-time variant loads four coordinates at a time and handles the
/// remainder with scalar indexing.
///
/// # Parameters
///
/// - `a`: First vector.
/// - `b`: Equal-length second vector.
///
/// # Returns
///
/// Returns `(a · b, ||a||², ||b||²)`.
///
/// # Rust Notes for Java/C Engineers
///
/// The unsafe block is narrowly scoped around architecture intrinsics. Rust
/// still manages the slice allocations; only the compiler-checked bounds are
/// replaced by the established equal-length and chunk-count argument.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline]
fn cosine_components(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    use std::arch::aarch64::*;

    let chunks = a.len() / 4;
    let remainder = a.len() % 4;

    // SAFETY: the public wrappers assert `a.len() == b.len()`, and `chunks`
    // is derived from that common length, so every four-float load at `base`
    // is in bounds for both slices.
    let (mut dot_sum, mut norm_a, mut norm_b) = unsafe {
        let mut dot_acc = vdupq_n_f32(0.0);
        let mut norm_a_acc = vdupq_n_f32(0.0);
        let mut norm_b_acc = vdupq_n_f32(0.0);

        for i in 0..chunks {
            let base = i * 4;
            let va = vld1q_f32(a.as_ptr().add(base));
            let vb = vld1q_f32(b.as_ptr().add(base));
            dot_acc = vfmaq_f32(dot_acc, va, vb);
            norm_a_acc = vfmaq_f32(norm_a_acc, va, va);
            norm_b_acc = vfmaq_f32(norm_b_acc, vb, vb);
        }

        (
            vaddvq_f32(dot_acc),
            vaddvq_f32(norm_a_acc),
            vaddvq_f32(norm_b_acc),
        )
    };

    // Handle the remainder with safe bounds-checked indexing.
    let base = chunks * 4;
    for i in 0..remainder {
        let ai = a[base + i];
        let bi = b[base + i];
        dot_sum += ai * bi;
        norm_a += ai * ai;
        norm_b += bi * bi;
    }

    (dot_sum, norm_a, norm_b)
}

/// Accumulates squared Euclidean distance with the scalar kernel.
///
/// # Parameters
///
/// - `a`: First vector.
/// - `b`: Equal-length second vector.
///
/// # Returns
///
/// Returns the sum of squared coordinate differences.
///
/// # Performance
///
/// Processes eight coordinates per outer loop to encourage auto-vectorization.
#[cfg(not(any(
    all(target_arch = "x86_64", target_feature = "avx2"),
    all(target_arch = "aarch64", target_feature = "neon"),
)))]
#[inline]
fn euclidean_squared_inner(a: &[f32], b: &[f32]) -> f32 {
    let mut sum: f32 = 0.0;
    let chunks = a.len() / 8;
    let remainder = a.len() % 8;

    for i in 0..chunks {
        let base = i * 8;
        let mut tmp = [0.0f32; 8];
        for j in 0..8 {
            let d = a[base + j] - b[base + j];
            tmp[j] = d * d;
        }
        for val in tmp {
            sum += val;
        }
    }

    let base = chunks * 8;
    for i in 0..remainder {
        let d = a[base + i] - b[base + i];
        sum += d * d;
    }

    sum
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
/// Accumulates squared Euclidean distance with x86-64 AVX2.
///
/// Unaligned eight-float loads are safe only because callers supply equal-length
/// slices and the chunk count is derived from that common length.
fn euclidean_squared_inner(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let chunks = a.len() / 8;
    let remainder = a.len() % 8;

    // SAFETY: the public wrappers assert `a.len() == b.len()`, and `chunks`
    // is derived from that common length, so every unaligned eight-float load
    // at `base` is in bounds for both slices.
    let mut sum = unsafe {
        let mut acc = _mm256_setzero_ps();
        for i in 0..chunks {
            let base = i * 8;
            let va = _mm256_loadu_ps(a.as_ptr().add(base));
            let vb = _mm256_loadu_ps(b.as_ptr().add(base));
            let diff = _mm256_sub_ps(va, vb);
            acc = _mm256_fmadd_ps(diff, diff, acc);
        }
        hsum_avx(acc)
    };

    // Handle the remainder with safe bounds-checked indexing.
    let base = chunks * 8;
    for i in 0..remainder {
        let d = a[base + i] - b[base + i];
        sum += d * d;
    }
    sum
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline]
/// Accumulates squared Euclidean distance with AArch64 NEON.
///
/// Four-float vector loads cover complete chunks and checked scalar indexing
/// handles the tail.
fn euclidean_squared_inner(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    let chunks = a.len() / 4;
    let remainder = a.len() % 4;

    // SAFETY: the public wrappers assert `a.len() == b.len()`, and `chunks`
    // is derived from that common length, so every four-float load at `base`
    // is in bounds for both slices.
    let mut sum = unsafe {
        let mut acc = vdupq_n_f32(0.0);
        for i in 0..chunks {
            let base = i * 4;
            let va = vld1q_f32(a.as_ptr().add(base));
            let vb = vld1q_f32(b.as_ptr().add(base));
            let diff = vsubq_f32(va, vb);
            acc = vfmaq_f32(acc, diff, diff);
        }
        vaddvq_f32(acc)
    };

    // Handle the remainder with safe bounds-checked indexing.
    let base = chunks * 4;
    for i in 0..remainder {
        let d = a[base + i] - b[base + i];
        sum += d * d;
    }
    sum
}

/// Accumulates a dot product with the scalar kernel.
///
/// # Parameters
///
/// - `a`: First vector.
/// - `b`: Equal-length second vector.
///
/// # Returns
///
/// Returns the non-negated inner product.
///
/// # Performance
///
/// Processes eight products per outer iteration to encourage auto-vectorization.
#[cfg(not(any(
    all(target_arch = "x86_64", target_feature = "avx2"),
    all(target_arch = "aarch64", target_feature = "neon"),
)))]
#[inline]
fn dot_product_inner(a: &[f32], b: &[f32]) -> f32 {
    let mut sum: f32 = 0.0;
    let chunks = a.len() / 8;
    let remainder = a.len() % 8;

    for i in 0..chunks {
        let base = i * 8;
        let mut tmp = [0.0f32; 8];
        for j in 0..8 {
            tmp[j] = a[base + j] * b[base + j];
        }
        for val in tmp {
            sum += val;
        }
    }

    let base = chunks * 8;
    for i in 0..remainder {
        sum += a[base + i] * b[base + i];
    }

    sum
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
/// Accumulates a dot product with x86-64 AVX2 fused multiply-add.
///
/// Unaligned loads cover groups of eight coordinates; the tail remains scalar.
fn dot_product_inner(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let chunks = a.len() / 8;
    let remainder = a.len() % 8;

    // SAFETY: the public wrappers assert `a.len() == b.len()`, and `chunks`
    // is derived from that common length, so every unaligned eight-float load
    // at `base` is in bounds for both slices.
    let mut sum = unsafe {
        let mut acc = _mm256_setzero_ps();
        for i in 0..chunks {
            let base = i * 8;
            let va = _mm256_loadu_ps(a.as_ptr().add(base));
            let vb = _mm256_loadu_ps(b.as_ptr().add(base));
            acc = _mm256_fmadd_ps(va, vb, acc);
        }
        hsum_avx(acc)
    };

    // Handle the remainder with safe bounds-checked indexing.
    let base = chunks * 8;
    for i in 0..remainder {
        sum += a[base + i] * b[base + i];
    }
    sum
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline]
/// Accumulates a dot product with AArch64 NEON fused multiply-add.
///
/// Vector loads cover groups of four coordinates; the tail remains scalar.
fn dot_product_inner(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    let chunks = a.len() / 4;
    let remainder = a.len() % 4;

    // SAFETY: the public wrappers assert `a.len() == b.len()`, and `chunks`
    // is derived from that common length, so every four-float load at `base`
    // is in bounds for both slices.
    let mut sum = unsafe {
        let mut acc = vdupq_n_f32(0.0);
        for i in 0..chunks {
            let base = i * 4;
            let va = vld1q_f32(a.as_ptr().add(base));
            let vb = vld1q_f32(b.as_ptr().add(base));
            acc = vfmaq_f32(acc, va, vb);
        }
        vaddvq_f32(acc)
    };

    // Handle the remainder with safe bounds-checked indexing.
    let base = chunks * 4;
    for i in 0..remainder {
        sum += a[base + i] * b[base + i];
    }
    sum
}

/// Reduces eight AVX float lanes to one scalar sum.
///
/// # Parameters
///
/// - `v`: Initialized AVX register whose lanes should be added.
///
/// # Returns
///
/// Returns the arithmetic sum of all eight lanes.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn hsum_avx(v: std::arch::x86_64::__m256) -> f32 {
    use std::arch::x86_64::*;
    // SAFETY: pure register arithmetic on an already-valid `__m256`; nothing
    // is dereferenced. AVX2 availability is guaranteed by the enclosing
    // compile-time target-feature gate.
    unsafe {
        // Add high 128 to low 128.
        let hi = _mm256_extractf128_ps(v, 1);
        let lo = _mm256_castps256_ps128(v);
        let sum128 = _mm_add_ps(lo, hi);
        // Horizontal add within 128-bit lane.
        let shuf = _mm_movehdup_ps(sum128);
        let sums = _mm_add_ps(sum128, shuf);
        let shuf2 = _mm_movehl_ps(sums, sums);
        let sums2 = _mm_add_ss(sums, shuf2);
        _mm_cvtss_f32(sums2)
    }
}

/// Computes a vector's Euclidean, or L2, norm.
///
/// # Parameters
///
/// - `v`: Borrowed vector coordinates.
///
/// # Returns
///
/// Returns `sqrt(sum(x²))`; the empty or all-zero vector returns zero.
///
/// # Performance
///
/// Performs one linear scalar pass and one square root without allocation.
///
/// # Examples
///
/// The norm of `[3, 4]` is `5`.
#[inline]
pub fn l2_norm(v: &[f32]) -> f32 {
    let mut sum: f32 = 0.0;
    for &x in v {
        sum += x * x;
    }
    sum.sqrt()
}

/// Normalizes a mutable vector in place to unit L2 length.
///
/// Zero and near-zero vectors remain unchanged to avoid division by a value
/// below [`f32::EPSILON`].
///
/// # Parameters
///
/// - `v`: Exclusively borrowed coordinate slice to update.
///
/// # Returns
///
/// Returns unit after scaling, or immediately for a near-zero norm.
///
/// # Side Effects
///
/// Mutates every coordinate in the caller's slice when normalization occurs.
///
/// # Performance
///
/// Uses one pass to compute the norm and a second pass to scale coordinates.
///
/// # Examples
///
/// `[3, 4]` becomes approximately `[0.6, 0.8]`; `[0, 0]` stays unchanged.
///
/// # Rust Notes for Java/C Engineers
///
/// `&mut [f32]` is an exclusive borrow. While this function runs, Rust prevents
/// any other ordinary reference from reading or writing the same slice. Java
/// allows shared array aliases; C relies on caller discipline or `restrict`.
#[inline]
pub fn normalize(v: &mut [f32]) {
    let norm = l2_norm(v);
    if norm > f32::EPSILON {
        let inv = 1.0 / norm;
        for x in v.iter_mut() {
            *x *= inv;
        }
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    //! Regression tests for metric definitions, ordering, normalization, and
    //! scalar/SIMD remainder handling.

    use super::*;

    /// Verifies identical directions have zero cosine distance.
    #[test]
    fn test_cosine_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let d = cosine_distance(&a, &a);
        assert!(
            (d - 0.0).abs() < 1e-6,
            "identical vectors should have distance ~0, got {d}"
        );
    }

    /// Verifies opposite directions reach the maximum cosine distance.
    #[test]
    fn test_cosine_opposite() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![-1.0, 0.0, 0.0];
        let d = cosine_distance(&a, &b);
        assert!(
            (d - 2.0).abs() < 1e-6,
            "opposite vectors should have distance ~2, got {d}"
        );
    }

    /// Verifies a vector has zero squared Euclidean distance from itself.
    #[test]
    fn test_euclidean_zero() {
        let a = vec![3.0, 4.0];
        let d = euclidean_distance(&a, &a);
        assert!((d - 0.0).abs() < 1e-6);
    }

    /// Verifies the Euclidean helper deliberately returns the squared value.
    #[test]
    fn test_euclidean_known() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        // Squared distance = 9 + 16 = 25
        let d = euclidean_distance(&a, &b);
        assert!((d - 25.0).abs() < 1e-6);
    }

    /// Verifies orthogonal vectors have zero dot-product distance.
    #[test]
    fn test_dot_product_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let d = dot_product_distance(&a, &b);
        assert!((d - 0.0).abs() < 1e-6, "orthogonal => dot=0 => distance=0");
    }

    /// Verifies dot-product similarity is negated for ascending ranking.
    #[test]
    fn test_dot_product_parallel() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![1.0, 2.0, 3.0];
        // dot = 1+4+9 = 14, distance = -14
        let d = dot_product_distance(&a, &b);
        assert!((d - (-14.0)).abs() < 1e-6);
    }

    /// Verifies metric dispatch preserves each public helper's result.
    #[test]
    fn test_compute_distance_dispatch() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        // Cosine: 1 - 0/1 = 1.0
        let dc = compute_distance(&a, &b, DistanceMetric::Cosine);
        assert!((dc - 1.0).abs() < 1e-6);
        // Euclidean: (1-0)^2 + (0-1)^2 = 2.0
        let de = compute_distance(&a, &b, DistanceMetric::Euclidean);
        assert!((de - 2.0).abs() < 1e-6);
    }

    /// Verifies in-place normalization produces unit L2 length.
    #[test]
    fn test_normalize() {
        let mut v = vec![3.0, 4.0];
        normalize(&mut v);
        let norm = l2_norm(&v);
        assert!((norm - 1.0).abs() < 1e-6);
    }

    /// Exercises full SIMD-sized chunks plus a scalar remainder.
    #[test]
    fn test_large_dimension() {
        // Test with a dimension that exercises the chunked loop + remainder.
        let dim = 131; // not divisible by 8
        let a: Vec<f32> = (0..dim).map(|i| i as f32 * 0.1).collect();
        let b: Vec<f32> = (0..dim).map(|i| (dim - i) as f32 * 0.1).collect();

        let d_cos = cosine_distance(&a, &b);
        assert!(d_cos.is_finite());
        let d_euc = euclidean_distance(&a, &b);
        assert!(d_euc >= 0.0);
        let d_dot = dot_product_distance(&a, &b);
        assert!(d_dot.is_finite());
    }
}
