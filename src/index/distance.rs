//! Distance functions for vector comparison.
//!
//! Provides cosine, euclidean, and dot-product distance metrics with scalar
//! implementations and architecture-specific SIMD hints for auto-vectorization.

use crate::types::DistanceMetric;

/// Dispatch to the appropriate distance function based on the metric.
///
/// All distance functions return a *distance* (lower is closer) so that
/// callers can sort ascending and take the first `k` results.
#[inline]
pub fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::Euclidean => euclidean_distance(a, b),
        DistanceMetric::DotProduct => dot_product_distance(a, b),
    }
}

/// Cosine distance: `1.0 - cosine_similarity(a, b)`.
///
/// Returns 0.0 for identical directions and 2.0 for opposite directions.
/// If either vector has zero magnitude, returns 1.0 (orthogonal).
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dimensions must match");

    let (dot, norm_a, norm_b) = cosine_components(a, b);

    let denom = (norm_a * norm_b).sqrt();
    if denom < f32::EPSILON {
        return 1.0;
    }

    let similarity = dot / denom;
    // Clamp to [-1, 1] to handle floating-point drift.
    1.0 - similarity.clamp(-1.0, 1.0)
}

/// Squared Euclidean distance: `sum((a_i - b_i)^2)`.
///
/// We return squared distance to avoid the sqrt cost. This preserves
/// ordering for top-k selection.
#[inline]
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dimensions must match");
    euclidean_squared_inner(a, b)
}

/// Dot-product distance: `-dot(a, b)`.
///
/// Negated so that higher similarity (larger dot product) yields a
/// smaller distance value, keeping the "lower is closer" invariant.
#[inline]
pub fn dot_product_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dimensions must match");
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

/// Compute (dot, ||a||^2, ||b||^2) in a single pass.
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

/// AVX2 cosine components for x86_64.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn cosine_components(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    unsafe {
        let mut dot_acc = _mm256_setzero_ps();
        let mut norm_a_acc = _mm256_setzero_ps();
        let mut norm_b_acc = _mm256_setzero_ps();

        let chunks = a.len() / 8;
        let remainder = a.len() % 8;

        for i in 0..chunks {
            let base = i * 8;
            let va = _mm256_loadu_ps(a.as_ptr().add(base));
            let vb = _mm256_loadu_ps(b.as_ptr().add(base));
            dot_acc = _mm256_fmadd_ps(va, vb, dot_acc);
            norm_a_acc = _mm256_fmadd_ps(va, va, norm_a_acc);
            norm_b_acc = _mm256_fmadd_ps(vb, vb, norm_b_acc);
        }

        // Horizontal sum for each accumulator.
        let dot = hsum_avx(dot_acc);
        let mut norm_a = hsum_avx(norm_a_acc);
        let mut norm_b = hsum_avx(norm_b_acc);
        let mut dot_sum = dot;

        // Handle remainder.
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
}

/// NEON cosine components for aarch64.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline]
fn cosine_components(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    use std::arch::aarch64::*;

    unsafe {
        let mut dot_acc = vdupq_n_f32(0.0);
        let mut norm_a_acc = vdupq_n_f32(0.0);
        let mut norm_b_acc = vdupq_n_f32(0.0);

        let chunks = a.len() / 4;
        let remainder = a.len() % 4;

        for i in 0..chunks {
            let base = i * 4;
            let va = vld1q_f32(a.as_ptr().add(base));
            let vb = vld1q_f32(b.as_ptr().add(base));
            dot_acc = vfmaq_f32(dot_acc, va, vb);
            norm_a_acc = vfmaq_f32(norm_a_acc, va, va);
            norm_b_acc = vfmaq_f32(norm_b_acc, vb, vb);
        }

        let dot = vaddvq_f32(dot_acc);
        let mut norm_a = vaddvq_f32(norm_a_acc);
        let mut norm_b = vaddvq_f32(norm_b_acc);
        let mut dot_sum = dot;

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
}

/// Squared Euclidean distance scalar kernel.
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
fn euclidean_squared_inner(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    unsafe {
        let mut acc = _mm256_setzero_ps();
        let chunks = a.len() / 8;
        let remainder = a.len() % 8;

        for i in 0..chunks {
            let base = i * 8;
            let va = _mm256_loadu_ps(a.as_ptr().add(base));
            let vb = _mm256_loadu_ps(b.as_ptr().add(base));
            let diff = _mm256_sub_ps(va, vb);
            acc = _mm256_fmadd_ps(diff, diff, acc);
        }

        let mut sum = hsum_avx(acc);
        let base = chunks * 8;
        for i in 0..remainder {
            let d = a[base + i] - b[base + i];
            sum += d * d;
        }
        sum
    }
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline]
fn euclidean_squared_inner(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    unsafe {
        let mut acc = vdupq_n_f32(0.0);
        let chunks = a.len() / 4;
        let remainder = a.len() % 4;

        for i in 0..chunks {
            let base = i * 4;
            let va = vld1q_f32(a.as_ptr().add(base));
            let vb = vld1q_f32(b.as_ptr().add(base));
            let diff = vsubq_f32(va, vb);
            acc = vfmaq_f32(acc, diff, diff);
        }

        let mut sum = vaddvq_f32(acc);
        let base = chunks * 4;
        for i in 0..remainder {
            let d = a[base + i] - b[base + i];
            sum += d * d;
        }
        sum
    }
}

/// Dot-product scalar kernel.
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
fn dot_product_inner(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    unsafe {
        let mut acc = _mm256_setzero_ps();
        let chunks = a.len() / 8;
        let remainder = a.len() % 8;

        for i in 0..chunks {
            let base = i * 8;
            let va = _mm256_loadu_ps(a.as_ptr().add(base));
            let vb = _mm256_loadu_ps(b.as_ptr().add(base));
            acc = _mm256_fmadd_ps(va, vb, acc);
        }

        let mut sum = hsum_avx(acc);
        let base = chunks * 8;
        for i in 0..remainder {
            sum += a[base + i] * b[base + i];
        }
        sum
    }
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[inline]
fn dot_product_inner(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    unsafe {
        let mut acc = vdupq_n_f32(0.0);
        let chunks = a.len() / 4;
        let remainder = a.len() % 4;

        for i in 0..chunks {
            let base = i * 4;
            let va = vld1q_f32(a.as_ptr().add(base));
            let vb = vld1q_f32(b.as_ptr().add(base));
            acc = vfmaq_f32(acc, va, vb);
        }

        let mut sum = vaddvq_f32(acc);
        let base = chunks * 4;
        for i in 0..remainder {
            sum += a[base + i] * b[base + i];
        }
        sum
    }
}

/// Horizontal sum of an AVX 256-bit float register.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
unsafe fn hsum_avx(v: std::arch::x86_64::__m256) -> f32 {
    use std::arch::x86_64::*;
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

/// Compute the L2 norm of a vector.
#[inline]
pub fn l2_norm(v: &[f32]) -> f32 {
    let mut sum: f32 = 0.0;
    for &x in v {
        sum += x * x;
    }
    sum.sqrt()
}

/// Normalize a vector in-place to unit length.
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
    use super::*;

    #[test]
    fn test_cosine_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let d = cosine_distance(&a, &a);
        assert!(
            (d - 0.0).abs() < 1e-6,
            "identical vectors should have distance ~0, got {d}"
        );
    }

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

    #[test]
    fn test_euclidean_zero() {
        let a = vec![3.0, 4.0];
        let d = euclidean_distance(&a, &a);
        assert!((d - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_known() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        // Squared distance = 9 + 16 = 25
        let d = euclidean_distance(&a, &b);
        assert!((d - 25.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let d = dot_product_distance(&a, &b);
        assert!((d - 0.0).abs() < 1e-6, "orthogonal => dot=0 => distance=0");
    }

    #[test]
    fn test_dot_product_parallel() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![1.0, 2.0, 3.0];
        // dot = 1+4+9 = 14, distance = -14
        let d = dot_product_distance(&a, &b);
        assert!((d - (-14.0)).abs() < 1e-6);
    }

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

    #[test]
    fn test_normalize() {
        let mut v = vec![3.0, 4.0];
        normalize(&mut v);
        let norm = l2_norm(&v);
        assert!((norm - 1.0).abs() < 1e-6);
    }

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
