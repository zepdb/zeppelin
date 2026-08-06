//! Closed-form lognormal latency distribution shared by the sizing model
//! and the perf-contract latency injector.
//!
//! TTFB samples use a closed-form lognormal fit:
//! `mu = ln(p50)` and `sigma = (ln(p99) - ln(p50)) / 2.326`.
//! Sampling is fully deterministic in the provided seed so injected latency
//! is reproducible across runs and machines.

/// The standard-normal z-score of the 99th percentile used by the fit.
const NORMAL_P99_Z: f64 = 2.326;

/// Lognormal milliseconds fitted to configured p50 and p99 values.
#[derive(Debug, Clone, Copy)]
pub struct LognormalMs {
    mu: f64,
    sigma: f64,
}

impl LognormalMs {
    /// Fits the distribution so its p50 and p99 match the given millisecond
    /// values exactly.
    ///
    /// # Panics
    ///
    /// Panics when `p50_ms` is not finite and positive, or when `p99_ms` is
    /// not finite or is below `p50_ms`.
    #[must_use]
    pub fn from_percentiles(p50_ms: f64, p99_ms: f64) -> Self {
        assert!(
            p50_ms.is_finite() && p50_ms > 0.0,
            "latency p50 must be finite and positive"
        );
        assert!(
            p99_ms.is_finite() && p99_ms >= p50_ms,
            "latency p99 must be finite and at least p50"
        );
        Self {
            mu: p50_ms.ln(),
            sigma: (p99_ms.ln() - p50_ms.ln()) / NORMAL_P99_Z,
        }
    }

    /// Draws one deterministic sample in microseconds for the given seed.
    ///
    /// # Panics
    ///
    /// Panics when the sampled value falls outside the `u64` microsecond
    /// range, which indicates a degenerate fit rather than valid input.
    #[must_use]
    pub fn sample_us(self, seed: u64) -> u64 {
        let first = splitmix64(seed);
        let second = splitmix64(first);
        let u1 = unit_open(first);
        let u2 = unit_open(second);
        let standard_normal = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
        let micros = (self.mu + self.sigma * standard_normal).exp() * 1_000.0;
        assert!(
            micros.is_finite() && micros >= 0.0 && micros <= u64::MAX as f64,
            "sampled latency is outside the u64 microsecond range"
        );
        micros.round() as u64
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

fn unit_open(value: u64) -> f64 {
    ((value >> 11) as f64 + 0.5) / ((1_u64 << 53) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampling_is_deterministic_in_the_seed() {
        let fitted = LognormalMs::from_percentiles(15.0, 60.0);
        assert_eq!(fitted.sample_us(42), fitted.sample_us(42));
        assert_ne!(fitted.sample_us(42), fitted.sample_us(43));
    }

    #[test]
    fn percentile_fit_is_exact_at_distribution_quantiles() {
        let fitted = LognormalMs::from_percentiles(15.0, 60.0);
        assert!((fitted.mu.exp() - 15.0).abs() < 1e-12);
        assert!(((fitted.mu + NORMAL_P99_Z * fitted.sigma).exp() - 60.0).abs() < 1e-9);
    }
}
