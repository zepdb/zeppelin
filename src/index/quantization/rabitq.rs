use thiserror::Error;

/// The power-of-two block used by the no-padding structured rotation.
pub const BLOCK_DIM: usize = 256;

/// Three rounds provide cross-block mixing while keeping the transform cheap.
pub const ROTATION_ROUNDS: usize = 3;

#[derive(Debug, Clone, Error)]
pub enum RabitqError {
    #[error("RaBitQ dimension {dim} must be a non-zero multiple of {BLOCK_DIM}")]
    InvalidDimension { dim: usize },
    #[error("{name} length mismatch: expected {expected}, got {actual}")]
    LengthMismatch {
        name: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("{name} contains a non-finite value at coordinate {index}")]
    NonFiniteValue { name: &'static str, index: usize },
    #[error("query residual norm squared must be finite and non-negative, got {value}")]
    InvalidQueryNorm { value: f32 },
    #[error(
        "structured rotation MSE delta {mse_delta} exceeds five standard errors ({standard_error}); structured RMSE {structured_rmse}, dense RMSE {dense_rmse}"
    )]
    StructuredDenseQualityMismatch {
        mse_delta: f64,
        standard_error: f64,
        structured_rmse: f64,
        dense_rmse: f64,
    },
    #[error(
        "structured rotation RMSE {structured_rmse} is not at most {maximum_ratio} of identity RMSE {identity_rmse}"
    )]
    StructuredIdentityQualityMismatch {
        structured_rmse: f64,
        identity_rmse: f64,
        maximum_ratio: f64,
    },
}

#[derive(Debug, Clone)]
struct RotationRound {
    sign_words: Vec<u64>,
    /// Scatter convention: input coordinate `i` is written to `permutation[i]`.
    permutation: Vec<usize>,
}

/// An orthogonal, no-padding rotation for dimensions divisible by 256.
///
/// Each round applies an independently seeded Rademacher diagonal, a
/// normalized FHT to every 256-coordinate block, and a full-coordinate
/// permutation. All randomness is expanded deterministically from `seed`.
#[derive(Debug, Clone)]
pub struct StructuredRotation {
    dim: usize,
    rounds: Vec<RotationRound>,
}

impl StructuredRotation {
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

    #[must_use]
    pub const fn dim(&self) -> usize {
        self.dim
    }

    /// Rotate `values` in place using caller-owned scratch storage.
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
pub const QUERY_BITS: usize = 4;
const QUERY_LEVELS: u8 = (1 << QUERY_BITS) - 1;

/// A one-bit RaBitQ code over a rotated cluster residual.
#[derive(Debug, Clone)]
pub struct OneBitCode {
    pub words: Vec<u64>,
    pub residual_norm: f32,
    /// `<h / sqrt(D), residual>` from the RaBitQ estimator.
    pub bar_dot_residual: f32,
    dim: usize,
}

impl OneBitCode {
    #[must_use]
    pub const fn dim(&self) -> usize {
        self.dim
    }
}

/// Allocation-free scalar output from one-bit encoding.
#[derive(Debug, Clone, Copy)]
pub struct OneBitFactors {
    pub residual_norm: f32,
    pub bar_dot_residual: f32,
}

/// A two-bit Extended-RaBitQ code. Plane 0 is the low bit and plane 1
/// is the sign/high bit of the unsigned grid code in `[0, 3]`.
#[derive(Debug, Clone)]
pub struct TwoBitCode {
    pub planes: [Vec<u64>; 2],
    pub residual_norm: f32,
    /// `<y / ||y||, residual>` for the selected half-integer grid vector.
    pub bar_dot_residual: f32,
    dim: usize,
}

impl TwoBitCode {
    #[must_use]
    pub const fn dim(&self) -> usize {
        self.dim
    }
}

/// Allocation-free scalar output from exact two-bit encoding.
#[derive(Debug, Clone, Copy)]
pub struct TwoBitFactors {
    pub residual_norm: f32,
    pub bar_dot_residual: f32,
}

/// Four bit-plane query representation used by popcount ADC.
#[derive(Debug, Clone)]
pub struct QueryAdc4 {
    planes: [Vec<u64>; QUERY_BITS],
    lower: f32,
    step: f32,
    code_sum: u32,
    dim: usize,
}

impl QueryAdc4 {
    #[must_use]
    pub const fn dim(&self) -> usize {
        self.dim
    }

    /// Return `sum_i h_i * q_hat_i`, where `h_i` is the stored sign.
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
pub fn words_per_code(dim: usize) -> Result<usize, RabitqError> {
    validate_quantized_dim(dim)?;
    Ok(dim / 64)
}

/// Encode into caller-owned flat storage, avoiding one allocation per row.
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
#[must_use]
pub fn estimate_cosine_from_unit_l2(estimated_l2: f32) -> f32 {
    1.0 - 0.5 * estimated_l2
}

#[allow(clippy::manual_is_multiple_of)] // `usize::is_multiple_of` is newer than the MSRV.
fn validate_quantized_dim(dim: usize) -> Result<(), RabitqError> {
    if dim == 0 || dim % BLOCK_DIM != 0 {
        Err(RabitqError::InvalidDimension { dim })
    } else {
        Ok(())
    }
}

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

fn validate_query_norm(value: f32) -> Result<(), RabitqError> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(RabitqError::InvalidQueryNorm { value })
    }
}

#[cfg(test)]
fn bit_at(words: &[u64], index: usize) -> bool {
    ((words[index / 64] >> (index % 64)) & 1) != 0
}

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

#[derive(Debug, Clone, Copy)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

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
#[derive(Debug, Clone, Copy)]
pub struct RotationQuality {
    pub pairs: usize,
    pub structured_rmse: f64,
    pub dense_rmse: f64,
    pub identity_rmse: f64,
    /// Paired `structured MSE - dense MSE` mean.
    pub mse_delta: f64,
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

const ANISOTROPIC_ACTIVE_OFFSET: usize = 13;
const ANISOTROPIC_ACTIVE_DIM: usize = 24;

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

fn offline_dot(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| f64::from(*left) * f64::from(*right))
        .sum()
}

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
    dim: usize,
    rows: Vec<f64>,
}

impl DenseRotation {
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

#[derive(Debug, Clone, Copy)]
struct StandardNormal {
    rng: SplitMix64,
    spare: Option<f64>,
}

impl StandardNormal {
    const fn new(seed: u64) -> Self {
        Self {
            rng: SplitMix64::new(seed),
            spare: None,
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

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

    #[test]
    fn two_bit_estimator_is_unbiased_after_structured_rotation_over_ten_thousand_pairs() {
        let (mean_error, standard_error) = structured_two_bit_anisotropic_error_stats(10_000);
        assert!(
            mean_error.abs() <= 5.0 * standard_error,
            "mean error {mean_error} exceeds five standard errors {standard_error}"
        );
    }

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

    #[test]
    fn exact_two_bit_search_improves_estimator_rmse() {
        let (one_bit_rmse, two_bit_rmse) = estimator_rmse(768, 3_000, 0x2b17);
        assert!(
            two_bit_rmse < one_bit_rmse * 0.7,
            "2-bit RMSE {two_bit_rmse} did not improve enough over 1-bit {one_bit_rmse}"
        );
    }

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

    fn normalize(values: &mut [f32]) {
        let norm = dot(values, values).sqrt();
        for value in values {
            *value /= norm;
        }
    }

    fn remove_projection_and_normalize(axis: &[f32], values: &mut [f32]) {
        let projection = dot(axis, values);
        for (value, axis_value) in values.iter_mut().zip(axis) {
            *value -= projection * axis_value;
        }
        normalize(values);
    }

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

    fn dot(left: &[f32], right: &[f32]) -> f32 {
        left.iter().zip(right).map(|(l, r)| l * r).sum()
    }

    fn assert_close(actual: f32, expected: f32, tolerance: f32) {
        assert!(
            (actual - expected).abs() <= tolerance,
            "actual={actual}, expected={expected}, tolerance={tolerance}"
        );
    }

    fn two_bit_value(planes: &[Vec<u64>; 2], index: usize) -> u8 {
        u8::from(bit_at(&planes[0], index)) | (u8::from(bit_at(&planes[1], index)) << 1)
    }
}
