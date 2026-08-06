//! Closed-form stored-bytes-per-row for each quantization variant.
//!
//! These formulas mirror the production encoders and are pinned against
//! them by tests (the RaBitQ forms are asserted equal to the private
//! `rabitq_row_bytes` in `crate::index::ivf_flat::sketch`). RaBitQ rows pad
//! dimensions to complete 256-d rotation blocks and append two f32
//! correction scalars; SQ8 stores one byte per dimension; PQ stores one
//! code byte per subquantizer.

/// Structured-rotation block width used by the RaBitQ encoders.
///
/// Mirrors `BLOCK_DIM` in `crate::index::quantization::rabitq`.
const BLOCK_DIM: usize = 256;

/// A stored-row quantization variant priced by the sizing model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quantization {
    /// Full-precision f32 rows.
    F32,
    /// Scalar 8-bit quantization: one byte per dimension.
    Sq8,
    /// Two-bit RaBitQ: two bit planes plus two f32 scalars. The shipped
    /// default (>= 99.3% recall retention at nprobe 32 on e5-768).
    RabitqTwoBit,
    /// One-bit RaBitQ: one bit plane plus two f32 scalars. Retains only
    /// ~95% recall on e5-768; kept for modeling, not recommended.
    RabitqOneBit,
    /// Product quantization with `m` subquantizers: `m` code bytes.
    Pq {
        /// Subquantizer count; must divide the dimension.
        m: usize,
    },
}

impl Quantization {
    /// Stable lowercase label matching profile `row_bytes` keys.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::F32 => "f32",
            Self::Sq8 => "sq8",
            Self::RabitqTwoBit => "rabitq-2bit",
            Self::RabitqOneBit => "rabitq-1bit",
            Self::Pq { .. } => "current-v3-pq",
        }
    }
}

/// Stored bytes per logical row for `quantization` at `dims` dimensions.
///
/// # Panics
///
/// Panics when `dims` is zero, or for [`Quantization::Pq`] when `m` is zero
/// or does not divide `dims` — both are invalid encoder configurations.
#[must_use]
pub fn row_bytes(quantization: Quantization, dims: usize) -> usize {
    assert!(dims > 0, "row-bytes dims must be nonzero");
    match quantization {
        Quantization::F32 => dims * 4,
        Quantization::Sq8 => dims,
        Quantization::RabitqTwoBit => padded_dims(dims) / 4 + 2 * std::mem::size_of::<f32>(),
        Quantization::RabitqOneBit => padded_dims(dims) / 8 + 2 * std::mem::size_of::<f32>(),
        Quantization::Pq { m } => {
            assert!(m > 0, "PQ subquantizer count must be nonzero");
            assert!(dims % m == 0, "PQ subquantizer count must divide dims");
            m
        }
    }
}

/// Rounds a logical dimension up to complete 256-d rotation blocks.
fn padded_dims(dims: usize) -> usize {
    dims.div_ceil(BLOCK_DIM) * BLOCK_DIM
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_row_bytes_match_the_measured_profile_table() {
        // Values pinned by tests/perf_contract/profiles/s3-3node-wikidpr.toml
        // and the quant_bakeoff report at dims = 768.
        assert_eq!(row_bytes(Quantization::F32, 768), 3072);
        assert_eq!(row_bytes(Quantization::Sq8, 768), 768);
        assert_eq!(row_bytes(Quantization::RabitqTwoBit, 768), 200);
        assert_eq!(row_bytes(Quantization::RabitqOneBit, 768), 104);
        assert_eq!(row_bytes(Quantization::Pq { m: 8 }, 768), 8);
    }

    #[test]
    fn rabitq_rows_pad_to_full_rotation_blocks() {
        // 100 dims pad to 256: 256/4 + 8 = 72 and 256/8 + 8 = 40.
        assert_eq!(row_bytes(Quantization::RabitqTwoBit, 100), 72);
        assert_eq!(row_bytes(Quantization::RabitqOneBit, 100), 40);
        // 1536 is already block-aligned: 1536/4 + 8 = 392.
        assert_eq!(row_bytes(Quantization::RabitqTwoBit, 1536), 392);
    }

    #[test]
    fn labels_are_stable() {
        assert_eq!(Quantization::RabitqTwoBit.label(), "rabitq-2bit");
        assert_eq!(Quantization::Sq8.label(), "sq8");
    }
}
