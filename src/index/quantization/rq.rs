//! Owns cluster-aligned two-bit RaBitQ rows.
//!
//! This module is the in-memory payload layer between the existing RaBitQ
//! encoder and a future cluster-object section. It performs no object-store I/O
//! and does not make an artifact visible.

use bytes::Bytes;
use thiserror::Error;

use super::rabitq::{self, QueryAdc4, RabitqError, StructuredRotation, TwoBitFactors, BLOCK_DIM};
use crate::types::DistanceMetric;

const RQ_MAGIC: &[u8; 4] = b"ZRQ1";
const RQ_VERSION: u8 = 1;
const RQ_HEADER_LEN: usize = RQ_MAGIC.len() + 1 + 2 * std::mem::size_of::<u64>();
const FACTOR_BYTES: usize = 2 * std::mem::size_of::<f32>();

/// Reports an invalid input or corrupt two-bit cluster payload.
#[derive(Debug, Error)]
pub enum RqError {
    /// A vector, centroid, rotation, or query used a different dimension.
    #[error("{name} dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch {
        /// Input whose dimension was wrong.
        name: &'static str,
        /// Dimension required by the cluster payload.
        expected: usize,
        /// Dimension supplied by the caller.
        actual: usize,
    },
    /// The number of IDs did not match the number of cluster rows.
    #[error("RQ row/ID count mismatch: {rows} rows, {ids} IDs")]
    RowIdCountMismatch {
        /// Number of encoded or declared rows.
        rows: usize,
        /// Number of IDs supplied or completely decoded.
        ids: usize,
    },
    /// A row did not contain both complete bit planes.
    #[error(
        "RQ plane buffer truncated at row {row}: expected {expected_bytes} bytes, got {actual_bytes}"
    )]
    TruncatedPlaneBuffer {
        /// Zero-based row being decoded.
        row: usize,
        /// Bytes required for both bit planes.
        expected_bytes: usize,
        /// Bytes available at the plane offset.
        actual_bytes: usize,
    },
    /// The payload signature was not the RQ signature.
    #[error("bad RQ magic: {actual:?}")]
    BadMagic {
        /// Four signature bytes found in the payload.
        actual: [u8; 4],
    },
    /// The payload version is not supported by this reader.
    #[error("bad RQ version: expected {expected}, got {actual}")]
    BadVersion {
        /// Version this reader accepts.
        expected: u8,
        /// Version found in the payload.
        actual: u8,
    },
    /// The number of complete factor pairs did not match the row count.
    #[error("RQ factor count mismatch: {rows} rows, {factors} factor pairs")]
    FactorCountMismatch {
        /// Number of declared rows.
        rows: usize,
        /// Number of complete factor pairs decoded.
        factors: usize,
    },
    /// The fixed payload header was incomplete.
    #[error("RQ header truncated: expected {expected_bytes} bytes, got {actual_bytes}")]
    TruncatedHeader {
        /// Fixed header size.
        expected_bytes: usize,
        /// Bytes supplied by the caller.
        actual_bytes: usize,
    },
    /// A persisted row ID was not valid UTF-8.
    #[error("RQ row {row} ID is not valid UTF-8")]
    InvalidRowId {
        /// Zero-based row containing the invalid ID.
        row: usize,
    },
    /// A persisted size could not be represented on this platform.
    #[error("RQ {field} value {value} does not fit in memory")]
    SizeOverflow {
        /// Header field that overflowed.
        field: &'static str,
        /// Persisted unsigned value.
        value: u64,
    },
    /// Bytes remained after all declared rows were decoded.
    #[error("RQ payload has {bytes} trailing bytes")]
    TrailingBytes {
        /// Unconsumed byte count.
        bytes: usize,
    },
    /// A scorer requested a row outside the cluster.
    #[error("RQ row index {row} is out of bounds for {rows} rows")]
    RowIndexOutOfBounds {
        /// Requested zero-based row.
        row: usize,
        /// Number of rows in the cluster.
        rows: usize,
    },
    /// The underlying rotation, encoder, or estimator rejected its inputs.
    #[error(transparent)]
    Rabitq(#[from] RabitqError),
}

/// Owns IDs, packed two-bit planes, and factors for one logical cluster.
///
/// The three buffers are private so row alignment cannot be changed
/// independently after construction. Each row occupies its low plane followed
/// by its high plane in `planes`; `factors[row]` and `ids[row]` describe that
/// same logical row.
#[must_use]
#[derive(Debug, Clone)]
pub struct RqClusterCodes {
    dim: usize,
    ids: Vec<String>,
    planes: Vec<u64>,
    factors: Vec<TwoBitFactors>,
}

impl RqClusterCodes {
    /// Encodes cluster rows against their authoritative centroid.
    ///
    /// `ids` and `rows` must have the same order and count. The supplied
    /// rotation is the global, manifest-versioned rotation for this dimension.
    /// Plane storage and all scratch buffers are allocated once and reused for
    /// every row.
    pub fn encode(
        ids: &[String],
        rows: &[&[f32]],
        centroid: &[f32],
        rotation: &StructuredRotation,
    ) -> Result<Self, RqError> {
        if ids.len() != rows.len() {
            return Err(RqError::RowIdCountMismatch {
                rows: rows.len(),
                ids: ids.len(),
            });
        }

        let dim = rotation.dim();
        check_dimension("centroid", centroid.len(), dim)?;
        for row in rows {
            check_dimension("row", row.len(), dim)?;
        }

        let words_per_plane = dim / 64;
        let words_per_row =
            words_per_plane
                .checked_mul(2)
                .ok_or(RqError::TruncatedPlaneBuffer {
                    row: 0,
                    expected_bytes: usize::MAX,
                    actual_bytes: 0,
                })?;
        let total_words =
            rows.len()
                .checked_mul(words_per_row)
                .ok_or(RqError::TruncatedPlaneBuffer {
                    row: rows.len(),
                    expected_bytes: usize::MAX,
                    actual_bytes: 0,
                })?;
        let mut planes = vec![0_u64; total_words];
        let mut factors = Vec::with_capacity(rows.len());
        let mut rotated = vec![0.0_f32; dim];
        let mut rotation_scratch = vec![0.0_f32; dim];
        let mut order_scratch = vec![0_usize; dim];

        for (row_index, row) in rows.iter().enumerate() {
            rotation.rotate_residual(row, centroid, &mut rotated, &mut rotation_scratch)?;
            let row_start = row_index * words_per_row;
            let row_end = row_start + words_per_row;
            let (low_plane, high_plane) = planes[row_start..row_end].split_at_mut(words_per_plane);
            factors.push(rabitq::encode_two_bit_into(
                &rotated,
                low_plane,
                high_plane,
                &mut order_scratch,
            )?);
        }

        Ok(Self {
            dim,
            ids: ids.to_vec(),
            planes,
            factors,
        })
    }

    /// Returns the vector dimension represented by every row.
    #[must_use]
    pub const fn dim(&self) -> usize {
        self.dim
    }

    /// Returns the number of positionally aligned rows.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.ids.len()
    }

    /// Returns all row IDs in cluster order.
    #[must_use]
    pub fn ids(&self) -> &[String] {
        &self.ids
    }

    /// Returns the ID aligned with `row`, if it exists.
    #[must_use]
    pub fn id(&self, row: usize) -> Option<&str> {
        self.ids.get(row).map(String::as_str)
    }

    /// Returns the flat low-plane/high-plane words for all rows.
    #[must_use]
    pub fn packed_planes(&self) -> &[u64] {
        &self.planes
    }

    /// Returns the factors aligned with the cluster rows.
    #[must_use]
    pub fn factors(&self) -> &[TwoBitFactors] {
        &self.factors
    }

    /// Serializes this container with its magic and version byte.
    ///
    /// Rows are written in cluster order as a length-prefixed UTF-8 ID, two
    /// fixed-width little-endian bit planes, and two little-endian factor
    /// scalars.
    #[must_use]
    pub fn to_bytes(&self) -> Bytes {
        let mut data = Vec::new();
        data.extend_from_slice(RQ_MAGIC);
        data.push(RQ_VERSION);
        data.extend_from_slice(&(self.dim as u64).to_le_bytes());
        data.extend_from_slice(&(self.ids.len() as u64).to_le_bytes());

        let words_per_row = 2 * (self.dim / 64);
        for row in 0..self.ids.len() {
            let id = self.ids[row].as_bytes();
            data.extend_from_slice(&(id.len() as u64).to_le_bytes());
            data.extend_from_slice(id);
            let plane_start = row * words_per_row;
            for word in &self.planes[plane_start..plane_start + words_per_row] {
                data.extend_from_slice(&word.to_le_bytes());
            }
            data.extend_from_slice(&self.factors[row].residual_norm.to_le_bytes());
            data.extend_from_slice(&self.factors[row].bar_dot_residual.to_le_bytes());
        }

        Bytes::from(data)
    }

    /// Decodes and validates one complete RQ cluster container.
    ///
    /// Malformed signatures, dimensions, IDs, planes, factors, and trailing
    /// bytes are rejected rather than skipped.
    pub fn from_bytes(data: &[u8]) -> Result<Self, RqError> {
        if data.len() < RQ_HEADER_LEN {
            return Err(RqError::TruncatedHeader {
                expected_bytes: RQ_HEADER_LEN,
                actual_bytes: data.len(),
            });
        }

        let mut actual_magic = [0_u8; 4];
        actual_magic.copy_from_slice(&data[..4]);
        if &actual_magic != RQ_MAGIC {
            return Err(RqError::BadMagic {
                actual: actual_magic,
            });
        }
        if data[4] != RQ_VERSION {
            return Err(RqError::BadVersion {
                expected: RQ_VERSION,
                actual: data[4],
            });
        }

        let dim = read_header_usize(data, 5, "dimension")?;
        if dim == 0 || dim % BLOCK_DIM != 0 {
            return Err(RabitqError::InvalidDimension { dim }.into());
        }
        let row_count = read_header_usize(data, 13, "row count")?;
        let words_per_plane = dim / 64;
        let words_per_row =
            words_per_plane
                .checked_mul(2)
                .ok_or(RqError::TruncatedPlaneBuffer {
                    row: 0,
                    expected_bytes: usize::MAX,
                    actual_bytes: 0,
                })?;
        let plane_bytes = words_per_row
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or(RqError::TruncatedPlaneBuffer {
                row: 0,
                expected_bytes: usize::MAX,
                actual_bytes: 0,
            })?;

        let mut ids = Vec::new();
        let mut planes = Vec::new();
        let mut factors = Vec::new();
        let mut offset = RQ_HEADER_LEN;
        for row in 0..row_count {
            let Some(id_len_end) = offset.checked_add(8).filter(|&end| end <= data.len()) else {
                return Err(RqError::RowIdCountMismatch {
                    rows: row_count,
                    ids: row,
                });
            };
            let id_len_u64 = read_u64(&data[offset..id_len_end]);
            let id_len = usize::try_from(id_len_u64).map_err(|_| RqError::SizeOverflow {
                field: "row ID length",
                value: id_len_u64,
            })?;
            offset = id_len_end;
            let Some(id_end) = offset.checked_add(id_len).filter(|&end| end <= data.len()) else {
                return Err(RqError::RowIdCountMismatch {
                    rows: row_count,
                    ids: row,
                });
            };
            let id = std::str::from_utf8(&data[offset..id_end])
                .map_err(|_| RqError::InvalidRowId { row })?
                .to_owned();
            offset = id_end;

            let available = data.len() - offset;
            if available < plane_bytes {
                return Err(RqError::TruncatedPlaneBuffer {
                    row,
                    expected_bytes: plane_bytes,
                    actual_bytes: available,
                });
            }
            let plane_end = offset + plane_bytes;
            for chunk in data[offset..plane_end].chunks_exact(8) {
                planes.push(read_u64(chunk));
            }
            offset = plane_end;

            let available = data.len() - offset;
            if available < FACTOR_BYTES {
                return Err(RqError::FactorCountMismatch {
                    rows: row_count,
                    factors: row,
                });
            }
            let residual_norm = read_f32(&data[offset..offset + 4]);
            let bar_dot_residual = read_f32(&data[offset + 4..offset + FACTOR_BYTES]);
            factors.push(TwoBitFactors {
                residual_norm,
                bar_dot_residual,
            });
            ids.push(id);
            offset += FACTOR_BYTES;
        }

        if offset != data.len() {
            return Err(RqError::TrailingBytes {
                bytes: data.len() - offset,
            });
        }
        if ids.len() != row_count {
            return Err(RqError::RowIdCountMismatch {
                rows: row_count,
                ids: ids.len(),
            });
        }
        if factors.len() != row_count {
            return Err(RqError::FactorCountMismatch {
                rows: row_count,
                factors: factors.len(),
            });
        }

        Ok(Self {
            dim,
            ids,
            planes,
            factors,
        })
    }

    /// Scores one row with the existing two-bit popcount estimator.
    ///
    /// The query ADC must use the same rotation and centroid as the encoded
    /// rows. Euclidean and cosine return the row-varying squared-L2 term; the
    /// omitted query-residual norm is constant within this cluster. Dot product
    /// returns the negated residual-dot term; a cross-cluster caller adds the
    /// cluster's negated centroid/query dot before comparing clusters.
    pub fn asymmetric_distance(
        &self,
        query: &QueryAdc4,
        row: usize,
        metric: DistanceMetric,
    ) -> Result<f32, RqError> {
        check_dimension("query", query.dim(), self.dim)?;
        let factors = self
            .factors
            .get(row)
            .copied()
            .ok_or(RqError::RowIndexOutOfBounds {
                row,
                rows: self.row_count(),
            })?;
        let words_per_plane = self.dim / 64;
        let words_per_row = words_per_plane * 2;
        let plane_start = row * words_per_row;
        let planes = &self.planes[plane_start..plane_start + words_per_row];
        let (low_plane, high_plane) = planes.split_at(words_per_plane);

        match metric {
            DistanceMetric::Cosine | DistanceMetric::Euclidean => Ok(
                rabitq::estimate_l2_two_bit_parts(low_plane, high_plane, factors, query, 0.0)?,
            ),
            DistanceMetric::DotProduct => Ok(-rabitq::estimate_residual_dot_two_bit_parts(
                low_plane, high_plane, factors, query,
            )?),
        }
    }
}

fn check_dimension(name: &'static str, actual: usize, expected: usize) -> Result<(), RqError> {
    if actual == expected {
        Ok(())
    } else {
        Err(RqError::DimensionMismatch {
            name,
            expected,
            actual,
        })
    }
}

fn read_header_usize(data: &[u8], offset: usize, field: &'static str) -> Result<usize, RqError> {
    let value = read_u64(&data[offset..offset + 8]);
    usize::try_from(value).map_err(|_| RqError::SizeOverflow { field, value })
}

fn read_u64(data: &[u8]) -> u64 {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(data);
    u64::from_le_bytes(bytes)
}

fn read_f32(data: &[u8]) -> f32 {
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(data);
    f32::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SEED: u64 = 0x5251_434c_5553_5445;

    #[test]
    fn cluster_codes_round_trip() {
        let dim = BLOCK_DIM;
        let rotation = StructuredRotation::new(dim, TEST_SEED).expect("valid rotation");
        let centroid = vec![0.25_f32; dim];
        let rows = [vec![0.5_f32; dim], vec![-0.125_f32; dim]];
        let row_refs: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
        let ids = vec!["row-0".to_owned(), "row-1".to_owned()];

        let encoded =
            RqClusterCodes::encode(&ids, &row_refs, &centroid, &rotation).expect("encode");
        let decoded = RqClusterCodes::from_bytes(&encoded.to_bytes()).expect("decode");

        assert_eq!(decoded.dim(), encoded.dim());
        assert_eq!(decoded.ids(), encoded.ids());
        assert_eq!(decoded.packed_planes(), encoded.packed_planes());
        assert_eq!(decoded.factors().len(), encoded.factors().len());
        for (actual, expected) in decoded.factors().iter().zip(encoded.factors()) {
            assert_eq!(
                actual.residual_norm.to_bits(),
                expected.residual_norm.to_bits()
            );
            assert_eq!(
                actual.bar_dot_residual.to_bits(),
                expected.bar_dot_residual.to_bits()
            );
        }
    }

    #[test]
    fn cluster_codes_preserve_row_id_alignment() {
        let dim = BLOCK_DIM;
        let rotation = StructuredRotation::new(dim, TEST_SEED).expect("valid rotation");
        let centroid = vec![0.0_f32; dim];
        let rows = [vec![0.5_f32; dim], vec![-0.5_f32; dim]];
        let row_refs: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
        let ids = vec!["first".to_owned(), "second".to_owned()];

        let mismatch = RqClusterCodes::encode(&ids[..1], &row_refs, &centroid, &rotation);
        assert!(matches!(
            mismatch,
            Err(RqError::RowIdCountMismatch { rows: 2, ids: 1 })
        ));

        let encoded =
            RqClusterCodes::encode(&ids, &row_refs, &centroid, &rotation).expect("encode");
        let decoded = RqClusterCodes::from_bytes(&encoded.to_bytes()).expect("decode");
        assert_eq!(decoded.id(0), Some("first"));
        assert_eq!(decoded.id(1), Some("second"));
    }

    #[test]
    fn two_bit_estimator_recovers_top_ten_with_four_x_margin() {
        const DIM: usize = 768;
        const ROWS: usize = 1_800;
        const QUERIES: usize = 10;
        const GROUP_SIZE: usize = 20;
        const TOP_K: usize = 10;
        const CANDIDATES: usize = TOP_K * 4;
        const RECALL_FLOOR: f32 = 0.96;

        let rotation = StructuredRotation::new(DIM, TEST_SEED).expect("valid rotation");
        let centroid = vec![0.125_f32; DIM];
        let mut rng = TestRng::new(TEST_SEED ^ 0x5341_4e49_5459);
        let mut rows = Vec::with_capacity(ROWS);
        for _ in 0..(ROWS / GROUP_SIZE) {
            let mut prototype = (0..DIM).map(|_| rng.next_signed()).collect::<Vec<f32>>();
            normalize_to(&mut prototype, 1.0);
            for _ in 0..GROUP_SIZE {
                let mut residual = prototype
                    .iter()
                    .map(|value| value + 0.15 * rng.next_signed())
                    .collect::<Vec<f32>>();
                normalize_to(&mut residual, 0.35);
                rows.push(
                    residual
                        .iter()
                        .zip(&centroid)
                        .map(|(value, center)| value + center)
                        .collect::<Vec<f32>>(),
                );
            }
        }
        let ids = (0..ROWS)
            .map(|row| format!("row-{row}"))
            .collect::<Vec<_>>();
        let row_refs: Vec<&[f32]> = rows.iter().map(Vec::as_slice).collect();
        let codes = RqClusterCodes::encode(&ids, &row_refs, &centroid, &rotation).expect("encode");

        let mut recovered = 0_usize;
        let mut rotated_query = vec![0.0_f32; DIM];
        let mut rotation_scratch = vec![0.0_f32; DIM];
        for query_index in 0..QUERIES {
            let mut query = rows[query_index * GROUP_SIZE].clone();
            for value in &mut query {
                *value += 0.002 * rng.next_signed();
            }
            rotation
                .rotate_residual(&query, &centroid, &mut rotated_query, &mut rotation_scratch)
                .expect("rotate query");
            let query_adc =
                rabitq::prepare_query_adc4(&rotated_query, TEST_SEED ^ query_index as u64)
                    .expect("prepare query");

            let mut exact = rows
                .iter()
                .enumerate()
                .map(|(row, vector)| (row, squared_l2(&query, vector)))
                .collect::<Vec<_>>();
            exact.sort_unstable_by(|left, right| left.1.total_cmp(&right.1));

            let mut approximate = (0..ROWS)
                .map(|row| {
                    (
                        row,
                        codes
                            .asymmetric_distance(&query_adc, row, DistanceMetric::Euclidean)
                            .expect("score row"),
                    )
                })
                .collect::<Vec<_>>();
            approximate.sort_unstable_by(|left, right| left.1.total_cmp(&right.1));

            recovered += exact[..TOP_K]
                .iter()
                .filter(|(exact_row, _)| {
                    approximate[..CANDIDATES]
                        .iter()
                        .any(|(candidate_row, _)| candidate_row == exact_row)
                })
                .count();
        }

        let recall = recovered as f32 / (QUERIES * TOP_K) as f32;
        assert!(
            recall >= RECALL_FLOOR,
            "two-bit top-10 candidate recall {recall} fell below {RECALL_FLOOR}"
        );
    }

    fn squared_l2(left: &[f32], right: &[f32]) -> f32 {
        left.iter()
            .zip(right)
            .map(|(lhs, rhs)| {
                let delta = lhs - rhs;
                delta * delta
            })
            .sum()
    }

    fn normalize_to(values: &mut [f32], norm: f32) {
        let current = values.iter().map(|value| value * value).sum::<f32>().sqrt();
        for value in values {
            *value *= norm / current;
        }
    }

    struct TestRng {
        state: u64,
    }

    impl TestRng {
        const fn new(seed: u64) -> Self {
            Self { state: seed }
        }

        fn next_signed(&mut self) -> f32 {
            self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut value = self.state;
            value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            value ^= value >> 31;
            let unit = (value >> 40) as f32 / (1_u32 << 24) as f32;
            2.0 * unit - 1.0
        }
    }
}
