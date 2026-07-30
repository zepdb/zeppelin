use bytes::{BufMut, Bytes, BytesMut};
use sha2::{Digest, Sha256};

use crate::error::Result;

use super::{LateInteractionError, MultiVectorMatrixRef};

const TRANSFORM_MAGIC: &[u8; 4] = b"ZFT1";
const TRANSFORM_FORMAT_VERSION: u8 = 1;

/// Versioned FDE construction semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FdeAlgorithmVersion {
    /// The paper construction with a dense scaled Rademacher projection.
    PaperV1,
    /// Google's released identity/AMS/Count-Sketch construction.
    ReferenceV1,
}

/// Projection applied independently to every bucket block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InnerProjection {
    /// Retain the input vector coordinates.
    Identity,
    /// Use a dense Rademacher matrix scaled by `1 / sqrt(d_proj)`.
    Rademacher {
        /// Projected dimension of every bucket block.
        d_proj: u32,
    },
    /// Use the reference sparse AMS projection.
    AmsSketch {
        /// Projected dimension of every bucket block.
        d_proj: u32,
    },
}

/// Optional projection over the concatenated FDE.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FinalProjection {
    /// Retain the concatenated repetitions and buckets.
    None,
    /// Apply a sparse Count-Sketch projection.
    CountSketch {
        /// Final output dimension.
        d_final: u32,
    },
}

/// Complete parameters for one materialized FDE transform.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FdeParams {
    /// Construction and projection semantics.
    pub algorithm: FdeAlgorithmVersion,
    /// Number of independent partitions/projections to concatenate.
    pub repetitions: u32,
    /// Gaussian SimHash bits per repetition.
    pub simhash_bits: u32,
    /// Dimension of every input row.
    pub input_dimension: u32,
    /// Per-bucket projection.
    pub inner: InnerProjection,
    /// Optional projection of the concatenated FDE.
    pub final_projection: FinalProjection,
}

/// Materialized, portable FDE transform.
///
/// Serialized bytes, not a seed, are the transform's identity. SimHash planes,
/// inner projection values, and Count-Sketch assignments are all persisted.
#[derive(Clone, Debug)]
pub struct FdeTransform {
    params: FdeParams,
    simhash_planes: Vec<f32>,
    inner_projection: Vec<f32>,
    final_indices: Vec<u32>,
    final_signs: Vec<f32>,
    output_dimension: usize,
}

#[derive(Clone, Copy, Debug)]
struct ValidatedParams {
    repetitions: usize,
    simhash_bits: usize,
    bucket_count: usize,
    input_dimension: usize,
    projected_dimension: usize,
    base_dimension: usize,
    output_dimension: usize,
}

impl FdeTransform {
    /// Generates and materializes a deterministic transform.
    ///
    /// Gaussian planes use SplitMix64 followed by Box-Muller (`u64` to `f64`
    /// to `f32`). That generator and conversion order are part of both V1
    /// algorithm identities. Persisted transform bytes remain authoritative.
    #[must_use = "transform generation errors must be handled"]
    pub fn generate(params: &FdeParams, seed: u64) -> Result<Self> {
        let validated = validate_params(params)?;
        let plane_count = checked_product(&[
            validated.repetitions,
            validated.simhash_bits,
            validated.input_dimension,
        ])?;
        let inner_count = match params.inner {
            InnerProjection::Identity => 0,
            InnerProjection::Rademacher { .. } | InnerProjection::AmsSketch { .. } => {
                checked_product(&[
                    validated.repetitions,
                    validated.projected_dimension,
                    validated.input_dimension,
                ])?
            }
        };
        let final_count = match params.final_projection {
            FinalProjection::None => 0,
            FinalProjection::CountSketch { .. } => validated.base_dimension,
        };

        let mut random = SplitMixBoxMuller::new(seed);
        let mut simhash_planes = Vec::new();
        reserve_transform_values(&mut simhash_planes, plane_count)?;
        for _ in 0..plane_count {
            simhash_planes.push(random.gaussian());
        }

        let mut inner_projection = Vec::new();
        reserve_transform_values(&mut inner_projection, inner_count)?;
        match params.inner {
            InnerProjection::Identity => {}
            InnerProjection::Rademacher { .. } => {
                let scale = 1.0_f32 / (validated.projected_dimension as f32).sqrt();
                for _ in 0..inner_count {
                    inner_projection.push(random.sign() * scale);
                }
            }
            InnerProjection::AmsSketch { .. } => {
                inner_projection.resize(inner_count, 0.0);
                for repetition in 0..validated.repetitions {
                    let base =
                        repetition * validated.projected_dimension * validated.input_dimension;
                    for input in 0..validated.input_dimension {
                        let output = random.index(validated.projected_dimension);
                        inner_projection[base + output * validated.input_dimension + input] =
                            random.sign();
                    }
                }
            }
        }

        let mut final_indices = Vec::new();
        let mut final_signs = Vec::new();
        reserve_transform_values(&mut final_indices, final_count)?;
        reserve_transform_values(&mut final_signs, final_count)?;
        if let FinalProjection::CountSketch { .. } = params.final_projection {
            for _ in 0..final_count {
                final_indices.push(random.index(validated.output_dimension) as u32);
                final_signs.push(random.sign());
            }
        }

        Ok(Self {
            params: *params,
            simhash_planes,
            inner_projection,
            final_indices,
            final_signs,
            output_dimension: validated.output_dimension,
        })
    }

    /// Serializes all materialized transform data.
    #[must_use]
    pub fn to_bytes(&self) -> Bytes {
        let capacity = 4
            + 1
            + 1
            + 3 * size_of::<u32>()
            + 1
            + size_of::<u32>()
            + 1
            + size_of::<u32>()
            + self.simhash_planes.len() * size_of::<f32>()
            + self.inner_projection.len() * size_of::<f32>()
            + self.final_indices.len() * (size_of::<u32>() + size_of::<f32>());
        let mut bytes = BytesMut::with_capacity(capacity);
        bytes.extend_from_slice(TRANSFORM_MAGIC);
        bytes.put_u8(TRANSFORM_FORMAT_VERSION);
        bytes.put_u8(match self.params.algorithm {
            FdeAlgorithmVersion::PaperV1 => 1,
            FdeAlgorithmVersion::ReferenceV1 => 2,
        });
        bytes.put_u32_le(self.params.repetitions);
        bytes.put_u32_le(self.params.simhash_bits);
        bytes.put_u32_le(self.params.input_dimension);
        match self.params.inner {
            InnerProjection::Identity => {
                bytes.put_u8(0);
                bytes.put_u32_le(0);
            }
            InnerProjection::Rademacher { d_proj } => {
                bytes.put_u8(1);
                bytes.put_u32_le(d_proj);
            }
            InnerProjection::AmsSketch { d_proj } => {
                bytes.put_u8(2);
                bytes.put_u32_le(d_proj);
            }
        }
        match self.params.final_projection {
            FinalProjection::None => {
                bytes.put_u8(0);
                bytes.put_u32_le(0);
            }
            FinalProjection::CountSketch { d_final } => {
                bytes.put_u8(1);
                bytes.put_u32_le(d_final);
            }
        }
        for value in &self.simhash_planes {
            bytes.put_f32_le(*value);
        }
        for value in &self.inner_projection {
            bytes.put_f32_le(*value);
        }
        for (index, sign) in self.final_indices.iter().zip(&self.final_signs) {
            bytes.put_u32_le(*index);
            bytes.put_f32_le(*sign);
        }
        bytes.freeze()
    }

    /// Parses one complete transform artifact.
    #[must_use = "transform decode errors must be handled"]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut reader = TransformReader::new(bytes);
        if reader.read_exact(TRANSFORM_MAGIC.len())? != TRANSFORM_MAGIC {
            return Err(invalid_transform("bad transform magic"));
        }
        if reader.read_u8()? != TRANSFORM_FORMAT_VERSION {
            return Err(invalid_transform("unsupported transform format version"));
        }
        let algorithm = match reader.read_u8()? {
            1 => FdeAlgorithmVersion::PaperV1,
            2 => FdeAlgorithmVersion::ReferenceV1,
            _ => return Err(invalid_transform("unknown algorithm version")),
        };
        let repetitions = reader.read_u32()?;
        let simhash_bits = reader.read_u32()?;
        let input_dimension = reader.read_u32()?;
        let inner_tag = reader.read_u8()?;
        let inner_dimension = reader.read_u32()?;
        let inner = match (inner_tag, inner_dimension) {
            (0, 0) => InnerProjection::Identity,
            (1, d_proj) => InnerProjection::Rademacher { d_proj },
            (2, d_proj) => InnerProjection::AmsSketch { d_proj },
            _ => {
                return Err(invalid_transform(
                    "invalid inner projection tag or dimension",
                ))
            }
        };
        let final_tag = reader.read_u8()?;
        let final_dimension = reader.read_u32()?;
        let final_projection = match (final_tag, final_dimension) {
            (0, 0) => FinalProjection::None,
            (1, d_final) => FinalProjection::CountSketch { d_final },
            _ => {
                return Err(invalid_transform(
                    "invalid final projection tag or dimension",
                ))
            }
        };
        let params = FdeParams {
            algorithm,
            repetitions,
            simhash_bits,
            input_dimension,
            inner,
            final_projection,
        };
        let validated = validate_params(&params)
            .map_err(|error| invalid_transform(format!("invalid persisted parameters: {error}")))?;
        let plane_count = checked_product(&[
            validated.repetitions,
            validated.simhash_bits,
            validated.input_dimension,
        ])
        .map_err(|error| invalid_transform(error.to_string()))?;
        let inner_count = match inner {
            InnerProjection::Identity => 0,
            InnerProjection::Rademacher { .. } | InnerProjection::AmsSketch { .. } => {
                checked_product(&[
                    validated.repetitions,
                    validated.projected_dimension,
                    validated.input_dimension,
                ])
                .map_err(|error| invalid_transform(error.to_string()))?
            }
        };
        let final_count = match final_projection {
            FinalProjection::None => 0,
            FinalProjection::CountSketch { .. } => validated.base_dimension,
        };
        let required_bytes = plane_count
            .checked_add(inner_count)
            .and_then(|count| count.checked_mul(size_of::<f32>()))
            .and_then(|scalar_bytes| {
                final_count
                    .checked_mul(size_of::<u32>() + size_of::<f32>())
                    .and_then(|final_bytes| scalar_bytes.checked_add(final_bytes))
            })
            .ok_or_else(|| invalid_transform("transform byte length overflows"))?;
        if reader.remaining() != required_bytes {
            return Err(invalid_transform(format!(
                "transform payload length mismatch: expected {required_bytes}, got {}",
                reader.remaining()
            )));
        }

        let simhash_planes = reader.read_finite_f32s(plane_count)?;
        let inner_projection = reader.read_finite_f32s(inner_count)?;
        let mut final_indices = Vec::new();
        let mut final_signs = Vec::new();
        reserve_transform_values(&mut final_indices, final_count)
            .map_err(|error| invalid_transform(error.to_string()))?;
        reserve_transform_values(&mut final_signs, final_count)
            .map_err(|error| invalid_transform(error.to_string()))?;
        for _ in 0..final_count {
            let index = reader.read_u32()?;
            let sign = reader.read_f32()?;
            if index as usize >= validated.output_dimension {
                return Err(invalid_transform("Count-Sketch index is out of range"));
            }
            if sign != -1.0 && sign != 1.0 {
                return Err(invalid_transform("Count-Sketch sign is not -1 or 1"));
            }
            final_indices.push(index);
            final_signs.push(sign);
        }
        if reader.remaining() != 0 {
            return Err(invalid_transform("trailing transform bytes"));
        }

        Ok(Self {
            params,
            simhash_planes,
            inner_projection,
            final_indices,
            final_signs,
            output_dimension: validated.output_dimension,
        })
    }

    /// Returns SHA-256 over [`Self::to_bytes`].
    #[must_use]
    pub fn checksum(&self) -> [u8; 32] {
        Sha256::digest(self.to_bytes()).into()
    }

    /// Returns the fixed output dimension.
    #[must_use]
    pub const fn output_dimension(&self) -> usize {
        self.output_dimension
    }

    /// Encodes a document with average bucket contents and deterministic fill.
    ///
    /// Empty document buckets choose the populated bucket with minimum Hamming
    /// distance. Ties choose the lowest bucket index, then the lowest row
    /// ordinal in that bucket. This ordering is part of both V1 algorithms.
    #[must_use = "FDE encoding errors must be handled"]
    pub fn encode_document(&self, matrix: &MultiVectorMatrixRef<'_>) -> Result<Vec<f32>> {
        self.encode(matrix, MatrixRole::Document)
    }

    /// Encodes a query with summed bucket contents and zero empty buckets.
    #[must_use = "FDE encoding errors must be handled"]
    pub fn encode_query(&self, matrix: &MultiVectorMatrixRef<'_>) -> Result<Vec<f32>> {
        self.encode(matrix, MatrixRole::Query)
    }

    fn encode(&self, matrix: &MultiVectorMatrixRef<'_>, role: MatrixRole) -> Result<Vec<f32>> {
        let validated = validate_params(&self.params)?;
        if matrix.vector_dimension() != validated.input_dimension {
            return Err(LateInteractionError::DimensionMismatch {
                expected: validated.input_dimension,
                actual: matrix.vector_dimension(),
            }
            .into());
        }
        if role == MatrixRole::Document {
            for row in 0..matrix.vector_count() {
                if matrix.row(row).iter().all(|value| *value == 0.0) {
                    return Err(LateInteractionError::ZeroDocumentVector { row }.into());
                }
            }
        }

        let mut output = vec![0.0_f32; validated.base_dimension];
        let projected_value_count = matrix
            .vector_count()
            .checked_mul(validated.projected_dimension)
            .ok_or(LateInteractionError::InvalidFdeParams {
                reason: "projected matrix shape overflows",
            })?;

        for repetition in 0..validated.repetitions {
            let mut row_buckets = Vec::with_capacity(matrix.vector_count());
            let mut projected_rows = Vec::new();
            reserve_transform_values(&mut projected_rows, projected_value_count)?;
            for row in 0..matrix.vector_count() {
                row_buckets.push(self.bucket_for_row(matrix.row(row), repetition, &validated));
                self.project_row(matrix.row(row), repetition, &validated, &mut projected_rows);
            }

            let repetition_base =
                repetition * validated.bucket_count * validated.projected_dimension;
            let mut counts = vec![0_usize; validated.bucket_count];
            let mut first_rows = vec![None; validated.bucket_count];
            for (row, &bucket) in row_buckets.iter().enumerate() {
                counts[bucket] += 1;
                first_rows[bucket].get_or_insert(row);
                let output_base = repetition_base + bucket * validated.projected_dimension;
                let row_base = row * validated.projected_dimension;
                for coordinate in 0..validated.projected_dimension {
                    output[output_base + coordinate] += projected_rows[row_base + coordinate];
                }
            }

            if role == MatrixRole::Document {
                for (bucket, &count) in counts.iter().enumerate() {
                    let output_base = repetition_base + bucket * validated.projected_dimension;
                    if count > 0 {
                        let denominator = count as f32;
                        for coordinate in 0..validated.projected_dimension {
                            output[output_base + coordinate] /= denominator;
                        }
                        continue;
                    }

                    let source_bucket = first_rows
                        .iter()
                        .enumerate()
                        .filter_map(|(candidate, row)| {
                            row.map(|_| ((bucket ^ candidate).count_ones(), candidate))
                        })
                        .min()
                        .map(|(_, candidate)| candidate)
                        .ok_or_else(|| invalid_transform("document populated no FDE bucket"))?;
                    let source_row = first_rows[source_bucket]
                        .ok_or_else(|| invalid_transform("selected FDE bucket is empty"))?;
                    let row_base = source_row * validated.projected_dimension;
                    output[output_base..output_base + validated.projected_dimension]
                        .copy_from_slice(
                            &projected_rows[row_base..row_base + validated.projected_dimension],
                        );
                }
            }
        }

        let encoded = match self.params.final_projection {
            FinalProjection::None => output,
            FinalProjection::CountSketch { .. } => {
                let mut projected = vec![0.0_f32; validated.output_dimension];
                for (source, value) in output.into_iter().enumerate() {
                    projected[self.final_indices[source] as usize] +=
                        self.final_signs[source] * value;
                }
                projected
            }
        };
        if encoded.iter().any(|value| !value.is_finite()) {
            return Err(invalid_transform(
                "FDE encoding produced a non-finite value",
            ));
        }
        Ok(encoded)
    }

    fn bucket_for_row(&self, row: &[f32], repetition: usize, validated: &ValidatedParams) -> usize {
        let mut bucket = 0_usize;
        for bit in 0..validated.simhash_bits {
            let plane_base =
                (repetition * validated.simhash_bits + bit) * validated.input_dimension;
            let dot = row
                .iter()
                .zip(&self.simhash_planes[plane_base..plane_base + validated.input_dimension])
                .map(|(value, plane)| value * plane)
                .sum::<f32>();
            bucket = (bucket << 1) | usize::from(dot > 0.0);
        }
        bucket
    }

    fn project_row(
        &self,
        row: &[f32],
        repetition: usize,
        validated: &ValidatedParams,
        output: &mut Vec<f32>,
    ) {
        match self.params.inner {
            InnerProjection::Identity => output.extend_from_slice(row),
            InnerProjection::Rademacher { .. } | InnerProjection::AmsSketch { .. } => {
                let repetition_base =
                    repetition * validated.projected_dimension * validated.input_dimension;
                for projected in 0..validated.projected_dimension {
                    let projection_base = repetition_base + projected * validated.input_dimension;
                    output.push(
                        row.iter()
                            .zip(
                                &self.inner_projection
                                    [projection_base..projection_base + validated.input_dimension],
                            )
                            .map(|(value, projection)| value * projection)
                            .sum(),
                    );
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatrixRole {
    Query,
    Document,
}

fn validate_params(params: &FdeParams) -> Result<ValidatedParams> {
    let repetitions = usize::try_from(params.repetitions).map_err(|_| {
        LateInteractionError::InvalidFdeParams {
            reason: "repetition count does not fit usize",
        }
    })?;
    if repetitions == 0 {
        return Err(LateInteractionError::InvalidFdeParams {
            reason: "repetitions must be positive",
        }
        .into());
    }
    let simhash_bits = usize::try_from(params.simhash_bits).map_err(|_| {
        LateInteractionError::InvalidFdeParams {
            reason: "simhash bit count does not fit usize",
        }
    })?;
    let bucket_count =
        1_usize
            .checked_shl(params.simhash_bits)
            .ok_or(LateInteractionError::InvalidFdeParams {
                reason: "simhash bucket count overflows",
            })?;
    let input_dimension = usize::try_from(params.input_dimension).map_err(|_| {
        LateInteractionError::InvalidFdeParams {
            reason: "input dimension does not fit usize",
        }
    })?;
    if input_dimension == 0 {
        return Err(LateInteractionError::InvalidFdeParams {
            reason: "input dimension must be positive",
        }
        .into());
    }
    let projected_dimension = match params.inner {
        InnerProjection::Identity => {
            if params.algorithm != FdeAlgorithmVersion::ReferenceV1 {
                return Err(LateInteractionError::InvalidFdeParams {
                    reason: "identity projection requires ReferenceV1",
                }
                .into());
            }
            input_dimension
        }
        InnerProjection::Rademacher { d_proj } => {
            if params.algorithm != FdeAlgorithmVersion::PaperV1 {
                return Err(LateInteractionError::InvalidFdeParams {
                    reason: "Rademacher projection requires PaperV1",
                }
                .into());
            }
            checked_projected_dimension(d_proj, input_dimension)?
        }
        InnerProjection::AmsSketch { d_proj } => {
            if params.algorithm != FdeAlgorithmVersion::ReferenceV1 {
                return Err(LateInteractionError::InvalidFdeParams {
                    reason: "AMS projection requires ReferenceV1",
                }
                .into());
            }
            checked_projected_dimension(d_proj, input_dimension)?
        }
    };
    if params.algorithm == FdeAlgorithmVersion::PaperV1
        && !matches!(params.final_projection, FinalProjection::None)
    {
        return Err(LateInteractionError::InvalidFdeParams {
            reason: "PaperV1 does not support a final projection",
        }
        .into());
    }
    let base_dimension = checked_product(&[repetitions, bucket_count, projected_dimension])?;
    let output_dimension = match params.final_projection {
        FinalProjection::None => base_dimension,
        FinalProjection::CountSketch { d_final } => {
            if params.algorithm != FdeAlgorithmVersion::ReferenceV1 {
                return Err(LateInteractionError::InvalidFdeParams {
                    reason: "Count-Sketch final projection requires ReferenceV1",
                }
                .into());
            }
            let dimension =
                usize::try_from(d_final).map_err(|_| LateInteractionError::InvalidFdeParams {
                    reason: "final dimension does not fit usize",
                })?;
            if dimension == 0 {
                return Err(LateInteractionError::InvalidFdeParams {
                    reason: "final dimension must be positive",
                }
                .into());
            }
            if dimension > base_dimension {
                return Err(LateInteractionError::InvalidFdeParams {
                    reason: "final dimension exceeds the unprojected FDE dimension",
                }
                .into());
            }
            dimension
        }
    };
    Ok(ValidatedParams {
        repetitions,
        simhash_bits,
        bucket_count,
        input_dimension,
        projected_dimension,
        base_dimension,
        output_dimension,
    })
}

fn checked_projected_dimension(dimension: u32, input_dimension: usize) -> Result<usize> {
    let projected =
        usize::try_from(dimension).map_err(|_| LateInteractionError::InvalidFdeParams {
            reason: "projected dimension does not fit usize",
        })?;
    if projected == 0 {
        return Err(LateInteractionError::InvalidFdeParams {
            reason: "projected dimension must be positive",
        }
        .into());
    }
    if projected > input_dimension {
        return Err(LateInteractionError::InvalidFdeParams {
            reason: "projected dimension exceeds input dimension",
        }
        .into());
    }
    Ok(projected)
}

fn checked_product(factors: &[usize]) -> Result<usize> {
    factors.iter().try_fold(1_usize, |product, factor| {
        product.checked_mul(*factor).ok_or_else(|| {
            LateInteractionError::InvalidFdeParams {
                reason: "FDE dimension arithmetic overflows",
            }
            .into()
        })
    })
}

fn reserve_transform_values<T>(values: &mut Vec<T>, count: usize) -> Result<()> {
    values.try_reserve_exact(count).map_err(|_| {
        LateInteractionError::InvalidFdeParams {
            reason: "FDE transform allocation failed",
        }
        .into()
    })
}

fn invalid_transform(reason: impl Into<String>) -> crate::error::ZeppelinError {
    LateInteractionError::InvalidFdeTransform {
        reason: reason.into(),
    }
    .into()
}

struct TransformReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> TransformReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| invalid_transform("transform cursor overflows"))?;
        if end > self.bytes.len() {
            return Err(invalid_transform("truncated transform"));
        }
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_exact(size_of::<u32>())?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_f32(&mut self) -> Result<f32> {
        let bytes = self.read_exact(size_of::<f32>())?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_finite_f32s(&mut self, count: usize) -> Result<Vec<f32>> {
        let mut values = Vec::new();
        reserve_transform_values(&mut values, count)
            .map_err(|error| invalid_transform(error.to_string()))?;
        for _ in 0..count {
            let value = self.read_f32()?;
            if !value.is_finite() {
                return Err(invalid_transform("transform contains a non-finite scalar"));
            }
            values.push(value);
        }
        Ok(values)
    }
}

struct SplitMixBoxMuller {
    state: u64,
    spare_gaussian: Option<f64>,
}

impl SplitMixBoxMuller {
    const fn new(seed: u64) -> Self {
        Self {
            state: seed,
            spare_gaussian: None,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn unit_open(&mut self) -> f64 {
        const DENOMINATOR: f64 = (1_u64 << 53) as f64 + 1.0;
        ((self.next_u64() >> 11) as f64 + 1.0) / DENOMINATOR
    }

    fn gaussian(&mut self) -> f32 {
        if let Some(spare) = self.spare_gaussian.take() {
            return spare as f32;
        }
        let radius = (-2.0 * self.unit_open().ln()).sqrt();
        let angle = std::f64::consts::TAU * self.unit_open();
        self.spare_gaussian = Some(radius * angle.sin());
        (radius * angle.cos()) as f32
    }

    fn sign(&mut self) -> f32 {
        if self.next_u64() & 1 == 0 {
            -1.0
        } else {
            1.0
        }
    }

    fn index(&mut self, upper_bound: usize) -> usize {
        (self.next_u64() % upper_bound as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use crate::error::ZeppelinError;

    use super::*;
    use crate::index::late_interaction::max_sim;

    const MAX_VECTORS: usize = 32;
    const FIXTURES_JSON: &str = include_str!("../../../tests/fixtures/mmli/fde_fixtures.json");

    #[derive(Debug, Deserialize)]
    struct Fixtures {
        fixtures: Vec<Fixture>,
    }

    #[derive(Debug, Deserialize)]
    struct Fixture {
        name: String,
        transform_hex: String,
        query: MatrixFixture,
        document: MatrixFixture,
        expected_query_fde: Vec<f32>,
        expected_document_fde: Vec<f32>,
        expected_maxsim: f32,
        query_zero_coordinates: Vec<usize>,
    }

    #[derive(Debug, Deserialize)]
    struct MatrixFixture {
        values: Vec<f32>,
        vector_count: usize,
        vector_dimension: usize,
    }

    #[test]
    fn numpy_fixtures_match_both_fde_variants_and_maxsim() {
        let fixtures: Fixtures = serde_json::from_str(FIXTURES_JSON).expect("valid fixture JSON");
        assert!(fixtures.fixtures.len() >= 3);
        for fixture in fixtures.fixtures {
            let transform_bytes = decode_hex(&fixture.transform_hex);
            let transform =
                FdeTransform::from_bytes(&transform_bytes).expect("valid fixture transform");
            let query = MultiVectorMatrixRef::new(
                &fixture.query.values,
                fixture.query.vector_count,
                fixture.query.vector_dimension,
                MAX_VECTORS,
            )
            .expect("valid fixture query");
            let document = MultiVectorMatrixRef::new(
                &fixture.document.values,
                fixture.document.vector_count,
                fixture.document.vector_dimension,
                MAX_VECTORS,
            )
            .expect("valid fixture document");
            let actual_query = transform.encode_query(&query).expect("query FDE");
            let actual_document = transform.encode_document(&document).expect("document FDE");
            assert_close(
                &fixture.name,
                &actual_query,
                &fixture.expected_query_fde,
                1.0e-5,
            );
            assert_close(
                &fixture.name,
                &actual_document,
                &fixture.expected_document_fde,
                1.0e-5,
            );
            let actual_maxsim = max_sim(&query, &document).expect("MaxSim");
            assert!(
                (actual_maxsim - fixture.expected_maxsim).abs() <= 1.0e-6,
                "{} MaxSim: expected {}, got {}",
                fixture.name,
                fixture.expected_maxsim,
                actual_maxsim
            );
            for coordinate in fixture.query_zero_coordinates {
                assert_eq!(
                    actual_query[coordinate], 0.0,
                    "{} query coordinate {coordinate} must remain zero",
                    fixture.name
                );
                assert_ne!(
                    actual_document[coordinate], 0.0,
                    "{} document coordinate {coordinate} must be filled",
                    fixture.name
                );
            }
        }
    }

    #[test]
    fn transform_bytes_checksum_and_encoding_are_deterministic() {
        let params = FdeParams {
            algorithm: FdeAlgorithmVersion::ReferenceV1,
            repetitions: 2,
            simhash_bits: 2,
            input_dimension: 3,
            inner: InnerProjection::AmsSketch { d_proj: 2 },
            final_projection: FinalProjection::CountSketch { d_final: 5 },
        };
        let first = FdeTransform::generate(&params, 42).expect("first transform");
        let second = FdeTransform::generate(&params, 42).expect("second transform");
        assert_eq!(first.to_bytes(), second.to_bytes());
        assert_eq!(first.checksum(), second.checksum());

        let decoded = FdeTransform::from_bytes(&first.to_bytes()).expect("round trip");
        assert_eq!(decoded.to_bytes(), first.to_bytes());
        assert_eq!(decoded.checksum(), first.checksum());
        assert_eq!(decoded.output_dimension(), 5);

        let values = [1.0, 2.0, 3.0, -1.0, 0.5, 2.0];
        let matrix = MultiVectorMatrixRef::new(&values, 2, 3, MAX_VECTORS).expect("valid matrix");
        assert_eq!(
            decoded.encode_query(&matrix).expect("first encoding"),
            decoded.encode_query(&matrix).expect("second encoding")
        );
    }

    #[test]
    fn invalid_dimensions_and_zero_document_rows_fail_loud() {
        let invalid = [
            FdeParams {
                algorithm: FdeAlgorithmVersion::PaperV1,
                repetitions: 0,
                simhash_bits: 2,
                input_dimension: 3,
                inner: InnerProjection::Identity,
                final_projection: FinalProjection::None,
            },
            FdeParams {
                algorithm: FdeAlgorithmVersion::PaperV1,
                repetitions: 1,
                simhash_bits: usize::BITS,
                input_dimension: 3,
                inner: InnerProjection::Identity,
                final_projection: FinalProjection::None,
            },
            FdeParams {
                algorithm: FdeAlgorithmVersion::PaperV1,
                repetitions: 1,
                simhash_bits: 2,
                input_dimension: 3,
                inner: InnerProjection::Rademacher { d_proj: 4 },
                final_projection: FinalProjection::None,
            },
            FdeParams {
                algorithm: FdeAlgorithmVersion::PaperV1,
                repetitions: 1,
                simhash_bits: 2,
                input_dimension: 3,
                inner: InnerProjection::Identity,
                final_projection: FinalProjection::None,
            },
        ];
        for params in invalid {
            assert!(matches!(
                FdeTransform::generate(&params, 1),
                Err(ZeppelinError::LateInteraction(
                    LateInteractionError::InvalidFdeParams { .. }
                ))
            ));
        }

        let params = FdeParams {
            algorithm: FdeAlgorithmVersion::PaperV1,
            repetitions: 1,
            simhash_bits: 1,
            input_dimension: 2,
            inner: InnerProjection::Rademacher { d_proj: 2 },
            final_projection: FinalProjection::None,
        };
        let transform = FdeTransform::generate(&params, 7).expect("valid transform");
        let values = [0.0, 0.0, 1.0, 2.0];
        let document = MultiVectorMatrixRef::new(&values, 2, 2, MAX_VECTORS).expect("valid matrix");
        assert!(matches!(
            transform.encode_document(&document),
            Err(ZeppelinError::LateInteraction(
                LateInteractionError::ZeroDocumentVector { row: 0 }
            ))
        ));
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        assert_eq!(value.len() % 2, 0, "hex fixture length");
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let pair = std::str::from_utf8(pair).expect("ASCII hex");
                u8::from_str_radix(pair, 16).expect("valid hex")
            })
            .collect()
    }

    fn assert_close(name: &str, actual: &[f32], expected: &[f32], tolerance: f32) {
        assert_eq!(actual.len(), expected.len(), "{name} dimension");
        for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
            assert!(
                (actual - expected).abs() <= tolerance,
                "{name} coordinate {index}: expected {expected}, got {actual}"
            );
        }
    }
}
