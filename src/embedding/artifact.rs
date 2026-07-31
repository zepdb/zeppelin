//! Deterministic codecs for immutable late-interaction vector artifacts.
//!
//! Exact document matrices use profile-pinned f16 or symmetric groupwise INT8
//! payloads. FDE vectors and candidate-centering means remain IEEE-754 binary32
//! little-endian. Every format binds its semantic identity in the header and
//! rejects trailing bytes, non-finite values, shape drift, and caller-supplied
//! identity mismatches.

use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::error::{Result, ZeppelinError};

use super::{
    ArtifactChecksum, ContentHash, FdeGenerationId, MatrixDtype, MultiVectorEmbedding,
    MultiVectorEpochId,
};

const MATRIX_MAGIC: &[u8; 4] = b"ZME1";
const FDE_MAGIC: &[u8; 4] = b"ZFD1";
const CENTERING_MAGIC: &[u8; 4] = b"ZCM1";
const MATRIX_DTYPE_F16: u8 = 1;
const MATRIX_DTYPE_INT8_SYM_V1: u8 = 2;
const F32_LE_ENCODING: u8 = 1;

/// Typed matrix-header failures which must never trigger a format fallback.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EmbeddingArtifactError {
    /// The authoritative matrix header carries an unknown dtype tag.
    #[error("unsupported matrix dtype discriminant {discriminant}")]
    UnsupportedMatrixDtype {
        /// Raw persisted header byte.
        discriminant: u8,
    },
    /// Section/profile metadata disagrees with the authoritative header.
    #[error("matrix dtype mismatch: expected {expected:?}, header declared {actual:?}")]
    MatrixDtypeMismatch {
        /// Dtype copied into authoritative section metadata.
        expected: MatrixDtype,
        /// Dtype decoded from the artifact header.
        actual: MatrixDtype,
    },
    /// A known dtype tag carries a non-canonical parameter encoding.
    #[error("invalid matrix dtype header: {reason}")]
    InvalidMatrixDtypeHeader {
        /// Stable corruption diagnostic.
        reason: String,
    },
}

/// Current exact-matrix artifact format.
pub const MATRIX_ARTIFACT_FORMAT_VERSION: u8 = 1;

/// Current FDE-vector artifact format.
pub const FDE_ARTIFACT_FORMAT_VERSION: u8 = 1;

/// Current centering-mean artifact format.
pub const CENTERING_ARTIFACT_FORMAT_VERSION: u8 = 1;

/// Deterministic bytes plus their content-address identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImmutableArtifactBytes {
    bytes: Bytes,
    checksum: ArtifactChecksum,
}

impl ImmutableArtifactBytes {
    fn new(bytes: Bytes) -> Self {
        let checksum = ArtifactChecksum::digest(&bytes);
        Self { bytes, checksum }
    }

    /// Return the complete immutable artifact.
    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Return SHA-256 over the complete artifact.
    #[must_use]
    pub const fn checksum(&self) -> ArtifactChecksum {
        self.checksum
    }

    /// Validate bytes found at this artifact's create-only content address.
    ///
    /// Exact byte equality is checked after the digest so a theoretical
    /// same-digest/different-bytes collision is still an integrity failure.
    pub fn validate_existing(&self, existing: &[u8]) -> Result<()> {
        let existing_checksum = ArtifactChecksum::digest(existing);
        if existing_checksum != self.checksum {
            return Err(artifact_error(format!(
                "content-address checksum mismatch: expected {}, got {}",
                self.checksum.to_hex(),
                existing_checksum.to_hex()
            )));
        }
        if existing != self.bytes.as_ref() {
            return Err(artifact_error(
                "content-address collision: existing bytes differ".to_string(),
            ));
        }
        Ok(())
    }
}

/// One content-bound exact document matrix within a matrix artifact.
#[derive(Clone, Debug, PartialEq)]
pub struct MatrixArtifactRow {
    content_hash: ContentHash,
    embedding: MultiVectorEmbedding,
}

impl MatrixArtifactRow {
    /// Bind a validated owned matrix to its source content identity.
    #[must_use]
    pub const fn new(content_hash: ContentHash, embedding: MultiVectorEmbedding) -> Self {
        Self {
            content_hash,
            embedding,
        }
    }

    /// Return the exact source content identity.
    #[must_use]
    pub const fn content_hash(&self) -> ContentHash {
        self.content_hash
    }

    /// Borrow the exact matrix.
    #[must_use]
    pub const fn embedding(&self) -> &MultiVectorEmbedding {
        &self.embedding
    }
}

/// One deterministic fragment of exact document matrices.
#[derive(Clone, Debug, PartialEq)]
pub struct MatrixArtifact {
    dtype: MatrixDtype,
    semantic_epoch: MultiVectorEpochId,
    source_fragment_checksum: u64,
    vector_dimension: usize,
    rows: Vec<MatrixArtifactRow>,
}

impl MatrixArtifact {
    /// Validate and construct an ordered exact-matrix fragment.
    pub fn new(
        dtype: MatrixDtype,
        semantic_epoch: MultiVectorEpochId,
        source_fragment_checksum: u64,
        vector_dimension: usize,
        rows: Vec<MatrixArtifactRow>,
    ) -> Result<Self> {
        dtype
            .validate_for_dimension(u32::try_from(vector_dimension).map_err(|_| {
                artifact_error("matrix vector dimension exceeds u32".to_string())
            })?)?;
        validate_matrix_rows(vector_dimension, &rows)?;
        Ok(Self {
            dtype,
            semantic_epoch,
            source_fragment_checksum,
            vector_dimension,
            rows,
        })
    }

    /// Return the authoritative persisted matrix dtype.
    #[must_use]
    pub const fn dtype(&self) -> MatrixDtype {
        self.dtype
    }

    /// Return the semantic epoch that produced every matrix.
    #[must_use]
    pub const fn semantic_epoch(&self) -> MultiVectorEpochId {
        self.semantic_epoch
    }

    /// Return the immutable source input-fragment checksum.
    #[must_use]
    pub const fn source_fragment_checksum(&self) -> u64 {
        self.source_fragment_checksum
    }

    /// Return the common matrix dimension.
    #[must_use]
    pub const fn vector_dimension(&self) -> usize {
        self.vector_dimension
    }

    /// Borrow rows in their deterministic source order.
    #[must_use]
    pub fn rows(&self) -> &[MatrixArtifactRow] {
        &self.rows
    }

    /// Encode deterministic, content-addressed matrix bytes.
    pub fn to_bytes(&self) -> Result<ImmutableArtifactBytes> {
        self.dtype
            .validate_for_dimension(u32::try_from(self.vector_dimension).map_err(|_| {
                artifact_error("matrix vector dimension exceeds u32".to_string())
            })?)?;
        validate_matrix_rows(self.vector_dimension, &self.rows)?;
        let vector_dimension = u32::try_from(self.vector_dimension)
            .map_err(|_| artifact_error("matrix vector dimension exceeds u32".to_string()))?;
        let row_count = u32::try_from(self.rows.len())
            .map_err(|_| artifact_error("matrix artifact row count exceeds u32".to_string()))?;
        let total_vectors = self.rows.iter().try_fold(0_u64, |total, row| {
            let count = u64::try_from(row.embedding.vector_count())
                .map_err(|_| artifact_error("matrix vector count exceeds u64".to_string()))?;
            total
                .checked_add(count)
                .ok_or_else(|| artifact_error("matrix total vector count overflows".to_string()))
        })?;
        let bytes_per_vector = matrix_bytes_per_vector(self.dtype, self.vector_dimension)?;
        let payload_bytes = usize::try_from(total_vectors)
            .map_err(|_| artifact_error("matrix vector count exceeds usize".to_string()))?
            .checked_mul(bytes_per_vector)
            .ok_or_else(|| artifact_error("matrix payload byte count overflows".to_string()))?;
        let directory_bytes = self
            .rows
            .len()
            .checked_mul(32 + size_of::<u32>() + size_of::<u64>())
            .ok_or_else(|| artifact_error("matrix directory byte count overflows".to_string()))?;
        let capacity = matrix_header_len()
            .checked_add(directory_bytes)
            .and_then(|length| length.checked_add(payload_bytes))
            .ok_or_else(|| artifact_error("matrix artifact byte count overflows".to_string()))?;
        let mut bytes = BytesMut::with_capacity(capacity);
        bytes.extend_from_slice(MATRIX_MAGIC);
        bytes.put_u8(MATRIX_ARTIFACT_FORMAT_VERSION);
        let (dtype_discriminant, group_size) = matrix_dtype_header(self.dtype);
        bytes.put_u8(dtype_discriminant);
        bytes.put_u16_le(group_size);
        bytes.extend_from_slice(self.semantic_epoch.as_bytes());
        bytes.put_u64_le(self.source_fragment_checksum);
        bytes.put_u32_le(vector_dimension);
        bytes.put_u32_le(row_count);
        bytes.put_u64_le(total_vectors);

        let mut vector_offset = 0_u64;
        for row in &self.rows {
            bytes.extend_from_slice(row.content_hash.as_bytes());
            bytes.put_u32_le(
                u32::try_from(row.embedding.vector_count()).map_err(|_| {
                    artifact_error("matrix row vector count exceeds u32".to_string())
                })?,
            );
            bytes.put_u64_le(vector_offset);
            vector_offset = vector_offset
                .checked_add(u64::try_from(row.embedding.vector_count()).map_err(|_| {
                    artifact_error("matrix row vector count exceeds u64".to_string())
                })?)
                .ok_or_else(|| artifact_error("matrix vector offset overflows".to_string()))?;
        }
        match self.dtype {
            MatrixDtype::F16 => {
                for row in &self.rows {
                    for value in row.embedding.values() {
                        bytes.put_u16_le(f32_to_f16_bits(*value)?);
                    }
                }
            }
            MatrixDtype::Int8SymV1 { group_size } => {
                let group_size = usize::from(group_size);
                for row in &self.rows {
                    for vector in row.embedding.values().chunks_exact(self.vector_dimension) {
                        let encoded = encode_int8_sym_v1_vector(vector, group_size)?;
                        for code in encoded.codes {
                            bytes.put_i8(code);
                        }
                        for scale in encoded.folded_scale_bits {
                            bytes.put_u16_le(scale);
                        }
                    }
                }
            }
        }
        Ok(ImmutableArtifactBytes::new(bytes.freeze()))
    }

    /// Decode and validate complete matrix bytes against caller authority.
    #[allow(clippy::too_many_arguments)]
    pub fn from_bytes(
        bytes: &[u8],
        expected_checksum: ArtifactChecksum,
        expected_dtype: MatrixDtype,
        expected_epoch: MultiVectorEpochId,
        expected_source_fragment_checksum: u64,
        expected_vector_dimension: usize,
        max_rows: usize,
        max_vectors_per_row: usize,
    ) -> Result<Self> {
        verify_checksum(bytes, expected_checksum, "matrix")?;
        let mut reader = ArtifactReader::new(bytes, "matrix");
        reader.expect_magic(MATRIX_MAGIC)?;
        reader.expect_u8(
            MATRIX_ARTIFACT_FORMAT_VERSION,
            "unsupported matrix artifact version",
        )?;
        let dtype_discriminant = reader.read_u8()?;
        let group_size = reader.read_u16()?;
        let dtype = matrix_dtype_from_header(dtype_discriminant, group_size)?;
        if dtype != expected_dtype {
            return Err(EmbeddingArtifactError::MatrixDtypeMismatch {
                expected: expected_dtype,
                actual: dtype,
            }
            .into());
        }
        let semantic_epoch = MultiVectorEpochId::new(reader.read_array::<32>()?);
        if semantic_epoch != expected_epoch {
            return Err(artifact_error(
                "matrix artifact semantic epoch mismatch".to_string(),
            ));
        }
        let source_fragment_checksum = reader.read_u64()?;
        if source_fragment_checksum != expected_source_fragment_checksum {
            return Err(artifact_error(
                "matrix artifact source-fragment checksum mismatch".to_string(),
            ));
        }
        let vector_dimension = usize::try_from(reader.read_u32()?)
            .map_err(|_| artifact_error("matrix dimension exceeds usize".to_string()))?;
        if vector_dimension != expected_vector_dimension {
            return Err(artifact_error(format!(
                "matrix artifact dimension mismatch: expected {expected_vector_dimension}, got {vector_dimension}"
            )));
        }
        dtype.validate_for_dimension(
            u32::try_from(vector_dimension)
                .map_err(|_| artifact_error("matrix dimension exceeds u32".to_string()))?,
        )?;
        let row_count = usize::try_from(reader.read_u32()?)
            .map_err(|_| artifact_error("matrix row count exceeds usize".to_string()))?;
        if row_count == 0 || row_count > max_rows {
            return Err(artifact_error(format!(
                "matrix artifact row count {row_count} is outside 1..={max_rows}"
            )));
        }
        let total_vectors = reader.read_u64()?;
        let mut directory = Vec::new();
        directory.try_reserve_exact(row_count).map_err(|error| {
            artifact_error(format!("matrix directory allocation failed: {error}"))
        })?;
        let mut expected_offset = 0_u64;
        for _ in 0..row_count {
            let content_hash = ContentHash::new(reader.read_array::<32>()?);
            let vector_count = usize::try_from(reader.read_u32()?)
                .map_err(|_| artifact_error("matrix vector count exceeds usize".to_string()))?;
            let vector_offset = reader.read_u64()?;
            if vector_offset != expected_offset {
                return Err(artifact_error(
                    "matrix artifact vector offsets are not canonical".to_string(),
                ));
            }
            if vector_count == 0 || vector_count > max_vectors_per_row {
                return Err(artifact_error(format!(
                    "matrix row vector count {vector_count} is outside 1..={max_vectors_per_row}"
                )));
            }
            expected_offset =
                expected_offset
                    .checked_add(u64::try_from(vector_count).map_err(|_| {
                        artifact_error("matrix vector count exceeds u64".to_string())
                    })?)
                    .ok_or_else(|| artifact_error("matrix vector offset overflows".to_string()))?;
            directory.push((content_hash, vector_count));
        }
        if expected_offset != total_vectors {
            return Err(artifact_error(
                "matrix artifact total vector count disagrees with directory".to_string(),
            ));
        }
        let required_payload = usize::try_from(total_vectors)
            .map_err(|_| artifact_error("matrix vector count exceeds usize".to_string()))?
            .checked_mul(matrix_bytes_per_vector(dtype, vector_dimension)?)
            .ok_or_else(|| artifact_error("matrix payload length overflows".to_string()))?;
        if reader.remaining() != required_payload {
            return Err(artifact_error(format!(
                "matrix payload length mismatch: expected {required_payload}, got {}",
                reader.remaining()
            )));
        }
        let mut rows = Vec::new();
        rows.try_reserve_exact(row_count)
            .map_err(|error| artifact_error(format!("matrix row allocation failed: {error}")))?;
        for (content_hash, vector_count) in directory {
            let row_scalar_count = vector_count
                .checked_mul(vector_dimension)
                .ok_or_else(|| artifact_error("matrix row scalar count overflows".to_string()))?;
            let mut values = Vec::new();
            values
                .try_reserve_exact(row_scalar_count)
                .map_err(|error| {
                    artifact_error(format!("matrix scalar allocation failed: {error}"))
                })?;
            match dtype {
                MatrixDtype::F16 => {
                    for _ in 0..row_scalar_count {
                        values.push(f16_bits_to_f32(reader.read_u16()?)?);
                    }
                }
                MatrixDtype::Int8SymV1 { group_size } => {
                    for _ in 0..vector_count {
                        decode_int8_sym_v1_vector(
                            &mut reader,
                            vector_dimension,
                            usize::from(group_size),
                            &mut values,
                        )?;
                    }
                }
            }
            rows.push(MatrixArtifactRow::new(
                content_hash,
                MultiVectorEmbedding::new(
                    values,
                    vector_count,
                    vector_dimension,
                    max_vectors_per_row,
                )?,
            ));
        }
        if reader.remaining() != 0 {
            return Err(artifact_error(
                "matrix artifact contains trailing bytes".to_string(),
            ));
        }
        Self::new(
            dtype,
            semantic_epoch,
            source_fragment_checksum,
            vector_dimension,
            rows,
        )
    }
}

/// One content-bound f32 FDE vector within an FDE artifact.
#[derive(Clone, Debug, PartialEq)]
pub struct FdeArtifactRow {
    content_hash: ContentHash,
    values: Vec<f32>,
}

impl FdeArtifactRow {
    /// Validate and bind one FDE vector to its source content identity.
    pub fn new(
        content_hash: ContentHash,
        values: Vec<f32>,
        expected_dimension: usize,
    ) -> Result<Self> {
        validate_f32_vector(&values, expected_dimension, "FDE")?;
        Ok(Self {
            content_hash,
            values,
        })
    }

    /// Return the exact source content identity.
    #[must_use]
    pub const fn content_hash(&self) -> ContentHash {
        self.content_hash
    }

    /// Borrow the complete f32 FDE vector.
    #[must_use]
    pub fn values(&self) -> &[f32] {
        &self.values
    }
}

/// One deterministic fragment of f32 fixed-dimensional encodings.
#[derive(Clone, Debug, PartialEq)]
pub struct FdeArtifact {
    generation: FdeGenerationId,
    embedding_fragment_checksum: ArtifactChecksum,
    fde_dimension: usize,
    rows: Vec<FdeArtifactRow>,
}

impl FdeArtifact {
    /// Validate and construct an ordered FDE fragment.
    pub fn new(
        generation: FdeGenerationId,
        embedding_fragment_checksum: ArtifactChecksum,
        fde_dimension: usize,
        rows: Vec<FdeArtifactRow>,
    ) -> Result<Self> {
        if rows.is_empty() {
            return Err(artifact_error(
                "FDE artifact must contain at least one row".to_string(),
            ));
        }
        for row in &rows {
            validate_f32_vector(&row.values, fde_dimension, "FDE")?;
        }
        Ok(Self {
            generation,
            embedding_fragment_checksum,
            fde_dimension,
            rows,
        })
    }

    /// Return the immutable FDE generation.
    #[must_use]
    pub const fn generation(&self) -> FdeGenerationId {
        self.generation
    }

    /// Return the exact matrix-fragment checksum used as input.
    #[must_use]
    pub const fn embedding_fragment_checksum(&self) -> ArtifactChecksum {
        self.embedding_fragment_checksum
    }

    /// Return the common f32 FDE dimension.
    #[must_use]
    pub const fn fde_dimension(&self) -> usize {
        self.fde_dimension
    }

    /// Borrow rows in their deterministic source order.
    #[must_use]
    pub fn rows(&self) -> &[FdeArtifactRow] {
        &self.rows
    }

    /// Encode deterministic, content-addressed f32 FDE bytes.
    pub fn to_bytes(&self) -> Result<ImmutableArtifactBytes> {
        let fde_dimension = u32::try_from(self.fde_dimension)
            .map_err(|_| artifact_error("FDE dimension exceeds u32".to_string()))?;
        let row_count = u32::try_from(self.rows.len())
            .map_err(|_| artifact_error("FDE row count exceeds u32".to_string()))?;
        let payload_scalars = self
            .rows
            .len()
            .checked_mul(self.fde_dimension)
            .ok_or_else(|| artifact_error("FDE scalar count overflows".to_string()))?;
        let payload_bytes = payload_scalars
            .checked_mul(size_of::<f32>())
            .ok_or_else(|| artifact_error("FDE payload byte count overflows".to_string()))?;
        let directory_bytes = self
            .rows
            .len()
            .checked_mul(32)
            .ok_or_else(|| artifact_error("FDE directory byte count overflows".to_string()))?;
        let capacity = fde_header_len()
            .checked_add(directory_bytes)
            .and_then(|length| length.checked_add(payload_bytes))
            .ok_or_else(|| artifact_error("FDE artifact byte count overflows".to_string()))?;
        let mut bytes = BytesMut::with_capacity(capacity);
        bytes.extend_from_slice(FDE_MAGIC);
        bytes.put_u8(FDE_ARTIFACT_FORMAT_VERSION);
        bytes.put_u8(F32_LE_ENCODING);
        bytes.extend_from_slice(self.generation.as_bytes());
        bytes.extend_from_slice(self.embedding_fragment_checksum.as_bytes());
        bytes.put_u32_le(fde_dimension);
        bytes.put_u32_le(row_count);
        for row in &self.rows {
            validate_f32_vector(&row.values, self.fde_dimension, "FDE")?;
            bytes.extend_from_slice(row.content_hash.as_bytes());
        }
        for row in &self.rows {
            for value in &row.values {
                bytes.put_f32_le(*value);
            }
        }
        Ok(ImmutableArtifactBytes::new(bytes.freeze()))
    }

    /// Decode and validate complete f32 FDE bytes against caller authority.
    #[allow(clippy::too_many_arguments)]
    pub fn from_bytes(
        bytes: &[u8],
        expected_checksum: ArtifactChecksum,
        expected_generation: FdeGenerationId,
        expected_embedding_fragment_checksum: ArtifactChecksum,
        expected_fde_dimension: usize,
        max_rows: usize,
    ) -> Result<Self> {
        verify_checksum(bytes, expected_checksum, "FDE")?;
        let mut reader = ArtifactReader::new(bytes, "FDE");
        reader.expect_magic(FDE_MAGIC)?;
        reader.expect_u8(
            FDE_ARTIFACT_FORMAT_VERSION,
            "unsupported FDE artifact version",
        )?;
        reader.expect_u8(F32_LE_ENCODING, "unsupported FDE scalar encoding")?;
        let generation = FdeGenerationId::new(reader.read_array::<32>()?);
        if generation != expected_generation {
            return Err(artifact_error(
                "FDE artifact generation mismatch".to_string(),
            ));
        }
        let embedding_fragment_checksum = ArtifactChecksum::new(reader.read_array::<32>()?);
        if embedding_fragment_checksum != expected_embedding_fragment_checksum {
            return Err(artifact_error(
                "FDE artifact embedding-fragment checksum mismatch".to_string(),
            ));
        }
        let fde_dimension = usize::try_from(reader.read_u32()?)
            .map_err(|_| artifact_error("FDE dimension exceeds usize".to_string()))?;
        if fde_dimension != expected_fde_dimension {
            return Err(artifact_error(format!(
                "FDE artifact dimension mismatch: expected {expected_fde_dimension}, got {fde_dimension}"
            )));
        }
        let row_count = usize::try_from(reader.read_u32()?)
            .map_err(|_| artifact_error("FDE row count exceeds usize".to_string()))?;
        if row_count == 0 || row_count > max_rows {
            return Err(artifact_error(format!(
                "FDE artifact row count {row_count} is outside 1..={max_rows}"
            )));
        }
        let payload_bytes = row_count
            .checked_mul(fde_dimension)
            .and_then(|count| count.checked_mul(size_of::<f32>()))
            .ok_or_else(|| artifact_error("FDE payload byte count overflows".to_string()))?;
        let directory_bytes = row_count
            .checked_mul(32)
            .ok_or_else(|| artifact_error("FDE directory byte count overflows".to_string()))?;
        let expected_remaining = directory_bytes
            .checked_add(payload_bytes)
            .ok_or_else(|| artifact_error("FDE remaining byte count overflows".to_string()))?;
        if reader.remaining() != expected_remaining {
            return Err(artifact_error(format!(
                "FDE payload length mismatch: expected {expected_remaining}, got {}",
                reader.remaining()
            )));
        }
        let mut content_hashes = Vec::new();
        content_hashes
            .try_reserve_exact(row_count)
            .map_err(|error| artifact_error(format!("FDE directory allocation failed: {error}")))?;
        for _ in 0..row_count {
            content_hashes.push(ContentHash::new(reader.read_array::<32>()?));
        }
        let mut rows = Vec::new();
        rows.try_reserve_exact(row_count)
            .map_err(|error| artifact_error(format!("FDE row allocation failed: {error}")))?;
        for content_hash in content_hashes {
            let mut values = Vec::new();
            values.try_reserve_exact(fde_dimension).map_err(|error| {
                artifact_error(format!("FDE scalar allocation failed: {error}"))
            })?;
            for _ in 0..fde_dimension {
                let value = reader.read_f32()?;
                if !value.is_finite() {
                    return Err(artifact_error(
                        "FDE artifact contains a non-finite value".to_string(),
                    ));
                }
                values.push(value);
            }
            rows.push(FdeArtifactRow::new(content_hash, values, fde_dimension)?);
        }
        if reader.remaining() != 0 {
            return Err(artifact_error(
                "FDE artifact contains trailing bytes".to_string(),
            ));
        }
        Self::new(generation, embedding_fragment_checksum, fde_dimension, rows)
    }
}

/// One immutable f32 candidate-centering mean.
#[derive(Clone, Debug, PartialEq)]
pub struct CenteringArtifact {
    values: Vec<f32>,
}

impl CenteringArtifact {
    /// Validate and take ownership of one non-empty finite mean vector.
    pub fn new(values: Vec<f32>) -> Result<Self> {
        let dimension = values.len();
        validate_f32_vector(&values, dimension, "centering")?;
        Ok(Self { values })
    }

    /// Borrow the complete f32 mean vector.
    #[must_use]
    pub fn values(&self) -> &[f32] {
        &self.values
    }

    /// Return the number of f32 coordinates.
    #[must_use]
    pub fn vector_dimension(&self) -> usize {
        self.values.len()
    }

    /// Encode deterministic, content-addressed f32 centering bytes.
    pub fn to_bytes(&self) -> Result<ImmutableArtifactBytes> {
        let dimension = u32::try_from(self.values.len())
            .map_err(|_| artifact_error("centering dimension exceeds u32".to_string()))?;
        validate_f32_vector(&self.values, self.values.len(), "centering")?;
        let payload_bytes = self
            .values
            .len()
            .checked_mul(size_of::<f32>())
            .ok_or_else(|| artifact_error("centering payload byte count overflows".to_string()))?;
        let capacity = centering_header_len()
            .checked_add(payload_bytes)
            .ok_or_else(|| artifact_error("centering artifact byte count overflows".to_string()))?;
        let mut bytes = BytesMut::with_capacity(capacity);
        bytes.extend_from_slice(CENTERING_MAGIC);
        bytes.put_u8(CENTERING_ARTIFACT_FORMAT_VERSION);
        bytes.put_u8(F32_LE_ENCODING);
        bytes.put_u32_le(dimension);
        for value in &self.values {
            bytes.put_f32_le(*value);
        }
        Ok(ImmutableArtifactBytes::new(bytes.freeze()))
    }

    /// Decode and validate complete f32 centering bytes.
    pub fn from_bytes(
        bytes: &[u8],
        expected_checksum: ArtifactChecksum,
        expected_dimension: usize,
    ) -> Result<Self> {
        verify_checksum(bytes, expected_checksum, "centering")?;
        let mut reader = ArtifactReader::new(bytes, "centering");
        reader.expect_magic(CENTERING_MAGIC)?;
        reader.expect_u8(
            CENTERING_ARTIFACT_FORMAT_VERSION,
            "unsupported centering artifact version",
        )?;
        reader.expect_u8(F32_LE_ENCODING, "unsupported centering scalar encoding")?;
        let dimension = usize::try_from(reader.read_u32()?)
            .map_err(|_| artifact_error("centering dimension exceeds usize".to_string()))?;
        if dimension != expected_dimension {
            return Err(artifact_error(format!(
                "centering dimension mismatch: expected {expected_dimension}, got {dimension}"
            )));
        }
        let payload_bytes = dimension
            .checked_mul(size_of::<f32>())
            .ok_or_else(|| artifact_error("centering payload length overflows".to_string()))?;
        if reader.remaining() != payload_bytes {
            return Err(artifact_error(format!(
                "centering payload length mismatch: expected {payload_bytes}, got {}",
                reader.remaining()
            )));
        }
        let mut values = Vec::new();
        values.try_reserve_exact(dimension).map_err(|error| {
            artifact_error(format!("centering value allocation failed: {error}"))
        })?;
        for _ in 0..dimension {
            let value = reader.read_f32()?;
            if !value.is_finite() {
                return Err(artifact_error(
                    "centering artifact contains a non-finite value".to_string(),
                ));
            }
            values.push(value);
        }
        Self::new(values)
    }
}

fn validate_matrix_rows(vector_dimension: usize, rows: &[MatrixArtifactRow]) -> Result<()> {
    if vector_dimension == 0 {
        return Err(artifact_error(
            "matrix artifact vector dimension must be positive".to_string(),
        ));
    }
    if rows.is_empty() {
        return Err(artifact_error(
            "matrix artifact must contain at least one row".to_string(),
        ));
    }
    for row in rows {
        if row.embedding.vector_dimension() != vector_dimension {
            return Err(artifact_error(format!(
                "matrix artifact dimension mismatch: expected {vector_dimension}, got {}",
                row.embedding.vector_dimension()
            )));
        }
        row.embedding.matrix_ref()?;
    }
    Ok(())
}

fn matrix_dtype_header(dtype: MatrixDtype) -> (u8, u16) {
    match dtype {
        MatrixDtype::F16 => (MATRIX_DTYPE_F16, 0),
        MatrixDtype::Int8SymV1 { group_size } => (MATRIX_DTYPE_INT8_SYM_V1, group_size),
    }
}

fn matrix_dtype_from_header(discriminant: u8, group_size: u16) -> Result<MatrixDtype> {
    match discriminant {
        MATRIX_DTYPE_F16 if group_size == 0 => Ok(MatrixDtype::F16),
        MATRIX_DTYPE_F16 => Err(EmbeddingArtifactError::InvalidMatrixDtypeHeader {
            reason: format!("f16 requires group size 0, got {group_size}"),
        }
        .into()),
        MATRIX_DTYPE_INT8_SYM_V1 if matches!(group_size, 16 | 32 | 128) => {
            Ok(MatrixDtype::Int8SymV1 { group_size })
        }
        MATRIX_DTYPE_INT8_SYM_V1 => Err(EmbeddingArtifactError::InvalidMatrixDtypeHeader {
            reason: format!(
                "int8_sym_v1 group size must be one of 16, 32, or 128, got {group_size}"
            ),
        }
        .into()),
        discriminant => Err(EmbeddingArtifactError::UnsupportedMatrixDtype { discriminant }.into()),
    }
}

fn matrix_bytes_per_vector(dtype: MatrixDtype, vector_dimension: usize) -> Result<usize> {
    dtype.validate_for_dimension(
        u32::try_from(vector_dimension)
            .map_err(|_| artifact_error("matrix dimension exceeds u32".to_string()))?,
    )?;
    match dtype {
        MatrixDtype::F16 => vector_dimension
            .checked_mul(size_of::<u16>())
            .ok_or_else(|| artifact_error("f16 matrix row byte count overflows".to_string())),
        MatrixDtype::Int8SymV1 { group_size } => {
            let scale_count = vector_dimension / usize::from(group_size);
            vector_dimension
                .checked_add(scale_count.checked_mul(size_of::<u16>()).ok_or_else(|| {
                    artifact_error("int8 matrix scale byte count overflows".to_string())
                })?)
                .ok_or_else(|| artifact_error("int8 matrix row byte count overflows".to_string()))
        }
    }
}

struct EncodedInt8Vector {
    codes: Vec<i8>,
    folded_scale_bits: Vec<u16>,
}

fn encode_int8_sym_v1_vector(values: &[f32], group_size: usize) -> Result<EncodedInt8Vector> {
    if values.is_empty() || values.len() % group_size != 0 {
        return Err(artifact_error(
            "int8_sym_v1 vector shape is not divisible by its group size".to_string(),
        ));
    }

    let mut f16_values = Vec::new();
    f16_values
        .try_reserve_exact(values.len())
        .map_err(|error| artifact_error(format!("int8 f16 row allocation failed: {error}")))?;
    for value in values {
        f16_values.push(f16_bits_to_f32(f32_to_f16_bits(*value)?)?);
    }

    let group_count = values.len() / group_size;
    let mut codes = Vec::new();
    codes
        .try_reserve_exact(values.len())
        .map_err(|error| artifact_error(format!("int8 code allocation failed: {error}")))?;
    let mut base_scales = Vec::new();
    base_scales
        .try_reserve_exact(group_count)
        .map_err(|error| artifact_error(format!("int8 scale allocation failed: {error}")))?;
    let mut norm_squared = 0.0_f64;

    for group in f16_values.chunks_exact(group_size) {
        let max_abs = group
            .iter()
            .map(|value| value.abs())
            .fold(0.0_f32, f32::max);
        if max_abs == 0.0 {
            codes.extend(std::iter::repeat_n(0_i8, group_size));
            base_scales.push(0.0);
            continue;
        }

        let base_scale = f16_bits_to_f32(f32_to_f16_bits(max_abs / 127.0)?)?;
        if base_scale == 0.0 {
            return Err(artifact_error(
                "non-zero int8_sym_v1 group scale rounded to zero in f16".to_string(),
            ));
        }
        for value in group {
            let rounded = round_ties_away(*value / base_scale);
            let code = rounded.clamp(-127.0, 127.0) as i8;
            let reconstructed = f32::from(code) * base_scale;
            norm_squared += f64::from(reconstructed) * f64::from(reconstructed);
            codes.push(code);
        }
        base_scales.push(base_scale);
    }

    if !norm_squared.is_finite() || norm_squared == 0.0 {
        return Err(artifact_error(
            "int8_sym_v1 row reconstruction has invalid zero or non-finite norm".to_string(),
        ));
    }
    let norm = norm_squared.sqrt();
    let mut folded_scale_bits = Vec::new();
    folded_scale_bits
        .try_reserve_exact(group_count)
        .map_err(|error| artifact_error(format!("folded scale allocation failed: {error}")))?;
    for scale in base_scales {
        if scale == 0.0 {
            folded_scale_bits.push(0);
            continue;
        }
        let folded = (f64::from(scale) / norm) as f32;
        let bits = f32_to_f16_bits(folded)?;
        if bits == 0 {
            return Err(artifact_error(
                "non-zero int8_sym_v1 folded scale rounded to zero in f16".to_string(),
            ));
        }
        folded_scale_bits.push(bits);
    }

    Ok(EncodedInt8Vector {
        codes,
        folded_scale_bits,
    })
}

fn decode_int8_sym_v1_vector(
    reader: &mut ArtifactReader<'_>,
    vector_dimension: usize,
    group_size: usize,
    values: &mut Vec<f32>,
) -> Result<()> {
    let mut codes = Vec::new();
    codes
        .try_reserve_exact(vector_dimension)
        .map_err(|error| artifact_error(format!("int8 code allocation failed: {error}")))?;
    for _ in 0..vector_dimension {
        codes.push(reader.read_u8()? as i8);
    }

    for group in codes.chunks_exact(group_size) {
        let scale_bits = reader.read_u16()?;
        if scale_bits & 0x8000 != 0 {
            return Err(artifact_error(
                "int8_sym_v1 stored scale must be non-negative".to_string(),
            ));
        }
        let scale = f16_bits_to_f32(scale_bits)?;
        if scale == 0.0 && group.iter().any(|code| *code != 0) {
            return Err(artifact_error(
                "int8_sym_v1 zero scale has non-zero coordinates".to_string(),
            ));
        }
        for code in group {
            values.push(f32::from(*code) * scale);
        }
    }
    Ok(())
}

fn round_ties_away(value: f32) -> f32 {
    value.round()
}

fn validate_f32_vector(values: &[f32], expected_dimension: usize, label: &str) -> Result<()> {
    if expected_dimension == 0 {
        return Err(artifact_error(format!(
            "{label} vector dimension must be positive"
        )));
    }
    if values.len() != expected_dimension {
        return Err(artifact_error(format!(
            "{label} vector length mismatch: expected {expected_dimension}, got {}",
            values.len()
        )));
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err(artifact_error(format!(
            "{label} vector contains a non-finite value"
        )));
    }
    Ok(())
}

fn verify_checksum(bytes: &[u8], expected: ArtifactChecksum, label: &str) -> Result<()> {
    let actual = ArtifactChecksum::digest(bytes);
    if actual != expected {
        return Err(artifact_error(format!(
            "{label} artifact checksum mismatch: expected {}, got {}",
            expected.to_hex(),
            actual.to_hex()
        )));
    }
    Ok(())
}

fn f32_to_f16_bits(value: f32) -> Result<u16> {
    if !value.is_finite() {
        return Err(artifact_error(
            "matrix artifact cannot encode a non-finite value".to_string(),
        ));
    }
    let bits = value.to_bits();
    let sign = ((bits >> 16) & 0x8000) as u16;
    let exponent = ((bits >> 23) & 0xff) as i32;
    let mantissa = bits & 0x007f_ffff;
    let half_exponent = exponent - 127 + 15;
    let magnitude = if half_exponent >= 31 {
        return Err(artifact_error(
            "matrix value exceeds finite f16 range".to_string(),
        ));
    } else if half_exponent <= 0 {
        if half_exponent < -10 {
            0_u16
        } else {
            let significand = mantissa | 0x0080_0000;
            let shift = u32::try_from(14 - half_exponent)
                .map_err(|_| artifact_error("invalid f16 subnormal shift".to_string()))?;
            let truncated = significand >> shift;
            let remainder_mask = (1_u32 << shift) - 1;
            let remainder = significand & remainder_mask;
            let halfway = 1_u32 << (shift - 1);
            let rounded = truncated
                + u32::from(remainder > halfway || (remainder == halfway && truncated & 1 == 1));
            u16::try_from(rounded)
                .map_err(|_| artifact_error("f16 subnormal rounding overflowed".to_string()))?
        }
    } else {
        let mut rounded = (u32::try_from(half_exponent)
            .map_err(|_| artifact_error("invalid f16 exponent".to_string()))?
            << 10)
            | (mantissa >> 13);
        let remainder = mantissa & 0x1fff;
        rounded += u32::from(remainder > 0x1000 || (remainder == 0x1000 && rounded & 1 == 1));
        if rounded >= 0x7c00 {
            return Err(artifact_error(
                "matrix value rounds outside finite f16 range".to_string(),
            ));
        }
        u16::try_from(rounded).map_err(|_| artifact_error("f16 rounding overflowed".to_string()))?
    };
    Ok(sign | magnitude)
}

fn f16_bits_to_f32(bits: u16) -> Result<f32> {
    let sign = (u32::from(bits & 0x8000)) << 16;
    let exponent = (bits >> 10) & 0x1f;
    let fraction = bits & 0x03ff;
    let value_bits = match exponent {
        0 if fraction == 0 => sign,
        0 => {
            let mut normalized = u32::from(fraction);
            let mut unbiased_exponent = -14_i32;
            while normalized & 0x0400 == 0 {
                normalized <<= 1;
                unbiased_exponent -= 1;
            }
            normalized &= 0x03ff;
            sign | (u32::try_from(unbiased_exponent + 127)
                .map_err(|_| artifact_error("invalid f16 subnormal exponent".to_string()))?
                << 23)
                | (normalized << 13)
        }
        31 => {
            return Err(artifact_error(
                "matrix artifact contains a non-finite f16 value".to_string(),
            ))
        }
        _ => sign | ((u32::from(exponent) + 112) << 23) | (u32::from(fraction) << 13),
    };
    let value = f32::from_bits(value_bits);
    if !value.is_finite() {
        return Err(artifact_error(
            "matrix artifact contains a non-finite decoded value".to_string(),
        ));
    }
    Ok(value)
}

const fn matrix_header_len() -> usize {
    4 + 1 + 1 + size_of::<u16>() + 32 + size_of::<u64>() + 2 * size_of::<u32>() + size_of::<u64>()
}

const fn fde_header_len() -> usize {
    4 + 1 + 1 + 32 + 32 + 2 * size_of::<u32>()
}

const fn centering_header_len() -> usize {
    4 + 1 + 1 + size_of::<u32>()
}

fn artifact_error(reason: String) -> ZeppelinError {
    ZeppelinError::Serialization(format!("invalid late-interaction artifact: {reason}"))
}

struct ArtifactReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
    label: &'static str,
}

impl<'a> ArtifactReader<'a> {
    const fn new(bytes: &'a [u8], label: &'static str) -> Self {
        Self {
            bytes,
            cursor: 0,
            label,
        }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.cursor
    }

    fn expect_magic(&mut self, expected: &[u8]) -> Result<()> {
        if self.read_exact(expected.len())? != expected {
            return Err(artifact_error(format!("bad {} artifact magic", self.label)));
        }
        Ok(())
    }

    fn expect_u8(&mut self, expected: u8, reason: &str) -> Result<()> {
        if self.read_u8()? != expected {
            return Err(artifact_error(reason.to_string()));
        }
        Ok(())
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .cursor
            .checked_add(length)
            .ok_or_else(|| artifact_error(format!("{} read offset overflows", self.label)))?;
        if end > self.bytes.len() {
            return Err(artifact_error(format!(
                "{} artifact is truncated",
                self.label
            )));
        }
        let value = &self.bytes[self.cursor..end];
        self.cursor = end;
        Ok(value)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.read_exact(N)?
            .try_into()
            .map_err(|_| artifact_error(format!("{} fixed field is truncated", self.label)))
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }

    fn read_f32(&mut self) -> Result<f32> {
        Ok(f32::from_bits(self.read_u32()?))
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::{
        matrix_header_len, round_ties_away, CenteringArtifact, EmbeddingArtifactError, FdeArtifact,
        FdeArtifactRow, MatrixArtifact, MatrixArtifactRow, MATRIX_MAGIC,
    };
    use crate::embedding::{
        ArtifactChecksum, ContentHash, FdeGenerationId, MatrixDtype, MultiVectorEmbedding,
        MultiVectorEpochId,
    };
    use crate::error::ZeppelinError;

    #[test]
    fn f16_matrix_artifact_is_deterministic_and_round_trips() {
        let artifact = matrix_artifact();
        let first = artifact.to_bytes().expect("first matrix bytes");
        let second = artifact.to_bytes().expect("second matrix bytes");
        assert_eq!(first, second);
        assert_eq!(&first.bytes()[..4], MATRIX_MAGIC);
        first
            .validate_existing(second.bytes())
            .expect("identical create-only retry");

        let decoded = MatrixArtifact::from_bytes(
            first.bytes(),
            first.checksum(),
            MatrixDtype::F16,
            artifact.semantic_epoch(),
            artifact.source_fragment_checksum(),
            artifact.vector_dimension(),
            8,
            16,
        )
        .expect("matrix round trip");
        assert_eq!(decoded, artifact);
    }

    #[test]
    fn matrix_artifact_rejects_corruption_and_non_finite_f16() {
        let encoded = matrix_artifact().to_bytes().expect("matrix bytes");
        let mut corrupt = encoded.bytes().to_vec();
        corrupt[0] ^= 0xff;
        let checksum = ArtifactChecksum::digest(&corrupt);
        let error = MatrixArtifact::from_bytes(
            &corrupt,
            checksum,
            MatrixDtype::F16,
            MultiVectorEpochId::new([3; 32]),
            19,
            2,
            8,
            16,
        )
        .expect_err("bad magic");
        assert!(error.to_string().contains("magic"));

        let mut non_finite = encoded.bytes().to_vec();
        let payload_start = non_finite.len() - 8;
        non_finite[payload_start..payload_start + 2].copy_from_slice(&0x7c00_u16.to_le_bytes());
        let checksum = ArtifactChecksum::digest(&non_finite);
        let error = MatrixArtifact::from_bytes(
            &non_finite,
            checksum,
            MatrixDtype::F16,
            MultiVectorEpochId::new([3; 32]),
            19,
            2,
            8,
            16,
        )
        .expect_err("non-finite f16");
        assert!(error.to_string().contains("non-finite"));
    }

    #[test]
    fn int8_sym_v1_golden_bytes_round_trip_and_pin_ties_away() {
        assert_eq!(round_ties_away(0.5), 1.0);
        assert_eq!(round_ties_away(-0.5), -1.0);
        assert_eq!(round_ties_away(1.5), 2.0);
        assert_eq!(round_ties_away(-1.5), -2.0);

        let mut values = (0..128)
            .map(|index| (index as f32 - 63.5) / 64.0)
            .collect::<Vec<_>>();
        let norm = values
            .iter()
            .map(|value| f64::from(*value) * f64::from(*value))
            .sum::<f64>()
            .sqrt();
        for value in &mut values {
            *value = (f64::from(*value) / norm) as f32;
        }
        let artifact = one_vector_matrix(MatrixDtype::Int8SymV1 { group_size: 16 }, values);
        let encoded = artifact.to_bytes().expect("golden int8 bytes");
        assert_eq!(
            bytes_hex(encoded.bytes()),
            "5a4d4531010210000303030303030303030303030303030303030303030303030303030303030303130000000000000080000000010000000100000000000000010101010101010101010101010101010101010101010101010101010101010101000000000000000000000081838587898b8d8f91939597999b9d9f818486898c8e919496999c9ea1a4a6a98185898d9195999da1a5a9adb1b5b9be8189919aa2aab2bac3cbd3dbe3ecf4fc040c141d252d353d464e565e666f777f42474b4f53575b5f63676b6f73777b7f575a5c5f6264676a6c6f7274777a7c7f61636567696b6d6f71737577797b7d7fe5145413dd10c90cc90cdd105413e514"
        );

        let decoded = MatrixArtifact::from_bytes(
            encoded.bytes(),
            encoded.checksum(),
            MatrixDtype::Int8SymV1 { group_size: 16 },
            artifact.semantic_epoch(),
            artifact.source_fragment_checksum(),
            artifact.vector_dimension(),
            8,
            16,
        )
        .expect("int8 golden round trip");
        assert_eq!(decoded.dtype(), MatrixDtype::Int8SymV1 { group_size: 16 });
        assert_eq!(decoded.rows().len(), 1);
        assert_eq!(decoded.rows()[0].embedding().values().len(), 128);
    }

    #[test]
    fn int8_sym_v1_zero_groups_are_exact() {
        let mut values = vec![0.0_f32; 128];
        values[..16].fill(0.25);
        let artifact = one_vector_matrix(MatrixDtype::Int8SymV1 { group_size: 16 }, values);
        let encoded = artifact.to_bytes().expect("zero-group int8 bytes");
        let decoded = MatrixArtifact::from_bytes(
            encoded.bytes(),
            encoded.checksum(),
            artifact.dtype(),
            artifact.semantic_epoch(),
            artifact.source_fragment_checksum(),
            artifact.vector_dimension(),
            8,
            16,
        )
        .expect("zero-group decode");
        assert!(decoded.rows()[0].embedding().values()[16..]
            .iter()
            .all(|value| *value == 0.0));

        let payload_start = matrix_header_len() + 32 + size_of::<u32>() + size_of::<u64>();
        let scales_start = payload_start + 128;
        for group in 1..8 {
            let offset = scales_start + group * size_of::<u16>();
            assert_eq!(&encoded.bytes()[offset..offset + 2], &[0, 0]);
        }
    }

    #[test]
    fn matrix_dtype_header_is_authoritative_and_unknown_tags_are_typed() {
        let encoded = matrix_artifact().to_bytes().expect("f16 bytes");
        let mismatch = MatrixArtifact::from_bytes(
            encoded.bytes(),
            encoded.checksum(),
            MatrixDtype::Int8SymV1 { group_size: 16 },
            MultiVectorEpochId::new([3; 32]),
            19,
            2,
            8,
            16,
        )
        .expect_err("section/header dtype mismatch");
        assert!(matches!(
            mismatch,
            ZeppelinError::EmbeddingArtifact(EmbeddingArtifactError::MatrixDtypeMismatch {
                expected: MatrixDtype::Int8SymV1 { group_size: 16 },
                actual: MatrixDtype::F16,
            })
        ));

        let mut unknown = encoded.bytes().to_vec();
        unknown[5] = 0xff;
        let checksum = ArtifactChecksum::digest(&unknown);
        let error = MatrixArtifact::from_bytes(
            &unknown,
            checksum,
            MatrixDtype::F16,
            MultiVectorEpochId::new([3; 32]),
            19,
            2,
            8,
            16,
        )
        .expect_err("unknown dtype");
        assert!(matches!(
            error,
            ZeppelinError::EmbeddingArtifact(EmbeddingArtifactError::UnsupportedMatrixDtype {
                discriminant: 0xff
            })
        ));
    }

    #[test]
    fn f32_fde_artifact_round_trips_and_binds_matrix_checksum() {
        let matrix_checksum = ArtifactChecksum::digest(b"matrix");
        let generation = FdeGenerationId::new([7; 32]);
        let artifact = FdeArtifact::new(
            generation,
            matrix_checksum,
            3,
            vec![
                FdeArtifactRow::new(ContentHash::new([1; 32]), vec![1.0, -2.0, 3.5], 3)
                    .expect("first FDE"),
                FdeArtifactRow::new(ContentHash::new([2; 32]), vec![0.25, 0.5, 0.75], 3)
                    .expect("second FDE"),
            ],
        )
        .expect("FDE artifact");
        let encoded = artifact.to_bytes().expect("FDE bytes");
        let decoded = FdeArtifact::from_bytes(
            encoded.bytes(),
            encoded.checksum(),
            generation,
            matrix_checksum,
            3,
            8,
        )
        .expect("FDE round trip");
        assert_eq!(decoded, artifact);

        let error = FdeArtifact::from_bytes(
            encoded.bytes(),
            encoded.checksum(),
            generation,
            ArtifactChecksum::digest(b"other matrix"),
            3,
            8,
        )
        .expect_err("matrix checksum mismatch");
        assert!(error.to_string().contains("embedding-fragment"));
    }

    #[test]
    fn f32_centering_artifact_round_trips_and_rejects_wrong_dimension() {
        let artifact = CenteringArtifact::new(vec![0.25, -0.5, 1.0]).expect("centering");
        let encoded = artifact.to_bytes().expect("centering bytes");
        let decoded = CenteringArtifact::from_bytes(encoded.bytes(), encoded.checksum(), 3)
            .expect("centering round trip");
        assert_eq!(decoded, artifact);
        assert!(CenteringArtifact::from_bytes(encoded.bytes(), encoded.checksum(), 4).is_err());
    }

    #[test]
    fn create_only_validation_rejects_different_bytes() {
        let first = CenteringArtifact::new(vec![1.0])
            .expect("first centering")
            .to_bytes()
            .expect("first bytes");
        let second = CenteringArtifact::new(vec![2.0])
            .expect("second centering")
            .to_bytes()
            .expect("second bytes");
        assert!(first.validate_existing(second.bytes()).is_err());
    }

    fn matrix_artifact() -> MatrixArtifact {
        MatrixArtifact::new(
            MatrixDtype::F16,
            MultiVectorEpochId::new([3; 32]),
            19,
            2,
            vec![
                MatrixArtifactRow::new(
                    ContentHash::new([1; 32]),
                    MultiVectorEmbedding::new(vec![0.5, -1.0, 0.25, 2.0], 2, 2, 16)
                        .expect("first matrix"),
                ),
                MatrixArtifactRow::new(
                    ContentHash::new([2; 32]),
                    MultiVectorEmbedding::new(vec![0.0, -0.0], 1, 2, 16).expect("second matrix"),
                ),
            ],
        )
        .expect("matrix artifact")
    }

    fn one_vector_matrix(dtype: MatrixDtype, values: Vec<f32>) -> MatrixArtifact {
        MatrixArtifact::new(
            dtype,
            MultiVectorEpochId::new([3; 32]),
            19,
            128,
            vec![MatrixArtifactRow::new(
                ContentHash::new([1; 32]),
                MultiVectorEmbedding::new(values, 1, 128, 16).expect("one matrix vector"),
            )],
        )
        .expect("one-vector matrix artifact")
    }

    fn bytes_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
