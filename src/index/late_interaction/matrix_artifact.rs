//! Immutable record-major exact-matrix blocks for late segments.

use std::collections::BTreeSet;
use std::mem::size_of;

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::embedding::artifact::{
    decode_matrix_payload, encode_matrix_payload, matrix_bytes_per_vector,
    matrix_dtype_from_header, matrix_dtype_header,
};
use crate::embedding::{
    ArtifactChecksum, ContentHash, FdeGenerationId, MatrixDtype, MultiVectorEmbedding,
    MultiVectorEpochId,
};
use crate::error::{Result, ZeppelinError};
use crate::storage::{NamespaceObjectFamily, NamespaceObjectKey};
use crate::types::VectorId;

const MATRIX_BLOCK_MAGIC: &[u8; 4] = b"ZMB1";
const MATRIX_BLOCK_VERSION: u8 = 1;
const MATRIX_BLOCK_HEADER_LEN: usize =
    4 + 1 + 1 + size_of::<u16>() + 32 + 32 + 2 * size_of::<u32>() + 3 * size_of::<u64>() + 2 * 32;
const MATRIX_BLOCK_DIRECTORY_FIXED_LEN: usize =
    2 * size_of::<u32>() + 32 + size_of::<u64>() + size_of::<u32>() + 2 * size_of::<u64>() + 32;

/// Current persisted matrix-block format.
pub const MATRIX_BLOCK_FORMAT_VERSION: u32 = 1;

/// Direct wave-two locator carried by one candidate row.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixBlockLocator {
    /// Exact immutable block object key.
    pub object_key: String,
    /// Absolute start of this row's encoded matrix payload.
    pub byte_offset: u64,
    /// Exact encoded matrix payload length.
    pub byte_length: u64,
    /// Number of vectors in the document matrix.
    pub vector_count: u32,
    /// SHA-256 over this row's encoded matrix payload.
    pub payload_checksum: ArtifactChecksum,
}

/// Manifest metadata for one immutable matrix block.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixBlockRef {
    /// Exact immutable object key.
    pub key: String,
    /// SHA-256 over the complete block.
    pub checksum: ArtifactChecksum,
    /// Complete object size.
    pub size_bytes: u64,
    /// Epoch-uniform scalar representation.
    pub dtype: MatrixDtype,
    /// Exact-scoring semantic epoch.
    pub semantic_epoch: MultiVectorEpochId,
    /// Candidate FDE generation paired with the block.
    pub fde_generation: FdeGenerationId,
    /// Coordinates per vector.
    pub vector_dimension: u32,
    /// Retrieval-unit rows in this block.
    pub row_count: u32,
    /// Total vectors across retrieval-unit rows.
    pub total_vectors: u64,
    /// Persisted codec version.
    pub format_version: u32,
}

#[derive(Clone)]
pub(crate) struct MatrixBlockInputRow {
    pub(crate) id: VectorId,
    pub(crate) ordinal: u32,
    pub(crate) content_hash: ContentHash,
    pub(crate) embedding: MultiVectorEmbedding,
}

pub(crate) struct BuiltMatrixBlock {
    pub(crate) reference: MatrixBlockRef,
    pub(crate) bytes: Bytes,
    pub(crate) locators: Vec<MatrixBlockLocator>,
}

struct PreparedRow {
    id: VectorId,
    ordinal: u32,
    content_hash: ContentHash,
    vector_count: u32,
    payload: Bytes,
    payload_checksum: ArtifactChecksum,
}

/// One fully decoded row used by round-trip validation and compaction tests.
pub(crate) struct DecodedMatrixBlockRow {
    pub(crate) id: VectorId,
    pub(crate) ordinal: u32,
    pub(crate) content_hash: ContentHash,
    #[cfg(test)]
    pub(crate) vector_offset: u64,
    pub(crate) locator: MatrixBlockLocator,
    pub(crate) embedding: MultiVectorEmbedding,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_matrix_blocks(
    namespace: &str,
    segment_id: &str,
    dtype: MatrixDtype,
    semantic_epoch: MultiVectorEpochId,
    fde_generation: FdeGenerationId,
    vector_dimension: usize,
    max_object_bytes: usize,
    rows: Vec<MatrixBlockInputRow>,
) -> Result<Vec<BuiltMatrixBlock>> {
    if segment_id.is_empty() || segment_id.contains('/') {
        return Err(invalid_block(
            "matrix block segment id must be one non-empty path component",
        ));
    }
    if rows.is_empty() {
        return Err(invalid_block(
            "matrix block build requires at least one row",
        ));
    }
    dtype.validate_for_dimension(
        u32::try_from(vector_dimension)
            .map_err(|_| invalid_block("matrix block dimension exceeds u32"))?,
    )?;
    if max_object_bytes <= MATRIX_BLOCK_HEADER_LEN {
        return Err(invalid_block(
            "matrix block object bound cannot fit the fixed header",
        ));
    }

    let mut identities = BTreeSet::new();
    let mut prepared = Vec::new();
    prepared
        .try_reserve_exact(rows.len())
        .map_err(|error| invalid_block(format!("matrix row allocation failed: {error}")))?;
    for row in rows {
        if row.id.is_empty() {
            return Err(invalid_block("matrix block record id cannot be empty"));
        }
        if !identities.insert((row.id.clone(), row.ordinal)) {
            return Err(invalid_block(
                "matrix block contains a duplicate record id and ordinal",
            ));
        }
        let id_bytes = row.id.len();
        u32::try_from(id_bytes).map_err(|_| invalid_block("matrix record id exceeds u32"))?;
        if row.embedding.vector_dimension() != vector_dimension {
            return Err(ZeppelinError::DimensionMismatch {
                expected: vector_dimension,
                actual: row.embedding.vector_dimension(),
            });
        }
        let vector_count = u32::try_from(row.embedding.vector_count())
            .map_err(|_| invalid_block("matrix row vector count exceeds u32"))?;
        let payload = encode_matrix_payload(dtype, vector_dimension, &row.embedding)?;
        prepared.push(PreparedRow {
            id: row.id,
            ordinal: row.ordinal,
            content_hash: row.content_hash,
            vector_count,
            payload_checksum: ArtifactChecksum::digest(&payload),
            payload,
        });
    }

    let mut groups = Vec::<Vec<PreparedRow>>::new();
    let mut current = Vec::new();
    let mut current_directory_bytes = 0_usize;
    let mut current_payload_bytes = 0_usize;
    for row in prepared {
        let directory_bytes = MATRIX_BLOCK_DIRECTORY_FIXED_LEN
            .checked_add(row.id.len())
            .ok_or_else(|| invalid_block("matrix directory byte count overflows"))?;
        let row_bytes = directory_bytes
            .checked_add(row.payload.len())
            .ok_or_else(|| invalid_block("matrix row encoded byte count overflows"))?;
        let single_bytes = MATRIX_BLOCK_HEADER_LEN
            .checked_add(row_bytes)
            .ok_or_else(|| invalid_block("matrix block byte count overflows"))?;
        if single_bytes > max_object_bytes {
            return Err(invalid_block(format!(
                "matrix row requires {single_bytes} bytes, object maximum is {max_object_bytes}"
            )));
        }
        let candidate_bytes = MATRIX_BLOCK_HEADER_LEN
            .checked_add(current_directory_bytes)
            .and_then(|bytes| bytes.checked_add(current_payload_bytes))
            .and_then(|bytes| bytes.checked_add(row_bytes))
            .ok_or_else(|| invalid_block("matrix block byte count overflows"))?;
        if !current.is_empty() && candidate_bytes > max_object_bytes {
            groups.push(std::mem::take(&mut current));
            current_directory_bytes = 0;
            current_payload_bytes = 0;
        }
        current_directory_bytes = current_directory_bytes
            .checked_add(directory_bytes)
            .ok_or_else(|| invalid_block("matrix directory byte count overflows"))?;
        current_payload_bytes = current_payload_bytes
            .checked_add(row.payload.len())
            .ok_or_else(|| invalid_block("matrix payload byte count overflows"))?;
        current.push(row);
    }
    if !current.is_empty() {
        groups.push(current);
    }

    groups
        .into_iter()
        .enumerate()
        .map(|(block_index, rows)| {
            encode_matrix_block(
                namespace,
                segment_id,
                block_index,
                dtype,
                semantic_epoch,
                fde_generation,
                vector_dimension,
                rows,
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn encode_matrix_block(
    namespace: &str,
    segment_id: &str,
    block_index: usize,
    dtype: MatrixDtype,
    semantic_epoch: MultiVectorEpochId,
    fde_generation: FdeGenerationId,
    vector_dimension: usize,
    rows: Vec<PreparedRow>,
) -> Result<BuiltMatrixBlock> {
    let key = format!(
        "{}{segment_id}/matrix_{block_index}.bin",
        NamespaceObjectFamily::LateSegment.namespace_prefix(namespace)
    );
    let owned = NamespaceObjectKey::classify(namespace, key.clone())?;
    if owned.family() != NamespaceObjectFamily::LateSegment {
        return Err(invalid_block(
            "matrix block key is outside the late-segment family",
        ));
    }

    let directory_len = rows.iter().try_fold(0_usize, |total, row| {
        total
            .checked_add(MATRIX_BLOCK_DIRECTORY_FIXED_LEN)
            .and_then(|bytes| bytes.checked_add(row.id.len()))
            .ok_or_else(|| invalid_block("matrix directory byte count overflows"))
    })?;
    let values_len = rows.iter().try_fold(0_usize, |total, row| {
        total
            .checked_add(row.payload.len())
            .ok_or_else(|| invalid_block("matrix values byte count overflows"))
    })?;
    let values_start = MATRIX_BLOCK_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| invalid_block("matrix values offset overflows"))?;
    let mut directory = BytesMut::with_capacity(directory_len);
    let mut values = BytesMut::with_capacity(values_len);
    let mut locators = Vec::with_capacity(rows.len());
    let mut vector_offset = 0_u64;
    for row in rows {
        directory.put_u32_le(
            u32::try_from(row.id.len())
                .map_err(|_| invalid_block("matrix record id exceeds u32"))?,
        );
        directory.extend_from_slice(row.id.as_bytes());
        directory.put_u32_le(row.ordinal);
        directory.extend_from_slice(row.content_hash.as_bytes());
        directory.put_u64_le(vector_offset);
        directory.put_u32_le(row.vector_count);
        let byte_offset = values_start
            .checked_add(values.len())
            .ok_or_else(|| invalid_block("matrix row byte offset overflows"))?;
        let byte_offset = u64::try_from(byte_offset)
            .map_err(|_| invalid_block("matrix row byte offset exceeds u64"))?;
        let byte_length = u64::try_from(row.payload.len())
            .map_err(|_| invalid_block("matrix row byte length exceeds u64"))?;
        directory.put_u64_le(byte_offset);
        directory.put_u64_le(byte_length);
        directory.extend_from_slice(row.payload_checksum.as_bytes());
        values.extend_from_slice(&row.payload);
        locators.push(MatrixBlockLocator {
            object_key: key.clone(),
            byte_offset,
            byte_length,
            vector_count: row.vector_count,
            payload_checksum: row.payload_checksum,
        });
        vector_offset = vector_offset
            .checked_add(u64::from(row.vector_count))
            .ok_or_else(|| invalid_block("matrix vector offset overflows"))?;
    }
    if directory.len() != directory_len || values.len() != values_len {
        return Err(invalid_block(
            "matrix block sections disagree with their planned sizes",
        ));
    }
    let directory = directory.freeze();
    let values = values.freeze();
    let directory_checksum = ArtifactChecksum::digest(&directory);
    let values_checksum = ArtifactChecksum::digest(&values);
    let row_count =
        u32::try_from(locators.len()).map_err(|_| invalid_block("matrix rows exceed u32"))?;
    let vector_dimension = u32::try_from(vector_dimension)
        .map_err(|_| invalid_block("matrix dimension exceeds u32"))?;
    let directory_len_u64 = u64::try_from(directory_len)
        .map_err(|_| invalid_block("matrix directory bytes exceed u64"))?;
    let values_len_u64 =
        u64::try_from(values_len).map_err(|_| invalid_block("matrix values bytes exceed u64"))?;

    let mut bytes = BytesMut::with_capacity(
        MATRIX_BLOCK_HEADER_LEN
            .checked_add(directory_len)
            .and_then(|size| size.checked_add(values_len))
            .ok_or_else(|| invalid_block("matrix block size overflows"))?,
    );
    bytes.extend_from_slice(MATRIX_BLOCK_MAGIC);
    bytes.put_u8(MATRIX_BLOCK_VERSION);
    let (dtype_discriminant, group_size) = matrix_dtype_header(dtype);
    bytes.put_u8(dtype_discriminant);
    bytes.put_u16_le(group_size);
    bytes.extend_from_slice(semantic_epoch.as_bytes());
    bytes.extend_from_slice(fde_generation.as_bytes());
    bytes.put_u32_le(vector_dimension);
    bytes.put_u32_le(row_count);
    bytes.put_u64_le(vector_offset);
    bytes.put_u64_le(directory_len_u64);
    bytes.put_u64_le(values_len_u64);
    bytes.extend_from_slice(directory_checksum.as_bytes());
    bytes.extend_from_slice(values_checksum.as_bytes());
    bytes.extend_from_slice(&directory);
    bytes.extend_from_slice(&values);
    let bytes = bytes.freeze();
    let checksum = ArtifactChecksum::digest(&bytes);
    let size_bytes =
        u64::try_from(bytes.len()).map_err(|_| invalid_block("matrix block size exceeds u64"))?;
    Ok(BuiltMatrixBlock {
        reference: MatrixBlockRef {
            key,
            checksum,
            size_bytes,
            dtype,
            semantic_epoch,
            fde_generation,
            vector_dimension,
            row_count,
            total_vectors: vector_offset,
            format_version: MATRIX_BLOCK_FORMAT_VERSION,
        },
        bytes,
        locators,
    })
}

pub(crate) fn decode_matrix_row(
    bytes: &[u8],
    locator: &MatrixBlockLocator,
    dtype: MatrixDtype,
    vector_dimension: usize,
    max_vectors: usize,
) -> Result<MultiVectorEmbedding> {
    if u64::try_from(bytes.len()).ok() != Some(locator.byte_length) {
        return Err(invalid_block(
            "matrix row payload length disagrees with its locator",
        ));
    }
    if ArtifactChecksum::digest(bytes) != locator.payload_checksum {
        return Err(invalid_block("matrix row payload checksum mismatch"));
    }
    decode_matrix_payload(
        bytes,
        dtype,
        vector_dimension,
        usize::try_from(locator.vector_count)
            .map_err(|_| invalid_block("matrix row vector count exceeds usize"))?,
        max_vectors,
    )
}

pub(crate) fn decode_matrix_block(
    bytes: &[u8],
    reference: &MatrixBlockRef,
    max_rows: usize,
    max_vectors_per_row: usize,
) -> Result<Vec<DecodedMatrixBlockRow>> {
    if u64::try_from(bytes.len()).ok() != Some(reference.size_bytes)
        || ArtifactChecksum::digest(bytes) != reference.checksum
    {
        return Err(invalid_block("matrix block size or checksum mismatch"));
    }
    let mut reader = BlockReader::new(bytes);
    reader.expect(MATRIX_BLOCK_MAGIC)?;
    if reader.read_u8()? != MATRIX_BLOCK_VERSION {
        return Err(invalid_block("unsupported matrix block version"));
    }
    let dtype = matrix_dtype_from_header(reader.read_u8()?, reader.read_u16()?)?;
    let semantic_epoch = MultiVectorEpochId::new(reader.read_array()?);
    let fde_generation = FdeGenerationId::new(reader.read_array()?);
    let vector_dimension = usize::try_from(reader.read_u32()?)
        .map_err(|_| invalid_block("matrix dimension exceeds usize"))?;
    let row_count = usize::try_from(reader.read_u32()?)
        .map_err(|_| invalid_block("matrix row count exceeds usize"))?;
    let total_vectors = reader.read_u64()?;
    let directory_len = usize::try_from(reader.read_u64()?)
        .map_err(|_| invalid_block("matrix directory length exceeds usize"))?;
    let values_len = usize::try_from(reader.read_u64()?)
        .map_err(|_| invalid_block("matrix values length exceeds usize"))?;
    let directory_checksum = ArtifactChecksum::new(reader.read_array()?);
    let values_checksum = ArtifactChecksum::new(reader.read_array()?);
    if dtype != reference.dtype
        || semantic_epoch != reference.semantic_epoch
        || fde_generation != reference.fde_generation
        || u32::try_from(vector_dimension).ok() != Some(reference.vector_dimension)
        || u32::try_from(row_count).ok() != Some(reference.row_count)
        || total_vectors != reference.total_vectors
        || reference.format_version != MATRIX_BLOCK_FORMAT_VERSION
    {
        return Err(invalid_block(
            "matrix block header disagrees with its manifest reference",
        ));
    }
    if row_count == 0 || row_count > max_rows {
        return Err(invalid_block(format!(
            "matrix block row count {row_count} is outside 1..={max_rows}"
        )));
    }
    let directory = reader.read_exact(directory_len)?;
    let values = reader.read_exact(values_len)?;
    if reader.remaining() != 0
        || ArtifactChecksum::digest(directory) != directory_checksum
        || ArtifactChecksum::digest(values) != values_checksum
    {
        return Err(invalid_block(
            "matrix block sections are corrupt or contain trailing bytes",
        ));
    }

    let values_start = MATRIX_BLOCK_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| invalid_block("matrix values offset overflows"))?;
    let mut directory_reader = BlockReader::new(directory);
    let mut rows = Vec::with_capacity(row_count);
    let mut identities = BTreeSet::new();
    let mut expected_vector_offset = 0_u64;
    let mut expected_byte_offset = u64::try_from(values_start)
        .map_err(|_| invalid_block("matrix values offset exceeds u64"))?;
    for _ in 0..row_count {
        let id_len = usize::try_from(directory_reader.read_u32()?)
            .map_err(|_| invalid_block("matrix record id length exceeds usize"))?;
        let id = std::str::from_utf8(directory_reader.read_exact(id_len)?)
            .map_err(|_| invalid_block("matrix record id is not UTF-8"))?
            .to_string();
        let ordinal = directory_reader.read_u32()?;
        let content_hash = ContentHash::new(directory_reader.read_array()?);
        let vector_offset = directory_reader.read_u64()?;
        let vector_count = directory_reader.read_u32()?;
        let byte_offset = directory_reader.read_u64()?;
        let byte_length = directory_reader.read_u64()?;
        let payload_checksum = ArtifactChecksum::new(directory_reader.read_array()?);
        if id.is_empty() || !identities.insert((id.clone(), ordinal)) {
            return Err(invalid_block(
                "matrix block has an empty or duplicate record identity",
            ));
        }
        if vector_offset != expected_vector_offset || byte_offset != expected_byte_offset {
            return Err(invalid_block("matrix block row offsets are not canonical"));
        }
        let relative_start = usize::try_from(
            byte_offset
                .checked_sub(
                    u64::try_from(values_start)
                        .map_err(|_| invalid_block("matrix values offset exceeds u64"))?,
                )
                .ok_or_else(|| invalid_block("matrix row begins before values section"))?,
        )
        .map_err(|_| invalid_block("matrix row offset exceeds usize"))?;
        let relative_end = relative_start
            .checked_add(
                usize::try_from(byte_length)
                    .map_err(|_| invalid_block("matrix row length exceeds usize"))?,
            )
            .ok_or_else(|| invalid_block("matrix row end overflows"))?;
        let payload = values
            .get(relative_start..relative_end)
            .ok_or_else(|| invalid_block("matrix row range exceeds values section"))?;
        let expected_row_bytes = usize::try_from(vector_count)
            .map_err(|_| invalid_block("matrix vector count exceeds usize"))?
            .checked_mul(matrix_bytes_per_vector(dtype, vector_dimension)?)
            .ok_or_else(|| invalid_block("matrix row byte count overflows"))?;
        if u64::try_from(expected_row_bytes).ok() != Some(byte_length) {
            return Err(invalid_block(
                "matrix row byte length disagrees with dtype and shape",
            ));
        }
        let locator = MatrixBlockLocator {
            object_key: reference.key.clone(),
            byte_offset,
            byte_length,
            vector_count,
            payload_checksum,
        };
        let embedding = decode_matrix_row(
            payload,
            &locator,
            dtype,
            vector_dimension,
            max_vectors_per_row,
        )?;
        rows.push(DecodedMatrixBlockRow {
            id,
            ordinal,
            content_hash,
            #[cfg(test)]
            vector_offset,
            locator,
            embedding,
        });
        expected_vector_offset = expected_vector_offset
            .checked_add(u64::from(vector_count))
            .ok_or_else(|| invalid_block("matrix vector offset overflows"))?;
        expected_byte_offset = expected_byte_offset
            .checked_add(byte_length)
            .ok_or_else(|| invalid_block("matrix byte offset overflows"))?;
    }
    if directory_reader.remaining() != 0
        || expected_vector_offset != total_vectors
        || expected_byte_offset
            != u64::try_from(bytes.len())
                .map_err(|_| invalid_block("matrix block size exceeds u64"))?
    {
        return Err(invalid_block(
            "matrix block directory does not tile its sections",
        ));
    }
    Ok(rows)
}

fn invalid_block(reason: impl Into<String>) -> ZeppelinError {
    ZeppelinError::Serialization(format!("invalid late matrix block: {}", reason.into()))
}

struct BlockReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> BlockReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.cursor
    }

    fn expect(&mut self, expected: &[u8]) -> Result<()> {
        if self.read_exact(expected.len())? != expected {
            return Err(invalid_block("bad matrix block magic"));
        }
        Ok(())
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .cursor
            .checked_add(length)
            .ok_or_else(|| invalid_block("matrix block read offset overflows"))?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or_else(|| invalid_block("matrix block is truncated"))?;
        self.cursor = end;
        Ok(value)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.read_exact(N)?
            .try_into()
            .map_err(|_| invalid_block("matrix block fixed field is truncated"))
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
}

#[cfg(test)]
mod tests {
    use crate::embedding::{
        ContentHash, FdeGenerationId, MatrixDtype, MultiVectorEmbedding, MultiVectorEpochId,
    };

    use super::{build_matrix_blocks, decode_matrix_block, decode_matrix_row, MatrixBlockInputRow};

    #[test]
    fn record_major_matrix_block_round_trips_with_direct_locators() {
        let rows = vec![
            MatrixBlockInputRow {
                id: "first".to_string(),
                ordinal: 4,
                content_hash: ContentHash::new([1; 32]),
                embedding: MultiVectorEmbedding::new(vec![0.5, -0.5, 1.0, 0.0], 2, 2, 4)
                    .expect("first matrix"),
            },
            MatrixBlockInputRow {
                id: "second".to_string(),
                ordinal: 9,
                content_hash: ContentHash::new([2; 32]),
                embedding: MultiVectorEmbedding::new(vec![0.25, 0.75], 1, 2, 4)
                    .expect("second matrix"),
            },
        ];
        let blocks = build_matrix_blocks(
            "namespace",
            "segment",
            MatrixDtype::F16,
            MultiVectorEpochId::new([3; 32]),
            FdeGenerationId::new([4; 32]),
            2,
            8 * 1024,
            rows,
        )
        .expect("matrix blocks");

        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].locators.len(), 2);
        assert_eq!(&blocks[0].bytes[..4], b"ZMB1");
        assert_eq!(blocks[0].reference.dtype, MatrixDtype::F16);
        assert_eq!(blocks[0].reference.row_count, 2);
        assert_eq!(blocks[0].reference.total_vectors, 3);

        let decoded = decode_matrix_block(&blocks[0].bytes, &blocks[0].reference, 4, 4)
            .expect("complete block round trip");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].id, "first");
        assert_eq!(decoded[0].ordinal, 4);
        assert_eq!(decoded[0].content_hash, ContentHash::new([1; 32]));
        assert_eq!(decoded[0].vector_offset, 0);
        assert_eq!(decoded[0].locator, blocks[0].locators[0]);
        assert_eq!(decoded[0].embedding.values(), &[0.5_f32, -0.5, 1.0, 0.0]);
        assert_eq!(decoded[1].id, "second");
        assert_eq!(decoded[1].vector_offset, 2);

        for (row, locator) in decoded.iter().zip(&blocks[0].locators) {
            let start = usize::try_from(locator.byte_offset).expect("offset");
            let end = start + usize::try_from(locator.byte_length).expect("length");
            let ranged = decode_matrix_row(
                &blocks[0].bytes[start..end],
                locator,
                MatrixDtype::F16,
                2,
                4,
            )
            .expect("direct range decode");
            assert_eq!(ranged, row.embedding);
        }
    }
}
