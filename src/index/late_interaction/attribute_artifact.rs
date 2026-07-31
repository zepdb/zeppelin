//! Immutable record-major exact-attribute blocks for late segments.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::mem::size_of;

use bytes::{BufMut, Bytes, BytesMut};
use serde::de::{Error as _, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::embedding::ArtifactChecksum;
use crate::error::{Result, ZeppelinError};
use crate::storage::{NamespaceObjectFamily, NamespaceObjectKey};
use crate::types::{AttributeValue, VectorId};

use super::candidate::AttributeLocator;

const ATTRIBUTE_BLOCK_MAGIC: &[u8; 4] = b"ZAB1";
const ATTRIBUTE_BLOCK_VERSION: u8 = 1;
const ATTRIBUTE_BLOCK_HEADER_LEN: usize =
    4 + 2 * size_of::<u8>() + size_of::<u16>() + size_of::<u32>() + 2 * size_of::<u64>() + 2 * 32;
const ATTRIBUTE_BLOCK_DIRECTORY_FIXED_LEN: usize = 2 * size_of::<u32>() + 2 * size_of::<u64>() + 32;

/// Current persisted attribute-block format.
pub const ATTRIBUTE_BLOCK_FORMAT_VERSION: u32 = 1;

/// Manifest metadata for one immutable exact-attribute block.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AttributeBlockRef {
    /// Exact immutable block object key.
    pub key: String,
    /// SHA-256 over the complete block.
    pub checksum: ArtifactChecksum,
    /// Complete object size.
    pub size_bytes: u64,
    /// Retrieval-unit rows in this block.
    pub row_count: u32,
    /// Persisted codec version.
    pub format_version: u32,
}

#[derive(Clone)]
pub(crate) struct AttributeBlockInputRow {
    pub(crate) id: VectorId,
    pub(crate) ordinal: u32,
    pub(crate) attributes: Option<HashMap<String, AttributeValue>>,
}

pub(crate) struct BuiltAttributeBlock {
    pub(crate) reference: AttributeBlockRef,
    pub(crate) bytes: Bytes,
    pub(crate) locators: Vec<AttributeLocator>,
}

struct PreparedRow {
    id: VectorId,
    ordinal: u32,
    payload: Bytes,
    payload_checksum: ArtifactChecksum,
}

/// One fully decoded row used by compaction and artifact validation.
pub(crate) struct DecodedAttributeBlockRow {
    pub(crate) id: VectorId,
    pub(crate) ordinal: u32,
    pub(crate) locator: AttributeLocator,
    pub(crate) attributes: Option<HashMap<String, AttributeValue>>,
}

/// Build deterministic, bounded exact-attribute objects in caller row order.
pub(crate) fn build_attribute_blocks(
    namespace: &str,
    segment_id: &str,
    max_object_bytes: usize,
    rows: Vec<AttributeBlockInputRow>,
) -> Result<Vec<BuiltAttributeBlock>> {
    validate_segment_id(segment_id)?;
    if rows.is_empty() {
        return Err(invalid_block(
            "attribute block build requires at least one row",
        ));
    }
    if max_object_bytes <= ATTRIBUTE_BLOCK_HEADER_LEN {
        return Err(invalid_block(
            "attribute block object bound cannot fit the fixed header",
        ));
    }

    let mut identities = BTreeSet::new();
    let mut prepared = Vec::new();
    prepared
        .try_reserve_exact(rows.len())
        .map_err(|error| invalid_block(format!("attribute row allocation failed: {error}")))?;
    for row in rows {
        if row.id.is_empty() {
            return Err(invalid_block("attribute block record id cannot be empty"));
        }
        if !identities.insert((row.id.clone(), row.ordinal)) {
            return Err(invalid_block(
                "attribute block contains a duplicate record id and ordinal",
            ));
        }
        u32::try_from(row.id.len())
            .map_err(|_| invalid_block("attribute record id exceeds u32"))?;
        let ordered = row
            .attributes
            .map(|attributes| attributes.into_iter().collect::<BTreeMap<_, _>>());
        validate_attributes(ordered.as_ref())?;
        let payload = serde_json::to_vec(&ordered)
            .map(Bytes::from)
            .map_err(|error| {
                invalid_block(format!("attribute payload serialization failed: {error}"))
            })?;
        if payload.is_empty() {
            return Err(invalid_block("attribute payload cannot be empty"));
        }
        u64::try_from(payload.len())
            .map_err(|_| invalid_block("attribute row payload exceeds u64"))?;
        prepared.push(PreparedRow {
            id: row.id,
            ordinal: row.ordinal,
            payload_checksum: ArtifactChecksum::digest(&payload),
            payload,
        });
    }

    let mut groups = Vec::<Vec<PreparedRow>>::new();
    let mut current = Vec::new();
    let mut current_directory_bytes = 0_usize;
    let mut current_payload_bytes = 0_usize;
    for row in prepared {
        let directory_bytes = ATTRIBUTE_BLOCK_DIRECTORY_FIXED_LEN
            .checked_add(row.id.len())
            .ok_or_else(|| invalid_block("attribute directory byte count overflows"))?;
        let row_bytes = directory_bytes
            .checked_add(row.payload.len())
            .ok_or_else(|| invalid_block("attribute row encoded byte count overflows"))?;
        let single_bytes = ATTRIBUTE_BLOCK_HEADER_LEN
            .checked_add(row_bytes)
            .ok_or_else(|| invalid_block("attribute block byte count overflows"))?;
        if single_bytes > max_object_bytes {
            return Err(invalid_block(format!(
                "attribute row requires {single_bytes} bytes, object maximum is {max_object_bytes}"
            )));
        }
        let candidate_bytes = ATTRIBUTE_BLOCK_HEADER_LEN
            .checked_add(current_directory_bytes)
            .and_then(|bytes| bytes.checked_add(current_payload_bytes))
            .and_then(|bytes| bytes.checked_add(row_bytes))
            .ok_or_else(|| invalid_block("attribute block byte count overflows"))?;
        if !current.is_empty() && candidate_bytes > max_object_bytes {
            groups.push(std::mem::take(&mut current));
            current_directory_bytes = 0;
            current_payload_bytes = 0;
        }
        current_directory_bytes = current_directory_bytes
            .checked_add(directory_bytes)
            .ok_or_else(|| invalid_block("attribute directory byte count overflows"))?;
        current_payload_bytes = current_payload_bytes
            .checked_add(row.payload.len())
            .ok_or_else(|| invalid_block("attribute payload byte count overflows"))?;
        current.push(row);
    }
    if !current.is_empty() {
        groups.push(current);
    }

    groups
        .into_iter()
        .enumerate()
        .map(|(block_index, rows)| {
            encode_attribute_block(namespace, segment_id, block_index, max_object_bytes, rows)
        })
        .collect()
}

fn encode_attribute_block(
    namespace: &str,
    segment_id: &str,
    block_index: usize,
    max_object_bytes: usize,
    rows: Vec<PreparedRow>,
) -> Result<BuiltAttributeBlock> {
    let key = attribute_block_key(namespace, segment_id, block_index)?;
    let directory_len = rows.iter().try_fold(0_usize, |total, row| {
        total
            .checked_add(ATTRIBUTE_BLOCK_DIRECTORY_FIXED_LEN)
            .and_then(|bytes| bytes.checked_add(row.id.len()))
            .ok_or_else(|| invalid_block("attribute directory byte count overflows"))
    })?;
    let values_len = rows.iter().try_fold(0_usize, |total, row| {
        total
            .checked_add(row.payload.len())
            .ok_or_else(|| invalid_block("attribute values byte count overflows"))
    })?;
    let block_len = ATTRIBUTE_BLOCK_HEADER_LEN
        .checked_add(directory_len)
        .and_then(|bytes| bytes.checked_add(values_len))
        .ok_or_else(|| invalid_block("attribute block size overflows"))?;
    if block_len > max_object_bytes {
        return Err(invalid_block(
            "attribute block exceeds its caller-supplied object maximum",
        ));
    }
    let values_start = ATTRIBUTE_BLOCK_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| invalid_block("attribute values offset overflows"))?;
    let mut directory = BytesMut::with_capacity(directory_len);
    let mut values = BytesMut::with_capacity(values_len);
    let mut locators = Vec::with_capacity(rows.len());
    for row in rows {
        directory.put_u32_le(
            u32::try_from(row.id.len())
                .map_err(|_| invalid_block("attribute record id exceeds u32"))?,
        );
        directory.extend_from_slice(row.id.as_bytes());
        directory.put_u32_le(row.ordinal);
        let byte_offset = values_start
            .checked_add(values.len())
            .ok_or_else(|| invalid_block("attribute row byte offset overflows"))?;
        let byte_offset = u64::try_from(byte_offset)
            .map_err(|_| invalid_block("attribute row byte offset exceeds u64"))?;
        let byte_length = u64::try_from(row.payload.len())
            .map_err(|_| invalid_block("attribute row byte length exceeds u64"))?;
        directory.put_u64_le(byte_offset);
        directory.put_u64_le(byte_length);
        directory.extend_from_slice(row.payload_checksum.as_bytes());
        values.extend_from_slice(&row.payload);
        locators.push(AttributeLocator {
            object_key: key.clone(),
            byte_offset,
            byte_length,
            payload_checksum: row.payload_checksum,
        });
    }
    if directory.len() != directory_len || values.len() != values_len {
        return Err(invalid_block(
            "attribute block sections disagree with their planned sizes",
        ));
    }

    let directory = directory.freeze();
    let values = values.freeze();
    let row_count =
        u32::try_from(locators.len()).map_err(|_| invalid_block("attribute rows exceed u32"))?;
    let mut bytes = BytesMut::with_capacity(block_len);
    bytes.extend_from_slice(ATTRIBUTE_BLOCK_MAGIC);
    bytes.put_u8(ATTRIBUTE_BLOCK_VERSION);
    bytes.put_u8(0);
    bytes.put_u16_le(0);
    bytes.put_u32_le(row_count);
    bytes.put_u64_le(
        u64::try_from(directory_len)
            .map_err(|_| invalid_block("attribute directory bytes exceed u64"))?,
    );
    bytes.put_u64_le(
        u64::try_from(values_len)
            .map_err(|_| invalid_block("attribute values bytes exceed u64"))?,
    );
    bytes.extend_from_slice(ArtifactChecksum::digest(&directory).as_bytes());
    bytes.extend_from_slice(ArtifactChecksum::digest(&values).as_bytes());
    bytes.extend_from_slice(&directory);
    bytes.extend_from_slice(&values);
    if bytes.len() != block_len {
        return Err(invalid_block(
            "attribute block disagrees with its planned size",
        ));
    }
    let bytes = bytes.freeze();
    Ok(BuiltAttributeBlock {
        reference: AttributeBlockRef {
            key,
            checksum: ArtifactChecksum::digest(&bytes),
            size_bytes: u64::try_from(bytes.len())
                .map_err(|_| invalid_block("attribute block size exceeds u64"))?,
            row_count,
            format_version: ATTRIBUTE_BLOCK_FORMAT_VERSION,
        },
        bytes,
        locators,
    })
}

/// Decode one exact ranged attribute payload selected by wave two.
pub(crate) fn decode_attribute_row(
    bytes: &[u8],
    locator: &AttributeLocator,
    max_payload_bytes: usize,
) -> Result<Option<HashMap<String, AttributeValue>>> {
    validate_attribute_block_key(&locator.object_key)?;
    if locator.byte_length == 0
        || locator
            .byte_offset
            .checked_add(locator.byte_length)
            .is_none()
    {
        return Err(invalid_block("attribute row locator range is invalid"));
    }
    if bytes.len() > max_payload_bytes {
        return Err(invalid_block(format!(
            "attribute row payload has {} bytes, maximum is {max_payload_bytes}",
            bytes.len()
        )));
    }
    if u64::try_from(bytes.len()).ok() != Some(locator.byte_length) {
        return Err(invalid_block(
            "attribute row payload length disagrees with its locator",
        ));
    }
    if ArtifactChecksum::digest(bytes) != locator.payload_checksum {
        return Err(invalid_block("attribute row payload checksum mismatch"));
    }
    decode_canonical_attributes(bytes)
}

/// Decode and fully validate one complete immutable attribute block.
pub(crate) fn decode_attribute_block(
    bytes: &[u8],
    reference: &AttributeBlockRef,
    max_rows: usize,
    max_payload_bytes: usize,
) -> Result<Vec<DecodedAttributeBlockRow>> {
    validate_attribute_block_key(&reference.key)?;
    if u64::try_from(bytes.len()).ok() != Some(reference.size_bytes)
        || ArtifactChecksum::digest(bytes) != reference.checksum
    {
        return Err(invalid_block("attribute block size or checksum mismatch"));
    }
    let mut reader = BlockReader::new(bytes);
    reader.expect(ATTRIBUTE_BLOCK_MAGIC)?;
    if reader.read_u8()? != ATTRIBUTE_BLOCK_VERSION {
        return Err(invalid_block("unsupported attribute block version"));
    }
    if reader.read_u8()? != 0 || reader.read_u16()? != 0 {
        return Err(invalid_block(
            "attribute block reserved header fields are nonzero",
        ));
    }
    let row_count = usize::try_from(reader.read_u32()?)
        .map_err(|_| invalid_block("attribute row count exceeds usize"))?;
    let directory_len = usize::try_from(reader.read_u64()?)
        .map_err(|_| invalid_block("attribute directory length exceeds usize"))?;
    let values_len = usize::try_from(reader.read_u64()?)
        .map_err(|_| invalid_block("attribute values length exceeds usize"))?;
    let directory_checksum = ArtifactChecksum::new(reader.read_array()?);
    let values_checksum = ArtifactChecksum::new(reader.read_array()?);
    if u32::try_from(row_count).ok() != Some(reference.row_count)
        || reference.format_version != ATTRIBUTE_BLOCK_FORMAT_VERSION
    {
        return Err(invalid_block(
            "attribute block header disagrees with its manifest reference",
        ));
    }
    if row_count == 0 || row_count > max_rows {
        return Err(invalid_block(format!(
            "attribute block row count {row_count} is outside 1..={max_rows}"
        )));
    }
    let minimum_directory_len = row_count
        .checked_mul(ATTRIBUTE_BLOCK_DIRECTORY_FIXED_LEN)
        .ok_or_else(|| invalid_block("attribute minimum directory size overflows"))?;
    if directory_len < minimum_directory_len {
        return Err(invalid_block(
            "attribute directory is too short for its declared row count",
        ));
    }
    let directory = reader.read_exact(directory_len)?;
    let values = reader.read_exact(values_len)?;
    if reader.remaining() != 0
        || ArtifactChecksum::digest(directory) != directory_checksum
        || ArtifactChecksum::digest(values) != values_checksum
    {
        return Err(invalid_block(
            "attribute block sections are corrupt or contain trailing bytes",
        ));
    }

    let values_start = ATTRIBUTE_BLOCK_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| invalid_block("attribute values offset overflows"))?;
    let values_start_u64 = u64::try_from(values_start)
        .map_err(|_| invalid_block("attribute values offset exceeds u64"))?;
    let mut directory_reader = BlockReader::new(directory);
    let mut rows = Vec::with_capacity(row_count);
    let mut identities = BTreeSet::new();
    let mut expected_byte_offset = values_start_u64;
    for _ in 0..row_count {
        let id_len = usize::try_from(directory_reader.read_u32()?)
            .map_err(|_| invalid_block("attribute record id length exceeds usize"))?;
        let id = std::str::from_utf8(directory_reader.read_exact(id_len)?)
            .map_err(|_| invalid_block("attribute record id is not UTF-8"))?
            .to_string();
        let ordinal = directory_reader.read_u32()?;
        let byte_offset = directory_reader.read_u64()?;
        let byte_length = directory_reader.read_u64()?;
        let payload_checksum = ArtifactChecksum::new(directory_reader.read_array()?);
        if id.is_empty() || !identities.insert((id.clone(), ordinal)) {
            return Err(invalid_block(
                "attribute block has an empty or duplicate record identity",
            ));
        }
        if byte_offset != expected_byte_offset || byte_length == 0 {
            return Err(invalid_block(
                "attribute block row offsets are not canonical",
            ));
        }
        let relative_start = usize::try_from(
            byte_offset
                .checked_sub(values_start_u64)
                .ok_or_else(|| invalid_block("attribute row begins before values section"))?,
        )
        .map_err(|_| invalid_block("attribute row offset exceeds usize"))?;
        let relative_end = relative_start
            .checked_add(
                usize::try_from(byte_length)
                    .map_err(|_| invalid_block("attribute row length exceeds usize"))?,
            )
            .ok_or_else(|| invalid_block("attribute row end overflows"))?;
        let payload = values
            .get(relative_start..relative_end)
            .ok_or_else(|| invalid_block("attribute row range exceeds values section"))?;
        let locator = AttributeLocator {
            object_key: reference.key.clone(),
            byte_offset,
            byte_length,
            payload_checksum,
        };
        let attributes = decode_attribute_row(payload, &locator, max_payload_bytes)?;
        rows.push(DecodedAttributeBlockRow {
            id,
            ordinal,
            locator,
            attributes,
        });
        expected_byte_offset = expected_byte_offset
            .checked_add(byte_length)
            .ok_or_else(|| invalid_block("attribute byte offset overflows"))?;
    }
    if directory_reader.remaining() != 0
        || expected_byte_offset
            != u64::try_from(bytes.len())
                .map_err(|_| invalid_block("attribute block size exceeds u64"))?
    {
        return Err(invalid_block(
            "attribute block directory does not tile its sections",
        ));
    }
    Ok(rows)
}

fn decode_canonical_attributes(bytes: &[u8]) -> Result<Option<HashMap<String, AttributeValue>>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let CanonicalAttributes(ordered) = CanonicalAttributes::deserialize(&mut deserializer)
        .map_err(|error| invalid_block(format!("attribute payload is invalid JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| invalid_block(format!("attribute payload has trailing data: {error}")))?;
    validate_attributes(ordered.as_ref())?;
    let canonical = serde_json::to_vec(&ordered).map_err(|error| {
        invalid_block(format!(
            "decoded attribute payload could not be canonicalized: {error}"
        ))
    })?;
    if canonical != bytes {
        return Err(invalid_block("attribute payload is not canonical JSON"));
    }
    Ok(ordered.map(|attributes| attributes.into_iter().collect()))
}

fn validate_attributes(attributes: Option<&BTreeMap<String, AttributeValue>>) -> Result<()> {
    let Some(attributes) = attributes else {
        return Ok(());
    };
    for (field, value) in attributes {
        if field.is_empty() {
            return Err(invalid_block("attribute field cannot be empty"));
        }
        match value {
            AttributeValue::Float(value) if !value.is_finite() => {
                return Err(invalid_block("attribute contains a non-finite float"));
            }
            AttributeValue::FloatList(values) if values.iter().any(|value| !value.is_finite()) => {
                return Err(invalid_block(
                    "attribute contains a non-finite float-list value",
                ));
            }
            AttributeValue::String(_)
            | AttributeValue::Integer(_)
            | AttributeValue::Float(_)
            | AttributeValue::Bool(_)
            | AttributeValue::StringList(_)
            | AttributeValue::IntegerList(_)
            | AttributeValue::FloatList(_) => {}
        }
    }
    Ok(())
}

fn attribute_block_key(namespace: &str, segment_id: &str, block_index: usize) -> Result<String> {
    let key = format!(
        "{}{segment_id}/attrs_{block_index}.bin",
        NamespaceObjectFamily::LateSegment.namespace_prefix(namespace)
    );
    validate_attribute_block_key(&key)?;
    Ok(key)
}

fn validate_attribute_block_key(key: &str) -> Result<()> {
    let (namespace, _) = key
        .split_once('/')
        .ok_or_else(|| invalid_block("attribute block key has no namespace prefix"))?;
    let owned = NamespaceObjectKey::classify(namespace, key.to_string())?;
    if owned.family() != NamespaceObjectFamily::LateSegment {
        return Err(invalid_block(
            "attribute block key is outside the late-segment family",
        ));
    }
    let prefix = NamespaceObjectFamily::LateSegment.namespace_prefix(namespace);
    let descendant = key
        .strip_prefix(&prefix)
        .ok_or_else(|| invalid_block("attribute block key has the wrong family prefix"))?;
    let mut parts = descendant.split('/');
    let segment_id = parts
        .next()
        .ok_or_else(|| invalid_block("attribute block key has no segment id"))?;
    let file_name = parts
        .next()
        .ok_or_else(|| invalid_block("attribute block key has no file name"))?;
    if segment_id.is_empty() || parts.next().is_some() {
        return Err(invalid_block(
            "attribute block key must have one segment component",
        ));
    }
    let block_index = file_name
        .strip_prefix("attrs_")
        .and_then(|value| value.strip_suffix(".bin"))
        .ok_or_else(|| invalid_block("attribute block file name is not canonical"))?;
    let parsed = block_index
        .parse::<usize>()
        .map_err(|_| invalid_block("attribute block index is not canonical decimal"))?;
    if parsed.to_string() != block_index {
        return Err(invalid_block(
            "attribute block index is not canonical decimal",
        ));
    }
    Ok(())
}

fn validate_segment_id(segment_id: &str) -> Result<()> {
    if segment_id.is_empty() || segment_id.contains('/') {
        return Err(invalid_block(
            "attribute block segment id must be one non-empty path component",
        ));
    }
    Ok(())
}

fn invalid_block(reason: impl Into<String>) -> ZeppelinError {
    ZeppelinError::Serialization(format!("invalid late attribute block: {}", reason.into()))
}

struct CanonicalAttributes(Option<BTreeMap<String, AttributeValue>>);

impl<'de> Deserialize<'de> for CanonicalAttributes {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer
            .deserialize_option(OptionalAttributesVisitor)
            .map(Self)
    }
}

struct OptionalAttributesVisitor;

impl<'de> Visitor<'de> for OptionalAttributesVisitor {
    type Value = Option<BTreeMap<String, AttributeValue>>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("null or an attribute object")
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(None)
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(AttributeMapVisitor).map(Some)
    }
}

struct AttributeMapVisitor;

impl<'de> Visitor<'de> for AttributeMapVisitor {
    type Value = BTreeMap<String, AttributeValue>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an attribute object with unique fields")
    }

    fn visit_map<A>(self, mut entries: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut attributes = BTreeMap::new();
        while let Some((field, value)) = entries.next_entry::<String, AttributeValue>()? {
            if attributes.insert(field, value).is_some() {
                return Err(A::Error::custom("duplicate attribute field"));
            }
        }
        Ok(attributes)
    }
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
            return Err(invalid_block("bad attribute block magic"));
        }
        Ok(())
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .cursor
            .checked_add(length)
            .ok_or_else(|| invalid_block("attribute block read offset overflows"))?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or_else(|| invalid_block("attribute block is truncated"))?;
        self.cursor = end;
        Ok(value)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.read_exact(N)?
            .try_into()
            .map_err(|_| invalid_block("attribute block fixed field is truncated"))
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
    use std::collections::HashMap;

    use crate::types::AttributeValue;

    use super::{
        build_attribute_blocks, decode_attribute_block, decode_attribute_row,
        AttributeBlockInputRow, ATTRIBUTE_BLOCK_HEADER_LEN,
    };

    #[test]
    fn bounded_record_major_attribute_blocks_round_trip_with_direct_locators() {
        let first_attributes = HashMap::from([
            ("z".to_string(), AttributeValue::String("last".to_string())),
            ("a".to_string(), AttributeValue::Integer(7)),
        ]);
        let rows = vec![
            AttributeBlockInputRow {
                id: "first".to_string(),
                ordinal: 4,
                attributes: Some(first_attributes.clone()),
            },
            AttributeBlockInputRow {
                id: "second".to_string(),
                ordinal: 9,
                attributes: None,
            },
            AttributeBlockInputRow {
                id: "third".to_string(),
                ordinal: 12,
                attributes: Some(HashMap::new()),
            },
        ];
        let blocks =
            build_attribute_blocks("namespace", "segment", 240, rows).expect("attribute blocks");

        assert_eq!(blocks.len(), 2);
        assert_eq!(
            blocks[0].reference.key,
            "namespace/late/segments/segment/attrs_0.bin"
        );
        assert_eq!(&blocks[0].bytes[..4], b"ZAB1");
        assert_eq!(blocks[0].reference.row_count, 2);
        assert_eq!(blocks[1].reference.row_count, 1);

        let expected = [Some(first_attributes), None, Some(HashMap::new())];
        let expected_identities = [("first", 4), ("second", 9), ("third", 12)];
        let mut decoded_index = 0;
        for block in &blocks {
            let decoded = decode_attribute_block(&block.bytes, &block.reference, 3, 1024)
                .expect("complete block round trip");
            assert_eq!(decoded.len(), block.locators.len());
            for (row, locator) in decoded.iter().zip(&block.locators) {
                assert_eq!(
                    (row.id.as_str(), row.ordinal),
                    expected_identities[decoded_index]
                );
                assert_eq!(row.locator, *locator);
                assert_eq!(row.attributes, expected[decoded_index]);
                let start = usize::try_from(locator.byte_offset).expect("offset");
                let end = start + usize::try_from(locator.byte_length).expect("length");
                let ranged = decode_attribute_row(&block.bytes[start..end], locator, 1024)
                    .expect("direct range decode");
                assert_eq!(ranged, expected[decoded_index]);
                decoded_index += 1;
            }
        }
        assert_eq!(decoded_index, 3);
        let first_locator = &blocks[0].locators[0];
        let first_start = usize::try_from(first_locator.byte_offset).expect("offset");
        let first_end =
            first_start + usize::try_from(first_locator.byte_length).expect("payload length");
        assert_eq!(
            &blocks[0].bytes[first_start..first_end],
            br#"{"a":7,"z":"last"}"#
        );

        let oversized = build_attribute_blocks(
            "namespace",
            "segment",
            ATTRIBUTE_BLOCK_HEADER_LEN + 1,
            vec![AttributeBlockInputRow {
                id: "too-large".to_string(),
                ordinal: 0,
                attributes: None,
            }],
        );
        assert!(oversized.is_err(), "one oversized row must fail loudly");
    }
}
