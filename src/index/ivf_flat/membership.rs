//! Encodes the immutable vector-to-cluster map carried by each IVF-Flat segment.
//!
//! A membership artifact records which logical IVF cluster owns every vector
//! ID. The IVF builder and incremental compactor create it beside the segment's
//! other immutable objects. Incremental compaction reads the published map to
//! find surviving rows without scanning every old cluster, and point lookup can
//! use it to locate an ID. This module only builds and validates bytes; callers
//! perform object-store PUTs and make the resulting [`MembershipRef`] visible by
//! publishing a manifest.
//!
//! ```text
//! cluster-ordered vector IDs
//!            |
//!            v
//! build + sort deterministic membership bytes
//!            |
//!            v
//! upload immutable membership.bin  (exists, not authoritative yet)
//!            |
//!            v
//! publish segment in manifest      (membership becomes visible)
//!            |
//!            v
//! compaction / point lookup validates and reads the map
//! ```
//!
//! The binary layout is intentionally small and self-describing enough to fail
//! loudly on incompatible or truncated input:
//!
//! ```text
//! magic "ZMB1" | version u32 | cluster_count u32 | entry_count u64
//! repeated entry_count times:
//!     id_len u16 | UTF-8 id bytes | cluster_index u32
//! ```
//!
//! ## Reading map
//!
//! 1. [`MembershipData`] describes the validated in-memory result.
//! 2. `build_membership_artifact` flattens cluster ownership into entries and
//!    constructs manifest metadata.
//! 3. [`serialize_membership`] establishes deterministic ID ordering.
//! 4. [`deserialize_membership`] and `MembershipReader` enforce format and
//!    bounds invariants on bytes loaded from authoritative storage.
//!
//! ## Invariants and compatibility
//!
//! - Artifacts are write-once; updates create a new segment key and are exposed
//!   only through the authoritative manifest.
//! - Entries are strictly sorted by vector ID, so IDs are unique after decode
//!   and serialization is independent of cluster traversal order.
//! - Every decoded cluster index is smaller than `cluster_count`.
//! - Only [`MEMBERSHIP_VERSION`] is accepted. A future layout must use a new
//!   version and retain explicit compatibility handling.
//!
//! ## Rust concepts used here
//!
//! `MembershipReader` borrows `&[u8]` and returns slices tied to that input's
//! lifetime. This resembles a checked cursor over a Java `ByteBuffer` or a C
//! pointer-plus-length, but Rust prevents returned byte views from outliving the
//! artifact buffer. Decoding allocates owned `String` IDs only after every slice
//! access has passed bounds checks.

use bytes::Bytes;

use crate::error::{Result, ZeppelinError};
use crate::wal::manifest::MembershipRef;

/// Four-byte signature that distinguishes membership data from other objects.
pub const MEMBERSHIP_MAGIC: &[u8; 4] = b"ZMB1";

/// Current binary membership format written and accepted by this module.
pub const MEMBERSHIP_VERSION: u32 = 1;

/// Fixed bytes before the first variable-length membership entry.
const HEADER_LEN: usize = 4 + 4 + 4 + 8;

/// Validated, owned membership data decoded from an immutable segment artifact.
///
/// Cloning this value clones every ID string and allocates a second entries
/// vector. Borrow it when a second owned copy is unnecessary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipData {
    /// Number of logical IVF clusters addressable by the entry indexes.
    pub cluster_count: u32,
    /// Unique `(vector_id, cluster_index)` pairs sorted by ID ascending.
    pub entries: Vec<(String, u32)>,
}

/// Constructs the object-store key for a segment's membership artifact.
///
/// # Parameters
///
/// - `namespace`: Namespace key prefix already validated by the caller.
/// - `segment_id`: Immutable segment identifier whose membership is described.
///
/// # Returns
///
/// An owned key ending in `segments/{segment_id}/membership.bin`.
///
/// # Examples
///
/// Namespace `catalog` and segment `seg-7` produce
/// `catalog/segments/seg-7/membership.bin`. This is an S3 key, not an HTTP path.
#[must_use]
pub fn membership_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/membership.bin")
}

/// Serializes membership entries in deterministic vector-ID order.
///
/// The caller supplies cluster indexes, while this function owns canonical
/// ordering and the versioned little-endian layout. It does not validate that
/// indexes are below `cluster_count`; [`deserialize_membership`] performs that
/// check when bytes are consumed.
///
/// # Parameters
///
/// - `cluster_count`: Number of logical clusters addressable by entries.
/// - `entries`: Borrowed `(ID, cluster index)` pairs in any order. Valid
///   artifacts require unique IDs and indexes below `cluster_count`. IDs are
///   cloned into a temporary vector so the caller's order remains unchanged.
///
/// # Returns
///
/// Shared [`Bytes`] containing one complete version-1 artifact. Equal sets of
/// valid, unique entries produce equal bytes regardless of their input order.
///
/// # Panics
///
/// Panics if an ID needs more than `u16::MAX` UTF-8 bytes or if the calculated
/// artifact size overflows `usize`. The build path treats those as violated
/// segment-format limits rather than emitting a truncated artifact.
///
/// # Performance
///
/// Clones and sorts all entries in `O(e log e)` time, then allocates one output
/// buffer sized to the encoded artifact.
///
/// # Examples
///
/// Entries `[("b", 1), ("a", 0)]` are written as `a` followed by `b`.
/// Serializing the reversed input produces byte-identical output.
///
/// # Rust Notes for Java/C Engineers
///
/// [`Bytes::from`] moves the completed `Vec<u8>` buffer into reference-counted
/// immutable storage without copying its payload. Java has a similar conceptual
/// result in a read-only byte buffer, while C would normally transfer ownership
/// manually and record who must free it.
#[must_use]
pub fn serialize_membership(cluster_count: u32, entries: &[(String, u32)]) -> Bytes {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|left, right| left.0.cmp(&right.0));

    let payload_len = sorted.iter().fold(HEADER_LEN, |acc, (id, _)| {
        let id_len = id.len();
        assert!(
            id_len <= u16::MAX as usize,
            "membership id length exceeds u16: id={id}, len={id_len}"
        );
        acc.checked_add(2 + id_len + 4)
            .unwrap_or_else(|| panic!("membership artifact size overflows"))
    });

    let mut buf = Vec::with_capacity(payload_len);
    buf.extend_from_slice(MEMBERSHIP_MAGIC);
    buf.extend_from_slice(&MEMBERSHIP_VERSION.to_le_bytes());
    buf.extend_from_slice(&cluster_count.to_le_bytes());
    buf.extend_from_slice(&(sorted.len() as u64).to_le_bytes());
    for (id, cluster_idx) in sorted {
        let id_bytes = id.as_bytes();
        buf.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(id_bytes);
        buf.extend_from_slice(&cluster_idx.to_le_bytes());
    }
    debug_assert_eq!(buf.len(), payload_len);
    Bytes::from(buf)
}

/// Builds manifest metadata and bytes from cluster-ordered vector IDs.
///
/// Each outer position is the cluster index; its strings become entries for
/// that cluster. Serialization then sorts all IDs globally so the artifact is
/// deterministic and searchable independently of cluster iteration order.
///
/// # Parameters
///
/// - `namespace`: Namespace prefix used only to construct the artifact key.
/// - `segment_id`: New immutable segment identifier.
/// - `cluster_ids`: Vector IDs grouped by logical cluster index.
///
/// # Returns
///
/// A [`MembershipRef`] describing the future object and its complete bytes.
/// This function does not upload or publish either value.
///
/// # Errors
///
/// Returns [`ZeppelinError::Membership`] if cluster count, cluster indexes, or
/// total entry count cannot fit their persisted/in-memory integer widths. No
/// external state is changed.
///
/// # Panics
///
/// Inherits [`serialize_membership`]'s ID-length and artifact-size limits.
///
/// # Consistency
///
/// Creating bytes does not make the map authoritative. The caller must upload
/// the immutable object and publish the containing segment through manifest
/// compare-and-swap.
///
/// # Performance
///
/// Clones every ID once while flattening, then serialization clones and sorts
/// the flattened entries before producing the final byte buffer.
///
/// # Examples
///
/// Given cluster 0 IDs `[a, c]` and cluster 1 ID `[b]`, the result describes
/// three entries and serializes them as `(a, 0), (b, 1), (c, 0)`.
pub(crate) fn build_membership_artifact(
    namespace: &str,
    segment_id: &str,
    cluster_ids: &[Vec<String>],
) -> Result<(MembershipRef, Bytes)> {
    let cluster_count = u32::try_from(cluster_ids.len()).map_err(|_| {
        ZeppelinError::Membership(format!(
            "membership cluster_count does not fit in u32: {}",
            cluster_ids.len()
        ))
    })?;
    let entry_count = cluster_ids
        .iter()
        .map(Vec::len)
        .try_fold(0usize, |acc, len| {
            acc.checked_add(len)
                .ok_or_else(|| ZeppelinError::Membership("membership entry_count overflows".into()))
        })?;
    let mut entries = Vec::with_capacity(entry_count);
    for (cluster_idx, ids) in cluster_ids.iter().enumerate() {
        let cluster_idx = u32::try_from(cluster_idx).map_err(|_| {
            ZeppelinError::Membership(format!(
                "membership cluster index does not fit in u32: {cluster_idx}"
            ))
        })?;
        for id in ids {
            entries.push((id.clone(), cluster_idx));
        }
    }

    let bytes = serialize_membership(cluster_count, &entries);
    let membership_ref = MembershipRef {
        key: membership_key(namespace, segment_id),
        size_bytes: bytes.len() as u64,
        entry_count: entries.len() as u64,
    };
    Ok((membership_ref, bytes))
}

/// Decodes a complete membership artifact and rejects incompatible corruption.
///
/// Validation covers the signature, exact supported version, checked lengths,
/// UTF-8 IDs, strict ID ordering, cluster-index bounds, declared entry count,
/// and absence of trailing bytes. Failure is explicit; callers never receive a
/// partial map.
///
/// # Parameters
///
/// - `data`: Complete borrowed object bytes loaded by the caller.
///
/// # Returns
///
/// Owned [`MembershipData`] whose IDs are unique, sorted, and safe to use as
/// cluster lookups after the input buffer is released.
///
/// # Errors
///
/// Returns [`ZeppelinError::Membership`] for a short or truncated object,
/// wrong magic, unsupported version, impossible entry count, invalid UTF-8,
/// duplicate/out-of-order IDs, out-of-range cluster indexes, integer-width
/// overflow, or trailing bytes. No partial entries escape.
///
/// # Performance
///
/// Performs one linear pass over the artifact and allocates one `String` per
/// entry plus the result vector. This function performs no object-store I/O.
///
/// # Examples
///
/// A valid artifact with IDs `a` and `b` returns those two sorted entries. If
/// the object is truncated after `b`'s length field, decoding returns an error
/// rather than treating `b` as absent.
pub fn deserialize_membership(data: &[u8]) -> Result<MembershipData> {
    if data.len() < HEADER_LEN {
        return Err(ZeppelinError::Membership(
            "membership blob too small for header".into(),
        ));
    }
    if !data.starts_with(MEMBERSHIP_MAGIC) {
        return Err(ZeppelinError::Membership(
            "membership magic mismatch".into(),
        ));
    }

    let mut reader = MembershipReader::new(data);
    reader.skip(MEMBERSHIP_MAGIC.len(), "membership magic")?;
    let version = reader.read_u32("membership version")?;
    if version != MEMBERSHIP_VERSION {
        return Err(ZeppelinError::Membership(format!(
            "unsupported membership version: {version}"
        )));
    }
    let cluster_count = reader.read_u32("membership cluster_count")?;
    let entry_count = reader.read_u64("membership entry_count")?;
    let remaining = data.len() - reader.offset;
    if entry_count > (remaining / 6) as u64 {
        return Err(ZeppelinError::Membership(format!(
            "membership entry_count {entry_count} exceeds remaining bytes {remaining}"
        )));
    }
    let entry_capacity = usize::try_from(entry_count).map_err(|_| {
        ZeppelinError::Membership(format!(
            "membership entry_count does not fit in usize: {entry_count}"
        ))
    })?;

    let mut entries = Vec::with_capacity(entry_capacity);
    let mut previous_id: Option<String> = None;
    for _ in 0..entry_count {
        let id_len = reader.read_u16("membership id_len")? as usize;
        let id_bytes = reader.read_bytes(id_len, "membership id")?;
        let id = std::str::from_utf8(id_bytes)
            .map_err(|e| ZeppelinError::Membership(format!("membership id is not utf8: {e}")))?
            .to_string();
        if let Some(previous) = previous_id.as_deref() {
            if previous >= id.as_str() {
                return Err(ZeppelinError::Membership(format!(
                    "membership ids are not strictly sorted: previous={previous}, current={id}"
                )));
            }
        }
        let cluster_idx = reader.read_u32("membership cluster_idx")?;
        if cluster_idx >= cluster_count {
            return Err(ZeppelinError::Membership(format!(
                "membership cluster_idx {cluster_idx} outside cluster_count {cluster_count}"
            )));
        }
        previous_id = Some(id.clone());
        entries.push((id, cluster_idx));
    }

    if reader.offset != data.len() {
        return Err(ZeppelinError::Membership(format!(
            "membership blob has trailing bytes: {}",
            data.len() - reader.offset
        )));
    }

    Ok(MembershipData {
        cluster_count,
        entries,
    })
}

/// Bounds-checked cursor over borrowed membership bytes.
///
/// The reader advances monotonically. Its lifetime `'a` ensures slices returned
/// by [`MembershipReader::read_bytes`] cannot outlive `data`.
struct MembershipReader<'a> {
    /// Complete borrowed artifact backing every returned byte slice.
    data: &'a [u8],
    /// Offset of the next unread byte.
    offset: usize,
}

impl<'a> MembershipReader<'a> {
    /// Starts a cursor at the first byte of an artifact.
    ///
    /// # Parameters
    ///
    /// - `data`: Borrowed bytes retained for the cursor's lifetime.
    ///
    /// # Returns
    ///
    /// A reader with offset zero and no allocation.
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    /// Advances over a fixed field after checking its bounds.
    ///
    /// # Parameters
    ///
    /// - `len`: Number of bytes to consume.
    /// - `label`: Field name included in a precise error.
    ///
    /// # Errors
    ///
    /// Returns a membership error if offset arithmetic overflows or the field
    /// extends beyond the artifact.
    fn skip(&mut self, len: usize, label: &str) -> Result<()> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| ZeppelinError::Membership(format!("{label} offset overflows")))?;
        if end > self.data.len() {
            return Err(ZeppelinError::Membership(format!("{label} truncated")));
        }
        self.offset = end;
        Ok(())
    }

    /// Borrows the next `len` bytes and advances the cursor.
    ///
    /// # Parameters
    ///
    /// - `len`: Requested field length.
    /// - `label`: Domain field name used in failure messages.
    ///
    /// # Returns
    ///
    /// A slice tied to the original artifact lifetime, without copying bytes.
    ///
    /// # Errors
    ///
    /// Returns a membership error for overflow or truncation; the cursor moves
    /// only after a valid range is found.
    fn read_bytes(&mut self, len: usize, label: &str) -> Result<&'a [u8]> {
        let start = self.offset;
        let end = start
            .checked_add(len)
            .ok_or_else(|| ZeppelinError::Membership(format!("{label} offset overflows")))?;
        let bytes = self
            .data
            .get(start..end)
            .ok_or_else(|| ZeppelinError::Membership(format!("{label} truncated")))?;
        self.offset = end;
        Ok(bytes)
    }

    /// Reads one little-endian 16-bit integer field.
    ///
    /// `label` identifies the field on truncation or conversion failure. The
    /// returned value is host-endian and the cursor advances by two bytes.
    fn read_u16(&mut self, label: &str) -> Result<u16> {
        let bytes = self.read_bytes(2, label)?;
        Ok(u16::from_le_bytes(bytes.try_into().map_err(|_| {
            ZeppelinError::Membership(format!("{label} parse error"))
        })?))
    }

    /// Reads one little-endian 32-bit integer field.
    ///
    /// `label` identifies the field on truncation or conversion failure. The
    /// returned value is host-endian and the cursor advances by four bytes.
    fn read_u32(&mut self, label: &str) -> Result<u32> {
        let bytes = self.read_bytes(4, label)?;
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
            ZeppelinError::Membership(format!("{label} parse error"))
        })?))
    }

    /// Reads one little-endian 64-bit integer field.
    ///
    /// `label` identifies the field on truncation or conversion failure. The
    /// returned value is host-endian and the cursor advances by eight bytes.
    fn read_u64(&mut self, label: &str) -> Result<u64> {
        let bytes = self.read_bytes(8, label)?;
        Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
            ZeppelinError::Membership(format!("{label} parse error"))
        })?))
    }
}

/// Unit tests for deterministic encoding and fail-loud corruption handling.
#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    /// Serialization canonicalizes entry order and round-trips all ownership data.
    #[test]
    fn membership_roundtrip_sorts_entries_by_id() {
        let bytes = serialize_membership(
            3,
            &[
                ("vec_c".to_string(), 2),
                ("vec_a".to_string(), 0),
                ("vec_b".to_string(), 1),
            ],
        );

        let decoded = deserialize_membership(&bytes).unwrap();
        assert_eq!(decoded.cluster_count, 3);
        assert_eq!(
            decoded.entries,
            vec![
                ("vec_a".to_string(), 0),
                ("vec_b".to_string(), 1),
                ("vec_c".to_string(), 2),
            ]
        );

        let bytes_again = serialize_membership(
            3,
            &[
                ("vec_b".to_string(), 1),
                ("vec_c".to_string(), 2),
                ("vec_a".to_string(), 0),
            ],
        );
        assert_eq!(bytes, bytes_again);
    }

    /// Short, wrong-magic, and every truncated prefix return errors without panics.
    #[test]
    fn membership_rejects_malformed_inputs_without_panicking() {
        assert!(deserialize_membership(b"bad").is_err());

        let mut wrong_magic = serialize_membership(1, &[("vec".to_string(), 0)]).to_vec();
        wrong_magic[0] = b'X';
        assert!(deserialize_membership(&wrong_magic).is_err());

        let bytes = serialize_membership(1, &[("vec".to_string(), 0)]);
        for len in 0..bytes.len() {
            assert!(
                deserialize_membership(&bytes[..len]).is_err(),
                "truncated len {len} must return Err"
            );
        }
    }
}
