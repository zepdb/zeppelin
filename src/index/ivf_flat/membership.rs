//! Immutable IVF-Flat segment membership artifacts.
//!
//! A membership artifact maps every vector id in a segment to the logical IVF
//! cluster that currently owns it. Stage 2C.1 only produces this artifact; no
//! query or compaction read path consumes it yet.

use bytes::Bytes;

use crate::error::{Result, ZeppelinError};
use crate::wal::manifest::MembershipRef;

/// Magic bytes for IVF-Flat membership artifacts.
pub const MEMBERSHIP_MAGIC: &[u8; 4] = b"ZMB1";

/// Current IVF-Flat membership artifact format version.
pub const MEMBERSHIP_VERSION: u32 = 1;

const HEADER_LEN: usize = 4 + 4 + 4 + 8;

/// Decoded IVF-Flat segment membership data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipData {
    /// Number of logical IVF clusters in the segment.
    pub cluster_count: u32,
    /// Entries sorted by vector id ascending.
    pub entries: Vec<(String, u32)>,
}

/// S3 key for an IVF-Flat segment membership artifact.
#[must_use]
pub fn membership_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/membership.bin")
}

/// Serialize a deterministic IVF-Flat segment membership artifact.
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

/// Deserialize and validate an IVF-Flat segment membership artifact.
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

struct MembershipReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> MembershipReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

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

    fn read_u16(&mut self, label: &str) -> Result<u16> {
        let bytes = self.read_bytes(2, label)?;
        Ok(u16::from_le_bytes(bytes.try_into().map_err(|_| {
            ZeppelinError::Membership(format!("{label} parse error"))
        })?))
    }

    fn read_u32(&mut self, label: &str) -> Result<u32> {
        let bytes = self.read_bytes(4, label)?;
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
            ZeppelinError::Membership(format!("{label} parse error"))
        })?))
    }

    fn read_u64(&mut self, label: &str) -> Result<u64> {
        let bytes = self.read_bytes(8, label)?;
        Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
            ZeppelinError::Membership(format!("{label} parse error"))
        })?))
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

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
