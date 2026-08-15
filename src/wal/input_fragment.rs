//! Immutable typed-input WAL fragments for late-interaction namespaces.
//!
//! These bytes are deliberately separate from dense [`super::fragment::WalFragment`]
//! bytes. Uploading an input fragment does not make it visible; only a root
//! manifest CAS that installs its [`super::manifest::InputFragmentRef`] does.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use xxhash_rust::xxh3::xxh3_64;

use crate::embedding::{ModalityCounts, RetrievalUnitRecord};
use crate::error::{Result, ZeppelinError};
use crate::storage::NamespaceObjectFamily;
use crate::types::{AttributeValue, VectorId};

pub(crate) const INPUT_WAL_MAGIC: &[u8; 4] = b"ZIW1";
pub(crate) const INPUT_WAL_VERSION: u8 = 1;

/// One immutable batch of typed retrieval-unit mutations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EncoderInputWalFragment {
    /// Unique immutable object identity.
    pub id: Ulid,
    /// Ordered typed upserts.
    pub upserts: Vec<RetrievalUnitRecord>,
    /// Ordered deletion tombstones.
    pub deletes: Vec<VectorId>,
    /// xxHash3-64 over canonical ordered operations, excluding [`Self::id`].
    pub checksum: u64,
}

impl EncoderInputWalFragment {
    /// Validate one operation batch and assign a fresh immutable identity.
    pub fn try_new(upserts: Vec<RetrievalUnitRecord>, deletes: Vec<VectorId>) -> Result<Self> {
        let deletes_set = deletes.iter().map(String::as_str).collect::<HashSet<_>>();
        for upsert in &upserts {
            if deletes_set.contains(upsert.id.as_str()) {
                return Err(ZeppelinError::Validation(format!(
                    "retrieval-unit ID '{}' appears in both upserts and deletes",
                    upsert.id
                )));
            }
            let actual = upsert.input.content_hash()?;
            if actual != upsert.content_hash {
                return Err(ZeppelinError::Validation(format!(
                    "retrieval-unit {} content hash does not match its typed input",
                    upsert.id
                )));
            }
        }
        let checksum = Self::compute_checksum(&upserts, &deletes)?;
        Ok(Self {
            id: Ulid::new(),
            upserts,
            deletes,
            checksum,
        })
    }

    fn compute_checksum(upserts: &[RetrievalUnitRecord], deletes: &[VectorId]) -> Result<u64> {
        #[allow(clippy::type_complexity)]
        let canonical = upserts
            .iter()
            .map(|record| {
                let attributes = record.attributes.as_ref().map(|attributes| {
                    attributes
                        .iter()
                        .collect::<BTreeMap<&String, &AttributeValue>>()
                });
                (
                    record.id.as_str(),
                    &record.input,
                    record.content_hash,
                    record.parent_id.as_deref(),
                    record.unit_ordinal,
                    attributes,
                )
            })
            .collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&(&canonical, deletes)).map_err(|error| {
            ZeppelinError::Serialization(format!(
                "canonical input-fragment serialization failed: {error}"
            ))
        })?;
        Ok(xxh3_64(&bytes))
    }

    /// Recompute and verify the ordered payload checksum.
    pub fn validate_checksum(&self) -> Result<()> {
        let expected = Self::compute_checksum(&self.upserts, &self.deletes)?;
        if expected != self.checksum {
            return Err(ZeppelinError::ChecksumMismatch {
                expected,
                actual: self.checksum,
            });
        }
        Ok(())
    }

    /// Serialize as `[ZIW1][version=1][MessagePack payload]`.
    pub fn to_bytes(&self) -> Result<Bytes> {
        // Serialize directly after the magic and version: one buffer, no
        // second copy of the payload.
        let mut bytes = Vec::with_capacity(INPUT_WAL_MAGIC.len() + 1);
        bytes.extend_from_slice(INPUT_WAL_MAGIC);
        bytes.push(INPUT_WAL_VERSION);
        rmp_serde::encode::write(&mut bytes, self).map_err(|error| {
            ZeppelinError::Serialization(format!("input WAL MessagePack serialize failed: {error}"))
        })?;
        Ok(Bytes::from(bytes))
    }

    /// Decode the exact input-WAL framing and verify its payload checksum.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < INPUT_WAL_MAGIC.len() + 1 {
            return Err(ZeppelinError::Serialization(
                "input WAL is shorter than its magic and version header".to_string(),
            ));
        }
        if &data[..INPUT_WAL_MAGIC.len()] != INPUT_WAL_MAGIC {
            return Err(ZeppelinError::Serialization(
                "input WAL has invalid ZIW1 magic".to_string(),
            ));
        }
        let version = data[INPUT_WAL_MAGIC.len()];
        if version != INPUT_WAL_VERSION {
            return Err(ZeppelinError::Serialization(format!(
                "unsupported input WAL version {version}"
            )));
        }
        let fragment =
            rmp_serde::from_slice::<Self>(&data[INPUT_WAL_MAGIC.len() + 1..]).map_err(|error| {
                ZeppelinError::Serialization(format!(
                    "input WAL MessagePack deserialize failed: {error}"
                ))
            })?;
        fragment.validate_checksum()?;
        Ok(fragment)
    }

    /// Build the immutable object key for this input fragment.
    #[must_use]
    pub fn object_store_key(namespace: &str, id: &Ulid) -> String {
        format!(
            "{}{id}.wal",
            NamespaceObjectFamily::InputWal.namespace_prefix(namespace)
        )
    }

    /// Return the number of typed upserts and tombstones.
    #[must_use]
    pub fn operation_count(&self) -> usize {
        self.upserts.len() + self.deletes.len()
    }

    /// Count typed upserts by modality.
    #[must_use]
    pub fn modality_counts(&self) -> ModalityCounts {
        ModalityCounts::from_records(&self.upserts)
    }

    /// Sum inline text and referenced encoded-image bytes.
    pub fn referenced_content_bytes(&self) -> Result<u64> {
        self.upserts.iter().try_fold(0_u64, |total, record| {
            total
                .checked_add(record.input.referenced_content_bytes()?)
                .ok_or_else(|| {
                    ZeppelinError::Validation(
                        "input-fragment referenced byte count exceeds u64".to_string(),
                    )
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::embedding::{EncoderInputRef, RetrievalUnitRecord, TextContentRef};
    use crate::types::AttributeValue;

    use super::{EncoderInputWalFragment, INPUT_WAL_MAGIC, INPUT_WAL_VERSION};

    fn text_record(attributes: HashMap<String, AttributeValue>) -> RetrievalUnitRecord {
        let input = EncoderInputRef::Text {
            content: TextContentRef::Inline("fixture text".to_string()),
        };
        RetrievalUnitRecord {
            id: "unit-1".to_string(),
            content_hash: input.content_hash().unwrap(),
            input,
            parent_id: Some("page-1".to_string()),
            unit_ordinal: Some(3),
            attributes: Some(attributes),
        }
    }

    #[test]
    fn input_wal_round_trip_uses_own_frozen_framing() {
        let attributes = HashMap::from([
            (
                "title".to_string(),
                AttributeValue::String("Zeppelin".to_string()),
            ),
            ("ordinal".to_string(), AttributeValue::Integer(3)),
        ]);
        let fragment =
            EncoderInputWalFragment::try_new(vec![text_record(attributes)], vec!["old".into()])
                .unwrap();

        let bytes = fragment.to_bytes().unwrap();
        assert_eq!(&bytes[..4], INPUT_WAL_MAGIC);
        assert_eq!(bytes[4], INPUT_WAL_VERSION);
        assert_eq!(
            EncoderInputWalFragment::from_bytes(&bytes).unwrap(),
            fragment
        );
    }

    #[test]
    fn checksum_canonicalizes_attribute_maps_and_excludes_ulid() {
        let first = HashMap::from([
            ("a".to_string(), AttributeValue::Integer(1)),
            ("b".to_string(), AttributeValue::Bool(true)),
        ]);
        let second = HashMap::from([
            ("b".to_string(), AttributeValue::Bool(true)),
            ("a".to_string(), AttributeValue::Integer(1)),
        ]);
        let left = EncoderInputWalFragment::try_new(vec![text_record(first)], Vec::new()).unwrap();
        let right =
            EncoderInputWalFragment::try_new(vec![text_record(second)], Vec::new()).unwrap();

        assert_ne!(left.id, right.id);
        assert_eq!(left.checksum, right.checksum);
    }

    #[test]
    fn checked_decode_rejects_payload_tampering() {
        let mut fragment =
            EncoderInputWalFragment::try_new(vec![text_record(HashMap::new())], Vec::new())
                .unwrap();
        fragment.upserts[0].parent_id = Some("changed".to_string());
        let bytes = fragment.to_bytes().unwrap();

        assert!(EncoderInputWalFragment::from_bytes(&bytes).is_err());
    }
}
