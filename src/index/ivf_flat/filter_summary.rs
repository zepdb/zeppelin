//! Segment-wide exact cardinalities for bitmap-indexed equality predicates.
//!
//! Compaction builds one [`FilterCardinalitySummary`] from the exact
//! [`ClusterBitmapIndex`] values that it writes (or carries) for every logical
//! cluster. The summary records which clusters contain each typed bitmap key
//! and how many segment rows match it. It deliberately does not store ordered
//! numeric keys or complements: range and negation predicates remain outside
//! this artifact's contract.
//!
//! The immutable wire format is `ZFCS`, version byte `1`, followed by a
//! MessagePack payload. All persisted maps and sets are ordered so identical
//! logical input produces identical bytes. A field appears in
//! [`FilterCardinalitySummary::covered_fields`] only when every logical cluster
//! has an exact bitmap for it and the configured value-count and byte bounds
//! retain it. Absence therefore means "unknown", never "zero matches".

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::error::{Result, ZeppelinError};
use crate::index::bitmap::{BitmapKey, ClusterBitmapIndex};

/// Four-byte discriminator for a filter-cardinality summary.
pub const FILTER_SUMMARY_MAGIC: &[u8; 4] = b"ZFCS";
/// Wire version for the MessagePack summary payload.
pub const FILTER_SUMMARY_VERSION: u8 = 1;

/// Exact location and row count for one typed value across a segment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValueCardinality {
    /// Logical cluster indexes containing at least one matching row.
    pub clusters: RoaringBitmap,
    /// Number of matching rows across the complete segment.
    pub total: u32,
}

/// Exact per-value cardinalities for one completely covered attribute field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldCardinality {
    /// Typed bitmap key to exact segment-wide location and row count.
    pub values: BTreeMap<BitmapKey, ValueCardinality>,
    /// Number of segment rows where this field is present.
    pub field_total_present: u32,
}

/// Bounded segment-wide summary derived from exact per-cluster bitmap indexes.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FilterCardinalitySummary {
    /// Retained fields and their exact typed-value cardinalities.
    pub fields: BTreeMap<String, FieldCardinality>,
    /// Fields for which the summary has exact, complete segment knowledge.
    pub covered_fields: BTreeSet<String>,
}

/// Result of applying the cardinality and encoded-byte bounds during a build.
#[derive(Debug)]
pub(crate) struct BuiltFilterCardinalitySummary {
    /// Exact bounded summary retained for the resident IVF handle.
    pub summary: FilterCardinalitySummary,
    /// Versioned bytes embedded verbatim in the segment bootstrap.
    pub bytes: Bytes,
    /// Complete fields before the summary-specific bounds were applied.
    pub eligible_fields: BTreeSet<String>,
    /// Fields omitted because their segment-wide distinct count exceeded the cap.
    pub skipped_high_cardinality_fields: Vec<String>,
    /// Fields removed, highest-cardinality first, to satisfy the byte cap.
    pub dropped_for_size_fields: Vec<String>,
}

impl FilterCardinalitySummary {
    /// Serializes this summary as `ZFCS`, version `1`, and MessagePack.
    ///
    /// Ordered collections make the output deterministic. This method only
    /// constructs immutable candidate bytes; it does not write or publish an
    /// object.
    pub fn to_bytes(&self) -> Result<Bytes> {
        validate_summary(self)?;
        let mut bytes = Vec::with_capacity(FILTER_SUMMARY_MAGIC.len() + 1);
        bytes.extend_from_slice(FILTER_SUMMARY_MAGIC);
        bytes.push(FILTER_SUMMARY_VERSION);
        rmp_serde::encode::write(&mut bytes, self).map_err(|error| {
            ZeppelinError::Serialization(format!("filter cardinality summary serialize: {error}"))
        })?;
        Ok(Bytes::from(bytes))
    }

    /// Strictly validates and decodes one complete `ZFCS` version-1 payload.
    ///
    /// Unknown versions are rejected before MessagePack decoding. The decoded
    /// coverage set must exactly equal the persisted field-map keys so malformed
    /// bytes cannot turn missing knowledge into an asserted zero.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < FILTER_SUMMARY_MAGIC.len() + 1 {
            return Err(ZeppelinError::Index(
                "filter cardinality summary data too short".to_string(),
            ));
        }
        if &data[..FILTER_SUMMARY_MAGIC.len()] != FILTER_SUMMARY_MAGIC {
            return Err(ZeppelinError::Index(format!(
                "invalid filter cardinality summary magic: expected ZFCS, got {:?}",
                &data[..FILTER_SUMMARY_MAGIC.len()]
            )));
        }
        let version = data[FILTER_SUMMARY_MAGIC.len()];
        if version != FILTER_SUMMARY_VERSION {
            return Err(ZeppelinError::Index(format!(
                "unsupported filter cardinality summary version: expected {FILTER_SUMMARY_VERSION}, got {version}"
            )));
        }
        let summary: Self = rmp_serde::from_slice(&data[FILTER_SUMMARY_MAGIC.len() + 1..])
            .map_err(|error| {
                ZeppelinError::Serialization(format!(
                    "filter cardinality summary deserialize: {error}"
                ))
            })?;
        validate_summary(&summary)?;
        Ok(summary)
    }
}

/// Builds an exact bounded summary from every logical cluster bitmap.
///
/// Eligible fields come from the already-computed complete-bitmap set and must
/// exist in every supplied cluster. Fields above `max_values_per_field` are
/// skipped. If the versioned encoded artifact remains above `max_bytes`, the
/// highest-cardinality retained field is removed until it fits; ties are removed
/// in reverse lexical order so the result remains deterministic.
pub(crate) fn build_filter_cardinality_summary(
    cluster_indexes: &[ClusterBitmapIndex],
    complete_bitmap_fields: &BTreeSet<String>,
    max_values_per_field: usize,
    max_bytes: usize,
) -> Result<BuiltFilterCardinalitySummary> {
    let eligible_fields = complete_bitmap_fields.clone();
    let mut fields = BTreeMap::new();
    let mut skipped_high_cardinality_fields = Vec::new();

    for field_name in &eligible_fields {
        let mut values: BTreeMap<BitmapKey, ValueCardinality> = BTreeMap::new();
        let mut field_total_present = 0u32;
        let mut exceeds_value_cap = false;

        'clusters: for (cluster_idx, index) in cluster_indexes.iter().enumerate() {
            let cluster_idx = u32::try_from(cluster_idx).map_err(|_| {
                ZeppelinError::Index(format!(
                    "filter cardinality summary cluster index exceeds u32: {cluster_idx}"
                ))
            })?;
            let field = index.fields.get(field_name).ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "filter cardinality summary complete field disappeared: {field_name}"
                ))
            })?;
            field_total_present = checked_add_bitmap_len(
                field_total_present,
                field.present.len(),
                field_name,
                "present rows",
            )?;

            for (key, rows) in &field.values {
                if rows.is_empty() {
                    continue;
                }
                let entry = values
                    .entry(key.clone())
                    .or_insert_with(|| ValueCardinality {
                        clusters: RoaringBitmap::new(),
                        total: 0,
                    });
                entry.clusters.insert(cluster_idx);
                entry.total =
                    checked_add_bitmap_len(entry.total, rows.len(), field_name, "value rows")?;
                if values.len() > max_values_per_field {
                    exceeds_value_cap = true;
                    break 'clusters;
                }
            }
        }

        if exceeds_value_cap {
            skipped_high_cardinality_fields.push(field_name.clone());
            continue;
        }
        fields.insert(
            field_name.clone(),
            FieldCardinality {
                values,
                field_total_present,
            },
        );
    }

    let mut summary = FilterCardinalitySummary {
        covered_fields: fields.keys().cloned().collect(),
        fields,
    };
    let mut dropped_for_size_fields = Vec::new();
    loop {
        let bytes = summary.to_bytes()?;
        if bytes.len() <= max_bytes {
            return Ok(BuiltFilterCardinalitySummary {
                summary,
                bytes,
                eligible_fields,
                skipped_high_cardinality_fields,
                dropped_for_size_fields,
            });
        }

        let field_to_drop = summary
            .fields
            .iter()
            .max_by(|(left_name, left), (right_name, right)| {
                left.values
                    .len()
                    .cmp(&right.values.len())
                    .then_with(|| left_name.cmp(right_name))
            })
            .map(|(name, _)| name.clone())
            .ok_or_else(|| {
                ZeppelinError::Index(format!(
                    "empty filter cardinality summary is {} bytes and exceeds configured maximum {max_bytes}",
                    bytes.len()
                ))
            })?;
        summary.fields.remove(&field_to_drop);
        summary.covered_fields.remove(&field_to_drop);
        dropped_for_size_fields.push(field_to_drop);
    }
}

#[cfg(test)]
fn complete_fields(cluster_indexes: &[ClusterBitmapIndex]) -> BTreeSet<String> {
    let Some(first) = cluster_indexes.first() else {
        return BTreeSet::new();
    };
    let mut complete: BTreeSet<String> = first.fields.keys().cloned().collect();
    for index in &cluster_indexes[1..] {
        complete.retain(|field| index.fields.contains_key(field));
    }
    complete
}

fn checked_add_bitmap_len(
    current: u32,
    additional: u64,
    field_name: &str,
    label: &str,
) -> Result<u32> {
    let additional = u32::try_from(additional).map_err(|_| {
        ZeppelinError::Index(format!(
            "filter cardinality summary {label} exceed u32 for field {field_name}"
        ))
    })?;
    current.checked_add(additional).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "filter cardinality summary {label} overflow u32 for field {field_name}"
        ))
    })
}

fn validate_summary(summary: &FilterCardinalitySummary) -> Result<()> {
    let field_names: BTreeSet<String> = summary.fields.keys().cloned().collect();
    if summary.covered_fields != field_names {
        return Err(ZeppelinError::Index(
            "filter cardinality summary covered fields do not match field entries".to_string(),
        ));
    }
    for (field_name, field) in &summary.fields {
        for (key, value) in &field.values {
            if value.total == 0 || value.clusters.is_empty() {
                return Err(ZeppelinError::Index(format!(
                    "filter cardinality summary field {field_name} value {} has empty cardinality",
                    key.0
                )));
            }
        }
    }
    Ok(())
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::index::bitmap::build::build_cluster_bitmaps;
    use crate::types::AttributeValue;

    use super::*;

    fn cluster(rows: Vec<HashMap<String, AttributeValue>>) -> ClusterBitmapIndex {
        let refs = rows.iter().map(Some).collect::<Vec<_>>();
        build_cluster_bitmaps(&refs)
    }

    fn build_summary(
        indexes: Vec<ClusterBitmapIndex>,
        max_values_per_field: usize,
        max_bytes: usize,
    ) -> Result<BuiltFilterCardinalitySummary> {
        let complete = complete_fields(&indexes);
        build_filter_cardinality_summary(&indexes, &complete, max_values_per_field, max_bytes)
    }

    #[test]
    fn builds_exact_totals_and_cluster_locations() {
        let first = cluster(vec![
            HashMap::from([
                (
                    "color".to_string(),
                    AttributeValue::String("red".to_string()),
                ),
                ("only_first".to_string(), AttributeValue::Bool(true)),
            ]),
            HashMap::from([(
                "color".to_string(),
                AttributeValue::String("blue".to_string()),
            )]),
            HashMap::from([(
                "color".to_string(),
                AttributeValue::String("red".to_string()),
            )]),
        ]);
        let second = cluster(vec![
            HashMap::from([(
                "color".to_string(),
                AttributeValue::String("red".to_string()),
            )]),
            HashMap::from([(
                "color".to_string(),
                AttributeValue::String("green".to_string()),
            )]),
        ]);

        let built =
            build_summary(vec![first, second], 4096, 1 << 20).expect("summary build must succeed");
        assert_eq!(built.eligible_fields, BTreeSet::from(["color".to_string()]));
        assert_eq!(built.summary.covered_fields, built.eligible_fields);
        let color = &built.summary.fields["color"];
        assert_eq!(color.field_total_present, 5);
        assert_eq!(color.values[&BitmapKey("s:red".to_string())].total, 3);
        assert_eq!(
            color.values[&BitmapKey("s:red".to_string())].clusters,
            RoaringBitmap::from_iter([0, 1])
        );
        assert_eq!(color.values[&BitmapKey("s:blue".to_string())].total, 1);
        assert_eq!(
            color.values[&BitmapKey("s:green".to_string())].clusters,
            RoaringBitmap::from_iter([1])
        );
    }

    #[test]
    fn preserves_typed_bitmap_keys() {
        let index = cluster(vec![HashMap::from([
            ("integer".to_string(), AttributeValue::Integer(1)),
            (
                "string".to_string(),
                AttributeValue::String("1".to_string()),
            ),
        ])]);
        let built = build_summary(vec![index], 4096, 1 << 20).unwrap();
        assert!(built.summary.fields["integer"]
            .values
            .contains_key(&BitmapKey("i:1".to_string())));
        assert!(built.summary.fields["string"]
            .values
            .contains_key(&BitmapKey("s:1".to_string())));
    }

    #[test]
    fn skips_segment_wide_high_cardinality_fields() {
        let index = cluster(vec![
            HashMap::from([
                ("wide".to_string(), AttributeValue::String("a".to_string())),
                ("small".to_string(), AttributeValue::Bool(true)),
            ]),
            HashMap::from([
                ("wide".to_string(), AttributeValue::String("b".to_string())),
                ("small".to_string(), AttributeValue::Bool(true)),
            ]),
        ]);
        let built = build_summary(vec![index], 1, 1 << 20).unwrap();
        assert_eq!(built.skipped_high_cardinality_fields, vec!["wide"]);
        assert_eq!(
            built.summary.covered_fields,
            BTreeSet::from(["small".to_string()])
        );
    }

    #[test]
    fn drops_highest_cardinality_field_until_encoded_bytes_fit() {
        let rows = (0..32)
            .map(|value| {
                HashMap::from([
                    ("wide".to_string(), AttributeValue::Integer(value)),
                    ("small".to_string(), AttributeValue::Bool(true)),
                ])
            })
            .collect::<Vec<_>>();
        let index = cluster(rows);
        let unbounded = build_summary(vec![index.clone()], 4096, usize::MAX).unwrap();
        let mut expected_bounded = unbounded.summary.clone();
        expected_bounded.fields.remove("wide");
        expected_bounded.covered_fields.remove("wide");
        let bound = expected_bounded.to_bytes().unwrap().len();
        assert!(unbounded.bytes.len() > bound);

        let bounded = build_summary(vec![index], 4096, bound).unwrap();
        assert_eq!(bounded.dropped_for_size_fields, vec!["wide"]);
        assert_eq!(
            bounded.summary.covered_fields,
            BTreeSet::from(["small".to_string()])
        );
        assert!(bounded.bytes.len() <= bound);
    }

    #[test]
    fn codec_round_trips_deterministically_and_rejects_unknown_versions() {
        assert_eq!(
            FilterCardinalitySummary::default()
                .to_bytes()
                .unwrap()
                .as_ref(),
            b"ZFCS\x01\x92\x80\x90"
        );
        let first = cluster(vec![HashMap::from([(
            "color".to_string(),
            AttributeValue::String("red".to_string()),
        )])]);
        let summary = build_summary(vec![first], 4096, 1 << 20).unwrap().summary;
        let bytes = summary.to_bytes().unwrap();
        assert_eq!(&bytes[..4], FILTER_SUMMARY_MAGIC);
        assert_eq!(bytes[4], FILTER_SUMMARY_VERSION);
        assert_eq!(
            FilterCardinalitySummary::from_bytes(&bytes).unwrap(),
            summary
        );
        assert_eq!(summary.to_bytes().unwrap(), bytes);

        let mut unknown = bytes.to_vec();
        unknown[4] = 99;
        assert_eq!(
            FilterCardinalitySummary::from_bytes(&unknown)
                .unwrap_err()
                .to_string(),
            "index error: unsupported filter cardinality summary version: expected 1, got 99"
        );
    }

    #[test]
    fn byte_cap_smaller_than_empty_artifact_fails_loudly() {
        let error = build_filter_cardinality_summary(&[], &BTreeSet::new(), 4096, 0).unwrap_err();
        assert!(error
            .to_string()
            .contains("empty filter cardinality summary"));
    }
}
