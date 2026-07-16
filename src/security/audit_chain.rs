//! Hash-chain and signed-anchor verification for durable audit streams.

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Result as ZeppelinResult, ZeppelinError};
use crate::storage::ZeppelinStore;

use super::AuditRecord;

const TERMINAL_SEAL_FORMAT: &str = "zeppelin_audit_terminal_seal_v1";

/// Signed terminal commitment for one `(UTC day, node)` audit stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditDayAnchor {
    /// UTC calendar day in `YYYY-MM-DD` form.
    pub day: String,
    /// Exact audit writer node identity.
    pub node_id: String,
    /// SHA-256 of the last canonical record, or `None` for an empty stream.
    pub last_hash: Option<String>,
    /// Number of records committed to the chain.
    pub record_count: u64,
    /// Published node signer identity.
    pub signer_node: String,
    /// Ed25519 signature over all preceding fields.
    pub signature: Vec<u8>,
}

impl AuditDayAnchor {
    pub(crate) fn unsigned_bytes(&self) -> ZeppelinResult<Vec<u8>> {
        let mut unsigned = self.clone();
        unsigned.signature.clear();
        super::receipt::canonical_json_bytes(&unsigned)
    }
}

/// First detected mutation in a persisted audit stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditChainDivergence {
    /// A JSONL record could not be decoded canonically.
    RecordDecode,
    /// A record named a different node or UTC day.
    RecordIdentity,
    /// A record did not name the prior record's exact hash.
    PreviousHash,
    /// A record omitted or broke the one-based cumulative chain position.
    RecordPosition,
    /// The create-only terminal slot is absent.
    TerminalSealMissing,
    /// The terminal slot does not commit to the recomputed chain tail.
    TerminalSealInvalid,
    /// The immutable day anchor is absent.
    AnchorMissing,
    /// The anchor signature does not verify.
    AnchorSignature,
    /// Anchor node/day metadata does not match the selected stream.
    AnchorIdentity,
    /// Anchor count does not equal the recomputed stream length.
    AnchorCount,
    /// Anchor terminal hash does not equal the recomputed chain tail.
    AnchorLastHash,
}

/// End-to-end audit verification report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditChainVerification {
    /// True only when records and signed anchor all agree.
    pub valid: bool,
    /// First divergence in object/key/line order.
    pub first_divergence: Option<AuditChainDivergence>,
    /// Number of records accepted before the first divergence.
    pub verified_records: u64,
    /// Key containing the first divergent record, when applicable.
    pub object_key: Option<String>,
    /// Zero-based JSONL line within `object_key`, when applicable.
    pub line_index: Option<usize>,
}

impl AuditChainVerification {
    fn invalid(
        divergence: AuditChainDivergence,
        verified_records: u64,
        object_key: Option<String>,
        line_index: Option<usize>,
    ) -> Self {
        Self {
            valid: false,
            first_divergence: Some(divergence),
            verified_records,
            object_key,
            line_index,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AuditChainState {
    pub last_hash: Option<String>,
    pub record_count: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedAuditChainTail {
    pub state: AuditChainState,
    pub terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuditTerminalSeal {
    format: String,
    day: String,
    node_id: String,
    last_hash: Option<String>,
    record_count: u64,
}

impl AuditTerminalSeal {
    pub(crate) fn from_state(day: NaiveDate, node_id: &str, state: &AuditChainState) -> Self {
        Self {
            format: TERMINAL_SEAL_FORMAT.to_string(),
            day: day.format("%Y-%m-%d").to_string(),
            node_id: node_id.to_string(),
            last_hash: state.last_hash.clone(),
            record_count: state.record_count,
        }
    }

    pub(crate) fn encode(&self) -> ZeppelinResult<bytes::Bytes> {
        serde_json::to_vec(self)
            .map(bytes::Bytes::from)
            .map_err(|error| ZeppelinError::Serialization(error.to_string()))
    }
}

pub(crate) fn record_hash(record: &AuditRecord) -> ZeppelinResult<String> {
    let bytes = super::receipt::canonical_json_bytes(record)?;
    Ok(hex_sha256(&bytes))
}

pub(crate) fn anchor_key(day: NaiveDate, node_id: &str) -> String {
    format!("_audit/anchors/{}/{node_id}.json", day.format("%Y-%m-%d"))
}

pub(crate) fn audit_slot_key(day: NaiveDate, node_id: &str, first_position: u64) -> String {
    let object_id = ulid::Ulid::from(u128::from(first_position));
    format!(
        "_audit/{}/{node_id}/{object_id}.jsonl",
        day.format("%Y-%m-%d")
    )
}

/// Recover the current state with one recursive LIST and, for a non-empty
/// stream, one GET of only the lexicographically last immutable batch.
///
/// Every persisted record carries its cumulative position. That makes the
/// count available from the tail object itself and avoids an unbounded replay
/// during process startup. Pre-Phase-10 records without a position are rejected
/// explicitly; recovery never guesses zero or scans older objects as a fallback.
pub(crate) async fn load_chain_tail(
    store: &ZeppelinStore,
    day: NaiveDate,
    node_id: &str,
) -> ZeppelinResult<LoadedAuditChainTail> {
    let prefix = format!("_audit/{}/{node_id}/", day.format("%Y-%m-%d"));
    let mut keys = store.list_prefix(&prefix).await?;
    keys.sort();
    let Some(key) = keys.last() else {
        return Ok(LoadedAuditChainTail {
            state: AuditChainState::default(),
            terminal: false,
        });
    };
    let bytes = store.get(key).await?;
    if let Some(seal) = decode_terminal_seal(&bytes)? {
        let state = match keys.iter().rev().nth(1) {
            Some(previous_key) => {
                let previous = store.get(previous_key).await?;
                recover_tail_body(&previous, previous_key, day, node_id)?
            }
            None => AuditChainState::default(),
        };
        validate_terminal_seal(&seal, &state, key, day, node_id)?;
        return Ok(LoadedAuditChainTail {
            state,
            terminal: true,
        });
    }
    Ok(LoadedAuditChainTail {
        state: recover_tail_body(&bytes, key, day, node_id)?,
        terminal: false,
    })
}

pub(crate) fn advance_tail_body(
    bytes: &[u8],
    key: &str,
    day: NaiveDate,
    node_id: &str,
    state: &AuditChainState,
) -> ZeppelinResult<AuditChainState> {
    if decode_terminal_seal(bytes)?.is_some() {
        return Err(ZeppelinError::Serialization(format!(
            "unexpected audit terminal seal while advancing {key}"
        )));
    }
    let mut next = state.clone();
    let mut count = 0usize;
    for (line_index, line) in bytes
        .split(|byte| *byte == b'\n')
        .enumerate()
        .filter(|(_, line)| !line.is_empty())
    {
        let record = decode_record(line, key, line_index)?;
        advance_chain(&mut next, &record, day, node_id).map_err(|divergence| {
            ZeppelinError::Serialization(format!("audit tail {divergence:?} at {key}:{line_index}"))
        })?;
        count += 1;
    }
    if count == 0 {
        return Err(ZeppelinError::Serialization(format!(
            "empty audit tail object {key} is unsupported"
        )));
    }
    Ok(next)
}

fn decode_terminal_seal(bytes: &[u8]) -> ZeppelinResult<Option<AuditTerminalSeal>> {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return Ok(None);
    };
    if value.get("format").and_then(serde_json::Value::as_str) != Some(TERMINAL_SEAL_FORMAT) {
        return Ok(None);
    }
    serde_json::from_value(value).map(Some).map_err(|error| {
        ZeppelinError::Serialization(format!("invalid audit terminal seal: {error}"))
    })
}

fn validate_terminal_seal(
    seal: &AuditTerminalSeal,
    state: &AuditChainState,
    key: &str,
    day: NaiveDate,
    node_id: &str,
) -> ZeppelinResult<()> {
    let next_position = state.record_count.checked_add(1).ok_or_else(|| {
        ZeppelinError::Serialization("audit terminal position overflow".to_string())
    })?;
    if seal.format != TERMINAL_SEAL_FORMAT
        || seal.day != day.format("%Y-%m-%d").to_string()
        || seal.node_id != node_id
        || seal.last_hash != state.last_hash
        || seal.record_count != state.record_count
        || key != audit_slot_key(day, node_id, next_position)
    {
        return Err(ZeppelinError::Serialization(format!(
            "audit terminal seal disagrees with chain tail at {key}"
        )));
    }
    Ok(())
}

fn recover_tail_body(
    bytes: &[u8],
    key: &str,
    day: NaiveDate,
    node_id: &str,
) -> ZeppelinResult<AuditChainState> {
    let mut lines = bytes
        .split(|byte| *byte == b'\n')
        .enumerate()
        .filter(|(_, line)| !line.is_empty());
    let Some((first_line_index, first_line)) = lines.next() else {
        return Err(ZeppelinError::Serialization(format!(
            "empty audit tail object {key} is unsupported"
        )));
    };
    let first: AuditRecord = decode_record(first_line, key, first_line_index)?;
    let first_position = first.chain_position.ok_or_else(|| {
        ZeppelinError::Serialization(format!(
            "audit record at {key}:{first_line_index} has no chain_position; pre-Phase-10 audit streams require an explicit offline migration"
        ))
    })?;
    let prior_count = first_position.get().checked_sub(1).ok_or_else(|| {
        ZeppelinError::Serialization(format!(
            "invalid zero audit chain position at {key}:{first_line_index}"
        ))
    })?;
    if (prior_count == 0) != first.prev_hash.is_none() {
        return Err(ZeppelinError::Serialization(format!(
            "audit tail predecessor presence disagrees with chain_position at {key}:{first_line_index}"
        )));
    }
    let mut state = AuditChainState {
        last_hash: first.prev_hash.clone(),
        record_count: prior_count,
    };
    advance_chain(&mut state, &first, day, node_id).map_err(|divergence| {
        ZeppelinError::Serialization(format!(
            "audit tail {divergence:?} at {key}:{first_line_index}"
        ))
    })?;
    for (line_index, line) in lines {
        let record = decode_record(line, key, line_index)?;
        advance_chain(&mut state, &record, day, node_id).map_err(|divergence| {
            ZeppelinError::Serialization(format!("audit tail {divergence:?} at {key}:{line_index}"))
        })?;
    }
    Ok(state)
}

fn decode_record(line: &[u8], key: &str, line_index: usize) -> ZeppelinResult<AuditRecord> {
    serde_json::from_slice(line).map_err(|error| {
        ZeppelinError::Serialization(format!(
            "invalid audit record at {key}:{line_index}: {error}"
        ))
    })
}

fn advance_chain(
    state: &mut AuditChainState,
    record: &AuditRecord,
    day: NaiveDate,
    node_id: &str,
) -> Result<(), AuditChainDivergence> {
    if record.node_id != node_id || record.ts.date_naive() != day {
        return Err(AuditChainDivergence::RecordIdentity);
    }
    if record.prev_hash != state.last_hash {
        return Err(AuditChainDivergence::PreviousHash);
    }
    let expected_position = state
        .record_count
        .checked_add(1)
        .ok_or(AuditChainDivergence::RecordPosition)?;
    if record.chain_position.map(super::AuditChainPosition::get) != Some(expected_position) {
        return Err(AuditChainDivergence::RecordPosition);
    }
    state.last_hash = Some(record_hash(record).map_err(|_| AuditChainDivergence::RecordDecode)?);
    state.record_count = expected_position;
    Ok(())
}

/// Verify every JSONL record and the immutable signed day anchor.
pub async fn verify_audit_day(
    store: &ZeppelinStore,
    day: NaiveDate,
    node_id: &str,
) -> ZeppelinResult<AuditChainVerification> {
    let prefix = format!("_audit/{}/{node_id}/", day.format("%Y-%m-%d"));
    let mut keys = store.list_prefix(&prefix).await?;
    keys.sort();
    let mut state = AuditChainState::default();
    let mut terminal = false;
    let last_key_index = keys.len().checked_sub(1);
    for (key_index, key) in keys.into_iter().enumerate() {
        let bytes = store.get(&key).await?;
        if let Some(seal) = decode_terminal_seal(&bytes)? {
            if Some(key_index) != last_key_index
                || validate_terminal_seal(&seal, &state, &key, day, node_id).is_err()
            {
                return Ok(AuditChainVerification::invalid(
                    AuditChainDivergence::TerminalSealInvalid,
                    state.record_count,
                    Some(key),
                    None,
                ));
            }
            terminal = true;
            continue;
        }
        if terminal {
            return Ok(AuditChainVerification::invalid(
                AuditChainDivergence::TerminalSealInvalid,
                state.record_count,
                Some(key),
                None,
            ));
        }
        for (line_index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }
            let record: AuditRecord = match serde_json::from_slice(line) {
                Ok(record) => record,
                Err(_) => {
                    return Ok(AuditChainVerification::invalid(
                        AuditChainDivergence::RecordDecode,
                        state.record_count,
                        Some(key),
                        Some(line_index),
                    ));
                }
            };
            if let Err(divergence) = advance_chain(&mut state, &record, day, node_id) {
                return Ok(AuditChainVerification::invalid(
                    divergence,
                    state.record_count,
                    Some(key),
                    Some(line_index),
                ));
            }
        }
    }

    if !terminal {
        return Ok(AuditChainVerification::invalid(
            AuditChainDivergence::TerminalSealMissing,
            state.record_count,
            None,
            None,
        ));
    }

    let key = anchor_key(day, node_id);
    let bytes = match store.get(&key).await {
        Ok(bytes) => bytes,
        Err(ZeppelinError::NotFound { .. }) => {
            return Ok(AuditChainVerification::invalid(
                AuditChainDivergence::AnchorMissing,
                state.record_count,
                None,
                None,
            ));
        }
        Err(error) => return Err(error),
    };
    let anchor: AuditDayAnchor = serde_json::from_slice(&bytes).map_err(|error| {
        ZeppelinError::Serialization(format!("invalid audit anchor {key}: {error}"))
    })?;
    if anchor.day != day.format("%Y-%m-%d").to_string() || anchor.node_id != node_id {
        return Ok(AuditChainVerification::invalid(
            AuditChainDivergence::AnchorIdentity,
            state.record_count,
            None,
            None,
        ));
    }
    if !super::delegation::verify_published_signature(
        store,
        &anchor.signer_node,
        &anchor.unsigned_bytes()?,
        &anchor.signature,
    )
    .await?
    {
        return Ok(AuditChainVerification::invalid(
            AuditChainDivergence::AnchorSignature,
            state.record_count,
            None,
            None,
        ));
    }
    if anchor.record_count != state.record_count {
        return Ok(AuditChainVerification::invalid(
            AuditChainDivergence::AnchorCount,
            state.record_count,
            None,
            None,
        ));
    }
    if anchor.last_hash != state.last_hash {
        return Ok(AuditChainVerification::invalid(
            AuditChainDivergence::AnchorLastHash,
            state.record_count,
            None,
            None,
        ));
    }
    Ok(AuditChainVerification {
        valid: true,
        first_divergence: None,
        verified_records: state.record_count,
        object_key: None,
        line_index: None,
    })
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use proptest::prelude::*;

    use super::{advance_chain, record_hash, AuditChainState};
    use crate::security::{AuditChainPosition, AuditRecord};

    fn chained_records(request_ids: &[String]) -> (Vec<AuditRecord>, AuditChainState) {
        let Some(day) = Utc.with_ymd_and_hms(2026, 7, 15, 12, 0, 0).single() else {
            panic!("property-test timestamp must exist");
        };
        let mut state = AuditChainState::default();
        let mut records = Vec::with_capacity(request_ids.len());
        for request_id in request_ids {
            let mut record = AuditRecord::open_unsafe_boot(day, "property-node");
            record.request_id.clone_from(request_id);
            record.prev_hash = state.last_hash.clone();
            let Some(position) = state
                .record_count
                .checked_add(1)
                .and_then(AuditChainPosition::new)
            else {
                panic!("bounded property sequence must fit");
            };
            record.chain_position = Some(position);
            let Ok(hash) = record_hash(&record) else {
                panic!("record must hash canonically");
            };
            state.last_hash = Some(hash);
            state.record_count = position.get();
            records.push(record);
        }
        (records, state)
    }

    fn differs_from_anchor(records: &[AuditRecord], expected: &AuditChainState) -> bool {
        let Some(day) = Utc.with_ymd_and_hms(2026, 7, 15, 12, 0, 0).single() else {
            panic!("property-test timestamp must exist");
        };
        let day = day.date_naive();
        let mut observed = AuditChainState::default();
        for record in records {
            if advance_chain(&mut observed, record, day, "property-node").is_err() {
                return true;
            }
        }
        observed.record_count != expected.record_count || observed.last_hash != expected.last_hash
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn any_single_record_mutation_deletion_or_reorder_breaks_the_anchored_chain(
            request_ids in proptest::collection::vec("[a-z0-9]{1,24}", 2..48),
            selected in any::<usize>(),
        ) {
            let (records, anchor) = chained_records(&request_ids);
            prop_assert!(!differs_from_anchor(&records, &anchor));

            let index = selected % records.len();
            let mut mutated = records.clone();
            mutated[index].request_id.push_str("-mutated");
            prop_assert!(differs_from_anchor(&mutated, &anchor));

            let mut deleted = records.clone();
            deleted.remove(index);
            prop_assert!(differs_from_anchor(&deleted, &anchor));

            let mut reordered = records;
            let other = (index + 1) % reordered.len();
            reordered.swap(index, other);
            prop_assert!(differs_from_anchor(&reordered, &anchor));
        }
    }
}
