//! Hash-chain and signed-anchor verification for durable audit streams.
//!
//! This file owns the tamper-evidence layer over durable audit: the per-record
//! chaining rule, the deterministic object-key layout that makes stream order
//! recoverable from a LIST, the create-only terminal seal that closes a day, the
//! signed day anchor that commits to the whole day, the crash-recovery tail
//! load, and the offline verifier [`verify_audit_day`].
//!
//! It deliberately does **not** own audit itself. `audit.rs` owns the
//! [`AuditRecord`] schema and its redaction rules; `audit_sink.rs` owns
//! delivery, batching, the writer lease, and every object write; `delegation.rs`
//! owns the signing keys and signature checking; `receipt.rs` owns the canonical
//! JSON encoding that record hashing depends on. Nothing here writes an audit
//! object — it computes hashes and keys, and it checks.
//!
//! ## Where this sits
//!
//! - `audit_sink.rs` is the only production writer. On startup it calls
//!   `load_chain_tail` to recover where the stream left off, `advance_tail_body`
//!   and `record_hash` while staging each batch, `audit_slot_key` to allocate
//!   the next immutable object, `AuditTerminalSeal` to close the day, and
//!   `anchor_key` to place the signed anchor.
//! - [`verify_audit_day`] is the read side, exposed publicly and driven by the
//!   `zeppelin_audit_verify` binary, the security integration tests, and the
//!   adversarial runner's `audit_chain_check` operation.
//!
//! Audit delivery is durable and blocking where policy requires it, and a failed
//! audit writer takes `/readyz` to 503 through `AuditClient::is_healthy`. That
//! is deliberate design, not a bug: a node that cannot record evidence must
//! leave the load balancer rather than keep serving unrecorded requests. Do not
//! make audit best-effort to make a readiness failure go away.
//!
//! ## Persisted artifacts and the chain
//!
//! One stream is one `(UTC day, node id)` pair. Batch objects are immutable and
//! create-only, and their ULID is derived deterministically from the batch's
//! first chain position, so lexicographic key order equals numeric position
//! order — sorting one recursive LIST recovers the stream.
//!
//! ```text
//! _audit/2026-07-24/node-a/
//!   <ulid(1)>.jsonl   r1  prev_hash=None  position=1   h1 = H(r1)
//!                     r2  prev_hash=h1    position=2   h2 = H(r2)
//!   <ulid(3)>.jsonl   r3  prev_hash=h2    position=3   h3 = H(r3)
//!   <ulid(4)>.jsonl   TERMINAL SEAL { last_hash = h3, record_count = 3 }
//!                     create-only, in the slot batch 4 would have used
//!
//! _audit/anchors/2026-07-24/node-a.json
//!                     Ed25519 over { day, node, last_hash = h3,
//!                                    record_count = 3, signer_node }
//! ```
//!
//! `H(r)` is SHA-256 over the record's recursively key-sorted canonical JSON,
//! which includes that record's own `prev_hash` and `chain_position`. Placing
//! the seal in the *next* batch's slot is what excludes an expired writer whose
//! final PUT is still in flight: both would have to create the same key.
//!
//! ## Reading map
//!
//! 1. `advance_chain` — the single acceptance rule for one record; every other
//!    path in this file is built on it.
//! 2. `load_chain_tail` — bounded crash recovery: one LIST, then one GET of the
//!    lexicographically last object (two when that object is the seal).
//! 3. `AuditTerminalSeal` and `validate_terminal_seal` — how a day is closed and
//!    why the seal's key is checked, not just its contents.
//! 4. [`verify_audit_day`] — full-stream verification and the order in which
//!    [`AuditChainDivergence`] values are reported.
//! 5. [`AuditDayAnchor`] — the signed commitment, and `unsigned_bytes` for what
//!    the signature actually covers.
//!
//! ## What the chain proves
//!
//! Against a valid signed anchor, for one `(day, node)` stream:
//!
//! - Any mutation, deletion, reordering, or truncation of the persisted records
//!   is detected. The recomputed tail hash or record count will disagree with
//!   the anchor. The property test at the bottom of this file exercises exactly
//!   those three mutations.
//! - Appending after a day closes is detected: the seal must be the
//!   lexicographically last object and must match the recomputed tail, so any
//!   object beyond it reports `TerminalSealInvalid`.
//! - Every record's position is explicit, so a gap cannot be papered over. A
//!   record with no `chain_position` is rejected outright — pre-Phase-10 streams
//!   need an explicit offline migration, and recovery never guesses zero or
//!   scans older objects as a fallback.
//!
//! What it does **not** prove:
//!
//! - **Nothing links days or nodes.** Each `(day, node)` stream is anchored
//!   independently; there is no chain from one day's anchor to the next and no
//!   registry of which streams should exist. Deleting an entire stream together
//!   with its anchor is only detectable by someone who independently knows that
//!   stream existed.
//! - **The anchor is self-signed by the same node that wrote the records.**
//!   A node holding its own signing key can produce a wholly fabricated but
//!   internally consistent stream. The chain proves after-the-fact tampering by
//!   a party *without* that key; it is not a proof of what actually happened.
//! - **The trust root is the S3-published signer inventory**, resolved through
//!   `delegation::verify_published_signature`, not an external CA or
//!   transparency log.
//! - **An unsealed day is reported invalid, which is not the same as tampered.**
//!   The writer seals and anchors at UTC day rollover and at graceful shutdown,
//!   so verifying the currently open day legitimately yields
//!   `TerminalSealMissing`. An aborted writer also intentionally leaves an
//!   unsealed tail.
//!
//! ## Invariants
//!
//! - **One acceptance rule, applied everywhere.** `advance_chain` requires the
//!   record's node id and UTC date to match the stream, its `prev_hash` to equal
//!   the running tail hash (`None` only at position 1), and its `chain_position`
//!   to be exactly the running count plus one.
//! - **The seal is validated by key as well as content.**
//!   `validate_terminal_seal` checks format, day, node, tail hash, count, *and*
//!   that the object sits at the slot key its recorded count implies.
//! - **Divergences are outcomes; failures are errors.** [`verify_audit_day`]
//!   returns `Ok(AuditChainVerification { valid: false, .. })` for structural
//!   disagreement, locating it by object key and JSONL line. Storage failures,
//!   and an anchor object that exists but cannot be decoded, propagate as
//!   errors instead. Note the asymmetry: an undecodable *record* is the
//!   `RecordDecode` divergence, while an undecodable *anchor* is an error.
//! - **`verified_records` counts what was accepted before the first
//!   divergence**, so a truncation report still says how much of the stream
//!   remains trustworthy.
//! - **Blank lines are tolerated, empty objects are not.** Empty JSONL lines are
//!   skipped so a trailing newline is not a record, but an audit object
//!   containing no records at all is rejected as unsupported.
//!
//! ## Cost
//!
//! `load_chain_tail` is one recursive LIST plus one GET — two when the last
//! object is the seal — regardless of how long the day is. That bound is the
//! reason every record carries its own cumulative position: startup must not
//! replay the day. [`verify_audit_day`] is deliberately the opposite: one LIST,
//! one GET per object, plus the anchor GET, so it is O(stream) and belongs
//! offline or in a scheduled check, never on a request path.
//!
//! ## Rust concepts used here
//!
//! `AuditRecord::chain_position` is an `Option<AuditChainPosition>` wrapping a
//! `NonZeroU64`. Position zero is unrepresentable rather than merely invalid, so
//! "absent" and "zero" cannot be confused — the distinction that separates a
//! legacy record from a corrupt one. Every count step uses `checked_add`, so
//! overflow becomes a typed failure rather than a wrap that would silently
//! restart the chain.
//!
//! `advance_chain` returns `Result<(), AuditChainDivergence>` while its callers
//! return `ZeppelinResult`. That is two error channels on purpose: the inner one
//! is a classification the verifier reports to a caller, and the outer one is a
//! genuine failure. Recovery paths convert the classification into a
//! `Serialization` error because a writer that cannot trust its own tail must
//! stop, whereas the verifier reports the same condition as data.

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
