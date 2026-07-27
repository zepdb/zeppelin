//! Durable, append-only delivery for structured security audit records.
//!
//! [`AuditClient`] is the cheap cloneable request-path handle. It emits the
//! always-on structured tracing event synchronously and, when durable audit is
//! configured, transfers the typed record into an unbounded Tokio channel. The
//! unbounded channel is deliberate: authentication and authorization rejection
//! paths must not await storage and must not silently discard evidence because
//! a bounded queue is temporarily full.
//!
//! [`AuditRuntime`] owns the single writer task in S3-backed mode. That actor
//! serializes records into JSON Lines batches, creates immutable objects below
//! `_audit/`, and acknowledges durable submissions only after the exact object
//! body is authoritative in object storage. One actor also gives each node a
//! deterministic, lexicographically ordered batch IDs from the first chain
//! position. That create-only key is the CAS layer beneath the writer lease:
//! two writers from one recovered tail must contend on the same object. Before
//! anchoring, the writer reserves that same next-position key with a terminal
//! seal, excluding an expired writer whose final PUT is still in flight.
//!
//! ```text
//! request middleware / handler
//!          |
//!          | submit_buffered or submit_durable
//!          v
//!      AuditClient -- structured tracing + outcome metric
//!          |
//!          | unbounded mpsc
//!          v
//!   one audit writer task
//!          |
//!          +-- 256 records
//!          +-- flush interval
//!          +-- durable barrier / explicit flush / shutdown
//!          v
//! _audit/yyyy-mm-dd/node/monotonic-ulid.jsonl
//! ```
//!
//! A failed create is followed by an exact-key authoritative read. If the
//! bytes match, the create is treated as committed despite an ambiguous client
//! error. Otherwise the staged key and bytes remain unchanged for the next
//! retry. The writer never falls back to an unconditional PUT and never
//! replaces a conflicting object.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::error::ZeppelinError;
use crate::storage::{ConditionalPutOutcome, CreateOnlyOutcome, StorageVersion, ZeppelinStore};

use super::audit_chain::{
    advance_tail_body, anchor_key, audit_slot_key, load_chain_tail, record_hash, AuditChainState,
    AuditTerminalSeal,
};
use super::{AuditChainPosition, AuditDayAnchor, AuditRecord};

const MAX_BATCH_RECORDS: usize = 256;
const WRITER_HEAD_RETRIES: usize = 16;
const MIN_WRITER_LEASE_SECS: u64 = 30;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuditWriterHead {
    signer_node: String,
    stream_id: String,
    open_day: String,
    lease_owner: Option<String>,
    lease_expires_at: Option<chrono::DateTime<Utc>>,
}

impl AuditWriterHead {
    fn open_day(&self) -> Result<NaiveDate, AuditSinkError> {
        NaiveDate::parse_from_str(&self.open_day, "%Y-%m-%d").map_err(|error| {
            AuditSinkError::Serialization(format!(
                "invalid authoritative audit writer open_day: {error}"
            ))
        })
    }
}

#[derive(Debug, Clone)]
struct LoadedAuditWriterHead {
    key: String,
    version: StorageVersion,
    document: AuditWriterHead,
}

impl LoadedAuditWriterHead {
    async fn refresh_lease(
        &mut self,
        store: &ZeppelinStore,
        lease_duration: Duration,
    ) -> Result<(), AuditSinkError> {
        let owner = self.document.lease_owner.clone().ok_or_else(|| {
            AuditSinkError::Serialization("active audit writer head has no lease owner".to_string())
        })?;
        if self
            .document
            .lease_expires_at
            .is_none_or(|expires_at| expires_at <= Utc::now())
        {
            return Err(AuditSinkError::WriterAlreadyActive);
        }
        let mut replacement = self.document.clone();
        replacement.lease_owner = Some(owner);
        replacement.lease_expires_at = Some(
            Utc::now()
                + chrono::Duration::from_std(lease_duration).map_err(|error| {
                    AuditSinkError::Serialization(format!(
                        "invalid audit writer lease duration: {error}"
                    ))
                })?,
        );
        let next_version = match store
            .put_if_match_outcome(&self.key, encode_writer_head(&replacement)?, &self.version)
            .await
            .map_err(|error| AuditSinkError::Storage(error.to_string()))?
        {
            ConditionalPutOutcome::Updated { version } => version,
            ConditionalPutOutcome::Conflict => return Err(AuditSinkError::WriterAlreadyActive),
        };
        self.document = replacement;
        if let Some(version) = next_version {
            self.version = version;
            return Ok(());
        }
        let reloaded = read_writer_head(store, &self.document.signer_node)
            .await
            .map_err(|error| AuditSinkError::WriterAuthorityLost(error.to_string()))?
            .ok_or_else(|| {
                AuditSinkError::WriterAuthorityLost(
                    "renewed head disappeared during ETag reconciliation".to_string(),
                )
            })?;
        if reloaded.document != self.document {
            return Err(AuditSinkError::WriterAuthorityLost(
                "renewed head diverged during ETag reconciliation".to_string(),
            ));
        }
        self.version = reloaded.version;
        Ok(())
    }

    async fn advance_open_day(
        &mut self,
        store: &ZeppelinStore,
        day: NaiveDate,
    ) -> Result<(), AuditSinkError> {
        let replacement = AuditWriterHead {
            signer_node: self.document.signer_node.clone(),
            stream_id: self.document.stream_id.clone(),
            open_day: day.format("%Y-%m-%d").to_string(),
            lease_owner: self.document.lease_owner.clone(),
            lease_expires_at: self.document.lease_expires_at,
        };
        let next_version = match store
            .put_if_match_outcome(&self.key, encode_writer_head(&replacement)?, &self.version)
            .await
            .map_err(|error| AuditSinkError::WriterAuthorityLost(error.to_string()))?
        {
            ConditionalPutOutcome::Updated { version } => version,
            ConditionalPutOutcome::Conflict => {
                return Err(AuditSinkError::WriterAuthorityLost(
                    "head changed while advancing the UTC day".to_string(),
                ));
            }
        };
        self.document = replacement;
        if let Some(version) = next_version {
            self.version = version;
            return Ok(());
        }

        let reloaded = read_writer_head(store, &self.document.signer_node)
            .await
            .map_err(|error| AuditSinkError::WriterAuthorityLost(error.to_string()))?
            .ok_or_else(|| {
                AuditSinkError::WriterAuthorityLost(
                    "updated head disappeared during ETag reconciliation".to_string(),
                )
            })?;
        if reloaded.document != self.document {
            return Err(AuditSinkError::WriterAuthorityLost(
                "updated head diverged during ETag reconciliation".to_string(),
            ));
        }
        self.version = reloaded.version;
        Ok(())
    }
}

fn writer_head_key(signer_node: &str) -> String {
    format!("_security/audit-writers/{signer_node}.json")
}

fn new_writer_head(signer_node: &str, day: NaiveDate) -> AuditWriterHead {
    AuditWriterHead {
        signer_node: signer_node.to_string(),
        stream_id: format!("{signer_node}.{}", ulid::Ulid::new()),
        open_day: day.format("%Y-%m-%d").to_string(),
        lease_owner: None,
        lease_expires_at: None,
    }
}

fn encode_writer_head(head: &AuditWriterHead) -> Result<Bytes, AuditSinkError> {
    serde_json::to_vec(head)
        .map(Bytes::from)
        .map_err(|error| AuditSinkError::Serialization(error.to_string()))
}

fn validate_writer_head(
    key: &str,
    signer_node: &str,
    head: AuditWriterHead,
) -> Result<AuditWriterHead, AuditSinkError> {
    if head.signer_node != signer_node
        || !head.stream_id.starts_with(&format!("{signer_node}."))
        || !valid_node_id(&head.stream_id)
        || head.open_day().is_err()
        || head.lease_owner.is_some() != head.lease_expires_at.is_some()
        || head
            .lease_owner
            .as_deref()
            .is_some_and(|owner| !valid_node_id(owner))
    {
        return Err(AuditSinkError::Serialization(format!(
            "invalid authoritative audit writer head {key}"
        )));
    }
    Ok(head)
}

async fn read_writer_head(
    store: &ZeppelinStore,
    signer_node: &str,
) -> Result<Option<LoadedAuditWriterHead>, AuditSinkError> {
    let key = writer_head_key(signer_node);
    let (body, metadata) = match store.get_with_object_metadata(&key).await {
        Ok(value) => value,
        Err(ZeppelinError::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(AuditSinkError::Storage(error.to_string())),
    };
    let document: AuditWriterHead = serde_json::from_slice(&body)
        .map_err(|error| AuditSinkError::Serialization(format!("invalid {key}: {error}")))?;
    let document = validate_writer_head(&key, signer_node, document)?;
    let version = metadata.version.ok_or_else(|| {
        AuditSinkError::Storage(format!(
            "authoritative audit writer head {key} has no version token"
        ))
    })?;
    Ok(Some(LoadedAuditWriterHead {
        key,
        version,
        document,
    }))
}

async fn resolve_writer_head(
    store: &ZeppelinStore,
    lease_duration: Duration,
    initial_day: NaiveDate,
) -> Result<LoadedAuditWriterHead, AuditSinkError> {
    let signer_node = store
        .object_signer_node()
        .map_err(|error| AuditSinkError::Serialization(error.to_string()))?
        .ok_or_else(|| {
            AuditSinkError::Serialization(
                "durable audit requires a stable published node signer identity".to_string(),
            )
        })?;
    let key = writer_head_key(&signer_node);
    let lease_owner = format!("audit-writer-{}", ulid::Ulid::new());
    for _attempt in 0..WRITER_HEAD_RETRIES {
        let Some(current) = read_writer_head(store, &signer_node).await? else {
            let candidate = new_writer_head(&signer_node, initial_day);
            match store
                .put_create_outcome(&key, encode_writer_head(&candidate)?)
                .await
                .map_err(|error| AuditSinkError::Storage(error.to_string()))?
            {
                CreateOnlyOutcome::Created { .. } | CreateOnlyOutcome::AlreadyExists => continue,
            }
        };

        let open_day = current.document.open_day()?;
        match store
            .get(&anchor_key(open_day, &current.document.stream_id))
            .await
        {
            Err(ZeppelinError::NotFound { .. }) => {
                let now = Utc::now();
                if current.document.lease_owner.as_deref() == Some(&lease_owner) {
                    return Ok(current);
                }
                if current
                    .document
                    .lease_expires_at
                    .is_some_and(|expires_at| expires_at > now)
                {
                    return Err(AuditSinkError::WriterAlreadyActive);
                }
                let lease_expires_at = now
                    + chrono::Duration::from_std(lease_duration).map_err(|error| {
                        AuditSinkError::Serialization(format!(
                            "invalid audit writer lease duration: {error}"
                        ))
                    })?;
                let mut replacement = current.document.clone();
                replacement.lease_owner = Some(lease_owner.clone());
                replacement.lease_expires_at = Some(lease_expires_at);
                match store
                    .put_if_match_outcome(
                        &current.key,
                        encode_writer_head(&replacement)?,
                        &current.version,
                    )
                    .await
                    .map_err(|error| AuditSinkError::Storage(error.to_string()))?
                {
                    ConditionalPutOutcome::Updated { .. } | ConditionalPutOutcome::Conflict => {
                        continue
                    }
                }
            }
            Err(error) => return Err(AuditSinkError::Storage(error.to_string())),
            Ok(_) => {}
        }

        let replacement = new_writer_head(&signer_node, initial_day);
        match store
            .put_if_match_outcome(
                &current.key,
                encode_writer_head(&replacement)?,
                &current.version,
            )
            .await
            .map_err(|error| AuditSinkError::Storage(error.to_string()))?
        {
            ConditionalPutOutcome::Updated { .. } | ConditionalPutOutcome::Conflict => continue,
        }
    }
    Err(AuditSinkError::Storage(
        "audit writer head changed too many times during startup".to_string(),
    ))
}

/// Failure to enqueue, serialize, or durably persist security audit evidence.
///
/// The variants intentionally retain only bounded internal diagnostics. HTTP
/// adapters must map any of them to the stable redacted `audit_unavailable`
/// response rather than exposing these strings to a client.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuditSinkError {
    /// The configured node identifier cannot be represented as one safe key segment.
    #[error("invalid audit node identifier")]
    InvalidNodeId,
    /// A zero-duration flush interval would create a continuously ready timer.
    #[error("audit flush interval must be greater than zero")]
    InvalidFlushInterval,
    /// A record was submitted to a client belonging to a different node.
    #[error("audit record node identifier does not match the audit client")]
    NodeIdMismatch,
    /// The writer no longer accepts messages.
    #[error("audit writer is unavailable")]
    WriterUnavailable,
    /// Another live process holds the signer-scoped S3 writer lease.
    #[error("audit writer for this node signer is already active")]
    WriterAlreadyActive,
    /// This actor can no longer prove ownership of the authoritative writer head.
    #[error("audit writer lost authoritative head: {0}")]
    WriterAuthorityLost(String),
    /// The selected immutable stream already owns its terminal chain slot.
    #[error("audit stream is already sealed")]
    StreamSealed,
    /// A deterministic immutable audit key contains a different valid writer's bytes.
    #[error("audit immutable object contains divergent bytes")]
    ImmutableObjectConflict,
    /// Durable object-storage delivery was explicitly disabled at boot.
    #[error("durable audit storage is disabled")]
    DurabilityDisabled,
    /// JSON Lines serialization rejected a typed record.
    #[error("audit record serialization failed: {0}")]
    Serialization(String),
    /// Object storage did not prove that the exact immutable body is durable.
    #[error("audit storage flush failed: {0}")]
    Storage(String),
    /// The writer task panicked or was cancelled before graceful completion.
    #[error("audit writer task failed: {0}")]
    WriterTask(String),
}

impl AuditSinkError {
    fn requires_writer_shutdown(&self) -> bool {
        matches!(
            self,
            Self::WriterAlreadyActive
                | Self::WriterAuthorityLost(_)
                | Self::StreamSealed
                | Self::ImmutableObjectConflict
                | Self::Serialization(_)
        )
    }
}

/// Cloneable request-path handle for tracing and queued audit delivery.
///
/// Clones share only an unbounded channel sender and the immutable node
/// identity. They do not clone the storage client or create additional writer
/// tasks.
#[derive(Clone, Debug)]
pub struct AuditClient {
    sender: Option<mpsc::UnboundedSender<Command>>,
    node_id: Arc<str>,
    writer_healthy: Option<Arc<AtomicBool>>,
}

impl AuditClient {
    /// Returns the process node identifier stamped into every accepted record.
    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Return whether this boot-composed sink can settle durable obligations.
    #[must_use]
    pub(crate) const fn supports_durability(&self) -> bool {
        self.sender.is_some()
    }

    /// Return whether the durable actor can still accept and settle evidence.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.sender
            .as_ref()
            .is_none_or(|sender| !sender.is_closed())
            && self
                .writer_healthy
                .as_ref()
                .is_none_or(|healthy| healthy.load(Ordering::Acquire))
    }

    /// Emits and queues one record without waiting for object storage.
    ///
    /// Authentication failures and authorization denials use this method so a
    /// reject path does not inherit S3 latency. Sending is synchronous and does
    /// not silently drop on queue pressure; it fails only after the owning
    /// runtime has stopped accepting records.
    pub fn submit_buffered(&self, record: AuditRecord) -> Result<(), AuditSinkError> {
        self.validate_record(&record)?;
        emit_record(&record);
        let Some(sender) = &self.sender else {
            // Explicit unsafe-open tracing-only mode has no durable queue. The
            // tracing event above is the complete configured buffered sink.
            return Ok(());
        };
        sender
            .send(Command::Record {
                record: Box::new(record),
                durable: None,
            })
            .map_err(|_| AuditSinkError::WriterUnavailable)
    }

    /// Emits and queues one record, then waits until its batch is durable.
    ///
    /// The acknowledgement settles only after a create-only PUT succeeds or an
    /// exact-key GET proves that an ambiguous PUT stored byte-for-byte identical
    /// content. A failure is explicit; callers must not report the protected
    /// operation as an ordinary success.
    pub async fn submit_durable(&self, record: AuditRecord) -> Result<(), AuditSinkError> {
        self.validate_record(&record)?;
        emit_record(&record);
        let Some(sender) = &self.sender else {
            return Err(AuditSinkError::DurabilityDisabled);
        };
        let (reply, waiting) = oneshot::channel();
        sender
            .send(Command::Record {
                record: Box::new(record),
                durable: Some(reply),
            })
            .map_err(|_| AuditSinkError::WriterUnavailable)?;
        waiting
            .await
            .map_err(|_| AuditSinkError::WriterUnavailable)?
    }

    /// Forces all records observed before this command into durable objects.
    ///
    /// This is primarily a lifecycle and test synchronization seam. Request
    /// handlers that require evidence should use [`Self::submit_durable`], which
    /// keeps the record and its barrier inseparable.
    pub async fn flush(&self) -> Result<(), AuditSinkError> {
        let Some(sender) = &self.sender else {
            return Err(AuditSinkError::DurabilityDisabled);
        };
        let (reply, waiting) = oneshot::channel();
        sender
            .send(Command::Flush { reply })
            .map_err(|_| AuditSinkError::WriterUnavailable)?;
        waiting
            .await
            .map_err(|_| AuditSinkError::WriterUnavailable)?
    }

    fn validate_record(&self, record: &AuditRecord) -> Result<(), AuditSinkError> {
        if record.node_id == self.node_id.as_ref() {
            Ok(())
        } else {
            Err(AuditSinkError::NodeIdMismatch)
        }
    }
}

/// Owned lifecycle handle for the single audit writer actor.
///
/// Call [`Self::shutdown`] after the HTTP server has stopped accepting work.
/// Shutdown closes the receiver, drains every message already accepted by any
/// client clone, flushes all remaining records, and joins the task.
#[derive(Debug)]
pub struct AuditRuntime {
    sender: Option<mpsc::UnboundedSender<Command>>,
    task: Option<JoinHandle<Result<(), AuditSinkError>>>,
}

impl AuditRuntime {
    /// Starts the production durable writer selected by the authoritative
    /// signer-scoped stream head.
    pub async fn start_for_published_signer(
        store: ZeppelinStore,
        flush_interval: Duration,
    ) -> Result<(AuditClient, Self), AuditSinkError> {
        Self::start_for_published_signer_at(store, flush_interval, Utc::now()).await
    }

    /// Starts the production durable writer at an application-selected UTC instant.
    ///
    /// The instant selects the initial chain day when no unsealed signer head
    /// exists. Lease expiry still uses real wall time because it coordinates
    /// independent processes rather than timestamping application evidence.
    pub async fn start_for_published_signer_at(
        store: ZeppelinStore,
        flush_interval: Duration,
        started_at: DateTime<Utc>,
    ) -> Result<(AuditClient, Self), AuditSinkError> {
        if flush_interval.is_zero() {
            return Err(AuditSinkError::InvalidFlushInterval);
        }
        let lease_duration = Duration::from_secs(
            flush_interval
                .as_secs()
                .saturating_mul(4)
                .max(MIN_WRITER_LEASE_SECS),
        );
        for _attempt in 0..WRITER_HEAD_RETRIES {
            let mut writer_head =
                resolve_writer_head(&store, lease_duration, started_at.date_naive()).await?;
            let chain_day = writer_head.document.open_day()?;
            let node_id = writer_head.document.stream_id.clone();
            let tail = load_chain_tail(&store, chain_day, &node_id)
                .await
                .map_err(|error| AuditSinkError::Storage(error.to_string()))?;
            if tail.terminal {
                writer_head.refresh_lease(&store, lease_duration).await?;
                write_anchor(&store, chain_day, &node_id, &tail.state).await?;
                continue;
            }
            return Self::start_inner(
                store,
                node_id,
                flush_interval,
                chain_day,
                tail.state,
                Some(writer_head),
                Some(lease_duration),
            )
            .await;
        }
        Err(AuditSinkError::Storage(
            "audit writer stream changed too many times during sealed-tail recovery".to_string(),
        ))
    }

    /// Starts one node-local writer and returns its request and lifecycle handles.
    ///
    /// `node_id` becomes exactly one object-key segment. Restricting its grammar
    /// here prevents a configuration value from changing the audit prefix
    /// layout. `flush_interval` controls the timer trigger; full batches and
    /// durable submissions flush immediately regardless of its value.
    pub async fn start(
        store: ZeppelinStore,
        node_id: impl Into<String>,
        flush_interval: Duration,
    ) -> Result<(AuditClient, Self), AuditSinkError> {
        Self::start_at(store, node_id, flush_interval, Utc::now()).await
    }

    /// Starts one node-local writer at an explicitly selected UTC instant.
    ///
    /// Compositions with an application-provided wall clock must pass that
    /// clock's instant here so the writer and the audit records share one time
    /// authority. The selected day's authoritative tail is still loaded and
    /// validated before a client handle is returned.
    pub async fn start_at(
        store: ZeppelinStore,
        node_id: impl Into<String>,
        flush_interval: Duration,
        started_at: DateTime<Utc>,
    ) -> Result<(AuditClient, Self), AuditSinkError> {
        let node_id = node_id.into();
        if !valid_node_id(&node_id) {
            return Err(AuditSinkError::InvalidNodeId);
        }
        if flush_interval.is_zero() {
            return Err(AuditSinkError::InvalidFlushInterval);
        }
        let chain_day = started_at.date_naive();
        let tail = load_chain_tail(&store, chain_day, &node_id)
            .await
            .map_err(|error| AuditSinkError::Storage(error.to_string()))?;
        if tail.terminal {
            return Err(AuditSinkError::StreamSealed);
        }
        Self::start_inner(
            store,
            node_id,
            flush_interval,
            chain_day,
            tail.state,
            None,
            None,
        )
        .await
    }

    async fn start_inner(
        store: ZeppelinStore,
        node_id: String,
        flush_interval: Duration,
        chain_day: NaiveDate,
        chain_state: AuditChainState,
        writer_head: Option<LoadedAuditWriterHead>,
        writer_lease_duration: Option<Duration>,
    ) -> Result<(AuditClient, Self), AuditSinkError> {
        if !valid_node_id(&node_id) {
            return Err(AuditSinkError::InvalidNodeId);
        }
        if flush_interval.is_zero() {
            return Err(AuditSinkError::InvalidFlushInterval);
        }

        let node_id: Arc<str> = Arc::from(node_id);
        let (sender, receiver) = mpsc::unbounded_channel();
        let writer_healthy = Arc::new(AtomicBool::new(true));
        let actor = Writer::new(
            store,
            Arc::clone(&node_id),
            flush_interval,
            receiver,
            WriterAuthority {
                chain_day,
                chain_state,
                writer_head,
                writer_lease_duration,
            },
            Arc::clone(&writer_healthy),
        );
        let task = tokio::spawn(actor.run());
        let client = AuditClient {
            sender: Some(sender.clone()),
            node_id,
            writer_healthy: Some(writer_healthy),
        };
        let runtime = Self {
            sender: Some(sender),
            task: Some(task),
        };
        Ok((client, runtime))
    }

    /// Constructs the explicit unsafe-open tracing-only audit mode.
    ///
    /// Buffered submissions still emit the same structured tracing event and
    /// outcome metric, but no S3 actor exists. Durable submissions and explicit
    /// flushes fail with [`AuditSinkError::DurabilityDisabled`]; this prevents a
    /// caller from mistaking process-local tracing for durable audit evidence.
    /// Shutdown succeeds immediately because there is no queued work to drain.
    pub fn tracing_only(node_id: impl Into<String>) -> Result<(AuditClient, Self), AuditSinkError> {
        let node_id = node_id.into();
        if !valid_node_id(&node_id) {
            return Err(AuditSinkError::InvalidNodeId);
        }
        Ok((
            AuditClient {
                sender: None,
                node_id: Arc::from(node_id),
                writer_healthy: None,
            },
            Self {
                sender: None,
                task: None,
            },
        ))
    }

    /// Stops acceptance, drains the channel, flushes all records, and joins the actor.
    pub async fn shutdown(mut self) -> Result<(), AuditSinkError> {
        let Some(sender) = self.sender.take() else {
            return if self.task.is_none() {
                Ok(())
            } else {
                Err(AuditSinkError::WriterUnavailable)
            };
        };
        let (reply, waiting) = oneshot::channel();
        let sent = sender.send(Command::Shutdown { reply }).is_ok();
        let acknowledged = if sent {
            waiting.await.map_err(|_| AuditSinkError::WriterUnavailable)
        } else {
            Err(AuditSinkError::WriterUnavailable)
        };

        let Some(task) = self.task.take() else {
            return Err(AuditSinkError::WriterUnavailable);
        };
        let joined = task
            .await
            .map_err(|join_error| AuditSinkError::WriterTask(join_error.to_string()))?;

        match acknowledged {
            Ok(result) => result.and(joined),
            Err(error) => joined.and(Err(error)),
        }
    }

    /// Abort the writer immediately and join its task without flushing or sealing.
    ///
    /// Crash simulation uses this path after HTTP has drained its accepted
    /// connections. It intentionally leaves an unsealed durable tail rather
    /// than converting a crash into graceful terminal evidence.
    pub async fn abort_and_join(mut self) -> Result<(), AuditSinkError> {
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        task.abort();
        // Keep the sender alive until the aborted writer has actually stopped.
        // Closing it first drives `receiver.recv()` to `None`, which is the
        // graceful path that flushes and writes a terminal seal.
        let result = match task.await {
            Err(error) if error.is_cancelled() => Ok(()),
            Ok(Ok(())) => panic!("audit writer exited normally while crash retirement was active"),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(AuditSinkError::WriterTask(error.to_string())),
        };
        self.sender.take();
        result
    }
}

impl Drop for AuditRuntime {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Debug)]
enum Command {
    Record {
        record: Box<AuditRecord>,
        durable: Option<oneshot::Sender<Result<(), AuditSinkError>>>,
    },
    Flush {
        reply: oneshot::Sender<Result<(), AuditSinkError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), AuditSinkError>>,
    },
}

struct WriterAuthority {
    chain_day: NaiveDate,
    chain_state: AuditChainState,
    writer_head: Option<LoadedAuditWriterHead>,
    writer_lease_duration: Option<Duration>,
}

struct Writer {
    store: ZeppelinStore,
    node_id: Arc<str>,
    flush_interval: Duration,
    receiver: mpsc::UnboundedReceiver<Command>,
    pending: VecDeque<AuditRecord>,
    staged: Option<StagedBatch>,
    chain_day: Option<NaiveDate>,
    chain_state: AuditChainState,
    writer_head: Option<LoadedAuditWriterHead>,
    writer_lease_duration: Option<Duration>,
    writer_healthy: Arc<AtomicBool>,
}

impl Writer {
    fn new(
        store: ZeppelinStore,
        node_id: Arc<str>,
        flush_interval: Duration,
        receiver: mpsc::UnboundedReceiver<Command>,
        authority: WriterAuthority,
        writer_healthy: Arc<AtomicBool>,
    ) -> Self {
        Self {
            store,
            node_id,
            flush_interval,
            receiver,
            pending: VecDeque::new(),
            staged: None,
            chain_day: Some(authority.chain_day),
            chain_state: authority.chain_state,
            writer_head: authority.writer_head,
            writer_lease_duration: authority.writer_lease_duration,
            writer_healthy,
        }
    }

    async fn run(mut self) -> Result<(), AuditSinkError> {
        let writer_healthy = Arc::clone(&self.writer_healthy);
        let result = self.run_inner().await;
        if let Err(error) = &result {
            writer_healthy.store(false, Ordering::Release);
            record_flush_failure(error);
        }
        result
    }

    async fn run_inner(&mut self) -> Result<(), AuditSinkError> {
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Tokio intervals yield an immediate first tick. Consume it so a newly
        // started empty writer does not execute a misleading timer flush.
        interval.tick().await;
        let mut lease_interval = self.writer_lease_duration.map(|duration| {
            let mut interval = tokio::time::interval(duration / 3);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval
        });
        if let Some(interval) = lease_interval.as_mut() {
            interval.tick().await;
        }

        loop {
            tokio::select! {
                command = self.receiver.recv() => {
                    match command {
                        Some(command) => {
                            if self.handle_command(command).await? {
                                return Ok(());
                            }
                        }
                        None => return self.flush_for_exit().await,
                    }
                }
                _ = interval.tick() => {
                    let result = self.flush_all().await;
                    if let Some(fatal) = observe_flush_result(&result) {
                        return Err(fatal);
                    }
                }
                _ = async {
                    match lease_interval.as_mut() {
                        Some(interval) => interval.tick().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let (Some(writer_head), Some(lease_duration)) =
                        (self.writer_head.as_mut(), self.writer_lease_duration)
                    else {
                        return Err(AuditSinkError::Serialization(
                            "audit writer lease timer exists without authoritative state".to_string(),
                        ));
                    };
                    if let Err(error) = writer_head.refresh_lease(&self.store, lease_duration).await {
                        let fatal = error.requires_writer_shutdown()
                            || writer_head
                                .document
                                .lease_expires_at
                                .is_none_or(|expires_at| expires_at <= Utc::now());
                        if fatal {
                            return Err(error);
                        }
                        record_flush_failure(&error);
                    }
                }
            }
        }
    }

    /// Returns `true` after a shutdown command fully terminates the actor.
    async fn handle_command(&mut self, command: Command) -> Result<bool, AuditSinkError> {
        if self
            .writer_head
            .as_ref()
            .and_then(|head| head.document.lease_expires_at)
            .is_some_and(|expires_at| expires_at <= Utc::now())
        {
            return Err(AuditSinkError::WriterAlreadyActive);
        }
        match command {
            Command::Record { record, durable } => {
                self.pending.push_back(*record);
                if let Some(reply) = durable {
                    let result = self.flush_all().await;
                    let fatal = observe_flush_result(&result);
                    let _ignored = reply.send(result);
                    if let Some(error) = fatal {
                        return Err(error);
                    }
                } else if self.staged.is_none() && self.pending.len() >= MAX_BATCH_RECORDS {
                    let result = self.flush_one().await;
                    if let Some(fatal) = observe_flush_result(&result) {
                        return Err(fatal);
                    }
                }
                Ok(false)
            }
            Command::Flush { reply } => {
                let result = self.flush_all().await;
                let fatal = observe_flush_result(&result);
                let _ignored = reply.send(result);
                if let Some(error) = fatal {
                    return Err(error);
                }
                Ok(false)
            }
            Command::Shutdown { reply } => {
                self.receiver.close();
                let mut replies = vec![reply];
                while let Some(queued) = self.receiver.recv().await {
                    match queued {
                        Command::Record { record, durable } => {
                            self.pending.push_back(*record);
                            if let Some(reply) = durable {
                                replies.push(reply);
                            }
                        }
                        Command::Flush { reply } | Command::Shutdown { reply } => {
                            replies.push(reply);
                        }
                    }
                }

                let result = self.flush_for_exit().await;
                for reply in replies {
                    let _ignored = reply.send(result.clone());
                }
                result?;
                Ok(true)
            }
        }
    }

    async fn flush_for_exit(&mut self) -> Result<(), AuditSinkError> {
        match self.flush_all().await {
            Ok(()) => self.write_current_anchor().await,
            Err(error) => Err(error),
        }
    }

    async fn flush_all(&mut self) -> Result<(), AuditSinkError> {
        while self.staged.is_some() || !self.pending.is_empty() {
            self.flush_one().await?;
        }
        Ok(())
    }

    async fn flush_one(&mut self) -> Result<(), AuditSinkError> {
        if self.staged.is_none() {
            let date = self
                .pending
                .front()
                .ok_or_else(|| {
                    AuditSinkError::Serialization(
                        "cannot select chain day for an empty audit batch".to_string(),
                    )
                })?
                .ts
                .date_naive();
            self.ensure_chain_day(date).await?;
            self.staged = Some(self.stage_next_batch()?);
        }
        let Some(staged) = self.staged.as_ref() else {
            return Ok(());
        };
        let key = staged.key.clone();
        let body = staged.body.clone();

        self.refresh_writer_lease().await?;
        persist_exact(&self.store, &key, body).await?;
        self.staged = None;
        Ok(())
    }

    fn stage_next_batch(&mut self) -> Result<StagedBatch, AuditSinkError> {
        let Some(first) = self.pending.front() else {
            return Err(AuditSinkError::Serialization(
                "cannot stage an empty audit batch".to_string(),
            ));
        };
        let date = first.ts.date_naive();
        let first_position = self
            .chain_state
            .record_count
            .checked_add(1)
            .ok_or_else(|| {
                AuditSinkError::Serialization("audit record count overflow".to_string())
            })?;
        let mut body = Vec::new();
        let mut next_chain_state = self.chain_state.clone();
        let records = self
            .pending
            .iter_mut()
            .take(MAX_BATCH_RECORDS)
            .take_while(|record| record.ts.date_naive() == date);
        let mut count = 0usize;
        for record in records {
            record.prev_hash = next_chain_state.last_hash.clone();
            let position = next_chain_state
                .record_count
                .checked_add(1)
                .and_then(AuditChainPosition::new)
                .ok_or_else(|| {
                    AuditSinkError::Serialization("audit record count overflow".to_string())
                })?;
            record.chain_position = Some(position);
            let line = record
                .to_json_line()
                .map_err(|error| AuditSinkError::Serialization(error.to_string()))?;
            body.extend_from_slice(&line);
            next_chain_state.last_hash = Some(
                record_hash(record)
                    .map_err(|error| AuditSinkError::Serialization(error.to_string()))?,
            );
            next_chain_state.record_count = position.get();
            count += 1;
        }

        let key = audit_slot_key(date, &self.node_id, first_position);
        // Nothing leaves `pending` until both serialization and immutable-key
        // allocation have succeeded. A local failure therefore cannot discard
        // evidence before the actor can report or retry it.
        self.chain_state = next_chain_state;
        self.pending.drain(..count);
        Ok(StagedBatch {
            key,
            body: Bytes::from(body),
        })
    }

    async fn ensure_chain_day(&mut self, day: NaiveDate) -> Result<(), AuditSinkError> {
        if self.chain_day == Some(day) {
            return Ok(());
        }
        if self.chain_day.is_some_and(|current| day < current) {
            return Err(AuditSinkError::Serialization(
                "audit record timestamp moved backward across a sealed UTC day".to_string(),
            ));
        }
        if self.chain_day.is_some()
            && (self.chain_state.record_count != 0 || self.chain_state.last_hash.is_some())
        {
            self.write_current_anchor().await?;
        }
        let tail = load_chain_tail(&self.store, day, &self.node_id)
            .await
            .map_err(classify_chain_tail_error)?;
        if tail.terminal {
            return Err(AuditSinkError::StreamSealed);
        }
        if let Some(writer_head) = self.writer_head.as_mut() {
            let current_day = self.chain_day.ok_or_else(|| {
                AuditSinkError::Serialization(
                    "audit writer head exists without an active chain day".to_string(),
                )
            })?;
            if writer_head.document.stream_id != self.node_id.as_ref()
                || writer_head.document.open_day()? != current_day
            {
                return Err(AuditSinkError::Serialization(
                    "audit writer head diverged from the active stream".to_string(),
                ));
            }
            writer_head.advance_open_day(&self.store, day).await?;
        }
        self.chain_state = tail.state;
        self.chain_day = Some(day);
        Ok(())
    }

    async fn refresh_writer_lease(&mut self) -> Result<(), AuditSinkError> {
        match (self.writer_head.as_mut(), self.writer_lease_duration) {
            (Some(writer_head), Some(lease_duration)) => {
                writer_head.refresh_lease(&self.store, lease_duration).await
            }
            (None, None) => Ok(()),
            _ => Err(AuditSinkError::Serialization(
                "audit writer lease configuration is internally inconsistent".to_string(),
            )),
        }
    }

    async fn write_current_anchor(&mut self) -> Result<(), AuditSinkError> {
        let Some(day) = self.chain_day else {
            return Ok(());
        };
        if self.chain_state.record_count == 0
            && self.chain_state.last_hash.is_none()
            && self.writer_head.is_none()
        {
            return Ok(());
        }
        self.reserve_terminal_slot(day).await?;
        write_anchor(&self.store, day, &self.node_id, &self.chain_state).await
    }

    async fn reserve_terminal_slot(&mut self, day: NaiveDate) -> Result<(), AuditSinkError> {
        for _attempt in 0..WRITER_HEAD_RETRIES {
            self.refresh_writer_lease().await?;
            let next_position = self
                .chain_state
                .record_count
                .checked_add(1)
                .ok_or_else(|| {
                    AuditSinkError::Serialization(
                        "audit terminal chain position overflow".to_string(),
                    )
                })?;
            let key = audit_slot_key(day, &self.node_id, next_position);
            let body = AuditTerminalSeal::from_state(day, &self.node_id, &self.chain_state)
                .encode()
                .map_err(|error| AuditSinkError::Serialization(error.to_string()))?;
            match self.store.put_create(&key, body.clone()).await {
                Ok(()) => return Ok(()),
                Err(ZeppelinError::Storage(put_error)) => match self.store.get(&key).await {
                    Ok(observed) if observed == body => return Ok(()),
                    Ok(observed) => {
                        self.chain_state = advance_tail_body(
                            &observed,
                            &key,
                            day,
                            &self.node_id,
                            &self.chain_state,
                        )
                        .map_err(|_error| AuditSinkError::ImmutableObjectConflict)?;
                    }
                    Err(read_error) => {
                        return Err(AuditSinkError::Storage(format!(
                            "terminal create failed ({put_error}); exact-key readback failed ({read_error})"
                        )));
                    }
                },
                Err(error) => return Err(AuditSinkError::Storage(error.to_string())),
            }
        }
        Err(AuditSinkError::ImmutableObjectConflict)
    }
}

fn classify_chain_tail_error(error: ZeppelinError) -> AuditSinkError {
    match error {
        error @ ZeppelinError::Storage(_) => AuditSinkError::Storage(error.to_string()),
        ZeppelinError::NotFound { .. } => AuditSinkError::ImmutableObjectConflict,
        ZeppelinError::Serialization(message) | ZeppelinError::Bincode(message) => {
            AuditSinkError::Serialization(message)
        }
        error => AuditSinkError::Serialization(error.to_string()),
    }
}

struct StagedBatch {
    key: String,
    body: Bytes,
}

async fn persist_exact(
    store: &ZeppelinStore,
    key: &str,
    body: Bytes,
) -> Result<(), AuditSinkError> {
    match store.put_create(key, body.clone()).await {
        Ok(()) => Ok(()),
        Err(ZeppelinError::Storage(put_error)) => match store.get(key).await {
            Ok(observed) if observed == body => Ok(()),
            Ok(_) => Err(AuditSinkError::ImmutableObjectConflict),
            Err(read_error) => Err(AuditSinkError::Storage(format!(
                "create failed ({put_error}); exact-key readback failed ({read_error})"
            ))),
        },
        Err(error) => Err(AuditSinkError::Storage(error.to_string())),
    }
}

fn observe_flush_result(result: &Result<(), AuditSinkError>) -> Option<AuditSinkError> {
    let error = result.as_ref().err()?;
    if error.requires_writer_shutdown() {
        Some(error.clone())
    } else {
        record_flush_failure(error);
        None
    }
}

async fn write_anchor(
    store: &ZeppelinStore,
    day: NaiveDate,
    node_id: &str,
    chain_state: &AuditChainState,
) -> Result<(), AuditSinkError> {
    let signer_node = store
        .object_signer_node()
        .map_err(|error| AuditSinkError::Serialization(error.to_string()))?
        .ok_or_else(|| {
            AuditSinkError::Serialization(
                "audit anchor signing capability is unavailable".to_string(),
            )
        })?;
    let mut anchor = AuditDayAnchor {
        day: day.format("%Y-%m-%d").to_string(),
        node_id: node_id.to_string(),
        last_hash: chain_state.last_hash.clone(),
        record_count: chain_state.record_count,
        signer_node,
        signature: Vec::new(),
    };
    let unsigned = anchor
        .unsigned_bytes()
        .map_err(|error| AuditSinkError::Serialization(error.to_string()))?;
    let (observed_signer, signature) = store
        .sign_object(&unsigned)
        .map_err(|error| AuditSinkError::Serialization(error.to_string()))?
        .ok_or_else(|| {
            AuditSinkError::Serialization("audit anchor signer became unavailable".to_string())
        })?;
    if observed_signer != anchor.signer_node {
        return Err(AuditSinkError::Serialization(
            "audit anchor signer changed during shutdown".to_string(),
        ));
    }
    anchor.signature = signature;
    let body = serde_json::to_vec(&anchor)
        .map(Bytes::from)
        .map_err(|error| AuditSinkError::Serialization(error.to_string()))?;
    persist_exact(store, &anchor_key(day, node_id), body).await
}

fn valid_node_id(node_id: &str) -> bool {
    !node_id.is_empty()
        && node_id.len() <= 128
        && node_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn emit_record(record: &AuditRecord) {
    crate::metrics::AUDIT_RECORDS_TOTAL
        .with_label_values(&[record.outcome.outcome_class()])
        .inc();
    info!(
        target: "zeppelin::audit",
        audit_ts = %record.ts.to_rfc3339(),
        audit_request_id = %record.request_id,
        audit_decision_id = ?record.decision_id,
        audit_principal_id = %record.principal_id.as_str(),
        audit_principal_kind = ?record.principal_kind,
        audit_delegation_parent = ?record.delegation_parent,
        audit_action = record.action.as_str(),
        audit_resource = ?record.resource,
        audit_policy_version = record.policy_version.get(),
        audit_source_ip = %record.source_ip,
        audit_outcome_class = record.outcome.outcome_class(),
        audit_outcome = ?record.outcome,
        audit_params = ?record.params,
        audit_node_id = %record.node_id,
        audit_prev_hash = ?record.prev_hash,
        audit_chain_position = ?record.chain_position.map(AuditChainPosition::get),
        "security audit record"
    );
}

fn record_flush_failure(flush_error: &AuditSinkError) {
    crate::metrics::AUDIT_FLUSH_FAILURES_TOTAL.inc();
    error!(
        target: "zeppelin::audit",
        error = %flush_error,
        "security audit flush failed"
    );
}

#[cfg(test)]
mod tests {
    use super::{AuditRuntime, AuditSinkError};

    #[tokio::test]
    async fn tracing_only_never_claims_durability_and_shuts_down_cleanly() {
        let Ok((client, runtime)) = AuditRuntime::tracing_only("node-a") else {
            panic!("test node identifier should be valid");
        };

        assert_eq!(client.node_id(), "node-a");
        assert!(!client.supports_durability());
        assert_eq!(
            client.flush().await,
            Err(AuditSinkError::DurabilityDisabled)
        );
        assert_eq!(runtime.shutdown().await, Ok(()));
    }

    #[test]
    fn tracing_only_rejects_node_ids_that_change_the_key_layout() {
        let result = AuditRuntime::tracing_only("nested/node");
        assert!(matches!(result, Err(AuditSinkError::InvalidNodeId)));
    }

    // Durable writer lifecycle and recovery use TestHarness-backed object
    // storage in tests/security_audit_chain_tests.rs.
}
