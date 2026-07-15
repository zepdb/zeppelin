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
//! single monotonic ULID sequence without a shared lock on request paths.
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
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use chrono::NaiveDate;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{error, info};
use ulid::Generator;

use crate::error::ZeppelinError;
use crate::storage::ZeppelinStore;

use super::AuditRecord;

const MAX_BATCH_RECORDS: usize = 256;

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
    /// Durable object-storage delivery was explicitly disabled at boot.
    #[error("durable audit storage is disabled")]
    DurabilityDisabled,
    /// JSON Lines serialization rejected a typed record.
    #[error("audit record serialization failed: {0}")]
    Serialization(String),
    /// A monotonic object identifier could not be allocated.
    #[error("audit object identifier generation failed: {0}")]
    ObjectId(String),
    /// Object storage did not prove that the exact immutable body is durable.
    #[error("audit storage flush failed: {0}")]
    Storage(String),
    /// The writer task panicked or was cancelled before graceful completion.
    #[error("audit writer task failed: {0}")]
    WriterTask(String),
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
    /// Starts one node-local writer and returns its request and lifecycle handles.
    ///
    /// `node_id` becomes exactly one object-key segment. Restricting its grammar
    /// here prevents a configuration value from changing the audit prefix
    /// layout. `flush_interval` controls the timer trigger; full batches and
    /// durable submissions flush immediately regardless of its value.
    pub fn start(
        store: ZeppelinStore,
        node_id: impl Into<String>,
        flush_interval: Duration,
    ) -> Result<(AuditClient, Self), AuditSinkError> {
        let node_id = node_id.into();
        if !valid_node_id(&node_id) {
            return Err(AuditSinkError::InvalidNodeId);
        }
        if flush_interval.is_zero() {
            return Err(AuditSinkError::InvalidFlushInterval);
        }

        let node_id: Arc<str> = Arc::from(node_id);
        let (sender, receiver) = mpsc::unbounded_channel();
        let actor = Writer::new(store, Arc::clone(&node_id), flush_interval, receiver);
        let task = tokio::spawn(actor.run());
        let client = AuditClient {
            sender: Some(sender.clone()),
            node_id,
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

struct Writer {
    store: ZeppelinStore,
    node_id: Arc<str>,
    flush_interval: Duration,
    receiver: mpsc::UnboundedReceiver<Command>,
    pending: VecDeque<AuditRecord>,
    staged: Option<StagedBatch>,
    generator: Generator,
}

impl Writer {
    fn new(
        store: ZeppelinStore,
        node_id: Arc<str>,
        flush_interval: Duration,
        receiver: mpsc::UnboundedReceiver<Command>,
    ) -> Self {
        Self {
            store,
            node_id,
            flush_interval,
            receiver,
            pending: VecDeque::new(),
            staged: None,
            generator: Generator::new(),
        }
    }

    async fn run(mut self) -> Result<(), AuditSinkError> {
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Tokio intervals yield an immediate first tick. Consume it so a newly
        // started empty writer does not execute a misleading timer flush.
        interval.tick().await;

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
                    if let Err(flush_error) = self.flush_all().await {
                        record_flush_failure(&flush_error);
                    }
                }
            }
        }
    }

    /// Returns `true` after a shutdown command fully terminates the actor.
    async fn handle_command(&mut self, command: Command) -> Result<bool, AuditSinkError> {
        match command {
            Command::Record { record, durable } => {
                self.pending.push_back(*record);
                if let Some(reply) = durable {
                    let result = self.flush_all().await;
                    if let Err(error) = &result {
                        record_flush_failure(error);
                    }
                    let _ignored = reply.send(result);
                } else if self.staged.is_none() && self.pending.len() >= MAX_BATCH_RECORDS {
                    if let Err(flush_error) = self.flush_one().await {
                        record_flush_failure(&flush_error);
                    }
                }
                Ok(false)
            }
            Command::Flush { reply } => {
                let result = self.flush_all().await;
                if let Err(error) = &result {
                    record_flush_failure(error);
                }
                let _ignored = reply.send(result);
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

                let result = self.flush_all().await;
                if let Err(error) = &result {
                    record_flush_failure(error);
                }
                for reply in replies {
                    let _ignored = reply.send(result.clone());
                }
                result?;
                Ok(true)
            }
        }
    }

    async fn flush_for_exit(&mut self) -> Result<(), AuditSinkError> {
        let result = self.flush_all().await;
        if let Err(error) = &result {
            record_flush_failure(error);
        }
        result
    }

    async fn flush_all(&mut self) -> Result<(), AuditSinkError> {
        while self.staged.is_some() || !self.pending.is_empty() {
            self.flush_one().await?;
        }
        Ok(())
    }

    async fn flush_one(&mut self) -> Result<(), AuditSinkError> {
        if self.staged.is_none() {
            self.staged = Some(self.stage_next_batch()?);
        }
        let Some(staged) = self.staged.as_ref() else {
            return Ok(());
        };

        persist_exact(&self.store, &staged.key, staged.body.clone()).await?;
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
        let timestamp: SystemTime = first.ts.into();
        let mut body = Vec::new();
        let records = self
            .pending
            .iter()
            .take(MAX_BATCH_RECORDS)
            .take_while(|record| record.ts.date_naive() == date);
        let mut count = 0usize;
        for record in records {
            let line = record
                .to_json_line()
                .map_err(|error| AuditSinkError::Serialization(error.to_string()))?;
            body.extend_from_slice(&line);
            count += 1;
        }

        let object_id = self
            .generator
            .generate_from_datetime(timestamp)
            .map_err(|error| AuditSinkError::ObjectId(error.to_string()))?;
        let key = audit_key(date, &self.node_id, object_id);
        // Nothing leaves `pending` until both serialization and immutable-key
        // allocation have succeeded. A local failure therefore cannot discard
        // evidence before the actor can report or retry it.
        self.pending.drain(..count);
        Ok(StagedBatch {
            key,
            body: Bytes::from(body),
        })
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
            Ok(_) => Err(AuditSinkError::Storage(format!(
                "create failed ({put_error}); exact-key readback found different bytes"
            ))),
            Err(read_error) => Err(AuditSinkError::Storage(format!(
                "create failed ({put_error}); exact-key readback failed ({read_error})"
            ))),
        },
        Err(error) => Err(AuditSinkError::Storage(error.to_string())),
    }
}

fn audit_key(date: NaiveDate, node_id: &str, object_id: ulid::Ulid) -> String {
    format!(
        "_audit/{}/{node_id}/{object_id}.jsonl",
        date.format("%Y-%m-%d")
    )
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
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::Utc;
    use object_store::memory::InMemory;

    use crate::security::AuditRecord;
    use crate::storage::ZeppelinStore;

    use super::{AuditRuntime, AuditSinkError};

    #[tokio::test]
    async fn full_batch_and_remainder_create_two_immutable_jsonl_objects() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let Ok((client, runtime)) =
            AuditRuntime::start(store.clone(), "node-a", Duration::from_secs(60))
        else {
            panic!("valid in-memory audit runtime should start");
        };
        assert!(client.supports_durability());

        for index in 0..257 {
            let mut record = AuditRecord::open_unsafe_boot(Utc::now(), "node-a");
            record.request_id = format!("request-{index}");
            assert_eq!(client.submit_buffered(record), Ok(()));
        }
        assert_eq!(client.flush().await, Ok(()));

        let Ok(keys) = store.list_prefix("_audit/").await else {
            panic!("in-memory audit objects should be listable");
        };
        assert_eq!(keys.len(), 2);
        let mut line_counts = Vec::new();
        for key in keys {
            let Ok(body) = store.get(&key).await else {
                panic!("listed in-memory audit object should be readable");
            };
            line_counts.push(
                body.split(|byte| *byte == b'\n')
                    .filter(|line| !line.is_empty())
                    .count(),
            );
        }
        line_counts.sort_unstable();
        assert_eq!(line_counts, vec![1, 256]);
        assert_eq!(runtime.shutdown().await, Ok(()));
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_and_flushes_a_partial_batch() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let Ok((client, runtime)) =
            AuditRuntime::start(store.clone(), "node-a", Duration::from_secs(60))
        else {
            panic!("valid in-memory audit runtime should start");
        };

        let record = AuditRecord::open_unsafe_boot(Utc::now(), "node-a");
        assert_eq!(client.submit_buffered(record), Ok(()));
        assert_eq!(runtime.shutdown().await, Ok(()));

        let Ok(keys) = store.list_prefix("_audit/").await else {
            panic!("shutdown-flushed audit object should be listable");
        };
        assert_eq!(keys.len(), 1);
        let Ok(body) = store.get(&keys[0]).await else {
            panic!("shutdown-flushed audit object should be readable");
        };
        assert_eq!(
            body.split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn interval_tick_flushes_a_buffered_partial_batch() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let Ok((client, runtime)) =
            AuditRuntime::start(store.clone(), "node-a", Duration::from_millis(10))
        else {
            panic!("valid in-memory audit runtime should start");
        };

        let record = AuditRecord::open_unsafe_boot(Utc::now(), "node-a");
        assert_eq!(client.submit_buffered(record), Ok(()));
        let flushed = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let Ok(keys) = store.list_prefix("_audit/").await else {
                    panic!("timer-flushed audit prefix should be listable");
                };
                if !keys.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            flushed.is_ok(),
            "audit interval did not flush the partial batch"
        );

        assert_eq!(runtime.shutdown().await, Ok(()));
    }

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
}
