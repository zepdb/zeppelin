//! Query-priority admission for one shared multi-vector encoder session.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::Notify;

use crate::error::{Result, ZeppelinError};

use super::{
    EncoderDocumentInput, EncoderQueryInput, MultiVectorEmbedding, MultiVectorEmbeddingBatch,
    MultiVectorEncoder, MultiVectorEpochId,
};

/// Serializes one encoder while admitting queued queries before queued documents.
pub(crate) struct QueryPriorityEncoder {
    inner: Arc<dyn MultiVectorEncoder>,
    gate: Arc<PriorityGate>,
}

impl QueryPriorityEncoder {
    /// Wrap one epoch-pinned encoder in the shared admission plane.
    pub(crate) fn wrap(inner: Arc<dyn MultiVectorEncoder>) -> Arc<dyn MultiVectorEncoder> {
        Arc::new(Self {
            inner,
            gate: Arc::new(PriorityGate::default()),
        })
    }
}

#[async_trait]
impl MultiVectorEncoder for QueryPriorityEncoder {
    fn epoch(&self) -> MultiVectorEpochId {
        self.inner.epoch()
    }

    fn output_dimension(&self) -> usize {
        self.inner.output_dimension()
    }

    async fn encode_documents(
        &self,
        inputs: &[EncoderDocumentInput],
    ) -> Result<MultiVectorEmbeddingBatch> {
        let _permit = self.gate.acquire_document().await?;
        self.inner.encode_documents(inputs).await
    }

    async fn encode_query(&self, input: EncoderQueryInput<'_>) -> Result<MultiVectorEmbedding> {
        let _permit = self.gate.acquire_query().await?;
        self.inner.encode_query(input).await
    }
}

#[derive(Default)]
struct PriorityGate {
    state: Mutex<PriorityState>,
    changed: Notify,
}

#[derive(Default)]
struct PriorityState {
    active: bool,
    waiting_queries: usize,
}

impl PriorityGate {
    async fn acquire_document(&self) -> Result<PriorityPermit<'_>> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.lock_state()?;
                if !state.active && state.waiting_queries == 0 {
                    state.active = true;
                    return Ok(PriorityPermit { gate: self });
                }
            }
            changed.await;
        }
    }

    async fn acquire_query(&self) -> Result<PriorityPermit<'_>> {
        {
            let mut state = self.lock_state()?;
            state.waiting_queries = state.waiting_queries.checked_add(1).ok_or_else(|| {
                ZeppelinError::Validation(
                    "encoder query-priority waiter count overflowed".to_string(),
                )
            })?;
        }
        let mut waiter = QueryWaiter {
            gate: self,
            registered: true,
        };
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.lock_state()?;
                if !state.active {
                    state.active = true;
                    state.waiting_queries =
                        state.waiting_queries.checked_sub(1).ok_or_else(|| {
                            ZeppelinError::Validation(
                                "encoder query-priority waiter count underflowed".to_string(),
                            )
                        })?;
                    waiter.registered = false;
                    return Ok(PriorityPermit { gate: self });
                }
            }
            changed.await;
        }
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, PriorityState>> {
        self.state.lock().map_err(|_| {
            ZeppelinError::Validation("encoder query-priority gate is poisoned".to_string())
        })
    }

    fn release(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|_| panic!("encoder query-priority gate is poisoned"));
        assert!(
            state.active,
            "encoder query-priority permit released while inactive"
        );
        state.active = false;
        drop(state);
        self.changed.notify_waiters();
    }

    fn cancel_query_waiter(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|_| panic!("encoder query-priority gate is poisoned"));
        state.waiting_queries = state
            .waiting_queries
            .checked_sub(1)
            .unwrap_or_else(|| panic!("encoder query-priority waiter count underflowed"));
        drop(state);
        self.changed.notify_waiters();
    }
}

struct PriorityPermit<'a> {
    gate: &'a PriorityGate,
}

impl Drop for PriorityPermit<'_> {
    fn drop(&mut self) {
        self.gate.release();
    }
}

struct QueryWaiter<'a> {
    gate: &'a PriorityGate,
    registered: bool,
}

impl Drop for QueryWaiter<'_> {
    fn drop(&mut self) {
        if self.registered {
            self.gate.cancel_query_waiter();
        }
    }
}
