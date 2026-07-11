use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use zeppelin::storage::ZeppelinStore;

use super::{
    Boundary, FaultKind, FaultScheduler, FaultSemantics, InjectedErrorKind, ObservedResult,
    StoreFaultAction, TimelineEvent,
};
use crate::adversarial::chaos::StoreOp;

#[derive(Debug)]
pub struct StoreFaultProxy {
    inner: Arc<dyn ObjectStore>,
    scheduler: FaultScheduler,
}

#[must_use]
pub fn store_fault_proxy(store: &ZeppelinStore, scheduler: FaultScheduler) -> ZeppelinStore {
    ZeppelinStore::new(Arc::new(StoreFaultProxy {
        inner: store.inner(),
        scheduler,
    }))
}

impl StoreFaultProxy {
    async fn apply_before(&self, action: &StoreFaultAction, key: &str) -> OsResult<()> {
        match action.kind {
            FaultKind::PreFail { error } => {
                self.record(
                    action,
                    key,
                    FaultSemantics::PreCall,
                    ObservedResult::DefiniteNotApplied,
                    None,
                );
                Err(injected_error(error, key))
            }
            FaultKind::Partition { .. } => {
                self.record(
                    action,
                    key,
                    FaultSemantics::WindowActive,
                    ObservedResult::DefiniteNotApplied,
                    None,
                );
                Err(injected_error(InjectedErrorKind::Generic, key))
            }
            FaultKind::Latency { .. } => {
                let delay = action
                    .latency_ms
                    .expect("latency decision must include deterministic delay");
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                self.record(
                    action,
                    key,
                    if action.window {
                        FaultSemantics::WindowActive
                    } else {
                        FaultSemantics::PreCall
                    },
                    ObservedResult::DefiniteApplied,
                    Some(format!("delegated after {delay}ms")),
                );
                Ok(())
            }
            FaultKind::PostCommitFail { .. } | FaultKind::TruncatedGetStream { .. } => Ok(()),
            _ => panic!(
                "client HTTP fault {:?} reached the object-store proxy",
                action.kind
            ),
        }
    }

    fn record(
        &self,
        action: &StoreFaultAction,
        key: &str,
        semantics: FaultSemantics,
        observed: ObservedResult,
        recovery: Option<String>,
    ) {
        self.scheduler.record(TimelineEvent {
            event_id: action.event_id.clone(),
            op_index: action.op_index,
            wall_ms: self.scheduler.wall_ms(),
            boundary: Boundary::ObjectStore,
            action: format!("{:?} call={}", action.kind, action.call_ordinal),
            key: Some(key.to_string()),
            semantics,
            observed,
            recovery,
        });
    }

    fn post_commit_error(&self, action: &StoreFaultAction, key: &str) -> object_store::Error {
        let FaultKind::PostCommitFail { error } = action.kind else {
            panic!("non-post-commit action reached post_commit_error")
        };
        self.record(
            action,
            key,
            FaultSemantics::PostCommit,
            ObservedResult::Ambiguous,
            Some("inner mutation completed; acknowledgement replaced".to_string()),
        );
        injected_error(error, key)
    }
}

fn injected_error(kind: InjectedErrorKind, key: &str) -> object_store::Error {
    let detail = match kind {
        InjectedErrorKind::Generic => "generic injected failure".to_string(),
        InjectedErrorKind::NotFound => "404 not found".to_string(),
        InjectedErrorKind::Precondition => "412 precondition failed".to_string(),
        InjectedErrorKind::Throttle429 => "429 retries exhausted".to_string(),
        InjectedErrorKind::Http500 => "500 retries exhausted".to_string(),
        InjectedErrorKind::Http503 => "503 retries exhausted".to_string(),
    };
    let source = Box::new(std::io::Error::other(format!("{detail} for object {key}")));
    match kind {
        InjectedErrorKind::NotFound => object_store::Error::NotFound {
            path: key.to_string(),
            source,
        },
        InjectedErrorKind::Precondition => object_store::Error::Precondition {
            path: key.to_string(),
            source,
        },
        _ => object_store::Error::Generic {
            store: "adversarial_fault_scheduler",
            source,
        },
    }
}

fn truncated_result(result: GetResult, after_bytes: usize, key: String) -> GetResult {
    let meta = result.meta.clone();
    let range = result.range.clone();
    let attributes = result.attributes.clone();
    let inner = result.into_stream();
    let stream = stream::unfold(
        (inner, after_bytes, false, key),
        |(mut inner, remaining, error_emitted, key)| async move {
            if error_emitted {
                return None;
            }
            if remaining == 0 {
                return Some((
                    Err(object_store::Error::Generic {
                        store: "adversarial_fault_scheduler",
                        source: Box::new(std::io::Error::other(format!(
                            "injected truncated GET stream for {key}"
                        ))),
                    }),
                    (inner, remaining, true, key),
                ));
            }
            match inner.next().await {
                Some(Ok(bytes)) if bytes.len() <= remaining => {
                    let next_remaining = remaining - bytes.len();
                    Some((Ok(bytes), (inner, next_remaining, false, key)))
                }
                Some(Ok(bytes)) => {
                    let prefix = bytes.slice(..remaining);
                    Some((Ok(prefix), (inner, 0, false, key)))
                }
                Some(Err(error)) => Some((Err(error), (inner, remaining, true, key))),
                None => Some((
                    Err(object_store::Error::Generic {
                        store: "adversarial_fault_scheduler",
                        source: Box::new(std::io::Error::other(format!(
                            "GET stream for {key} ended before truncation boundary"
                        ))),
                    }),
                    (inner, remaining, true, key),
                )),
            }
        },
    )
    .boxed();
    GetResult {
        payload: GetResultPayload::Stream(stream),
        meta,
        range,
        attributes,
    }
}

impl fmt::Display for StoreFaultProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StoreFaultProxy({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for StoreFaultProxy {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let key = location.to_string();
        if let Some(action) = self.scheduler.store_decision(StoreOp::Put, &key) {
            if matches!(action.kind, FaultKind::PostCommitFail { .. }) {
                return match self.inner.put_opts(location, payload, opts).await {
                    Ok(_) => Err(self.post_commit_error(&action, &key)),
                    Err(error) => Err(error),
                };
            }
            self.apply_before(&action, &key).await?;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let key = location.to_string();
        if let Some(action) = self.scheduler.store_decision(StoreOp::Get, &key) {
            if let FaultKind::TruncatedGetStream { after_bytes } = action.kind {
                let result = self.inner.get_opts(location, options).await?;
                self.record(
                    &action,
                    &key,
                    FaultSemantics::PostCommit,
                    ObservedResult::Ambiguous,
                    Some(format!("stream errors after {after_bytes} bytes")),
                );
                return Ok(truncated_result(result, after_bytes, key));
            }
            self.apply_before(&action, &key).await?;
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        let key = location.to_string();
        if let Some(action) = self.scheduler.store_decision(StoreOp::Head, &key) {
            self.apply_before(&action, &key).await?;
        }
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        let key = location.to_string();
        if let Some(action) = self.scheduler.store_decision(StoreOp::Delete, &key) {
            if matches!(action.kind, FaultKind::PostCommitFail { .. }) {
                return match self.inner.delete(location).await {
                    Ok(()) => Err(self.post_commit_error(&action, &key)),
                    Err(error) => Err(error),
                };
            }
            self.apply_before(&action, &key).await?;
        }
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        let key = prefix.map(ToString::to_string).unwrap_or_default();
        if let Some(action) = self.scheduler.store_decision(StoreOp::List, &key) {
            match action.kind {
                FaultKind::PreFail { error } => {
                    self.record(
                        &action,
                        &key,
                        FaultSemantics::PreCall,
                        ObservedResult::DefiniteNotApplied,
                        None,
                    );
                    return stream::once(async move { Err(injected_error(error, &key)) }).boxed();
                }
                FaultKind::Partition { .. } => {
                    self.record(
                        &action,
                        &key,
                        FaultSemantics::WindowActive,
                        ObservedResult::DefiniteNotApplied,
                        None,
                    );
                    return stream::once(async move {
                        Err(injected_error(InjectedErrorKind::Generic, &key))
                    })
                    .boxed();
                }
                FaultKind::Latency { .. } => {
                    let delay = action
                        .latency_ms
                        .expect("latency decision must include deterministic delay");
                    self.record(
                        &action,
                        &key,
                        FaultSemantics::WindowActive,
                        ObservedResult::DefiniteApplied,
                        Some(format!("delegated after {delay}ms")),
                    );
                    let mut inner = Some(self.inner.list(prefix));
                    return stream::once(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    })
                    .flat_map(move |()| inner.take().expect("delayed list stream reused"))
                    .boxed();
                }
                _ => panic!(
                    "invalid fault action for object-store list: {:?}",
                    action.kind
                ),
            }
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        let key = prefix.map(ToString::to_string).unwrap_or_default();
        if let Some(action) = self.scheduler.store_decision(StoreOp::List, &key) {
            self.apply_before(&action, &key).await?;
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        let key = format!("{from}->{to}");
        if let Some(action) = self.scheduler.store_decision(StoreOp::Copy, &key) {
            if matches!(action.kind, FaultKind::PostCommitFail { .. }) {
                return match self.inner.copy(from, to).await {
                    Ok(()) => Err(self.post_commit_error(&action, &key)),
                    Err(error) => Err(error),
                };
            }
            self.apply_before(&action, &key).await?;
        }
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        let key = format!("{from}->{to}");
        if let Some(action) = self.scheduler.store_decision(StoreOp::Copy, &key) {
            if matches!(action.kind, FaultKind::PostCommitFail { .. }) {
                return match self.inner.copy_if_not_exists(from, to).await {
                    Ok(()) => Err(self.post_commit_error(&action, &key)),
                    Err(error) => Err(error),
                };
            }
            self.apply_before(&action, &key).await?;
        }
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::memory::InMemory;

    use super::*;
    use crate::adversarial::faults::{
        Boundary, FaultEvent, FaultProfile, FaultSchedule, TargetSelector,
    };

    #[tokio::test]
    async fn post_commit_failure_persists_inner_write() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::PostCommit,
            events: vec![FaultEvent {
                id: "post-commit-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some(".wal".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::PostCommitFail {
                    error: InjectedErrorKind::Http503,
                },
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());

        assert!(faulted
            .put("ns/first.wal", Bytes::from_static(b"durable"))
            .await
            .is_err());
        assert_eq!(
            inner.get("ns/first.wal").await.unwrap(),
            Bytes::from_static(b"durable")
        );
        assert_eq!(scheduler.timeline().len(), 1);
        assert_eq!(scheduler.timeline()[0].observed, ObservedResult::Ambiguous);
    }
}
