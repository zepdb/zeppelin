use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::Manifest;

use super::{
    Boundary, FaultKind, FaultScheduler, FaultSemantics, InjectedErrorKind, ObservedResult,
    StoreFaultAction, TimelineEvent,
};
use crate::adversarial::chaos::StoreOp;
use crate::adversarial::faults::process::{
    CrashPoint, CrashRequest, ProcessController, TriggerPosition,
};
use crate::common::server::background_compaction_origin_active;

#[derive(Debug)]
pub struct StoreFaultProxy {
    inner: Arc<dyn ObjectStore>,
    scheduler: FaultScheduler,
    bodies: Mutex<BodyHistory>,
    admit_stale_manifest_cas_selftest: bool,
    operational_observer: Option<(OperationalStoreObserver, u8)>,
}

#[must_use]
pub fn store_fault_proxy(store: &ZeppelinStore, scheduler: FaultScheduler) -> ZeppelinStore {
    build_store_fault_proxy(store, scheduler, false)
}

/// Builds the only proxy permitted to admit a stale manifest CAS.
///
/// This constructor is intentionally restricted to an explicit, single-event
/// Ops self-test schedule. A regular proxy fails loudly if that mutation ever
/// reaches it.
#[must_use]
pub fn stale_manifest_cas_selftest_proxy(
    store: &ZeppelinStore,
    scheduler: FaultScheduler,
) -> ZeppelinStore {
    let events = &scheduler.schedule().events;
    assert_eq!(
        scheduler.schedule().profile,
        super::FaultProfile::Ops,
        "stale manifest CAS admission requires the Ops self-test profile"
    );
    assert_eq!(
        events.len(),
        1,
        "stale manifest CAS admission requires an isolated one-event schedule"
    );
    let event = &events[0];
    assert!(
        event.id.starts_with("dual-writer-stale-cas-selftest"),
        "stale manifest CAS admission requires the dedicated self-test event id"
    );
    assert_eq!(event.boundary, Boundary::ObjectStore);
    assert_eq!(event.target.store_op, Some(StoreOp::Put));
    assert!(
        event
            .target
            .key_substring
            .as_deref()
            .is_some_and(|key| key.ends_with("manifest.json")),
        "stale manifest CAS admission must target a manifest key"
    );
    assert!(matches!(event.kind, FaultKind::AdmitStaleManifestCas));
    build_store_fault_proxy(store, scheduler, true)
}

fn build_store_fault_proxy(
    store: &ZeppelinStore,
    scheduler: FaultScheduler,
    admit_stale_manifest_cas_selftest: bool,
) -> ZeppelinStore {
    ZeppelinStore::new(Arc::new(StoreFaultProxy {
        inner: store.inner(),
        scheduler,
        bodies: Mutex::new(BodyHistory::default()),
        admit_stale_manifest_cas_selftest,
        operational_observer: None,
    }))
}

/// Shared proof recorder for temporary two-node operational windows.
#[derive(Debug, Clone)]
pub struct OperationalStoreObserver {
    shared: Arc<OperationalObservationShared>,
}

#[derive(Debug)]
struct OperationalObservationShared {
    state: Mutex<Option<ActiveCompactionWindow>>,
    mutation_rendezvous: Mutex<Option<ActiveMutationRendezvous>>,
    revision: tokio::sync::watch::Sender<u64>,
}

#[derive(Debug)]
struct ActiveCompactionWindow {
    event_id: String,
    start_op: u64,
    namespaces: BTreeMap<String, NamespaceCompactionObservation>,
}

#[derive(Debug, Default)]
struct NamespaceCompactionObservation {
    attempted_nodes: BTreeSet<u8>,
    lease_publications: u64,
    fenced_manifest_publications: u64,
    background_manifest_publications: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionWindowEvidence {
    pub event_id: String,
    pub start_op: u64,
    pub namespace: String,
    pub attempted_nodes: BTreeSet<u8>,
    pub lease_publications: u64,
    pub fenced_manifest_publications: u64,
    pub background_manifest_publications: u64,
}

#[derive(Debug)]
struct ActiveMutationRendezvous {
    event_id: String,
    op_index: u64,
    namespace: String,
    entered: Option<(u8, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationRendezvousEvidence {
    pub event_id: String,
    pub op_index: u64,
    pub namespace: String,
    pub node: u8,
}

impl Default for OperationalStoreObserver {
    fn default() -> Self {
        let (revision, _) = tokio::sync::watch::channel(0);
        Self {
            shared: Arc::new(OperationalObservationShared {
                state: Mutex::new(None),
                mutation_rendezvous: Mutex::new(None),
                revision,
            }),
        }
    }
}

impl OperationalStoreObserver {
    pub fn arm_mutation_rendezvous(&self, event_id: &str, op_index: u64, namespace: &str) {
        assert!(
            !namespace.is_empty(),
            "operational mutation rendezvous requires a namespace"
        );
        let mut state = self
            .shared
            .mutation_rendezvous
            .lock()
            .expect("operational mutation rendezvous mutex poisoned");
        assert!(
            state.is_none(),
            "operational mutation rendezvous must not overlap: {state:#?}"
        );
        *state = Some(ActiveMutationRendezvous {
            event_id: event_id.to_string(),
            op_index,
            namespace: namespace.to_string(),
            entered: None,
        });
        drop(state);
        self.bump_revision();
    }

    async fn rendezvous_wal_put(&self, node: u8, key: &str) {
        let mut revision = self.shared.revision.subscribe();
        let entered = {
            let mut state = self
                .shared
                .mutation_rendezvous
                .lock()
                .expect("operational mutation rendezvous mutex poisoned");
            let Some(rendezvous) = state.as_mut() else {
                return;
            };
            let expected_prefix = format!("{}/wal/", rendezvous.namespace);
            if !key.starts_with(&expected_prefix) || !key.ends_with(".wal") {
                return;
            }
            assert!(
                rendezvous.entered.is_none(),
                "operational mutation rendezvous observed multiple WAL PUTs: \
                 first={:?} second_node={node} second_key={key}",
                rendezvous.entered
            );
            rendezvous.entered = Some((node, key.to_string()));
            true
        };
        if entered {
            self.bump_revision();
        }
        loop {
            if self
                .shared
                .mutation_rendezvous
                .lock()
                .expect("operational mutation rendezvous mutex poisoned")
                .is_none()
            {
                return;
            }
            revision
                .changed()
                .await
                .expect("operational store observer revision sender dropped");
        }
    }

    pub async fn wait_for_mutation_rendezvous(
        &self,
        timeout: std::time::Duration,
    ) -> MutationRendezvousEvidence {
        let mut revision = self.shared.revision.subscribe();
        let wait = async {
            loop {
                if let Some(evidence) = self.mutation_rendezvous_evidence() {
                    return evidence;
                }
                revision
                    .changed()
                    .await
                    .expect("operational store observer revision sender dropped");
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .unwrap_or_else(|_| {
                let state = self
                    .shared
                    .mutation_rendezvous
                    .lock()
                    .expect("operational mutation rendezvous mutex poisoned");
                let state_snapshot = format!("{state:#?}");
                drop(state);
                panic!(
                    "timed out waiting for operational WAL PUT rendezvous after {timeout:?}: \
                     {state_snapshot}"
                )
            })
    }

    pub fn release_mutation_rendezvous(&self, event_id: &str) -> MutationRendezvousEvidence {
        let rendezvous = self
            .shared
            .mutation_rendezvous
            .lock()
            .expect("operational mutation rendezvous mutex poisoned")
            .take()
            .expect("operational mutation rendezvous release requires an armed window");
        assert_eq!(
            rendezvous.event_id, event_id,
            "operational mutation rendezvous release event changed"
        );
        let (node, _) = rendezvous
            .entered
            .expect("operational mutation rendezvous released before WAL PUT entry");
        let evidence = MutationRendezvousEvidence {
            event_id: rendezvous.event_id,
            op_index: rendezvous.op_index,
            namespace: rendezvous.namespace,
            node,
        };
        self.bump_revision();
        evidence
    }

    fn mutation_rendezvous_evidence(&self) -> Option<MutationRendezvousEvidence> {
        let state = self
            .shared
            .mutation_rendezvous
            .lock()
            .expect("operational mutation rendezvous mutex poisoned");
        let rendezvous = state.as_ref()?;
        let (node, _) = rendezvous.entered.as_ref()?;
        Some(MutationRendezvousEvidence {
            event_id: rendezvous.event_id.clone(),
            op_index: rendezvous.op_index,
            namespace: rendezvous.namespace.clone(),
            node: *node,
        })
    }

    pub fn arm_compaction_contention_window(&self, event_id: &str, start_op: u64) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("operational store observer mutex poisoned");
        assert!(
            state.is_none(),
            "operational compaction windows must not overlap: {state:#?}"
        );
        *state = Some(ActiveCompactionWindow {
            event_id: event_id.to_string(),
            start_op,
            namespaces: BTreeMap::new(),
        });
        drop(state);
        self.bump_revision();
    }

    async fn rendezvous_lease_get(&self, node: u8, key: &str) {
        if !background_compaction_origin_active() {
            return;
        }
        let Some(namespace) = key.strip_suffix("/lease.json") else {
            return;
        };
        let mut revision = self.shared.revision.subscribe();
        loop {
            let (changed, common_namespace) = {
                let mut state = self
                    .shared
                    .state
                    .lock()
                    .expect("operational store observer mutex poisoned");
                let Some(window) = state.as_mut() else {
                    return;
                };
                let changed = window
                    .namespaces
                    .entry(namespace.to_string())
                    .or_default()
                    .attempted_nodes
                    .insert(node);
                let common_namespace = window
                    .namespaces
                    .values()
                    .any(|proof| proof.attempted_nodes == BTreeSet::from([0, 1]));
                (changed, common_namespace)
            };
            if changed {
                self.bump_revision();
            }
            if common_namespace {
                return;
            }
            revision
                .changed()
                .await
                .expect("operational store observer revision sender dropped");
        }
    }

    fn observe_successful_put(&self, _node: u8, key: &str, body: &bytes::Bytes) {
        if !background_compaction_origin_active() {
            return;
        }
        let mut state = self
            .shared
            .state
            .lock()
            .expect("operational store observer mutex poisoned");
        let Some(window) = state.as_mut() else {
            return;
        };
        let changed = if let Some(namespace) = key.strip_suffix("/lease.json") {
            let proof = window.namespaces.entry(namespace.to_string()).or_default();
            proof.lease_publications = proof
                .lease_publications
                .checked_add(1)
                .expect("operational lease publication count overflowed");
            true
        } else if let Some(namespace) = key.strip_suffix("/manifest.json") {
            let manifest = Manifest::from_bytes(body).unwrap_or_else(|error| {
                panic!("operational observer could not decode manifest PUT {key}: {error}")
            });
            let proof = window.namespaces.entry(namespace.to_string()).or_default();
            proof.background_manifest_publications = proof
                .background_manifest_publications
                .checked_add(1)
                .expect("operational background manifest publication count overflowed");
            if manifest.fencing_token > 0 {
                proof.fenced_manifest_publications = proof
                    .fenced_manifest_publications
                    .checked_add(1)
                    .expect("operational fenced manifest publication count overflowed");
            }
            true
        } else {
            false
        };
        drop(state);
        if changed {
            self.bump_revision();
        }
    }

    pub async fn wait_for_compaction_evidence(
        &self,
        timeout: std::time::Duration,
    ) -> CompactionWindowEvidence {
        let mut revision = self.shared.revision.subscribe();
        let wait = async {
            loop {
                if let Some(evidence) = self.take_ready_evidence() {
                    return evidence;
                }
                revision
                    .changed()
                    .await
                    .expect("operational store observer revision sender dropped");
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .unwrap_or_else(|_| {
                let state = self
                    .shared
                    .state
                    .lock()
                    .expect("operational store observer mutex poisoned");
                let state_snapshot = format!("{state:#?}");
                drop(state);
                panic!(
                    "timed out waiting for two-node compaction evidence after {timeout:?}: \
                     {state_snapshot}"
                )
            })
    }

    pub fn cancel_compaction_contention_window(&self) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("operational store observer mutex poisoned");
        if state.take().is_some() {
            drop(state);
            self.bump_revision();
        }
    }

    fn take_ready_evidence(&self) -> Option<CompactionWindowEvidence> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("operational store observer mutex poisoned");
        let ready_namespace = state.as_ref().and_then(|window| {
            window.namespaces.iter().find_map(|(namespace, proof)| {
                (proof.attempted_nodes == BTreeSet::from([0, 1])
                    && proof.lease_publications > 0
                    && proof.fenced_manifest_publications > 0
                    && proof.background_manifest_publications > 0)
                    .then(|| namespace.clone())
            })
        })?;
        let mut window = state
            .take()
            .expect("ready operational compaction window disappeared");
        let proof = window
            .namespaces
            .remove(&ready_namespace)
            .expect("ready namespace proof disappeared");
        Some(CompactionWindowEvidence {
            event_id: window.event_id,
            start_op: window.start_op,
            namespace: ready_namespace,
            attempted_nodes: proof.attempted_nodes,
            lease_publications: proof.lease_publications,
            fenced_manifest_publications: proof.fenced_manifest_publications,
            background_manifest_publications: proof.background_manifest_publications,
        })
    }

    fn bump_revision(&self) {
        self.shared
            .revision
            .send_modify(|revision| *revision = revision.saturating_add(1));
    }
}

/// Wraps one test node's store so the shared observer can distinguish worker A
/// from worker B without changing production server or compactor interfaces.
#[must_use]
pub fn operational_store_proxy(
    store: &ZeppelinStore,
    observer: OperationalStoreObserver,
    node: u8,
) -> ZeppelinStore {
    assert!(
        node <= 1,
        "operational store node must be 0 or 1, got {node}"
    );
    ZeppelinStore::new(Arc::new(StoreFaultProxy {
        inner: store.inner(),
        scheduler: FaultScheduler::from_schedule(super::FaultSchedule {
            profile: super::FaultProfile::Ops,
            events: Vec::new(),
        }),
        bodies: Mutex::new(BodyHistory::default()),
        admit_stale_manifest_cas_selftest: false,
        operational_observer: Some((observer, node)),
    }))
}

const BODY_HISTORY_CAPACITY: usize = 8;

#[derive(Debug)]
struct BodyVersion {
    key: String,
    current: bytes::Bytes,
    previous: Option<bytes::Bytes>,
}

#[derive(Debug, Default)]
struct BodyHistory {
    entries: VecDeque<BodyVersion>,
}

impl BodyHistory {
    fn observe_put(&mut self, key: String, body: bytes::Bytes) {
        let previous = self
            .entries
            .iter()
            .position(|entry| entry.key == key)
            .and_then(|index| self.entries.remove(index))
            .map(|entry| entry.current);
        self.entries.push_back(BodyVersion {
            key,
            current: body,
            previous,
        });
        while self.entries.len() > BODY_HISTORY_CAPACITY {
            self.entries.pop_front();
        }
    }

    fn most_recent_other(&self, key: &str) -> Option<bytes::Bytes> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.key != key)
            .map(|entry| entry.current.clone())
    }

    fn previous(&self, key: &str) -> Option<bytes::Bytes> {
        self.entries
            .iter()
            .find(|entry| entry.key == key)
            .and_then(|entry| entry.previous.clone())
    }
}

impl StoreFaultProxy {
    fn tracks_bodies(&self) -> bool {
        matches!(
            self.scheduler.schedule().profile,
            super::FaultProfile::Content
                | super::FaultProfile::Semantic
                | super::FaultProfile::ProviderContractAbuse
                | super::FaultProfile::Full
        )
    }

    fn observe_put(&self, key: String, body: bytes::Bytes) {
        self.bodies
            .lock()
            .expect("store fault body-history mutex poisoned")
            .observe_put(key, body);
    }

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
            FaultKind::HoldCall { for_ops } => {
                self.record(
                    action,
                    key,
                    FaultSemantics::WindowActive,
                    ObservedResult::DefiniteNotApplied,
                    Some(format!("parked until logical op +{for_ops}")),
                );
                self.scheduler.wait_for_hold_release(action).await;
                self.record(
                    action,
                    key,
                    FaultSemantics::WindowEnd,
                    ObservedResult::DefiniteNotApplied,
                    Some("released to the inner store".to_string()),
                );
                Ok(())
            }
            FaultKind::PostCommitFail { .. } | FaultKind::TruncatedGetStream { .. } => Ok(()),
            FaultKind::CrashAt {
                point,
                position: TriggerPosition::Pre,
            } => Err(self
                .trigger_crash(action, key, point, TriggerPosition::Pre)
                .await),
            FaultKind::CrashAt {
                position: TriggerPosition::Post,
                ..
            } => Ok(()),
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

    fn process_controller(&self) -> ProcessController {
        self.scheduler
            .process_controller()
            .expect("CrashAt action requires a process controller")
    }

    async fn trigger_crash(
        &self,
        action: &StoreFaultAction,
        key: &str,
        point: CrashPoint,
        position: TriggerPosition,
    ) -> object_store::Error {
        let controller = self.process_controller();
        controller.request_crash(CrashRequest {
            event_id: action.event_id.clone(),
            op_index: action.op_index,
            point,
            position,
            key: key.to_string(),
        });
        controller.park_token.cancelled().await;
        injected_error(InjectedErrorKind::Generic, key)
    }

    async fn copy_with_vanished_source(
        &self,
        action: &StoreFaultAction,
        key: &str,
        from: &Path,
        to: &Path,
        if_not_exists: bool,
    ) -> OsResult<()> {
        let source = self.inner.get(from).await?.bytes().await?;
        self.inner.delete(from).await?;
        let copy_result = if if_not_exists {
            self.inner.copy_if_not_exists(from, to).await
        } else {
            self.inner.copy(from, to).await
        };
        if copy_result.is_ok() {
            self.inner.delete(to).await?;
        }
        self.inner.put(from, PutPayload::from(source)).await?;
        self.record(
            action,
            key,
            FaultSemantics::PostCommit,
            ObservedResult::Corrupted,
            Some(format!(
                "source {from} restored after transient disappearance"
            )),
        );
        match copy_result {
            Ok(()) => Err(injected_error(InjectedErrorKind::NotFound, key)),
            Err(error) => Err(error),
        }
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

fn put_payload_bytes(payload: &PutPayload) -> bytes::Bytes {
    let chunks = payload.as_ref();
    if let [only] = chunks {
        return only.clone();
    }
    let mut bytes = bytes::BytesMut::with_capacity(payload.content_length());
    for chunk in chunks {
        bytes.extend_from_slice(chunk);
    }
    bytes.freeze()
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

async fn content_result(
    result: GetResult,
    mutate: impl FnOnce(bytes::Bytes) -> bytes::Bytes,
) -> OsResult<GetResult> {
    let mut meta = result.meta.clone();
    let attributes = result.attributes.clone();
    let bytes = mutate(result.bytes().await?);
    meta.size = bytes.len();
    let len = bytes.len();
    Ok(GetResult {
        payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
        meta,
        range: 0..len,
        attributes,
    })
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
        if let Some((observer, node)) = &self.operational_observer {
            observer.rendezvous_wal_put(*node, &key).await;
        }
        let tracked_body = self.tracks_bodies().then(|| put_payload_bytes(&payload));
        let operational_body = self
            .operational_observer
            .as_ref()
            .filter(|_| key.ends_with("/lease.json") || key.ends_with("/manifest.json"))
            .map(|_| put_payload_bytes(&payload));
        if let Some(action) = self.scheduler.store_decision(StoreOp::Put, &key) {
            if matches!(action.kind, FaultKind::AdmitStaleManifestCas) {
                assert!(
                    self.admit_stale_manifest_cas_selftest,
                    "stale manifest CAS mutation reached a non-selftest store proxy"
                );
                assert!(
                    key.ends_with("/manifest.json"),
                    "stale manifest CAS mutation reached non-manifest key {key}"
                );
                assert!(
                    matches!(&opts.mode, PutMode::Update(_)),
                    "stale manifest CAS mutation requires PutMode::Update, got {:?}",
                    opts.mode
                );
                let mut admitted_opts = opts;
                admitted_opts.mode = PutMode::Overwrite;
                let result = self.inner.put_opts(location, payload, admitted_opts).await;
                if result.is_ok() {
                    self.record(
                        &action,
                        &key,
                        FaultSemantics::PostCommit,
                        ObservedResult::Corrupted,
                        Some("admitted stale conditional manifest CAS as overwrite".to_string()),
                    );
                }
                return result;
            }
            if matches!(action.kind, FaultKind::CasConflict)
                && matches!(&opts.mode, PutMode::Update(_))
            {
                self.record(
                    &action,
                    &key,
                    FaultSemantics::PreCall,
                    ObservedResult::DefiniteNotApplied,
                    Some(format!("rejected conditional mode {:?}", opts.mode)),
                );
                return Err(injected_error(InjectedErrorKind::Precondition, &key));
            }
            if matches!(
                action.kind,
                FaultKind::Content(super::ContentFault::MisdirectedWrite)
            ) {
                let redirected_key = format!("{key}.misdirected");
                let redirected = Path::from(redirected_key.clone());
                let mut redirected_opts = opts;
                redirected_opts.mode = PutMode::Overwrite;
                let result = self
                    .inner
                    .put_opts(&redirected, payload, redirected_opts)
                    .await;
                if result.is_ok() {
                    self.record(
                        &action,
                        &key,
                        FaultSemantics::PostCommit,
                        ObservedResult::Corrupted,
                        Some(format!("payload persisted at {redirected_key}")),
                    );
                }
                return result;
            }
            if let FaultKind::Content(super::ContentFault::TornWrite { keep_bytes }) = action.kind {
                let bytes = put_payload_bytes(&payload);
                let torn = bytes.slice(..keep_bytes.min(bytes.len()));
                let result = self
                    .inner
                    .put_opts(location, PutPayload::from(torn), opts)
                    .await;
                if result.is_ok() {
                    self.record(
                        &action,
                        &key,
                        FaultSemantics::PostCommit,
                        ObservedResult::Corrupted,
                        Some(format!("persisted only the first {keep_bytes} bytes")),
                    );
                }
                return result;
            }
            if matches!(action.kind, FaultKind::PostCommitFail { .. }) {
                return match self.inner.put_opts(location, payload, opts).await {
                    Ok(_) => Err(self.post_commit_error(&action, &key)),
                    Err(error) => Err(error),
                };
            }
            if let FaultKind::CrashAt {
                point,
                position: TriggerPosition::Post,
            } = action.kind
            {
                return match self.inner.put_opts(location, payload, opts).await {
                    Ok(_) => Err(self
                        .trigger_crash(&action, &key, point, TriggerPosition::Post)
                        .await),
                    Err(error) => Err(error),
                };
            }
            if !matches!(action.kind, FaultKind::CasConflict) {
                self.apply_before(&action, &key).await?;
            }
        }
        let result = self.inner.put_opts(location, payload, opts).await;
        if result.is_ok() {
            if let Some(body) = tracked_body {
                self.observe_put(key.clone(), body);
            }
            if let (Some((observer, node)), Some(body)) =
                (&self.operational_observer, operational_body.as_ref())
            {
                observer.observe_successful_put(*node, &key, body);
            }
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        // Zeppelin's artifact writers use single-shot puts. Multipart protocol
        // faulting is intentionally outside this profile's claimed surface.
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let key = location.to_string();
        if let Some((observer, node)) = &self.operational_observer {
            observer.rendezvous_lease_get(*node, &key).await;
        }
        if let Some(action) = self.scheduler.store_decision(StoreOp::Get, &key) {
            if matches!(action.kind, FaultKind::HeadGetDiverge) {
                self.inner.head(location).await?;
                self.record(
                    &action,
                    &key,
                    FaultSemantics::PostCommit,
                    ObservedResult::Corrupted,
                    Some("HEAD succeeded before injected GET NotFound".to_string()),
                );
                return Err(injected_error(InjectedErrorKind::NotFound, &key));
            }
            if matches!(action.kind, FaultKind::StaleRead) {
                let previous = self
                    .bodies
                    .lock()
                    .expect("store fault body-history mutex poisoned")
                    .previous(&key)
                    .ok_or_else(|| injected_error(InjectedErrorKind::Generic, &key))?;
                let result = self.inner.get_opts(location, options).await?;
                let result = content_result(result, |_| previous).await?;
                self.record(
                    &action,
                    &key,
                    FaultSemantics::PostCommit,
                    ObservedResult::Corrupted,
                    Some("served the previous successful PUT body".to_string()),
                );
                return Ok(result);
            }
            if matches!(
                action.kind,
                FaultKind::Content(super::ContentFault::WrongObject)
            ) {
                let replacement = self
                    .bodies
                    .lock()
                    .expect("store fault body-history mutex poisoned")
                    .most_recent_other(&key)
                    .ok_or_else(|| injected_error(InjectedErrorKind::Generic, &key))?;
                let result = self.inner.get_opts(location, options).await?;
                let replacement_len = replacement.len();
                let result = content_result(result, |_| replacement).await?;
                self.record(
                    &action,
                    &key,
                    FaultSemantics::PostCommit,
                    ObservedResult::Corrupted,
                    Some(format!(
                        "served {replacement_len} bytes from another live key"
                    )),
                );
                return Ok(result);
            }
            if let FaultKind::Content(super::ContentFault::BitFlip { offset_hint }) = action.kind {
                let result = self.inner.get_opts(location, options).await?;
                let result = content_result(result, |bytes| {
                    assert!(!bytes.is_empty(), "BitFlip requires a non-empty GET body");
                    let mut corrupted = bytes.to_vec();
                    let offset = usize::try_from(offset_hint)
                        .unwrap_or(usize::MAX)
                        .wrapping_rem(corrupted.len());
                    corrupted[offset] ^= 1;
                    corrupted.into()
                })
                .await?;
                self.record(
                    &action,
                    &key,
                    FaultSemantics::PostCommit,
                    ObservedResult::Corrupted,
                    Some(format!("successful body bit flipped at hint {offset_hint}")),
                );
                return Ok(result);
            }
            if let FaultKind::Content(super::ContentFault::TruncateBody { keep_bytes }) =
                action.kind
            {
                let result = self.inner.get_opts(location, options).await?;
                let result =
                    content_result(result, |bytes| bytes.slice(..keep_bytes.min(bytes.len())))
                        .await?;
                self.record(
                    &action,
                    &key,
                    FaultSemantics::PostCommit,
                    ObservedResult::Corrupted,
                    Some(format!("successful body truncated to {keep_bytes} bytes")),
                );
                return Ok(result);
            }
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
            if let FaultKind::CrashAt {
                point,
                position: TriggerPosition::Post,
            } = action.kind
            {
                return match self.inner.get_opts(location, options).await {
                    Ok(_) => Err(self
                        .trigger_crash(&action, &key, point, TriggerPosition::Post)
                        .await),
                    Err(error) => Err(error),
                };
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
            if let FaultKind::BatchDeletePartial { fail_every } = action.kind {
                assert!(fail_every > 0, "BatchDeletePartial fail_every must be > 0");
                if action.call_ordinal % u64::from(fail_every) == 0 {
                    self.record(
                        &action,
                        &key,
                        FaultSemantics::PreCall,
                        ObservedResult::Corrupted,
                        Some(format!("retained every {fail_every}th batch delete entry")),
                    );
                    return Err(injected_error(InjectedErrorKind::Generic, &key));
                }
                return self.inner.delete(location).await;
            }
            if matches!(
                action.kind,
                FaultKind::Content(super::ContentFault::SilentDeleteFailure)
            ) {
                self.record(
                    &action,
                    &key,
                    FaultSemantics::PostCommit,
                    ObservedResult::Corrupted,
                    Some("delete acknowledgement returned without mutation".to_string()),
                );
                return Ok(());
            }
            if matches!(action.kind, FaultKind::PostCommitFail { .. }) {
                return match self.inner.delete(location).await {
                    Ok(()) => Err(self.post_commit_error(&action, &key)),
                    Err(error) => Err(error),
                };
            }
            if let FaultKind::CrashAt {
                point,
                position: TriggerPosition::Post,
            } = action.kind
            {
                return match self.inner.delete(location).await {
                    Ok(()) => Err(self
                        .trigger_crash(&action, &key, point, TriggerPosition::Post)
                        .await),
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
                FaultKind::ListOmit { nth } => {
                    self.record(
                        &action,
                        &key,
                        FaultSemantics::PostCommit,
                        ObservedResult::Corrupted,
                        Some(format!("omitted one-based LIST entry {nth}")),
                    );
                    return self
                        .inner
                        .list(prefix)
                        .enumerate()
                        .filter_map(move |(index, result)| {
                            futures::future::ready((index + 1 != nth as usize).then_some(result))
                        })
                        .boxed();
                }
                FaultKind::ListDuplicate { nth } => {
                    self.record(
                        &action,
                        &key,
                        FaultSemantics::PostCommit,
                        ObservedResult::Corrupted,
                        Some(format!("duplicated one-based LIST entry {nth}")),
                    );
                    return self
                        .inner
                        .list(prefix)
                        .enumerate()
                        .flat_map(move |(index, result)| {
                            let items = if index + 1 == nth as usize {
                                match result {
                                    Ok(meta) => vec![Ok(meta.clone()), Ok(meta)],
                                    Err(error) => vec![Err(error)],
                                }
                            } else {
                                vec![result]
                            };
                            stream::iter(items)
                        })
                        .boxed();
                }
                FaultKind::ListReorder => {
                    self.record(
                        &action,
                        &key,
                        FaultSemantics::PostCommit,
                        ObservedResult::Corrupted,
                        Some("reversed one buffered LIST page".to_string()),
                    );
                    let inner = self.inner.list(prefix);
                    return stream::once(async move {
                        let mut items = inner.collect::<Vec<_>>().await;
                        items.reverse();
                        items
                    })
                    .flat_map(stream::iter)
                    .boxed();
                }
                FaultKind::HoldCall { for_ops } => {
                    self.record(
                        &action,
                        &key,
                        FaultSemantics::WindowActive,
                        ObservedResult::DefiniteNotApplied,
                        Some(format!("parked LIST until logical op +{for_ops}")),
                    );
                    let action_for_wait = action.clone();
                    let key_for_wait = key.clone();
                    let mut inner = Some(self.inner.list(prefix));
                    return stream::once(async move {
                        self.scheduler.wait_for_hold_release(&action_for_wait).await;
                        self.record(
                            &action_for_wait,
                            &key_for_wait,
                            FaultSemantics::WindowEnd,
                            ObservedResult::DefiniteNotApplied,
                            Some("released LIST to the inner store".to_string()),
                        );
                    })
                    .flat_map(move |()| inner.take().expect("held LIST stream reused"))
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
            if matches!(
                action.kind,
                FaultKind::ListOmit { .. }
                    | FaultKind::ListDuplicate { .. }
                    | FaultKind::ListReorder
            ) {
                let mut result = self.inner.list_with_delimiter(prefix).await?;
                match action.kind {
                    FaultKind::ListOmit { nth } => {
                        let index = nth.saturating_sub(1) as usize;
                        if index < result.common_prefixes.len() {
                            result.common_prefixes.remove(index);
                        } else {
                            let object_index = index.saturating_sub(result.common_prefixes.len());
                            if object_index < result.objects.len() {
                                result.objects.remove(object_index);
                            }
                        }
                    }
                    FaultKind::ListDuplicate { nth } => {
                        let index = nth.saturating_sub(1) as usize;
                        if let Some(prefix) = result.common_prefixes.get(index).cloned() {
                            result.common_prefixes.insert(index, prefix);
                        } else {
                            let object_index = index.saturating_sub(result.common_prefixes.len());
                            if let Some(object) = result.objects.get(object_index).cloned() {
                                result.objects.insert(object_index, object);
                            }
                        }
                    }
                    FaultKind::ListReorder => {
                        result.common_prefixes.reverse();
                        result.objects.reverse();
                    }
                    _ => unreachable!("delimiter LIST kind was checked"),
                }
                self.record(
                    &action,
                    &key,
                    FaultSemantics::PostCommit,
                    ObservedResult::Corrupted,
                    Some("mutated delimiter LIST page".to_string()),
                );
                return Ok(result);
            }
            self.apply_before(&action, &key).await?;
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        let key = format!("{from}->{to}");
        if let Some(action) = self.scheduler.store_decision(StoreOp::Copy, &key) {
            if matches!(action.kind, FaultKind::CopySourceVanish) {
                return self
                    .copy_with_vanished_source(&action, &key, from, to, false)
                    .await;
            }
            if matches!(action.kind, FaultKind::PostCommitFail { .. }) {
                return match self.inner.copy(from, to).await {
                    Ok(()) => Err(self.post_commit_error(&action, &key)),
                    Err(error) => Err(error),
                };
            }
            if let FaultKind::CrashAt {
                point,
                position: TriggerPosition::Post,
            } = action.kind
            {
                return match self.inner.copy(from, to).await {
                    Ok(()) => Err(self
                        .trigger_crash(&action, &key, point, TriggerPosition::Post)
                        .await),
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
            if matches!(action.kind, FaultKind::CopySourceVanish) {
                return self
                    .copy_with_vanished_source(&action, &key, from, to, true)
                    .await;
            }
            if matches!(action.kind, FaultKind::PostCommitFail { .. }) {
                return match self.inner.copy_if_not_exists(from, to).await {
                    Ok(()) => Err(self.post_commit_error(&action, &key)),
                    Err(error) => Err(error),
                };
            }
            if let FaultKind::CrashAt {
                point,
                position: TriggerPosition::Post,
            } = action.kind
            {
                return match self.inner.copy_if_not_exists(from, to).await {
                    Ok(()) => Err(self
                        .trigger_crash(&action, &key, point, TriggerPosition::Post)
                        .await),
                    Err(error) => Err(error),
                };
            }
            self.apply_before(&action, &key).await?;
        }
        self.inner.copy_if_not_exists(from, to).await
    }
}

pub async fn run_crash_matrix() {
    use bytes::Bytes;
    use object_store::memory::InMemory;

    let points = [
        CrashPoint::WalFragmentPut,
        CrashPoint::ManifestCas,
        CrashPoint::SegmentPut,
        CrashPoint::StagingSideObjectPut,
        CrashPoint::StagingDrop,
        CrashPoint::CloneCopy { nth: 1 },
        CrashPoint::NamespaceDeleteBatch { nth: 1 },
        CrashPoint::SnapshotPut,
        CrashPoint::HydrationGet,
    ];
    for point in points {
        for position in [TriggerPosition::Pre, TriggerPosition::Post] {
            let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
            let key = crash_matrix_key(point);
            let copy_source = "matrix/segments/source.bin";
            if matches!(
                point,
                CrashPoint::StagingDrop
                    | CrashPoint::NamespaceDeleteBatch { .. }
                    | CrashPoint::HydrationGet
            ) {
                inner
                    .put(&key, Bytes::from_static(b"before"))
                    .await
                    .unwrap();
            }
            if matches!(point, CrashPoint::CloneCopy { .. }) {
                inner
                    .put(copy_source, Bytes::from_static(b"copy-source"))
                    .await
                    .unwrap();
            }
            let (store_op, key_substring, _) = point.selector();
            let scheduler =
                FaultScheduler::from_schedule(crate::adversarial::faults::FaultSchedule {
                    profile: crate::adversarial::faults::FaultProfile::Crash,
                    events: vec![crate::adversarial::faults::FaultEvent {
                        id: "crash-00".to_string(),
                        start_op: 0,
                        end_op: None,
                        boundary: Boundary::Process,
                        target: crate::adversarial::faults::TargetSelector {
                            store_op: Some(store_op),
                            key_substring: Some(key_substring.to_string()),
                            ..crate::adversarial::faults::TargetSelector::default()
                        },
                        kind: FaultKind::CrashAt { point, position },
                    }],
                });
            let controller = scheduler.process_controller().unwrap();
            let faulted = store_fault_proxy(&inner, scheduler);
            let task_key = key.clone();
            let task = tokio::spawn(async move {
                match point {
                    CrashPoint::WalFragmentPut
                    | CrashPoint::ManifestCas
                    | CrashPoint::SegmentPut
                    | CrashPoint::StagingSideObjectPut
                    | CrashPoint::SnapshotPut => {
                        faulted.put(&task_key, Bytes::from_static(b"after")).await
                    }
                    CrashPoint::StagingDrop | CrashPoint::NamespaceDeleteBatch { .. } => {
                        faulted.delete(&task_key).await
                    }
                    CrashPoint::CloneCopy { .. } => {
                        faulted
                            .copy_if_not_exists(copy_source, &task_key, "matrix")
                            .await
                    }
                    CrashPoint::HydrationGet => faulted.get(&task_key).await.map(|_| ()),
                }
            });
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                controller.crash_requested.notified(),
            )
            .await
            .unwrap_or_else(|_| panic!("crash matrix point {point:?}/{position:?} did not fire"));
            let request = controller.take_request();
            assert_eq!(request.point, point);
            assert_eq!(request.position, position);
            controller.park_token.cancel();
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
                .await
                .unwrap_or_else(|_| {
                    panic!("crash matrix point {point:?}/{position:?} leaked parked work")
                })
                .unwrap();
            assert!(result.is_err());

            let inner_exists = inner.exists(&key).await.unwrap();
            match (point, position) {
                (
                    CrashPoint::WalFragmentPut
                    | CrashPoint::ManifestCas
                    | CrashPoint::SegmentPut
                    | CrashPoint::StagingSideObjectPut
                    | CrashPoint::SnapshotPut
                    | CrashPoint::CloneCopy { .. },
                    TriggerPosition::Pre,
                ) => assert!(!inner_exists),
                (
                    CrashPoint::WalFragmentPut
                    | CrashPoint::ManifestCas
                    | CrashPoint::SegmentPut
                    | CrashPoint::StagingSideObjectPut
                    | CrashPoint::SnapshotPut
                    | CrashPoint::CloneCopy { .. },
                    TriggerPosition::Post,
                ) => assert!(inner_exists),
                (
                    CrashPoint::StagingDrop | CrashPoint::NamespaceDeleteBatch { .. },
                    TriggerPosition::Pre,
                ) => assert!(inner_exists),
                (
                    CrashPoint::StagingDrop | CrashPoint::NamespaceDeleteBatch { .. },
                    TriggerPosition::Post,
                ) => assert!(!inner_exists),
                (CrashPoint::HydrationGet, _) => assert!(inner_exists),
            }
            tokio::time::timeout(std::time::Duration::from_secs(2), inner.head(&key))
                .await
                .expect("store remained blocked after crash cancellation")
                .ok();
        }
    }
}

fn crash_matrix_key(point: CrashPoint) -> String {
    match point {
        CrashPoint::WalFragmentPut => "matrix/wal/first.wal",
        CrashPoint::ManifestCas => "matrix/manifest.json",
        CrashPoint::SegmentPut => "matrix/segments/output.bin",
        CrashPoint::StagingSideObjectPut | CrashPoint::StagingDrop => "matrix/_staging/1.json",
        CrashPoint::CloneCopy { .. } => "matrix/segments/copied.bin",
        CrashPoint::NamespaceDeleteBatch { .. } => "matrix/delete/me.bin",
        CrashPoint::SnapshotPut => "matrix/snapshots/pin.msgpack",
        CrashPoint::HydrationGet => "matrix/segments/segment/cluster_0.bin",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::memory::InMemory;

    use super::*;
    use crate::adversarial::faults::{
        Boundary, ContentFault, FaultEvent, FaultProfile, FaultSchedule, TargetSelector,
    };

    #[tokio::test]
    async fn truncate_body_returns_successful_short_payload_and_records_taint() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        inner
            .put("ns/object.bin", Bytes::from_static(b"abcdef"))
            .await
            .unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Content,
            events: vec![FaultEvent {
                id: "content-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("object.bin".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::Content(ContentFault::TruncateBody { keep_bytes: 3 }),
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());

        let result = faulted
            .inner()
            .get(&Path::from("ns/object.bin"))
            .await
            .unwrap();
        assert_eq!(result.range, 0..3);
        assert_eq!(result.bytes().await.unwrap(), Bytes::from_static(b"abc"));
        assert_eq!(scheduler.timeline().len(), 1);
        assert_eq!(scheduler.timeline()[0].observed, ObservedResult::Corrupted);
    }

    #[tokio::test]
    async fn bit_flip_changes_one_deterministic_body_bit() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        inner
            .put("ns/object.bin", Bytes::from_static(b"abc"))
            .await
            .unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Content,
            events: vec![FaultEvent {
                id: "content-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("object.bin".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::Content(ContentFault::BitFlip { offset_hint: 1 }),
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());

        let bytes = faulted.get("ns/object.bin").await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"acc"));
        assert_eq!(
            scheduler.timeline()[0].key.as_deref(),
            Some("ns/object.bin")
        );
        assert_eq!(scheduler.timeline()[0].observed, ObservedResult::Corrupted);
    }

    #[tokio::test]
    async fn torn_write_persists_only_prefix_while_returning_success() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Content,
            events: vec![FaultEvent {
                id: "content-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some("torn.bin".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::Content(ContentFault::TornWrite { keep_bytes: 3 }),
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());

        faulted
            .put("ns/torn.bin", Bytes::from_static(b"abcdef"))
            .await
            .unwrap();
        assert_eq!(
            inner.get("ns/torn.bin").await.unwrap(),
            Bytes::from_static(b"abc")
        );
        assert_eq!(scheduler.timeline()[0].observed, ObservedResult::Corrupted);
    }

    #[tokio::test]
    async fn misdirected_write_redirects_payload_and_leaves_real_key_absent() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Content,
            events: vec![FaultEvent {
                id: "content-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some("segment.bin".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::Content(ContentFault::MisdirectedWrite),
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());

        faulted
            .put("ns/segment.bin", Bytes::from_static(b"segment"))
            .await
            .unwrap();
        assert!(!inner.exists("ns/segment.bin").await.unwrap());
        assert_eq!(
            inner.get("ns/segment.bin.misdirected").await.unwrap(),
            Bytes::from_static(b"segment")
        );
        assert!(scheduler.timeline()[0]
            .recovery
            .as_deref()
            .is_some_and(|note| note.contains("segment.bin.misdirected")));
    }

    #[tokio::test]
    async fn silent_delete_failure_returns_success_without_deleting() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        inner
            .put("ns/orphan.wal", Bytes::from_static(b"wal"))
            .await
            .unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Content,
            events: vec![FaultEvent {
                id: "content-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Delete),
                    key_substring: Some("orphan.wal".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::Content(ContentFault::SilentDeleteFailure),
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());

        faulted.delete("ns/orphan.wal").await.unwrap();
        assert!(inner.exists("ns/orphan.wal").await.unwrap());
        assert_eq!(scheduler.timeline()[0].observed, ObservedResult::Corrupted);
    }

    #[tokio::test]
    async fn wrong_object_serves_most_recent_other_key_body() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Content,
            events: vec![FaultEvent {
                id: "content-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("second.bin".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::Content(ContentFault::WrongObject),
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());
        faulted
            .put("ns/first.bin", Bytes::from_static(b"first"))
            .await
            .unwrap();
        faulted
            .put("ns/second.bin", Bytes::from_static(b"second"))
            .await
            .unwrap();

        assert_eq!(
            faulted.get("ns/second.bin").await.unwrap(),
            Bytes::from_static(b"first")
        );
        assert_eq!(scheduler.timeline()[0].observed, ObservedResult::Corrupted);
    }

    #[tokio::test]
    async fn stale_read_serves_previous_body_once() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("versioned.bin".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::StaleRead,
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());
        faulted
            .put("ns/versioned.bin", Bytes::from_static(b"v1"))
            .await
            .unwrap();
        faulted
            .put("ns/versioned.bin", Bytes::from_static(b"v2"))
            .await
            .unwrap();

        assert_eq!(
            faulted.get("ns/versioned.bin").await.unwrap(),
            Bytes::from_static(b"v1")
        );
        assert_eq!(
            inner.get("ns/versioned.bin").await.unwrap(),
            Bytes::from_static(b"v2")
        );
        assert_eq!(scheduler.timeline()[0].observed, ObservedResult::Corrupted);
    }

    #[tokio::test]
    async fn list_omit_drops_the_selected_entry_from_successful_stream() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        for key in ["ns/a", "ns/b", "ns/c"] {
            inner.put(key, Bytes::from_static(b"x")).await.unwrap();
        }
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::List),
                    key_substring: Some("ns".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::ListOmit { nth: 2 },
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());

        assert_eq!(
            faulted.list_prefix("ns/").await.unwrap(),
            vec!["ns/a".to_string(), "ns/c".to_string()]
        );
        assert_eq!(scheduler.timeline()[0].observed, ObservedResult::Corrupted);
    }

    #[tokio::test]
    async fn list_duplicate_emits_the_selected_entry_twice() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        for key in ["ns/a", "ns/b", "ns/c"] {
            inner.put(key, Bytes::from_static(b"x")).await.unwrap();
        }
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::List),
                    key_substring: Some("ns".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::ListDuplicate { nth: 2 },
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler);

        assert_eq!(
            faulted.list_prefix("ns/").await.unwrap(),
            vec![
                "ns/a".to_string(),
                "ns/b".to_string(),
                "ns/b".to_string(),
                "ns/c".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn list_reorder_reverses_one_successful_page() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        for key in ["ns/a", "ns/b", "ns/c"] {
            inner.put(key, Bytes::from_static(b"x")).await.unwrap();
        }
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::List),
                    key_substring: Some("ns".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::ListReorder,
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler);

        assert_eq!(
            faulted.list_prefix("ns/").await.unwrap(),
            vec!["ns/c".to_string(), "ns/b".to_string(), "ns/a".to_string()]
        );
    }

    #[tokio::test]
    async fn delimiter_list_omit_removes_selected_common_prefix() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        for key in ["root/a/object", "root/b/object"] {
            inner.put(key, Bytes::from_static(b"x")).await.unwrap();
        }
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::List),
                    key_substring: Some("root".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::ListOmit { nth: 1 },
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler);

        assert_eq!(
            faulted.list_common_prefixes("root/").await.unwrap(),
            vec!["root/b".to_string()]
        );
    }

    #[tokio::test]
    async fn cas_conflict_only_rejects_update_without_changing_object() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        inner
            .put("ns/manifest.json", Bytes::from_static(b"before"))
            .await
            .unwrap();

        let overwrite_scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-overwrite".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some("manifest.json".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::CasConflict,
            }],
        });
        let overwrite_faulted = store_fault_proxy(&inner, overwrite_scheduler);

        overwrite_faulted
            .put("ns/manifest.json", Bytes::from_static(b"overwritten"))
            .await
            .unwrap();
        assert_eq!(
            inner.get("ns/manifest.json").await.unwrap(),
            Bytes::from_static(b"overwritten")
        );

        let etag = inner
            .head("ns/manifest.json")
            .await
            .unwrap()
            .e_tag
            .expect("in-memory object must expose an etag");
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some("manifest.json".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::CasConflict,
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());

        assert!(faulted
            .put_if_match(
                "ns/manifest.json",
                Bytes::from_static(b"after"),
                &etag,
                "ns",
            )
            .await
            .is_err());
        assert_eq!(
            inner.get("ns/manifest.json").await.unwrap(),
            Bytes::from_static(b"overwritten")
        );
        assert_eq!(
            scheduler.timeline()[0].observed,
            ObservedResult::DefiniteNotApplied
        );
    }

    #[tokio::test]
    async fn stale_manifest_cas_selftest_admits_exactly_one_conditional_update() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        inner
            .put("ns/manifest.json", Bytes::from_static(b"base"))
            .await
            .unwrap();
        let stale_etag = inner
            .head("ns/manifest.json")
            .await
            .unwrap()
            .e_tag
            .expect("in-memory object must expose an etag");
        inner
            .put_if_match(
                "ns/manifest.json",
                Bytes::from_static(b"winner"),
                &stale_etag,
                "ns",
            )
            .await
            .unwrap();

        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Ops,
            events: vec![FaultEvent {
                id: "dual-writer-stale-cas-selftest".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some("manifest.json".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::AdmitStaleManifestCas,
            }],
        });
        let faulted = stale_manifest_cas_selftest_proxy(&inner, scheduler.clone());

        faulted
            .put_if_match(
                "ns/manifest.json",
                Bytes::from_static(b"stale"),
                &stale_etag,
                "ns",
            )
            .await
            .unwrap();
        assert_eq!(
            inner.get("ns/manifest.json").await.unwrap(),
            Bytes::from_static(b"stale")
        );

        assert!(faulted
            .put_if_match(
                "ns/manifest.json",
                Bytes::from_static(b"second-stale"),
                &stale_etag,
                "ns",
            )
            .await
            .is_err());
        let timeline = scheduler.timeline();
        assert_eq!(timeline.len(), 1);
        assert_eq!(timeline[0].semantics, FaultSemantics::PostCommit);
        assert_eq!(timeline[0].observed, ObservedResult::Corrupted);
        assert_eq!(
            timeline[0].recovery.as_deref(),
            Some("admitted stale conditional manifest CAS as overwrite")
        );
    }

    #[tokio::test]
    async fn operational_observer_rejects_foreground_lease_and_manifest_activity() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        let observer = OperationalStoreObserver::default();
        let node_a = operational_store_proxy(&inner, observer.clone(), 0);
        let node_b = operational_store_proxy(&inner, observer.clone(), 1);
        observer.arm_compaction_contention_window("ops-second-node", 40);

        let node_a_get = tokio::spawn(async move { node_a.get("ns/lease.json").await });
        let node_b_get = tokio::spawn(async move { node_b.get("ns/lease.json").await });
        assert!(node_a_get.await.unwrap().is_err());
        assert!(node_b_get.await.unwrap().is_err());

        let node_a = operational_store_proxy(&inner, observer.clone(), 0);
        node_a
            .put(
                "ns/lease.json",
                Bytes::from(
                    serde_json::to_vec(&serde_json::json!({
                        "holder_id": "node-a",
                        "fencing_token": 1,
                        "acquired_at": "2026-07-11T00:00:00Z",
                        "expires_at": "2026-07-11T00:00:30Z"
                    }))
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
        let mut manifest = zeppelin::wal::Manifest::new();
        manifest.fencing_token = 1;
        node_a
            .put("ns/manifest.json", manifest.to_bytes().unwrap())
            .await
            .unwrap();

        let waiter = tokio::spawn({
            let observer = observer.clone();
            async move {
                observer
                    .wait_for_compaction_evidence(std::time::Duration::from_millis(25))
                    .await
            }
        });
        assert!(
            waiter.await.is_err(),
            "foreground lease and fenced-manifest traffic must not prove background activity"
        );

        observer.cancel_compaction_contention_window();
        observer.arm_compaction_contention_window("ops-after-timeout", 41);
        observer.cancel_compaction_contention_window();
    }

    #[tokio::test]
    async fn operational_observer_rejects_cross_namespace_node_tags() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        let observer = OperationalStoreObserver::default();
        let node_a = operational_store_proxy(&inner, observer.clone(), 0);
        let node_b = operational_store_proxy(&inner, observer.clone(), 1);
        observer.arm_compaction_contention_window("ops-cross-namespace", 40);

        let node_a_get = tokio::spawn(crate::common::server::with_background_compaction_origin(
            async move { node_a.get("ns-a/lease.json").await },
        ));
        let node_b_get = tokio::spawn(crate::common::server::with_background_compaction_origin(
            async move { node_b.get("ns-b/lease.json").await },
        ));
        tokio::task::yield_now().await;

        let proof_store = operational_store_proxy(&inner, observer.clone(), 0);
        crate::common::server::with_background_compaction_origin(async move {
            proof_store
                .put(
                    "ns-a/lease.json",
                    Bytes::from(
                        serde_json::to_vec(&serde_json::json!({
                            "holder_id": "node-a",
                            "fencing_token": 1,
                            "acquired_at": "2026-07-11T00:00:00Z",
                            "expires_at": "2026-07-11T00:00:30Z"
                        }))
                        .unwrap(),
                    ),
                )
                .await
                .unwrap();
        })
        .await;
        let mut manifest = zeppelin::wal::Manifest::new();
        manifest.fencing_token = 1;
        let proof_store = operational_store_proxy(&inner, observer.clone(), 0);
        crate::common::server::with_background_compaction_origin(async move {
            proof_store
                .put("ns-a/manifest.json", manifest.to_bytes().unwrap())
                .await
                .unwrap();
        })
        .await;

        let waiter = tokio::spawn({
            let observer = observer.clone();
            async move {
                observer
                    .wait_for_compaction_evidence(std::time::Duration::from_millis(25))
                    .await
            }
        });
        assert!(
            waiter.await.is_err(),
            "node tags from ns-a and ns-b must not prove same-namespace contention"
        );

        observer.cancel_compaction_contention_window();
        let node_a_bytes = node_a_get
            .await
            .unwrap()
            .expect("cancelled ns-a rendezvous must delegate to the now-present lease");
        assert_eq!(node_a_bytes, inner.get("ns-a/lease.json").await.unwrap());
        assert!(node_b_get.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn operational_observer_holds_wal_put_until_delete_rendezvous_release() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        let observer = OperationalStoreObserver::default();
        let node_a = operational_store_proxy(&inner, observer.clone(), 0);
        observer.arm_mutation_rendezvous("ops-delete-race", 17, "ns");

        let pending_put = tokio::spawn(async move {
            node_a
                .put("ns/wal/fragment.wal", Bytes::from_static(b"fragment"))
                .await
        });
        let entered = observer
            .wait_for_mutation_rendezvous(std::time::Duration::from_secs(1))
            .await;
        assert_eq!(entered.event_id, "ops-delete-race");
        assert_eq!(entered.op_index, 17);
        assert_eq!(entered.namespace, "ns");
        assert_eq!(entered.node, 0);
        assert!(!pending_put.is_finished());
        assert!(!inner.exists("ns/wal/fragment.wal").await.unwrap());

        let released = observer.release_mutation_rendezvous("ops-delete-race");
        assert_eq!(released, entered);
        pending_put.await.unwrap().unwrap();
        assert_eq!(
            inner.get("ns/wal/fragment.wal").await.unwrap(),
            Bytes::from_static(b"fragment")
        );
    }

    #[tokio::test]
    async fn operational_observer_mutation_timeout_does_not_poison_rendezvous() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        let observer = OperationalStoreObserver::default();
        observer.arm_mutation_rendezvous("ops-delete-timeout", 17, "ns");

        let waiter = tokio::spawn({
            let observer = observer.clone();
            async move {
                observer
                    .wait_for_mutation_rendezvous(std::time::Duration::from_millis(25))
                    .await
            }
        });
        assert!(
            waiter.await.is_err(),
            "an unmatched mutation rendezvous must time out loudly"
        );

        let node_a = operational_store_proxy(&inner, observer.clone(), 0);
        let pending_put = tokio::spawn(async move {
            node_a
                .put("ns/wal/fragment.wal", Bytes::from_static(b"fragment"))
                .await
        });
        let entered = observer
            .wait_for_mutation_rendezvous(std::time::Duration::from_secs(1))
            .await;
        assert_eq!(entered.event_id, "ops-delete-timeout");
        assert_eq!(entered.op_index, 17);
        assert_eq!(entered.namespace, "ns");
        assert_eq!(entered.node, 0);

        let released = observer.release_mutation_rendezvous("ops-delete-timeout");
        assert_eq!(released, entered);
        pending_put.await.unwrap().unwrap();
        assert_eq!(
            inner.get("ns/wal/fragment.wal").await.unwrap(),
            Bytes::from_static(b"fragment")
        );
    }

    #[tokio::test]
    async fn head_get_diverge_keeps_head_success_and_fails_get_once() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        inner
            .put("ns/diverge.bin", Bytes::from_static(b"body"))
            .await
            .unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("diverge.bin".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::HeadGetDiverge,
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());

        assert_eq!(faulted.head("ns/diverge.bin").await.unwrap().size, 4);
        assert!(matches!(
            faulted.get("ns/diverge.bin").await.unwrap_err(),
            zeppelin::error::ZeppelinError::NotFound { key }
                if key == "ns/diverge.bin"
        ));
        assert!(inner.exists("ns/diverge.bin").await.unwrap());
        assert_eq!(scheduler.timeline()[0].observed, ObservedResult::Corrupted);
    }

    #[tokio::test]
    async fn batch_delete_partial_fails_every_selected_entry() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        for key in ["ns/a", "ns/b", "ns/c"] {
            inner.put(key, Bytes::from_static(b"x")).await.unwrap();
        }
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: Some(10),
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Delete),
                    key_substring: Some("ns/".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::BatchDeletePartial { fail_every: 2 },
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler);
        let locations =
            stream::iter(["ns/a", "ns/b", "ns/c"].map(|key| Ok(Path::from(key)))).boxed();

        let results = faulted
            .inner()
            .delete_stream(locations)
            .collect::<Vec<_>>()
            .await;
        assert!(results[0].is_ok());
        assert!(results[1].is_err());
        assert!(results[2].is_ok());
        assert!(!inner.exists("ns/a").await.unwrap());
        assert!(inner.exists("ns/b").await.unwrap());
        assert!(!inner.exists("ns/c").await.unwrap());
    }

    #[tokio::test]
    async fn copy_source_vanish_fails_copy_and_restores_source() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        inner
            .put("source/segment.bin", Bytes::from_static(b"segment"))
            .await
            .unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Copy),
                    key_substring: Some("source/segment.bin".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::CopySourceVanish,
            }],
        });
        let faulted = store_fault_proxy(&inner, scheduler.clone());

        assert!(faulted
            .copy_if_not_exists("source/segment.bin", "target/segment.bin", "target")
            .await
            .is_err());
        assert_eq!(
            inner.get("source/segment.bin").await.unwrap(),
            Bytes::from_static(b"segment")
        );
        assert!(!inner.exists("target/segment.bin").await.unwrap());
        assert_eq!(scheduler.timeline()[0].observed, ObservedResult::Corrupted);
    }

    #[tokio::test]
    async fn hold_call_releases_only_after_the_configured_logical_ops() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        inner
            .put("ns/manifest.json", Bytes::from_static(b"manifest"))
            .await
            .unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Sched,
            events: vec![FaultEvent {
                id: "sched-00".to_string(),
                start_op: 5,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("manifest.json".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::HoldCall { for_ops: 3 },
            }],
        });
        let _ = scheduler.advance_to(5);
        let faulted = store_fault_proxy(&inner, scheduler.clone());
        let mut held = tokio::spawn(async move { faulted.get("ns/manifest.json").await });

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut held)
                .await
                .is_err(),
            "matching call completed before its logical release point"
        );
        let _ = scheduler.advance_to(7);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut held)
                .await
                .is_err(),
            "matching call released before three logical ops elapsed"
        );
        let _ = scheduler.advance_to(8);
        let bytes = tokio::time::timeout(std::time::Duration::from_secs(1), held)
            .await
            .expect("held call did not release at its logical deadline")
            .unwrap()
            .unwrap();

        assert_eq!(bytes, Bytes::from_static(b"manifest"));
        assert_eq!(scheduler.timeline().len(), 2);
        assert_eq!(
            scheduler.timeline()[0].semantics,
            FaultSemantics::WindowActive
        );
        assert_eq!(scheduler.timeline()[1].semantics, FaultSemantics::WindowEnd);
    }

    #[tokio::test]
    async fn hold_call_parks_list_streams_until_the_logical_release() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        inner.put("ns/a", Bytes::from_static(b"a")).await.unwrap();
        inner.put("ns/b", Bytes::from_static(b"b")).await.unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Sched,
            events: vec![FaultEvent {
                id: "sched-list".to_string(),
                start_op: 2,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::List),
                    key_substring: Some("ns".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::HoldCall { for_ops: 2 },
            }],
        });
        let _ = scheduler.advance_to(2);
        let faulted = store_fault_proxy(&inner, scheduler.clone());
        let mut held = tokio::spawn(async move { faulted.list_prefix("ns/").await });

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut held)
                .await
                .is_err(),
            "LIST completed before the hold released"
        );
        let _ = scheduler.advance_to(4);
        let mut keys = tokio::time::timeout(std::time::Duration::from_secs(1), held)
            .await
            .expect("held LIST did not release")
            .unwrap()
            .unwrap();
        keys.sort();

        assert_eq!(keys, vec!["ns/a".to_string(), "ns/b".to_string()]);
    }

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
