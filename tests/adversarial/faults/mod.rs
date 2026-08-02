pub mod clock;
pub mod http_proxy;
pub mod process;
pub mod store_proxy;

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reqwest::Method;
use serde::{Deserialize, Serialize};

use self::process::{CrashPoint, ProcessController, TriggerPosition};
use super::chaos::StoreOp;
use super::security_program::SECURITY_AUDIT_BARRIER_OP;

const FAULT_WINDOW_TRAILING_OPS: u64 = 8;
const LATE_STREAM_FIRST_FAULT_OP: u64 = 80;
const LATE_STREAM_FAULT_LIMIT: u64 = 480;
const LATE_STREAM_KEY_SUBSTRINGS: [&str; 3] = ["late/segments/", "/matrix_", "/attrs_"];

tokio::task_local! {
    static ARMED_HOLD_EVENT_ID: String;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FaultSchedule {
    pub profile: FaultProfile,
    pub events: Vec<FaultEvent>,
}

impl FaultSchedule {
    #[must_use]
    pub fn contracts(&self) -> Vec<FaultContract> {
        self.events.iter().map(FaultContract::from).collect()
    }

    #[must_use]
    pub fn blocks_v1(&self) -> bool {
        self.events
            .iter()
            .all(|event| event.contract_class().blocks_v1())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FaultProfile {
    LegacyChaos,
    PostCommit,
    Network,
    Crash,
    Clock,
    SupportedFull,
    Security,
    /// Stable deterministic namespace-branching workload; it schedules no
    /// generic fault family because the branch program carries its own replay
    /// and lifecycle boundaries.
    Branching,
    /// Deterministic MMLI-heavy workload. Phase 1 schedules no faults.
    Late,
    /// MMLI-heavy workload with deterministic streamed-read fault windows.
    LateStream,
    ProviderContractAbuse,
    FutureArchitecture,
    // Legacy Phase 5-7 profiles remain decodable and explicitly runnable for
    // artifact replay. New default schedules do not select them.
    Content,
    Semantic,
    Sched,
    Ops,
    Full,
}

impl FaultProfile {
    #[must_use]
    pub fn from_env(value: &str) -> Self {
        match value {
            "legacy_chaos" => Self::LegacyChaos,
            "post_commit" => Self::PostCommit,
            "network" => Self::Network,
            "crash" => Self::Crash,
            "clock" => Self::Clock,
            "supported_full" => Self::SupportedFull,
            "security" => Self::Security,
            "branching" => Self::Branching,
            "late" => Self::Late,
            "late-stream" => Self::LateStream,
            "provider_contract_abuse" => Self::ProviderContractAbuse,
            "future_architecture" => Self::FutureArchitecture,
            "content" => Self::Content,
            "semantic" => Self::Semantic,
            "sched" => Self::Sched,
            "ops" => Self::Ops,
            "full" => Self::Full,
            other => panic!("invalid ZEPPELIN_ADVERSARIAL_PROFILE: {other}"),
        }
    }

    #[must_use]
    pub fn as_env(self) -> &'static str {
        match self {
            Self::LegacyChaos => "legacy_chaos",
            Self::PostCommit => "post_commit",
            Self::Network => "network",
            Self::Crash => "crash",
            Self::Clock => "clock",
            Self::SupportedFull => "supported_full",
            Self::Security => "security",
            Self::Branching => "branching",
            Self::Late => "late",
            Self::LateStream => "late-stream",
            Self::ProviderContractAbuse => "provider_contract_abuse",
            Self::FutureArchitecture => "future_architecture",
            Self::Content => "content",
            Self::Semantic => "semantic",
            Self::Sched => "sched",
            Self::Ops => "ops",
            Self::Full => "full",
        }
    }

    fn id_prefix(self) -> &'static str {
        match self {
            Self::LegacyChaos => "legacy-chaos",
            Self::PostCommit => "post-commit",
            Self::Network => "network",
            Self::Crash => "crash",
            Self::Clock => "clock",
            Self::SupportedFull => "supported-full",
            Self::Security => "security",
            Self::Branching => "branching",
            Self::Late => "late",
            Self::LateStream => "late-stream",
            Self::ProviderContractAbuse => "provider-contract-abuse",
            Self::FutureArchitecture => "future-architecture",
            Self::Content => "content",
            Self::Semantic => "semantic",
            Self::Sched => "sched",
            Self::Ops => "ops",
            Self::Full => "full",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContentFault {
    TruncateBody { keep_bytes: usize },
    BitFlip { offset_hint: u64 },
    WrongObject,
    TornWrite { keep_bytes: usize },
    MisdirectedWrite,
    SilentDeleteFailure,
}

/// Product contract under which a generated fault finding may be interpreted.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ContractClass {
    SupportedV1,
    ProviderContractAbuse,
    FutureArchitecture,
    HarnessSelfTest,
}

impl ContractClass {
    #[must_use]
    pub fn blocks_v1(self) -> bool {
        matches!(self, Self::SupportedV1)
    }
}

/// Protected assumptions named by the overreach audit.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProtectedAssumption {
    A1,
    A2,
    A3,
    A4,
    A5,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FaultContract {
    pub event_id: String,
    pub contract_class: ContractClass,
    pub violated_assumptions: Vec<ProtectedAssumption>,
}

impl From<&FaultEvent> for FaultContract {
    fn from(event: &FaultEvent) -> Self {
        Self {
            event_id: event.id.clone(),
            contract_class: event.contract_class(),
            violated_assumptions: event.violated_assumptions().to_vec(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FaultEvent {
    pub id: String,
    pub start_op: u64,
    pub end_op: Option<u64>,
    pub boundary: Boundary,
    pub target: TargetSelector,
    pub kind: FaultKind,
}

impl FaultEvent {
    /// Classifies the simulated behavior independently of its legacy profile.
    #[must_use]
    pub fn contract_class(&self) -> ContractClass {
        match self.kind {
            FaultKind::Content(_)
            | FaultKind::ListOmit { .. }
            | FaultKind::ListDuplicate { .. }
            | FaultKind::ListReorder
            | FaultKind::StaleRead
            | FaultKind::HeadGetDiverge => ContractClass::ProviderContractAbuse,
            FaultKind::StartSecondNode { .. } => ContractClass::FutureArchitecture,
            FaultKind::AdmitStaleManifestCas => ContractClass::HarnessSelfTest,
            FaultKind::PreFail { .. }
            | FaultKind::Latency { .. }
            | FaultKind::Partition { .. }
            | FaultKind::PostCommitFail { .. }
            | FaultKind::TruncatedGetStream { .. }
            | FaultKind::DropRequest
            | FaultKind::DropResponse
            | FaultKind::TruncateResponse { .. }
            | FaultKind::ResetAfterRequest
            | FaultKind::ClientCancel { .. }
            | FaultKind::DuplicateRetry
            | FaultKind::CrashAt { .. }
            | FaultKind::ClockJump { .. }
            | FaultKind::ClockFreeze { .. }
            | FaultKind::CasConflict
            | FaultKind::BatchDeletePartial { .. }
            | FaultKind::CopySourceVanish
            | FaultKind::HoldCall { .. }
            | FaultKind::StartReadOnlyNode { .. }
            | FaultKind::PatchConfigDuringTraffic
            | FaultKind::DeleteNamespaceInFlight
            | FaultKind::FillDiskCache
            | FaultKind::ResourceExhaustion { .. } => ContractClass::SupportedV1,
        }
    }

    #[must_use]
    pub fn violated_assumptions(&self) -> &'static [ProtectedAssumption] {
        match self.kind {
            FaultKind::Content(ContentFault::TornWrite { .. })
            | FaultKind::Content(ContentFault::MisdirectedWrite)
            | FaultKind::Content(ContentFault::SilentDeleteFailure) => &[ProtectedAssumption::A1],
            FaultKind::Content(ContentFault::TruncateBody { .. })
            | FaultKind::Content(ContentFault::BitFlip { .. })
            | FaultKind::Content(ContentFault::WrongObject)
            | FaultKind::ListOmit { .. }
            | FaultKind::ListDuplicate { .. }
            | FaultKind::ListReorder
            | FaultKind::StaleRead
            | FaultKind::HeadGetDiverge => &[ProtectedAssumption::A2],
            FaultKind::StartSecondNode { .. } => &[ProtectedAssumption::A3],
            FaultKind::AdmitStaleManifestCas => &[ProtectedAssumption::A1],
            _ => &[],
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Boundary {
    ObjectStore,
    ClientHttp,
    Process,
    Clock,
    Runner,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TargetSelector {
    pub store_op: Option<StoreOp>,
    pub key_substring: Option<String>,
    pub path_substring: Option<String>,
    pub methods: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FaultKind {
    PreFail {
        error: InjectedErrorKind,
    },
    Latency {
        base_ms: u64,
        jitter_ms: u64,
    },
    Partition {
        direction: Direction,
    },
    PostCommitFail {
        error: InjectedErrorKind,
    },
    TruncatedGetStream {
        after_bytes: usize,
    },
    DropRequest,
    DropResponse,
    TruncateResponse {
        at_bytes: usize,
    },
    ResetAfterRequest,
    ClientCancel {
        after_ms: u64,
    },
    DuplicateRetry,
    CrashAt {
        point: CrashPoint,
        position: TriggerPosition,
    },
    ClockJump {
        delta_ms: i64,
    },
    ClockFreeze {
        for_ops: u64,
    },
    Content(ContentFault),
    CasConflict,
    /// Test-only fault that admits one stale conditional manifest publication.
    /// Regular seeded schedules never construct this variant; callers must also
    /// opt into the dedicated store-proxy constructor.
    AdmitStaleManifestCas,
    ListOmit {
        nth: u32,
    },
    ListDuplicate {
        nth: u32,
    },
    ListReorder,
    StaleRead,
    HeadGetDiverge,
    BatchDeletePartial {
        fail_every: u32,
    },
    CopySourceVanish,
    HoldCall {
        for_ops: u64,
    },
    StartSecondNode {
        for_ops: u64,
    },
    StartReadOnlyNode {
        for_ops: u64,
    },
    PatchConfigDuringTraffic,
    DeleteNamespaceInFlight,
    FillDiskCache,
    ResourceExhaustion {
        max_concurrent_queries: usize,
        disk_cache_max_bytes: u64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InjectedErrorKind {
    Generic,
    NotFound,
    Precondition,
    Throttle429,
    Http500,
    Http503,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    All,
    ReadsFail,
    WritesFail,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FaultSemantics {
    PreCall,
    PostCommit,
    WindowActive,
    WindowEnd,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObservedResult {
    DefiniteNotApplied,
    DefiniteApplied,
    Ambiguous,
    Corrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub event_id: String,
    pub op_index: u64,
    pub wall_ms: u64,
    pub boundary: Boundary,
    pub action: String,
    pub key: Option<String>,
    pub semantics: FaultSemantics,
    pub observed: ObservedResult,
    pub recovery: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StoreFaultAction {
    pub event_id: String,
    pub op_index: u64,
    pub kind: FaultKind,
    pub call_ordinal: u64,
    pub latency_ms: Option<u64>,
    pub window: bool,
    event_index: usize,
    deferred_get: bool,
    reserved_get: bool,
}

impl StoreFaultAction {
    #[must_use]
    pub fn is_deferred_get(&self) -> bool {
        self.deferred_get
    }
}

#[derive(Debug, Clone)]
pub struct HttpFaultAction {
    pub event_id: String,
    pub op_index: u64,
    pub kind: FaultKind,
    pub window: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClockCommand {
    Jump { event_id: String, delta_ms: i64 },
    Freeze { event_id: String, for_ops: u64 },
    Thaw { event_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerCommand {
    Clock(ClockCommand),
    StartSecondNode {
        event_id: String,
        for_ops: u64,
    },
    StopSecondNode {
        event_id: String,
    },
    StartReadOnlyNode {
        event_id: String,
        for_ops: u64,
    },
    StopReadOnlyNode {
        event_id: String,
    },
    PatchConfigDuringTraffic {
        event_id: String,
    },
    DeleteNamespaceInFlight {
        event_id: String,
    },
    FillDiskCache {
        event_id: String,
    },
    ResourceExhaustion {
        event_id: String,
        max_concurrent_queries: usize,
        disk_cache_max_bytes: u64,
    },
}

impl SchedulerCommand {
    #[must_use]
    pub fn contract_class(&self) -> ContractClass {
        match self {
            Self::StartSecondNode { .. } | Self::StopSecondNode { .. } => {
                ContractClass::FutureArchitecture
            }
            Self::Clock(_)
            | Self::StartReadOnlyNode { .. }
            | Self::StopReadOnlyNode { .. }
            | Self::PatchConfigDuringTraffic { .. }
            | Self::DeleteNamespaceInFlight { .. }
            | Self::FillDiskCache { .. }
            | Self::ResourceExhaustion { .. } => ContractClass::SupportedV1,
        }
    }

    #[must_use]
    pub fn violated_assumptions(&self) -> &'static [ProtectedAssumption] {
        match self {
            Self::StartSecondNode { .. } | Self::StopSecondNode { .. } => {
                &[ProtectedAssumption::A3]
            }
            _ => &[],
        }
    }
}

#[derive(Debug)]
struct EventRuntime {
    fired: AtomicBool,
    ended: AtomicBool,
    matches: AtomicU64,
    claimed_op: AtomicU64,
    store_reservation: Mutex<StoreReservationState>,
    store_reservation_revision_tx: tokio::sync::watch::Sender<u64>,
}

#[derive(Debug, Default)]
struct StoreReservationState {
    owner: Option<u64>,
    waiters: VecDeque<u64>,
}

#[derive(Debug)]
struct SchedulerRuntime {
    logical_op: AtomicU64,
    logical_op_tx: tokio::sync::watch::Sender<u64>,
    quiesced: AtomicBool,
    release_held_calls: AtomicBool,
    retirement_release_held_calls: AtomicBool,
    armed_hold_event_id: Mutex<Option<String>>,
    timeline: Mutex<Vec<TimelineEvent>>,
    timeline_revision_tx: tokio::sync::watch::Sender<u64>,
    events: Vec<EventRuntime>,
    store_calls: AtomicU64,
    started: Instant,
}

#[derive(Debug, Clone)]
pub struct FaultScheduler {
    schedule: Arc<FaultSchedule>,
    runtime: Arc<SchedulerRuntime>,
    rng_salt: u64,
    process: Option<Arc<Mutex<ProcessController>>>,
}

/// Scoped release of calls held by a server generation being retired.
///
/// Dropping the guard restores hold behavior for the replacement generation;
/// the permanent quiet-period release remains a separate scheduler state.
pub struct HeldCallRetirementGuard {
    runtime: Arc<SchedulerRuntime>,
}

impl Drop for HeldCallRetirementGuard {
    fn drop(&mut self) {
        assert!(
            self.runtime
                .retirement_release_held_calls
                .swap(false, Ordering::SeqCst),
            "held-call retirement guard dropped after its epoch was already inactive"
        );
        let current = self.runtime.logical_op.load(Ordering::SeqCst);
        self.runtime.logical_op_tx.send_replace(current);
    }
}

struct StoreReservationWaiter {
    runtime: Arc<SchedulerRuntime>,
    event_index: usize,
    call_ordinal: u64,
    queued: bool,
}

impl StoreReservationWaiter {
    fn enqueue(scheduler: &FaultScheduler, event_index: usize, call_ordinal: u64) -> Self {
        let event_runtime = &scheduler.runtime.events[event_index];
        let mut reservation = event_runtime
            .store_reservation
            .lock()
            .expect("store reservation mutex poisoned");
        assert!(
            !reservation.waiters.contains(&call_ordinal),
            "store call {call_ordinal} queued twice"
        );
        reservation.waiters.push_back(call_ordinal);
        drop(reservation);
        event_runtime
            .store_reservation_revision_tx
            .send_modify(|revision| *revision = revision.saturating_add(1));
        Self {
            runtime: Arc::clone(&scheduler.runtime),
            event_index,
            call_ordinal,
            queued: true,
        }
    }

    fn try_acquire(&mut self) -> bool {
        assert!(self.queued, "dequeued store waiter polled again");
        let event_runtime = &self.runtime.events[self.event_index];
        let mut reservation = event_runtime
            .store_reservation
            .lock()
            .expect("store reservation mutex poisoned");
        if reservation.owner.is_some()
            || reservation.waiters.front().copied() != Some(self.call_ordinal)
        {
            return false;
        }
        assert_eq!(reservation.waiters.pop_front(), Some(self.call_ordinal));
        reservation.owner = Some(self.call_ordinal);
        self.queued = false;
        true
    }

    fn cancel(&mut self) {
        if !self.queued {
            return;
        }
        let event_runtime = &self.runtime.events[self.event_index];
        let mut reservation = event_runtime
            .store_reservation
            .lock()
            .expect("store reservation mutex poisoned");
        let position = reservation
            .waiters
            .iter()
            .position(|waiter| *waiter == self.call_ordinal)
            .unwrap_or_else(|| {
                panic!(
                    "queued store call {} disappeared from event {}",
                    self.call_ordinal, self.event_index
                )
            });
        reservation.waiters.remove(position);
        self.queued = false;
        drop(reservation);
        event_runtime
            .store_reservation_revision_tx
            .send_modify(|revision| *revision = revision.saturating_add(1));
    }
}

impl Drop for StoreReservationWaiter {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForegroundHold {
    pub event_id: String,
    pub window_op: u64,
    pub release_op: u64,
}

impl FaultScheduler {
    #[must_use]
    pub fn for_seed(seed: u64, profile: FaultProfile) -> Self {
        let schedule = schedule_for_seed(seed, profile);
        Self::with_salt(schedule, seed ^ 0xfab1_e5c4_ed00_0001)
    }

    #[must_use]
    pub fn from_schedule(schedule: FaultSchedule) -> Self {
        let encoded = serde_json::to_vec(&schedule).expect("FaultSchedule must serialize");
        let salt = encoded.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
        });
        Self::with_salt(schedule, salt)
    }

    fn with_salt(schedule: FaultSchedule, rng_salt: u64) -> Self {
        let process = schedule
            .events
            .iter()
            .any(|event| matches!(event.kind, FaultKind::CrashAt { .. }))
            .then(|| Arc::new(Mutex::new(ProcessController::new())));
        let events = schedule
            .events
            .iter()
            .map(|_| {
                let (store_reservation_revision_tx, _) = tokio::sync::watch::channel(0);
                EventRuntime {
                    fired: AtomicBool::new(false),
                    ended: AtomicBool::new(false),
                    matches: AtomicU64::new(0),
                    claimed_op: AtomicU64::new(u64::MAX),
                    store_reservation: Mutex::new(StoreReservationState::default()),
                    store_reservation_revision_tx,
                }
            })
            .collect();
        let (logical_op_tx, _) = tokio::sync::watch::channel(0);
        let (timeline_revision_tx, _) = tokio::sync::watch::channel(0);
        Self {
            schedule: Arc::new(schedule),
            runtime: Arc::new(SchedulerRuntime {
                logical_op: AtomicU64::new(0),
                logical_op_tx,
                quiesced: AtomicBool::new(false),
                release_held_calls: AtomicBool::new(false),
                retirement_release_held_calls: AtomicBool::new(false),
                armed_hold_event_id: Mutex::new(None),
                timeline: Mutex::new(Vec::new()),
                timeline_revision_tx,
                events,
                store_calls: AtomicU64::new(0),
                started: Instant::now(),
            }),
            rng_salt,
            process,
        }
    }

    #[must_use]
    pub fn schedule(&self) -> &FaultSchedule {
        &self.schedule
    }

    /// Return scheduled event ids whose logical windows are currently active.
    #[must_use]
    pub fn active_event_ids(&self, op_index: u64) -> Vec<String> {
        self.schedule
            .events
            .iter()
            .zip(&self.runtime.events)
            .filter(|(event, runtime)| {
                event_is_active(event, op_index) && !runtime.ended.load(Ordering::SeqCst)
            })
            .map(|(event, _)| event.id.clone())
            .collect()
    }

    pub fn advance_to(&self, op_index: u64) -> Vec<SchedulerCommand> {
        self.runtime.logical_op.store(op_index, Ordering::SeqCst);
        self.runtime.logical_op_tx.send_replace(op_index);
        if self.runtime.quiesced.load(Ordering::SeqCst) {
            return Vec::new();
        }

        let mut commands = Vec::new();
        for (index, event) in self.schedule.events.iter().enumerate() {
            let runtime = &self.runtime.events[index];
            if event.boundary == Boundary::Runner {
                match event.kind {
                    FaultKind::StartSecondNode { for_ops } => {
                        if op_index == event.start_op
                            && runtime
                                .fired
                                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                                .is_ok()
                        {
                            commands.push(SchedulerCommand::StartSecondNode {
                                event_id: event.id.clone(),
                                for_ops,
                            });
                        }
                        if op_index == event.start_op.saturating_add(for_ops)
                            && runtime
                                .ended
                                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                                .is_ok()
                        {
                            commands.push(SchedulerCommand::StopSecondNode {
                                event_id: event.id.clone(),
                            });
                        }
                    }
                    FaultKind::StartReadOnlyNode { for_ops } => {
                        if op_index == event.start_op
                            && runtime
                                .fired
                                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                                .is_ok()
                        {
                            commands.push(SchedulerCommand::StartReadOnlyNode {
                                event_id: event.id.clone(),
                                for_ops,
                            });
                        }
                        if op_index == event.start_op.saturating_add(for_ops)
                            && runtime
                                .ended
                                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                                .is_ok()
                        {
                            commands.push(SchedulerCommand::StopReadOnlyNode {
                                event_id: event.id.clone(),
                            });
                        }
                    }
                    FaultKind::PatchConfigDuringTraffic if op_index == event.start_op => {
                        if runtime
                            .fired
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            commands.push(SchedulerCommand::PatchConfigDuringTraffic {
                                event_id: event.id.clone(),
                            });
                        }
                    }
                    FaultKind::DeleteNamespaceInFlight if op_index == event.start_op => {
                        if runtime
                            .fired
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            commands.push(SchedulerCommand::DeleteNamespaceInFlight {
                                event_id: event.id.clone(),
                            });
                        }
                    }
                    FaultKind::FillDiskCache if op_index == event.start_op => {
                        if runtime
                            .fired
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            commands.push(SchedulerCommand::FillDiskCache {
                                event_id: event.id.clone(),
                            });
                        }
                    }
                    FaultKind::ResourceExhaustion {
                        max_concurrent_queries,
                        disk_cache_max_bytes,
                    } if op_index == event.start_op
                        && runtime
                            .fired
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok() =>
                    {
                        commands.push(SchedulerCommand::ResourceExhaustion {
                            event_id: event.id.clone(),
                            max_concurrent_queries,
                            disk_cache_max_bytes,
                        });
                    }
                    _ => {}
                }
                continue;
            }
            if event.boundary != Boundary::Clock {
                continue;
            }
            match event.kind {
                FaultKind::ClockJump { delta_ms } if op_index == event.start_op => {
                    if runtime
                        .fired
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        commands.push(SchedulerCommand::Clock(ClockCommand::Jump {
                            event_id: event.id.clone(),
                            delta_ms,
                        }));
                    }
                }
                FaultKind::ClockFreeze { for_ops } => {
                    if op_index == event.start_op
                        && runtime
                            .fired
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        commands.push(SchedulerCommand::Clock(ClockCommand::Freeze {
                            event_id: event.id.clone(),
                            for_ops,
                        }));
                    }
                    if op_index == event.start_op.saturating_add(for_ops)
                        && runtime
                            .ended
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        commands.push(SchedulerCommand::Clock(ClockCommand::Thaw {
                            event_id: event.id.clone(),
                        }));
                    }
                }
                _ => {}
            }
        }
        commands
    }

    /// Predicts whether one of the supplied object-store calls must
    /// participate in an active foreground hold, without consuming that
    /// scheduled event. A hold already claimed at this logical op remains
    /// visible until its exact release boundary.
    #[must_use]
    pub fn foreground_hold_release_for_calls(
        &self,
        op_index: u64,
        calls: &[(StoreOp, String)],
    ) -> Option<u64> {
        self.foreground_hold_for_calls(op_index, calls)
            .map(|hold| hold.release_op)
    }

    /// Returns the exact scheduled hold a foreground call footprint can claim.
    #[must_use]
    pub fn foreground_hold_for_calls(
        &self,
        op_index: u64,
        calls: &[(StoreOp, String)],
    ) -> Option<ForegroundHold> {
        self.foreground_hold_for_calls_excluding(op_index, calls, None)
    }

    /// Returns the first matching scheduled hold other than an optionally
    /// excluded event that is already tracked by the foreground runner.
    #[must_use]
    pub fn foreground_hold_for_calls_excluding(
        &self,
        op_index: u64,
        calls: &[(StoreOp, String)],
        excluded_event_id: Option<&str>,
    ) -> Option<ForegroundHold> {
        if self.runtime.quiesced.load(Ordering::SeqCst) {
            return None;
        }
        self.schedule
            .events
            .iter()
            .zip(&self.runtime.events)
            .find_map(|(event, runtime)| {
                if excluded_event_id == Some(event.id.as_str()) {
                    return None;
                }
                let FaultKind::HoldCall { for_ops } = event.kind else {
                    return None;
                };
                let claimed_op = runtime.claimed_op.load(Ordering::SeqCst);
                let window_op = if claimed_op == u64::MAX {
                    op_index
                } else {
                    claimed_op
                };
                let release_op = window_op.saturating_add(for_ops);
                (event.boundary == Boundary::ObjectStore
                    && event_is_active(event, op_index)
                    && (!runtime.fired.load(Ordering::SeqCst) || op_index < release_op)
                    && calls.iter().any(|(op, key)| {
                        store_target_matches(self.schedule.profile, event, *op, key)
                    }))
                .then(|| ForegroundHold {
                    event_id: event.id.clone(),
                    window_op,
                    release_op,
                })
            })
    }

    /// Restricts a scheduled `HoldCall` to one runner-managed async task.
    /// Background workers share the same store proxy but must never claim a
    /// logical-time hold that the foreground runner cannot join and release.
    pub async fn with_armed_hold<F>(&self, event_id: String, future: F) -> F::Output
    where
        F: Future,
    {
        {
            let mut armed = self
                .runtime
                .armed_hold_event_id
                .lock()
                .expect("armed hold mutex poisoned");
            assert!(
                armed.is_none(),
                "attempted to arm hold {event_id} while {armed:?} was still armed"
            );
            *armed = Some(event_id.clone());
        }

        let result = ARMED_HOLD_EVENT_ID.scope(event_id.clone(), future).await;
        let mut armed = self
            .runtime
            .armed_hold_event_id
            .lock()
            .expect("armed hold mutex poisoned");
        if armed.as_deref() == Some(event_id.as_str()) {
            *armed = None;
        }
        result
    }

    #[must_use]
    pub fn store_decision(&self, op: StoreOp, key: &str) -> Option<StoreFaultAction> {
        if self.runtime.quiesced.load(Ordering::SeqCst) {
            return None;
        }
        let current = self.runtime.logical_op.load(Ordering::SeqCst);
        let call_ordinal = self.runtime.store_calls.fetch_add(1, Ordering::SeqCst) + 1;
        for (index, event) in self.schedule.events.iter().enumerate() {
            if !matches!(event.boundary, Boundary::ObjectStore | Boundary::Process)
                || !event_is_active(event, current)
                || !store_target_matches(self.schedule.profile, event, op, key)
                || !partition_matches(&event.kind, op)
                || !self.claim(index, event, current)
            {
                continue;
            }
            return Some(self.store_action(index, event, current, call_ordinal, false, false));
        }
        None
    }

    /// Selects a GET fault while deferring faults that require a successful
    /// body-bearing response until the inner store has answered.
    ///
    /// A one-shot deferred event, and any nth-selected windowed crash, is
    /// reserved, not fired. Calls contending for that same event wait here so
    /// they cannot skip ahead to a later schedule entry. Calls that do not
    /// match the event remain fully concurrent.
    #[must_use]
    pub async fn get_decision(&self, key: &str) -> Option<StoreFaultAction> {
        if self.runtime.quiesced.load(Ordering::SeqCst) {
            return None;
        }
        let current = self.runtime.logical_op.load(Ordering::SeqCst);
        let call_ordinal = self.runtime.store_calls.fetch_add(1, Ordering::SeqCst) + 1;

        'events: for (index, event) in self.schedule.events.iter().enumerate() {
            if !matches!(event.boundary, Boundary::ObjectStore | Boundary::Process)
                || !event_is_active(event, current)
                || !store_target_matches(self.schedule.profile, event, StoreOp::Get, key)
                || !partition_matches(&event.kind, StoreOp::Get)
            {
                continue;
            }

            if !deferred_get_fault(&event.kind) {
                if !self.claim(index, event, current) {
                    continue;
                }
                return Some(self.store_action(index, event, current, call_ordinal, false, false));
            }

            let runtime = &self.runtime.events[index];
            let requires_reservation = event.end_op.is_none()
                || matches!(
                    event.kind,
                    FaultKind::CrashAt {
                        position: TriggerPosition::Post,
                        ..
                    }
                );
            if !requires_reservation {
                return Some(self.store_action(index, event, current, call_ordinal, true, false));
            }

            let mut revision = runtime.store_reservation_revision_tx.subscribe();
            let mut waiter = StoreReservationWaiter::enqueue(self, index, call_ordinal);
            loop {
                if self.runtime.quiesced.load(Ordering::SeqCst) {
                    waiter.cancel();
                    return None;
                }
                if runtime.fired.load(Ordering::SeqCst) {
                    waiter.cancel();
                    continue 'events;
                }
                if waiter.try_acquire() {
                    // Quiescence can race the FIFO acquisition after the loop's
                    // first check. A queued call must still bypass the event.
                    if self.runtime.quiesced.load(Ordering::SeqCst) {
                        self.release_store_reservation(index, call_ordinal);
                        return None;
                    }
                    // A committer may have fired the event between the first
                    // load and this FIFO acquisition. Release that stale
                    // reservation before considering later events.
                    if runtime.fired.load(Ordering::SeqCst) {
                        self.release_store_reservation(index, call_ordinal);
                        continue 'events;
                    }
                    return Some(self.store_action(
                        index,
                        event,
                        current,
                        call_ordinal,
                        true,
                        true,
                    ));
                }
                revision
                    .changed()
                    .await
                    .expect("store reservation revision sender dropped");
            }
        }
        None
    }

    /// Commits a deferred GET fault after its response mutation is ready.
    /// Returns false only when a post-GET crash has not yet reached its exact
    /// body-bearing-success ordinal.
    #[must_use]
    pub fn commit_deferred_get(&self, action: &StoreFaultAction) -> bool {
        assert!(action.deferred_get, "committed an immediate store fault");
        let runtime = &self.runtime.events[action.event_index];
        let should_apply = if let FaultKind::CrashAt {
            point,
            position: TriggerPosition::Post,
        } = action.kind
        {
            let (_, _, nth) = point.selector();
            let successful_body_ordinal = runtime.matches.fetch_add(1, Ordering::SeqCst) + 1;
            successful_body_ordinal == u64::from(nth)
        } else {
            runtime.matches.fetch_add(1, Ordering::SeqCst);
            true
        };

        if !should_apply {
            if action.reserved_get {
                self.release_store_reservation(action.event_index, action.call_ordinal);
            }
            return false;
        }
        if action.window {
            if action.reserved_get {
                self.release_store_reservation(action.event_index, action.call_ordinal);
            }
            return true;
        }

        assert!(
            action.reserved_get,
            "one-shot deferred GET fault was not reserved"
        );
        runtime
            .fired
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .unwrap_or_else(|_| panic!("deferred event {} fired twice", action.event_id));
        self.release_store_reservation(action.event_index, action.call_ordinal);
        true
    }

    /// Releases a deferred GET reservation when the inner store returned no
    /// body (including NotModified, Precondition, NotFound, or transport error).
    pub fn cancel_deferred_get(&self, action: &StoreFaultAction) {
        assert!(action.deferred_get, "cancelled an immediate store fault");
        if action.reserved_get {
            self.release_store_reservation(action.event_index, action.call_ordinal);
        }
    }

    fn store_action(
        &self,
        index: usize,
        event: &FaultEvent,
        current: u64,
        call_ordinal: u64,
        deferred_get: bool,
        reserved_get: bool,
    ) -> StoreFaultAction {
        let latency_ms = match event.kind {
            FaultKind::Latency { base_ms, jitter_ms } => {
                let jitter = if jitter_ms == 0 {
                    0
                } else {
                    StdRng::seed_from_u64(self.rng_salt ^ call_ordinal).gen_range(0..=jitter_ms)
                };
                Some(base_ms + jitter)
            }
            _ => None,
        };
        StoreFaultAction {
            event_id: event.id.clone(),
            op_index: current,
            kind: event.kind.clone(),
            call_ordinal,
            latency_ms,
            window: event.end_op.is_some(),
            event_index: index,
            deferred_get,
            reserved_get,
        }
    }

    fn release_store_reservation(&self, event_index: usize, call_ordinal: u64) {
        let runtime = &self.runtime.events[event_index];
        let mut reservation = runtime
            .store_reservation
            .lock()
            .expect("store reservation mutex poisoned");
        assert_eq!(
            reservation.owner,
            Some(call_ordinal),
            "store reservation owner changed"
        );
        reservation.owner = None;
        drop(reservation);
        runtime
            .store_reservation_revision_tx
            .send_modify(|revision| *revision = revision.saturating_add(1));
    }

    #[must_use]
    pub fn http_decision(&self, method: &Method, path: &str) -> Option<HttpFaultAction> {
        if self.runtime.quiesced.load(Ordering::SeqCst) {
            return None;
        }
        let current = self.runtime.logical_op.load(Ordering::SeqCst);
        for (index, event) in self.schedule.events.iter().enumerate() {
            if event.boundary != Boundary::ClientHttp
                || !event_is_active(event, current)
                || !http_target_matches(event, method, path)
                || !self.claim(index, event, current)
            {
                continue;
            }
            return Some(HttpFaultAction {
                event_id: event.id.clone(),
                op_index: current,
                kind: event.kind.clone(),
                window: event.end_op.is_some(),
            });
        }
        None
    }

    fn claim(&self, index: usize, event: &FaultEvent, current: u64) -> bool {
        let runtime = &self.runtime.events[index];
        let match_ordinal = runtime.matches.fetch_add(1, Ordering::SeqCst) + 1;
        if let FaultKind::CrashAt { point, .. } = event.kind {
            let (_, _, nth) = point.selector();
            if match_ordinal != u64::from(nth) {
                return false;
            }
        }
        if matches!(event.kind, FaultKind::HoldCall { .. }) {
            let task_armed = ARMED_HOLD_EVENT_ID
                .try_with(|event_id| event_id == &event.id)
                .unwrap_or(false);
            let request_armed = self
                .runtime
                .armed_hold_event_id
                .lock()
                .expect("armed hold mutex poisoned")
                .as_deref()
                == Some(event.id.as_str());
            if !task_armed
                && (!request_armed || crate::common::server::background_compaction_origin_active())
            {
                return false;
            }
            let claimed = runtime
                .claimed_op
                .compare_exchange(u64::MAX, current, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
            if claimed {
                let mut armed = self
                    .runtime
                    .armed_hold_event_id
                    .lock()
                    .expect("armed hold mutex poisoned");
                if armed.as_deref() == Some(event.id.as_str()) {
                    *armed = None;
                }
                runtime.fired.store(true, Ordering::SeqCst);
            }
            return claimed;
        }
        event.end_op.is_some()
            || runtime
                .fired
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }

    pub fn quiesce(&self) {
        self.runtime.quiesced.store(true, Ordering::SeqCst);
        let current = self.runtime.logical_op.load(Ordering::SeqCst);
        self.runtime.logical_op_tx.send_replace(current);
        self.wake_store_reservation_waiters();
    }

    pub fn begin_quiet_period(&self, event: TimelineEvent) {
        let mut timeline = self
            .runtime
            .timeline
            .lock()
            .expect("fault timeline mutex poisoned");
        self.runtime.quiesced.store(true, Ordering::SeqCst);
        timeline.push(event);
        let current = self.runtime.logical_op.load(Ordering::SeqCst);
        self.runtime.logical_op_tx.send_replace(current);
        drop(timeline);
        self.wake_store_reservation_waiters();
        self.runtime
            .timeline_revision_tx
            .send_modify(|revision| *revision = revision.saturating_add(1));
    }

    fn wake_store_reservation_waiters(&self) {
        for runtime in &self.runtime.events {
            runtime
                .store_reservation_revision_tx
                .send_modify(|revision| *revision = revision.saturating_add(1));
        }
    }

    pub fn release_held_calls(&self) {
        self.runtime
            .release_held_calls
            .store(true, Ordering::SeqCst);
        let current = self.runtime.logical_op.load(Ordering::SeqCst);
        self.runtime.logical_op_tx.send_replace(current);
    }

    /// Temporarily release held calls owned by the server generation being retired.
    ///
    /// # Panics
    ///
    /// Panics when two server retirements overlap. The runner owns exactly one
    /// primary generation and must serialize crash recovery.
    #[must_use]
    pub fn begin_held_call_retirement(&self) -> HeldCallRetirementGuard {
        self.runtime
            .retirement_release_held_calls
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .unwrap_or_else(|_| panic!("overlapping held-call server retirement epochs"));
        let current = self.runtime.logical_op.load(Ordering::SeqCst);
        self.runtime.logical_op_tx.send_replace(current);
        HeldCallRetirementGuard {
            runtime: Arc::clone(&self.runtime),
        }
    }

    pub async fn wait_for_hold_release(&self, action: &StoreFaultAction) {
        let FaultKind::HoldCall { for_ops } = action.kind else {
            panic!("wait_for_hold_release requires HoldCall");
        };
        let release_op = action.op_index.saturating_add(for_ops);
        let mut logical_op = self.runtime.logical_op_tx.subscribe();
        loop {
            if self.runtime.release_held_calls.load(Ordering::SeqCst)
                || self
                    .runtime
                    .retirement_release_held_calls
                    .load(Ordering::SeqCst)
                || *logical_op.borrow() >= release_op
            {
                return;
            }
            logical_op
                .changed()
                .await
                .expect("fault scheduler logical-op sender dropped");
        }
    }

    #[must_use]
    pub fn process_controller(&self) -> Option<ProcessController> {
        self.process.as_ref().map(|controller| {
            controller
                .lock()
                .expect("process controller mutex poisoned")
                .clone()
        })
    }

    pub fn reset_process_controller(&self) {
        let Some(controller) = &self.process else {
            return;
        };
        *controller
            .lock()
            .expect("process controller mutex poisoned") = ProcessController::new();
    }

    pub fn record(&self, event: TimelineEvent) {
        self.runtime
            .timeline
            .lock()
            .expect("fault timeline mutex poisoned")
            .push(event);
        self.runtime
            .timeline_revision_tx
            .send_modify(|revision| *revision = revision.saturating_add(1));
    }

    pub async fn wait_for_hold_window_active(
        &self,
        event_id: &str,
        op_index: u64,
    ) -> TimelineEvent {
        assert!(
            self.schedule.events.iter().any(|event| {
                event.id == event_id
                    && event.boundary == Boundary::ObjectStore
                    && matches!(event.kind, FaultKind::HoldCall { .. })
            }),
            "hold-window waiter requires a scheduled object-store HoldCall event: {event_id}"
        );

        let mut timeline_revision = self.runtime.timeline_revision_tx.subscribe();
        loop {
            if let Some(event) = self
                .runtime
                .timeline
                .lock()
                .expect("fault timeline mutex poisoned")
                .iter()
                .find(|event| {
                    event.event_id == event_id
                        && event.op_index == op_index
                        && event.boundary == Boundary::ObjectStore
                        && event.semantics == FaultSemantics::WindowActive
                })
                .cloned()
            {
                return event;
            }
            timeline_revision
                .changed()
                .await
                .expect("fault scheduler timeline sender dropped");
        }
    }

    #[must_use]
    pub fn timeline(&self) -> Vec<TimelineEvent> {
        self.runtime
            .timeline
            .lock()
            .expect("fault timeline mutex poisoned")
            .clone()
    }

    #[must_use]
    pub fn wall_ms(&self) -> u64 {
        self.runtime.started.elapsed().as_millis() as u64
    }

    #[must_use]
    pub fn fault_window_active(&self, op_index: u64, namespace: &str) -> bool {
        if self.runtime.quiesced.load(Ordering::SeqCst) {
            return false;
        }

        if self.schedule.profile == FaultProfile::LateStream {
            return namespace.ends_with("-late")
                && self.schedule.events.iter().any(|event| {
                    event.boundary == Boundary::ObjectStore
                        && event.target.store_op == Some(StoreOp::Get)
                        && event_is_active(event, op_index)
                });
        }

        if !matches!(
            self.schedule.profile,
            FaultProfile::Content
                | FaultProfile::Semantic
                | FaultProfile::ProviderContractAbuse
                | FaultProfile::SupportedFull
                | FaultProfile::Full
        ) {
            return false;
        }

        let manifest_key = format!("{namespace}/manifest.json");
        let prospective_one_shot =
            self.schedule
                .events
                .iter()
                .zip(&self.runtime.events)
                .any(|(event, runtime)| {
                    event.boundary == Boundary::ObjectStore
                        && event_is_active(event, op_index)
                        && matches!(event.kind, FaultKind::StaleRead)
                        && !runtime.fired.load(Ordering::SeqCst)
                        && store_target_matches(
                            self.schedule.profile,
                            event,
                            StoreOp::Get,
                            &manifest_key,
                        )
                        && partition_matches(&event.kind, StoreOp::Get)
                });
        if prospective_one_shot {
            return true;
        }

        let scheduled_window = self.schedule.events.iter().any(|event| {
            event.boundary == Boundary::ObjectStore
                && event.end_op.is_some_and(|end| {
                    op_index >= event.start_op
                        && op_index <= end.saturating_add(FAULT_WINDOW_TRAILING_OPS)
                })
        });
        if scheduled_window {
            return true;
        }

        self.timeline().into_iter().any(|event| {
            event.boundary == Boundary::ObjectStore
                && !event.action.starts_with("StaleRead")
                && op_index >= event.op_index
                && op_index <= event.op_index.saturating_add(FAULT_WINDOW_TRAILING_OPS)
                && event
                    .key
                    .as_deref()
                    .is_some_and(|key| key_is_in_namespace(key, namespace))
        })
    }

    /// Reports whether a bounded object-store fault window is scheduled to be
    /// active at `op_index`.
    ///
    /// Windows open and close on the logical op index, which freezes while an
    /// operation waits in-op for storage convergence. A convergence wait that
    /// starts inside such a window can therefore never observe the window
    /// closing, so the runner must defer convergence-waiting operations until
    /// the window has passed.
    pub fn scheduled_store_fault_window_active(&self, op_index: u64) -> bool {
        if self.runtime.quiesced.load(Ordering::SeqCst) {
            return false;
        }
        self.schedule.events.iter().any(|event| {
            event.boundary == Boundary::ObjectStore
                && event.end_op.is_some()
                && event_is_active(event, op_index)
        })
    }

    /// Reports whether every object-store read is deliberately partitioned at
    /// `op_index`. Operational read bursts use this narrower proof to accept
    /// the canonical retryable storage error without treating unrelated 500s
    /// as expected.
    pub fn global_read_partition_active(&self, op_index: u64) -> bool {
        if self.runtime.quiesced.load(Ordering::SeqCst) {
            return false;
        }
        self.schedule.events.iter().any(|event| {
            event.boundary == Boundary::ObjectStore
                && event_is_active(event, op_index)
                && event.target.store_op.is_none()
                && event.target.key_substring.is_none()
                && matches!(
                    event.kind,
                    FaultKind::Partition {
                        direction: Direction::All | Direction::ReadsFail,
                    }
                )
        })
    }
}

fn key_is_in_namespace(key: &str, namespace: &str) -> bool {
    key == namespace
        || key
            .strip_prefix(namespace)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || key.split("->").any(|part| {
            part == namespace
                || part
                    .strip_prefix(namespace)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
}

impl FaultSchedule {
    #[must_use]
    pub fn dropped_response_selftest() -> Self {
        Self {
            profile: FaultProfile::Network,
            events: vec![FaultEvent {
                id: "network-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ClientHttp,
                target: TargetSelector {
                    path_substring: Some("/vectors".to_string()),
                    methods: Some(vec!["POST".to_string()]),
                    ..TargetSelector::default()
                },
                kind: FaultKind::DropResponse,
            }],
        }
    }

    #[must_use]
    pub fn crash_lost_ack_selftest() -> Self {
        let point = CrashPoint::WalFragmentPut;
        let (store_op, key_substring, _) = point.selector();
        Self {
            profile: FaultProfile::Crash,
            events: vec![FaultEvent {
                id: "crash-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::Process,
                target: TargetSelector {
                    store_op: Some(store_op),
                    key_substring: Some(key_substring.to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::CrashAt {
                    point,
                    position: TriggerPosition::Post,
                },
            }],
        }
    }

    #[must_use]
    pub fn clock_gc_eats_live_selftest() -> Self {
        Self {
            profile: FaultProfile::Clock,
            events: vec![FaultEvent {
                id: "clock-00".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::Clock,
                target: TargetSelector::default(),
                kind: FaultKind::ClockJump { delta_ms: 120_000 },
            }],
        }
    }

    #[must_use]
    pub fn swallow_corruption_selftest() -> Self {
        Self {
            profile: FaultProfile::Content,
            events: vec![FaultEvent {
                id: "content-swallow-corruption".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("cluster_".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::Content(ContentFault::BitFlip { offset_hint: 3 }),
            }],
        }
    }

    #[must_use]
    pub fn misdirected_write_reachability_selftest() -> Self {
        Self {
            profile: FaultProfile::Content,
            events: vec![FaultEvent {
                id: "content-misdirected-write".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some("segments/".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::Content(ContentFault::MisdirectedWrite),
            }],
        }
    }

    /// Pins the dual-writer fencing self-test to one deterministic interleaving.
    ///
    /// Node B starts before traffic at logical op 0 and remains active for 20
    /// ops. During that same window, the first lease PUT observed from either
    /// node is held for eight logical ops. The runner's mutation may admit one
    /// stale-token result, but the S3 lineage oracle must still report I21.
    #[must_use]
    pub fn dual_writer_fencing_selftest() -> Self {
        Self {
            profile: FaultProfile::Ops,
            events: vec![
                FaultEvent {
                    id: "ops-dual-writer-second-node".to_string(),
                    start_op: 0,
                    end_op: Some(20),
                    boundary: Boundary::Runner,
                    target: TargetSelector::default(),
                    kind: FaultKind::StartSecondNode { for_ops: 20 },
                },
                FaultEvent {
                    id: "ops-dual-writer-lease-hold".to_string(),
                    start_op: 0,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector {
                        store_op: Some(StoreOp::Put),
                        key_substring: Some("lease.json".to_string()),
                        ..TargetSelector::default()
                    },
                    kind: FaultKind::HoldCall { for_ops: 8 },
                },
            ],
        }
    }

    /// Builds the isolated proxy schedule used to admit one stale manifest CAS.
    ///
    /// This mutation is deliberately absent from seeded Ops schedules. The
    /// store proxy additionally requires its dedicated self-test constructor,
    /// so loading this event through a normal campaign fails loudly.
    #[must_use]
    pub fn stale_manifest_cas_selftest() -> Self {
        Self {
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
        }
    }
}

fn event_is_active(event: &FaultEvent, current: u64) -> bool {
    current >= event.start_op && event.end_op.is_none_or(|end| current < end)
}

fn store_target_matches(profile: FaultProfile, event: &FaultEvent, op: StoreOp, key: &str) -> bool {
    (profile != FaultProfile::LateStream || key.contains("/late/segments/"))
        && event.target.store_op.is_none_or(|expected| expected == op)
        && event
            .target
            .key_substring
            .as_deref()
            .is_none_or(|needle| key.contains(needle))
}

fn deferred_get_fault(kind: &FaultKind) -> bool {
    matches!(
        kind,
        FaultKind::StaleRead
            | FaultKind::Content(
                ContentFault::TruncateBody { .. }
                    | ContentFault::BitFlip { .. }
                    | ContentFault::WrongObject
            )
            | FaultKind::TruncatedGetStream { .. }
            | FaultKind::CrashAt {
                position: TriggerPosition::Post,
                ..
            }
    )
}

fn partition_matches(kind: &FaultKind, op: StoreOp) -> bool {
    let FaultKind::Partition { direction } = kind else {
        return true;
    };
    match direction {
        Direction::All => true,
        Direction::ReadsFail => matches!(op, StoreOp::Get | StoreOp::Head | StoreOp::List),
        Direction::WritesFail => matches!(op, StoreOp::Put | StoreOp::Delete | StoreOp::Copy),
    }
}

fn http_target_matches(event: &FaultEvent, method: &Method, path: &str) -> bool {
    event
        .target
        .path_substring
        .as_deref()
        .is_none_or(|needle| path.contains(needle))
        && event.target.methods.as_ref().is_none_or(|methods| {
            methods
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(method.as_str()))
        })
}

fn schedule_for_seed(seed: u64, profile: FaultProfile) -> FaultSchedule {
    let mut rng = StdRng::seed_from_u64(seed ^ 0x5c4e_d01e_fa17_2026);
    let mut events = Vec::new();
    match profile {
        FaultProfile::LegacyChaos | FaultProfile::Branching | FaultProfile::Late => {}
        FaultProfile::LateStream => {
            // Phase-1 seed 0 persisted late truth artifacts as
            // `late/segments/<segment>/matrix_0.bin` and
            // `late/segments/<segment>/attrs_0.bin`. Keep these selectors
            // coupled to logical operation windows so replay never depends on
            // wall-clock timing.
            let count: usize = rng.gen_range(3..=5);
            let lane_width = (LATE_STREAM_FAULT_LIMIT - LATE_STREAM_FIRST_FAULT_OP)
                / u64::try_from(count).expect("late-stream event count must fit u64");
            let kind_rotation: usize = rng.gen_range(0..3);
            let swap_truth_selectors = rng.gen_bool(0.5);

            for index in 0..count {
                let lane_start = LATE_STREAM_FIRST_FAULT_OP
                    + u64::try_from(index).expect("late-stream event index must fit u64")
                        * lane_width;
                let lane_end = if index + 1 == count {
                    LATE_STREAM_FAULT_LIMIT
                } else {
                    lane_start + lane_width
                };
                // The exclusive one-op end leaves at least one clean logical
                // slot before the next event for the lifecycle follow-up.
                let start_op = rng.gen_range(lane_start..=lane_end - 2);
                let (kind, key_substring) = match (kind_rotation + index) % 3 {
                    0 => (
                        FaultKind::PreFail {
                            error: if rng.gen_bool(0.5) {
                                InjectedErrorKind::Http500
                            } else {
                                InjectedErrorKind::Http503
                            },
                        },
                        if swap_truth_selectors {
                            "/attrs_"
                        } else {
                            "/matrix_"
                        },
                    ),
                    1 => (
                        FaultKind::Latency {
                            base_ms: rng.gen_range(5..=15),
                            jitter_ms: rng.gen_range(10..=40),
                        },
                        "late/segments/",
                    ),
                    2 => (
                        FaultKind::TruncatedGetStream {
                            after_bytes: rng.gen_range(1..=32),
                        },
                        if swap_truth_selectors {
                            "/matrix_"
                        } else {
                            "/attrs_"
                        },
                    ),
                    _ => unreachable!("late-stream kind rotation has three entries"),
                };
                push_event(
                    &mut events,
                    profile,
                    start_op,
                    Some(start_op + 1),
                    Boundary::ObjectStore,
                    TargetSelector {
                        store_op: Some(StoreOp::Get),
                        key_substring: Some(key_substring.to_string()),
                        ..TargetSelector::default()
                    },
                    kind,
                );
            }
        }
        FaultProfile::PostCommit => {
            let selectors = [
                (StoreOp::Put, ".wal"),
                (StoreOp::Put, "manifest.json"),
                (StoreOp::Copy, "segments/"),
                (StoreOp::Delete, "segments/"),
            ];
            for _ in 0..rng.gen_range(2..=4) {
                let (store_op, key_substring) = selectors[rng.gen_range(0..selectors.len())];
                let error = if rng.gen_bool(0.5) {
                    InjectedErrorKind::Http500
                } else {
                    InjectedErrorKind::Http503
                };
                push_event(
                    &mut events,
                    profile,
                    rng.gen_range(5..480),
                    None,
                    Boundary::ObjectStore,
                    TargetSelector {
                        store_op: Some(store_op),
                        key_substring: Some(key_substring.to_string()),
                        ..TargetSelector::default()
                    },
                    FaultKind::PostCommitFail { error },
                );
            }
        }
        FaultProfile::Network => {
            for _ in 0..rng.gen_range(1..=2) {
                let start = rng.gen_range(5..450);
                let len = rng.gen_range(10..=30);
                let direction = match rng.gen_range(0..4) {
                    0 | 1 => Direction::All,
                    2 => Direction::ReadsFail,
                    _ => Direction::WritesFail,
                };
                push_event(
                    &mut events,
                    profile,
                    start,
                    Some((start + len).min(500)),
                    Boundary::ObjectStore,
                    TargetSelector::default(),
                    FaultKind::Partition { direction },
                );
            }

            let start = rng.gen_range(5..440);
            let len = rng.gen_range(20..=50);
            push_event(
                &mut events,
                profile,
                start,
                Some((start + len).min(500)),
                Boundary::ObjectStore,
                TargetSelector::default(),
                FaultKind::Latency {
                    base_ms: rng.gen_range(20..=60),
                    jitter_ms: rng.gen_range(0..=40),
                },
            );

            let store_selectors = [
                (StoreOp::Put, ".wal"),
                (StoreOp::Put, "manifest.json"),
                (StoreOp::Get, "segments/"),
                (StoreOp::Delete, "segments/"),
            ];
            let errors = [
                InjectedErrorKind::Throttle429,
                InjectedErrorKind::Http500,
                InjectedErrorKind::Http503,
            ];
            for _ in 0..rng.gen_range(2..=4) {
                let (store_op, key_substring) =
                    store_selectors[rng.gen_range(0..store_selectors.len())];
                push_event(
                    &mut events,
                    profile,
                    rng.gen_range(5..480),
                    None,
                    Boundary::ObjectStore,
                    TargetSelector {
                        store_op: Some(store_op),
                        key_substring: Some(key_substring.to_string()),
                        ..TargetSelector::default()
                    },
                    FaultKind::PreFail {
                        error: errors[rng.gen_range(0..errors.len())],
                    },
                );
            }

            let http_targets = [
                ("/vectors", "POST"),
                ("/vectors", "DELETE"),
                ("/snapshots/", "PUT"),
                ("/clone", "POST"),
                ("/v1/namespaces", "POST"),
            ];
            for _ in 0..rng.gen_range(2..=5) {
                let kind = match rng.gen_range(0..6) {
                    0 => FaultKind::DropRequest,
                    1 => FaultKind::DropResponse,
                    2 => FaultKind::TruncateResponse {
                        at_bytes: rng.gen_range(1..=32),
                    },
                    3 => FaultKind::ResetAfterRequest,
                    4 => FaultKind::ClientCancel {
                        after_ms: rng.gen_range(1..=20),
                    },
                    _ => FaultKind::DuplicateRetry,
                };
                let eligible_targets = if matches!(kind, FaultKind::DuplicateRetry) {
                    &http_targets[..2]
                } else {
                    &http_targets[..]
                };
                let (path, method) = eligible_targets[rng.gen_range(0..eligible_targets.len())];
                push_event(
                    &mut events,
                    profile,
                    rng.gen_range(5..480),
                    None,
                    Boundary::ClientHttp,
                    TargetSelector {
                        path_substring: Some(path.to_string()),
                        methods: Some(vec![method.to_string()]),
                        ..TargetSelector::default()
                    },
                    kind,
                );
            }

            if rng.gen_bool(0.5) {
                push_event(
                    &mut events,
                    profile,
                    rng.gen_range(5..480),
                    None,
                    Boundary::ObjectStore,
                    TargetSelector {
                        store_op: Some(StoreOp::Get),
                        key_substring: Some("segments/".to_string()),
                        ..TargetSelector::default()
                    },
                    FaultKind::TruncatedGetStream {
                        after_bytes: rng.gen_range(1..=256),
                    },
                );
            }
        }
        FaultProfile::Crash => {
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
            let count = rng.gen_range(1..=3);
            for index in 0..count {
                let point = points[rng.gen_range(0..points.len())];
                let (store_op, key_substring, _) = point.selector();
                let start_op = 20 + (index as u64 * 150) + rng.gen_range(0..=30);
                let position = if rng.gen_bool(0.5) {
                    TriggerPosition::Pre
                } else {
                    TriggerPosition::Post
                };
                push_event(
                    &mut events,
                    profile,
                    start_op,
                    None,
                    Boundary::Process,
                    TargetSelector {
                        store_op: Some(store_op),
                        key_substring: Some(key_substring.to_string()),
                        ..TargetSelector::default()
                    },
                    FaultKind::CrashAt { point, position },
                );
            }
        }
        FaultProfile::Clock => {
            push_event(
                &mut events,
                profile,
                rng.gen_range(25..=75),
                None,
                Boundary::Clock,
                TargetSelector::default(),
                FaultKind::ClockJump {
                    delta_ms: rng.gen_range(1..=120) * 1_000,
                },
            );
            push_event(
                &mut events,
                profile,
                rng.gen_range(100..=150),
                None,
                Boundary::Clock,
                TargetSelector::default(),
                FaultKind::ClockJump {
                    delta_ms: -rng.gen_range(1..=30) * 1_000,
                },
            );
            let freeze_start = rng.gen_range(175..=240);
            let for_ops = rng.gen_range(5..=20);
            push_event(
                &mut events,
                profile,
                freeze_start,
                Some(freeze_start + for_ops),
                Boundary::Clock,
                TargetSelector::default(),
                FaultKind::ClockFreeze { for_ops },
            );
            let (start_op, delta_ms) = if seed % 2 == 0 {
                (rng.gen_range(300..=360), rng.gen_range(15..=120) * 1_000)
            } else {
                (rng.gen_range(470..=485), rng.gen_range(31..=120) * 1_000)
            };
            push_event(
                &mut events,
                profile,
                start_op,
                None,
                Boundary::Clock,
                TargetSelector::default(),
                FaultKind::ClockJump { delta_ms },
            );
        }
        FaultProfile::SupportedFull => {
            let sources = [
                FaultProfile::PostCommit,
                FaultProfile::Network,
                FaultProfile::Crash,
                FaultProfile::Clock,
                FaultProfile::Semantic,
                FaultProfile::Sched,
                FaultProfile::Ops,
            ];
            for (index, source) in sources.into_iter().enumerate() {
                let source_seed = seed
                    ^ (u64::try_from(index + 1)
                        .expect("supported-full source index must fit u64")
                        .wrapping_mul(0x9e37_79b9_7f4a_7c15));
                let mut event =
                    first_event_for_class(source_seed, source, ContractClass::SupportedV1);
                event.id = format!("supported-full-{}-{index:02}", source.id_prefix());
                events.push(event);
            }
            let start = rng.gen_range(40..=120);
            push_event(
                &mut events,
                profile,
                start,
                Some(start + 20),
                Boundary::Runner,
                TargetSelector::default(),
                FaultKind::StartReadOnlyNode { for_ops: 20 },
            );
        }
        FaultProfile::Security => {
            push_event(
                &mut events,
                profile,
                SECURITY_AUDIT_BARRIER_OP,
                None,
                Boundary::ObjectStore,
                TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("_security/heads/policy.json".to_string()),
                    ..TargetSelector::default()
                },
                FaultKind::PreFail {
                    error: InjectedErrorKind::Http503,
                },
            );
            push_event(
                &mut events,
                profile,
                24,
                None,
                Boundary::ClientHttp,
                TargetSelector {
                    path_substring: Some("/v1/security".to_string()),
                    methods: Some(vec!["POST".to_string(), "DELETE".to_string()]),
                    ..TargetSelector::default()
                },
                FaultKind::DropResponse,
            );
            push_event(
                &mut events,
                profile,
                20,
                None,
                Boundary::Process,
                TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some("_security/heads/policy.json".to_string()),
                    ..TargetSelector::default()
                },
                FaultKind::CrashAt {
                    point: CrashPoint::ManifestCas,
                    position: TriggerPosition::Pre,
                },
            );
            push_event(
                &mut events,
                profile,
                32,
                None,
                Boundary::Clock,
                TargetSelector::default(),
                FaultKind::ClockJump { delta_ms: 120_000 },
            );
        }
        FaultProfile::ProviderContractAbuse => {
            for (index, source) in [FaultProfile::Content, FaultProfile::Semantic]
                .into_iter()
                .enumerate()
            {
                let source_seed = seed
                    ^ (u64::try_from(index + 1)
                        .expect("provider-abuse source index must fit u64")
                        .wrapping_mul(0xd6e8_feb8_6659_fd93));
                let mut event = first_event_for_class(
                    source_seed,
                    source,
                    ContractClass::ProviderContractAbuse,
                );
                event.id = format!("provider-contract-abuse-{}-{index:02}", source.id_prefix());
                events.push(event);
            }
        }
        FaultProfile::FutureArchitecture => {
            let mut event =
                first_event_for_class(seed, FaultProfile::Ops, ContractClass::FutureArchitecture);
            event.id = "future-architecture-dual-writer-00".to_string();
            events.push(event);
        }
        FaultProfile::Content => {
            let put_targets = ["manifest.json", ".wal", "segments/"];
            let put_target = put_targets[rng.gen_range(0..put_targets.len())];
            let durable = if rng.gen_bool(0.5) {
                FaultKind::Content(ContentFault::TornWrite {
                    keep_bytes: rng.gen_range(1..=64),
                })
            } else {
                FaultKind::Content(ContentFault::MisdirectedWrite)
            };
            push_event(
                &mut events,
                profile,
                rng.gen_range(10..=320),
                None,
                Boundary::ObjectStore,
                TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some(put_target.to_string()),
                    ..TargetSelector::default()
                },
                durable,
            );

            let get_targets = [
                "manifest.json",
                ".wal",
                "segments/",
                "centroids",
                "bootstrap.bin",
                "coarse_sketch.bin",
                "cluster_",
            ];
            for _ in 0..rng.gen_range(1..=3) {
                let target = get_targets[rng.gen_range(0..get_targets.len())];
                let kind = match rng.gen_range(0..3) {
                    0 => FaultKind::Content(ContentFault::TruncateBody {
                        keep_bytes: rng.gen_range(1..=128),
                    }),
                    1 => FaultKind::Content(ContentFault::BitFlip {
                        offset_hint: rng.gen(),
                    }),
                    _ => FaultKind::Content(ContentFault::WrongObject),
                };
                push_event(
                    &mut events,
                    profile,
                    rng.gen_range(10..=450),
                    None,
                    Boundary::ObjectStore,
                    TargetSelector {
                        store_op: Some(StoreOp::Get),
                        key_substring: Some(target.to_string()),
                        ..TargetSelector::default()
                    },
                    kind,
                );
            }

            if rng.gen_bool(0.5) {
                let delete_targets = [".wal", "segments/"];
                let target = delete_targets[rng.gen_range(0..delete_targets.len())];
                push_event(
                    &mut events,
                    profile,
                    rng.gen_range(250..=480),
                    None,
                    Boundary::ObjectStore,
                    TargetSelector {
                        store_op: Some(StoreOp::Delete),
                        key_substring: Some(target.to_string()),
                        ..TargetSelector::default()
                    },
                    FaultKind::Content(ContentFault::SilentDeleteFailure),
                );
            }
        }
        FaultProfile::Semantic => {
            for _ in 0..rng.gen_range(2..=5) {
                let (store_op, key_substring, end_op, kind) = match rng.gen_range(0..8) {
                    0 => (
                        StoreOp::Put,
                        Some("manifest.json"),
                        None,
                        FaultKind::CasConflict,
                    ),
                    1 => (
                        StoreOp::List,
                        None,
                        None,
                        FaultKind::ListOmit {
                            nth: rng.gen_range(1..=3),
                        },
                    ),
                    2 => (
                        StoreOp::List,
                        None,
                        None,
                        FaultKind::ListDuplicate {
                            nth: rng.gen_range(1..=3),
                        },
                    ),
                    3 => (StoreOp::List, None, None, FaultKind::ListReorder),
                    4 => (
                        StoreOp::Get,
                        Some("manifest.json"),
                        None,
                        FaultKind::StaleRead,
                    ),
                    5 => (
                        StoreOp::Get,
                        Some("manifest.json"),
                        None,
                        FaultKind::HeadGetDiverge,
                    ),
                    6 => (
                        StoreOp::Delete,
                        None,
                        Some(40),
                        FaultKind::BatchDeletePartial {
                            fail_every: rng.gen_range(2..=4),
                        },
                    ),
                    _ => (
                        StoreOp::Copy,
                        Some("segments/"),
                        None,
                        FaultKind::CopySourceVanish,
                    ),
                };
                let start = rng.gen_range(10..=440);
                push_event(
                    &mut events,
                    profile,
                    start,
                    end_op.map(|length| (start + length).min(500)),
                    Boundary::ObjectStore,
                    TargetSelector {
                        store_op: Some(store_op),
                        key_substring: key_substring.map(str::to_string),
                        ..TargetSelector::default()
                    },
                    kind,
                );
            }
        }
        FaultProfile::Sched => {
            let targets = [
                (StoreOp::Get, "manifest.json"),
                (StoreOp::Put, "lease.json"),
                (StoreOp::Get, "cluster_"),
                (StoreOp::List, ""),
            ];
            for _ in 0..rng.gen_range(1..=3) {
                let (store_op, key_substring) = targets[rng.gen_range(0..targets.len())];
                push_event(
                    &mut events,
                    profile,
                    rng.gen_range(10..=450),
                    None,
                    Boundary::ObjectStore,
                    TargetSelector {
                        store_op: Some(store_op),
                        key_substring: Some(key_substring.to_string()),
                        ..TargetSelector::default()
                    },
                    FaultKind::HoldCall {
                        for_ops: rng.gen_range(2..=8),
                    },
                );
            }
        }
        FaultProfile::Ops => {
            let second_node_start = rng.gen_range(40..=120);
            push_event(
                &mut events,
                profile,
                second_node_start,
                Some(second_node_start + 20),
                Boundary::Runner,
                TargetSelector::default(),
                FaultKind::StartSecondNode { for_ops: 20 },
            );
            push_event(
                &mut events,
                profile,
                rng.gen_range(140..=220),
                None,
                Boundary::Runner,
                TargetSelector::default(),
                FaultKind::FillDiskCache,
            );
            push_event(
                &mut events,
                profile,
                rng.gen_range(230..=310),
                None,
                Boundary::Runner,
                TargetSelector::default(),
                FaultKind::PatchConfigDuringTraffic,
            );
            push_event(
                &mut events,
                profile,
                rng.gen_range(400..=460),
                None,
                Boundary::Runner,
                TargetSelector::default(),
                FaultKind::DeleteNamespaceInFlight,
            );
            push_event(
                &mut events,
                profile,
                0,
                None,
                Boundary::Runner,
                TargetSelector::default(),
                FaultKind::ResourceExhaustion {
                    max_concurrent_queries: 1,
                    disk_cache_max_bytes: if seed % 2 == 0 {
                        2 * 1024 * 1024
                    } else {
                        4 * 1024 * 1024
                    },
                },
            );
        }
        FaultProfile::Full => {
            let sources = [
                FaultProfile::PostCommit,
                FaultProfile::Network,
                FaultProfile::Crash,
                FaultProfile::Clock,
                FaultProfile::Content,
                FaultProfile::Semantic,
                FaultProfile::Sched,
                FaultProfile::Ops,
            ];
            for (index, source) in sources.into_iter().enumerate() {
                let source_seed = seed
                    ^ (u64::try_from(index + 1)
                        .expect("full profile source index must fit u64")
                        .wrapping_mul(0x9e37_79b9_7f4a_7c15));
                let mut event = schedule_for_seed(source_seed, source)
                    .events
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| panic!("{source:?} generated no Full-profile event"));
                event.id = format!("full-{}-{index:02}", source.id_prefix());
                events.push(event);
            }
        }
    }
    relocate_node_starts_outside_global_read_partitions(&mut events);
    FaultSchedule { profile, events }
}

fn relocate_node_starts_outside_global_read_partitions(events: &mut [FaultEvent]) {
    let node_windows = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match event.kind {
            FaultKind::StartSecondNode { for_ops } | FaultKind::StartReadOnlyNode { for_ops } => {
                Some((index, for_ops))
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    for (index, for_ops) in node_windows {
        let mut start_op = events[index].start_op;
        loop {
            let blocking_end = events
                .iter()
                .filter(|event| {
                    event.boundary == Boundary::ObjectStore
                        && event.target.store_op.is_none()
                        && event.target.key_substring.is_none()
                        && event_is_active(event, start_op)
                        && matches!(
                            event.kind,
                            FaultKind::Partition {
                                direction: Direction::All | Direction::ReadsFail,
                            }
                        )
                })
                .map(|event| {
                    event
                        .end_op
                        .expect("global read partitions in generated campaigns must be bounded")
                })
                .max();
            let Some(end_op) = blocking_end else {
                break;
            };
            assert!(
                end_op > start_op,
                "an active global read partition must end after the node-start boundary"
            );
            start_op = end_op;
        }

        events[index].start_op = start_op;
        events[index].end_op = Some(
            start_op
                .checked_add(for_ops)
                .expect("relocated node window must fit the logical-op range"),
        );
    }
}

fn first_event_for_class(seed: u64, profile: FaultProfile, class: ContractClass) -> FaultEvent {
    (0..1_024)
        .find_map(|offset| {
            schedule_for_seed(seed.wrapping_add(offset), profile)
                .events
                .into_iter()
                .find(|event| event.contract_class() == class)
        })
        .unwrap_or_else(|| panic!("{profile:?} generated no {class:?} event"))
}

fn push_event(
    events: &mut Vec<FaultEvent>,
    profile: FaultProfile,
    start_op: u64,
    end_op: Option<u64>,
    boundary: Boundary,
    target: TargetSelector,
    kind: FaultKind,
) {
    let id = format!("{}-{:02}", profile.id_prefix(), events.len());
    events.push(FaultEvent {
        id,
        start_op,
        end_op,
        boundary,
        target,
        kind,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hold_window_wait_observes_existing_and_future_timeline_updates() {
        let scheduler = || {
            FaultScheduler::from_schedule(FaultSchedule {
                profile: FaultProfile::Sched,
                events: vec![FaultEvent {
                    id: "sched-held-manifest-get".to_string(),
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
            })
        };
        let hold_active = || TimelineEvent {
            event_id: "sched-held-manifest-get".to_string(),
            op_index: 5,
            wall_ms: 0,
            boundary: Boundary::ObjectStore,
            action: "HoldCall { for_ops: 3 } call=1".to_string(),
            key: Some("ns/manifest.json".to_string()),
            semantics: FaultSemantics::WindowActive,
            observed: ObservedResult::DefiniteNotApplied,
            recovery: Some("parked until logical op +3".to_string()),
        };

        let already_recorded = scheduler();
        already_recorded.record(hold_active());
        let observed = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            already_recorded.wait_for_hold_window_active("sched-held-manifest-get", 5),
        )
        .await
        .expect("an already-recorded hold must not lose its wakeup");
        assert_eq!(observed.event_id, "sched-held-manifest-get");
        assert_eq!(observed.op_index, 5);
        assert_eq!(observed.semantics, FaultSemantics::WindowActive);

        let recorded_later = scheduler();
        let waiter = tokio::spawn({
            let scheduler = recorded_later.clone();
            async move {
                scheduler
                    .wait_for_hold_window_active("sched-held-manifest-get", 5)
                    .await
            }
        });
        tokio::task::yield_now().await;
        recorded_later.record(TimelineEvent {
            event_id: "unrelated-window".to_string(),
            ..hold_active()
        });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "an unrelated timeline update must not satisfy the hold waiter"
        );
        recorded_later.record(hold_active());
        let observed = tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
            .await
            .expect("recording the matching hold must notify the waiter")
            .expect("hold waiter task must not panic");
        assert_eq!(observed.event_id, "sched-held-manifest-get");
        assert_eq!(recorded_later.timeline().len(), 2);
    }

    #[test]
    fn scheduled_store_fault_windows_are_bounded_and_quiesce_aware() {
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::SupportedFull,
            events: vec![
                FaultEvent {
                    id: "supported-full-semantic-04".to_string(),
                    start_op: 41,
                    end_op: Some(81),
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector {
                        store_op: Some(StoreOp::Delete),
                        ..TargetSelector::default()
                    },
                    kind: FaultKind::BatchDeletePartial { fail_every: 3 },
                },
                FaultEvent {
                    id: "supported-full-post-commit-00".to_string(),
                    start_op: 10,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector::default(),
                    kind: FaultKind::PostCommitFail {
                        error: InjectedErrorKind::Http500,
                    },
                },
                FaultEvent {
                    id: "supported-full-network-01".to_string(),
                    start_op: 100,
                    end_op: Some(120),
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector::default(),
                    kind: FaultKind::Partition {
                        direction: Direction::ReadsFail,
                    },
                },
            ],
        });

        assert!(!scheduler.scheduled_store_fault_window_active(40));
        assert!(scheduler.scheduled_store_fault_window_active(41));
        assert!(
            scheduler.fault_window_active(41, "ns"),
            "SupportedFull S3 oracles must tolerate reads inside bounded faults"
        );
        assert!(scheduler.scheduled_store_fault_window_active(80));
        assert!(
            !scheduler.scheduled_store_fault_window_active(81),
            "end_op is exclusive, matching event_is_active"
        );
        assert!(
            !scheduler.scheduled_store_fault_window_active(15),
            "unbounded events never close, so they must not defer workload ops"
        );
        assert!(!scheduler.global_read_partition_active(99));
        assert!(scheduler.global_read_partition_active(100));
        assert!(scheduler.global_read_partition_active(119));
        assert!(!scheduler.global_read_partition_active(120));

        scheduler.quiesce();
        assert!(!scheduler.scheduled_store_fault_window_active(41));
        assert!(!scheduler.global_read_partition_active(100));
    }

    #[tokio::test]
    async fn foreground_hold_prediction_is_non_consuming_and_returns_exact_release() {
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Sched,
            events: vec![FaultEvent {
                id: "sched-foreground-manifest-get".to_string(),
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
        let calls = [(StoreOp::Get, "ns/manifest.json".to_string())];

        scheduler.advance_to(4);
        assert_eq!(scheduler.foreground_hold_release_for_calls(4, &calls), None);
        scheduler.advance_to(5);
        assert_eq!(
            scheduler.foreground_hold_release_for_calls(5, &calls),
            Some(8)
        );
        assert_eq!(
            scheduler.foreground_hold_release_for_calls(
                5,
                &[(StoreOp::Put, "ns/manifest.json".to_string())]
            ),
            None
        );

        assert!(
            scheduler
                .store_decision(StoreOp::Get, "ns/manifest.json")
                .is_none(),
            "an unarmed background call must not claim a foreground hold"
        );
        let hold = scheduler
            .foreground_hold_for_calls(5, &calls)
            .expect("matching foreground footprint must nominate the hold");
        let action = scheduler
            .with_armed_hold(hold.event_id, async {
                scheduler.store_decision(StoreOp::Get, "ns/manifest.json")
            })
            .await
            .expect("armed foreground call must claim the predicted hold");
        assert!(matches!(action.kind, FaultKind::HoldCall { for_ops: 3 }));
        assert_eq!(
            scheduler.foreground_hold_release_for_calls(5, &calls),
            Some(8)
        );
    }

    #[tokio::test]
    async fn foreground_hold_prediction_tracks_an_armed_claim_until_exact_release() {
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Sched,
            events: vec![FaultEvent {
                id: "sched-background-manifest-get".to_string(),
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
        let calls = [(StoreOp::Get, "ns/manifest.json".to_string())];

        scheduler.advance_to(5);
        assert!(
            scheduler
                .store_decision(StoreOp::Get, "ns/manifest.json")
                .is_none(),
            "background calls must not claim runner-managed holds"
        );
        let hold = scheduler
            .foreground_hold_for_calls(5, &calls)
            .expect("foreground call footprint must predict the hold");
        let action = scheduler
            .with_armed_hold(hold.event_id, async {
                scheduler.store_decision(StoreOp::Get, "ns/manifest.json")
            })
            .await
            .expect("armed store call must claim the scheduled hold");
        assert_eq!(action.op_index, 5);
        assert_eq!(
            scheduler.foreground_hold_release_for_calls(5, &calls),
            Some(8),
            "the foreground runner must join an already claimed hold window"
        );

        scheduler.advance_to(7);
        assert_eq!(
            scheduler.foreground_hold_release_for_calls(7, &calls),
            Some(8)
        );
        scheduler.advance_to(8);
        assert_eq!(scheduler.foreground_hold_release_for_calls(8, &calls), None);
    }

    #[tokio::test]
    async fn foreground_hold_preserves_armed_window_op_across_logical_ops() {
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Sched,
            events: vec![FaultEvent {
                id: "sched-cross-boundary-manifest-get".to_string(),
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
        let calls = [(StoreOp::Get, "ns/manifest.json".to_string())];

        scheduler.advance_to(5);
        let predicted = scheduler
            .foreground_hold_for_calls(5, &calls)
            .expect("foreground call footprint must predict the hold");
        scheduler
            .with_armed_hold(predicted.event_id, async {
                scheduler.store_decision(StoreOp::Get, "ns/manifest.json")
            })
            .await
            .expect("armed store call must claim the scheduled hold");
        scheduler.advance_to(6);

        let hold = scheduler
            .foreground_hold_for_calls(6, &calls)
            .expect("claimed hold must remain visible before release");
        assert_eq!(hold.event_id, "sched-cross-boundary-manifest-get");
        assert_eq!(hold.window_op, 5);
        assert_eq!(hold.release_op, 8);
    }

    #[tokio::test]
    async fn foreground_hold_exclusion_finds_overlapping_matching_event() {
        let manifest_get = TargetSelector {
            store_op: Some(StoreOp::Get),
            key_substring: Some("manifest.json".to_string()),
            ..TargetSelector::default()
        };
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Sched,
            events: vec![
                FaultEvent {
                    id: "sched-first".to_string(),
                    start_op: 5,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: manifest_get.clone(),
                    kind: FaultKind::HoldCall { for_ops: 3 },
                },
                FaultEvent {
                    id: "sched-second".to_string(),
                    start_op: 5,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: manifest_get,
                    kind: FaultKind::HoldCall { for_ops: 4 },
                },
            ],
        });
        let calls = [(StoreOp::Get, "ns/manifest.json".to_string())];

        scheduler.advance_to(5);
        let first = scheduler
            .foreground_hold_for_calls(5, &calls)
            .expect("first scheduled hold must be predicted");
        let claimed = scheduler
            .with_armed_hold(first.event_id, async {
                scheduler.store_decision(StoreOp::Get, "ns/manifest.json")
            })
            .await
            .expect("first scheduled hold must be claimed");
        assert_eq!(claimed.event_id, "sched-first");
        assert_eq!(
            scheduler
                .foreground_hold_for_calls(5, &calls)
                .expect("default prediction keeps the claimed event first")
                .event_id,
            "sched-first"
        );

        let second = scheduler
            .foreground_hold_for_calls_excluding(5, &calls, Some("sched-first"))
            .expect("excluding the pending event must reveal the overlapping hold");
        assert_eq!(second.event_id, "sched-second");
        assert_eq!(second.window_op, 5);
        assert_eq!(second.release_op, 9);
    }

    #[test]
    fn schedule_is_pure_function_of_seed() {
        for profile in [
            FaultProfile::PostCommit,
            FaultProfile::Network,
            FaultProfile::Crash,
            FaultProfile::Clock,
            FaultProfile::Security,
            FaultProfile::Late,
            FaultProfile::LateStream,
            FaultProfile::Content,
            FaultProfile::Semantic,
            FaultProfile::Sched,
            FaultProfile::Ops,
            FaultProfile::Full,
        ] {
            for seed in 0..20 {
                let first = FaultScheduler::for_seed(seed, profile);
                let second = FaultScheduler::for_seed(seed, profile);
                assert_eq!(first.schedule(), second.schedule());
                assert!(first.schedule().events.iter().all(|event| {
                    event.start_op < 500
                        && event
                            .end_op
                            .is_none_or(|end_op| end_op <= 500 && end_op > event.start_op)
                }));
            }
        }
    }

    #[test]
    fn late_stream_schedule_is_get_only_mixed_and_has_clean_followup_slots() {
        for seed in 0..100 {
            let scheduler = FaultScheduler::for_seed(seed, FaultProfile::LateStream);
            let schedule = scheduler.schedule();
            let events = &schedule.events;
            assert_eq!(schedule.profile, FaultProfile::LateStream);
            assert!((2..=5).contains(&events.len()), "seed {seed}: {events:#?}");

            let mut kinds_seen = [false; 3];
            let mut selectors_seen = [false; 3];
            let mut previous_start = None;
            for event in events {
                assert_eq!(event.boundary, Boundary::ObjectStore);
                assert_eq!(event.target.store_op, Some(StoreOp::Get));
                assert!(event.target.path_substring.is_none());
                assert!(event.target.methods.is_none());
                assert!(event.start_op >= LATE_STREAM_FIRST_FAULT_OP);
                assert_eq!(event.end_op, Some(event.start_op + 1));
                assert!(event.start_op < LATE_STREAM_FAULT_LIMIT);
                if let Some(previous_start) = previous_start {
                    assert!(
                        event.start_op >= previous_start + 2,
                        "seed {seed} has no clean slot between {previous_start} and {}",
                        event.start_op
                    );
                }
                previous_start = Some(event.start_op);

                let selector = event
                    .target
                    .key_substring
                    .as_deref()
                    .expect("late-stream GET must select a persisted artifact family");
                match event.kind {
                    FaultKind::PreFail { .. } => {
                        assert!(matches!(selector, "/matrix_" | "/attrs_"));
                        kinds_seen[0] = true;
                    }
                    FaultKind::Latency { jitter_ms, .. } => {
                        assert!(jitter_ms > 0, "seed {seed}: {event:#?}");
                        assert_eq!(selector, "late/segments/");
                        kinds_seen[1] = true;
                    }
                    FaultKind::TruncatedGetStream { after_bytes } => {
                        assert!(after_bytes > 0, "seed {seed}: {event:#?}");
                        assert!(matches!(selector, "/matrix_" | "/attrs_"));
                        kinds_seen[2] = true;
                    }
                    ref other => panic!("seed {seed} scheduled non-stream fault {other:?}"),
                }

                let selector_index = LATE_STREAM_KEY_SUBSTRINGS
                    .iter()
                    .position(|expected| selector == *expected)
                    .unwrap_or_else(|| panic!("seed {seed} used unknown selector {selector}"));
                selectors_seen[selector_index] = true;

                assert!(scheduler.fault_window_active(event.start_op, "test-prefix-adv-7-late"));
                assert!(
                    !scheduler.fault_window_active(event.start_op + 1, "test-prefix-adv-7-late")
                );
                assert!(!scheduler.fault_window_active(event.start_op, "ordinary-namespace"));
            }

            assert_eq!(kinds_seen, [true; 3], "seed {seed}: {events:#?}");
            assert_eq!(selectors_seen, [true; 3], "seed {seed}: {events:#?}");
        }
    }

    #[test]
    fn late_stream_artifact_selector_cannot_fault_ordinary_ivf_keys() {
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::LateStream,
            events: vec![FaultEvent {
                id: "late-stream-selector".to_string(),
                start_op: 7,
                end_op: Some(8),
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("/attrs_".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::PreFail {
                    error: InjectedErrorKind::Http500,
                },
            }],
        });
        scheduler.advance_to(7);

        assert!(scheduler
            .store_decision(StoreOp::Get, "ordinary/segments/segment-id/attrs_0.bin")
            .is_none());
        assert!(scheduler
            .store_decision(
                StoreOp::Get,
                "namespace-late/late/segments/segment-id/attrs_0.bin"
            )
            .is_some());
    }

    #[test]
    fn full_profile_draws_one_deterministic_event_from_each_family() {
        let scheduler = FaultScheduler::for_seed(11, FaultProfile::Full);
        let schedule = scheduler.schedule();
        assert_eq!(schedule.profile, FaultProfile::Full);
        assert_eq!(schedule.events.len(), 8, "{schedule:#?}");
        assert_eq!(
            schedule
                .events
                .iter()
                .map(|event| event.id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "full-post-commit-00",
                "full-network-01",
                "full-crash-02",
                "full-clock-03",
                "full-content-04",
                "full-semantic-05",
                "full-sched-06",
                "full-ops-07",
            ]
        );
        assert!(scheduler.process_controller().is_some());
        assert_eq!(
            schedule,
            FaultScheduler::for_seed(11, FaultProfile::Full).schedule()
        );
    }

    #[tokio::test]
    async fn dual_writer_fencing_selftest_pins_node_window_and_lease_hold() {
        let schedule = FaultSchedule::dual_writer_fencing_selftest();
        assert_eq!(schedule.profile, FaultProfile::Ops);
        assert_eq!(schedule.events.len(), 2, "{schedule:#?}");

        let second_node = &schedule.events[0];
        assert_eq!(second_node.id, "ops-dual-writer-second-node");
        assert_eq!(second_node.start_op, 0);
        assert_eq!(second_node.end_op, Some(20));
        assert_eq!(second_node.boundary, Boundary::Runner);
        assert_eq!(second_node.target, TargetSelector::default());
        assert!(matches!(
            second_node.kind,
            FaultKind::StartSecondNode { for_ops: 20 }
        ));

        let lease_hold = &schedule.events[1];
        assert_eq!(lease_hold.id, "ops-dual-writer-lease-hold");
        assert_eq!(lease_hold.start_op, 0);
        assert_eq!(lease_hold.end_op, None);
        assert_eq!(lease_hold.boundary, Boundary::ObjectStore);
        assert_eq!(lease_hold.target.store_op, Some(StoreOp::Put));
        assert_eq!(
            lease_hold.target.key_substring.as_deref(),
            Some("lease.json")
        );
        assert!(matches!(
            lease_hold.kind,
            FaultKind::HoldCall { for_ops: 8 }
        ));
        let lease_hold_id = lease_hold.id.clone();

        let scheduler = FaultScheduler::from_schedule(schedule);
        assert_eq!(
            scheduler.advance_to(0),
            vec![SchedulerCommand::StartSecondNode {
                event_id: "ops-dual-writer-second-node".to_string(),
                for_ops: 20,
            }]
        );
        let action = scheduler
            .with_armed_hold(lease_hold_id, async {
                scheduler.store_decision(StoreOp::Put, "ns/lease.json")
            })
            .await
            .expect("pinned schedule must hold the first lease PUT");
        assert_eq!(action.event_id, "ops-dual-writer-lease-hold");
        assert!(matches!(action.kind, FaultKind::HoldCall { for_ops: 8 }));
        assert_eq!(
            scheduler.advance_to(20),
            vec![SchedulerCommand::StopSecondNode {
                event_id: "ops-dual-writer-second-node".to_string(),
            }]
        );
    }

    #[test]
    fn duplicate_retries_only_target_vector_mutations() {
        let mut duplicate_retries = 0;
        for seed in 0..1_000 {
            let scheduler = FaultScheduler::for_seed(seed, FaultProfile::Network);
            for event in &scheduler.schedule().events {
                if !matches!(event.kind, FaultKind::DuplicateRetry) {
                    continue;
                }

                duplicate_retries += 1;
                assert_eq!(event.target.path_substring.as_deref(), Some("/vectors"));
                assert!(matches!(
                    event.target.methods.as_deref(),
                    Some([method]) if method == "POST" || method == "DELETE"
                ));
            }
        }
        assert!(
            duplicate_retries > 0,
            "seed sweep generated no duplicate retries"
        );
    }

    #[test]
    fn content_schedule_bounds_durable_corruption_and_targets_catalog_keys() {
        let catalog = [
            "manifest.json",
            ".wal",
            "segments/",
            "centroids",
            "bootstrap.bin",
            "coarse_sketch.bin",
            "cluster_",
        ];
        for seed in 0..100 {
            let scheduler = FaultScheduler::for_seed(seed, FaultProfile::Content);
            let events = &scheduler.schedule().events;
            let durable = events
                .iter()
                .filter(|event| {
                    matches!(
                        event.kind,
                        FaultKind::Content(
                            ContentFault::TornWrite { .. } | ContentFault::MisdirectedWrite
                        )
                    )
                })
                .count();
            let gets = events
                .iter()
                .filter(|event| event.target.store_op == Some(StoreOp::Get))
                .count();
            let silent_deletes = events
                .iter()
                .filter(|event| {
                    matches!(
                        event.kind,
                        FaultKind::Content(ContentFault::SilentDeleteFailure)
                    )
                })
                .count();

            assert_eq!(durable, 1, "seed {seed}: {events:#?}");
            assert!((1..=3).contains(&gets), "seed {seed}: {events:#?}");
            assert!(silent_deletes <= 1, "seed {seed}: {events:#?}");
            assert!(events.iter().all(|event| {
                event
                    .target
                    .key_substring
                    .as_deref()
                    .is_some_and(|key| catalog.contains(&key))
            }));
        }
    }

    #[test]
    fn semantic_schedule_has_two_to_five_declared_events() {
        for seed in 0..100 {
            let scheduler = FaultScheduler::for_seed(seed, FaultProfile::Semantic);
            let events = &scheduler.schedule().events;
            assert!((2..=5).contains(&events.len()), "seed {seed}: {events:#?}");
            assert!(events.iter().all(|event| {
                matches!(
                    event.kind,
                    FaultKind::CasConflict
                        | FaultKind::ListOmit { .. }
                        | FaultKind::ListDuplicate { .. }
                        | FaultKind::ListReorder
                        | FaultKind::StaleRead
                        | FaultKind::HeadGetDiverge
                        | FaultKind::BatchDeletePartial { .. }
                        | FaultKind::CopySourceVanish
                )
            }));
        }
    }

    #[test]
    fn sched_schedule_has_one_to_three_bounded_toctou_holds() {
        let targets = [
            (StoreOp::Get, "manifest.json"),
            (StoreOp::Put, "lease.json"),
            (StoreOp::Get, "cluster_"),
            (StoreOp::List, ""),
        ];
        for seed in 0..100 {
            let scheduler = FaultScheduler::for_seed(seed, FaultProfile::Sched);
            let events = &scheduler.schedule().events;
            assert!((1..=3).contains(&events.len()), "seed {seed}: {events:#?}");
            assert!(events.iter().all(|event| {
                let FaultKind::HoldCall { for_ops } = event.kind else {
                    return false;
                };
                (2..=8).contains(&for_ops)
                    && event.boundary == Boundary::ObjectStore
                    && targets.iter().any(|(op, key)| {
                        event.target.store_op == Some(*op)
                            && event.target.key_substring.as_deref() == Some(*key)
                    })
            }));
        }
    }

    #[test]
    fn ops_schedule_emits_a_twenty_op_node_window_and_all_events() {
        for seed in 0..100 {
            let scheduler = FaultScheduler::for_seed(seed, FaultProfile::Ops);
            let events = &scheduler.schedule().events;
            assert_eq!(events.len(), 5, "seed {seed}: {events:#?}");
            let start = events
                .iter()
                .find_map(|event| match event.kind {
                    FaultKind::StartSecondNode { for_ops: 20 } => Some(event.start_op),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("seed {seed} omitted second-node window"));
            assert!(events
                .iter()
                .any(|event| matches!(event.kind, FaultKind::PatchConfigDuringTraffic)));
            assert!(events
                .iter()
                .any(|event| matches!(event.kind, FaultKind::DeleteNamespaceInFlight)));
            assert!(events
                .iter()
                .any(|event| matches!(event.kind, FaultKind::FillDiskCache)));
            assert!(events
                .iter()
                .any(|event| matches!(event.kind, FaultKind::ResourceExhaustion { .. })));

            assert!(scheduler.advance_to(start).iter().any(|command| {
                matches!(
                    command,
                    SchedulerCommand::StartSecondNode { for_ops: 20, .. }
                )
            }));
            assert!(scheduler
                .advance_to(start + 20)
                .iter()
                .any(|command| matches!(command, SchedulerCommand::StopSecondNode { .. })));
        }
    }

    #[test]
    fn window_activation_math() {
        let schedule = FaultSchedule {
            profile: FaultProfile::Network,
            events: vec![
                FaultEvent {
                    id: "network-00".to_string(),
                    start_op: 3,
                    end_op: Some(5),
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector::default(),
                    kind: FaultKind::Partition {
                        direction: Direction::All,
                    },
                },
                FaultEvent {
                    id: "network-01".to_string(),
                    start_op: 4,
                    end_op: None,
                    boundary: Boundary::ClientHttp,
                    target: TargetSelector::default(),
                    kind: FaultKind::DropRequest,
                },
            ],
        };
        let scheduler = FaultScheduler::from_schedule(schedule);
        let _ = scheduler.advance_to(2);
        assert!(scheduler.store_decision(StoreOp::Get, "key").is_none());
        let _ = scheduler.advance_to(3);
        assert!(scheduler.store_decision(StoreOp::Get, "key").is_some());
        let _ = scheduler.advance_to(4);
        assert!(scheduler.store_decision(StoreOp::Put, "key").is_some());
        assert!(scheduler.http_decision(&Method::POST, "/path").is_some());
        assert!(scheduler.http_decision(&Method::POST, "/path").is_none());
        let _ = scheduler.advance_to(5);
        assert!(scheduler.store_decision(StoreOp::Get, "key").is_none());
        scheduler.quiesce();
        let _ = scheduler.advance_to(4);
        assert!(scheduler.store_decision(StoreOp::Get, "key").is_none());
    }

    #[test]
    fn corruption_fault_window_is_namespace_scoped_with_trailing_slop() {
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Content,
            events: Vec::new(),
        });
        scheduler.record(TimelineEvent {
            event_id: "content-00".to_string(),
            op_index: 3,
            wall_ms: 0,
            boundary: Boundary::ObjectStore,
            action: "Content(BitFlip)".to_string(),
            key: Some("ns/segments/cluster_0.bin".to_string()),
            semantics: FaultSemantics::PostCommit,
            observed: ObservedResult::Corrupted,
            recovery: None,
        });

        assert!(!scheduler.fault_window_active(2, "ns"));
        assert!(scheduler.fault_window_active(3, "ns"));
        assert!(scheduler.fault_window_active(11, "ns"));
        assert!(!scheduler.fault_window_active(12, "ns"));
        assert!(!scheduler.fault_window_active(4, "other"));
    }

    #[test]
    fn full_profile_inherits_semantic_fault_window_attribution() {
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Full,
            events: vec![FaultEvent {
                id: "full-semantic-05".to_string(),
                start_op: 55,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("manifest.json".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::HeadGetDiverge,
            }],
        });
        let _ = scheduler.advance_to(55);
        let action = scheduler
            .store_decision(StoreOp::Get, "ns/manifest.json")
            .expect("pinned semantic event must fire");
        scheduler.record(TimelineEvent {
            event_id: action.event_id,
            op_index: 55,
            wall_ms: 0,
            boundary: Boundary::ObjectStore,
            action: "HeadGetDiverge".to_string(),
            key: Some("ns/manifest.json".to_string()),
            semantics: FaultSemantics::PostCommit,
            observed: ObservedResult::Corrupted,
            recovery: Some("HEAD succeeded before injected GET NotFound".to_string()),
        });

        assert!(scheduler.fault_window_active(56, "ns"));
        assert!(!scheduler.fault_window_active(56, "other"));
    }

    #[test]
    fn generated_campaigns_carry_explicit_contract_classes() {
        for seed in 0..100 {
            let supported = FaultScheduler::for_seed(seed, FaultProfile::SupportedFull);
            assert!(!supported.schedule().events.is_empty(), "seed {seed}");
            assert!(supported.schedule().events.iter().all(|event| {
                event.contract_class() == ContractClass::SupportedV1
                    && event.violated_assumptions().is_empty()
            }));
            assert!(supported
                .schedule()
                .events
                .iter()
                .any(|event| matches!(event.kind, FaultKind::StartReadOnlyNode { .. })));
            assert!(!supported
                .schedule()
                .events
                .iter()
                .any(|event| matches!(event.kind, FaultKind::StartSecondNode { .. })));

            let provider = FaultScheduler::for_seed(seed, FaultProfile::ProviderContractAbuse);
            assert!(!provider.schedule().events.is_empty(), "seed {seed}");
            assert!(provider.schedule().events.iter().all(|event| {
                event.contract_class() == ContractClass::ProviderContractAbuse
                    && !event.violated_assumptions().is_empty()
            }));

            let future = FaultScheduler::for_seed(seed, FaultProfile::FutureArchitecture);
            assert!(!future.schedule().events.is_empty(), "seed {seed}");
            assert!(future.schedule().events.iter().all(|event| {
                event.contract_class() == ContractClass::FutureArchitecture
                    && event.violated_assumptions() == [ProtectedAssumption::A3]
            }));
        }
    }

    #[test]
    fn stale_manifest_cas_selftest_exercises_the_write_contract() {
        let schedule = FaultSchedule::stale_manifest_cas_selftest();
        assert_eq!(schedule.events.len(), 1);
        assert_eq!(
            schedule.events[0].contract_class(),
            ContractClass::HarnessSelfTest
        );
        assert_eq!(
            schedule.events[0].violated_assumptions(),
            [ProtectedAssumption::A1]
        );
    }

    #[test]
    fn security_profile_is_supported_and_spans_only_the_four_v1_boundaries() {
        let schedule = FaultScheduler::for_seed(7, FaultProfile::Security)
            .schedule()
            .clone();
        assert_eq!(schedule.events.len(), 4, "{schedule:#?}");
        assert_eq!(
            schedule
                .events
                .iter()
                .map(|event| event.boundary)
                .collect::<Vec<_>>(),
            vec![
                Boundary::ObjectStore,
                Boundary::ClientHttp,
                Boundary::Process,
                Boundary::Clock,
            ]
        );
        assert!(schedule.events.iter().all(|event| {
            event.contract_class() == ContractClass::SupportedV1
                && event.violated_assumptions().is_empty()
                && !matches!(
                    event.kind,
                    FaultKind::Content(_)
                        | FaultKind::ListOmit { .. }
                        | FaultKind::ListDuplicate { .. }
                        | FaultKind::ListReorder
                        | FaultKind::StaleRead
                        | FaultKind::HeadGetDiverge
                        | FaultKind::StartSecondNode { .. }
                )
        }));
        let mut starts = schedule
            .events
            .iter()
            .map(|event| event.start_op)
            .collect::<Vec<_>>();
        starts.sort_unstable();
        assert_eq!(starts, vec![20, 24, 32, 38]);
    }

    #[test]
    fn composite_node_starts_avoid_global_read_partitions() {
        for profile in [FaultProfile::SupportedFull, FaultProfile::Full] {
            for seed in 0..100 {
                let scheduler = FaultScheduler::for_seed(seed, profile);
                for start_op in scheduler.schedule().events.iter().filter_map(|event| {
                    matches!(
                        event.kind,
                        FaultKind::StartReadOnlyNode { .. } | FaultKind::StartSecondNode { .. }
                    )
                    .then_some(event.start_op)
                }) {
                    assert!(
                        !scheduler.global_read_partition_active(start_op),
                        "profile={profile:?} seed={seed} start_op={start_op}"
                    );
                }
            }
        }
    }

    #[test]
    fn explicit_research_profiles_and_legacy_schedule_names_remain_decodable() {
        assert_eq!(
            FaultProfile::from_env("provider_contract_abuse"),
            FaultProfile::ProviderContractAbuse
        );
        assert_eq!(
            FaultProfile::from_env("future_architecture"),
            FaultProfile::FutureArchitecture
        );
        assert_eq!(FaultProfile::from_env("content"), FaultProfile::Content);
        assert_eq!(FaultProfile::from_env("full"), FaultProfile::Full);
        assert_eq!(FaultProfile::from_env("late"), FaultProfile::Late);
        assert_eq!(FaultProfile::Late.as_env(), "late");
        assert_eq!(
            FaultProfile::from_env("late-stream"),
            FaultProfile::LateStream
        );
        assert_eq!(FaultProfile::LateStream.as_env(), "late-stream");
        assert!(FaultScheduler::for_seed(7, FaultProfile::Late)
            .schedule()
            .events
            .is_empty());

        let legacy = serde_json::json!({
            "profile": "full",
            "events": [{
                "id": "full-content-04",
                "start_op": 12,
                "end_op": null,
                "boundary": "object_store",
                "target": {
                    "store_op": "put",
                    "key_substring": "manifest.json",
                    "path_substring": null,
                    "methods": null
                },
                "kind": { "content": "misdirected_write" }
            }]
        });
        let schedule: FaultSchedule = serde_json::from_value(legacy).unwrap();
        assert_eq!(schedule.profile, FaultProfile::Full);
        assert_eq!(
            schedule.events[0].contract_class(),
            ContractClass::ProviderContractAbuse
        );
        assert_eq!(
            schedule.events[0].violated_assumptions(),
            [ProtectedAssumption::A1]
        );
    }
}
