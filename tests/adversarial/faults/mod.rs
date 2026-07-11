pub mod clock;
pub mod http_proxy;
pub mod process;
pub mod store_proxy;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reqwest::Method;
use serde::{Deserialize, Serialize};

use self::process::{CrashPoint, ProcessController, TriggerPosition};
use super::chaos::StoreOp;

const FAULT_WINDOW_TRAILING_OPS: u64 = 8;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FaultSchedule {
    pub profile: FaultProfile,
    pub events: Vec<FaultEvent>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FaultProfile {
    LegacyChaos,
    PostCommit,
    Network,
    Crash,
    Clock,
    Content,
    Semantic,
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
            "content" => Self::Content,
            "semantic" => Self::Semantic,
            other => panic!("invalid ZEPPELIN_ADVERSARIAL_PROFILE: {other}"),
        }
    }

    fn id_prefix(self) -> &'static str {
        match self {
            Self::LegacyChaos => "legacy-chaos",
            Self::PostCommit => "post-commit",
            Self::Network => "network",
            Self::Crash => "crash",
            Self::Clock => "clock",
            Self::Content => "content",
            Self::Semantic => "semantic",
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FaultEvent {
    pub id: String,
    pub start_op: u64,
    pub end_op: Option<u64>,
    pub boundary: Boundary,
    pub target: TargetSelector,
    pub kind: FaultKind,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Boundary {
    ObjectStore,
    ClientHttp,
    Process,
    Clock,
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

#[derive(Debug)]
struct EventRuntime {
    fired: AtomicBool,
    ended: AtomicBool,
    matches: AtomicU64,
}

#[derive(Debug)]
struct SchedulerRuntime {
    logical_op: AtomicU64,
    quiesced: AtomicBool,
    timeline: Mutex<Vec<TimelineEvent>>,
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
        let process = (schedule.profile == FaultProfile::Crash)
            .then(|| Arc::new(Mutex::new(ProcessController::new())));
        let events = schedule
            .events
            .iter()
            .map(|_| EventRuntime {
                fired: AtomicBool::new(false),
                ended: AtomicBool::new(false),
                matches: AtomicU64::new(0),
            })
            .collect();
        Self {
            schedule: Arc::new(schedule),
            runtime: Arc::new(SchedulerRuntime {
                logical_op: AtomicU64::new(0),
                quiesced: AtomicBool::new(false),
                timeline: Mutex::new(Vec::new()),
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

    pub fn advance_to(&self, op_index: u64) -> Vec<ClockCommand> {
        self.runtime.logical_op.store(op_index, Ordering::SeqCst);
        if self.runtime.quiesced.load(Ordering::SeqCst) {
            return Vec::new();
        }

        let mut commands = Vec::new();
        for (index, event) in self.schedule.events.iter().enumerate() {
            if event.boundary != Boundary::Clock {
                continue;
            }
            let runtime = &self.runtime.events[index];
            match event.kind {
                FaultKind::ClockJump { delta_ms } if op_index == event.start_op => {
                    if runtime
                        .fired
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        commands.push(ClockCommand::Jump {
                            event_id: event.id.clone(),
                            delta_ms,
                        });
                    }
                }
                FaultKind::ClockFreeze { for_ops } => {
                    if op_index == event.start_op
                        && runtime
                            .fired
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        commands.push(ClockCommand::Freeze {
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
                        commands.push(ClockCommand::Thaw {
                            event_id: event.id.clone(),
                        });
                    }
                }
                _ => {}
            }
        }
        commands
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
                || !store_target_matches(event, op, key)
                || !partition_matches(&event.kind, op)
                || !self.claim(index, event)
            {
                continue;
            }
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
            return Some(StoreFaultAction {
                event_id: event.id.clone(),
                op_index: current,
                kind: event.kind.clone(),
                call_ordinal,
                latency_ms,
                window: event.end_op.is_some(),
            });
        }
        None
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
                || !self.claim(index, event)
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

    fn claim(&self, index: usize, event: &FaultEvent) -> bool {
        let runtime = &self.runtime.events[index];
        let match_ordinal = runtime.matches.fetch_add(1, Ordering::SeqCst) + 1;
        if let FaultKind::CrashAt { point, .. } = event.kind {
            let (_, _, nth) = point.selector();
            if match_ordinal != u64::from(nth) {
                return false;
            }
        }
        event.end_op.is_some()
            || runtime
                .fired
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }

    pub fn quiesce(&self) {
        self.runtime.quiesced.store(true, Ordering::SeqCst);
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
        if self.runtime.quiesced.load(Ordering::SeqCst)
            || !matches!(
                self.schedule.profile,
                FaultProfile::Content | FaultProfile::Semantic
            )
        {
            return false;
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
                && op_index >= event.op_index
                && op_index <= event.op_index.saturating_add(FAULT_WINDOW_TRAILING_OPS)
                && event
                    .key
                    .as_deref()
                    .is_some_and(|key| key_is_in_namespace(key, namespace))
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
}

fn event_is_active(event: &FaultEvent, current: u64) -> bool {
    current >= event.start_op && event.end_op.is_none_or(|end| current < end)
}

fn store_target_matches(event: &FaultEvent, op: StoreOp, key: &str) -> bool {
    event.target.store_op.is_none_or(|expected| expected == op)
        && event
            .target
            .key_substring
            .as_deref()
            .is_none_or(|needle| key.contains(needle))
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
        FaultProfile::LegacyChaos => {}
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
    }
    FaultSchedule { profile, events }
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

    #[test]
    fn schedule_is_pure_function_of_seed() {
        for profile in [
            FaultProfile::PostCommit,
            FaultProfile::Network,
            FaultProfile::Crash,
            FaultProfile::Clock,
            FaultProfile::Content,
            FaultProfile::Semantic,
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
}
