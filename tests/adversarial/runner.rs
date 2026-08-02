use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::{Client, Method, StatusCode};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;
use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::compaction::gc;
use zeppelin::compaction::Compactor;
use zeppelin::config::{Config, GcConfig};
use zeppelin::embedding::MultiVectorEncoderProvider;
use zeppelin::error::ZeppelinError;
use zeppelin::namespace::manager::{NamespaceMetadata, NamespaceState};
use zeppelin::namespace::NamespaceManager;
use zeppelin::security::{
    verify_audit_day, AuditRecord, AuditRuntime, PolicyHead, PolicySnapshot, SecurityKernel,
};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::Clock;
use zeppelin::types::ConsistencyLevel;
use zeppelin::wal::{Lease, LeaseManager, Manifest, WalReader};

use crate::common::counting::{counting_store, ArtifactClass, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::common::server::{
    cleanup_ns, client_with_bearer, start_test_server_full,
    start_test_server_full_with_disk_cache_max_bytes,
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer,
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer_and_encoder_provider,
    start_test_server_full_with_disk_cache_max_bytes_and_encoder_provider, FullTestServer,
    FullTestServerWatchdogHandle, WorkloadCredentialRegistry,
};

use super::artifacts::{
    read_ops, read_seed_config, FailureManifest, ObjectStoreCensus, ObjectStorePhaseCensus,
    RunArtifacts, SeedArtifacts, SeedReport,
};
use super::chaos::{chaos_store, ChaosHandle, FaultPlan, FiredFault, StoreOp};
use super::faults::clock::TestClock;
use super::faults::http_proxy::{HttpFaultInjector, HttpFaultRequestHandle};
use super::faults::process::{CrashRequest, ProcessController, TriggerPosition};
use super::faults::store_proxy::{
    operational_store_proxy, stale_manifest_cas_selftest_proxy, store_fault_proxy,
    OperationalStoreObserver,
};
use super::faults::{
    Boundary, ClockCommand, ContentFault, FaultKind, FaultProfile, FaultSchedule, FaultScheduler,
    FaultSemantics, ForegroundHold, HttpFaultAction, ObservedResult, SchedulerCommand,
    TimelineEvent,
};
use super::generator::{AdversarialGenerator, Coverage};
use super::late_support::{
    activate_late_embedding_profile, encode_matrix_text, enrich_pending_retrieval_units,
    late_encoder_provider,
};
use super::model::{
    AmbiguityReason, IndetEffect, Model, ModelRecord, NsIndeterminate, OpOutcome, OracleMutation,
};
use super::ops::{
    ActorSel, BranchingOp, DelegatedTokenSpec, DeleteUnderLockSurface, ExecutionMetadata,
    ExecutionPhase, ForbiddenWriteKind, GenVector, GeneratedQuery, GrantChange,
    HeldExecutionMetadata, HoldReleaseCause, InvalidProbe, LockSel, NamespaceSpec, Op, OpRecord,
    PreservationScopeSpec, QueryOracleClass, TenantProbeSurface, TokenSel,
};
use super::oracle::{self, Violation, ViolationId};
use super::s3_oracle::{self, S3Tracker};
use super::security_program::{
    check_i22_authz_decision, check_i23_tenant_leak, check_i24_revocation_freshness,
    check_i25_audit_evidence, check_i26_security_state, check_i27_constraint_drop,
    modeled_preservation_reason, ExpectedDecision, SecurityFinding, SecurityProgramConfig,
    SecurityStateObservation, SECURITY_AUDIT_BARRIER_OP,
};
use super::{effective_seed_assignment, PreserveMode, RunMode, RunnerEnv};

const AMBIGUITY_MARKER: &str = "_adversarial_ambiguity";
const STORE_FAULT_MARKER: &str = "_adversarial_store_fault";
const DUAL_WRITER_LEASE_HOLD_EVENT_ID: &str = "ops-dual-writer-lease-hold";
const SEED_WATCHDOG_TIMEOUT: Duration = Duration::from_secs(60);
const SEED_WATCHDOG_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const SECURITY_OP_KINDS: [&str; 20] = [
    "create_key",
    "rotate_key",
    "revoke_key",
    "publish_grant_change",
    "tenant_boundary_probe",
    "use_revoked_credential",
    "forbidden_write_probe",
    "export_probe",
    "security_admin_probe",
    "audit_barrier",
    "audit_chain_check",
    "mint_token",
    "use_token",
    "token_exceed_scope_probe",
    "use_expired_token",
    "revoke_parent_then_use_token",
    "create_lock",
    "release_lock",
    "delete_under_lock",
    "gc_under_lock",
];

/// Delegated bearers are deliberately process-local and are redacted before an
/// operation response reaches persisted artifacts. The ephemeral server URL is
/// part of the key so a failed mint can never fall back to an earlier seed's
/// credential.
static DELEGATED_TOKEN_BEARERS: LazyLock<RwLock<BTreeMap<(String, TokenSel), String>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));

static PRESERVATION_LOCK_IDS: LazyLock<RwLock<BTreeMap<(String, LockSel), String>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));

fn install_preservation_lock(base_url: &str, lock: LockSel, lock_id: String) {
    PRESERVATION_LOCK_IDS
        .write()
        .unwrap_or_else(|_| panic!("preservation lock registry poisoned"))
        .insert((base_url.to_string(), lock), lock_id);
}

fn registered_preservation_lock_id(base_url: &str, lock: LockSel) -> Option<String> {
    PRESERVATION_LOCK_IDS
        .read()
        .unwrap_or_else(|_| panic!("preservation lock registry poisoned"))
        .get(&(base_url.to_string(), lock))
        .cloned()
}

async fn preservation_lock_id_or_resolve(target: &OpExecutionTarget, lock: LockSel) -> String {
    if let Some(lock_id) = registered_preservation_lock_id(&target.base_url, lock) {
        return lock_id;
    }

    let preservation = target
        .security
        .preservation_service()
        .expect("security profile omitted the preservation authority");
    preservation
        .refresh_once()
        .await
        .unwrap_or_else(|error| panic!("preservation lock recovery refresh failed: {error}"));
    let reason = modeled_preservation_reason(lock);
    let matching = preservation
        .list_active()
        .unwrap_or_else(|error| panic!("preservation lock recovery list failed: {error}"))
        .into_iter()
        .filter(|record| record.reason_text == reason)
        .map(|record| record.lock_id.as_str().to_string())
        .collect::<Vec<_>>();
    match matching.as_slice() {
        [lock_id] => {
            install_preservation_lock(&target.base_url, lock, lock_id.clone());
            lock_id.clone()
        }
        [] => {
            // The create was authoritatively not applied. A strict, valid but
            // nonexistent identity lets the release exercise product NotFound
            // handling without inventing a successful create in the model.
            "plk_01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string()
        }
        _ => panic!(
            "preservation selector {:?} matched multiple authoritative locks",
            lock
        ),
    }
}

fn install_delegated_token(base_url: &str, token: TokenSel, bearer: String) {
    DELEGATED_TOKEN_BEARERS
        .write()
        .unwrap_or_else(|_| panic!("delegated token registry lock poisoned"))
        .insert((base_url.to_string(), token), bearer);
}

fn delegated_token_client(base_url: &str, token: TokenSel) -> Client {
    let bearer = DELEGATED_TOKEN_BEARERS
        .read()
        .unwrap_or_else(|_| panic!("delegated token registry lock poisoned"))
        .get(&(base_url.to_string(), token))
        .cloned()
        .unwrap_or_else(|| panic!("token selector {:?} has not been minted", token));
    client_with_bearer(&bearer)
}

fn delegated_token_secrets(base_url: &str) -> Vec<String> {
    DELEGATED_TOKEN_BEARERS
        .read()
        .unwrap_or_else(|_| panic!("delegated token registry lock poisoned"))
        .iter()
        .filter(|((server_url, _), _)| server_url == base_url)
        .map(|(_, bearer)| bearer.clone())
        .collect()
}

fn bytes_contain_secret(bytes: &[u8], secrets: &[String]) -> bool {
    secrets.iter().any(|secret| {
        !secret.is_empty()
            && bytes
                .windows(secret.len())
                .any(|window| window == secret.as_bytes())
    })
}

fn initialize_security_model(
    model: &mut Model,
    program: SecurityProgramConfig,
    policy_version: u64,
    credentials: &WorkloadCredentialRegistry,
) {
    let bootstrap_actors = program
        .principals
        .iter()
        .filter(|principal| principal.bootstrap_key)
        .map(|principal| principal.actor)
        .collect::<Vec<_>>();
    model.security.initialize(program, policy_version);
    for actor in bootstrap_actors {
        let credential = credentials.credential(actor.0, 0);
        model
            .security
            .register_known_key(super::ops::KeySel { actor, retired: 0 }, credential.key_id);
    }
}

tokio::task_local! {
    static REQUEST_AMBIGUITY_ALLOWED: bool;
    static REQUEST_IS_MUTATION: bool;
    static HTTP_FAULT_CONTEXT: Option<HttpFaultContext>;
    static WORKLOAD_REQUEST_ID: String;
}

#[derive(Clone)]
struct HttpFaultContext {
    scheduler: FaultScheduler,
    injector: HttpFaultRequestHandle,
    bookkeeping_store: ZeppelinStore,
    direct_base_url: String,
    proxy_base_url: String,
}

impl HttpFaultContext {
    fn for_node(&self, server: &FullTestServer) -> Self {
        let mut context = self.clone();
        context.direct_base_url = server.base_url.clone();
        context
    }
}

#[derive(Debug, Clone, Serialize)]
struct SeedProgressSnapshot {
    seed: u64,
    current_op: u64,
    runner_phase: String,
    active_event_ids: Vec<String>,
    pending_held_operation: Option<serde_json::Value>,
    deferred_operation_count: usize,
    quiet_drain_operation_count: usize,
    server_lifecycle: serde_json::Value,
    artifact_path: PathBuf,
    preserved_prefix: Option<String>,
}

#[derive(Clone)]
struct SeedWatchdogContext {
    progress: Arc<Mutex<SeedProgressSnapshot>>,
    server: Arc<Mutex<Option<FullTestServerWatchdogHandle>>>,
    scheduler: Arc<Mutex<Option<FaultScheduler>>>,
}

impl SeedWatchdogContext {
    fn new(seed: u64, artifact_path: PathBuf) -> Self {
        Self {
            progress: Arc::new(Mutex::new(SeedProgressSnapshot {
                seed,
                current_op: 0,
                runner_phase: "starting".to_string(),
                active_event_ids: Vec::new(),
                pending_held_operation: None,
                deferred_operation_count: 0,
                quiet_drain_operation_count: 0,
                server_lifecycle: json!({"state": "not-started"}),
                artifact_path,
                preserved_prefix: None,
            })),
            server: Arc::new(Mutex::new(None)),
            scheduler: Arc::new(Mutex::new(None)),
        }
    }

    fn register_scheduler(&self, scheduler: Option<&FaultScheduler>) {
        *self
            .scheduler
            .lock()
            .expect("seed watchdog scheduler mutex poisoned") = scheduler.cloned();
    }

    fn register_prefix(&self, prefix: &str) {
        self.progress
            .lock()
            .expect("seed watchdog progress mutex poisoned")
            .preserved_prefix = Some(prefix.to_string());
    }

    fn register_server(&self, server: &FullTestServer) {
        let handle = server.watchdog_handle();
        let lifecycle = handle.lifecycle_state();
        *self
            .server
            .lock()
            .expect("seed watchdog server mutex poisoned") = Some(handle);
        self.progress
            .lock()
            .expect("seed watchdog progress mutex poisoned")
            .server_lifecycle = lifecycle;
    }

    fn update(
        &self,
        phase: &str,
        op_index: u64,
        scheduler: Option<&FaultScheduler>,
        pending_held_op: Option<&PendingHeldOp>,
        deferred_operation_count: usize,
        quiet_drain_operation_count: usize,
    ) {
        let active_event_ids =
            scheduler.map_or_else(Vec::new, |scheduler| scheduler.active_event_ids(op_index));
        let pending_held_operation = pending_held_op.map(|pending| {
            json!({
                "event_id": pending.event_id,
                "operation_id": pending.op_index,
                "namespace": pending.namespace,
                "scheduled_release_op": pending.scheduled_release_op,
                "actual_release_op": pending.release_op,
                "release_cause": pending.release_cause,
            })
        });
        let server_lifecycle = self
            .server
            .lock()
            .expect("seed watchdog server mutex poisoned")
            .as_ref()
            .map_or_else(
                || json!({"state": "not-started"}),
                FullTestServerWatchdogHandle::lifecycle_state,
            );
        let previous = self.snapshot();
        *self
            .progress
            .lock()
            .expect("seed watchdog progress mutex poisoned") = SeedProgressSnapshot {
            seed: previous.seed,
            current_op: op_index,
            runner_phase: phase.to_string(),
            active_event_ids,
            pending_held_operation,
            deferred_operation_count,
            quiet_drain_operation_count,
            server_lifecycle,
            artifact_path: previous.artifact_path,
            preserved_prefix: previous.preserved_prefix,
        };
    }

    fn snapshot(&self) -> SeedProgressSnapshot {
        self.progress
            .lock()
            .expect("seed watchdog progress mutex poisoned")
            .clone()
    }

    fn mark_phase(&self, phase: &str) {
        let lifecycle = self
            .server
            .lock()
            .expect("seed watchdog server mutex poisoned")
            .as_ref()
            .map_or_else(
                || json!({"state": "not-started"}),
                FullTestServerWatchdogHandle::lifecycle_state,
            );
        let mut progress = self
            .progress
            .lock()
            .expect("seed watchdog progress mutex poisoned");
        progress.runner_phase = phase.to_string();
        progress.server_lifecycle = lifecycle;
    }

    fn refresh_server_lifecycle(&self) {
        let lifecycle = self
            .server
            .lock()
            .expect("seed watchdog server mutex poisoned")
            .as_ref()
            .map_or_else(
                || json!({"state": "not-started"}),
                FullTestServerWatchdogHandle::lifecycle_state,
            );
        self.progress
            .lock()
            .expect("seed watchdog progress mutex poisoned")
            .server_lifecycle = lifecycle;
    }

    fn begin_abort(&self) {
        if let Some(scheduler) = self
            .scheduler
            .lock()
            .expect("seed watchdog scheduler mutex poisoned")
            .as_ref()
        {
            scheduler.release_held_calls();
            if let Some(controller) = scheduler.process_controller() {
                controller.park_token.cancel();
            }
        }
        if let Some(server) = self
            .server
            .lock()
            .expect("seed watchdog server mutex poisoned")
            .as_ref()
        {
            server.begin_abort();
        }
        let mut progress = self
            .progress
            .lock()
            .expect("seed watchdog progress mutex poisoned");
        progress.runner_phase = "watchdog-expired".to_string();
        progress.server_lifecycle = self
            .server
            .lock()
            .expect("seed watchdog server mutex poisoned")
            .as_ref()
            .map_or_else(
                || json!({"state": "not-started"}),
                FullTestServerWatchdogHandle::lifecycle_state,
            );
    }

    async fn finish_cleanup(&self) -> Result<(), String> {
        let server = self
            .server
            .lock()
            .expect("seed watchdog server mutex poisoned")
            .clone();
        match server {
            Some(server) => server.finish_cleanup().await,
            None => Ok(()),
        }
    }
}

/// Owns the primary test server across simulated crashes.
///
/// Keeping the server in an explicit slot makes the lifecycle boundary visible:
/// crash recovery must consume and drop the old node before installing a
/// replacement, rather than evaluating a replacement while the old security
/// refresh tasks are still alive.
struct RestartableFullTestServer {
    server: Option<FullTestServer>,
    watchdog: Option<SeedWatchdogContext>,
}

impl RestartableFullTestServer {
    fn new(server: FullTestServer) -> Self {
        Self {
            server: Some(server),
            watchdog: None,
        }
    }

    fn new_with_watchdog(server: FullTestServer, watchdog: SeedWatchdogContext) -> Self {
        watchdog.register_server(&server);
        Self {
            server: Some(server),
            watchdog: Some(watchdog),
        }
    }

    fn take(&mut self) -> FullTestServer {
        if let Some(watchdog) = &self.watchdog {
            watchdog.mark_phase("crash-retirement");
        }
        self.server
            .take()
            .expect("primary test server must be present before lifecycle transition")
    }

    fn install(&mut self, replacement: FullTestServer) {
        assert!(
            self.server.is_none(),
            "replacement may only be installed after the old primary test server is dropped"
        );
        if let Some(watchdog) = &self.watchdog {
            watchdog.register_server(&replacement);
        }
        self.server = Some(replacement);
    }

    fn into_inner(mut self) -> FullTestServer {
        self.server
            .take()
            .expect("primary test server must be present before final shutdown")
    }
}

impl Deref for RestartableFullTestServer {
    type Target = FullTestServer;

    fn deref(&self) -> &Self::Target {
        self.server
            .as_ref()
            .expect("primary test server must be present while it is in use")
    }
}

impl DerefMut for RestartableFullTestServer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.server
            .as_mut()
            .expect("primary test server must be present while it is in use")
    }
}

#[derive(Clone)]
struct OpExecutionTarget {
    base_url: String,
    store: ZeppelinStore,
    clock: Clock,
    compactor: Arc<Compactor>,
    lease_manager: Arc<LeaseManager>,
    manifest_cache: Arc<ManifestCache>,
    namespace_manager: Arc<NamespaceManager>,
    encoder_provider: Arc<dyn MultiVectorEncoderProvider>,
    object_store_counter: Option<GetCounter>,
    security: Arc<SecurityKernel>,
    audit: zeppelin::security::AuditClient,
    audit_node_id: String,
    workload_credentials: WorkloadCredentialRegistry,
}

impl From<&FullTestServer> for OpExecutionTarget {
    fn from(server: &FullTestServer) -> Self {
        Self {
            base_url: server.base_url.clone(),
            store: server.store.clone(),
            clock: server.clock.clone(),
            compactor: Arc::clone(&server.compactor),
            lease_manager: Arc::clone(&server.lease_manager),
            manifest_cache: Arc::clone(&server.manifest_cache),
            namespace_manager: Arc::clone(&server.namespace_manager),
            encoder_provider: Arc::clone(&server.encoder_provider),
            object_store_counter: server.object_store_counter.clone(),
            security: Arc::clone(&server.security),
            audit: server.audit.clone(),
            audit_node_id: server.audit_node_id.clone(),
            workload_credentials: server.workload_credentials.clone(),
        }
    }
}

#[derive(Default)]
struct OperationalState {
    second_node: Option<FullTestServer>,
    next_target_node: u8,
    second_node_active: bool,
    second_node_ever_active: bool,
    writable_second_node_ever_active: bool,
    second_node_read_only: bool,
}

struct NodeCommandContext<'a> {
    scheduler: Option<&'a FaultScheduler>,
    store: &'a ZeppelinStore,
    shared_clock: Option<Clock>,
    operational_observer: Option<&'a OperationalStoreObserver>,
    require_compaction_evidence: bool,
    prefix: &'a str,
    config: &'a Config,
    admin_bearer: &'a str,
    disk_cache_max_bytes: u64,
    op_index: u64,
}

struct EnvironmentCommandContext<'a> {
    scheduler: Option<&'a FaultScheduler>,
    operational_observer: Option<&'a OperationalStoreObserver>,
    client: &'a Client,
    primary: &'a FullTestServer,
    model: &'a mut Model,
    op_index: u64,
}

#[derive(Default)]
struct EnvironmentCommandOutcome {
    crash: Option<CrashRequest>,
    violations: Vec<Violation>,
}

struct OperationalQueryResult {
    target_node: u8,
    namespace: String,
    exchange: RequestExchange,
}

async fn operational_query(
    client: Client,
    target_node: u8,
    namespace: String,
    base_url: String,
    body: serde_json::Value,
) -> OperationalQueryResult {
    let path = format!("/v1/namespaces/{namespace}/query");
    let exchange = request_exchange(
        &client,
        Method::POST,
        &format!("{base_url}{path}"),
        Some(body),
        true,
    )
    .await;
    OperationalQueryResult {
        target_node,
        namespace,
        exchange,
    }
}

fn operational_query_failure(
    event_id: &str,
    op_index: u64,
    result: &OperationalQueryResult,
    detail: impl Into<String>,
) -> Violation {
    Violation {
        id: ViolationId::I19CrashRecovery,
        op_index,
        namespace: result.namespace.clone(),
        detail: detail.into(),
        evidence: json!({
            "event_id": event_id,
            "node": result.target_node,
            "status": result.exchange.status,
            "response": result.exchange.response,
            "request_outcome": result.exchange.outcome.label(),
        }),
    }
}

impl OperationalState {
    fn record_second_node_started(&mut self) {
        assert!(
            !self.second_node_active,
            "second-node activity windows must not overlap"
        );
        self.second_node_active = true;
        self.second_node_ever_active = true;
        self.writable_second_node_ever_active = true;
        self.second_node_read_only = false;
    }

    fn record_read_only_node_started(&mut self) {
        assert!(
            !self.second_node_active,
            "second-node activity windows must not overlap"
        );
        self.second_node_active = true;
        self.second_node_ever_active = true;
        self.second_node_read_only = true;
    }

    fn record_second_node_stopped(&mut self) {
        assert!(
            self.second_node_active,
            "cannot stop a second-node activity window that is not active"
        );
        self.second_node_active = false;
        self.second_node_read_only = false;
    }

    fn second_node_active(&self) -> bool {
        debug_assert!(!self.second_node_active || self.second_node_ever_active);
        self.second_node_active
    }

    fn second_node_ever_active(&self) -> bool {
        self.second_node_ever_active
    }

    fn quiescent_vector_count_must_be_exact(&self) -> bool {
        self.writable_second_node_ever_active
    }

    fn generation_checkpoints_enabled(&self) -> bool {
        !self.second_node_active() || self.second_node_read_only
    }

    fn choose_target_node(&mut self) -> u8 {
        if !self.second_node_active() {
            return 0;
        }
        let selected = self.next_target_node;
        self.next_target_node ^= 1;
        selected
    }

    fn choose_target_node_for_op(&mut self, op: &Op) -> u8 {
        if matches!(op, Op::LateUpsert { .. } | Op::LateQuery { .. }) {
            return 0;
        }
        if self.second_node_read_only && !op.is_read_only_request() {
            return 0;
        }
        self.choose_target_node()
    }

    fn choose_read_target_node(&mut self) -> u8 {
        self.choose_target_node()
    }

    fn choose_write_target_node(&mut self) -> u8 {
        if self.second_node_read_only {
            0
        } else {
            self.choose_target_node()
        }
    }

    fn target<'a>(&'a self, primary: &'a FullTestServer, target_node: u8) -> &'a FullTestServer {
        match target_node {
            0 => primary,
            1 => self
                .second_node
                .as_ref()
                .expect("target node 1 requires an active second node"),
            invalid => panic!("target_node must be 0 or 1, got {invalid}"),
        }
    }

    async fn apply_node_commands(
        &mut self,
        commands: Vec<SchedulerCommand>,
        context: NodeCommandContext<'_>,
    ) -> Vec<SchedulerCommand> {
        let NodeCommandContext {
            scheduler,
            store,
            shared_clock,
            operational_observer,
            require_compaction_evidence,
            prefix,
            config,
            admin_bearer,
            disk_cache_max_bytes,
            op_index,
        } = context;
        let mut remaining = Vec::new();
        for command in commands {
            match command {
                SchedulerCommand::StartSecondNode { event_id, for_ops } => {
                    assert!(
                        self.second_node.is_none(),
                        "second-node windows must not overlap"
                    );
                    if require_compaction_evidence {
                        operational_observer
                            .expect("Ops compaction proof requires an operational store observer")
                            .arm_compaction_contention_window(&event_id, op_index);
                    }
                    let second_node_store = operational_observer.map_or_else(
                        || store.clone(),
                        |observer| operational_store_proxy(store, observer.clone(), 1),
                    );
                    self.second_node = Some(
                        start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
                            second_node_store,
                            Some(prefix.to_string()),
                            config.clone(),
                            true,
                            shared_clock.clone(),
                            disk_cache_max_bytes,
                            admin_bearer,
                        )
                        .await,
                    );
                    self.record_second_node_started();
                    self.next_target_node = 0;
                    record_operational_timeline(
                        scheduler,
                        event_id,
                        op_index,
                        format!("start second node for {for_ops} ops"),
                        FaultSemantics::WindowActive,
                        None,
                    );
                }
                SchedulerCommand::StopSecondNode { event_id } => {
                    let compaction_evidence = if require_compaction_evidence {
                        let evidence = operational_observer
                            .expect("Ops compaction proof requires an operational store observer")
                            .wait_for_compaction_evidence(Duration::from_secs(10))
                            .await;
                        assert_eq!(evidence.event_id, event_id);
                        assert!(
                            !evidence.namespace.is_empty(),
                            "two-node contention proof requires one common namespace"
                        );
                        assert_eq!(
                            evidence.attempted_nodes,
                            BTreeSet::from([0, 1]),
                            "both background workers must reach the lease rendezvous"
                        );
                        assert!(
                            evidence.lease_publications > 0,
                            "the two-node window must publish a real compaction lease"
                        );
                        assert!(
                            evidence.fenced_manifest_publications > 0,
                            "the two-node window must publish a fenced manifest"
                        );
                        assert!(
                            evidence.background_manifest_publications > 0,
                            "the two-node window must publish a fenced background manifest"
                        );
                        Some(evidence)
                    } else {
                        None
                    };
                    let server = self
                        .second_node
                        .take()
                        .expect("scheduled second-node stop requires an active server");
                    server.shutdown().await;
                    self.record_second_node_stopped();
                    record_operational_timeline(
                        scheduler,
                        event_id,
                        op_index,
                        "stop second node".to_string(),
                        FaultSemantics::WindowEnd,
                        compaction_evidence.map(|evidence| {
                            format!(
                                "namespace={}; lease_attempt_nodes=[0,1]; \
                                 lease_publication=true; fenced_manifest=true; \
                                 background_activity=true",
                                evidence.namespace
                            )
                        }),
                    );
                }
                SchedulerCommand::StartReadOnlyNode { event_id, for_ops } => {
                    assert!(
                        self.second_node.is_none(),
                        "second-node windows must not overlap"
                    );
                    let second_node_store = operational_observer.map_or_else(
                        || store.clone(),
                        |observer| operational_store_proxy(store, observer.clone(), 1),
                    );
                    self.second_node = Some(
                        start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
                            second_node_store,
                            Some(prefix.to_string()),
                            config.clone(),
                            false,
                            shared_clock.clone(),
                            disk_cache_max_bytes,
                            admin_bearer,
                        )
                        .await,
                    );
                    self.record_read_only_node_started();
                    self.next_target_node = 0;
                    record_operational_timeline(
                        scheduler,
                        event_id,
                        op_index,
                        format!("start read-only node for {for_ops} ops"),
                        FaultSemantics::WindowActive,
                        Some(
                            "writer_node=0; read_only_node=1; secondary_background_writer=false"
                                .to_string(),
                        ),
                    );
                }
                SchedulerCommand::StopReadOnlyNode { event_id } => {
                    assert!(
                        self.second_node_read_only,
                        "read-only stop requires a read-only secondary"
                    );
                    let server = self
                        .second_node
                        .take()
                        .expect("scheduled read-only stop requires an active server");
                    server.shutdown().await;
                    self.record_second_node_stopped();
                    record_operational_timeline(
                        scheduler,
                        event_id,
                        op_index,
                        "stop read-only node".to_string(),
                        FaultSemantics::WindowEnd,
                        Some(
                            "writer_node=0; read_only_node=1; secondary_background_writer=false; secondary_mutations=0"
                                .to_string(),
                        ),
                    );
                }
                SchedulerCommand::ResourceExhaustion {
                    event_id,
                    max_concurrent_queries,
                    disk_cache_max_bytes,
                } => {
                    record_operational_timeline(
                        scheduler,
                        event_id,
                        op_index,
                        format!(
                            "resource limits queries={max_concurrent_queries} \
                             disk_cache_bytes={disk_cache_max_bytes}"
                        ),
                        FaultSemantics::PreCall,
                        None,
                    );
                }
                other => remaining.push(other),
            }
        }
        remaining
    }

    async fn stop_second_node(&mut self, operational_observer: Option<&OperationalStoreObserver>) {
        if let Some(observer) = operational_observer {
            observer.cancel_compaction_contention_window();
        }
        if let Some(server) = self.second_node.take() {
            server.shutdown().await;
            self.record_second_node_stopped();
        }
    }

    async fn apply_environment_commands(
        &mut self,
        commands: Vec<SchedulerCommand>,
        context: EnvironmentCommandContext<'_>,
    ) -> EnvironmentCommandOutcome {
        let mut outcome = EnvironmentCommandOutcome::default();
        let EnvironmentCommandContext {
            scheduler,
            operational_observer,
            client,
            primary,
            model,
            op_index,
        } = context;
        for command in commands {
            match command {
                SchedulerCommand::PatchConfigDuringTraffic { event_id } => {
                    let Some(ns) = operational_namespace(model) else {
                        record_operational_timeline(
                            scheduler,
                            event_id,
                            op_index,
                            "patch config skipped: no live namespace".to_string(),
                            FaultSemantics::PreCall,
                            Some("no live namespace".to_string()),
                        );
                        continue;
                    };
                    let nlist = model.namespaces[&ns].spec.num_centroids;
                    let target_node = self.choose_write_target_node();
                    let base_url = self.target(primary, target_node).base_url.clone();
                    let path = format!("/v1/namespaces/{ns}/index_config");
                    let (status, _) = request_json(
                        client,
                        Method::PATCH,
                        &format!("{base_url}{path}"),
                        Some(json!({ "nlist": nlist })),
                    )
                    .await;
                    assert!(
                        (200..300).contains(&status),
                        "operational config patch failed for {ns}: {status}"
                    );
                    record_operational_timeline(
                        scheduler,
                        event_id,
                        op_index,
                        format!("patch config on node {target_node}"),
                        FaultSemantics::PreCall,
                        Some(format!("status={status}")),
                    );
                }
                SchedulerCommand::FillDiskCache { event_id } => {
                    let queries = operational_queries(model);
                    assert!(
                        !queries.is_empty(),
                        "fill disk cache requires a live modeled namespace"
                    );
                    const BURST_REQUESTS: usize = 8;
                    let mut tasks = tokio::task::JoinSet::new();
                    let mut task_metadata = Vec::with_capacity(BURST_REQUESTS);
                    for burst in 0..BURST_REQUESTS {
                        let (ns, body) = queries[burst % queries.len()].clone();
                        let target_node = self.choose_read_target_node();
                        let base_url = self.target(primary, target_node).base_url.clone();
                        let client = client.clone();
                        let task = tasks.spawn(operational_query(
                            client,
                            target_node,
                            ns.clone(),
                            base_url,
                            body,
                        ));
                        task_metadata.push((task.id(), target_node, ns));
                    }
                    let mut served = BTreeSet::new();
                    let mut completed = 0usize;
                    let mut successful = 0usize;
                    let mut load_shed = 0usize;
                    let mut storage_faulted = 0usize;
                    let mut crash_ambiguous = 0usize;
                    let controller = scheduler.and_then(FaultScheduler::process_controller);
                    let mut crash = None;
                    let read_partition_active = scheduler
                        .is_some_and(|scheduler| scheduler.global_read_partition_active(op_index));
                    while !tasks.is_empty() {
                        let joined = if crash.is_none() {
                            if let Some(controller) = controller.as_ref() {
                                tokio::select! {
                                    () = controller.crash_requested.notified() => {
                                        let requested = controller.take_request();
                                        controller.park_token.cancel();
                                        crash = Some(requested);
                                        continue;
                                    }
                                    joined = tasks.join_next_with_id() => joined,
                                }
                            } else {
                                tasks.join_next_with_id().await
                            }
                        } else {
                            tasks.join_next_with_id().await
                        };
                        let Some(joined) = joined else {
                            break;
                        };
                        let result = match joined {
                            Ok((task_id, result)) => {
                                task_metadata.retain(|(id, _, _)| *id != task_id);
                                result
                            }
                            Err(error) => {
                                let (_, target_node, namespace) = task_metadata
                                    .iter()
                                    .find(|(id, _, _)| *id == error.id())
                                    .cloned()
                                    .unwrap_or_else(|| {
                                        panic!(
                                            "operational query task {} had no request metadata",
                                            error.id()
                                        )
                                    });
                                task_metadata.retain(|(id, _, _)| *id != error.id());
                                let failed = OperationalQueryResult {
                                    target_node,
                                    namespace,
                                    exchange: ambiguous_exchange(
                                        0,
                                        AmbiguityReason::ConnectionError,
                                    ),
                                };
                                outcome.violations.push(operational_query_failure(
                                    &event_id,
                                    op_index,
                                    &failed,
                                    format!(
                                        "operational cache-fill child task failed while joining: \
                                         {error}"
                                    ),
                                ));
                                completed += 1;
                                continue;
                            }
                        };
                        let OperationalQueryResult {
                            target_node,
                            namespace: ns,
                            exchange:
                                RequestExchange {
                                    status,
                                    response,
                                    outcome: request_outcome,
                                },
                        } = &result;
                        let expected_storage_fault = read_partition_active
                            && is_expected_partitioned_cache_fill_response(*status, response);
                        let request_was_ambiguous =
                            matches!(request_outcome, OpOutcome::Ambiguous { .. });
                        let crash_affected =
                            crash.is_some() && (request_was_ambiguous || *status >= 500);
                        if !is_expected_cache_fill_response(*status, response)
                            && !expected_storage_fault
                            && !crash_affected
                        {
                            outcome.violations.push(operational_query_failure(
                                &event_id,
                                op_index,
                                &result,
                                format!(
                                    "unarmed operational cache-fill query failed for {ns}: \
                                     status={status} response={response}"
                                ),
                            ));
                        }
                        completed += 1;
                        if (200..300).contains(status) {
                            successful += 1;
                        } else if expected_storage_fault {
                            storage_faulted += 1;
                        } else if crash_affected {
                            crash_ambiguous += 1;
                        } else {
                            load_shed += 1;
                        }
                        served.insert(*target_node);
                    }
                    if crash.is_none() {
                        if let Some(requested) = controller
                            .as_ref()
                            .and_then(ProcessController::try_take_request)
                        {
                            controller
                                .as_ref()
                                .expect("taken process crash requires a controller")
                                .park_token
                                .cancel();
                            crash = Some(requested);
                        }
                    }
                    assert_eq!(
                        completed, BURST_REQUESTS,
                        "operational cache-fill burst did not complete every request"
                    );
                    record_operational_timeline(
                        scheduler,
                        event_id,
                        op_index,
                        "fill disk cache with eight concurrent queries".to_string(),
                        FaultSemantics::WindowEnd,
                        Some(format!(
                            "completed={completed} successful={successful} \
                             load_shed={load_shed} storage_faulted={storage_faulted} \
                             crash_ambiguous={crash_ambiguous} nodes={served:?}"
                        )),
                    );
                    assert!(
                        outcome.crash.is_none(),
                        "overlapping operational commands requested two process crashes"
                    );
                    outcome.crash = crash;
                }
                SchedulerCommand::DeleteNamespaceInFlight { event_id } => {
                    let operational_observer = operational_observer.expect(
                        "delete/upsert operational rendezvous requires an observed node store",
                    );
                    let generation_checkpoints_enabled = self.generation_checkpoints_enabled();
                    let Some(ns) = operational_namespace(model) else {
                        record_operational_timeline(
                            scheduler,
                            event_id,
                            op_index,
                            "delete race skipped: no live namespace".to_string(),
                            FaultSemantics::PreCall,
                            Some("no live namespace".to_string()),
                        );
                        continue;
                    };
                    let dims = model.namespaces[&ns].spec.dims;
                    let mut values = vec![0.0; dims];
                    values[0] = 1.0;
                    let upsert = Op::Upsert {
                        actor: ActorSel::ADMIN,
                        ns: ns.clone(),
                        vectors: vec![GenVector {
                            id: format!("operational-race-{op_index}"),
                            values,
                            attributes: None,
                        }],
                    };
                    let delete = Op::DeleteNamespace {
                        actor: ActorSel::ADMIN,
                        ns: ns.clone(),
                    };
                    let held = OpOutcome::Ambiguous {
                        reason: AmbiguityReason::HeldInFlight,
                        status: None,
                    };
                    model.apply_outcome(&upsert, &held, None, None, op_index);
                    model.apply_outcome(&delete, &held, None, None, op_index);

                    let upsert_node = self.choose_write_target_node();
                    let upsert_base_url = self.target(primary, upsert_node).base_url.clone();
                    let delete_node = self.choose_write_target_node();
                    let delete_base_url = self.target(primary, delete_node).base_url.clone();
                    let upsert_client = client.clone();
                    let upsert_ns = ns.clone();
                    let upsert_vectors = match &upsert {
                        Op::Upsert { vectors, .. } => vectors.clone(),
                        _ => unreachable!(),
                    };
                    operational_observer.arm_mutation_rendezvous(&event_id, op_index, &ns);
                    let pending_upsert = tokio::spawn(async move {
                        let path = format!("/v1/namespaces/{upsert_ns}/vectors");
                        request_exchange(
                            &upsert_client,
                            Method::POST,
                            &format!("{upsert_base_url}{path}"),
                            Some(json!({ "vectors": upsert_vectors })),
                            true,
                        )
                        .await
                    });
                    let entered = operational_observer
                        .wait_for_mutation_rendezvous(Duration::from_secs(5))
                        .await;
                    assert_eq!(entered.event_id, event_id);
                    assert_eq!(entered.op_index, op_index);
                    assert_eq!(entered.namespace, ns);
                    assert_eq!(entered.node, upsert_node);
                    assert!(
                        !pending_upsert.is_finished(),
                        "in-flight upsert escaped its WAL PUT rendezvous"
                    );
                    record_operational_timeline(
                        scheduler,
                        event_id.clone(),
                        op_index,
                        format!("upsert reached WAL PUT rendezvous on node {upsert_node}"),
                        FaultSemantics::WindowActive,
                        Some(
                            "barrier=wal_put_entered; delete_joined=false; \
                             barrier_released=false; upsert_joined=false"
                                .to_string(),
                        ),
                    );
                    let delete_path = format!("/v1/namespaces/{ns}");
                    let delete_exchange = request_exchange(
                        client,
                        Method::DELETE,
                        &format!("{delete_base_url}{delete_path}"),
                        None,
                        true,
                    )
                    .await;
                    let released = operational_observer.release_mutation_rendezvous(&event_id);
                    assert_eq!(released, entered);
                    let upsert_exchange = pending_upsert
                        .await
                        .expect("operational in-flight upsert task panicked");
                    let upsert_status = upsert_exchange.status;
                    let delete_status = delete_exchange.status;
                    model.apply_joined_outcome_with_generation_checkpoints(
                        &upsert,
                        &upsert_exchange.outcome,
                        None,
                        None,
                        op_index,
                        generation_checkpoints_enabled,
                    );
                    model.apply_joined_outcome_with_generation_checkpoints(
                        &delete,
                        &delete_exchange.outcome,
                        None,
                        None,
                        op_index,
                        generation_checkpoints_enabled,
                    );
                    record_operational_timeline(
                        scheduler,
                        event_id,
                        op_index,
                        format!(
                            "delete namespace with in-flight upsert \
                             upsert_node={upsert_node} delete_node={delete_node}"
                        ),
                        FaultSemantics::WindowEnd,
                        Some(format!(
                            "barrier=wal_put_entered; delete_joined=true; \
                             barrier_released=true; upsert_joined=true; \
                             upsert_status={upsert_status}; delete_status={delete_status}"
                        )),
                    );
                }
                other => panic!("node command was not enacted: {other:?}"),
            }
        }
        outcome
    }
}

fn operational_namespace(model: &Model) -> Option<String> {
    model
        .namespaces
        .iter()
        .find(|(_, namespace)| {
            !namespace
                .indeterminate_ns
                .iter()
                .any(|entry| matches!(entry, NsIndeterminate::MaybeDeletedNs))
        })
        .map(|(namespace, _)| namespace.clone())
}

fn operational_queries(model: &Model) -> Vec<(String, serde_json::Value)> {
    model
        .namespaces
        .iter()
        .map(|(namespace, state)| {
            let body = state.canonical_queries.first().map_or_else(
                || exhaustive_query_from_model(model, namespace).body,
                |query| query.body.clone(),
            );
            (namespace.clone(), body)
        })
        .collect()
}

fn is_expected_cache_fill_response(status: u16, response: &serde_json::Value) -> bool {
    if (200..300).contains(&status) {
        return true;
    }
    if status != StatusCode::SERVICE_UNAVAILABLE.as_u16() {
        return false;
    }
    let Some(object) = response.as_object() else {
        return false;
    };
    object.get("code").and_then(serde_json::Value::as_str) == Some("CONCURRENCY_LIMIT")
        && object.get("status").and_then(serde_json::Value::as_u64) == Some(u64::from(status))
        && object.get("retryable").and_then(serde_json::Value::as_bool) == Some(true)
        && object
            .get("error")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|error| !error.is_empty())
        && object
            .get("request_id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|request_id| !request_id.is_empty())
}

fn is_expected_partitioned_cache_fill_response(status: u16, response: &serde_json::Value) -> bool {
    if status != StatusCode::INTERNAL_SERVER_ERROR.as_u16() {
        return false;
    }
    let Some(object) = response.as_object() else {
        return false;
    };
    object.get("code").and_then(serde_json::Value::as_str) == Some("STORAGE_ERROR")
        && object.get("status").and_then(serde_json::Value::as_u64) == Some(u64::from(status))
        && object.get("retryable").and_then(serde_json::Value::as_bool) == Some(true)
        && object
            .get("error")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|error| !error.is_empty())
        && object
            .get("request_id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|request_id| !request_id.is_empty())
}

fn record_operational_timeline(
    scheduler: Option<&FaultScheduler>,
    event_id: String,
    op_index: u64,
    action: String,
    semantics: FaultSemantics,
    recovery: Option<String>,
) {
    let Some(scheduler) = scheduler else {
        return;
    };
    scheduler.record(TimelineEvent {
        event_id,
        op_index,
        wall_ms: scheduler.wall_ms(),
        boundary: Boundary::Runner,
        action,
        key: None,
        semantics,
        observed: ObservedResult::DefiniteApplied,
        recovery,
    });
}

fn disk_cache_max_bytes_for_schedule(scheduler: Option<&FaultScheduler>) -> u64 {
    scheduler
        .into_iter()
        .flat_map(|scheduler| &scheduler.schedule().events)
        .find_map(|event| match event.kind {
            FaultKind::ResourceExhaustion {
                disk_cache_max_bytes,
                ..
            } => Some(disk_cache_max_bytes),
            _ => None,
        })
        .unwrap_or(100 * 1024 * 1024)
}

fn requires_two_node_compaction_evidence(scheduler: Option<&FaultScheduler>) -> bool {
    requires_two_node_compaction_evidence_for_schedule(scheduler.map(FaultScheduler::schedule))
}

fn requires_two_node_compaction_evidence_for_schedule(schedule: Option<&FaultSchedule>) -> bool {
    schedule.is_some_and(|schedule| {
        let has_second_node = schedule
            .events
            .iter()
            .any(|event| matches!(event.kind, FaultKind::StartSecondNode { .. }));
        let bounded_ops_campaign = schedule.profile == FaultProfile::Ops
            && schedule
                .events
                .iter()
                .any(|event| matches!(event.kind, FaultKind::ResourceExhaustion { .. }));
        // The publication rendezvous is an Ops campaign proof. Full profiles
        // deliberately overlap families that may block either worker.
        has_second_node && bounded_ops_campaign
    })
}

fn requires_operational_store_observer(scheduler: Option<&FaultScheduler>) -> bool {
    scheduler.is_some_and(|scheduler| {
        scheduler.schedule().events.iter().any(|event| {
            matches!(
                event.kind,
                FaultKind::StartSecondNode { .. }
                    | FaultKind::PatchConfigDuringTraffic
                    | FaultKind::DeleteNamespaceInFlight
                    | FaultKind::FillDiskCache
                    | FaultKind::ResourceExhaustion { .. }
            )
        })
    })
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub seeds_run: u64,
    pub failed_seeds: u64,
    pub non_blocking_findings: u64,
    pub ops_total: u64,
    pub compactions_total: u64,
    pub background_compactions_total: u64,
    pub ops_per_sec: f64,
    pub coverage: Coverage,
}

#[derive(Debug)]
struct SeedOutcome {
    mode: RunMode,
    profile: Option<FaultProfile>,
    failed: bool,
    blocking_v1: bool,
    ops: u64,
    compactions: u64,
    background_compactions: u64,
    coverage: Coverage,
    violations: Vec<Violation>,
    wall_secs: f64,
    object_store: ObjectStorePhaseCensus,
    fired_faults: Vec<FiredFault>,
    repaired_terminal_lifecycle: bool,
    repaired_clone_publication: bool,
}

#[derive(Debug, Serialize)]
struct SeedWatchdogExpiration {
    progress: SeedProgressSnapshot,
    watchdog_timeout_ms: u128,
    owner_task_join: String,
    cleanup_timeout_ms: u128,
    cleanup_result: String,
}

enum OwnedSeedTask<T> {
    Completed(T),
    Expired(Box<SeedWatchdogExpiration>),
}

async fn run_owned_seed_task<F, T>(
    future: F,
    watchdog: SeedWatchdogContext,
    watchdog_timeout: Duration,
    cleanup_timeout: Duration,
) -> OwnedSeedTask<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let mut owner = tokio::spawn(future);
    match tokio::time::timeout(watchdog_timeout, &mut owner).await {
        Ok(result) => OwnedSeedTask::Completed(
            result
                .unwrap_or_else(|error| panic!("bounded seed task failed while joining: {error}")),
        ),
        Err(_) => {
            watchdog.begin_abort();
            owner.abort();
            let owner_task_join = match owner.await {
                Err(error) if error.is_cancelled() => "cancelled-and-joined".to_string(),
                Ok(_) => "completed-during-watchdog-cancellation".to_string(),
                Err(error) => format!("join-failed:{error}"),
            };

            let cleanup_watchdog = watchdog.clone();
            let mut cleanup = tokio::spawn(async move { cleanup_watchdog.finish_cleanup().await });
            let cleanup_result = match tokio::time::timeout(cleanup_timeout, &mut cleanup).await {
                Ok(Ok(Ok(()))) => "completed".to_string(),
                Ok(Ok(Err(error))) => format!("failed:{error}"),
                Ok(Err(error)) => format!("join-failed:{error}"),
                Err(_) => {
                    cleanup.abort();
                    let joined = cleanup.await;
                    format!("timed-out; abort_join={joined:?}")
                }
            };
            watchdog.refresh_server_lifecycle();
            OwnedSeedTask::Expired(Box::new(SeedWatchdogExpiration {
                progress: watchdog.snapshot(),
                watchdog_timeout_ms: watchdog_timeout.as_millis(),
                owner_task_join,
                cleanup_timeout_ms: cleanup_timeout.as_millis(),
                cleanup_result,
            }))
        }
    }
}

fn seed_watchdog_outcome(
    env: &RunnerEnv,
    artifacts: &RunArtifacts,
    seed: u64,
    mutation: Option<OracleMutation>,
    started: Instant,
    expiration: SeedWatchdogExpiration,
) -> SeedOutcome {
    let assignment = effective_seed_assignment(env.mode, env.profile, seed);
    let profile = scheduled_profile(assignment.profile);
    let mode = if profile.is_some() {
        RunMode::Chaos
    } else {
        assignment.mode
    };
    let evidence =
        serde_json::to_value(&expiration).expect("seed watchdog expiration must serialize");
    let violation = Violation {
        id: ViolationId::I19CrashRecovery,
        op_index: expiration.progress.current_op,
        namespace: expiration
            .progress
            .pending_held_operation
            .as_ref()
            .and_then(|pending| pending.get("namespace"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("_runner")
            .to_string(),
        detail: "per-seed watchdog expired before the runner reached a terminal outcome"
            .to_string(),
        evidence: evidence.clone(),
    };
    let backend = env
        .env_echo
        .get("TEST_BACKEND")
        .map(String::as_str)
        .unwrap_or("memory");
    let replay_max_ops = expiration
        .progress
        .current_op
        .checked_add(1)
        .expect("watchdog replay max-ops overflowed");
    artifacts.write_watchdog_failure(
        seed,
        &FailureManifest {
            seed,
            mode,
            op_index: expiration.progress.current_op,
            violations: vec![violation.clone()],
            preserved_prefix: expiration
                .progress
                .preserved_prefix
                .clone()
                .unwrap_or_else(|| "_runner-watchdog".to_string()),
            fault_plan: mutation.map(|mutation| mutation.key().to_string()),
            repro_cmd: format!(
                "TEST_BACKEND={backend} ZEPPELIN_ADVERSARIAL_REPLAY={} \
                 ZEPPELIN_ADVERSARIAL_MAX_OPS={replay_max_ops} \
                 cargo test --test adversarial_workload_tests replay_seed -- --ignored --nocapture",
                expiration.progress.artifact_path.display(),
            ),
            inspect_cmd: format!(
                "TEST_BACKEND={backend} ZEPPELIN_ADVERSARIAL_INSPECT={} \
                 cargo test --test adversarial_workload_tests inspect -- --ignored --nocapture",
                expiration.progress.artifact_path.display(),
            ),
        },
        &evidence,
    );

    SeedOutcome {
        mode,
        profile,
        failed: true,
        blocking_v1: true,
        ops: expiration.progress.current_op,
        compactions: 0,
        background_compactions: 0,
        coverage: Coverage::default(),
        violations: vec![violation],
        wall_secs: started.elapsed().as_secs_f64().max(0.001),
        object_store: ObjectStorePhaseCensus::default(),
        fired_faults: Vec::new(),
        repaired_terminal_lifecycle: false,
        repaired_clone_publication: false,
    }
}

async fn run_seed_bounded(
    env: &RunnerEnv,
    artifacts: &RunArtifacts,
    seed: u64,
    deadline: Instant,
    mutation: Option<OracleMutation>,
    selftest_probe: Option<OracleMutation>,
) -> SeedOutcome {
    let started = Instant::now();
    let artifact_path = artifacts.root().join(format!("seed-{seed}"));
    let watchdog = SeedWatchdogContext::new(seed, artifact_path.clone());
    let task_watchdog = watchdog.clone();
    let owned_env = env.clone();
    let owned_artifacts = artifacts.clone();
    let task = async move {
        Box::pin(run_seed_inner(
            &owned_env,
            &owned_artifacts,
            seed,
            deadline,
            mutation,
            selftest_probe,
            Some(&task_watchdog),
        ))
        .await
    };
    let expiration = match run_owned_seed_task(
        task,
        watchdog,
        SEED_WATCHDOG_TIMEOUT,
        SEED_WATCHDOG_CLEANUP_TIMEOUT,
    )
    .await
    {
        OwnedSeedTask::Completed(outcome) => return outcome,
        OwnedSeedTask::Expired(expiration) => *expiration,
    };
    seed_watchdog_outcome(env, artifacts, seed, mutation, started, expiration)
}

pub async fn run_smoke(env: RunnerEnv) -> RunSummary {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(env.seconds);
    let artifacts = RunArtifacts::create(&env);
    let artifact_root = artifacts.root().to_path_buf();
    let mut seed_reports = Vec::new();
    let mut summary = RunSummary {
        seeds_run: 0,
        failed_seeds: 0,
        non_blocking_findings: 0,
        ops_total: 0,
        compactions_total: 0,
        background_compactions_total: 0,
        ops_per_sec: 0.0,
        coverage: Coverage::default(),
    };

    for seed in &env.seeds {
        let outcome = run_seed_bounded(
            &env,
            &artifacts,
            *seed,
            deadline,
            env.selftest,
            env.selftest,
        )
        .await;
        summary.seeds_run += 1;
        summary.failed_seeds += u64::from(outcome.failed && outcome.blocking_v1);
        summary.non_blocking_findings += u64::from(outcome.failed && !outcome.blocking_v1);
        summary.ops_total += outcome.ops;
        summary.compactions_total += outcome.compactions;
        summary.background_compactions_total += outcome.background_compactions;
        summary.coverage.merge(&outcome.coverage);
        seed_reports.push(SeedReport {
            seed: *seed,
            mode: outcome.mode,
            profile: outcome.profile,
            dir: artifact_root.join(format!("seed-{seed}")),
            failed: outcome.failed,
            ops: outcome.ops,
            compactions: outcome.compactions,
            background_compactions: outcome.background_compactions,
            violations: outcome.violations,
            wall_secs: outcome.wall_secs,
            object_store: outcome.object_store,
            fired_faults: outcome.fired_faults,
        });
    }
    summary.ops_per_sec = summary.ops_total as f64 / started.elapsed().as_secs_f64().max(0.001);
    artifacts.write_report(&env, &seed_reports, &summary.coverage, false);
    summary
}

/// Returns the seed at `seed_index` in the deterministic overnight stream.
///
/// The configured prefix is emitted verbatim for artifact compatibility. The
/// remainder enumerates the smallest seeds not present in that prefix, which
/// keeps every emitted seed unique and advances through every Mixed-mode slot.
#[must_use]
fn overnight_seed(configured_seeds: &[u64], seed_index: usize) -> u64 {
    assert!(
        !configured_seeds.is_empty(),
        "overnight requires at least one configured seed"
    );
    let configured = configured_seeds.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(
        configured.len(),
        configured_seeds.len(),
        "overnight configured seeds must be unique"
    );

    if seed_index < configured_seeds.len() {
        return configured_seeds[seed_index];
    }

    let mut candidate = u64::try_from(seed_index - configured_seeds.len())
        .expect("overnight seed index does not fit in u64");
    for configured_seed in configured {
        if configured_seed > candidate {
            break;
        }
        candidate = candidate
            .checked_add(1)
            .expect("overnight seed space exhausted");
    }
    candidate
}

pub async fn run_overnight(env: RunnerEnv) -> RunSummary {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(env.seconds);
    let artifacts = RunArtifacts::create(&env);
    let artifact_root = artifacts.root().to_path_buf();
    let mut seed_reports = Vec::new();
    let mut summary = RunSummary {
        seeds_run: 0,
        failed_seeds: 0,
        non_blocking_findings: 0,
        ops_total: 0,
        compactions_total: 0,
        background_compactions_total: 0,
        ops_per_sec: 0.0,
        coverage: Coverage::default(),
    };
    let mut seed_index = 0usize;

    while Instant::now() < deadline || summary.seeds_run == 0 {
        let seed = overnight_seed(&env.seeds, seed_index);
        let outcome =
            run_seed_bounded(&env, &artifacts, seed, deadline, env.selftest, env.selftest).await;
        summary.seeds_run += 1;
        summary.failed_seeds += u64::from(outcome.failed && outcome.blocking_v1);
        summary.non_blocking_findings += u64::from(outcome.failed && !outcome.blocking_v1);
        summary.ops_total += outcome.ops;
        summary.compactions_total += outcome.compactions;
        summary.background_compactions_total += outcome.background_compactions;
        summary.coverage.merge(&outcome.coverage);
        seed_reports.push(SeedReport {
            seed,
            mode: outcome.mode,
            profile: outcome.profile,
            dir: artifact_root.join(format!("seed-{seed}")),
            failed: outcome.failed,
            ops: outcome.ops,
            compactions: outcome.compactions,
            background_compactions: outcome.background_compactions,
            violations: outcome.violations,
            wall_secs: outcome.wall_secs,
            object_store: outcome.object_store,
            fired_faults: outcome.fired_faults,
        });
        seed_index += 1;
    }

    summary.ops_per_sec = summary.ops_total as f64 / started.elapsed().as_secs_f64().max(0.001);
    if env.profile == Some(FaultProfile::Security) {
        for kind in SECURITY_OP_KINDS {
            let count = summary.coverage.op_counts.get(kind).copied().unwrap_or(0);
            assert!(
                count >= 5,
                "security overnight coverage floor requires {kind} >= 5, observed {count}"
            );
        }
        for oracle in ["I22", "I23", "I24", "I25", "I26", "I27", "I28"] {
            let count = summary
                .coverage
                .security_oracle_counts
                .get(oracle)
                .copied()
                .unwrap_or(0);
            assert!(
                count > 0,
                "security overnight scenario coverage requires {oracle}, observed {count}"
            );
        }
    }
    artifacts.write_report(&env, &seed_reports, &summary.coverage, true);
    summary
}

pub async fn replay_seed_from_env() {
    let env = RunnerEnv::from_env();
    let replay = std::env::var("ZEPPELIN_ADVERSARIAL_REPLAY")
        .map(PathBuf::from)
        .expect("ZEPPELIN_ADVERSARIAL_REPLAY must point at a seed artifact dir");
    let expected_failure = read_failure_manifest(&replay);
    let outcome = Box::pin(run_replay(&env, &replay)).await;

    if outcome.failed {
        if let Some(expected) = expected_failure {
            let actual = outcome
                .violations
                .first()
                .unwrap_or_else(|| panic!("replay failed without a recorded violation"));
            let expected_violation = expected
                .violations
                .first()
                .unwrap_or_else(|| panic!("failure.json had no violations"));
            assert_eq!(
                actual.op_index, expected_violation.op_index,
                "replay reproduced a violation at the wrong op index"
            );
            assert_eq!(
                actual.id, expected_violation.id,
                "replay reproduced the wrong violation id"
            );
            panic!(
                "replay reproduced {:?} at op {} in {}",
                actual.id,
                actual.op_index,
                replay.display()
            );
        }
        panic!(
            "replay produced unexpected violations: {:?}",
            outcome.violations
        );
    }

    if let Some(expected) = expected_failure {
        let limit = env.max_ops.unwrap_or(u64::MAX);
        let expected_index = expected
            .violations
            .first()
            .map_or(expected.op_index, |violation| violation.op_index);
        assert!(
            limit <= expected_index
                || outcome.repaired_terminal_lifecycle
                || outcome.repaired_clone_publication,
            "replay did not reproduce expected violation {:?} at op {}",
            expected.violations.first().map(|violation| violation.id),
            expected_index
        );
    }

    println!(
        "replay clean: dir={} ops={} compactions={} background_compactions={}",
        replay.display(),
        outcome.ops,
        outcome.compactions,
        outcome.background_compactions
    );
}

pub async fn inspect_from_env() {
    let target = std::env::var("ZEPPELIN_ADVERSARIAL_INSPECT")
        .expect("ZEPPELIN_ADVERSARIAL_INSPECT must be a seed dir or namespace prefix");
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let namespaces = inspect_namespaces(&store, &target).await;
    assert!(
        !namespaces.is_empty(),
        "inspect target {target:?} did not resolve to any namespaces"
    );
    let server =
        start_test_server_full(store.clone(), None, inspection_config(), false, None).await;
    println!("inspect server: {}", server.base_url);
    for ns in &namespaces {
        print_namespace_inspection(&store, ns).await;
    }
    let hold_secs = std::env::var("ZEPPELIN_ADVERSARIAL_INSPECT_HOLD_SECS")
        .ok()
        .map(|value| {
            value.parse::<u64>().unwrap_or_else(|error| {
                panic!("invalid ZEPPELIN_ADVERSARIAL_INSPECT_HOLD_SECS={value}: {error}")
            })
        })
        .unwrap_or(0);
    if hold_secs > 0 {
        println!("holding inspect server for {hold_secs}s");
        tokio::time::sleep(Duration::from_secs(hold_secs)).await;
    }
    if let Some(shutdown) = server.shutdown_compaction.as_ref() {
        let _ = shutdown.send(true);
    }
}

async fn run_replay(env: &RunnerEnv, replay: &Path) -> SeedOutcome {
    let seed_config = replay_seed_config(replay);
    let replay_mutation = env.selftest.or(seed_config.fault_plan);
    let replay_post_commit_selftest = matches!(
        seed_config.selftest_probe,
        Some(OracleMutation::PostCommitLostWrite | OracleMutation::IndetResolutionLie)
    );
    let scheduler = seed_config
        .fault_schedule
        .clone()
        .map(FaultScheduler::from_schedule);
    let test_clock = test_clock_for_scheduler(scheduler.as_ref());
    let mode = if scheduler.is_some() {
        RunMode::Chaos
    } else {
        effective_seed_mode(seed_config.mode, seed_config.seed)
    };
    let chaos_plan = if seed_config.chaos_plan.is_some() {
        seed_config.chaos_plan.clone()
    } else if mode == RunMode::Chaos && scheduler.is_none() {
        Some(
            seed_config
                .chaos_plan
                .clone()
                .unwrap_or_else(|| FaultPlan::for_seed(seed_config.seed)),
        )
    } else {
        None
    };
    let chaos_plan_json = chaos_plan
        .as_ref()
        .map(|plan| serde_json::to_value(plan).expect("FaultPlan must serialize"));
    let active_profile = scheduler
        .as_ref()
        .map(|scheduler| scheduler.schedule().profile)
        .or_else(|| chaos_plan.as_ref().map(|_| FaultProfile::LegacyChaos));
    let old_prefix = recorded_namespace_prefix(seed_config.seed, &seed_config.namespace_specs);
    let harness = TestHarness::new().await;
    let prefix = harness.prefix.clone();
    let (counted_backend, counter) = counting_store(&harness.store);
    let (legacy_instrumented_store, chaos_handle) =
        wrap_chaos_store(&counted_backend, chaos_plan.clone());
    if let Some(chaos) = &chaos_handle {
        chaos.disable();
    }
    let instrumented_store = scheduler
        .as_ref()
        .map_or(legacy_instrumented_store.clone(), |scheduler| {
            store_fault_proxy(&legacy_instrumented_store, scheduler.clone())
        });
    let store = instrumented_store;
    let require_compaction_evidence = requires_two_node_compaction_evidence(scheduler.as_ref());
    let operational_observer = requires_operational_store_observer(scheduler.as_ref())
        .then(OperationalStoreObserver::default);
    let primary_store = operational_observer.as_ref().map_or_else(
        || store.clone(),
        |observer| operational_store_proxy(&store, observer.clone(), 0),
    );
    let mut config = seed_config.config.clone();
    let specs = seed_config
        .namespace_specs
        .iter()
        .map(|(ns, spec)| (rewrite_prefix(ns, &old_prefix, &prefix), spec.clone()))
        .collect::<BTreeMap<_, _>>();
    apply_late_namespace_config(&mut config, &specs);
    let security_program = seed_config
        .security_program
        .as_ref()
        .map(|program| program.rewrite_namespace_prefix(&old_prefix, &prefix));
    let run_artifacts = RunArtifacts::create(env);
    let mut artifacts = run_artifacts.seed_with_security(
        seed_config.seed,
        &config,
        &specs,
        mode,
        replay_mutation.map(OracleMutation::key),
        seed_config.selftest_probe.map(OracleMutation::key),
        chaos_plan_json.as_ref(),
        scheduler.as_ref().map(FaultScheduler::schedule),
        security_program.as_ref(),
    );
    let disk_cache_max_bytes = disk_cache_max_bytes_for_schedule(scheduler.as_ref());
    let encoder_provider = late_encoder_provider(&config.mmli)
        .expect("adversarial replay encoder provider must construct");
    let mut server = RestartableFullTestServer::new(
        start_test_server_full_with_disk_cache_max_bytes_and_encoder_provider(
            primary_store,
            Some(prefix.clone()),
            config.clone(),
            mode == RunMode::Chaos,
            injected_clock(test_clock.as_ref()),
            disk_cache_max_bytes,
            encoder_provider,
            counter.clone(),
        )
        .await,
    );
    let bootstrapped_policy_version = if let Some(program) = &security_program {
        Some(bootstrap_security_program(&server, program).await)
    } else {
        None
    };
    let mut operational_state = OperationalState::default();
    let mut injector = if scheduler.is_some() {
        Some(start_http_fault_injector(&server.base_url).await)
    } else {
        None
    };
    let mut http_fault_context =
        scheduler
            .as_ref()
            .zip(injector.as_ref())
            .map(|(scheduler, injector)| HttpFaultContext {
                scheduler: scheduler.clone(),
                injector: injector.request_handle(),
                bookkeeping_store: harness.store.clone(),
                direct_base_url: server.base_url.clone(),
                proxy_base_url: injector.base_url(),
            });
    let client = adversarial_client(&server);
    if let Some(chaos) = &chaos_handle {
        chaos.enable();
    }
    let mut model = Model::default();
    if let (Some(program), Some(policy_version)) =
        (security_program.clone(), bootstrapped_policy_version)
    {
        initialize_security_model(
            &mut model,
            program,
            policy_version,
            &server.workload_credentials,
        );
    }
    let mut coverage = Coverage::default();
    let mut s3_tracker = S3Tracker::default();
    let mut corruption_tracker = CorruptionTracker::default();
    let mut created_namespaces = Vec::new();
    let mut background_compaction_starts = BTreeMap::new();
    let mut failed = false;
    let mut failure_violations = Vec::new();
    let mut compactions = 0u64;
    let mut pending_held_op = None;
    let started = Instant::now();
    let max_ops = env.max_ops.unwrap_or(u64::MAX);

    let source_records = read_ops(replay);
    let source_failure = read_failure_manifest(replay);
    let source_failure_before_quiet =
        source_failure_precedes_unrecorded_quiet_period(&source_records, source_failure.as_ref());
    let (exact_execution_trace, workload_records) = replay_workload_records(&source_records);
    let workload_record_count = workload_records.len();
    let records = workload_records
        .into_iter()
        .take(max_ops as usize)
        .collect::<Vec<_>>();
    let replayed_full_workload = records.len() == workload_record_count;
    let replayed_workload_count =
        u64::try_from(records.len()).expect("replayed workload count must fit in u64");
    let mut replay_op_index = 0u64;
    let mut quiet_drain_ops = VecDeque::new();
    for (record_position, source) in records.iter().cloned().enumerate() {
        let commands =
            advance_scheduled_faults(scheduler.as_ref(), test_clock.as_ref(), source.index);
        let enters_quiet_period =
            pending_held_op
                .as_ref()
                .is_some_and(|pending: &PendingHeldOp| {
                    pending.release_op <= source.index
                        && exact_execution_trace
                        && replayed_hold_releases_before_nominal(
                            scheduler
                                .as_ref()
                                .expect("recorded foreground hold requires a fault scheduler"),
                            pending,
                        )
                });
        if enters_quiet_period {
            assert_eq!(
                source.index, replay_op_index,
                "quiet-period replay drain changed its operation boundary"
            );
            for source in records.iter().skip(record_position).cloned() {
                assert_eq!(
                    source.execution.phase,
                    ExecutionPhase::DeferredDrain,
                    "records after a quiesced hold release must remain deferred drain"
                );
                assert!(
                    source.execution.hold.is_none(),
                    "deferred-drain replay record {} starts another hold",
                    source.index
                );
                let inject_post_commit_ack_loss = replay_post_commit_selftest
                    && source.status == 0
                    && source.outcome == "ambiguous:connection_error";
                let op = rewrite_replayed_op(&source.op, &old_prefix, &prefix);
                quiet_drain_ops.push_back(QuietDrainOp::Replay {
                    source: Box::new(source),
                    op,
                    inject_post_commit_ack_loss,
                });
            }
            break;
        }
        if pending_held_op
            .as_ref()
            .is_some_and(|pending: &PendingHeldOp| pending.release_op <= source.index)
        {
            let pending = pending_held_op
                .take()
                .expect("release-ready replayed held op disappeared");
            let step = finish_pending_held_op(
                pending,
                &client,
                &mut artifacts,
                &mut model,
                &mut coverage,
                &mut s3_tracker,
                &mut corruption_tracker,
                replay_mutation,
                mode,
            )
            .await;
            apply_step_bookkeeping(
                &step,
                &mut created_namespaces,
                &mut background_compaction_starts,
                &mut compactions,
            );
            if !step.violations.is_empty() {
                failed = true;
                failure_violations = step.violations;
                break;
            }
            if let Some(crash) = take_step_crash(&step, scheduler.as_ref()) {
                let scheduler = scheduler
                    .as_ref()
                    .expect("replayed held-call process crash requires a scheduler");
                let controller = scheduler
                    .process_controller()
                    .expect("replayed held-call process crash requires a controller");
                let recovery = restart_after_crash(
                    &mut server,
                    &controller,
                    scheduler,
                    &mut injector,
                    &mut http_fault_context,
                    &store,
                    &harness.store,
                    &prefix,
                    &config,
                    true,
                    &client,
                    &model,
                    source.index,
                    crash,
                )
                .await;
                if !recovery.is_empty() {
                    failed = true;
                    failure_violations = recovery;
                    break;
                }
            }
        }
        let remaining = operational_state
            .apply_node_commands(
                commands,
                NodeCommandContext {
                    scheduler: scheduler.as_ref(),
                    store: &store,
                    shared_clock: injected_clock(test_clock.as_ref()),
                    operational_observer: operational_observer.as_ref(),
                    require_compaction_evidence,
                    prefix: &prefix,
                    config: &config,
                    admin_bearer: &server.admin_bearer,
                    disk_cache_max_bytes,
                    op_index: source.index,
                },
            )
            .await;
        let environment = Box::pin(operational_state.apply_environment_commands(
            remaining,
            EnvironmentCommandContext {
                scheduler: scheduler.as_ref(),
                operational_observer: operational_observer.as_ref(),
                client: &client,
                primary: &server,
                model: &mut model,
                op_index: source.index,
            },
        ))
        .await;
        if !environment.violations.is_empty() {
            failed = true;
            failure_violations = environment.violations;
            break;
        }
        if let Some(crash) = environment.crash {
            let scheduler = scheduler
                .as_ref()
                .expect("replayed operational process crash requires a fault scheduler");
            let controller = scheduler
                .process_controller()
                .expect("replayed operational process crash requires a controller");
            let recovery = restart_after_crash(
                &mut server,
                &controller,
                scheduler,
                &mut injector,
                &mut http_fault_context,
                &store,
                &harness.store,
                &prefix,
                &config,
                true,
                &client,
                &model,
                source.index,
                crash,
            )
            .await;
            if !recovery.is_empty() {
                failed = true;
                failure_violations = recovery;
                break;
            }
        }
        let replay_lost_ack = replay_post_commit_selftest
            && source.status == 0
            && source.outcome == "ambiguous:connection_error";
        let op = rewrite_replayed_op(&source.op, &old_prefix, &prefix);
        if let Some(pending) = pending_held_op.as_ref() {
            if exact_execution_trace {
                assert!(
                    source.execution.hold.is_none(),
                    "replay record {} starts a second foreground hold",
                    source.index
                );
                assert!(
                    !op_conflicts_with_held_namespace(&op, &pending.namespace),
                    "replay record {} violates its recorded held-namespace isolation",
                    source.index
                );
            } else {
                assert!(
                    !op_conflicts_with_pending_hold(&op, source.index, scheduler.as_ref(), pending,),
                    "legacy replay op {} conflicts with the predicted foreground hold",
                    source.index
                );
            }
        }
        let target_server = operational_state.target(&server, source.target_node);
        let op_http_fault_context = http_fault_context
            .as_ref()
            .map(|context| context.for_node(target_server));
        let replay_phase = if exact_execution_trace {
            source.execution.phase
        } else {
            ExecutionPhase::Workload
        };
        let execution = if pending_held_op.is_some() {
            RecordedExecutionOutcome::Completed(Box::new(
                execute_recorded_op(
                    &client,
                    target_server,
                    &mut artifacts,
                    &mut model,
                    &mut coverage,
                    &mut s3_tracker,
                    &mut corruption_tracker,
                    &op,
                    source.index,
                    started,
                    replay_mutation,
                    mode,
                    replay_phase,
                    operational_state.generation_checkpoints_enabled(),
                    source.target_node,
                    op_http_fault_context.as_ref(),
                    replay_lost_ack,
                )
                .await,
            ))
        } else if exact_execution_trace {
            if let Some(recorded_hold) = &source.execution.hold {
                assert_eq!(
                    source.execution.phase,
                    ExecutionPhase::Workload,
                    "recorded foreground hold {} must originate in workload phase",
                    recorded_hold.event_id
                );
                let scheduler = scheduler
                    .as_ref()
                    .expect("recorded foreground hold requires a fault scheduler");
                match execute_hold_candidate(
                    scheduler,
                    ForegroundHold {
                        event_id: recorded_hold.event_id.clone(),
                        window_op: recorded_hold.window_op,
                        release_op: recorded_hold.actual_join_op,
                    },
                    client.clone(),
                    OpExecutionTarget::from(target_server),
                    op.clone(),
                    source.index,
                    started,
                    replay_mutation,
                    mode,
                    operational_state.generation_checkpoints_enabled(),
                    source.target_node,
                    op_http_fault_context.clone(),
                    replay_lost_ack,
                    corruption_tracker
                        .durably_tainted_keys(op.namespace())
                        .cloned(),
                    &mut model,
                )
                .await
                {
                    HoldCandidateOutcome::Held(mut pending) => {
                        pending.scheduled_release_op = recorded_hold
                            .scheduled_release_op
                            .unwrap_or(recorded_hold.actual_join_op);
                        pending.release_op = recorded_hold.actual_join_op;
                        pending.release_cause = recorded_hold.release_cause;
                        RecordedExecutionOutcome::Held(pending)
                    }
                    HoldCandidateOutcome::Completed(raw) => RecordedExecutionOutcome::Held(
                        preserve_recorded_hold_after_early_completion(
                            raw,
                            recorded_hold,
                            &op,
                            source.index,
                            replay_mutation,
                            &mut model,
                        ),
                    ),
                }
            } else {
                RecordedExecutionOutcome::Completed(Box::new(
                    execute_recorded_op(
                        &client,
                        target_server,
                        &mut artifacts,
                        &mut model,
                        &mut coverage,
                        &mut s3_tracker,
                        &mut corruption_tracker,
                        &op,
                        source.index,
                        started,
                        replay_mutation,
                        mode,
                        source.execution.phase,
                        operational_state.generation_checkpoints_enabled(),
                        source.target_node,
                        op_http_fault_context.as_ref(),
                        replay_lost_ack,
                    )
                    .await,
                ))
            }
        } else {
            execute_recorded_op_or_hold(
                scheduler.as_ref(),
                &client,
                target_server,
                &mut artifacts,
                &mut model,
                &mut coverage,
                &mut s3_tracker,
                &mut corruption_tracker,
                &op,
                source.index,
                started,
                replay_mutation,
                mode,
                operational_state.generation_checkpoints_enabled(),
                source.target_node,
                op_http_fault_context.as_ref(),
                replay_lost_ack,
            )
            .await
        };
        replay_op_index = source
            .index
            .checked_add(1)
            .expect("replayed operation cursor overflowed");
        let step = match execution {
            RecordedExecutionOutcome::Completed(step) => *step,
            RecordedExecutionOutcome::Held(pending) => {
                assert!(
                    !replay_lost_ack,
                    "replay cannot combine a held foreground op with a lost acknowledgement"
                );
                assert!(
                    pending_held_op.replace(pending).is_none(),
                    "replay attempted to track more than one held foreground op"
                );
                continue;
            }
        };
        let pending_crash = take_step_crash(&step, scheduler.as_ref());
        assert_eq!(
            step.post_commit_ack_lost, replay_lost_ack,
            "replay could not reproduce the recorded post-commit acknowledgement loss"
        );
        if matches!(op, Op::CreateNamespace { .. }) && (200..300).contains(&step.status) {
            let ns = op.namespace().to_string();
            note_background_compaction_namespace(&mut background_compaction_starts, &ns);
            created_namespaces.push(ns);
        }
        if let Op::CloneNamespace { target, .. } = &op {
            if (200..300).contains(&step.status) {
                note_background_compaction_namespace(&mut background_compaction_starts, target);
                created_namespaces.push(target.clone());
            }
        }
        if let Op::DeleteNamespace { ns, .. } = &op {
            if (200..300).contains(&step.status) {
                created_namespaces.retain(|created| created != ns);
            }
        }
        if matches!(
            &op,
            Op::CompactInline { .. } | Op::CompactEndpoint { .. } | Op::ProbeSandwich { .. }
        ) && (200..300).contains(&step.status)
        {
            compactions += 1;
        }
        if !step.violations.is_empty() {
            failed = true;
            failure_violations = step.violations;
            break;
        }
        if let Some(crash) = pending_crash {
            let scheduler = scheduler
                .as_ref()
                .expect("replayed process crash requires a scheduler");
            let controller = scheduler
                .process_controller()
                .expect("replayed process crash requires a controller");
            let recovery = restart_after_crash(
                &mut server,
                &controller,
                scheduler,
                &mut injector,
                &mut http_fault_context,
                &store,
                &harness.store,
                &prefix,
                &config,
                true,
                &client,
                &model,
                source.index,
                crash,
            )
            .await;
            if !recovery.is_empty() {
                failed = true;
                failure_violations = recovery;
                break;
            }
        }
    }

    let mut op_count = replay_op_index;
    let exact_quiescent_vector_count = operational_state.quiescent_vector_count_must_be_exact();
    let mut no_dual_writer_lease_hold = None;
    let object_store_in_run = object_store_breakdown(&counter);
    let quiet = QuietPeriod {
        client: &client,
        server: &mut server,
        scheduler: scheduler.as_ref(),
        test_clock: test_clock.as_ref(),
        injector: &mut injector,
        http_fault_context: &mut http_fault_context,
        chaos: chaos_handle.as_ref(),
        operational_state: &mut operational_state,
        operational_observer: operational_observer.as_ref(),
        pending_held_op: &mut pending_held_op,
        dual_writer_lease_hold: &mut no_dual_writer_lease_hold,
        initial_dual_writer_stale_fencing_token: None,
        artifacts: &mut artifacts,
        model: &mut model,
        coverage: &mut coverage,
        s3_tracker: &mut s3_tracker,
        corruption_tracker: &mut corruption_tracker,
        created_namespaces: &mut created_namespaces,
        background_compaction_starts: &mut background_compaction_starts,
        op_index: &mut op_count,
        compactions: &mut compactions,
        started,
        mutation: replay_mutation,
        mode,
        exact_vector_count: exact_quiescent_vector_count,
        verify: !failed && !source_failure_before_quiet,
        preserve_recorded_holds: true,
        prefix: &prefix,
        config: &config,
        disk_cache_max_bytes,
        drain_ops: &mut quiet_drain_ops,
    }
    .run()
    .await;
    replay_op_index = replay_op_index
        .checked_add(quiet.drained_ops)
        .expect("replayed workload cursor overflowed after deferred drain");
    let replayed_all_selected_records = replay_op_index == replayed_workload_count;
    if !failed {
        assert!(
            replayed_all_selected_records,
            "replay must advance through every selected workload record exactly once: \
             advanced={replay_op_index} selected={replayed_workload_count}"
        );
    }
    assert!(
        !quiet.post_commit_ack_lost,
        "replayed held foreground op unexpectedly lost its acknowledgement"
    );
    assert!(
        quiet.dual_writer_stale_fencing_token.is_none(),
        "ordinary replay unexpectedly produced a dual-writer fencing token"
    );
    if !quiet.violations.is_empty() {
        if !failed {
            failure_violations = quiet.violations;
        } else {
            failure_violations.extend(quiet.violations);
        }
        failed = true;
    }

    if replayed_all_selected_records {
        let replayed_records = read_ops(&artifacts.dir);
        let (_, replayed_workload) = replay_workload_records(&replayed_records);
        assert_eq!(
            u64::try_from(replayed_workload.len())
                .expect("replayed workload record count must fit in u64"),
            replayed_workload_count,
            "replay must execute every selected workload record exactly once"
        );
        assert_contiguous_record_indices(
            &replayed_workload,
            replayed_workload_count,
            "replayed workload trace",
        );
        if exact_execution_trace {
            assert_normalized_replay_structure(&records, &old_prefix, &replayed_workload, &prefix);
        }
    }

    let audit_store = server.store.clone();
    let audit_day = server.clock.now().date_naive();
    let audit_node_id = server.audit_node_id.clone();
    drop(client);
    server.into_inner().shutdown().await;
    if security_program.is_some() {
        let verification =
            zeppelin::security::verify_audit_day(&audit_store, audit_day, &audit_node_id)
                .await
                .unwrap_or_else(|error| {
                    panic!("signed audit-chain verification failed during replay: {error}")
                });
        coverage.record_security_oracle("I25");
        if !verification.valid {
            failed = true;
            failure_violations.push(Violation {
                id: ViolationId::I25AuditEvidence,
                op_index: op_count,
                namespace: "_audit".to_string(),
                detail: "signed audit day failed after graceful replay shutdown".to_string(),
                evidence: serde_json::to_value(verification)
                    .expect("audit verification report must serialize"),
            });
        }
    }

    artifacts.write_model_final(&model);
    artifacts.write_s3_final(&store, &created_namespaces).await;
    artifacts.write_coverage(&coverage);
    let fired_faults = chaos_handle
        .as_ref()
        .map(ChaosHandle::fired)
        .unwrap_or_default();
    if active_profile == Some(FaultProfile::LegacyChaos) {
        artifacts.write_faults(&fired_faults);
    }
    let mut timeline = scheduler
        .as_ref()
        .map(FaultScheduler::timeline)
        .unwrap_or_default();
    timeline.extend(quiet.timeline);
    artifacts.write_timeline(&timeline);
    if exact_execution_trace
        && replayed_full_workload
        && source_records.len() > workload_record_count
    {
        let replay_records = read_ops(&artifacts.dir);
        let terminal_lifecycle_names = terminal_lifecycle_resolution_names(&artifacts.dir);
        assert_normalized_full_replay_structure(
            &source_records,
            &old_prefix,
            &replay_records,
            &prefix,
            source_failure.as_ref(),
            &terminal_lifecycle_names,
        );
    }
    let terminal_lifecycle_names = terminal_lifecycle_resolution_names(&artifacts.dir);
    let repaired_terminal_lifecycle = exact_execution_trace
        && replayed_full_workload
        && source_failure.as_ref().is_some_and(|failure| {
            failure.violations.iter().any(|violation| {
                violation.id == ViolationId::I16Quiescence
                    && terminal_lifecycle_names.contains(&rewrite_prefix(
                        &violation.namespace,
                        &old_prefix,
                        &prefix,
                    ))
            })
        });
    let non_applied_clone_targets = non_applied_clone_resolution_targets(&artifacts.dir);
    let repaired_clone_publication = exact_execution_trace
        && replayed_full_workload
        && source_failure.as_ref().is_some_and(|failure| {
            failure.violations.iter().any(|violation| {
                violation.id == ViolationId::I16Quiescence
                    && non_applied_clone_targets.contains(&rewrite_prefix(
                        &violation.namespace,
                        &old_prefix,
                        &prefix,
                    ))
            })
        });
    let object_store_total = object_store_breakdown(&counter);
    let object_store = ObjectStorePhaseCensus {
        quiet_period: object_store_delta(&object_store_total, &object_store_in_run),
        in_run: object_store_in_run,
    };
    let background_compactions = background_compactions_since(&background_compaction_starts);
    if should_cleanup(env.preserve, failed) {
        for ns in &created_namespaces {
            cleanup_ns(&store, ns).await;
        }
        harness.cleanup().await;
    } else {
        println!("preserved replay prefix {prefix}");
    }
    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    let blocking_v1 = replay_mutation.is_none()
        && scheduler
            .as_ref()
            .is_none_or(|scheduler| scheduler.schedule().blocks_v1());
    SeedOutcome {
        mode,
        profile: active_profile,
        failed,
        blocking_v1,
        ops: op_count,
        compactions,
        background_compactions,
        coverage,
        violations: failure_violations,
        wall_secs: elapsed,
        object_store,
        fired_faults,
        repaired_terminal_lifecycle,
        repaired_clone_publication,
    }
}

#[derive(Debug, serde::Deserialize)]
struct ReplaySeedConfig {
    seed: u64,
    mode: RunMode,
    #[serde(default)]
    fault_plan: Option<OracleMutation>,
    #[serde(default)]
    selftest_probe: Option<OracleMutation>,
    #[serde(default)]
    chaos_plan: Option<FaultPlan>,
    #[serde(default)]
    fault_schedule: Option<FaultSchedule>,
    #[serde(default)]
    security_program: Option<SecurityProgramConfig>,
    config: Config,
    namespace_specs: BTreeMap<String, NamespaceSpec>,
}

fn replay_seed_config(path: &Path) -> ReplaySeedConfig {
    let mut artifact = read_seed_config(path);
    if let Some(config) = artifact
        .get_mut("config")
        .and_then(serde_json::Value::as_object_mut)
    {
        config.entry("security").or_insert_with(|| {
            serde_json::to_value(zeppelin::config::SecurityConfig::default())
                .expect("default replay security config must serialize")
        });
    }
    serde_json::from_value(artifact)
        .unwrap_or_else(|error| panic!("failed to parse replay seed config: {error}"))
}

fn read_failure_manifest(path: &Path) -> Option<FailureManifest> {
    let failure_path = path.join("failure.json");
    if !failure_path.exists() {
        return None;
    }
    let bytes = fs::read(&failure_path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", failure_path.display()));
    Some(serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!(
            "failed to parse failure manifest {}: {error}",
            failure_path.display()
        )
    }))
}

fn recorded_namespace_prefix(
    seed: u64,
    namespace_specs: &BTreeMap<String, NamespaceSpec>,
) -> String {
    let marker = format!("-adv-{seed}-");
    for ns in namespace_specs.keys() {
        if let Some(index) = ns.find(&marker) {
            return ns[..index].to_string();
        }
    }
    let first = namespace_specs
        .keys()
        .next()
        .unwrap_or_else(|| panic!("replay config contained no namespace specs"));
    first
        .rsplit_once('-')
        .map_or_else(|| first.clone(), |(prefix, _)| prefix.to_string())
}

fn assert_contiguous_record_indices(records: &[OpRecord], expected_count: u64, context: &str) {
    assert_eq!(
        u64::try_from(records.len()).expect("record count must fit in u64"),
        expected_count,
        "{context} record count changed"
    );
    for (expected, record) in records.iter().enumerate() {
        assert_eq!(
            record.index,
            u64::try_from(expected).expect("record index must fit in u64"),
            "{context} contains a non-contiguous operation index"
        );
    }
}

#[derive(Debug, Clone, Serialize)]
struct WorkloadAccountingSnapshot {
    selected_operation_ids: Vec<u64>,
    completed_operation_ids: Vec<u64>,
    held_operation_ids: Vec<u64>,
    quiet_drain_operation_ids: Vec<u64>,
}

#[derive(Serialize)]
struct WorkloadAccountingArtifact {
    pre_quiet: WorkloadAccountingSnapshot,
    post_quiet: WorkloadAccountingSnapshot,
}

fn assert_workload_accounting_bijection(accounting: &WorkloadAccountingSnapshot, context: &str) {
    let selected = accounting
        .selected_operation_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        selected.len(),
        accounting.selected_operation_ids.len(),
        "{context} selected operation ids contain duplicates"
    );

    let mut accounted = BTreeSet::new();
    for (state, operation_ids) in [
        ("completed", &accounting.completed_operation_ids),
        ("held", &accounting.held_operation_ids),
        ("quiet-drain", &accounting.quiet_drain_operation_ids),
    ] {
        for operation_id in operation_ids {
            assert!(
                accounted.insert(*operation_id),
                "{context} operation {operation_id} appears in more than one accounting state \
                 (last state: {state})"
            );
        }
    }
    assert_eq!(
        accounted, selected,
        "{context} generated, held, quiet-drained, and completed operation ids are not bijective"
    );
}

fn assert_pre_quiet_workload_accounting(accounting: &WorkloadAccountingSnapshot, context: &str) {
    assert_workload_accounting_bijection(accounting, context);
    if accounting.held_operation_ids.is_empty() && accounting.quiet_drain_operation_ids.is_empty() {
        assert_eq!(
            accounting.completed_operation_ids.len(),
            accounting.selected_operation_ids.len(),
            "every selected workload operation must complete exactly once"
        );
    }
}

fn workload_accounting_snapshot(
    selected_count: u64,
    completed_operation_ids: Vec<u64>,
    held_operation_id: Option<u64>,
    quiet_drain_start: u64,
    quiet_drain_count: u64,
) -> WorkloadAccountingSnapshot {
    let quiet_drain_end = quiet_drain_start
        .checked_add(quiet_drain_count)
        .expect("quiet-drain operation id range overflowed");
    assert!(
        quiet_drain_end <= selected_count,
        "quiet-drain operation ids exceed the selected workload range"
    );
    WorkloadAccountingSnapshot {
        selected_operation_ids: (0..selected_count).collect(),
        completed_operation_ids,
        held_operation_ids: held_operation_id.into_iter().collect(),
        quiet_drain_operation_ids: (quiet_drain_start..quiet_drain_end).collect(),
    }
}

fn write_workload_accounting_artifact(
    seed_dir: &Path,
    pre_quiet: WorkloadAccountingSnapshot,
    post_quiet: WorkloadAccountingSnapshot,
) {
    let artifact = WorkloadAccountingArtifact {
        pre_quiet,
        post_quiet,
    };
    let encoded =
        serde_json::to_vec_pretty(&artifact).expect("workload accounting artifact must serialize");
    fs::write(seed_dir.join("workload-accounting.json"), encoded)
        .expect("failed to write workload-accounting.json");
}

fn replay_workload_records(records: &[OpRecord]) -> (bool, Vec<OpRecord>) {
    let legacy_count = records
        .iter()
        .filter(|record| record.execution.phase == ExecutionPhase::Legacy)
        .count();
    assert!(
        legacy_count == 0 || legacy_count == records.len(),
        "replay artifact mixes legacy and explicit execution metadata"
    );
    if legacy_count > 0 || records.is_empty() {
        assert_contiguous_record_indices(
            records,
            u64::try_from(records.len()).expect("legacy record count must fit in u64"),
            "legacy replay source",
        );
        return (false, records.to_vec());
    }

    let mut entered_deferred_drain = false;
    let mut entered_quiescence = false;
    let mut workload = Vec::new();
    for record in records {
        match record.execution.phase {
            ExecutionPhase::Legacy => unreachable!("legacy trace handled above"),
            ExecutionPhase::Workload => {
                assert!(
                    !entered_deferred_drain && !entered_quiescence,
                    "workload record {} appears after deferred drain or quiescence began",
                    record.index
                );
                workload.push(record.clone());
            }
            ExecutionPhase::DeferredDrain => {
                assert!(
                    !entered_quiescence,
                    "deferred-drain record {} appears after quiescence verification began",
                    record.index
                );
                assert!(
                    record.execution.hold.is_none(),
                    "deferred-drain record {} cannot start a foreground hold",
                    record.index
                );
                entered_deferred_drain = true;
                workload.push(record.clone());
            }
            ExecutionPhase::Quiescence => {
                entered_quiescence = true;
                assert!(
                    record.execution.hold.is_none(),
                    "quiescence record {} cannot carry a foreground hold",
                    record.index
                );
            }
        }
    }
    assert_contiguous_record_indices(
        &workload,
        u64::try_from(workload.len()).expect("workload record count must fit in u64"),
        "explicit replay workload source",
    );
    (true, workload)
}

fn normalized_op_execution_structure(records: &[OpRecord], prefix: &str) -> Vec<serde_json::Value> {
    records
        .iter()
        .map(|record| {
            let mut op = serde_json::to_value(&record.op).expect("Op must serialize for replay");
            rewrite_json_strings(&mut op, prefix, "<run-prefix>");
            json!({
                "index": record.index,
                "op": op,
                "target_node": record.target_node,
                "execution": record.execution,
            })
        })
        .collect()
}

fn assert_normalized_replay_structure(
    source: &[OpRecord],
    source_prefix: &str,
    replay: &[OpRecord],
    replay_prefix: &str,
) {
    assert_eq!(
        replay.len(),
        source.len(),
        "replay record count differs from its explicit source trace"
    );
    assert_eq!(
        normalized_op_execution_structure(replay, replay_prefix),
        normalized_op_execution_structure(source, source_prefix),
        "replay op and execution structure differs from its explicit source trace"
    );
}

fn source_ends_at_quiet_failure(source: &[OpRecord], failure: Option<&FailureManifest>) -> bool {
    let Some(failure) = failure else {
        return false;
    };
    let Some(first_quiet) = source
        .iter()
        .find(|record| record.execution.phase == ExecutionPhase::Quiescence)
    else {
        return false;
    };
    let Some(last) = source.last() else {
        return false;
    };
    let after_last = last
        .index
        .checked_add(1)
        .expect("source replay trace index overflowed");

    !failure.violations.is_empty()
        && failure.op_index >= first_quiet.index
        && matches!(failure.op_index, boundary if boundary == last.index || boundary == after_last)
}

fn source_failure_precedes_unrecorded_quiet_period(
    source: &[OpRecord],
    failure: Option<&FailureManifest>,
) -> bool {
    let Some(failure) = failure else {
        return false;
    };
    let Some(last) = source.last() else {
        return false;
    };
    if source.iter().any(|record| {
        !matches!(
            record.execution.phase,
            ExecutionPhase::Workload | ExecutionPhase::DeferredDrain
        )
    }) {
        return false;
    }
    let after_last = last
        .index
        .checked_add(1)
        .expect("source replay trace index overflowed");

    !failure.violations.is_empty()
        && matches!(failure.op_index, boundary if boundary == last.index || boundary == after_last)
}

fn assert_normalized_full_replay_structure(
    source: &[OpRecord],
    source_prefix: &str,
    replay: &[OpRecord],
    replay_prefix: &str,
    source_failure: Option<&FailureManifest>,
    terminal_lifecycle_names: &BTreeSet<String>,
) {
    if !source_ends_at_quiet_failure(source, source_failure) {
        assert_normalized_replay_structure(source, source_prefix, replay, replay_prefix);
        return;
    }

    assert_contiguous_record_indices(
        source,
        u64::try_from(source.len()).expect("source trace length must fit in u64"),
        "quiet-failure replay source",
    );
    assert_contiguous_record_indices(
        replay,
        u64::try_from(replay.len()).expect("replay trace length must fit in u64"),
        "repaired quiet-failure replay",
    );

    let terminal_subject = source_failure
        .into_iter()
        .flat_map(|failure| &failure.violations)
        .find(|violation| violation.id == ViolationId::I16Quiescence)
        .and_then(|violation| {
            let replay_namespace =
                rewrite_prefix(&violation.namespace, source_prefix, replay_prefix);
            terminal_lifecycle_names
                .contains(&replay_namespace)
                .then_some((violation.namespace.as_str(), replay_namespace))
        });
    if let Some((source_namespace, replay_namespace)) = terminal_subject {
        let divergence = source
            .iter()
            .position(|record| {
                record.execution.phase == ExecutionPhase::Quiescence
                    && record.op.namespace() == source_namespace
            })
            .expect("terminal lifecycle failure omitted its quiet namespace operation");
        assert!(
            matches!(source[divergence].op, Op::CompactInline { .. }),
            "terminal lifecycle divergence must begin at forced compaction"
        );
        assert!(
            replay.len() >= divergence,
            "terminal lifecycle replay stopped before its source divergence"
        );
        assert_eq!(
            normalized_op_execution_structure(&replay[..divergence], replay_prefix),
            normalized_op_execution_structure(&source[..divergence], source_prefix),
            "terminal lifecycle replay changed the trace before its typed disposition"
        );
        assert!(
            replay[divergence..].iter().all(|record| {
                record.execution.phase == ExecutionPhase::Quiescence
                    && record.execution.hold.is_none()
                    && record.op.namespace() != replay_namespace
            }),
            "terminal lifecycle replay retained the disposed namespace or appended non-quiescence work"
        );
        return;
    }

    assert!(
        replay.len() >= source.len(),
        "repaired quiet-failure replay stopped before its source failure boundary"
    );
    assert_eq!(
        normalized_op_execution_structure(&replay[..source.len()], replay_prefix),
        normalized_op_execution_structure(source, source_prefix),
        "repaired quiet-failure replay changed its source trace prefix"
    );
    assert!(
        replay[source.len()..].iter().all(|record| {
            record.execution.phase == ExecutionPhase::Quiescence && record.execution.hold.is_none()
        }),
        "repaired quiet-failure replay appended non-quiescence work"
    );
}

fn terminal_lifecycle_resolution_names(seed_dir: &Path) -> BTreeSet<String> {
    let path = seed_dir.join("resolutions.json");
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return BTreeSet::new(),
        Err(error) => panic!(
            "failed to read lifecycle resolutions {}: {error}",
            path.display()
        ),
    };
    let resolutions: Vec<serde_json::Value> =
        serde_json::from_slice(&bytes).unwrap_or_else(|error| {
            panic!(
                "failed to decode lifecycle resolutions {}: {error}",
                path.display()
            )
        });
    resolutions
        .into_iter()
        .filter_map(|resolution| {
            let terminal = resolution["effect"] == "maybe_deleted_namespace"
                && resolution["resolved"] == "applied"
                && matches!(
                    resolution["lifecycle_disposition"].as_str(),
                    Some("absent" | "deleting" | "deletion_fenced")
                );
            terminal.then(|| {
                resolution["namespace"]
                    .as_str()
                    .expect("terminal lifecycle resolution omitted namespace")
                    .to_string()
            })
        })
        .collect()
}

fn non_applied_clone_resolution_targets(seed_dir: &Path) -> BTreeSet<String> {
    let path = seed_dir.join("resolutions.json");
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return BTreeSet::new(),
        Err(error) => panic!(
            "failed to read clone resolutions {}: {error}",
            path.display()
        ),
    };
    let resolutions: Vec<serde_json::Value> =
        serde_json::from_slice(&bytes).unwrap_or_else(|error| {
            panic!(
                "failed to decode clone resolutions {}: {error}",
                path.display()
            )
        });
    resolutions
        .into_iter()
        .filter_map(|resolution| {
            let disproved = resolution["effect"] == "maybe_cloned"
                && resolution["resolved"] == "not_applied"
                && matches!(
                    resolution["publication_disposition"].as_str(),
                    Some("candidate_mismatch" | "manifest_absent")
                );
            disproved.then(|| {
                resolution["target"]
                    .as_str()
                    .expect("non-applied clone resolution omitted target")
                    .to_string()
            })
        })
        .collect()
}

fn rewrite_prefix(value: &str, old_prefix: &str, new_prefix: &str) -> String {
    value.strip_prefix(old_prefix).map_or_else(
        || value.to_string(),
        |suffix| format!("{new_prefix}{suffix}"),
    )
}

fn effective_seed_mode(mode: RunMode, seed: u64) -> RunMode {
    effective_seed_assignment(mode, None, seed).mode
}

fn reproduction_environment(
    backend: &str,
    mutation: Option<OracleMutation>,
    profile: Option<FaultProfile>,
) -> String {
    let mut environment = format!("TEST_BACKEND={backend}");
    if let Some(mutation) = mutation {
        environment.push_str(&format!(
            " ZEPPELIN_ADVERSARIAL_SELFTEST={}",
            mutation.key()
        ));
    }
    if let Some(profile) = profile {
        environment.push_str(&format!(
            " ZEPPELIN_ADVERSARIAL_PROFILE={}",
            profile.as_env()
        ));
    }
    environment
}

fn config_for_mode(mode: RunMode, seed: u64, schedule: Option<&FaultSchedule>) -> Config {
    let mut config = deterministic_config();
    let profile = schedule.map(|schedule| schedule.profile);
    if mode == RunMode::Chaos {
        config.cache.manifest_cache_ttl_ms = 500;
        config.compaction.interval_secs = 2 + (seed % 4);
        config.gc.compaction_upload_window_secs = 2;
    }
    if matches!(
        profile,
        Some(FaultProfile::Clock | FaultProfile::SupportedFull | FaultProfile::Full)
    ) {
        config.cache.namespace_registry_ttl_ms = 500;
        config.compaction.interval_secs = 2;
        config.compaction.lease_duration_secs = 10;
        config.gc.horizon_secs = 30;
        config.gc.allow_unsafe_short_horizon = true;
    }
    if matches!(
        profile,
        Some(
            FaultProfile::Ops
                | FaultProfile::SupportedFull
                | FaultProfile::FutureArchitecture
                | FaultProfile::Full
        )
    ) {
        config.server.max_concurrent_queries = 1;
        config.cache.memory_cache_max_mb = 1;
    }
    if requires_two_node_compaction_evidence_for_schedule(schedule) {
        // Keep maintenance frequent enough for both real background workers
        // to reach the proof rendezvous, but never run an immediately-ready
        // loop. Retaining no replaced descriptors keeps the manifest's
        // aggregate vector count exact for this quiescence profile.
        config.compaction.interval_secs = 1;
        config.compaction.max_wal_fragments_before_compact = 2;
        config.compaction.max_old_segments = 0;
    }
    if mode == RunMode::Chaos {
        // Background GC overlaps foreground WAL publication in chaos runs. A
        // zero horizon can delete a newly listed WAL between the sweep's
        // manifest re-read and the writer's successful manifest CAS.
        config.gc.horizon_secs = config
            .gc_horizon_floor_secs()
            .expect("adversarial GC horizon floor must not overflow");
        config.gc.allow_unsafe_short_horizon = false;
    }
    config
}

fn wrap_chaos_store(
    store: &zeppelin::storage::ZeppelinStore,
    plan: Option<FaultPlan>,
) -> (zeppelin::storage::ZeppelinStore, Option<ChaosHandle>) {
    if let Some(plan) = plan {
        let (store, handle) = chaos_store(store, plan);
        (store, Some(handle))
    } else {
        (store.clone(), None)
    }
}

fn scheduled_profile(profile: Option<FaultProfile>) -> Option<FaultProfile> {
    profile.filter(|profile| {
        !matches!(
            profile,
            FaultProfile::LegacyChaos | FaultProfile::Branching | FaultProfile::Late
        )
    })
}

fn test_clock_for_scheduler(scheduler: Option<&FaultScheduler>) -> Option<Arc<TestClock>> {
    scheduler
        .is_some_and(|scheduler| {
            scheduler.schedule().events.iter().any(|event| {
                matches!(
                    event.kind,
                    FaultKind::ClockJump { .. }
                        | FaultKind::ClockFreeze { .. }
                        | FaultKind::CrashAt { .. }
                ) || event.id == DUAL_WRITER_LEASE_HOLD_EVENT_ID
            })
        })
        .then(|| Arc::new(TestClock::default()))
}

fn advance_quiescence_clock_past_lease(clock: &TestClock, config: &Config) -> i64 {
    let advance_ms = config
        .compaction
        .lease_duration_secs
        .checked_add(1)
        .and_then(|seconds| seconds.checked_mul(1_000))
        .and_then(|millis| i64::try_from(millis).ok())
        .expect("compaction lease duration must fit a signed millisecond clock jump");
    clock.jump(advance_ms);
    advance_ms
}

fn injected_clock(test_clock: Option<&Arc<TestClock>>) -> Option<Clock> {
    test_clock.map(|clock| Clock::from_source(clock.clone()))
}

fn advance_scheduled_faults(
    scheduler: Option<&FaultScheduler>,
    test_clock: Option<&Arc<TestClock>>,
    op_index: u64,
) -> Vec<SchedulerCommand> {
    let Some(scheduler) = scheduler else {
        return Vec::new();
    };
    let mut operational = Vec::new();
    for command in scheduler.advance_to(op_index) {
        let SchedulerCommand::Clock(command) = command else {
            operational.push(command);
            continue;
        };
        let clock = test_clock.expect("clock command requires a shared TestClock");
        let (event_id, action, semantics) = match command {
            ClockCommand::Jump { event_id, delta_ms } => {
                clock.jump(delta_ms);
                let action = if delta_ms % 1_000 == 0 {
                    format!("jump {:+}s", delta_ms / 1_000)
                } else {
                    format!("jump {delta_ms:+}ms")
                };
                (event_id, action, FaultSemantics::PreCall)
            }
            ClockCommand::Freeze { event_id, for_ops } => {
                clock.freeze();
                (
                    event_id,
                    format!("freeze({for_ops} ops)"),
                    FaultSemantics::PreCall,
                )
            }
            ClockCommand::Thaw { event_id } => {
                clock.thaw();
                (event_id, "thaw".to_string(), FaultSemantics::WindowEnd)
            }
        };
        scheduler.record(TimelineEvent {
            event_id,
            op_index,
            wall_ms: scheduler.wall_ms(),
            boundary: Boundary::Clock,
            action,
            key: None,
            semantics,
            observed: ObservedResult::DefiniteApplied,
            recovery: None,
        });
    }
    operational
}

fn adversarial_client(server: &FullTestServer) -> Client {
    Client::builder()
        .default_headers(crate::common::server::bearer_headers(&server.admin_bearer))
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build adversarial reqwest client")
}

async fn bootstrap_security_program(
    server: &FullTestServer,
    program: &SecurityProgramConfig,
) -> u64 {
    let admin = crate::common::server::client_with_bearer(&server.admin_bearer);
    let mut policy_version = 1u64;
    for principal in &program.principals {
        let response = admin
            .post(format!("{}/v1/security/principals", server.base_url))
            .json(&json!({
                "principal_id": principal.principal_id,
                "kind": "service",
                "display_name": principal.display_name,
            }))
            .send()
            .await
            .unwrap_or_else(|error| panic!("security principal bootstrap request failed: {error}"));
        let body = required_bootstrap_response(response, StatusCode::CREATED, "principal").await;
        policy_version = policy_version.max(required_policy_version(&body));
        server
            .workload_credentials
            .register_principal(principal.actor.0, &principal.principal_id);

        for grant in &principal.grants {
            let response = admin
                .post(format!("{}/v1/security/grants", server.base_url))
                .json(&security_grant_body(&principal.principal_id, grant))
                .send()
                .await
                .unwrap_or_else(|error| panic!("security grant bootstrap request failed: {error}"));
            let body = required_bootstrap_response(response, StatusCode::CREATED, "grant").await;
            policy_version = policy_version.max(required_policy_version(&body));
        }

        if principal.bootstrap_key {
            let response = admin
                .post(format!("{}/v1/security/keys", server.base_url))
                .json(&json!({
                    "principal_id": principal.principal_id,
                    "name": format!("adversarial-{}-bootstrap", principal.actor.0),
                }))
                .send()
                .await
                .unwrap_or_else(|error| panic!("security key bootstrap request failed: {error}"));
            let body = required_bootstrap_response(response, StatusCode::CREATED, "key").await;
            policy_version = policy_version.max(required_policy_version(&body));
            let key_id = body["key_id"]
                .as_str()
                .expect("security key bootstrap response omitted key_id");
            let bearer = body["api_key"]
                .as_str()
                .expect("security key bootstrap response omitted api_key");
            server
                .workload_credentials
                .install(principal.actor.0, key_id, bearer);
        }
    }
    policy_version
}

async fn required_bootstrap_response(
    response: reqwest::Response,
    expected: StatusCode,
    operation: &str,
) -> serde_json::Value {
    let status = response.status();
    let body = response
        .json::<serde_json::Value>()
        .await
        .unwrap_or_else(|error| {
            panic!("security {operation} bootstrap response was not JSON: {error}")
        });
    assert_eq!(
        status, expected,
        "security {operation} bootstrap failed: {body}"
    );
    body
}

fn required_policy_version(body: &serde_json::Value) -> u64 {
    body["policy_version"]
        .as_u64()
        .unwrap_or_else(|| panic!("security mutation response omitted policy_version: {body}"))
}

fn security_grant_body(
    principal_id: &str,
    grant: &super::ops::SecurityGrantSpec,
) -> serde_json::Value {
    let scope = grant.namespace.as_deref().map_or_else(
        || json!({"kind": "global"}),
        |namespace| json!({"kind": "namespace", "namespace": namespace}),
    );
    let mut body = json!({
        "principal_id": principal_id,
        "scope": scope,
        "actions": {"kind": "selected", "actions": grant.actions},
    });
    let object = body
        .as_object_mut()
        .expect("security grant request must be an object");
    if let Some(filter) = &grant.mandatory_filter {
        object.insert("mandatory_filter".to_string(), filter.clone());
    }
    if let Some(constraints) = &grant.write_constraints {
        object.insert("write_constraints".to_string(), constraints.clone());
    }
    body
}

#[cfg(test)]
fn raw_adversarial_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build raw adversarial reqwest client")
}

async fn start_http_fault_injector(base_url: &str) -> Arc<HttpFaultInjector> {
    let upstream = base_url
        .strip_prefix("http://")
        .unwrap_or_else(|| panic!("test server URL is not HTTP: {base_url}"))
        .parse::<SocketAddr>()
        .unwrap_or_else(|error| panic!("test server URL has invalid socket address: {error}"));
    Arc::new(
        HttpFaultInjector::start(upstream)
            .await
            .unwrap_or_else(|error| panic!("failed to start HTTP fault injector: {error}")),
    )
}

async fn shutdown_http_fault_injector(injector: &mut Option<Arc<HttpFaultInjector>>) {
    if let Some(injector) = injector.take() {
        injector.disarm();
        Arc::try_unwrap(injector)
            .unwrap_or_else(|_| panic!("HTTP fault injector still has live request contexts"))
            .shutdown()
            .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn restart_after_crash(
    server: &mut RestartableFullTestServer,
    controller: &ProcessController,
    scheduler: &FaultScheduler,
    injector: &mut Option<Arc<HttpFaultInjector>>,
    http_fault_context: &mut Option<HttpFaultContext>,
    server_store: &ZeppelinStore,
    bookkeeping_store: &ZeppelinStore,
    prefix: &str,
    config: &Config,
    spawn_compaction_loop: bool,
    client: &Client,
    model: &Model,
    op_index: u64,
    crash: CrashRequest,
) -> Vec<Violation> {
    *http_fault_context = None;
    let clock = server.clock.clone();
    let admin_bearer = server.admin_bearer.clone();
    let workload_credentials = server.workload_credentials.clone();
    let encoder_provider = Arc::clone(&server.encoder_provider);
    let object_store_counter = server.object_store_counter.clone();
    let old_server = server.take();
    controller.park_token.cancel();
    let held_call_retirement = scheduler.begin_held_call_retirement();
    old_server
        .abort_and_drop()
        .await
        .unwrap_or_else(|error| panic!("crashed primary HTTP retirement failed: {error}"));
    drop(held_call_retirement);
    shutdown_http_fault_injector(injector).await;

    let mut replacement = if let Some(counter) = object_store_counter {
        start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer_and_encoder_provider(
            server_store.clone(),
            Some(prefix.to_string()),
            config.clone(),
            spawn_compaction_loop,
            Some(clock),
            100 * 1024 * 1024,
            &admin_bearer,
            encoder_provider,
            counter,
        )
        .await
    } else {
        start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
            server_store.clone(),
            Some(prefix.to_string()),
            config.clone(),
            spawn_compaction_loop,
            Some(clock),
            100 * 1024 * 1024,
            &admin_bearer,
        )
        .await
    };
    replacement.workload_credentials = workload_credentials;
    server.install(replacement);
    wait_for_health(client, &server.base_url).await;
    scheduler.record(TimelineEvent {
        event_id: crash.event_id,
        op_index: crash.op_index,
        wall_ms: scheduler.wall_ms(),
        boundary: Boundary::Process,
        action: format!("crash@{:?}/{:?}", crash.point, crash.position),
        key: Some(crash.key),
        semantics: match crash.position {
            TriggerPosition::Pre => FaultSemantics::PreCall,
            TriggerPosition::Post => FaultSemantics::PostCommit,
        },
        observed: ObservedResult::Ambiguous,
        recovery: Some("restart+health-wait".to_string()),
    });
    scheduler.reset_process_controller();

    let new_injector = start_http_fault_injector(&server.base_url).await;
    *http_fault_context = Some(HttpFaultContext {
        scheduler: scheduler.clone(),
        injector: new_injector.request_handle(),
        bookkeeping_store: bookkeeping_store.clone(),
        direct_base_url: server.base_url.clone(),
        proxy_base_url: new_injector.base_url(),
    });
    *injector = Some(new_injector);

    crash_recovery_probe(client, &*server, model, op_index).await
}

async fn wait_for_health(client: &Client, base_url: &str) {
    let url = format!("{base_url}/healthz");
    for _ in 0..50 {
        if client
            .get(&url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("replacement test server never became healthy at {url}");
}

async fn crash_recovery_probe(
    client: &Client,
    server: &FullTestServer,
    model: &Model,
    op_index: u64,
) -> Vec<Violation> {
    let mut violations = Vec::new();
    for (namespace, ns_model) in &model.namespaces {
        let response = client
            .get(format!("{}/v1/namespaces/{namespace}", server.base_url))
            .send()
            .await
            .unwrap_or_else(|error| {
                panic!("crash recovery namespace probe failed for {namespace}: {error}")
            });
        let status = response.status().as_u16();
        let namespace_ambiguous = !ns_model.indeterminate_ns.is_empty();
        if status == 200 || (namespace_ambiguous && status == 404) {
            continue;
        }
        violations.push(Violation {
            id: ViolationId::I19CrashRecovery,
            op_index,
            namespace: namespace.clone(),
            detail: "modeled-live namespace was unavailable after restart".to_string(),
            evidence: json!({
                "status": status,
                "namespace_ambiguous": namespace_ambiguous,
            }),
        });
    }
    violations
}

fn sanitize_op_for_mode(op: Op, mode: RunMode, preserve_late_ops: bool) -> Op {
    if mode != RunMode::Chaos {
        return op;
    }
    match op {
        // Chaos mode injects S3 faults while foreground APIs are running.
        // Manual maintenance probes can fail for injected-storage reasons
        // that drown out foreground invariants; keep explicit maintenance out
        // of chaos mode and use the live background loop for compaction
        // coverage instead.
        Op::CompactInline { actor, ns }
        | Op::CompactEndpoint { actor, ns }
        | Op::GcCycle { actor, ns, .. }
        | Op::ProbeSandwich { actor, ns, .. } => Op::GetNamespace { actor, ns },
        // Phase 1 deliberately gives MMLI its own deterministic profile. Keep
        // its direct enrichment/compaction bridge outside legacy fault runs;
        // later phases add late-specific fault semantics.
        late @ (Op::LateUpsert { .. } | Op::LateQuery { .. }) if preserve_late_ops => late,
        Op::LateUpsert { actor, ns, .. } | Op::LateQuery { actor, ns, .. } => {
            Op::GetNamespace { actor, ns }
        }
        Op::FetchVectors { actor, ns, ids, .. } => Op::FetchVectors {
            actor,
            ns,
            ids,
            consistency: ConsistencyLevel::Strong,
        },
        Op::Query {
            actor,
            ns,
            mut q,
            as_of,
        } => {
            if let Some(object) = q.body.as_object_mut() {
                object.insert("consistency".to_string(), json!(ConsistencyLevel::Strong));
            }
            q.class = match q.class {
                QueryOracleClass::ExactAnn { top_k, filter, .. } => QueryOracleClass::ExactAnn {
                    top_k,
                    consistency: ConsistencyLevel::Strong,
                    filter,
                },
                QueryOracleClass::Membership { .. } => QueryOracleClass::Membership {
                    consistency: ConsistencyLevel::Strong,
                },
                QueryOracleClass::ExpectError { status, code } => {
                    QueryOracleClass::ExpectError { status, code }
                }
                QueryOracleClass::Unauthorized => QueryOracleClass::Unauthorized,
                QueryOracleClass::Forbidden => QueryOracleClass::Forbidden,
            };
            Op::Query {
                actor,
                ns,
                q,
                as_of,
            }
        }
        other => other,
    }
}

fn note_background_compaction_namespace(starts: &mut BTreeMap<String, u64>, ns: &str) {
    starts
        .entry(ns.to_string())
        .or_insert_with(|| background_compaction_metric(ns));
}

fn background_compactions_since(starts: &BTreeMap<String, u64>) -> u64 {
    starts
        .iter()
        .map(|(ns, start)| background_compaction_metric(ns).saturating_sub(*start))
        .sum()
}

fn background_compaction_metric(ns: &str) -> u64 {
    ["success", "failure"]
        .into_iter()
        .map(|status| {
            zeppelin::metrics::COMPACTIONS_TOTAL
                .with_label_values(&[ns, status])
                .get()
        })
        .sum()
}

fn successful_background_compaction_metric(ns: &str) -> u64 {
    zeppelin::metrics::COMPACTIONS_TOTAL
        .with_label_values(&[ns, "success"])
        .get()
}

fn object_store_breakdown(counter: &GetCounter) -> ObjectStoreCensus {
    counter.class_breakdown()
}

#[must_use]
fn object_store_delta(
    total: &ObjectStoreCensus,
    baseline: &ObjectStoreCensus,
) -> ObjectStoreCensus {
    assert_eq!(
        total.keys().collect::<Vec<_>>(),
        baseline.keys().collect::<Vec<_>>(),
        "object-store census classes changed across the quiet-period boundary"
    );
    total
        .iter()
        .map(|(class, total)| {
            let baseline = baseline
                .get(class)
                .unwrap_or_else(|| panic!("quiet-period baseline omitted class {class}"));
            let subtract = |metric: &str, total: u64, baseline: u64| {
                total.checked_sub(baseline).unwrap_or_else(|| {
                    panic!(
                        "object-store counter regressed for {class}.{metric}: \
                         total={total} baseline={baseline}"
                    )
                })
            };
            (
                *class,
                ClassStats {
                    get_ops: subtract("get_ops", total.get_ops, baseline.get_ops),
                    get_bytes: subtract("get_bytes", total.get_bytes, baseline.get_bytes),
                    put_ops: subtract("put_ops", total.put_ops, baseline.put_ops),
                    put_bytes: subtract("put_bytes", total.put_bytes, baseline.put_bytes),
                },
            )
        })
        .collect()
}

fn recorded_seed_ops_if_requested(env: &RunnerEnv, seed: u64, prefix: &str) -> Option<Vec<Op>> {
    if std::env::var_os("ZEPPELIN_ADVERSARIAL_SEED").is_none()
        || std::env::var_os("ZEPPELIN_ADVERSARIAL_REPLAY").is_some()
    {
        return None;
    }

    let recorded = latest_recorded_seed_dir(env, seed)?;
    let seed_config = replay_seed_config(&recorded);
    let old_prefix = recorded_namespace_prefix(seed_config.seed, &seed_config.namespace_specs);
    let ops = read_ops(&recorded)
        .into_iter()
        .map(|record| rewrite_replayed_op(&record.op, &old_prefix, prefix))
        .collect::<Vec<_>>();
    eprintln!(
        "determinism guard: comparing generated seed {} to {}",
        seed,
        recorded.display()
    );
    Some(ops)
}

fn latest_recorded_seed_dir(env: &RunnerEnv, seed: u64) -> Option<PathBuf> {
    let entries = match fs::read_dir(&env.artifacts) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!(
            "failed to read adversarial artifact root {}: {error}",
            env.artifacts.display()
        ),
    };
    let min_ops = env.max_ops.unwrap_or(0) as usize;
    let mut dirs = Vec::new();
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| {
            panic!(
                "failed to read entry under {}: {error}",
                env.artifacts.display()
            )
        });
        let path = entry.path().join(format!("seed-{seed}"));
        if !path.join("config.json").exists() || !path.join("ops.jsonl").exists() {
            continue;
        }
        let config = replay_seed_config(&path);
        if config.fault_plan == env.selftest && config.selftest_probe == env.selftest {
            let records = read_ops(&path);
            if !recorded_trace_uses_generated_ids(&records) {
                continue;
            }
            let op_count = records.len();
            if op_count >= min_ops {
                dirs.push((op_count, path));
            }
        }
    }
    dirs.sort();
    dirs.pop().map(|(_, path)| path)
}

fn recorded_trace_uses_generated_ids(records: &[OpRecord]) -> bool {
    for record in records {
        if let Op::Upsert { ns, vectors, .. } = &record.op {
            return vectors.iter().all(|vector| vector.id.starts_with(ns));
        }
    }
    true
}

fn assert_recorded_op_matches(recorded_ops: Option<&[Op]>, seed: u64, index: u64, actual: &Op) {
    let Some(recorded_ops) = recorded_ops else {
        return;
    };
    let expected = recorded_ops.get(index as usize).unwrap_or_else(|| {
        panic!("determinism guard for seed {seed} ended before generated op {index}: {actual:#?}")
    });
    let expected_json =
        serde_json::to_value(expected).expect("recorded Op must serialize for comparison");
    let actual_json = serde_json::to_value(actual).expect("generated Op must serialize");
    assert!(
        json_values_equivalent(&actual_json, &expected_json),
        "determinism guard diverged for seed {seed} at op {index}\nexpected: {expected:#?}\nactual: {actual:#?}"
    );
}

fn json_values_equivalent(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (left, right) {
        (serde_json::Value::Null, serde_json::Value::Null) => true,
        (serde_json::Value::Bool(left), serde_json::Value::Bool(right)) => left == right,
        (serde_json::Value::String(left), serde_json::Value::String(right)) => left == right,
        (serde_json::Value::Number(left), serde_json::Value::Number(right)) => {
            json_numbers_equivalent(left, right)
        }
        (serde_json::Value::Array(left), serde_json::Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right.iter())
                    .all(|(left, right)| json_values_equivalent(left, right))
        }
        (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| json_values_equivalent(left, right))
                })
        }
        _ => false,
    }
}

fn json_numbers_equivalent(left: &serde_json::Number, right: &serde_json::Number) -> bool {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return left == right;
    }
    if let (Some(left), Some(right)) = (left.as_u64(), right.as_u64()) {
        return left == right;
    }
    let left = left
        .as_f64()
        .expect("serde_json number should fit in f64 for guard comparison");
    let right = right
        .as_f64()
        .expect("serde_json number should fit in f64 for guard comparison");
    (left - right).abs() <= 1e-12
}

fn rewrite_replayed_op(op: &Op, old_prefix: &str, new_prefix: &str) -> Op {
    let mut value = serde_json::to_value(op).expect("Op must serialize for determinism guard");
    rewrite_json_strings(&mut value, old_prefix, new_prefix);
    serde_json::from_value(value).expect("rewritten Op must deserialize")
}

fn rewrite_json_strings(value: &mut serde_json::Value, old_prefix: &str, new_prefix: &str) {
    match value {
        serde_json::Value::String(string) => {
            if let Some(suffix) = string.strip_prefix(old_prefix) {
                *string = format!("{new_prefix}{suffix}");
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                rewrite_json_strings(value, old_prefix, new_prefix);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                rewrite_json_strings(value, old_prefix, new_prefix);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

async fn inspect_namespaces(store: &zeppelin::storage::ZeppelinStore, target: &str) -> Vec<String> {
    let path = Path::new(target);
    if path.exists() {
        let failure = read_failure_manifest(path);
        if let Some(failure) = failure {
            let discovered = discover_namespaces(store, &failure.preserved_prefix).await;
            if !discovered.is_empty() {
                return discovered;
            }
            let config = replay_seed_config(path);
            return config.namespace_specs.keys().cloned().collect();
        }
        let config = replay_seed_config(path);
        return config.namespace_specs.keys().cloned().collect();
    }
    discover_namespaces(store, target).await
}

async fn discover_namespaces(
    store: &zeppelin::storage::ZeppelinStore,
    prefix: &str,
) -> Vec<String> {
    let mut namespaces = store
        .list_common_prefixes("")
        .await
        .unwrap_or_else(|error| panic!("failed to list root namespace prefixes: {error}"))
        .into_iter()
        .filter_map(|key| key.strip_suffix('/').map(str::to_string))
        .filter(|namespace| namespace.starts_with(prefix))
        .collect::<Vec<_>>();
    namespaces.sort();
    namespaces
}

async fn print_namespace_inspection(store: &zeppelin::storage::ZeppelinStore, ns: &str) {
    println!("\n## namespace {ns}");
    let Some(manifest) = Manifest::read(store, ns)
        .await
        .unwrap_or_else(|error| panic!("failed to read manifest for {ns}: {error}"))
    else {
        println!("manifest: missing");
        return;
    };
    println!("manifest generation: {}", manifest.version());
    println!("fencing_token: {}", manifest.fencing_token);
    println!("next_sequence: {}", manifest.next_sequence);
    println!("active_segment: {:?}", manifest.active_segment);
    println!("compaction_watermark: {:?}", manifest.compaction_watermark);
    println!("updated_at: {}", manifest.updated_at);
    println!("pending_deletes: {}", manifest.pending_deletes.len());
    for key in &manifest.pending_deletes {
        println!("  pending {key}");
    }

    println!("fragments: {}", manifest.fragments.len());
    for fragment in &manifest.fragments {
        println!(
            "  {} seq={} vectors={} deletes={} bytes={}",
            fragment.id,
            fragment.sequence_number,
            fragment.vector_count,
            fragment.delete_count,
            fragment.size_bytes
        );
    }

    println!("segments: {}", manifest.segments.len());
    for segment in &manifest.segments {
        let carried = segment
            .cluster_owners
            .iter()
            .filter(|owner| *owner != &segment.id)
            .count();
        println!(
            "  {} vectors={} clusters={} quant={:?} hierarchical={} carried_clusters={} fts={:?} bitmap={:?} global_fts={}",
            segment.id,
            segment.vector_count,
            segment.cluster_count,
            segment.quantization,
            segment.hierarchical,
            carried,
            segment.fts_fields,
            segment.bitmap_fields,
            segment.has_global_fts
        );
        if let Some(sketch) = &segment.sketch {
            println!(
                "    sketch_key={} sketch_version={} code_dims={} bytes_per_vector={} size_bytes={} rotation_seed={:?}",
                sketch.key,
                sketch.version,
                sketch.code_dims,
                sketch.bytes_per_vector,
                sketch.size_bytes,
                sketch.rotation_seed
            );
        }
        if let Some(bootstrap) = &segment.bootstrap {
            println!(
                "    bootstrap_key={} bootstrap_size_bytes={}",
                bootstrap.key, bootstrap.size_bytes
            );
        }
    }

    let snapshots = zeppelin::wal::manifest::NamedSnapshot::list(store, ns)
        .await
        .unwrap_or_else(|error| panic!("failed to list snapshots for {ns}: {error}"));
    println!("snapshots: {}", snapshots.len());
    for snapshot in &snapshots {
        println!(
            "  {} -> generation {} at {}",
            snapshot.name, snapshot.generation, snapshot.created_at
        );
    }

    println!("history:");
    for entry in Manifest::list_history(store, ns)
        .await
        .unwrap_or_else(|error| panic!("failed to list history for {ns}: {error}"))
    {
        let history = Manifest::read_history(store, ns, entry.version)
            .await
            .unwrap_or_else(|error| panic!("failed to read history {}: {error}", entry.key))
            .unwrap_or_else(|| panic!("history key disappeared: {}", entry.key));
        let pins = snapshots
            .iter()
            .filter(|snapshot| snapshot.generation == entry.version)
            .map(|snapshot| snapshot.name.as_str())
            .collect::<Vec<_>>();
        println!(
            "  generation {} updated_at={} pins={:?}",
            history.version(),
            history.updated_at,
            pins
        );
    }

    let candidates = gc::load_gc_candidates(store, ns)
        .await
        .unwrap_or_else(|error| panic!("failed to load GC candidates for {ns}: {error}"));
    println!("gc candidates: {}", candidates.len());
    for candidate in &candidates {
        println!(
            "  {} first_seen={} since_generation={}",
            candidate.key,
            candidate.first_seen_unreachable_at,
            candidate.unreachable_since_manifest_version
        );
    }

    let reachable = gc::reachable_keys_with_retained_history_and_staging(
        store,
        ns,
        &manifest,
        &Default::default(),
    )
    .await
    .unwrap_or_else(|error| panic!("failed to compute reachability for {ns}: {error}"));
    let listed = store
        .list_prefix(&format!("{ns}/"))
        .await
        .unwrap_or_else(|error| panic!("failed to list namespace keys for {ns}: {error}"))
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let missing = reachable.difference(&listed).cloned().collect::<Vec<_>>();
    let extra = listed.difference(&reachable).cloned().collect::<Vec<_>>();
    println!("reach \\ listed: {}", missing.len());
    for key in missing {
        println!("  missing {key}");
    }
    println!("listed \\ reach: {}", extra.len());
    for key in extra {
        println!("  awaiting_gc {key}");
    }
}

pub async fn run_oracle_selftest(env: RunnerEnv) {
    let mutations = env
        .selftest
        .map_or_else(|| OracleMutation::ALL.to_vec(), |mutation| vec![mutation]);

    for mutation in mutations {
        let seed = 7;
        let clean_env = env.for_oracle_selftest(seed);
        let clean_artifacts = RunArtifacts::create(&clean_env);
        let clean = Box::pin(run_seed(
            &clean_env,
            &clean_artifacts,
            seed,
            Instant::now() + Duration::from_secs(clean_env.seconds),
            None,
            Some(mutation),
        ))
        .await;
        assert!(
            !clean.failed,
            "clean oracle selftest control for {} failed: {:?}",
            mutation.key(),
            clean.violations
        );

        let mutated_env = env.for_oracle_selftest(seed);
        let mutated_artifacts = RunArtifacts::create(&mutated_env);
        let mutated = Box::pin(run_seed(
            &mutated_env,
            &mutated_artifacts,
            seed,
            Instant::now() + Duration::from_secs(mutated_env.seconds),
            Some(mutation),
            Some(mutation),
        ))
        .await;
        assert!(
            mutated.failed,
            "oracle selftest mutation {} did not fail",
            mutation.key()
        );
        let fired: Vec<ViolationId> = mutated
            .violations
            .iter()
            .map(|violation| violation.id)
            .collect();
        let accepted = match mutation {
            OracleMutation::DropDelete | OracleMutation::PhantomId => {
                fired.contains(&ViolationId::I4FetchExact)
            }
            OracleMutation::SkewScore => fired.contains(&ViolationId::I1StrongExact),
            OracleMutation::LeakTombstone => fired.contains(&ViolationId::I3EventualExact),
            OracleMutation::FilterSkew => fired.contains(&ViolationId::I1StrongExact),
            OracleMutation::GcEatsLiveKey => fired.contains(&ViolationId::I14S3Reachability),
            OracleMutation::StaleCheckpoint => fired.contains(&ViolationId::I8AsOfExact),
            OracleMutation::ChaosLostWrite => fired.contains(&ViolationId::I16Quiescence),
            OracleMutation::PostCommitLostWrite => {
                fired.contains(&ViolationId::I16Quiescence)
                    || fired.contains(&ViolationId::I1StrongExact)
            }
            OracleMutation::IndetResolutionLie => {
                fired.contains(&ViolationId::I18IndeterminateResolution)
            }
            OracleMutation::DroppedResponseLostWrite => {
                fired.contains(&ViolationId::I18IndeterminateResolution)
            }
            OracleMutation::CrashLostAck => fired.contains(&ViolationId::I16Quiescence),
            OracleMutation::ClockGcEatsLive => fired.contains(&ViolationId::I14S3Reachability),
            OracleMutation::SwallowCorruption => {
                fired.contains(&ViolationId::I20CorruptionSurfaced)
            }
            OracleMutation::MisdirectedWriteReachability => {
                fired.contains(&ViolationId::I14S3Reachability)
            }
            OracleMutation::DualWriterFencing => fired == vec![ViolationId::I21FencingViolation],
            OracleMutation::GrantModelDesync => fired.contains(&ViolationId::I22AuthzDecision),
            OracleMutation::LeakedIdSuppression => fired.contains(&ViolationId::I23TenantLeak),
            OracleMutation::RevocationMisclassification => {
                fired.contains(&ViolationId::I24RevocationFreshness)
            }
            OracleMutation::AuditRecordDeletion => fired.contains(&ViolationId::I25AuditEvidence),
            OracleMutation::SecuritySecretLeak => {
                fired.contains(&ViolationId::I26SecurityStateSanity)
            }
            OracleMutation::ConstraintDrop => fired.contains(&ViolationId::I27ConstraintDrop),
            OracleMutation::DelegationParentDesync | OracleMutation::DelegationNarrowingBypass => {
                fired.contains(&ViolationId::I22AuthzDecision)
            }
            OracleMutation::PreservationBypass => {
                fired.contains(&ViolationId::I28PreservationBypass)
            }
            OracleMutation::AuditChainRecordDrop => fired.contains(&ViolationId::I25AuditEvidence),
            OracleMutation::LateSkewScore => fired.contains(&ViolationId::I31LateExactEquivalence),
            OracleMutation::LateHiddenGet => fired.contains(&ViolationId::I32LateReadAccounting),
            OracleMutation::LateTruncatedResultSuccess => {
                fired.contains(&ViolationId::I33LateWorkerLifecycle)
            }
        };
        assert!(
            accepted,
            "oracle selftest mutation {} fired {:?}, not the expected violation",
            mutation.key(),
            fired
        );
        println!(
            "oracle selftest {} fired {:?} after {} ops",
            mutation.key(),
            fired,
            mutated.ops
        );
    }
}

async fn run_seed(
    env: &RunnerEnv,
    artifacts: &RunArtifacts,
    seed: u64,
    deadline: Instant,
    mutation: Option<OracleMutation>,
    selftest_probe: Option<OracleMutation>,
) -> SeedOutcome {
    Box::pin(run_seed_inner(
        env,
        artifacts,
        seed,
        deadline,
        mutation,
        selftest_probe,
        None,
    ))
    .await
}

async fn run_seed_inner(
    env: &RunnerEnv,
    artifacts: &RunArtifacts,
    seed: u64,
    deadline: Instant,
    mutation: Option<OracleMutation>,
    selftest_probe: Option<OracleMutation>,
    watchdog: Option<&SeedWatchdogContext>,
) -> SeedOutcome {
    let post_commit_selftest = matches!(
        mutation.or(selftest_probe),
        Some(OracleMutation::PostCommitLostWrite | OracleMutation::IndetResolutionLie)
    );
    let dropped_response_selftest = matches!(
        mutation.or(selftest_probe),
        Some(OracleMutation::DroppedResponseLostWrite)
    );
    let crash_lost_ack_selftest = matches!(
        mutation.or(selftest_probe),
        Some(OracleMutation::CrashLostAck)
    );
    let clock_gc_eats_live_selftest = matches!(
        mutation.or(selftest_probe),
        Some(OracleMutation::ClockGcEatsLive)
    );
    let dual_writer_fencing_selftest = matches!(
        mutation.or(selftest_probe),
        Some(OracleMutation::DualWriterFencing)
    );
    let swallow_corruption_selftest = mutation == Some(OracleMutation::SwallowCorruption);
    let misdirected_write_selftest = mutation == Some(OracleMutation::MisdirectedWriteReachability);
    let assignment = effective_seed_assignment(env.mode, env.profile, seed);
    let branching_profile = assignment.profile == Some(FaultProfile::Branching);
    let late_profile = matches!(
        assignment.profile,
        Some(FaultProfile::Late | FaultProfile::LateStream)
    );
    let profile = scheduled_profile(assignment.profile);
    let mode = if profile.is_some()
        || mutation == Some(OracleMutation::ChaosLostWrite)
        || post_commit_selftest
        || dropped_response_selftest
        || crash_lost_ack_selftest
        || clock_gc_eats_live_selftest
        || swallow_corruption_selftest
        || misdirected_write_selftest
        || dual_writer_fencing_selftest
    {
        RunMode::Chaos
    } else {
        assignment.mode
    };
    let security_program_enabled = profile == Some(FaultProfile::Security)
        || mutation.is_some_and(OracleMutation::is_security)
        || selftest_probe.is_some_and(OracleMutation::is_security);
    let harness = TestHarness::new().await;
    let prefix = harness.prefix.clone();
    if let Some(watchdog) = watchdog {
        watchdog.register_prefix(&prefix);
    }
    let mut generator = if branching_profile {
        AdversarialGenerator::new_branching(seed, &prefix)
    } else if late_profile {
        AdversarialGenerator::new_late(seed, &prefix)
    } else if profile == Some(FaultProfile::Security) {
        AdversarialGenerator::new_security_profile(seed, &prefix)
    } else if security_program_enabled {
        AdversarialGenerator::new_security(seed, &prefix)
    } else {
        AdversarialGenerator::new(seed, &prefix)
    };
    let security_program = generator.security_program().cloned();
    let specs = generator.specs();
    let scheduler = if dual_writer_fencing_selftest {
        Some(FaultScheduler::from_schedule(
            FaultSchedule::dual_writer_fencing_selftest(),
        ))
    } else if swallow_corruption_selftest {
        Some(FaultScheduler::from_schedule(
            FaultSchedule::swallow_corruption_selftest(),
        ))
    } else if misdirected_write_selftest {
        Some(FaultScheduler::from_schedule(
            FaultSchedule::misdirected_write_reachability_selftest(),
        ))
    } else if clock_gc_eats_live_selftest {
        Some(FaultScheduler::from_schedule(
            FaultSchedule::clock_gc_eats_live_selftest(),
        ))
    } else if crash_lost_ack_selftest {
        Some(FaultScheduler::from_schedule(
            FaultSchedule::crash_lost_ack_selftest(),
        ))
    } else if dropped_response_selftest {
        Some(FaultScheduler::from_schedule(
            FaultSchedule::dropped_response_selftest(),
        ))
    } else {
        profile.map(|profile| FaultScheduler::for_seed(seed, profile))
    };
    if let Some(watchdog) = watchdog {
        watchdog.register_scheduler(scheduler.as_ref());
    }
    let test_clock = test_clock_for_scheduler(scheduler.as_ref());
    let chaos_plan = if matches!(
        mutation,
        Some(OracleMutation::DroppedResponseLostWrite | OracleMutation::CrashLostAck)
    ) {
        Some(FaultPlan::lost_write_selftest())
    } else if mode == RunMode::Chaos && scheduler.is_none() {
        Some(if mutation == Some(OracleMutation::ChaosLostWrite) {
            FaultPlan::lost_write_selftest()
        } else if post_commit_selftest {
            let first_upsert_manifest_call =
                u32::try_from(specs.len() + 1).expect("selftest namespace count must fit in u32");
            FaultPlan::post_commit_selftest(first_upsert_manifest_call)
        } else {
            FaultPlan::for_seed(seed)
        })
    } else {
        None
    };
    let chaos_plan_json = chaos_plan
        .as_ref()
        .map(|plan| serde_json::to_value(plan).expect("FaultPlan must serialize"));
    let active_profile = scheduler
        .as_ref()
        .map(|scheduler| scheduler.schedule().profile)
        .or_else(|| chaos_plan.as_ref().map(|_| FaultProfile::LegacyChaos))
        .or_else(|| branching_profile.then_some(FaultProfile::Branching))
        .or_else(|| late_profile.then_some(FaultProfile::Late));
    let (counted_backend, counter) = counting_store(&harness.store);
    let (legacy_instrumented_store, chaos_handle) =
        wrap_chaos_store(&counted_backend, chaos_plan.clone());
    if let Some(chaos) = &chaos_handle {
        chaos.disable();
    }
    let instrumented_store = scheduler
        .as_ref()
        .map_or(legacy_instrumented_store.clone(), |scheduler| {
            store_fault_proxy(&legacy_instrumented_store, scheduler.clone())
        });
    let store = instrumented_store;
    let require_compaction_evidence = requires_two_node_compaction_evidence(scheduler.as_ref());
    let operational_observer = requires_operational_store_observer(scheduler.as_ref())
        .then(OperationalStoreObserver::default);
    let primary_store = operational_observer.as_ref().map_or_else(
        || store.clone(),
        |observer| operational_store_proxy(&store, observer.clone(), 0),
    );
    let mut config = config_for_mode(mode, seed, scheduler.as_ref().map(FaultScheduler::schedule));
    apply_late_namespace_config(&mut config, &specs);
    if branching_profile {
        config.branching.enabled = true;
    }
    let recorded_ops = recorded_seed_ops_if_requested(env, seed, &prefix);
    let mut artifacts = artifacts.seed_with_security(
        seed,
        &config,
        &specs,
        mode,
        mutation.map(OracleMutation::key),
        selftest_probe.map(OracleMutation::key),
        chaos_plan_json.as_ref(),
        scheduler.as_ref().map(FaultScheduler::schedule),
        security_program.as_ref(),
    );
    let disk_cache_max_bytes = disk_cache_max_bytes_for_schedule(scheduler.as_ref());
    let encoder_provider = late_encoder_provider(&config.mmli)
        .expect("adversarial seed encoder provider must construct");
    let initial_server = start_test_server_full_with_disk_cache_max_bytes_and_encoder_provider(
        primary_store,
        Some(prefix.clone()),
        config.clone(),
        mode == RunMode::Chaos,
        injected_clock(test_clock.as_ref()),
        disk_cache_max_bytes,
        encoder_provider,
        counter.clone(),
    )
    .await;
    let mut server = match watchdog {
        Some(watchdog) => {
            RestartableFullTestServer::new_with_watchdog(initial_server, watchdog.clone())
        }
        None => RestartableFullTestServer::new(initial_server),
    };
    let bootstrapped_policy_version = if let Some(program) = &security_program {
        Some(bootstrap_security_program(&server, program).await)
    } else {
        None
    };
    let mut operational_state = OperationalState::default();
    let mut injector = if scheduler.is_some() {
        Some(start_http_fault_injector(&server.base_url).await)
    } else {
        None
    };
    let mut http_fault_context =
        scheduler
            .as_ref()
            .zip(injector.as_ref())
            .map(|(scheduler, injector)| HttpFaultContext {
                scheduler: scheduler.clone(),
                injector: injector.request_handle(),
                bookkeeping_store: harness.store.clone(),
                direct_base_url: server.base_url.clone(),
                proxy_base_url: injector.base_url(),
            });
    let client = adversarial_client(&server);
    if let Some(chaos) = &chaos_handle {
        chaos.enable();
    }
    let mut model = Model::default();
    if let (Some(program), Some(policy_version)) =
        (security_program.clone(), bootstrapped_policy_version)
    {
        initialize_security_model(
            &mut model,
            program,
            policy_version,
            &server.workload_credentials,
        );
    }
    let mut coverage = Coverage::default();
    let mut created_namespaces = Vec::new();
    let mut background_compaction_starts = BTreeMap::new();
    let mut s3_tracker = S3Tracker::default();
    let mut corruption_tracker = CorruptionTracker::default();
    let mut op_index = 0u64;
    let mut failed = false;
    let mut failure_violations = Vec::new();
    let mut compactions = 0u64;
    let mut post_commit_ack_loss_fired = false;
    let mut pending_held_op = None;
    let mut deferred_ops = VecDeque::new();
    let mut clean_late_followups = VecDeque::new();
    let mut late_stream_fault_probe = None;
    let mut quiet_drain_ops = VecDeque::new();
    let mut dual_writer_lease_hold = None;
    let mut dual_writer_lease_hold_activated = false;
    let mut dual_writer_stale_fencing_token = None;
    let started = Instant::now();
    let max_ops = env.max_ops.unwrap_or(500);
    let mut generation_cap_reached = false;

    loop {
        if let Some(watchdog) = watchdog {
            watchdog.update(
                "workload",
                op_index,
                scheduler.as_ref(),
                pending_held_op.as_ref(),
                deferred_ops.len(),
                quiet_drain_ops.len(),
            );
        }
        let commands = advance_scheduled_faults(scheduler.as_ref(), test_clock.as_ref(), op_index);
        if dual_writer_lease_hold.as_ref().is_some_and(
            |activation: &DualWriterLeaseHoldActivation| activation.release_op <= op_index,
        ) {
            let activation = dual_writer_lease_hold
                .take()
                .expect("release-ready dual-writer lease hold disappeared");
            let stale_fencing_token = finish_dual_writer_lease_hold(
                scheduler
                    .as_ref()
                    .expect("dual-writer lease hold requires a scheduler"),
                activation,
            )
            .await;
            assert!(
                dual_writer_stale_fencing_token
                    .replace(stale_fencing_token)
                    .is_none(),
                "dual-writer selftest completed its renewal race more than once"
            );
        }
        if pending_held_op
            .as_ref()
            .is_some_and(|pending: &PendingHeldOp| pending.release_op <= op_index)
        {
            let pending = pending_held_op
                .take()
                .expect("release-ready held op disappeared");
            let step = finish_pending_held_op(
                pending,
                &client,
                &mut artifacts,
                &mut model,
                &mut coverage,
                &mut s3_tracker,
                &mut corruption_tracker,
                mutation,
                mode,
            )
            .await;
            apply_step_bookkeeping(
                &step,
                &mut created_namespaces,
                &mut background_compaction_starts,
                &mut compactions,
            );
            post_commit_ack_loss_fired |= step.post_commit_ack_lost;
            if !step.violations.is_empty() {
                failed = true;
                failure_violations = step.violations;
                break;
            }
            if let Some(crash) = take_step_crash(&step, scheduler.as_ref()) {
                let scheduler = scheduler
                    .as_ref()
                    .expect("held-call process crash requires a fault scheduler");
                let controller = scheduler
                    .process_controller()
                    .expect("held-call process crash requires a process controller");
                let recovery = restart_after_crash(
                    &mut server,
                    &controller,
                    scheduler,
                    &mut injector,
                    &mut http_fault_context,
                    &store,
                    &harness.store,
                    &prefix,
                    &config,
                    true,
                    &client,
                    &model,
                    op_index,
                    crash,
                )
                .await;
                if !recovery.is_empty() {
                    failed = true;
                    failure_violations = recovery;
                    break;
                }
            }
        }
        let remaining = operational_state
            .apply_node_commands(
                commands,
                NodeCommandContext {
                    scheduler: scheduler.as_ref(),
                    store: &store,
                    shared_clock: injected_clock(test_clock.as_ref()),
                    operational_observer: operational_observer.as_ref(),
                    require_compaction_evidence,
                    prefix: &prefix,
                    config: &config,
                    admin_bearer: &server.admin_bearer,
                    disk_cache_max_bytes,
                    op_index,
                },
            )
            .await;
        let environment = Box::pin(operational_state.apply_environment_commands(
            remaining,
            EnvironmentCommandContext {
                scheduler: scheduler.as_ref(),
                operational_observer: operational_observer.as_ref(),
                client: &client,
                primary: &server,
                model: &mut model,
                op_index,
            },
        ))
        .await;
        if !environment.violations.is_empty() {
            failed = true;
            failure_violations = environment.violations;
            break;
        }
        if let Some(crash) = environment.crash {
            let scheduler = scheduler
                .as_ref()
                .expect("operational process crash requires a fault scheduler");
            let controller = scheduler
                .process_controller()
                .expect("operational process crash requires a controller");
            let recovery = restart_after_crash(
                &mut server,
                &controller,
                scheduler,
                &mut injector,
                &mut http_fault_context,
                &store,
                &harness.store,
                &prefix,
                &config,
                true,
                &client,
                &model,
                op_index,
                crash,
            )
            .await;
            if !recovery.is_empty() {
                failed = true;
                failure_violations = recovery;
                break;
            }
        }
        let deferred_count = u64::try_from(deferred_ops.len() + clean_late_followups.len())
            .expect("deferred operation count must fit in u64");
        let reserved_workload_slots = op_index
            .checked_add(deferred_count)
            .expect("workload operation reservation overflow");
        assert!(
            reserved_workload_slots <= max_ops,
            "generated workload operations exceeded max_ops: \
             recorded={op_index} deferred={deferred_count} max_ops={max_ops}"
        );
        generation_cap_reached |= reserved_workload_slots == max_ops;
        let generation_budget = if reserved_workload_slots < max_ops
            && (Instant::now() < deadline || reserved_workload_slots == 0)
        {
            max_ops - reserved_workload_slots
        } else {
            0
        };
        let clean_followup_ready =
            clean_late_followups
                .front()
                .is_some_and(|followup: &CleanLateFollowup| {
                    scheduler.as_ref().is_none_or(|scheduler| {
                        !scheduler.fault_window_active(op_index, followup.op.namespace())
                    })
                });
        let pending = pending_held_op.as_ref();
        let scheduled_late_stream_probe = late_stream_fault_probe_for_window(
            scheduler.as_ref(),
            op_index,
            late_stream_fault_probe.as_ref(),
        );
        let (op, clean_followup_source_index, late_stream_event_id) = if clean_followup_ready {
            let followup = clean_late_followups
                .pop_front()
                .expect("ready clean late-query follow-up disappeared");
            (followup.op, Some(followup.faulted_op_index), None)
        } else if let Some((event_id, probe)) = scheduled_late_stream_probe {
            (probe, None, Some(event_id))
        } else if let Some(op) = next_fifo_deferred_op_with_budget(
            &mut deferred_ops,
            generation_budget,
            || {
                sanitize_op_for_mode(
                    generator.next(&model),
                    mode,
                    assignment.profile == Some(FaultProfile::LateStream),
                )
            },
            |op, deferred| {
                pending.is_some_and(|pending| {
                    op_conflicts_with_pending_hold(op, op_index, scheduler.as_ref(), pending)
                }) || op_awaits_store_fault_window_close(op, op_index, scheduler.as_ref(), deferred)
            },
        ) {
            (op, None, None)
        } else {
            assert!(
                clean_late_followups.is_empty(),
                "workload exhausted before mandatory clean late-query follow-ups became runnable"
            );
            if let Some(pending) = pending_held_op.as_mut() {
                assert!(
                    op_index <= pending.release_op,
                    "boundary release moved a foreground hold past its scheduled release"
                );
                pending.release_op = op_index;
                pending.release_cause = HoldReleaseCause::Quiesce;
            }
            let mut ack_loss_reserved = post_commit_ack_loss_fired;
            for (offset, op) in deferred_ops.drain(..).enumerate() {
                let index = op_index
                    .checked_add(
                        u64::try_from(offset).expect("deferred drain offset must fit in u64"),
                    )
                    .expect("deferred drain operation index overflowed");
                assert_recorded_op_matches(recorded_ops.as_deref(), seed, index, &op);
                let inject_post_commit_ack_loss =
                    post_commit_selftest && !ack_loss_reserved && matches!(op, Op::Upsert { .. });
                ack_loss_reserved |= inject_post_commit_ack_loss;
                quiet_drain_ops.push_back(QuietDrainOp::Generated {
                    op,
                    inject_post_commit_ack_loss,
                });
            }
            break;
        };
        assert_recorded_op_matches(recorded_ops.as_deref(), seed, op_index, &op);
        let target_node = operational_state.choose_target_node_for_op(&op);
        let target_server = operational_state.target(&server, target_node);
        let op_http_fault_context = http_fault_context
            .as_ref()
            .map(|context| context.for_node(target_server));
        let execution_phase = ExecutionPhase::Workload;
        let inject_post_commit_ack_loss =
            post_commit_selftest && !post_commit_ack_loss_fired && matches!(op, Op::Upsert { .. });
        let faulted_late_query = assignment.profile == Some(FaultProfile::LateStream)
            && matches!(op, Op::LateQuery { .. })
            && scheduler
                .as_ref()
                .is_some_and(|scheduler| scheduler.fault_window_active(op_index, op.namespace()));
        let execution = if pending_held_op.is_some() {
            RecordedExecutionOutcome::Completed(Box::new(
                execute_recorded_op(
                    &client,
                    target_server,
                    &mut artifacts,
                    &mut model,
                    &mut coverage,
                    &mut s3_tracker,
                    &mut corruption_tracker,
                    &op,
                    op_index,
                    started,
                    mutation,
                    mode,
                    execution_phase,
                    operational_state.generation_checkpoints_enabled(),
                    target_node,
                    op_http_fault_context.as_ref(),
                    inject_post_commit_ack_loss,
                )
                .await,
            ))
        } else {
            execute_recorded_op_or_hold(
                scheduler.as_ref(),
                &client,
                target_server,
                &mut artifacts,
                &mut model,
                &mut coverage,
                &mut s3_tracker,
                &mut corruption_tracker,
                &op,
                op_index,
                started,
                mutation,
                mode,
                operational_state.generation_checkpoints_enabled(),
                target_node,
                op_http_fault_context.as_ref(),
                inject_post_commit_ack_loss,
            )
            .await
        };
        let mut step = match execution {
            RecordedExecutionOutcome::Completed(step) => *step,
            RecordedExecutionOutcome::Held(pending) => {
                assert!(
                    pending_held_op.replace(pending).is_none(),
                    "runner attempted to track more than one held foreground op"
                );
                op_index += 1;
                continue;
            }
        };
        if let Some(event_id) = late_stream_event_id.as_deref() {
            let event_fired_on_late_truth = scheduler
                .as_ref()
                .expect("late-stream probe requires a fault scheduler")
                .timeline()
                .iter()
                .any(|event| {
                    event.event_id == event_id
                        && event.op_index == op_index
                        && event
                            .key
                            .as_deref()
                            .is_some_and(|key| key.contains("/late/segments/"))
                });
            if !event_fired_on_late_truth {
                step.violations.push(Violation {
                    id: ViolationId::I33LateWorkerLifecycle,
                    op_index,
                    namespace: op.namespace().to_string(),
                    detail: "scheduled late-stream fault did not reach a late truth read"
                        .to_string(),
                    evidence: json!({ "event_id": event_id }),
                });
            }
        }
        if let Some(faulted_op_index) = clean_followup_source_index {
            if !(200..300).contains(&step.status)
                && !step
                    .violations
                    .iter()
                    .any(|violation| violation.id == ViolationId::I33LateWorkerLifecycle)
            {
                step.violations.push(Violation {
                    id: ViolationId::I33LateWorkerLifecycle,
                    op_index,
                    namespace: op.namespace().to_string(),
                    detail: "mandatory clean late query failed after a faulted query".to_string(),
                    evidence: json!({
                        "faulted_op_index": faulted_op_index,
                        "followup_status": step.status,
                    }),
                });
            }
        }
        let pending_crash = take_step_crash(&step, scheduler.as_ref());
        post_commit_ack_loss_fired |= step.post_commit_ack_lost;
        if matches!(op, Op::CreateNamespace { .. }) && (200..300).contains(&step.status) {
            let ns = op.namespace().to_string();
            note_background_compaction_namespace(&mut background_compaction_starts, &ns);
            created_namespaces.push(ns.clone());
            if dual_writer_fencing_selftest && !dual_writer_lease_hold_activated {
                let second_node = operational_state.second_node.as_ref().unwrap_or_else(|| {
                    panic!("dual-writer lease activation requires the scheduled second node")
                });
                dual_writer_lease_hold =
                    Some(
                        begin_dual_writer_lease_hold(
                            scheduler
                                .as_ref()
                                .expect("dual-writer lease activation requires a scheduler"),
                            &harness.store,
                            server.store.clone(),
                            second_node.store.clone(),
                            Arc::clone(test_clock.as_ref().expect(
                                "dual-writer lease choreography requires a shared TestClock",
                            )),
                            &ns,
                            op_index,
                        )
                        .await,
                    );
                dual_writer_lease_hold_activated = true;
            }
        }
        if let Op::CloneNamespace { target, .. } = &op {
            if (200..300).contains(&step.status) {
                note_background_compaction_namespace(&mut background_compaction_starts, target);
                created_namespaces.push(target.clone());
            }
        }
        if let Op::DeleteNamespace { ns, .. } = &op {
            if (200..300).contains(&step.status) {
                created_namespaces.retain(|created| created != ns);
            }
        }
        if matches!(
            op,
            Op::CompactInline { .. } | Op::CompactEndpoint { .. } | Op::ProbeSandwich { .. }
        ) && (200..300).contains(&step.status)
        {
            compactions += 1;
        }
        if assignment.profile == Some(FaultProfile::LateStream)
            && late_stream_event_id.is_none()
            && (200..300).contains(&step.status)
            && matches!(op, Op::LateQuery { filter: None, .. })
        {
            late_stream_fault_probe = Some(op.clone());
        }
        op_index += 1;
        if faulted_late_query {
            clean_late_followups.push_back(CleanLateFollowup {
                faulted_op_index: op_index - 1,
                op: op.clone(),
            });
        }
        if !step.violations.is_empty() {
            failed = true;
            failure_violations = step.violations;
            break;
        }
        if let Some(crash) = pending_crash {
            let scheduler = scheduler
                .as_ref()
                .expect("process crash requires a fault scheduler");
            let controller = scheduler
                .process_controller()
                .expect("process crash requires a process controller");
            let recovery = restart_after_crash(
                &mut server,
                &controller,
                scheduler,
                &mut injector,
                &mut http_fault_context,
                &store,
                &harness.store,
                &prefix,
                &config,
                true,
                &client,
                &model,
                op_index,
                crash,
            )
            .await;
            if !recovery.is_empty() {
                failed = true;
                failure_violations = recovery;
                break;
            }
        }

        let deferred_count = u64::try_from(deferred_ops.len() + clean_late_followups.len())
            .expect("deferred operation count must fit in u64");
        let probe_slot_available = op_index
            .checked_add(deferred_count)
            .is_some_and(|reserved| reserved < max_ops);
        if let Some(probe) =
            selftest_probe.filter(|_| pending_held_op.is_none() && probe_slot_available)
        {
            if let Some(probe_op) = selftest_probe_op(probe, &op, &model, &mut generator) {
                let commands =
                    advance_scheduled_faults(scheduler.as_ref(), test_clock.as_ref(), op_index);
                let remaining = operational_state
                    .apply_node_commands(
                        commands,
                        NodeCommandContext {
                            scheduler: scheduler.as_ref(),
                            store: &store,
                            shared_clock: injected_clock(test_clock.as_ref()),
                            operational_observer: operational_observer.as_ref(),
                            require_compaction_evidence,
                            prefix: &prefix,
                            config: &config,
                            admin_bearer: &server.admin_bearer,
                            disk_cache_max_bytes,
                            op_index,
                        },
                    )
                    .await;
                let environment = Box::pin(operational_state.apply_environment_commands(
                    remaining,
                    EnvironmentCommandContext {
                        scheduler: scheduler.as_ref(),
                        operational_observer: operational_observer.as_ref(),
                        client: &client,
                        primary: &server,
                        model: &mut model,
                        op_index,
                    },
                ))
                .await;
                if !environment.violations.is_empty() {
                    failed = true;
                    failure_violations = environment.violations;
                    break;
                }
                if let Some(crash) = environment.crash {
                    let scheduler = scheduler
                        .as_ref()
                        .expect("operational process crash requires a fault scheduler");
                    let controller = scheduler
                        .process_controller()
                        .expect("operational process crash requires a controller");
                    let recovery = restart_after_crash(
                        &mut server,
                        &controller,
                        scheduler,
                        &mut injector,
                        &mut http_fault_context,
                        &store,
                        &harness.store,
                        &prefix,
                        &config,
                        true,
                        &client,
                        &model,
                        op_index,
                        crash,
                    )
                    .await;
                    if !recovery.is_empty() {
                        failed = true;
                        failure_violations = recovery;
                        break;
                    }
                }
                assert_recorded_op_matches(recorded_ops.as_deref(), seed, op_index, &probe_op);
                let target_node = operational_state.choose_target_node_for_op(&probe_op);
                let target_server = operational_state.target(&server, target_node);
                let op_http_fault_context = http_fault_context
                    .as_ref()
                    .map(|context| context.for_node(target_server));
                let inject_post_commit_ack_loss = post_commit_selftest
                    && !post_commit_ack_loss_fired
                    && matches!(probe_op, Op::Upsert { .. });
                let step = execute_recorded_op(
                    &client,
                    target_server,
                    &mut artifacts,
                    &mut model,
                    &mut coverage,
                    &mut s3_tracker,
                    &mut corruption_tracker,
                    &probe_op,
                    op_index,
                    started,
                    mutation,
                    mode,
                    ExecutionPhase::Workload,
                    operational_state.generation_checkpoints_enabled(),
                    target_node,
                    op_http_fault_context.as_ref(),
                    inject_post_commit_ack_loss,
                )
                .await;
                let pending_crash = step.crash.clone().or_else(|| {
                    scheduler
                        .as_ref()
                        .and_then(FaultScheduler::process_controller)
                        .and_then(|controller| controller.try_take_request())
                });
                post_commit_ack_loss_fired |= step.post_commit_ack_lost;
                op_index += 1;
                if !step.violations.is_empty() {
                    failed = true;
                    failure_violations = step.violations;
                    break;
                }
                if let Some(crash) = pending_crash {
                    let scheduler = scheduler
                        .as_ref()
                        .expect("process crash requires a fault scheduler");
                    let controller = scheduler
                        .process_controller()
                        .expect("process crash requires a process controller");
                    let recovery = restart_after_crash(
                        &mut server,
                        &controller,
                        scheduler,
                        &mut injector,
                        &mut http_fault_context,
                        &store,
                        &harness.store,
                        &prefix,
                        &config,
                        true,
                        &client,
                        &model,
                        op_index,
                        crash,
                    )
                    .await;
                    if !recovery.is_empty() {
                        failed = true;
                        failure_violations = recovery;
                        break;
                    }
                }
            }
        }
    }

    let deferred_drain_count =
        u64::try_from(quiet_drain_ops.len()).expect("deferred drain count must fit in u64");
    let expected_workload_count = op_index
        .checked_add(deferred_drain_count)
        .expect("expected workload count overflowed");
    let pre_quiet_accounting = workload_accounting_snapshot(
        expected_workload_count,
        artifacts.completed_operation_ids(),
        pending_held_op
            .as_ref()
            .map(|pending: &PendingHeldOp| pending.op_index),
        op_index,
        deferred_drain_count,
    );
    if !failed {
        assert!(
            deferred_ops.is_empty(),
            "workload ended with {} generated operations still deferred",
            deferred_ops.len()
        );
        assert!(
            clean_late_followups.is_empty(),
            "workload ended with {} mandatory clean late-query follow-ups pending",
            clean_late_followups.len()
        );
        if generation_cap_reached {
            assert_eq!(
                expected_workload_count, max_ops,
                "max_ops must remain the executed workload record count"
            );
        }
        assert_pre_quiet_workload_accounting(
            &pre_quiet_accounting,
            "pre-quiet workload accounting",
        );
        if pending_held_op.is_none() && quiet_drain_ops.is_empty() {
            let workload_records = read_ops(&artifacts.dir);
            assert_contiguous_record_indices(
                &workload_records,
                op_index,
                "generated workload trace",
            );
        }
    }

    let exact_quiescent_vector_count = operational_state.quiescent_vector_count_must_be_exact();
    let object_store_in_run = object_store_breakdown(&counter);
    if let Some(watchdog) = watchdog {
        watchdog.update(
            "quiet-period",
            op_index,
            scheduler.as_ref(),
            pending_held_op.as_ref(),
            deferred_ops.len(),
            quiet_drain_ops.len(),
        );
    }
    let quiet = QuietPeriod {
        client: &client,
        server: &mut server,
        scheduler: scheduler.as_ref(),
        test_clock: test_clock.as_ref(),
        injector: &mut injector,
        http_fault_context: &mut http_fault_context,
        chaos: chaos_handle.as_ref(),
        operational_state: &mut operational_state,
        operational_observer: operational_observer.as_ref(),
        pending_held_op: &mut pending_held_op,
        dual_writer_lease_hold: &mut dual_writer_lease_hold,
        initial_dual_writer_stale_fencing_token: dual_writer_stale_fencing_token,
        artifacts: &mut artifacts,
        model: &mut model,
        coverage: &mut coverage,
        s3_tracker: &mut s3_tracker,
        corruption_tracker: &mut corruption_tracker,
        created_namespaces: &mut created_namespaces,
        background_compaction_starts: &mut background_compaction_starts,
        op_index: &mut op_index,
        compactions: &mut compactions,
        started,
        mutation,
        mode,
        exact_vector_count: exact_quiescent_vector_count,
        verify: !failed,
        preserve_recorded_holds: false,
        prefix: &prefix,
        config: &config,
        disk_cache_max_bytes,
        drain_ops: &mut quiet_drain_ops,
    }
    .run()
    .await;
    post_commit_ack_loss_fired |= quiet.post_commit_ack_lost;
    let records = read_ops(&artifacts.dir);
    let (_, workload_records) = replay_workload_records(&records);
    let remaining_quiet_drain_count =
        u64::try_from(quiet_drain_ops.len()).expect("remaining quiet-drain count must fit in u64");
    let remaining_quiet_drain_start = expected_workload_count
        .checked_sub(remaining_quiet_drain_count)
        .expect("remaining quiet-drain count exceeds selected workload count");
    let post_quiet_accounting = workload_accounting_snapshot(
        expected_workload_count,
        workload_records.iter().map(|record| record.index).collect(),
        pending_held_op
            .as_ref()
            .map(|pending: &PendingHeldOp| pending.op_index),
        remaining_quiet_drain_start,
        remaining_quiet_drain_count,
    );
    write_workload_accounting_artifact(
        &artifacts.dir,
        pre_quiet_accounting,
        post_quiet_accounting.clone(),
    );
    if !failed
        && quiet.violations.is_empty()
        && pending_held_op.is_none()
        && quiet_drain_ops.is_empty()
    {
        assert_workload_accounting_bijection(
            &post_quiet_accounting,
            "post-quiet workload accounting",
        );
        assert_contiguous_record_indices(
            &workload_records,
            expected_workload_count,
            "generated post-quiet workload trace",
        );
    }
    if !quiet.violations.is_empty() {
        if !failed {
            failure_violations = quiet.violations;
        } else {
            failure_violations.extend(quiet.violations);
        }
        failed = true;
    }

    if dual_writer_fencing_selftest {
        assert!(
            dual_writer_lease_hold_activated,
            "dual-writer selftest never activated its pinned lease HoldCall"
        );
        assert_dual_writer_lease_hold_timeline(
            scheduler
                .as_ref()
                .expect("dual-writer selftest requires a scheduler"),
        );
    }

    if mutation == Some(OracleMutation::ChaosLostWrite) {
        assert!(
            chaos_handle.as_ref().is_some_and(|handle| {
                handle
                    .fired()
                    .iter()
                    .any(|fault| fault.site_id == "chaos-lost-write")
            }),
            "chaos lost-write selftest never exercised its pinned WAL SilentDrop"
        );
    }

    if post_commit_selftest {
        assert!(
            post_commit_ack_loss_fired,
            "post-commit selftest never lost an applied HTTP acknowledgement"
        );
        assert!(
            chaos_handle.as_ref().is_some_and(|handle| {
                handle
                    .fired()
                    .iter()
                    .any(|fault| fault.site_id == "post-commit-lost-write")
            }),
            "post-commit selftest never exercised manifest acknowledgement recovery"
        );
    }

    if assignment.profile == Some(FaultProfile::LateStream) && !failed {
        let scheduler = scheduler
            .as_ref()
            .expect("late-stream campaign requires a fault scheduler");
        let fired_event_ids = scheduler
            .timeline()
            .into_iter()
            .map(|event| event.event_id)
            .collect::<BTreeSet<_>>();
        let missing_event_ids = scheduler
            .schedule()
            .events
            .iter()
            .filter(|event| !fired_event_ids.contains(&event.id))
            .map(|event| event.id.clone())
            .collect::<Vec<_>>();
        if !missing_event_ids.is_empty() {
            failed = true;
            failure_violations.push(Violation {
                id: ViolationId::I33LateWorkerLifecycle,
                op_index,
                namespace: "late-stream".to_string(),
                detail: "late-stream campaign finished without firing every scheduled fault"
                    .to_string(),
                evidence: json!({ "missing_event_ids": missing_event_ids }),
            });
        }
    }

    let audit_store = server.store.clone();
    let audit_day = server.clock.now().date_naive();
    let audit_node_id = server.audit_node_id.clone();
    if let Some(watchdog) = watchdog {
        watchdog.update(
            "server-shutdown",
            op_index,
            scheduler.as_ref(),
            pending_held_op.as_ref(),
            deferred_ops.len(),
            quiet_drain_ops.len(),
        );
    }
    drop(client);
    server.into_inner().shutdown().await;
    if let Some(watchdog) = watchdog {
        watchdog.update(
            "artifact-finalization",
            op_index,
            scheduler.as_ref(),
            pending_held_op.as_ref(),
            deferred_ops.len(),
            quiet_drain_ops.len(),
        );
    }
    if security_program_enabled {
        let verification =
            zeppelin::security::verify_audit_day(&audit_store, audit_day, &audit_node_id)
                .await
                .unwrap_or_else(|error| panic!("signed audit-chain verification failed: {error}"));
        coverage.record_security_oracle("I25");
        if !verification.valid {
            failed = true;
            failure_violations.push(Violation {
                id: ViolationId::I25AuditEvidence,
                op_index,
                namespace: "_audit".to_string(),
                detail: "signed audit day failed after graceful runner shutdown".to_string(),
                evidence: serde_json::to_value(verification)
                    .expect("audit verification report must serialize"),
            });
        }
    }

    artifacts.write_model_final(&model);
    artifacts.write_s3_final(&store, &created_namespaces).await;
    artifacts.write_coverage(&coverage);
    let fired_faults = chaos_handle
        .as_ref()
        .map(ChaosHandle::fired)
        .unwrap_or_default();
    if active_profile == Some(FaultProfile::LegacyChaos) {
        artifacts.write_faults(&fired_faults);
    }
    let mut timeline = scheduler
        .as_ref()
        .map(FaultScheduler::timeline)
        .unwrap_or_default();
    timeline.extend(quiet.timeline);
    artifacts.write_timeline(&timeline);
    let object_store_total = object_store_breakdown(&counter);
    let object_store = ObjectStorePhaseCensus {
        quiet_period: object_store_delta(&object_store_total, &object_store_in_run),
        in_run: object_store_in_run,
    };
    let background_compactions = background_compactions_since(&background_compaction_starts);

    if failed {
        let failure_op_index = failure_violations
            .first()
            .map_or(op_index, |violation| violation.op_index);
        artifacts
            .capture_s3_metadata(
                &store,
                &created_namespaces,
                std::env::var("ZEPPELIN_ADVERSARIAL_DUMP_S3").as_deref() == Ok("full"),
            )
            .await;
        let replay_max_ops = failure_op_index + 1;
        let backend = env
            .env_echo
            .get("TEST_BACKEND")
            .map(String::as_str)
            .unwrap_or("memory");
        let repro_env = reproduction_environment(backend, mutation, active_profile);
        artifacts.write_failure(&FailureManifest {
            seed,
            mode,
            op_index: failure_op_index,
            violations: failure_violations.clone(),
            preserved_prefix: prefix.clone(),
            fault_plan: mutation.map(|mutation| mutation.key().to_string()),
            repro_cmd: format!(
                "{repro_env} ZEPPELIN_ADVERSARIAL_REPLAY={} ZEPPELIN_ADVERSARIAL_MAX_OPS={} cargo test --test adversarial_workload_tests replay_seed -- --ignored --nocapture",
                artifacts.dir.display(),
                replay_max_ops
            ),
            inspect_cmd: format!(
                "TEST_BACKEND={} ZEPPELIN_ADVERSARIAL_INSPECT={} cargo test --test adversarial_workload_tests inspect -- --ignored --nocapture",
                backend,
                artifacts.dir.display()
            ),
        });
    }

    if should_cleanup(env.preserve, failed) {
        for ns in &created_namespaces {
            cleanup_ns(&store, ns).await;
        }
        harness.cleanup().await;
    } else {
        println!("preserved adversarial prefix {prefix}");
    }

    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    println!(
        "seed {}: failed={} ops={} compactions={} background_compactions={} ops/sec={:.2}",
        seed,
        failed,
        op_index,
        compactions,
        background_compactions,
        op_index as f64 / elapsed
    );

    let blocking_v1 = mutation.is_none()
        && scheduler
            .as_ref()
            .is_none_or(|scheduler| scheduler.schedule().blocks_v1());
    SeedOutcome {
        mode,
        profile: active_profile,
        failed,
        blocking_v1,
        ops: op_index,
        compactions,
        background_compactions,
        coverage,
        violations: failure_violations,
        wall_secs: elapsed,
        object_store,
        fired_faults,
        repaired_terminal_lifecycle: false,
        repaired_clone_publication: false,
    }
}

#[derive(Debug)]
struct StepOutcome {
    op: Op,
    status: u16,
    violations: Vec<Violation>,
    post_commit_ack_lost: bool,
    crash: Option<CrashRequest>,
}

#[derive(Debug)]
struct CleanLateFollowup {
    faulted_op_index: u64,
    op: Op,
}

fn take_step_crash(step: &StepOutcome, scheduler: Option<&FaultScheduler>) -> Option<CrashRequest> {
    step.crash.clone().or_else(|| {
        scheduler
            .and_then(FaultScheduler::process_controller)
            .and_then(|controller| controller.try_take_request())
    })
}

struct PendingHeldOp {
    event_id: String,
    window_op: u64,
    scheduled_release_op: u64,
    release_op: u64,
    release_cause: HoldReleaseCause,
    op_index: u64,
    namespace: String,
    holds_query_admission: bool,
    task: JoinHandle<RawRecordedOp>,
}

fn preserve_recorded_hold_after_early_completion(
    raw: Box<RawRecordedOp>,
    recorded_hold: &HeldExecutionMetadata,
    op: &Op,
    op_index: u64,
    mutation: Option<OracleMutation>,
    model: &mut Model,
) -> PendingHeldOp {
    // Exact replay preserves the recorded logical join even when cache state or
    // a concurrent process crash lets the request finish before the store hold
    // waiter wins. Pending operations are namespace-isolated, so delaying the
    // already-completed result preserves the recorded causal boundary without
    // executing the operation twice.
    if op.is_mutating() {
        model.apply_outcome(
            op,
            &OpOutcome::Ambiguous {
                reason: AmbiguityReason::HeldInFlight,
                status: None,
            },
            None,
            mutation,
            op_index,
        );
    }
    let raw = *raw;
    PendingHeldOp {
        event_id: recorded_hold.event_id.clone(),
        window_op: recorded_hold.window_op,
        scheduled_release_op: recorded_hold
            .scheduled_release_op
            .unwrap_or(recorded_hold.actual_join_op),
        release_op: recorded_hold.actual_join_op,
        release_cause: recorded_hold.release_cause,
        op_index,
        namespace: op.namespace().to_string(),
        holds_query_admission: op_uses_query_admission(op),
        task: tokio::spawn(async move { raw }),
    }
}

enum QuietDrainOp {
    Generated {
        op: Op,
        inject_post_commit_ack_loss: bool,
    },
    Replay {
        source: Box<OpRecord>,
        op: Op,
        inject_post_commit_ack_loss: bool,
    },
}

fn replayed_hold_releases_before_nominal(
    scheduler: &FaultScheduler,
    pending: &PendingHeldOp,
) -> bool {
    let event = scheduler
        .schedule()
        .events
        .iter()
        .find(|event| event.id == pending.event_id)
        .unwrap_or_else(|| {
            panic!(
                "recorded foreground hold references missing schedule event {}",
                pending.event_id
            )
        });
    assert_eq!(
        event.boundary,
        Boundary::ObjectStore,
        "recorded foreground hold event {} changed boundary",
        pending.event_id
    );
    let FaultKind::HoldCall { for_ops } = event.kind else {
        panic!(
            "recorded foreground hold event {} is not HoldCall",
            pending.event_id
        );
    };
    let nominal_release_op = pending
        .window_op
        .checked_add(for_ops)
        .expect("recorded foreground hold nominal release overflowed");
    assert!(
        pending.release_op >= pending.window_op,
        "recorded foreground hold {} releases before its window",
        pending.event_id
    );
    match pending.release_cause {
        HoldReleaseCause::Legacy => pending.release_op < nominal_release_op,
        HoldReleaseCause::LogicalOp => {
            assert_eq!(
                pending.scheduled_release_op, nominal_release_op,
                "recorded foreground hold {} changed its scheduled release",
                pending.event_id
            );
            assert_eq!(
                pending.release_op, pending.scheduled_release_op,
                "logical foreground hold {} joined away from its scheduled release",
                pending.event_id
            );
            false
        }
        HoldReleaseCause::Quiesce => {
            assert_eq!(
                pending.scheduled_release_op, nominal_release_op,
                "quiesced foreground hold {} changed its scheduled release",
                pending.event_id
            );
            assert!(
                pending.release_op <= pending.scheduled_release_op,
                "quiesced foreground hold {} joined after its scheduled release",
                pending.event_id
            );
            true
        }
    }
}

struct DualWriterLeaseHoldActivation {
    release_op: u64,
    namespace: String,
    stale_fencing_token: u64,
    node_a_renew: JoinHandle<Result<Lease, ZeppelinError>>,
    node_b_manager: Arc<LeaseManager>,
    node_b_lease: Lease,
}

async fn renew_or_observe_takeover(
    manager: &LeaseManager,
    namespace: &str,
    lease: &Lease,
) -> Result<Lease, ZeppelinError> {
    match manager.renew(namespace, lease).await {
        // Older renewal surfaced the lost CAS directly. CAS-first renewal now
        // classifies that conflict authoritatively and reports LeaseExpired.
        // The selftest still performs one explicit acquire probe so its proof
        // records the exact successor holder instead of weakening the oracle.
        Err(ZeppelinError::ManifestConflict { .. } | ZeppelinError::LeaseExpired { .. }) => {
            manager.acquire(namespace).await
        }
        result => result,
    }
}

async fn begin_dual_writer_lease_hold(
    scheduler: &FaultScheduler,
    bookkeeping_store: &ZeppelinStore,
    node_a_store: ZeppelinStore,
    node_b_store: ZeppelinStore,
    test_clock: Arc<TestClock>,
    namespace: &str,
    op_index: u64,
) -> DualWriterLeaseHoldActivation {
    let hold = scheduler
        .foreground_hold_for_calls(
            op_index,
            &[(StoreOp::Put, format!("{namespace}/lease.json"))],
        )
        .unwrap_or_else(|| {
            panic!("dual-writer selftest has no active lease HoldCall at op {op_index}")
        });
    assert_eq!(
        hold.event_id, DUAL_WRITER_LEASE_HOLD_EVENT_ID,
        "dual-writer activation selected the wrong HoldCall"
    );

    let lease_duration = Duration::from_secs(30);
    let shared_clock = Clock::from_source(test_clock.clone());
    let node_a_holder = "adversarial-dual-writer-node-a".to_string();
    let node_a_initializer = LeaseManager::with_clock(
        bookkeeping_store.clone(),
        node_a_holder.clone(),
        lease_duration,
        shared_clock.clone(),
    );
    let node_a_lease = node_a_initializer
        .acquire(namespace)
        .await
        .unwrap_or_else(|error| {
            panic!("dual-writer selftest node A could not acquire token 1: {error}")
        });
    assert_eq!(
        node_a_lease.fencing_token, 1,
        "dual-writer selftest must begin with node A's exact token 1"
    );
    let node_a_manager = Arc::new(LeaseManager::with_clock(
        node_a_store,
        node_a_holder,
        lease_duration,
        shared_clock.clone(),
    ));
    let node_b_manager = Arc::new(LeaseManager::with_clock(
        node_b_store,
        "adversarial-dual-writer-node-b".to_string(),
        lease_duration,
        shared_clock.clone(),
    ));

    test_clock.jump(31_000);
    assert!(
        node_a_lease.expires_at < shared_clock.now(),
        "shared test clock did not expire node A's token 1"
    );

    let node_a_namespace = namespace.to_string();
    let stale_fencing_token = node_a_lease.fencing_token;
    let armed_scheduler = scheduler.clone();
    let hold_event_id = hold.event_id.clone();
    let node_a_renew = tokio::spawn(async move {
        armed_scheduler
            .with_armed_hold(hold_event_id, async move {
                renew_or_observe_takeover(&node_a_manager, &node_a_namespace, &node_a_lease).await
            })
            .await
    });
    let active = scheduler
        .wait_for_hold_window_active(DUAL_WRITER_LEASE_HOLD_EVENT_ID, hold.window_op)
        .await;
    assert_eq!(active.op_index, op_index);
    assert_eq!(active.semantics, FaultSemantics::WindowActive);

    let node_b_lease = node_b_manager
        .acquire(namespace)
        .await
        .unwrap_or_else(|error| {
            panic!("dual-writer selftest node B failed to win lease takeover: {error}")
        });
    assert_eq!(
        node_b_lease.fencing_token,
        stale_fencing_token
            .checked_add(1)
            .expect("dual-writer selftest fencing token overflow"),
        "node B did not win the exact next fencing token"
    );
    assert!(
        !node_a_renew.is_finished(),
        "node A lease renewal escaped the pinned HoldCall"
    );

    DualWriterLeaseHoldActivation {
        release_op: hold.release_op,
        namespace: namespace.to_string(),
        stale_fencing_token,
        node_a_renew,
        node_b_manager,
        node_b_lease,
    }
}

async fn finish_dual_writer_lease_hold(
    scheduler: &FaultScheduler,
    activation: DualWriterLeaseHoldActivation,
) -> u64 {
    let node_a_result = activation
        .node_a_renew
        .await
        .unwrap_or_else(|error| panic!("dual-writer node A renewal task failed: {error}"));
    match node_a_result {
        Err(ZeppelinError::LeaseHeld { holder, .. }) => assert_eq!(
            holder, "adversarial-dual-writer-node-b",
            "node A renewal did not observe node B's authoritative takeover"
        ),
        Ok(lease) => panic!(
            "product fencing failure: held node A lease renewal succeeded after node B won; \
             node_a_token={} node_b_token={}",
            lease.fencing_token, activation.node_b_lease.fencing_token
        ),
        Err(error) => {
            panic!(
                "dual-writer node A renewal returned an unexpected error after takeover: {error}"
            )
        }
    }
    activation
        .node_b_manager
        .release(&activation.namespace, &activation.node_b_lease)
        .await
        .unwrap_or_else(|error| {
            panic!("dual-writer selftest could not release node B lease: {error}")
        });
    assert_dual_writer_lease_hold_timeline(scheduler);
    activation.stale_fencing_token
}

fn dual_writer_lease_hold_timeline(scheduler: &FaultScheduler) -> Vec<TimelineEvent> {
    scheduler
        .timeline()
        .into_iter()
        .filter(|event| event.event_id == DUAL_WRITER_LEASE_HOLD_EVENT_ID)
        .collect()
}

fn assert_dual_writer_lease_hold_timeline(scheduler: &FaultScheduler) {
    let timeline = dual_writer_lease_hold_timeline(scheduler);
    assert_eq!(
        timeline.len(),
        2,
        "dual-writer lease HoldCall must record one start and one end: {timeline:#?}"
    );
    assert_eq!(timeline[0].boundary, Boundary::ObjectStore);
    assert_eq!(timeline[0].semantics, FaultSemantics::WindowActive);
    assert_eq!(timeline[1].boundary, Boundary::ObjectStore);
    assert_eq!(timeline[1].semantics, FaultSemantics::WindowEnd);
    assert_eq!(timeline[0].op_index, timeline[1].op_index);
}

fn apply_step_bookkeeping(
    step: &StepOutcome,
    created_namespaces: &mut Vec<String>,
    background_compaction_starts: &mut BTreeMap<String, u64>,
    compactions: &mut u64,
) {
    if !(200..300).contains(&step.status) {
        return;
    }
    match &step.op {
        Op::CreateNamespace { ns, .. } => {
            note_background_compaction_namespace(background_compaction_starts, ns);
            created_namespaces.push(ns.clone());
        }
        Op::CloneNamespace { target, .. } => {
            note_background_compaction_namespace(background_compaction_starts, target);
            created_namespaces.push(target.clone());
        }
        Op::DeleteNamespace { ns, .. } => {
            created_namespaces.retain(|created| created != ns);
        }
        Op::CompactInline { .. } | Op::CompactEndpoint { .. } | Op::ProbeSandwich { .. } => {
            *compactions += 1;
        }
        _ => {}
    }
}

fn foreground_hold_calls(op: &Op) -> Vec<(StoreOp, String)> {
    let ns = op.namespace();
    let mut calls = match op {
        Op::CreateNamespace { .. } => Vec::new(),
        _ => vec![(StoreOp::Get, format!("{ns}/manifest.json"))],
    };
    if matches!(
        op,
        Op::FetchVectors { .. }
            | Op::Query { .. }
            | Op::LateQuery { .. }
            | Op::BatchQuery { .. }
            | Op::PaginateAll { .. }
            | Op::Hydrate { .. }
    ) {
        calls.push((StoreOp::Get, format!("{ns}/segments/cluster_")));
    }
    if matches!(op, Op::GcCycle { .. }) {
        calls.push((StoreOp::List, format!("{ns}/")));
    }
    if matches!(
        op,
        Op::CompactEndpoint { .. } | Op::CompactInline { .. } | Op::ProbeSandwich { .. }
    ) {
        calls.push((StoreOp::Put, format!("{ns}/lease.json")));
    }
    calls
}

fn mutation_conflicts_with_held_namespace(op: &Op, held_namespace: &str) -> bool {
    op.is_mutating() && op.namespace() == held_namespace
}

fn op_conflicts_with_held_namespace(op: &Op, held_namespace: &str) -> bool {
    op.namespace() == held_namespace
}

fn op_can_run_while_hold_is_pending(op: &Op, held_namespace: &str) -> bool {
    !op.is_mutating() && !op_conflicts_with_held_namespace(op, held_namespace)
}

fn op_uses_query_admission(op: &Op) -> bool {
    matches!(
        op,
        Op::Query { .. } | Op::LateQuery { .. } | Op::BatchQuery { .. } | Op::PaginateAll { .. }
    ) || matches!(
        op,
        Op::InvalidProbe {
            probe: InvalidProbe::NanVector
                | InvalidProbe::OversizedBatch
                | InvalidProbe::UnknownField
                | InvalidProbe::BadCursorToken
                | InvalidProbe::GroupingPlusCursor
                | InvalidProbe::WeightsLenMismatch
                | InvalidProbe::AsOfGenZero
                | InvalidProbe::AsOfGenFuture,
            ..
        }
    )
}

fn op_requires_exact_query_error(op: &Op) -> bool {
    matches!(
        op,
        Op::Query {
            q: GeneratedQuery {
                class: QueryOracleClass::ExpectError { .. },
                ..
            },
            ..
        }
    ) || matches!(op, Op::InvalidProbe { .. }) && op_uses_query_admission(op)
}

fn foreground_hold_for_op(
    scheduler: Option<&FaultScheduler>,
    op: &Op,
    op_index: u64,
) -> Option<ForegroundHold> {
    foreground_hold_for_op_excluding(scheduler, op, op_index, None)
}

fn foreground_hold_for_op_excluding(
    scheduler: Option<&FaultScheduler>,
    op: &Op,
    op_index: u64,
    excluded_event_id: Option<&str>,
) -> Option<ForegroundHold> {
    let scheduler = scheduler?;
    let calls = foreground_hold_calls(op);
    (!calls.is_empty())
        .then(|| scheduler.foreground_hold_for_calls_excluding(op_index, &calls, excluded_event_id))
        .flatten()
}

fn op_conflicts_with_pending_hold(
    op: &Op,
    op_index: u64,
    scheduler: Option<&FaultScheduler>,
    pending: &PendingHeldOp,
) -> bool {
    !op_can_run_while_hold_is_pending(op, &pending.namespace)
        || pending.holds_query_admission && op_requires_exact_query_error(op)
        || foreground_hold_for_op_excluding(scheduler, op, op_index, Some(&pending.event_id))
            .is_some()
}

/// A namespace delete waits in-op until the purge converges to 404, so its
/// logical index cannot advance past a scheduled object-store fault window
/// that keeps failing purge deletes — the window can only close when the op
/// index moves. Defer the delete, and every later op touching the same
/// namespace (notably the scripted recreate), until the window has passed.
fn op_awaits_store_fault_window_close(
    op: &Op,
    op_index: u64,
    scheduler: Option<&FaultScheduler>,
    deferred: &VecDeque<Op>,
) -> bool {
    let Some(scheduler) = scheduler else {
        return false;
    };
    if let Op::DeleteNamespace { ns, .. } = op {
        if scheduler.scheduled_store_fault_window_active(op_index) {
            return true;
        }
        return deferred_namespace_delete_targets(deferred, ns);
    }
    deferred_namespace_delete_targets(deferred, op.namespace())
}

fn late_stream_fault_probe_for_window(
    scheduler: Option<&FaultScheduler>,
    op_index: u64,
    cached_probe: Option<&Op>,
) -> Option<(String, Op)> {
    let scheduler = scheduler?;
    if scheduler.schedule().profile != FaultProfile::LateStream {
        return None;
    }
    let mut active_events = scheduler.schedule().events.iter().filter(|event| {
        event.boundary == Boundary::ObjectStore
            && event.target.store_op == Some(StoreOp::Get)
            && op_index >= event.start_op
            && event.end_op.is_none_or(|end_op| op_index < end_op)
    });
    let event = active_events.next()?;
    assert!(
        active_events.next().is_none(),
        "late-stream GET fault windows must not overlap"
    );
    let probe = cached_probe.unwrap_or_else(|| {
        panic!(
            "late-stream window {} opened before a clean probe",
            event.id
        )
    });
    assert!(
        matches!(probe, Op::LateQuery { filter: None, .. }),
        "late-stream fault probe must be an unfiltered late query"
    );
    Some((event.id.clone(), probe.clone()))
}

fn deferred_namespace_delete_targets(deferred: &VecDeque<Op>, namespace: &str) -> bool {
    deferred
        .iter()
        .any(|pending| matches!(pending, Op::DeleteNamespace { ns, .. } if ns == namespace))
}

fn next_fifo_deferred_op<G, C>(deferred: &mut VecDeque<Op>, mut generate: G, mut conflicts: C) -> Op
where
    G: FnMut() -> Op,
    C: FnMut(&Op, &VecDeque<Op>) -> bool,
{
    next_fifo_deferred_op_with_budget(deferred, 10_000, &mut generate, &mut conflicts)
        .unwrap_or_else(|| {
            panic!("generator could not produce an op compatible with the pending foreground hold")
        })
}

fn next_fifo_deferred_op_with_budget<G, C>(
    deferred: &mut VecDeque<Op>,
    generation_budget: u64,
    mut generate: G,
    mut conflicts: C,
) -> Option<Op>
where
    G: FnMut() -> Op,
    C: FnMut(&Op, &VecDeque<Op>) -> bool,
{
    let front_runnable = deferred.front().is_some_and(|op| !conflicts(op, deferred));
    if front_runnable {
        return Some(
            deferred
                .pop_front()
                .expect("runnable deferred op disappeared"),
        );
    }

    let attempts = generation_budget.min(10_000);
    for _ in 0..attempts {
        let op = generate();
        if conflicts(&op, deferred) {
            deferred.push_back(op);
        } else {
            return Some(op);
        }
    }
    assert!(
        generation_budget <= attempts,
        "generator could not produce an op compatible with the pending foreground hold"
    );
    None
}

#[derive(Debug, Default)]
struct CorruptionTracker {
    seen_timeline_events: usize,
    tainted: BTreeMap<String, BTreeSet<String>>,
    durably_tainted: BTreeMap<String, BTreeSet<String>>,
}

impl CorruptionTracker {
    fn observe(&mut self, timeline: &[TimelineEvent], namespaces: &[String]) {
        if timeline.len() < self.seen_timeline_events {
            self.seen_timeline_events = 0;
        }
        for event in timeline.iter().skip(self.seen_timeline_events) {
            if event.observed != ObservedResult::Corrupted {
                continue;
            }
            let Some(key) = event.key.as_ref() else {
                continue;
            };
            let namespace = namespaces
                .iter()
                .filter(|namespace| timeline_key_matches_namespace(key, namespace))
                .max_by_key(|namespace| namespace.len());
            if let Some(namespace) = namespace {
                self.tainted
                    .entry(namespace.clone())
                    .or_default()
                    .insert(key.clone());
                if durable_content_corruption(event) {
                    self.durably_tainted
                        .entry(namespace.clone())
                        .or_default()
                        .insert(key.clone());
                }
            }
        }
        self.seen_timeline_events = timeline.len();
    }

    fn tainted_keys(&self, namespace: &str) -> Option<&BTreeSet<String>> {
        self.tainted.get(namespace).filter(|keys| !keys.is_empty())
    }

    fn durably_tainted_keys(&self, namespace: &str) -> Option<&BTreeSet<String>> {
        self.durably_tainted
            .get(namespace)
            .filter(|keys| !keys.is_empty())
    }

    fn retain_reachable(&mut self, namespace: &str, reachable: &BTreeSet<String>) {
        let Some(keys) = self.tainted.get_mut(namespace) else {
            return;
        };
        keys.retain(|key| reachable.contains(key));
        if keys.is_empty() {
            self.tainted.remove(namespace);
        }
        let Some(keys) = self.durably_tainted.get_mut(namespace) else {
            return;
        };
        keys.retain(|key| reachable.contains(key));
        if keys.is_empty() {
            self.durably_tainted.remove(namespace);
        }
    }

    fn forget_namespace(&mut self, namespace: &str) {
        self.tainted.remove(namespace);
        self.durably_tainted.remove(namespace);
    }
}

fn durable_content_corruption(event: &TimelineEvent) -> bool {
    event.semantics == FaultSemantics::PostCommit
        && (event.action.starts_with("Content(TornWrite")
            || event.action.starts_with("Content(MisdirectedWrite)"))
}

fn timeline_key_matches_namespace(key: &str, namespace: &str) -> bool {
    key.split("->").any(|part| {
        part == namespace
            || part
                .strip_prefix(namespace)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn should_observe_lineage(
    mode: RunMode,
    scheduled_profile: Option<FaultProfile>,
    op_index: u64,
) -> bool {
    matches!(
        scheduled_profile,
        Some(FaultProfile::Ops | FaultProfile::FutureArchitecture)
    ) || (op_index % 25 == 0
        && (mode == RunMode::Deterministic
            || matches!(
                scheduled_profile,
                Some(
                    FaultProfile::Content
                        | FaultProfile::Semantic
                        | FaultProfile::ProviderContractAbuse
                        | FaultProfile::SupportedFull
                )
            )))
}

async fn reachable_keys_for_taint(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<Option<BTreeSet<String>>, String> {
    let manifest = Manifest::read(store, namespace)
        .await
        .map_err(|error| error.to_string())?;
    let Some(manifest) = manifest else {
        return Ok(None);
    };
    gc::reachable_keys_with_retained_history_and_staging(
        store,
        namespace,
        &manifest,
        &BTreeSet::new(),
    )
    .await
    .map(Some)
    .map_err(|error| error.to_string())
}

fn unresolved_create_allows_missing_manifest_bookkeeping(model: &Model, op: &Op) -> bool {
    let Op::InvalidProbe { ns, probe, .. } = op else {
        return false;
    };
    probe.is_write_shaped()
        && model.namespaces.get(ns).is_some_and(|namespace| {
            namespace
                .indeterminate_ns
                .iter()
                .any(|effect| matches!(effect, NsIndeterminate::MaybeCreatedNs))
        })
}

#[allow(clippy::too_many_arguments)]
async fn execute_recorded_op(
    client: &Client,
    server: &FullTestServer,
    artifacts: &mut SeedArtifacts,
    model: &mut Model,
    coverage: &mut Coverage,
    s3_tracker: &mut S3Tracker,
    corruption_tracker: &mut CorruptionTracker,
    op: &Op,
    index: u64,
    started: Instant,
    mutation: Option<OracleMutation>,
    mode: RunMode,
    phase: ExecutionPhase,
    generation_checkpoints_enabled: bool,
    target_node: u8,
    http_fault_context: Option<&HttpFaultContext>,
    inject_post_commit_ack_loss: bool,
) -> StepOutcome {
    let allow_missing_manifest_bookkeeping =
        unresolved_create_allows_missing_manifest_bookkeeping(model, op);
    let durably_tainted_keys = corruption_tracker
        .durably_tainted_keys(op.namespace())
        .cloned();
    let mut raw = execute_raw_recorded_op(
        client.clone(),
        OpExecutionTarget::from(server),
        op.clone(),
        index,
        started,
        mode,
        generation_checkpoints_enabled,
        target_node,
        http_fault_context.cloned(),
        allow_missing_manifest_bookkeeping,
        durably_tainted_keys,
        inject_post_commit_ack_loss,
    )
    .await;
    raw.rec.execution = ExecutionMetadata { phase, hold: None };
    finish_recorded_op(
        client,
        artifacts,
        model,
        coverage,
        s3_tracker,
        corruption_tracker,
        raw,
        mutation,
        mode,
        false,
    )
    .await
}

struct RawRecordedOp {
    rec: OpRecord,
    late_read_observation: Option<oracle::LateReadObservation>,
    outcome: OpOutcome,
    post_commit_ack_lost: bool,
    crash: Option<CrashRequest>,
    generation_checkpoints_enabled: bool,
    target: OpExecutionTarget,
    http_fault_context: Option<HttpFaultContext>,
}

#[allow(clippy::too_many_arguments)]
async fn execute_raw_recorded_op(
    client: Client,
    target: OpExecutionTarget,
    op: Op,
    index: u64,
    started: Instant,
    mode: RunMode,
    generation_checkpoints_enabled: bool,
    target_node: u8,
    http_fault_context: Option<HttpFaultContext>,
    allow_missing_manifest_bookkeeping: bool,
    durably_tainted_keys: Option<BTreeSet<String>>,
    inject_post_commit_ack_loss: bool,
) -> RawRecordedOp {
    let ambiguity_allowed = mode == RunMode::Chaos;
    let timeline_start = http_fault_context
        .as_ref()
        .map(|context| context.scheduler.timeline().len());
    let process_controller = http_fault_context
        .as_ref()
        .and_then(|context| context.scheduler.process_controller());
    let request_target = target.clone();
    let ordinary_actor = ordinary_execution_actor(&op);
    if ordinary_actor.is_some()
        && matches!(
            &op,
            Op::GcCycle { .. } | Op::ProbeSandwich { .. } | Op::CompactInline { .. }
        )
    {
        panic!(
            "{} cannot execute as actor {} because it bypasses the HTTP authorization seam",
            op.kind(),
            op.actor().0
        );
    }
    let execution_client = ordinary_actor.map_or_else(
        || client.clone(),
        |actor| target.workload_credentials.client(actor.0, 0),
    );
    let late_read_baseline = matches!(op, Op::LateQuery { .. }).then(|| {
        let counter = target
            .object_store_counter
            .as_ref()
            .expect("late query execution requires a per-seed CountingStore");
        oracle::LateReadObservation {
            candidate_gets: counter.foreground_gets_matching("candidate-cluster-"),
            truth_gets: counter.foreground_gets_matching("/matrix_")
                + counter.foreground_gets_matching("/attrs_"),
        }
    });
    let request = WORKLOAD_REQUEST_ID.scope(
        format!("adv-{index}-{}", op.kind()),
        REQUEST_AMBIGUITY_ALLOWED.scope(
            ambiguity_allowed,
            REQUEST_IS_MUTATION.scope(
                op.is_mutating(),
                Box::pin(execute_op(
                    &execution_client,
                    &request_target,
                    &op,
                    index,
                    started,
                    allow_missing_manifest_bookkeeping,
                    durably_tainted_keys.as_ref(),
                )),
            ),
        ),
    );
    let request = HTTP_FAULT_CONTEXT.scope(http_fault_context.clone(), request);
    tokio::pin!(request);
    let (mut rec, crash) = if let Some(controller) = &process_controller {
        tokio::select! {
            rec = &mut request => (rec, None),
            _ = controller.crash_requested.notified() => {
                let crash = controller.take_request();
                (
                    crashed_op_record(&op, index, started, target_node, &crash),
                    Some(crash),
                )
            }
        }
    } else {
        (request.await, None)
    };
    let late_read_observation = late_read_baseline.map(|baseline| {
        let counter = target
            .object_store_counter
            .as_ref()
            .expect("late query execution requires a per-seed CountingStore");
        oracle::LateReadObservation {
            candidate_gets: counter
                .foreground_gets_matching("candidate-cluster-")
                .checked_sub(baseline.candidate_gets)
                .expect("candidate GET counter must remain monotonic"),
            truth_gets: (counter.foreground_gets_matching("/matrix_")
                + counter.foreground_gets_matching("/attrs_"))
            .checked_sub(baseline.truth_gets)
            .expect("truth GET counter must remain monotonic"),
        }
    });
    rec.target_node = target_node;
    mark_injected_store_failure(
        &mut rec,
        index,
        http_fault_context
            .as_ref()
            .map(|context| &context.scheduler),
        timeline_start,
    );
    let post_commit_ack_lost = inject_post_commit_ack_loss
        && inject_lost_http_acknowledgement_after_commit(&mut rec, ambiguity_allowed);
    let outcome = classify_record_outcome(&rec, ambiguity_allowed);
    rec.outcome = outcome.label();
    if (200..300).contains(&rec.status) {
        match &op {
            Op::CloneNamespace { .. } => {
                rec.gen_after = rec
                    .response
                    .get("generation")
                    .and_then(serde_json::Value::as_u64);
            }
            Op::DeleteNamespace { .. } | Op::PatchIndexConfig { .. } => {}
            _ if op.is_security_op() => {}
            _ if op.is_mutating() => {
                rec.gen_after = if let Some(context) = http_fault_context.as_ref() {
                    if op_records_manifest_generation(&op) {
                        tainted_aware_authoritative_generation(
                            &context.bookkeeping_store,
                            &context.scheduler,
                            op.namespace(),
                            op.kind(),
                        )
                        .await
                    } else {
                        None
                    }
                } else {
                    Some(compact_generation(&client, &target.base_url, op.namespace()).await)
                };
            }
            _ => {}
        }
    }
    RawRecordedOp {
        rec,
        late_read_observation,
        outcome,
        post_commit_ack_lost,
        crash,
        generation_checkpoints_enabled,
        target,
        http_fault_context,
    }
}

#[must_use]
fn ordinary_execution_actor(op: &Op) -> Option<ActorSel> {
    (!op.is_security_op() && op.actor() != ActorSel::ADMIN).then(|| op.actor())
}

enum HoldCandidateOutcome {
    Completed(Box<RawRecordedOp>),
    Held(PendingHeldOp),
}

#[allow(clippy::too_many_arguments)]
async fn execute_hold_candidate(
    scheduler: &FaultScheduler,
    hold: ForegroundHold,
    client: Client,
    target: OpExecutionTarget,
    op: Op,
    index: u64,
    started: Instant,
    mutation: Option<OracleMutation>,
    mode: RunMode,
    generation_checkpoints_enabled: bool,
    target_node: u8,
    http_fault_context: Option<HttpFaultContext>,
    inject_post_commit_ack_loss: bool,
    durably_tainted_keys: Option<BTreeSet<String>>,
    model: &mut Model,
) -> HoldCandidateOutcome {
    let provisional_op = op.clone();
    let allow_missing_manifest_bookkeeping =
        unresolved_create_allows_missing_manifest_bookkeeping(model, &provisional_op);
    let event_id = hold.event_id.clone();
    let namespace = op.namespace().to_string();
    let holds_query_admission = op_uses_query_admission(&op);
    let armed_scheduler = scheduler.clone();
    let armed_event_id = event_id.clone();
    let mut task = tokio::spawn(async move {
        armed_scheduler
            .with_armed_hold(
                armed_event_id,
                execute_raw_recorded_op(
                    client,
                    target,
                    op,
                    index,
                    started,
                    mode,
                    generation_checkpoints_enabled,
                    target_node,
                    http_fault_context,
                    allow_missing_manifest_bookkeeping,
                    durably_tainted_keys,
                    inject_post_commit_ack_loss,
                ),
            )
            .await
    });
    tokio::select! {
        joined = &mut task => HoldCandidateOutcome::Completed(Box::new(
            joined.unwrap_or_else(|error| {
                panic!("foreground hold candidate op {index} task failed: {error}")
            })
        )),
        _ = scheduler.wait_for_hold_window_active(&hold.event_id, hold.window_op) => {
            if provisional_op.is_mutating() {
                model.apply_outcome(
                    &provisional_op,
                    &OpOutcome::Ambiguous {
                        reason: AmbiguityReason::HeldInFlight,
                        status: None,
                    },
                    None,
                    mutation,
                    index,
                );
            }
            HoldCandidateOutcome::Held(PendingHeldOp {
                event_id,
                window_op: hold.window_op,
                scheduled_release_op: hold.release_op,
                release_op: hold.release_op,
                release_cause: HoldReleaseCause::LogicalOp,
                op_index: index,
                namespace,
                holds_query_admission,
                task,
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_pending_held_op(
    pending: PendingHeldOp,
    client: &Client,
    artifacts: &mut SeedArtifacts,
    model: &mut Model,
    coverage: &mut Coverage,
    s3_tracker: &mut S3Tracker,
    corruption_tracker: &mut CorruptionTracker,
    mutation: Option<OracleMutation>,
    mode: RunMode,
) -> StepOutcome {
    let mut raw = pending.task.await.unwrap_or_else(|error| {
        panic!(
            "held foreground op {} task failed while joining: {error}",
            pending.op_index
        )
    });
    assert_eq!(
        raw.rec.index, pending.op_index,
        "held foreground task changed its original op index"
    );
    raw.rec.execution = ExecutionMetadata {
        phase: ExecutionPhase::Workload,
        hold: Some(HeldExecutionMetadata {
            event_id: pending.event_id,
            window_op: pending.window_op,
            scheduled_release_op: Some(pending.scheduled_release_op),
            actual_join_op: pending.release_op,
            release_cause: pending.release_cause,
        }),
    };
    finish_recorded_op(
        client,
        artifacts,
        model,
        coverage,
        s3_tracker,
        corruption_tracker,
        raw,
        mutation,
        mode,
        true,
    )
    .await
}

enum RecordedExecutionOutcome {
    Completed(Box<StepOutcome>),
    Held(PendingHeldOp),
}

#[allow(clippy::too_many_arguments)]
async fn execute_recorded_op_or_hold(
    scheduler: Option<&FaultScheduler>,
    client: &Client,
    server: &FullTestServer,
    artifacts: &mut SeedArtifacts,
    model: &mut Model,
    coverage: &mut Coverage,
    s3_tracker: &mut S3Tracker,
    corruption_tracker: &mut CorruptionTracker,
    op: &Op,
    index: u64,
    started: Instant,
    mutation: Option<OracleMutation>,
    mode: RunMode,
    generation_checkpoints_enabled: bool,
    target_node: u8,
    http_fault_context: Option<&HttpFaultContext>,
    inject_post_commit_ack_loss: bool,
) -> RecordedExecutionOutcome {
    let Some(hold) = foreground_hold_for_op(scheduler, op, index) else {
        return RecordedExecutionOutcome::Completed(Box::new(
            execute_recorded_op(
                client,
                server,
                artifacts,
                model,
                coverage,
                s3_tracker,
                corruption_tracker,
                op,
                index,
                started,
                mutation,
                mode,
                ExecutionPhase::Workload,
                generation_checkpoints_enabled,
                target_node,
                http_fault_context,
                inject_post_commit_ack_loss,
            )
            .await,
        ));
    };
    let scheduler = scheduler.expect("foreground hold prediction requires a scheduler");
    match execute_hold_candidate(
        scheduler,
        hold,
        client.clone(),
        OpExecutionTarget::from(server),
        op.clone(),
        index,
        started,
        mutation,
        mode,
        generation_checkpoints_enabled,
        target_node,
        http_fault_context.cloned(),
        inject_post_commit_ack_loss,
        corruption_tracker
            .durably_tainted_keys(op.namespace())
            .cloned(),
        model,
    )
    .await
    {
        HoldCandidateOutcome::Completed(raw) => RecordedExecutionOutcome::Completed(Box::new(
            finish_recorded_op(
                client,
                artifacts,
                model,
                coverage,
                s3_tracker,
                corruption_tracker,
                *raw,
                mutation,
                mode,
                false,
            )
            .await,
        )),
        HoldCandidateOutcome::Held(pending) => RecordedExecutionOutcome::Held(pending),
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_recorded_op(
    client: &Client,
    artifacts: &mut SeedArtifacts,
    model: &mut Model,
    coverage: &mut Coverage,
    s3_tracker: &mut S3Tracker,
    corruption_tracker: &mut CorruptionTracker,
    raw: RawRecordedOp,
    mutation: Option<OracleMutation>,
    mode: RunMode,
    joined_hold: bool,
) -> StepOutcome {
    let RawRecordedOp {
        rec,
        late_read_observation,
        outcome,
        post_commit_ack_lost,
        crash,
        generation_checkpoints_enabled,
        target,
        http_fault_context,
    } = raw;
    let op = &rec.op;
    let http_fault_context = http_fault_context.as_ref();
    coverage.record(op);
    artifacts.write_op(&rec);
    if joined_hold {
        model.apply_joined_outcome_with_generation_checkpoints(
            op,
            &outcome,
            rec.gen_after,
            mutation,
            rec.index,
            generation_checkpoints_enabled,
        );
    } else {
        model.apply_outcome_with_generation_checkpoints(
            op,
            &outcome,
            rec.gen_after,
            mutation,
            rec.index,
            generation_checkpoints_enabled,
        );
    }
    if (200..300).contains(&rec.status)
        && mutation != Some(OracleMutation::MisdirectedWriteReachability)
        && matches!(
            op,
            Op::CompactInline { .. }
                | Op::CompactEndpoint { .. }
                | Op::ProbeSandwich { .. }
                | Op::GcCycle { .. }
        )
    {
        let reachability_store = http_fault_context
            .map(|context| &context.bookkeeping_store)
            .unwrap_or(&target.store);
        match reachable_keys_for_taint(reachability_store, op.namespace()).await {
            Ok(Some(reachable)) => {
                corruption_tracker.retain_reachable(op.namespace(), &reachable);
            }
            Ok(None) => corruption_tracker.forget_namespace(op.namespace()),
            Err(error) => eprintln!(
                "taint reachability refresh failed for {}: {error}",
                op.namespace()
            ),
        }
    }
    if let Some(context) = http_fault_context {
        corruption_tracker.observe(&context.scheduler.timeline(), &model.namespace_names());
    }
    if (200..300).contains(&rec.status) {
        if let Op::DeleteNamespace { ns, .. } = op {
            s3_tracker.forget_namespace(ns);
            corruption_tracker.forget_namespace(ns);
        }
    }
    let corruption = corruption_tracker
        .tainted_keys(op.namespace())
        .map(|tainted_keys| oracle::CorruptionContext {
            tainted_keys,
            fault_window_active: http_fault_context.is_some_and(|context| {
                context
                    .scheduler
                    .fault_window_active(rec.index, op.namespace())
            }),
        });
    let mut violations =
        oracle::check_op_with_faults(model, &rec, mode, mutation, corruption.as_ref());
    let late_stream_profile = http_fault_context
        .is_some_and(|context| context.scheduler.schedule().profile == FaultProfile::LateStream);
    let late_stream_fault_window_active = late_stream_profile
        && http_fault_context.is_some_and(|context| {
            context
                .scheduler
                .fault_window_active(rec.index, op.namespace())
        });
    if late_stream_profile && matches!(op, Op::LateQuery { .. }) {
        violations.extend(oracle::check_i33_late_worker_lifecycle(
            model,
            &rec,
            late_stream_fault_window_active,
            mutation,
        ));
        if (200..300).contains(&rec.status) {
            violations.extend(oracle::check_i31_late_exact(
                model,
                &rec,
                RunMode::Deterministic,
                mutation,
            ));
        }
    }
    if (mode == RunMode::Deterministic || late_stream_profile)
        && (200..300).contains(&rec.status)
        && matches!(op, Op::LateQuery { .. })
    {
        violations.extend(oracle::check_i32_late_read_accounting(
            &rec,
            late_read_observation
                .expect("successful deterministic late query must retain GET accounting"),
            late_stream_fault_window_active,
            mutation,
        ));
    }
    if matches!(
        mutation,
        Some(
            OracleMutation::ChaosLostWrite
                | OracleMutation::PostCommitLostWrite
                | OracleMutation::IndetResolutionLie
                | OracleMutation::CrashLostAck
        )
    ) && mode == RunMode::Chaos
    {
        violations.clear();
    }
    if (200..300).contains(&rec.status) {
        if let Op::CloneNamespace {
            target: clone_target,
            ..
        } = op
        {
            violations.extend(
                s3_oracle::check_clone_manifest(
                    &target.store,
                    clone_target,
                    &rec.response,
                    rec.index,
                )
                .await,
            );
        }
    }
    let scheduled_profile = http_fault_context.map(|context| context.scheduler.schedule().profile);
    if should_observe_lineage(mode, scheduled_profile, rec.index) {
        for (ns, ns_model) in &model.namespaces {
            if !ns_model.spec.is_exact() {
                continue;
            }
            let known_tainted_keys = corruption_tracker
                .durably_tainted_keys(ns)
                .cloned()
                .unwrap_or_default();
            let status = if let Some(context) = http_fault_context {
                periodic_s3_oracle_status(&context.bookkeeping_store, ns).await
            } else {
                periodic_server_lineage_status(
                    client,
                    &target.store,
                    &target.base_url,
                    ns,
                    &known_tainted_keys,
                )
                .await
            };
            let fault_window_active = http_fault_context
                .is_some_and(|context| context.scheduler.fault_window_active(rec.index, ns));
            violations.extend(
                s3_tracker
                    .check_namespace_with_fault_context(
                        &target.store,
                        ns,
                        rec.index,
                        &status,
                        matches!(
                            mutation,
                            Some(OracleMutation::GcEatsLiveKey | OracleMutation::ClockGcEatsLive)
                        ),
                        fault_window_active,
                        &known_tainted_keys,
                    )
                    .await,
            );
        }
    }
    StepOutcome {
        op: rec.op.clone(),
        status: rec.status,
        violations,
        post_commit_ack_lost,
        crash,
    }
}

fn crashed_op_record(
    op: &Op,
    index: u64,
    started: Instant,
    target_node: u8,
    crash: &CrashRequest,
) -> OpRecord {
    let exchange = ambiguous_exchange(0, AmbiguityReason::ServerCrashed);
    OpRecord {
        index,
        wall_ms: started.elapsed().as_millis() as u64,
        op: op.clone(),
        method: "CRASHED".to_string(),
        path: format!("crash::{:?}::{:?}", crash.point, crash.position),
        status: exchange.status,
        response: exchange.response,
        outcome: String::new(),
        target_node,
        execution: ExecutionMetadata::workload(),
        gen_after: None,
        duration_ms: 0,
        violations: Vec::new(),
    }
}

fn op_records_manifest_generation(op: &Op) -> bool {
    matches!(
        op,
        Op::CreateNamespace { .. }
            | Op::Upsert { .. }
            | Op::LateUpsert { .. }
            | Op::DeleteVectors { .. }
            | Op::CompactEndpoint { .. }
            | Op::ProbeSandwich { .. }
            | Op::CompactInline { .. }
    )
}

fn mark_injected_store_failure(
    rec: &mut OpRecord,
    op_index: u64,
    scheduler: Option<&FaultScheduler>,
    timeline_start: Option<usize>,
) {
    let code = rec.response.get("code").and_then(serde_json::Value::as_str);
    if rec.status < 500 || !matches!(code, Some("STORAGE_ERROR" | "INTERNAL_DATA_MISSING")) {
        return;
    }
    let Some((scheduler, timeline_start)) = scheduler.zip(timeline_start) else {
        return;
    };
    let manifest_key = format!("{}/manifest.json", rec.op.namespace());
    let fault_fired = scheduler
        .timeline()
        .into_iter()
        .skip(timeline_start)
        .any(|event| {
            event.op_index == op_index
                && event.boundary == Boundary::ObjectStore
                && match code {
                    Some("STORAGE_ERROR") => true,
                    Some("INTERNAL_DATA_MISSING") => {
                        event.action.starts_with("HeadGetDiverge")
                            && event.key.as_deref() == Some(manifest_key.as_str())
                    }
                    _ => false,
                }
        });
    if fault_fired {
        rec.response
            .as_object_mut()
            .expect("storage error response must be an object")
            .insert(STORE_FAULT_MARKER.to_string(), json!(true));
    }
}

fn inject_lost_http_acknowledgement_after_commit(
    rec: &mut OpRecord,
    ambiguity_allowed: bool,
) -> bool {
    if !(200..300).contains(&rec.status) {
        return false;
    }
    assert!(
        ambiguity_allowed,
        "post-commit acknowledgement loss is only valid in chaos mode"
    );

    // A post-commit error on the WAL object itself only leaves an orphan: the
    // manifest never references it, so no logical write was committed. A lost
    // manifest acknowledgement is now recovered authoritatively by WalWriter.
    // The deterministic oracle self-test therefore models the remaining sound
    // boundary: the HTTP mutation committed, then its 2xx acknowledgement was
    // lost before the client observed it.
    let ambiguous = ambiguous_exchange(0, AmbiguityReason::ConnectionError);
    rec.status = ambiguous.status;
    rec.response = ambiguous.response;
    true
}

async fn execute_op(
    client: &Client,
    target: &OpExecutionTarget,
    op: &Op,
    index: u64,
    started: Instant,
    allow_missing_manifest_bookkeeping: bool,
    durably_tainted_keys: Option<&BTreeSet<String>>,
) -> OpRecord {
    let before = Instant::now();
    let (method, path, status, response) = match op {
        Op::CreateNamespace { ns, spec, .. } => {
            let path = "/v1/namespaces".to_string();
            let body = spec.create_body(ns);
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(body),
            )
            .await;
            if (200..300).contains(&status) && spec.late_interaction.is_some() {
                activate_late_embedding_profile(&target.store, &target.namespace_manager, ns)
                    .await
                    .unwrap_or_else(|error| {
                        panic!("failed to activate adversarial late profile for {ns}: {error}")
                    });
            }
            ("POST".to_string(), path, status, response)
        }
        Op::GetNamespace { ns, .. } => {
            let path = format!("/v1/namespaces/{ns}");
            let (status, response) = request_json(
                client,
                Method::GET,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            ("GET".to_string(), path, status, response)
        }
        Op::Upsert { ns, vectors, .. } => {
            let path = format!("/v1/namespaces/{ns}/vectors");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({ "vectors": vectors })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::LateUpsert { ns, records, .. } => {
            let path = format!("/v1/namespaces/{ns}/retrieval-units");
            let upserts = records
                .iter()
                .map(|record| {
                    let text = encode_matrix_text(&record.values).unwrap_or_else(|error| {
                        panic!(
                            "failed to encode adversarial late document matrix {}: {error}",
                            record.id
                        )
                    });
                    json!({
                        "id": record.id,
                        "input": { "type": "text", "text": text },
                        "attributes": record.attributes,
                    })
                })
                .collect::<Vec<_>>();
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({ "upserts": upserts })),
            )
            .await;
            if (200..300).contains(&status) {
                let admitted = enrich_pending_retrieval_units(
                    &target.store,
                    Arc::clone(&target.lease_manager),
                    Arc::clone(&target.encoder_provider),
                    &target.namespace_manager,
                    ns,
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("failed to enrich adversarial late upsert for {ns}: {error}")
                });
                assert!(
                    admitted > 0,
                    "successful adversarial late upsert for {ns} must admit enrichment work"
                );
                let compaction = target.compactor.compact(ns).await.unwrap_or_else(|error| {
                    panic!("failed to compact adversarial late namespace {ns}: {error}")
                });
                assert!(
                    compaction.segment_id.is_some(),
                    "adversarial late upsert for {ns} must publish an immutable segment"
                );
            }
            ("POST".to_string(), path, status, response)
        }
        Op::DeleteVectors { ns, ids, .. } => {
            let path = format!("/v1/namespaces/{ns}/vectors");
            let (status, response) = request_json(
                client,
                Method::DELETE,
                &format!("{}{}", target.base_url, path),
                Some(json!({ "ids": ids })),
            )
            .await;
            ("DELETE".to_string(), path, status, response)
        }
        Op::FetchVectors {
            ns,
            ids,
            consistency,
            ..
        } => {
            let path = format!("/v1/namespaces/{ns}/vectors/get");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "ids": ids,
                    "include_vector": true,
                    "include_attributes": true,
                    "consistency": consistency,
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::Query { ns, q, as_of, .. } => {
            let path = if let Some(as_of) = as_of {
                format!("/v1/namespaces/{ns}/query?as_of={as_of}")
            } else {
                format!("/v1/namespaces/{ns}/query")
            };
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(q.body.clone()),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::LateQuery {
            ns,
            query,
            top_k,
            filter,
            consistency,
            ..
        } => {
            let path = format!("/v1/namespaces/{ns}/query");
            let text = encode_matrix_text(query).unwrap_or_else(|error| {
                panic!("failed to encode adversarial late query matrix for {ns}: {error}")
            });
            let mut body = json!({
                "sources": [{
                    "type": "late_interaction",
                    "text": text,
                    "top_k": top_k,
                    "semantic_wait_ms": 5_000,
                }],
                "candidate_k": 64,
                "top_k": top_k,
                "consistency": consistency,
                "projection": { "include_attributes": true },
                "debug": true,
            });
            if let Some(filter) = filter {
                body.as_object_mut()
                    .expect("late query body must be an object")
                    .insert("filter".to_string(), json!(filter));
            }
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(body),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::BatchQuery { ns, qs, .. } => {
            let path = format!("/v1/namespaces/{ns}/query/batch");
            let queries = qs.iter().map(|q| q.body.clone()).collect::<Vec<_>>();
            let (status, batch) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({ "queries": queries })),
            )
            .await;
            let mut individual = Vec::with_capacity(qs.len());
            for q in qs {
                let single_path = format!("/v1/namespaces/{ns}/query");
                let (single_status, single_body) = request_json(
                    client,
                    Method::POST,
                    &format!("{}{}", target.base_url, single_path),
                    Some(q.body.clone()),
                )
                .await;
                individual.push(json!({
                    "status": single_status,
                    "body": single_body
                }));
            }
            (
                "POST".to_string(),
                path,
                status,
                json!({
                    "batch": batch,
                    "individual": individual
                }),
            )
        }
        Op::PaginateAll {
            ns, q, page_size, ..
        } => {
            let path = format!("/v1/namespaces/{ns}/query");
            let mut pages = Vec::new();
            let mut cursor = json!({ "type": "none" });
            let mut status = StatusCode::OK.as_u16();
            for _ in 0..50 {
                let mut page_body = q.body.clone();
                let page_object = page_body.as_object_mut().expect("query body is object");
                page_object.insert("top_k".to_string(), json!(page_size));
                page_object.insert("cursor".to_string(), cursor.clone());
                let (page_status, page_response) = request_json(
                    client,
                    Method::POST,
                    &format!("{}{}", target.base_url, path),
                    Some(page_body),
                )
                .await;
                if !(200..300).contains(&page_status) {
                    status = page_status;
                    pages.push(json!({ "status": page_status, "body": page_response }));
                    break;
                }
                let next = page_response
                    .get("next_cursor")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string);
                pages.push(json!({ "status": page_status, "body": page_response }));
                let Some(next) = next else {
                    break;
                };
                cursor = json!({ "type": "after", "token": next });
            }

            let mut big_body = q.body.clone();
            let paged_result_count = pages
                .iter()
                .filter_map(|page| page["body"]["results"].as_array())
                .map(Vec::len)
                .sum::<usize>()
                .max(*page_size);
            let big_object = big_body.as_object_mut().expect("query body is object");
            big_object.insert("top_k".to_string(), json!(paged_result_count));
            big_object.insert("cursor".to_string(), json!({ "type": "none" }));
            let (big_status, big_response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(big_body),
            )
            .await;
            if !(200..300).contains(&big_status) {
                status = big_status;
            }
            (
                "POST".to_string(),
                path,
                status,
                json!({
                    "pages": pages,
                    "big": {
                        "status": big_status,
                        "body": big_response
                    }
                }),
            )
        }
        Op::InvalidProbe { ns, probe, .. } => {
            let status_before = if probe.is_write_shaped() {
                Some(
                    bookkeeping_compact_status(
                        client,
                        target,
                        ns,
                        allow_missing_manifest_bookkeeping,
                        durably_tainted_keys,
                    )
                    .await,
                )
            } else {
                None
            };
            let (method, path, status, mut response) =
                execute_invalid_probe(client, target, ns, *probe).await;
            if let Some(before) = status_before {
                let after = bookkeeping_compact_status(
                    client,
                    target,
                    ns,
                    allow_missing_manifest_bookkeeping,
                    durably_tainted_keys,
                )
                .await;
                let response_object = response
                    .as_object_mut()
                    .expect("invalid probe error response is object");
                response_object.insert("compact_status_before".to_string(), before);
                response_object.insert("compact_status_after".to_string(), after);
            }
            (method, path, status, response)
        }
        Op::CompactEndpoint { ns, .. } => {
            let path = format!("/v1/namespaces/{ns}/compact");
            let (trigger_status, trigger) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            let mut final_status = trigger_status;
            let mut status_body = serde_json::Value::Null;
            if (200..300).contains(&trigger_status) {
                status_body = wait_compaction_ready(client, &target.base_url, ns).await;
                final_status = StatusCode::OK.as_u16();
            }
            (
                "POST".to_string(),
                path,
                final_status,
                json!({
                    "trigger_status": trigger_status,
                    "trigger": trigger,
                    "status": status_body
                }),
            )
        }
        Op::GcCycle { ns, keep_count, .. } => {
            let path = format!("gc::run_gc_cycle({ns})");
            let config = gc_config(*keep_count);
            let mut runner = gc::GcRunner::new(target.store.clone(), config)
                .with_preservation_service(target.security.preservation_service().cloned());
            let report = runner
                .run_cycle_at(
                    gc::GcNamespaceIncarnation::new(ns.clone(), target.clock.now()),
                    target.clock.now(),
                )
                .await
                .unwrap_or_else(|error| panic!("gc cycle failed for {ns}: {error}"));
            target.manifest_cache.invalidate_at(ns, target.clock.now());
            let retained_generations = Manifest::list_history(&target.store, ns)
                .await
                .unwrap_or_else(|error| panic!("history list after gc failed for {ns}: {error}"))
                .into_iter()
                .map(|entry| entry.version)
                .collect::<Vec<_>>();
            (
                "IN_PROCESS".to_string(),
                path,
                StatusCode::OK.as_u16(),
                json!({
                    "candidates_marked": report.candidates_marked,
                    "objects_deleted": report.objects_deleted,
                    "pending_deletes_deleted": report.pending_deletes_deleted,
                    "pending_deletes_pruned": report.pending_deletes_pruned,
                    "pending_deletes_retained": report.pending_deletes_retained,
                    "bytes_reclaimed": report.bytes_reclaimed,
                    "candidates_skipped": report.candidates_skipped,
                    "retained_generations": retained_generations,
                    "keep_count": keep_count
                }),
            )
        }
        Op::CreateSnapshot { ns, name, .. } => {
            let path = format!("/v1/namespaces/{ns}/snapshots/{name}");
            let (status, response) = request_json(
                client,
                Method::PUT,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            ("PUT".to_string(), path, status, response)
        }
        Op::GetSnapshot { ns, name, .. } => {
            let path = format!("/v1/namespaces/{ns}/snapshots/{name}");
            let (status, response) = request_json(
                client,
                Method::GET,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            ("GET".to_string(), path, status, response)
        }
        Op::ListSnapshots { ns, .. } => {
            let path = format!("/v1/namespaces/{ns}/snapshots");
            let (status, response) = request_json(
                client,
                Method::GET,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            ("GET".to_string(), path, status, response)
        }
        Op::DeleteSnapshot { ns, name, .. } => {
            let path = format!("/v1/namespaces/{ns}/snapshots/{name}");
            let (status, response) = request_json(
                client,
                Method::DELETE,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            ("DELETE".to_string(), path, status, response)
        }
        Op::CloneNamespace {
            source,
            target: clone_target,
            as_of,
            ..
        } => {
            let path = format!("/v1/namespaces/{source}/clone");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "target": clone_target,
                    "as_of": as_of.to_string()
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::PatchIndexConfig { ns, patch, .. } => {
            let path = format!("/v1/namespaces/{ns}/index_config");
            let (status, response) = request_json(
                client,
                Method::PATCH,
                &format!("{}{}", target.base_url, path),
                Some(patch.clone()),
            )
            .await;
            ("PATCH".to_string(), path, status, response)
        }
        Op::Hydrate { ns, .. } => {
            let path = format!("/v1/namespaces/{ns}/hydrate");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::DeleteNamespace { ns, .. } => {
            let path = format!("/v1/namespaces/{ns}");
            let (status, response) = request_json(
                client,
                Method::DELETE,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            if (200..300).contains(&status) {
                wait_namespace_gone(client, &target.base_url, ns).await;
            }
            ("DELETE".to_string(), path, status, response)
        }
        Op::Branching(operation) => {
            execute_branching_http(client, &target.base_url, operation).await
        }
        Op::ProbeSandwich {
            ns, maintenance, ..
        } => {
            let path = format!("probe_sandwich({ns},{maintenance:?})");
            let before = compact_status(client, &target.base_url, ns).await;
            let maintenance_response =
                execute_sandwich_maintenance(client, target, ns, *maintenance).await;
            let after = compact_status(client, &target.base_url, ns).await;
            (
                "COMPOSITE".to_string(),
                path,
                StatusCode::OK.as_u16(),
                json!({
                    "before": before,
                    "maintenance": maintenance_response,
                    "after": after
                }),
            )
        }
        Op::CompactInline { ns, .. } => match target.compactor.compact(ns).await {
            Ok(result) => {
                target.manifest_cache.invalidate_at(ns, target.clock.now());
                (
                    "IN_PROCESS".to_string(),
                    format!("compactor.compact({ns})"),
                    StatusCode::OK.as_u16(),
                    json!({
                        "segment_id": result.segment_id,
                        "vectors_compacted": result.vectors_compacted,
                        "fragments_removed": result.fragments_removed,
                        "old_segment_removed": result.old_segment_removed,
                    }),
                )
            }
            Err(error) => {
                target.manifest_cache.invalidate_at(ns, target.clock.now());
                (
                    "IN_PROCESS".to_string(),
                    format!("compactor.compact({ns})"),
                    StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    json!({
                        "code": "INTERNAL_ERROR",
                        "error": error.to_string(),
                        "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                        "retryable": true,
                        "request_id": "in-process",
                    }),
                )
            }
        },
        Op::CreateKey { .. }
        | Op::RotateKey { .. }
        | Op::RevokeKey { .. }
        | Op::PublishGrantChange { .. }
        | Op::MintToken { .. }
        | Op::UseToken { .. }
        | Op::TokenExceedScopeProbe { .. }
        | Op::UseExpiredToken { .. }
        | Op::RevokeParentThenUseToken { .. }
        | Op::TenantBoundaryProbe { .. }
        | Op::UseRevokedCredential { .. }
        | Op::ForbiddenWriteProbe { .. }
        | Op::ExportProbe { .. }
        | Op::SecurityAdminProbe { .. }
        | Op::AuditBarrierOp { .. }
        | Op::AuditChainCheck { .. }
        | Op::CreateLock { .. }
        | Op::ReleaseLock { .. }
        | Op::DeleteUnderLock { .. }
        | Op::GcUnderLock { .. } => execute_security_op(client, target, op, index).await,
    };

    OpRecord {
        index,
        wall_ms: started.elapsed().as_millis() as u64,
        op: op.clone(),
        method,
        path,
        status,
        response,
        outcome: String::new(),
        target_node: 0,
        execution: ExecutionMetadata::workload(),
        gen_after: None,
        duration_ms: before.elapsed().as_millis() as u64,
        violations: Vec::new(),
    }
}

async fn execute_security_op(
    admin_client: &Client,
    target: &OpExecutionTarget,
    op: &Op,
    op_index: u64,
) -> (String, String, u16, serde_json::Value) {
    let token_client = match op {
        Op::UseToken { token, .. }
        | Op::TokenExceedScopeProbe { token, .. }
        | Op::UseExpiredToken { token, .. }
        | Op::RevokeParentThenUseToken { token, .. } => {
            Some(delegated_token_client(&target.base_url, *token))
        }
        _ => None,
    };
    let selected_key = match op {
        Op::UseRevokedCredential { key } => Some(*key),
        _ if token_client.is_none() && op.actor() != ActorSel::ADMIN => Some(super::ops::KeySel {
            actor: op.actor(),
            retired: 0,
        }),
        _ => None,
    };
    let actor_client =
        selected_key.map(|key| target.workload_credentials.client(key.actor.0, key.retired));
    let client = token_client
        .as_ref()
        .or(actor_client.as_ref())
        .unwrap_or(admin_client);
    let request_id = WORKLOAD_REQUEST_ID
        .try_with(Clone::clone)
        .expect("security operation must have a deterministic request id");

    match op {
        Op::CreateKey {
            subject,
            expires_after_secs,
            ..
        } => {
            let path = "/v1/security/keys".to_string();
            let mut body = json!({
                "principal_id": target.workload_credentials.principal_id(subject.0),
                "name": format!("adversarial-{}-runtime", subject.0),
            });
            if let Some(seconds) = expires_after_secs {
                let expires_at = target
                    .clock
                    .now()
                    .checked_add_signed(chrono::Duration::seconds(
                        i64::try_from(*seconds).expect("key expiry must fit i64 seconds"),
                    ))
                    .expect("key expiry timestamp overflowed");
                body.as_object_mut()
                    .expect("key body must be an object")
                    .insert("expires_at".to_string(), json!(expires_at.to_rfc3339()));
            }
            let (status, mut response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(body),
            )
            .await;
            if (200..300).contains(&status) {
                install_and_redact_credential(
                    &target.workload_credentials,
                    subject.0,
                    &mut response,
                );
                record_success_request_id(&mut response, &request_id);
            }
            ("POST".to_string(), path, status, response)
        }
        Op::RotateKey { key, .. } => {
            let credential = target
                .workload_credentials
                .credential(key.actor.0, key.retired);
            let path = format!("/v1/security/keys/{}/rotate", credential.key_id);
            let (status, mut response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({"overlap_secs": 0})),
            )
            .await;
            if (200..300).contains(&status) {
                install_and_redact_credential(
                    &target.workload_credentials,
                    key.actor.0,
                    &mut response,
                );
                record_success_request_id(&mut response, &request_id);
            }
            ("POST".to_string(), path, status, response)
        }
        Op::RevokeKey { key, .. } => {
            let credential = target
                .workload_credentials
                .credential(key.actor.0, key.retired);
            let path = format!("/v1/security/keys/{}", credential.key_id);
            let (status, mut response) = request_json(
                client,
                Method::DELETE,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            if (200..300).contains(&status) {
                record_success_request_id(&mut response, &request_id);
            }
            ("DELETE".to_string(), path, status, response)
        }
        Op::PublishGrantChange {
            principal,
            grants,
            change,
            ..
        } => {
            assert_eq!(grants.len(), 1, "adversarial grant mutations are atomic");
            let path = "/v1/security/grants".to_string();
            let principal_id = target.workload_credentials.principal_id(principal.0);
            let mut body = security_grant_body(&principal_id, &grants[0]);
            let method = match change {
                GrantChange::Add => Method::POST,
                GrantChange::Remove => {
                    let object = body
                        .as_object_mut()
                        .expect("grant removal body must be an object");
                    object.remove("mandatory_filter");
                    object.remove("write_constraints");
                    Method::DELETE
                }
            };
            let (status, mut response) = request_json(
                client,
                method.clone(),
                &format!("{}{}", target.base_url, path),
                Some(body),
            )
            .await;
            if (200..300).contains(&status) {
                record_success_request_id(&mut response, &request_id);
            }
            (method.to_string(), path, status, response)
        }
        Op::MintToken {
            token, narrowed, ..
        } => {
            let path = "/v1/security/tokens".to_string();
            let (status, mut response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(delegated_token_body(narrowed)),
            )
            .await;
            if (200..300).contains(&status) {
                let bearer = response
                    .get("token")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_else(|| panic!("successful token mint omitted bearer"))
                    .to_string();
                install_delegated_token(&target.base_url, *token, bearer);
                response
                    .as_object_mut()
                    .expect("token mint response must be an object")
                    .insert("token".to_string(), json!("<redacted>"));
                record_success_request_id(&mut response, &request_id);
            }
            ("POST".to_string(), path, status, response)
        }
        Op::UseToken { target_ns, .. }
        | Op::TokenExceedScopeProbe { target_ns, .. }
        | Op::UseExpiredToken { target_ns, .. }
        | Op::RevokeParentThenUseToken { target_ns, .. } => {
            execute_delegated_query(admin_client, client, target, target_ns).await
        }
        Op::TenantBoundaryProbe {
            target_ns, surface, ..
        } => execute_tenant_boundary_probe(admin_client, client, target, target_ns, *surface).await,
        Op::UseRevokedCredential { .. } => {
            let path = "/readyz".to_string();
            let (status, response) = request_json(
                client,
                Method::GET,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            ("GET".to_string(), path, status, response)
        }
        Op::ForbiddenWriteProbe {
            target_ns, kind, ..
        } => execute_forbidden_write_probe(admin_client, client, target, target_ns, *kind).await,
        Op::ExportProbe { target_ns, .. } => {
            let fetch_path = format!("/v1/namespaces/{target_ns}/vectors/get");
            let (fetch_status, fetch_body) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, fetch_path),
                Some(json!({
                    "ids": ["security-export-probe"],
                    "include_vector": true,
                    "include_attributes": true,
                    "consistency": "strong"
                })),
            )
            .await;
            let snapshot_path = format!("/v1/namespaces/{target_ns}/snapshots/security-export");
            let (snapshot_status, snapshot_body) = request_json(
                client,
                Method::GET,
                &format!("{}{}", target.base_url, snapshot_path),
                None,
            )
            .await;
            let status = if fetch_status != 403 {
                fetch_status
            } else {
                snapshot_status
            };
            (
                "COMPOSITE".to_string(),
                format!("export_probe({target_ns})"),
                status,
                json!({
                    "fetch": {"status": fetch_status, "body": fetch_body},
                    "snapshot": {"status": snapshot_status, "body": snapshot_body}
                }),
            )
        }
        Op::SecurityAdminProbe { .. } => {
            let path = "/v1/security/principals".to_string();
            let (status, response) = request_json(
                client,
                Method::GET,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            ("GET".to_string(), path, status, response)
        }
        Op::AuditBarrierOp { .. } => {
            let security_fault = HTTP_FAULT_CONTEXT
                .try_with(Clone::clone)
                .ok()
                .flatten()
                .filter(|context| {
                    context.scheduler.schedule().profile == FaultProfile::Security
                        && op_index == SECURITY_AUDIT_BARRIER_OP
                });
            if security_fault.is_some() {
                target
                    .security
                    .refresh_authoritative_policy_for_test()
                    .await
                    .expect_err(
                        "security profile must drop the scheduled policy-head GET before the audit barrier",
                    );
            }
            let path = "/v1/security/policy".to_string();
            let (status, mut response) = request_json(
                client,
                Method::GET,
                &format!("{}{}", target.base_url, path),
                None,
            )
            .await;
            if (200..300).contains(&status) {
                record_success_request_id(&mut response, &request_id);
            }
            ("GET".to_string(), path, status, response)
        }
        Op::AuditChainCheck { .. } => {
            target
                .audit
                .flush()
                .await
                .expect("audit-chain operation must flush accepted records");
            let response = verify_live_audit_links(target).await;
            (
                "IN_PROCESS".to_string(),
                format!("audit_chain_check({})", target.audit_node_id),
                StatusCode::OK.as_u16(),
                response,
            )
        }
        Op::CreateLock { lock, scope, .. } => {
            let path = "/v1/security/preservation".to_string();
            let (status, mut response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "scope": preservation_scope_body(scope),
                    "reason_kind": "investigation",
                    "reason_text": modeled_preservation_reason(*lock)
                })),
            )
            .await;
            if (200..300).contains(&status) {
                let lock_id = response
                    .get("lock_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_else(|| panic!("successful preservation create omitted lock_id"))
                    .to_string();
                install_preservation_lock(&target.base_url, *lock, lock_id);
                record_success_request_id(&mut response, &request_id);
            }
            ("POST".to_string(), path, status, response)
        }
        Op::ReleaseLock { lock, .. } => {
            let lock_id = preservation_lock_id_or_resolve(target, *lock).await;
            let path = format!("/v1/security/preservation/{lock_id}/release");
            let approver = target.workload_credentials.credential(5, 0).bearer;
            let response = client
                .post(format!("{}{}", target.base_url, path))
                .header("x-request-id", &request_id)
                .header("x-zeppelin-approval", approver)
                .send()
                .await
                .unwrap_or_else(|error| panic!("preservation release request failed: {error}"));
            let status = response.status().as_u16();
            let bytes = response
                .bytes()
                .await
                .unwrap_or_else(|error| panic!("preservation release body failed: {error}"));
            let mut response = if bytes.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_slice(&bytes)
                    .unwrap_or_else(|error| panic!("preservation release JSON failed: {error}"))
            };
            if (200..300).contains(&status) {
                record_success_request_id(&mut response, &request_id);
            }
            ("POST".to_string(), path, status, response)
        }
        Op::DeleteUnderLock { ns, surface, .. } => {
            let (method, path, status, response) = match surface {
                DeleteUnderLockSurface::Namespace => {
                    let path = format!("/v1/namespaces/{ns}");
                    let (status, response) = request_json(
                        client,
                        Method::DELETE,
                        &format!("{}{}", target.base_url, path),
                        None,
                    )
                    .await;
                    ("DELETE".to_string(), path, status, response)
                }
                DeleteUnderLockSurface::Snapshot => {
                    let path = format!("/v1/namespaces/{ns}/snapshots/adversarial-preservation");
                    let (create_status, create_response) = request_json(
                        client,
                        Method::PUT,
                        &format!("{}{}", target.base_url, path),
                        None,
                    )
                    .await;
                    assert!(
                        matches!(create_status, 200 | 201 | 409),
                        "preservation snapshot setup failed: {create_response}"
                    );
                    let (status, response) = request_json(
                        client,
                        Method::DELETE,
                        &format!("{}{}", target.base_url, path),
                        None,
                    )
                    .await;
                    ("DELETE".to_string(), path, status, response)
                }
                DeleteUnderLockSurface::VectorIds => {
                    let path = format!("/v1/namespaces/{ns}/vectors");
                    let (status, response) = request_json(
                        client,
                        Method::DELETE,
                        &format!("{}{}", target.base_url, path),
                        Some(json!({"ids": ["preservation-probe"]})),
                    )
                    .await;
                    ("DELETE".to_string(), path, status, response)
                }
                DeleteUnderLockSurface::VectorFilter => {
                    let path = format!("/v1/namespaces/{ns}/vectors");
                    let (status, response) = request_json(
                        client,
                        Method::DELETE,
                        &format!("{}{}", target.base_url, path),
                        Some(json!({
                            "filter": {"op": "eq", "field": "group", "value": "g0"}
                        })),
                    )
                    .await;
                    ("DELETE".to_string(), path, status, response)
                }
            };
            (method, path, status, response)
        }
        Op::GcUnderLock { ns, keep_count, .. } => {
            let config = gc_config(*keep_count);
            let mut runner = gc::GcRunner::new(target.store.clone(), config)
                .with_preservation_service(target.security.preservation_service().cloned());
            let report = runner
                .run_cycle_at(
                    gc::GcNamespaceIncarnation::new(ns.clone(), target.clock.now()),
                    target.clock.now(),
                )
                .await
                .unwrap_or_else(|error| panic!("lock-aware GC failed for {ns}: {error}"));
            (
                "IN_PROCESS".to_string(),
                format!("gc::GcRunner::run_cycle_at({ns})"),
                StatusCode::OK.as_u16(),
                json!({
                    "candidates_marked": report.candidates_marked,
                    "objects_deleted": report.objects_deleted,
                    "pending_deletes_deleted": report.pending_deletes_deleted,
                    "pending_deletes_pruned": report.pending_deletes_pruned,
                    "pending_deletes_retained": report.pending_deletes_retained,
                    "bytes_reclaimed": report.bytes_reclaimed,
                    "candidates_skipped": report.candidates_skipped,
                    "keep_count": keep_count
                }),
            )
        }
        _ => panic!("non-security operation reached execute_security_op"),
    }
}

fn preservation_scope_body(scope: &PreservationScopeSpec) -> serde_json::Value {
    match scope {
        PreservationScopeSpec::Global => json!({"kind": "global"}),
        PreservationScopeSpec::Namespace { namespace } => {
            json!({"kind": "namespace", "namespace": namespace})
        }
        PreservationScopeSpec::NamespaceFilter { namespace, filter } => json!({
            "kind": "namespace_filter",
            "namespace": namespace,
            "filter": filter
        }),
    }
}

fn delegated_token_body(narrowed: &DelegatedTokenSpec) -> serde_json::Value {
    json!({
        "actions": narrowed.actions,
        "namespaces": narrowed.namespaces,
        "mandatory_filter": narrowed.mandatory_filter,
        "purpose": narrowed.purpose,
        "expires_in_secs": narrowed.expires_after_secs,
    })
}

async fn execute_delegated_query(
    admin_client: &Client,
    token_client: &Client,
    target: &OpExecutionTarget,
    namespace: &str,
) -> (String, String, u16, serde_json::Value) {
    let metadata_path = format!("/v1/namespaces/{namespace}");
    let (metadata_status, metadata) = request_json(
        admin_client,
        Method::GET,
        &format!("{}{}", target.base_url, metadata_path),
        None,
    )
    .await;
    assert_eq!(
        metadata_status, 200,
        "admin delegated-query metadata lookup failed"
    );
    let dimensions = metadata["dimensions"]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .expect("delegated-query namespace metadata omitted dimensions");
    let path = format!("/v1/namespaces/{namespace}/query");
    let (status, response) = request_json(
        token_client,
        Method::POST,
        &format!("{}{}", target.base_url, path),
        Some(json!({
            "sources": [{"type": "ann", "vector": vec![0.0_f32; dimensions]}],
            "fusion": {"type": "none"},
            "top_k": 100,
            "consistency": "strong"
        })),
    )
    .await;
    ("POST".to_string(), path, status, response)
}

fn install_and_redact_credential(
    registry: &WorkloadCredentialRegistry,
    actor: u8,
    response: &mut serde_json::Value,
) {
    let key_id = response["key_id"]
        .as_str()
        .expect("successful key response omitted key_id")
        .to_string();
    let bearer = response["api_key"]
        .as_str()
        .expect("successful key response omitted one-time api_key")
        .to_string();
    registry.install(actor, &key_id, &bearer);
    response
        .as_object_mut()
        .expect("successful key response must be an object")
        .insert("api_key".to_string(), json!("[REDACTED]"));
}

fn record_success_request_id(response: &mut serde_json::Value, request_id: &str) {
    response
        .as_object_mut()
        .expect("successful security response must be an object")
        .insert("request_id".to_string(), json!(request_id));
}

async fn execute_tenant_boundary_probe(
    admin_client: &Client,
    client: &Client,
    target: &OpExecutionTarget,
    namespace: &str,
    surface: TenantProbeSurface,
) -> (String, String, u16, serde_json::Value) {
    let metadata_path = format!("/v1/namespaces/{namespace}");
    let (metadata_status, metadata) = request_json(
        admin_client,
        Method::GET,
        &format!("{}{}", target.base_url, metadata_path),
        None,
    )
    .await;
    assert_eq!(
        metadata_status, 200,
        "admin tenant-probe metadata lookup failed"
    );
    let dimensions = metadata["dimensions"]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .expect("tenant-probe namespace metadata omitted dimensions");
    let base_query = json!({
        "sources": [{"type": "ann", "vector": vec![0.0_f32; dimensions]}],
        "fusion": {"type": "none"},
        "top_k": 4,
        "consistency": "strong"
    });
    let (path, body) = match surface {
        TenantProbeSurface::Query => (format!("/v1/namespaces/{namespace}/query"), base_query),
        TenantProbeSurface::Batch => (
            format!("/v1/namespaces/{namespace}/query/batch"),
            json!({"queries": [base_query]}),
        ),
        TenantProbeSurface::Fetch => (
            format!("/v1/namespaces/{namespace}/vectors/get"),
            json!({"ids": ["tenant-boundary-probe"], "consistency": "strong"}),
        ),
        TenantProbeSurface::Paginate => (
            format!("/v1/namespaces/{namespace}/query"),
            json!({
                "sources": [{"type": "ann", "vector": vec![0.0_f32; dimensions]}],
                "fusion": {"type": "none"}, "top_k": 1,
                "cursor": {"type": "none"}, "consistency": "strong"
            }),
        ),
        TenantProbeSurface::Facet => (
            format!("/v1/namespaces/{namespace}/query"),
            json!({
                "sources": [{"type": "ann", "vector": vec![0.0_f32; dimensions]}],
                "fusion": {"type": "none"}, "top_k": 4,
                "facets": [{"field": "group", "limit": 8}], "consistency": "strong"
            }),
        ),
        TenantProbeSurface::Group => (
            format!("/v1/namespaces/{namespace}/query"),
            json!({
                "sources": [{"type": "ann", "vector": vec![0.0_f32; dimensions]}],
                "fusion": {"type": "none"}, "top_k": 4,
                "group_by": {"field": "group", "max_per_group": 2},
                "consistency": "strong"
            }),
        ),
        TenantProbeSurface::AsOf => (
            format!("/v1/namespaces/{namespace}/query?as_of=1"),
            base_query,
        ),
        TenantProbeSurface::Explain => (
            format!("/v1/namespaces/{namespace}/query"),
            json!({
                "sources": [{"type": "ann", "vector": vec![0.0_f32; dimensions]}],
                "fusion": {"type": "none"}, "top_k": 4,
                "explain": "full", "consistency": "strong"
            }),
        ),
    };
    let (status, response) = request_json(
        client,
        Method::POST,
        &format!("{}{}", target.base_url, path),
        Some(body),
    )
    .await;
    ("POST".to_string(), path, status, response)
}

async fn execute_forbidden_write_probe(
    admin_client: &Client,
    actor_client: &Client,
    target: &OpExecutionTarget,
    namespace: &str,
    kind: ForbiddenWriteKind,
) -> (String, String, u16, serde_json::Value) {
    let path = format!("/v1/namespaces/{namespace}/vectors");
    match kind {
        ForbiddenWriteKind::CrossScopeDelete => {
            let (status, response) = request_json(
                actor_client,
                Method::DELETE,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "filter": {"op": "eq", "field": "group", "value": "g1"}
                })),
            )
            .await;
            ("DELETE".to_string(), path, status, response)
        }
        ForbiddenWriteKind::StampForgery | ForbiddenWriteKind::ForbidSetAttribute => {
            let metadata_path = format!("/v1/namespaces/{namespace}");
            let (metadata_status, metadata) = request_json(
                admin_client,
                Method::GET,
                &format!("{}{}", target.base_url, metadata_path),
                None,
            )
            .await;
            assert_eq!(
                metadata_status, 200,
                "admin namespace metadata lookup failed"
            );
            let dimensions = metadata["dimensions"]
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .expect("namespace metadata omitted dimensions");
            let attributes = match kind {
                ForbiddenWriteKind::StampForgery => json!({"group": "g1"}),
                ForbiddenWriteKind::ForbidSetAttribute => {
                    json!({"classification": "restricted"})
                }
                ForbiddenWriteKind::CrossScopeDelete => unreachable!(),
            };
            let (status, response) = request_json(
                actor_client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "vectors": [{
                        "values": vec![0.0_f32; dimensions],
                        "attributes": attributes
                    }]
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
    }
}

async fn execute_invalid_probe(
    client: &Client,
    target: &OpExecutionTarget,
    ns: &str,
    probe: InvalidProbe,
) -> (String, String, u16, serde_json::Value) {
    match probe {
        InvalidProbe::NanVector => {
            let path = format!("/v1/namespaces/{ns}/query");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [serde_json::Value::Null, json!(0.0)]
                    }],
                    "fusion": { "type": "none" },
                    "top_k": 1
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::WrongDims => {
            let path = format!("/v1/namespaces/{ns}/vectors");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "vectors": [{
                        "id": "wrong-dims",
                        "values": [0.0]
                    }]
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::BadIdCharset => {
            let path = format!("/v1/namespaces/{ns}/vectors");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "vectors": [{
                        "id": "bad/id",
                        "values": [0.0, 0.0]
                    }]
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::EmptyBatch => {
            let path = format!("/v1/namespaces/{ns}/vectors");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({ "vectors": [] })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::OversizedBatch => {
            let path = format!("/v1/namespaces/{ns}/query/batch");
            let queries = (0..257)
                .map(|_| {
                    json!({
                        "sources": [{
                            "type": "ann",
                            "vector": [0.0, 0.0]
                        }],
                        "fusion": { "type": "none" },
                        "top_k": 1
                    })
                })
                .collect::<Vec<_>>();
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({ "queries": queries })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::UnknownField => {
            let path = format!("/v1/namespaces/{ns}/query");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [0.0, 0.0]
                    }],
                    "fusion": { "type": "none" },
                    "top_k": 1,
                    "unexpected": true
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::BadCursorToken => {
            let path = format!("/v1/namespaces/{ns}/query");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [0.0, 0.0]
                    }],
                    "top_k": 1,
                    "cursor": { "type": "after", "token": "not-a-cursor" }
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::GroupingPlusCursor => {
            let path = format!("/v1/namespaces/{ns}/query");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [0.0, 0.0]
                    }],
                    "top_k": 1,
                    "grouping": { "type": "field", "field": "group", "max_per_group": 1 },
                    "cursor": { "type": "none" }
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::WeightsLenMismatch => {
            let path = format!("/v1/namespaces/{ns}/query");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [0.0, 0.0]
                    }, {
                        "type": "ann",
                        "vector": [1.0, 0.0]
                    }],
                    "fusion": { "type": "weighted", "weights": [1.0] },
                    "top_k": 1
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::AsOfGenZero | InvalidProbe::AsOfGenFuture => {
            let generation = if probe == InvalidProbe::AsOfGenZero {
                0
            } else {
                compact_generation(client, &target.base_url, ns).await + 10_000
            };
            let path = format!("/v1/namespaces/{ns}/query?as_of={generation}");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", target.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [0.0, 0.0]
                    }],
                    "fusion": { "type": "none" },
                    "top_k": 1
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
    }
}

pub(crate) async fn execute_branching_http(
    client: &Client,
    base_url: &str,
    operation: &BranchingOp,
) -> (String, String, u16, serde_json::Value) {
    let (method, path, body) = match operation {
        BranchingOp::ForkNamespace { source, target, .. } => (
            Method::POST,
            format!("/v1/namespaces/{source}/branches"),
            Some(json!({ "target": target })),
        ),
        BranchingOp::ListBranches { source, .. } => (
            Method::GET,
            format!("/v1/namespaces/{source}/branches"),
            None,
        ),
        BranchingOp::CompactBranch { namespace, .. } => (
            Method::POST,
            format!("/v1/namespaces/{namespace}/compact"),
            None,
        ),
        BranchingOp::DeleteBranch { namespace, .. } => {
            (Method::DELETE, format!("/v1/namespaces/{namespace}"), None)
        }
        BranchingOp::DeleteSourceWithBranches { source, .. } => {
            (Method::DELETE, format!("/v1/namespaces/{source}"), None)
        }
    };
    let method_label = method.to_string();
    let (status, response) = request_json(client, method, &format!("{base_url}{path}"), body).await;
    (method_label, path, status, response)
}

async fn request_json(
    client: &Client,
    method: Method,
    url: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    let ambiguity_allowed = REQUEST_AMBIGUITY_ALLOWED
        .try_with(|allowed| *allowed)
        .unwrap_or(false);
    let exchange = request_exchange(client, method, url, body, ambiguity_allowed).await;
    (exchange.status, exchange.response)
}

struct RequestExchange {
    status: u16,
    response: serde_json::Value,
    outcome: OpOutcome,
}

async fn request_outcome(
    client: &Client,
    method: Method,
    url: &str,
    body: Option<serde_json::Value>,
    ambiguity_allowed: bool,
) -> OpOutcome {
    request_exchange(client, method, url, body, ambiguity_allowed)
        .await
        .outcome
}

async fn request_exchange(
    client: &Client,
    method: Method,
    url: &str,
    body: Option<serde_json::Value>,
    ambiguity_allowed: bool,
) -> RequestExchange {
    let request_is_mutation = REQUEST_IS_MUTATION
        .try_with(|is_mutation| *is_mutation)
        .unwrap_or_else(|_| http_request_is_mutating(&method, url));
    let context = HTTP_FAULT_CONTEXT.try_with(Clone::clone).ok().flatten();
    let (target_url, path, action) = if let Some(context) = &context {
        let suffix = url
            .strip_prefix(&context.direct_base_url)
            .unwrap_or_else(|| {
                panic!(
                    "faulted workload URL {url} did not use direct server base {}",
                    context.direct_base_url
                )
            });
        let path = suffix.to_string();
        let action = request_is_mutation
            .then(|| context.scheduler.http_decision(&method, &path))
            .flatten();
        let target_url = action.as_ref().map_or_else(
            || url.to_string(),
            |_| format!("{}{}", context.proxy_base_url, suffix),
        );
        (target_url, path, action)
    } else {
        (url.to_string(), url.to_string(), None)
    };

    let Some(action) = action else {
        return send_exchange(
            client,
            method,
            &target_url,
            body.as_ref(),
            ambiguity_allowed,
            request_is_mutation,
            false,
            None,
        )
        .await;
    };

    let context = context.expect("HTTP fault action requires a fault context");
    match action.kind.clone() {
        FaultKind::DropRequest => {
            let response = json!({
                "code": "RATE_LIMITED",
                "error": "request dropped before send by adversarial injector",
                "status": 429,
                "retryable": true,
                "request_id": "adversarial-drop-request"
            });
            record_http_action(
                &context,
                &action,
                &path,
                FaultSemantics::PreCall,
                ObservedResult::DefiniteNotApplied,
                None,
            );
            RequestExchange {
                status: 429,
                response: response.clone(),
                outcome: OpOutcome::NotApplied {
                    status: 429,
                    response,
                },
            }
        }
        FaultKind::DropResponse
        | FaultKind::TruncateResponse { .. }
        | FaultKind::ResetAfterRequest => {
            context.injector.arm(action.clone());
            let exchange = send_exchange(
                client,
                method,
                &target_url,
                body.as_ref(),
                ambiguity_allowed,
                request_is_mutation,
                true,
                Some(Duration::from_millis(300)),
            )
            .await;
            context.injector.disarm();
            record_http_action(
                &context,
                &action,
                &path,
                FaultSemantics::PostCommit,
                ObservedResult::Ambiguous,
                Some(exchange.outcome.label()),
            );
            exchange
        }
        FaultKind::ClientCancel { after_ms } => {
            let send = send_exchange(
                client,
                method,
                &target_url,
                body.as_ref(),
                ambiguity_allowed,
                request_is_mutation,
                true,
                Some(Duration::from_millis(300)),
            );
            let exchange = match tokio::time::timeout(Duration::from_millis(after_ms), send).await {
                Ok(exchange) => exchange,
                Err(_) => ambiguous_exchange(0, AmbiguityReason::HttpTimeout),
            };
            record_http_action(
                &context,
                &action,
                &path,
                FaultSemantics::PostCommit,
                ObservedResult::Ambiguous,
                Some(exchange.outcome.label()),
            );
            exchange
        }
        FaultKind::DuplicateRetry => {
            let first = send_exchange(
                client,
                method.clone(),
                &target_url,
                body.as_ref(),
                ambiguity_allowed,
                request_is_mutation,
                true,
                Some(Duration::from_millis(300)),
            )
            .await;
            let second = send_exchange(
                client,
                method,
                &target_url,
                body.as_ref(),
                ambiguity_allowed,
                request_is_mutation,
                true,
                Some(Duration::from_millis(300)),
            )
            .await;
            let observed = observed_result(&second.outcome);
            record_http_action(
                &context,
                &action,
                &path,
                FaultSemantics::PostCommit,
                observed,
                Some(format!(
                    "first={}; second={}",
                    first.outcome.label(),
                    second.outcome.label()
                )),
            );
            second
        }
        _ => panic!(
            "object-store fault {:?} reached the HTTP injector",
            action.kind
        ),
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_exchange(
    client: &Client,
    method: Method,
    url: &str,
    body: Option<&serde_json::Value>,
    ambiguity_allowed: bool,
    request_is_mutation: bool,
    force_close: bool,
    timeout: Option<Duration>,
) -> RequestExchange {
    let mut request = client.request(method, url);
    if let Ok(request_id) = WORKLOAD_REQUEST_ID.try_with(Clone::clone) {
        request = request.header("x-request-id", request_id);
    }
    if let Some(body) = body {
        request = request.json(body);
    }
    if force_close {
        request = request.header(reqwest::header::CONNECTION, "close");
    }
    if let Some(timeout) = timeout {
        request = request.timeout(timeout);
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) if ambiguity_allowed => {
            let reason = if error.is_timeout() {
                AmbiguityReason::HttpTimeout
            } else {
                AmbiguityReason::ConnectionError
            };
            return ambiguous_exchange(0, reason);
        }
        Err(error) => panic!("HTTP request failed for {url}: {error}"),
    };
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        assert!(
            response.headers().contains_key("x-request-id"),
            "non-2xx response missing x-request-id header for {url}"
        );
    }
    if status == StatusCode::NO_CONTENT.as_u16() {
        return RequestExchange {
            status,
            response: serde_json::Value::Null,
            outcome: OpOutcome::Applied {
                status,
                response: serde_json::Value::Null,
            },
        };
    }
    let response = match response.json::<serde_json::Value>().await {
        Ok(response) => response,
        Err(_) if ambiguity_allowed => {
            return ambiguous_exchange(status, AmbiguityReason::JsonParse);
        }
        Err(error) => panic!("HTTP response JSON parse failed for {url}: {error}"),
    };
    let outcome = if (200..300).contains(&status) {
        OpOutcome::Applied {
            status,
            response: response.clone(),
        }
    } else if status >= 500 && ambiguity_allowed && request_is_mutation {
        return ambiguous_exchange_with_response(
            status,
            AmbiguityReason::ServerError { status },
            response,
        );
    } else {
        OpOutcome::NotApplied {
            status,
            response: response.clone(),
        }
    };
    RequestExchange {
        status,
        response,
        outcome,
    }
}

fn observed_result(outcome: &OpOutcome) -> ObservedResult {
    match outcome {
        OpOutcome::Applied { .. } => ObservedResult::DefiniteApplied,
        OpOutcome::NotApplied { .. } => ObservedResult::DefiniteNotApplied,
        OpOutcome::Ambiguous { .. } => ObservedResult::Ambiguous,
    }
}

fn record_http_action(
    context: &HttpFaultContext,
    action: &HttpFaultAction,
    path: &str,
    semantics: FaultSemantics,
    observed: ObservedResult,
    recovery: Option<String>,
) {
    context.scheduler.record(TimelineEvent {
        event_id: action.event_id.clone(),
        op_index: action.op_index,
        wall_ms: context.scheduler.wall_ms(),
        boundary: Boundary::ClientHttp,
        action: format!("{:?}", action.kind),
        key: Some(path.to_string()),
        semantics: if action.window {
            FaultSemantics::WindowActive
        } else {
            semantics
        },
        observed,
        recovery,
    });
}

fn http_request_is_mutating(method: &Method, url: &str) -> bool {
    match *method {
        Method::PUT | Method::PATCH | Method::DELETE => true,
        Method::POST => {
            !url.contains("/query") && !url.ends_with("/vectors/get") && !url.ends_with("/hydrate")
        }
        _ => false,
    }
}

fn ambiguous_exchange(status: u16, reason: AmbiguityReason) -> RequestExchange {
    ambiguous_exchange_with_response(status, reason, serde_json::Value::Null)
}

fn ambiguous_exchange_with_response(
    status: u16,
    reason: AmbiguityReason,
    response: serde_json::Value,
) -> RequestExchange {
    let mut response = match response {
        serde_json::Value::Object(object) => serde_json::Value::Object(object),
        other => json!({ "response": other }),
    };
    response
        .as_object_mut()
        .expect("ambiguity response must be an object")
        .insert(
            AMBIGUITY_MARKER.to_string(),
            serde_json::to_value(&reason).expect("AmbiguityReason must serialize"),
        );
    RequestExchange {
        status,
        response,
        outcome: OpOutcome::Ambiguous {
            reason,
            status: (status != 0).then_some(status),
        },
    }
}

fn classify_record_outcome(rec: &OpRecord, ambiguity_allowed: bool) -> OpOutcome {
    if let Some(encoded) = rec.response.get(AMBIGUITY_MARKER) {
        let reason = serde_json::from_value(encoded.clone())
            .expect("recorded ambiguity reason must deserialize");
        return OpOutcome::Ambiguous {
            reason,
            status: (rec.status != 0).then_some(rec.status),
        };
    }
    if (200..300).contains(&rec.status) {
        OpOutcome::Applied {
            status: rec.status,
            response: rec.response.clone(),
        }
    } else if rec.status >= 500
        && ambiguity_allowed
        && rec.op.is_mutating()
        && rec.method != "IN_PROCESS"
    {
        OpOutcome::Ambiguous {
            reason: AmbiguityReason::ServerError { status: rec.status },
            status: Some(rec.status),
        }
    } else {
        OpOutcome::NotApplied {
            status: rec.status,
            response: rec.response.clone(),
        }
    }
}

async fn compact_generation(client: &Client, base_url: &str, ns: &str) -> u64 {
    compact_status(client, base_url, ns).await["manifest_generation"]
        .as_u64()
        .unwrap_or_else(|| panic!("compact/status missing manifest_generation for {ns}"))
}

async fn authoritative_generation(store: &ZeppelinStore, ns: &str, op_kind: &str) -> u64 {
    read_authoritative_manifest(store, ns, op_kind)
        .await
        .unwrap_or_else(|| panic!("authoritative manifest missing for {ns} during {op_kind}"))
        .version()
}

/// Reads the post-op manifest generation for bookkeeping, tolerating an
/// unreadable or missing live manifest only when a fired durable content
/// fault (torn or misdirected write) is recorded against that exact manifest
/// key. A conformant successful PUT is not read back by the product, so an
/// acknowledged mutation under such a fault legitimately leaves the live
/// manifest corrupt or unwritten; anything else must still fail loudly.
async fn tainted_aware_authoritative_generation(
    store: &ZeppelinStore,
    scheduler: &FaultScheduler,
    ns: &str,
    op_kind: &str,
) -> Option<u64> {
    let outcome = match Manifest::read(store, ns).await {
        Ok(Some(manifest)) => return Some(manifest.version()),
        Ok(None) => "missing".to_string(),
        Err(error) => format!("unreadable: {error}"),
    };
    assert!(
        durable_manifest_corruption_recorded(scheduler, ns),
        "authoritative manifest {outcome} for {ns} during {op_kind} without a recorded durable \
         manifest content fault"
    );
    eprintln!(
        "authoritative manifest {outcome} for {ns} during {op_kind}; attributed to a recorded \
         durable manifest content fault, skipping the generation checkpoint"
    );
    None
}

fn durable_manifest_corruption_recorded(scheduler: &FaultScheduler, ns: &str) -> bool {
    let manifest_key = Manifest::s3_key(ns);
    scheduler.timeline().into_iter().any(|event| {
        event.observed == ObservedResult::Corrupted
            && durable_content_corruption(&event)
            && event.key.as_deref() == Some(manifest_key.as_str())
    })
}

async fn read_authoritative_manifest(
    store: &ZeppelinStore,
    ns: &str,
    context: &str,
) -> Option<Manifest> {
    Manifest::read(store, ns).await.unwrap_or_else(|error| {
        panic!("authoritative manifest read failed for {ns} during {context}: {error}")
    })
}

async fn periodic_s3_oracle_status(store: &ZeppelinStore, ns: &str) -> serde_json::Value {
    match Manifest::read(store, ns).await {
        Ok(Some(manifest)) => json!({
            "manifest_generation": manifest.version(),
        }),
        Ok(None) => json!({ "manifest_generation": null }),
        Err(error) => {
            eprintln!(
                "periodic S3 oracle authoritative read failed for {ns}; deferring to the S3 \
                 oracle classifier: {error}"
            );
            json!({
                "manifest_generation": null,
                "manifest_read_error": error.to_string(),
            })
        }
    }
}

async fn periodic_server_lineage_status(
    client: &Client,
    store: &ZeppelinStore,
    base_url: &str,
    ns: &str,
    durably_tainted_keys: &BTreeSet<String>,
) -> serde_json::Value {
    let manifest_key = Manifest::s3_key(ns);
    if !durably_tainted_keys.contains(&manifest_key) {
        return compact_status(client, base_url, ns).await;
    }

    let status = periodic_s3_oracle_status(store, ns).await;
    if status.get("manifest_read_error").is_none() {
        return compact_status(client, base_url, ns).await;
    }

    let url = format!("{base_url}/v1/namespaces/{ns}/compact/status");
    let (http_status, response) = request_json(client, Method::GET, &url, None).await;
    assert!(
        accept_loud_durable_manifest_resolution(
            http_status,
            &response,
            ns,
            Some(durably_tainted_keys),
        ),
        "periodic lineage authoritative manifest read failed without exact durable manifest \
         taint and a valid loud compact/status response for {ns}: status={http_status}; \
         response={response}"
    );
    eprintln!(
        "accepted loud periodic-lineage compact/status failure for exact durable manifest taint \
         in {ns}: status={http_status}"
    );
    status
}

async fn bookkeeping_compact_status(
    client: &Client,
    target: &OpExecutionTarget,
    ns: &str,
    allow_missing_manifest: bool,
    durably_tainted_keys: Option<&BTreeSet<String>>,
) -> serde_json::Value {
    let context = HTTP_FAULT_CONTEXT.try_with(Clone::clone).ok().flatten();
    if let Some(context) = context {
        match Manifest::read(&context.bookkeeping_store, ns).await {
            Ok(Some(manifest)) => json!({
                "manifest_present": true,
                "manifest_generation": manifest.version(),
                "uncompacted_fragments": manifest.uncompacted_fragments().len(),
            }),
            Ok(None) => json!({
                "manifest_present": false,
                "manifest_generation": null,
                "uncompacted_fragments": null,
            }),
            Err(error) => {
                let url = format!(
                    "{}/v1/namespaces/{ns}/compact/status",
                    context.direct_base_url
                );
                let (status, response) = request_json(client, Method::GET, &url, None).await;
                assert!(
                    accept_loud_durable_manifest_resolution(
                        status,
                        &response,
                        ns,
                        durably_tainted_keys,
                    ),
                    "invalid-probe authoritative manifest read failed without exact durable \
                     manifest taint and a valid loud compact/status response for {ns}: \
                     error={error}; status={status}; response={response}"
                );
                eprintln!(
                    "accepted loud invalid-probe bookkeeping failure for exact durable manifest \
                     taint in {ns}: status={status}"
                );
                json!({
                    "manifest_present": null,
                    "manifest_generation": null,
                    "uncompacted_fragments": null,
                    "manifest_read_error": error.to_string(),
                    "loud_failure": {
                        "status": status,
                        "response": response,
                    },
                })
            }
        }
    } else if allow_missing_manifest {
        compact_status_for_indeterminate_create(client, &target.base_url, ns).await
    } else {
        compact_status(client, &target.base_url, ns).await
    }
}

async fn compact_status_for_indeterminate_create(
    client: &Client,
    base_url: &str,
    ns: &str,
) -> serde_json::Value {
    let url = format!("{base_url}/v1/namespaces/{ns}/compact/status");
    let (status, response) = request_json(client, Method::GET, &url, None).await;
    if status == StatusCode::OK.as_u16() {
        return response;
    }
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        "indeterminate create compact/status returned an unexpected status for {ns}"
    );
    assert_eq!(
        response.get("code").and_then(serde_json::Value::as_str),
        Some("INTERNAL_DATA_MISSING"),
        "indeterminate create compact/status returned an unexpected error for {ns}"
    );
    assert_eq!(
        response.get("status").and_then(serde_json::Value::as_u64),
        Some(u64::from(StatusCode::INTERNAL_SERVER_ERROR.as_u16())),
        "indeterminate create compact/status returned a mismatched status body for {ns}"
    );
    assert_eq!(
        response
            .get("retryable")
            .and_then(serde_json::Value::as_bool),
        Some(false),
        "indeterminate create compact/status must fail non-retryably for {ns}"
    );
    assert!(
        response
            .get("error")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|error| !error.is_empty()),
        "indeterminate create compact/status omitted its error message for {ns}"
    );
    assert!(
        response
            .get("request_id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|request_id| !request_id.is_empty()),
        "indeterminate create compact/status omitted its request id for {ns}"
    );
    json!({
        "manifest_present": false,
        "manifest_generation": null,
        "uncompacted_fragments": null,
    })
}

async fn compact_status(client: &Client, base_url: &str, ns: &str) -> serde_json::Value {
    let url = format!("{base_url}/v1/namespaces/{ns}/compact/status");
    let response = client
        .get(&url)
        .send()
        .await
        .unwrap_or_else(|error| panic!("compact/status request failed for {ns}: {error}"));
    assert_eq!(
        response.status().as_u16(),
        200,
        "compact/status failed for {ns}"
    );
    response
        .json::<serde_json::Value>()
        .await
        .unwrap_or_else(|error| panic!("compact/status JSON parse failed for {ns}: {error}"))
}

async fn quiescent_s3_oracle_status(
    client: &Client,
    base_url: &str,
    ns: &str,
    durably_tainted_keys: Option<&BTreeSet<String>>,
) -> Option<serde_json::Value> {
    let url = format!("{base_url}/v1/namespaces/{ns}/compact/status");
    let (status, response) = request_json(client, Method::GET, &url, None).await;
    if status == StatusCode::OK.as_u16() {
        return Some(response);
    }
    let exact_durable_failure =
        accept_loud_durable_manifest_resolution(status, &response, ns, durably_tainted_keys);
    assert!(
        exact_durable_failure,
        "quiet S3 compact/status failed without exact durable manifest taint for {ns}: \
         status={status}; response={response}"
    );
    eprintln!(
        "accepted loud quiet-S3 compact/status failure for exact durable manifest taint in \
         {ns}: status={status}"
    );
    None
}

async fn wait_compaction_ready(client: &Client, base_url: &str, ns: &str) -> serde_json::Value {
    for _ in 0..300 {
        let status = compact_status(client, base_url, ns).await;
        if status["ready"].as_bool().unwrap_or(false) {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("compact endpoint did not reach ready for {ns}");
}

async fn wait_namespace_gone(client: &Client, base_url: &str, ns: &str) {
    let mut last_status = None;
    for _ in 0..300 {
        let response = client
            .get(format!("{base_url}/v1/namespaces/{ns}"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("namespace delete poll failed for {ns}: {error}"));
        match response.status() {
            StatusCode::NOT_FOUND => return,
            status @ (StatusCode::OK | StatusCode::GONE | StatusCode::ACCEPTED) => {
                last_status = Some(status);
            }
            status => panic!("unexpected namespace delete poll status for {ns}: {status}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "namespace {ns} did not reach 404 after delete; last poll status={:?} \
         (purge may be blocked by an active store fault window)",
        last_status
    );
}

async fn execute_sandwich_maintenance(
    client: &Client,
    target: &OpExecutionTarget,
    ns: &str,
    maintenance: super::ops::MaintenanceKind,
) -> serde_json::Value {
    match maintenance {
        super::ops::MaintenanceKind::CompactInline => {
            let result = target
                .compactor
                .compact(ns)
                .await
                .unwrap_or_else(|error| panic!("sandwich compaction failed for {ns}: {error}"));
            target.manifest_cache.invalidate_at(ns, target.clock.now());
            json!({
                "kind": "compact_inline",
                "segment_id": result.segment_id,
                "vectors_compacted": result.vectors_compacted,
            })
        }
        super::ops::MaintenanceKind::CompactEndpoint => {
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}/v1/namespaces/{ns}/compact", target.base_url),
                None,
            )
            .await;
            let ready = if (200..300).contains(&status) {
                Some(wait_compaction_ready(client, &target.base_url, ns).await)
            } else {
                None
            };
            json!({ "kind": "compact_endpoint", "status": status, "response": response, "ready": ready })
        }
        super::ops::MaintenanceKind::GcCycle => {
            let config = gc_config(4);
            let report = gc::run_gc_cycle_at(&target.store, ns, &config, target.clock.now())
                .await
                .unwrap_or_else(|error| panic!("sandwich gc failed for {ns}: {error}"));
            target.manifest_cache.invalidate_at(ns, target.clock.now());
            json!({
                "kind": "gc_cycle",
                "candidates_marked": report.candidates_marked,
                "objects_deleted": report.objects_deleted,
            })
        }
        super::ops::MaintenanceKind::Hydrate => {
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}/v1/namespaces/{ns}/hydrate", target.base_url),
                None,
            )
            .await;
            json!({ "kind": "hydrate", "status": status, "response": response })
        }
    }
}

fn gc_config(keep_count: u64) -> GcConfig {
    GcConfig {
        horizon_secs: 0,
        compaction_upload_window_secs: 0,
        skew_slop_secs: 0,
        allow_unsafe_short_horizon: true,
        manifest_history_keep_count: keep_count as usize,
        pitr_retention_secs: 0,
    }
}

fn accept_loud_tainted_quiescence(
    status: u16,
    violations: &[Violation],
    tainted_keys: Option<&BTreeSet<String>>,
) -> bool {
    (500..600).contains(&status)
        && violations.is_empty()
        && tainted_keys.is_some_and(|keys| !keys.is_empty())
}

fn accept_loud_durable_manifest_resolution(
    status: u16,
    response: &serde_json::Value,
    namespace: &str,
    durably_tainted_keys: Option<&BTreeSet<String>>,
) -> bool {
    let manifest_key = Manifest::s3_key(namespace);
    if !durably_tainted_keys.is_some_and(|keys| keys.contains(&manifest_key)) {
        return false;
    }
    let record = OpRecord {
        index: 0,
        wall_ms: 0,
        op: Op::FetchVectors {
            actor: ActorSel::ADMIN,
            ns: namespace.to_string(),
            ids: Vec::new(),
            consistency: ConsistencyLevel::Strong,
        },
        method: "POST".to_string(),
        path: format!("/v1/namespaces/{namespace}/vectors/get"),
        status,
        response: response.clone(),
        outcome: "not_applied".to_string(),
        target_node: 0,
        execution: ExecutionMetadata {
            phase: ExecutionPhase::Quiescence,
            hold: None,
        },
        gen_after: None,
        duration_ms: 0,
        violations: Vec::new(),
    };
    let envelope_violations = oracle::check_op(&Model::default(), &record, RunMode::Chaos, None);
    accept_loud_tainted_quiescence(status, &envelope_violations, durably_tainted_keys)
}

async fn inject_dual_writer_fencing_mutation(
    store: &ZeppelinStore,
    _workload_s3_tracker: &mut S3Tracker,
    namespace: &str,
    op_index: u64,
    stale_fencing_token: u64,
) -> Vec<Violation> {
    let (original, stale_version) = Manifest::read_versioned(store, namespace)
        .await
        .unwrap_or_else(|error| panic!("dual-writer mutation manifest read failed: {error}"))
        .unwrap_or_else(|| panic!("dual-writer mutation manifest missing for {namespace}"));
    let mut winner = original.clone();
    winner.fencing_token = stale_fencing_token
        .checked_add(1)
        .expect("dual-writer winner fencing token overflowed");
    let key = Manifest::s3_key(namespace);
    winner
        .write_conditional(store, namespace, &stale_version)
        .await
        .unwrap_or_else(|error| panic!("dual-writer winner conditional CAS failed: {error}"));
    let winner_bytes = store
        .get(&key)
        .await
        .unwrap_or_else(|error| panic!("dual-writer winner read-back failed: {error}"));
    let winner_manifest = Manifest::from_bytes(&winner_bytes)
        .unwrap_or_else(|error| panic!("dual-writer winner manifest decode failed: {error}"));
    assert_eq!(winner_manifest.version(), winner.version());
    assert_eq!(
        winner_manifest.fencing_token,
        stale_fencing_token + 1,
        "committed winner did not carry node B's exact fencing token"
    );
    assert_eq!(
        winner_manifest.version(),
        original
            .version()
            .checked_add(1)
            .expect("dual-writer winner generation overflowed")
    );

    // The mutation candidate is the already-committed winner at the same
    // namespace and generation with only node A's stale token substituted.
    // The real backend remains authoritative throughout this choreography.
    let mut stale = winner_manifest.clone();
    stale.fencing_token = stale_fencing_token;
    let stale_bytes = stale
        .to_bytes()
        .unwrap_or_else(|error| panic!("dual-writer stale manifest encode failed: {error}"));
    let stale_manifest = Manifest::from_bytes(&stale_bytes)
        .unwrap_or_else(|error| panic!("dual-writer stale manifest decode failed: {error}"));
    assert_eq!(stale_manifest.version(), winner_manifest.version());
    assert_eq!(stale_manifest.fencing_token, stale_fencing_token);
    assert_ne!(stale_bytes, winner_bytes);

    let status = json!({ "manifest_generation": winner_manifest.version() });
    let mut mutation_tracker = S3Tracker::default();
    let baseline = mutation_tracker
        .check_namespace(store, namespace, op_index, &status, false)
        .await;
    assert!(
        baseline.is_empty(),
        "dual-writer mutation winner was not a clean lineage baseline: {baseline:#?}"
    );

    let mutation_scheduler =
        FaultScheduler::from_schedule(FaultSchedule::stale_manifest_cas_selftest());
    let mutation_store = stale_manifest_cas_selftest_proxy(store, mutation_scheduler.clone());
    let stale_identity = zeppelin::storage::StorageVersion::require(stale_version.version(), &key)
        .expect("dual-writer stale CAS requires a backend version token");
    mutation_store
        .put_if_match(&key, stale_bytes, stale_identity, namespace)
        .await
        .unwrap_or_else(|error| {
            panic!("dual-writer stale conditional CAS was not admitted: {error}")
        });

    let violations = mutation_tracker
        .check_namespace(store, namespace, op_index.saturating_add(1), &status, false)
        .await;
    assert_eq!(
        violations.len(),
        1,
        "dual-writer stale conditional CAS must fire exactly one oracle violation: {violations:#?}"
    );
    assert_eq!(violations[0].id, ViolationId::I21FencingViolation);
    let mutation_timeline = mutation_scheduler.timeline();
    assert_eq!(
        mutation_timeline.len(),
        1,
        "dual-writer stale CAS proxy must mutate exactly once: {mutation_timeline:#?}"
    );
    assert_eq!(mutation_timeline[0].semantics, FaultSemantics::PostCommit);
    assert_eq!(mutation_timeline[0].observed, ObservedResult::Corrupted);

    store
        .put(&key, winner_bytes.clone())
        .await
        .unwrap_or_else(|error| panic!("dual-writer authoritative winner restore failed: {error}"));
    assert_eq!(
        store
            .get(&key)
            .await
            .unwrap_or_else(|error| panic!("dual-writer winner restore read-back failed: {error}")),
        winner_bytes
    );
    violations
}

struct QuietPeriod<'a> {
    client: &'a Client,
    server: &'a mut RestartableFullTestServer,
    scheduler: Option<&'a FaultScheduler>,
    test_clock: Option<&'a Arc<TestClock>>,
    injector: &'a mut Option<Arc<HttpFaultInjector>>,
    http_fault_context: &'a mut Option<HttpFaultContext>,
    chaos: Option<&'a ChaosHandle>,
    operational_state: &'a mut OperationalState,
    operational_observer: Option<&'a OperationalStoreObserver>,
    pending_held_op: &'a mut Option<PendingHeldOp>,
    dual_writer_lease_hold: &'a mut Option<DualWriterLeaseHoldActivation>,
    initial_dual_writer_stale_fencing_token: Option<u64>,
    artifacts: &'a mut SeedArtifacts,
    model: &'a mut Model,
    coverage: &'a mut Coverage,
    s3_tracker: &'a mut S3Tracker,
    corruption_tracker: &'a mut CorruptionTracker,
    created_namespaces: &'a mut Vec<String>,
    background_compaction_starts: &'a mut BTreeMap<String, u64>,
    op_index: &'a mut u64,
    compactions: &'a mut u64,
    started: Instant,
    mutation: Option<OracleMutation>,
    mode: RunMode,
    exact_vector_count: bool,
    verify: bool,
    preserve_recorded_holds: bool,
    prefix: &'a str,
    config: &'a Config,
    disk_cache_max_bytes: u64,
    drain_ops: &'a mut VecDeque<QuietDrainOp>,
}

struct QuietPeriodOutcome {
    violations: Vec<Violation>,
    post_commit_ack_lost: bool,
    dual_writer_stale_fencing_token: Option<u64>,
    drained_ops: u64,
    timeline: Vec<TimelineEvent>,
}

impl QuietPeriod<'_> {
    async fn run(self) -> QuietPeriodOutcome {
        let mut timeline = Vec::new();
        let mut violations = Vec::new();
        let mut post_commit_ack_lost = false;
        let mut dual_writer_stale_fencing_token = self.initial_dual_writer_stale_fencing_token;
        let mut drained_ops = 0u64;
        let bookkeeping_store = self
            .http_fault_context
            .as_ref()
            .map(|context| context.bookkeeping_store.clone());

        let quiet_start = quiet_event(
            self.scheduler,
            self.started,
            *self.op_index,
            1,
            Boundary::Runner,
            "scheduler-quiesce",
            ObservedResult::DefiniteApplied,
            Some("new fault admission disabled".to_string()),
        );
        if let Some(scheduler) = self.scheduler {
            scheduler.begin_quiet_period(quiet_start);
        } else {
            timeline.push(quiet_start);
        }

        if let Some(clock) = self.test_clock {
            clock.thaw();
        }
        *self.http_fault_context = None;
        if let Some(chaos) = self.chaos {
            chaos.disable();
        }
        shutdown_http_fault_injector(self.injector).await;
        push_quiet_event(
            &mut timeline,
            self.scheduler,
            self.started,
            *self.op_index,
            2,
            Boundary::ClientHttp,
            "restore-network",
            ObservedResult::DefiniteApplied,
            Some("injectors disabled; test clock thawed".to_string()),
        );

        if let Some(scheduler) = self.scheduler {
            scheduler.release_held_calls();
        }
        if let Some(activation) = self.dual_writer_lease_hold.take() {
            dual_writer_stale_fencing_token = Some(
                finish_dual_writer_lease_hold(
                    self.scheduler
                        .expect("dual-writer lease hold requires a fault scheduler"),
                    activation,
                )
                .await,
            );
        }
        if let Some(mut pending) = self.pending_held_op.take() {
            if !self.preserve_recorded_holds && pending.release_op > *self.op_index {
                pending.release_op = *self.op_index;
                pending.release_cause = HoldReleaseCause::Quiesce;
            }
            let step = finish_pending_held_op(
                pending,
                self.client,
                self.artifacts,
                self.model,
                self.coverage,
                self.s3_tracker,
                self.corruption_tracker,
                self.mutation,
                self.mode,
            )
            .await;
            apply_step_bookkeeping(
                &step,
                self.created_namespaces,
                self.background_compaction_starts,
                self.compactions,
            );
            post_commit_ack_lost |= step.post_commit_ack_lost;
            let pending_crash = take_step_crash(&step, self.scheduler);
            violations.extend(step.violations);
            if violations.is_empty() {
                if let Some(crash) = pending_crash {
                    let scheduler = self
                        .scheduler
                        .expect("quiesced held-call process crash requires a scheduler");
                    let controller = scheduler
                        .process_controller()
                        .expect("quiesced held-call process crash requires a controller");
                    let server_store = self.server.store.clone();
                    let spawn_compaction_loop = self.server.shutdown_compaction.is_some();
                    let recovery = restart_after_crash(
                        self.server,
                        &controller,
                        scheduler,
                        self.injector,
                        self.http_fault_context,
                        &server_store,
                        bookkeeping_store.as_ref().expect(
                            "quiesced held-call process crash requires a bookkeeping store",
                        ),
                        self.prefix,
                        self.config,
                        spawn_compaction_loop,
                        self.client,
                        self.model,
                        *self.op_index,
                        crash,
                    )
                    .await;
                    violations.extend(recovery);
                    *self.http_fault_context = None;
                    shutdown_http_fault_injector(self.injector).await;
                }
            }
        }
        let deferred_drain_count = self.drain_ops.len();
        while violations.is_empty() {
            let Some(drain) = self.drain_ops.pop_front() else {
                break;
            };
            let (op, target_node, phase, inject_post_commit_ack_loss) = match drain {
                QuietDrainOp::Generated {
                    op,
                    inject_post_commit_ack_loss,
                } => {
                    let target_node = self.operational_state.choose_target_node_for_op(&op);
                    (
                        op,
                        target_node,
                        ExecutionPhase::DeferredDrain,
                        inject_post_commit_ack_loss,
                    )
                }
                QuietDrainOp::Replay {
                    source,
                    op,
                    inject_post_commit_ack_loss,
                } => {
                    let source = *source;
                    assert_eq!(
                        source.index, *self.op_index,
                        "replayed deferred drain changed its operation index"
                    );
                    assert_eq!(
                        source.execution.phase,
                        ExecutionPhase::DeferredDrain,
                        "replayed quiet-period continuation changed phase"
                    );
                    assert!(
                        source.execution.hold.is_none(),
                        "replayed quiet-period continuation cannot start a hold"
                    );
                    (
                        op,
                        source.target_node,
                        source.execution.phase,
                        inject_post_commit_ack_loss,
                    )
                }
            };
            let generation_checkpoints = self.operational_state.generation_checkpoints_enabled();
            let target_server = self.operational_state.target(self.server, target_node);
            let step = execute_recorded_op(
                self.client,
                target_server,
                self.artifacts,
                self.model,
                self.coverage,
                self.s3_tracker,
                self.corruption_tracker,
                &op,
                *self.op_index,
                self.started,
                self.mutation,
                self.mode,
                phase,
                generation_checkpoints,
                target_node,
                None,
                inject_post_commit_ack_loss,
            )
            .await;
            *self.op_index = self
                .op_index
                .checked_add(1)
                .expect("quiet-period deferred drain index overflowed");
            drained_ops = drained_ops
                .checked_add(1)
                .expect("quiet-period deferred drain count overflowed");
            apply_step_bookkeeping(
                &step,
                self.created_namespaces,
                self.background_compaction_starts,
                self.compactions,
            );
            assert!(
                step.crash.is_none(),
                "a quiesced deferred operation cannot complete through a process crash"
            );
            assert_eq!(
                step.post_commit_ack_lost, inject_post_commit_ack_loss,
                "quiet-period deferred drain changed acknowledgement-loss replay"
            );
            post_commit_ack_lost |= step.post_commit_ack_lost;
            violations.extend(step.violations);
        }
        push_quiet_event(
            &mut timeline,
            self.scheduler,
            self.started,
            *self.op_index,
            3,
            Boundary::ObjectStore,
            "release-held",
            quiet_observed(&violations),
            Some(format!(
                "held calls released and joined; deferred_ops={deferred_drain_count}"
            )),
        );

        self.operational_state
            .stop_second_node(self.operational_observer)
            .await;
        push_quiet_event(
            &mut timeline,
            self.scheduler,
            self.started,
            *self.op_index,
            4,
            Boundary::Runner,
            "stop-second-node",
            ObservedResult::DefiniteApplied,
            Some("secondary server stopped".to_string()),
        );

        let restarted = if self.server.server_task.is_finished() {
            let store = self.server.store.clone();
            let clock = self.server.clock.clone();
            let admin_bearer = self.server.admin_bearer.clone();
            let workload_credentials = self.server.workload_credentials.clone();
            let encoder_provider = Arc::clone(&self.server.encoder_provider);
            let object_store_counter = self.server.object_store_counter.clone();
            let spawn_compaction_loop = self.server.shutdown_compaction.is_some();
            let old_server = self.server.take();
            if let Err(error) = old_server.abort_and_drop().await {
                tracing::warn!(
                    error = %error,
                    "retired primary whose HTTP task had already failed"
                );
            }
            let mut replacement = if let Some(counter) = object_store_counter {
                start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer_and_encoder_provider(
                    store,
                    Some(self.prefix.to_string()),
                    self.config.clone(),
                    spawn_compaction_loop,
                    Some(clock),
                    self.disk_cache_max_bytes,
                    &admin_bearer,
                    encoder_provider,
                    counter,
                )
                .await
            } else {
                start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
                    store,
                    Some(self.prefix.to_string()),
                    self.config.clone(),
                    spawn_compaction_loop,
                    Some(clock),
                    self.disk_cache_max_bytes,
                    &admin_bearer,
                )
                .await
            };
            replacement.workload_credentials = workload_credentials;
            self.server.install(replacement);
            true
        } else {
            false
        };
        wait_for_health(self.client, &self.server.base_url).await;
        push_quiet_event(
            &mut timeline,
            self.scheduler,
            self.started,
            *self.op_index,
            5,
            Boundary::Process,
            "primary-health",
            ObservedResult::DefiniteApplied,
            Some(if restarted {
                "primary restarted and healthy".to_string()
            } else {
                "primary already healthy".to_string()
            }),
        );

        let security_refresh_violations = run_security_refresh_checks(
            self.server,
            self.model,
            self.coverage,
            self.prefix,
            *self.op_index,
            self.mutation,
        )
        .await;
        violations.extend(security_refresh_violations);
        push_quiet_event(
            &mut timeline,
            self.scheduler,
            self.started,
            *self.op_index,
            6,
            Boundary::ObjectStore,
            "security-refresh",
            quiet_observed(&violations),
            Some(if self.model.security.enabled() {
                format!(
                    "policy_version={}; violations={}",
                    self.model.security.policy_version,
                    violations.len()
                )
            } else {
                "security program not active; implicit-admin compatibility retained".to_string()
            }),
        );

        stop_background_compaction(self.server).await;
        let misdirected_recovery =
            restore_misdirected_write_artifacts(&self.server.store, self.scheduler, self.mutation)
                .await;
        let quiescence_clock_advance_ms = self
            .test_clock
            .map(|clock| advance_quiescence_clock_past_lease(clock, self.config));
        push_quiet_event(
            &mut timeline,
            self.scheduler,
            self.started,
            *self.op_index,
            7,
            Boundary::Runner,
            "stop-background",
            ObservedResult::DefiniteApplied,
            Some(match quiescence_clock_advance_ms {
                Some(advance_ms) => format!(
                    "background compaction joined; {misdirected_recovery}; \
                     test clock advanced {advance_ms}ms past stale lease lifetime"
                ),
                None => format!("background compaction joined; {misdirected_recovery}"),
            }),
        );

        let verify = self.verify && violations.is_empty();
        if verify {
            violations.extend(
                run_quiescent_checks(
                    self.client,
                    self.server,
                    self.artifacts,
                    self.model,
                    self.coverage,
                    self.s3_tracker,
                    self.corruption_tracker,
                    self.op_index,
                    self.compactions,
                    self.started,
                    dual_writer_stale_fencing_token,
                    self.mutation,
                    RunMode::Deterministic,
                    self.exact_vector_count,
                    &mut timeline,
                    self.scheduler,
                )
                .await,
            );
        } else {
            for (step, boundary, action) in [
                (8, Boundary::Runner, "resolve-indeterminates"),
                (9, Boundary::Runner, "force-compaction"),
                (10, Boundary::Runner, "gc-twice"),
                (11, Boundary::ObjectStore, "s3-oracles"),
                (12, Boundary::ClientHttp, "exhaustive-sweep"),
            ] {
                push_quiet_event(
                    &mut timeline,
                    self.scheduler,
                    self.started,
                    *self.op_index,
                    step,
                    boundary,
                    action,
                    ObservedResult::DefiniteNotApplied,
                    Some("skipped after an earlier violation".to_string()),
                );
            }
        }

        QuietPeriodOutcome {
            violations,
            post_commit_ack_lost,
            dual_writer_stale_fencing_token,
            drained_ops,
            timeline,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn push_quiet_event(
    timeline: &mut Vec<TimelineEvent>,
    scheduler: Option<&FaultScheduler>,
    started: Instant,
    op_index: u64,
    step: u8,
    boundary: Boundary,
    action: &str,
    observed: ObservedResult,
    recovery: Option<String>,
) {
    let event = quiet_event(
        scheduler, started, op_index, step, boundary, action, observed, recovery,
    );
    if let Some(scheduler) = scheduler {
        scheduler.record(event);
    } else {
        timeline.push(event);
    }
}

#[allow(clippy::too_many_arguments)]
fn quiet_event(
    scheduler: Option<&FaultScheduler>,
    started: Instant,
    op_index: u64,
    step: u8,
    boundary: Boundary,
    action: &str,
    observed: ObservedResult,
    recovery: Option<String>,
) -> TimelineEvent {
    TimelineEvent {
        event_id: format!("quiet-{step:02}"),
        op_index,
        wall_ms: scheduler.map_or_else(
            || {
                u64::try_from(started.elapsed().as_millis())
                    .expect("quiet-period wall time must fit u64")
            },
            FaultScheduler::wall_ms,
        ),
        boundary,
        action: format!("quiet:{action}"),
        key: None,
        semantics: FaultSemantics::WindowEnd,
        observed,
        recovery,
    }
}

fn quiet_observed(violations: &[Violation]) -> ObservedResult {
    if violations.is_empty() {
        ObservedResult::DefiniteApplied
    } else {
        ObservedResult::Corrupted
    }
}

async fn verify_live_audit_links(target: &OpExecutionTarget) -> serde_json::Value {
    let day = target.clock.now().date_naive();
    let prefix = format!(
        "_audit/{}/{}/",
        day.format("%Y-%m-%d"),
        target.audit_node_id
    );
    let mut keys = target
        .store
        .list_prefix(&prefix)
        .await
        .unwrap_or_else(|error| panic!("audit-chain LIST failed: {error}"));
    keys.sort();
    let mut previous = None;
    let mut checked = 0_u64;
    for key in keys {
        let bytes = target
            .store
            .get(&key)
            .await
            .unwrap_or_else(|error| panic!("audit-chain GET failed for {key}: {error}"));
        for (line_index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }
            let record: AuditRecord = match serde_json::from_slice(line) {
                Ok(record) => record,
                Err(error) => {
                    return json!({
                        "valid": false,
                        "first_divergence": "record_decode",
                        "object_key": key,
                        "line_index": line_index,
                        "error": error.to_string(),
                        "records_checked": checked
                    });
                }
            };
            if record.node_id != target.audit_node_id
                || record.ts.date_naive() != day
                || record.prev_hash != previous
            {
                return json!({
                    "valid": false,
                    "first_divergence": "previous_hash",
                    "object_key": key,
                    "line_index": line_index,
                    "records_checked": checked
                });
            }
            let value = serde_json::to_value(&record)
                .expect("typed audit record must convert to canonical JSON");
            let bytes = serde_json::to_vec(&canonicalize_json(value))
                .expect("canonical audit record must encode");
            previous = Some(
                Sha256::digest(bytes)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>(),
            );
            checked = checked
                .checked_add(1)
                .expect("audit-chain checked-record count overflowed");
        }
    }
    json!({
        "valid": true,
        "first_divergence": null,
        "records_checked": checked,
        "terminal_hash": previous
    })
}

async fn exercise_audit_record_drop(
    store: &ZeppelinStore,
    day: chrono::NaiveDate,
    prefix: &str,
    record: Option<AuditRecord>,
) -> zeppelin::security::AuditChainVerification {
    let node_suffix = Sha256::digest(prefix.as_bytes())
        .iter()
        .take(6)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let node_id = format!("audit-mutation-{node_suffix}");
    let (client, runtime) =
        AuditRuntime::start(store.clone(), node_id.clone(), Duration::from_secs(60))
            .await
            .expect("audit mutation runtime must start");
    let mut record = record.expect("audit mutation requires one observed durable record");
    record.node_id.clone_from(&node_id);
    record.prev_hash = None;
    client
        .submit_durable(record)
        .await
        .expect("audit mutation record must become durable");
    runtime
        .shutdown()
        .await
        .expect("audit mutation runtime must publish its signed anchor");

    let valid = verify_audit_day(store, day, &node_id)
        .await
        .expect("audit mutation baseline verification must execute");
    assert!(valid.valid, "audit mutation baseline chain must be valid");

    let chain_prefix = format!("_audit/{}/{node_id}/", day.format("%Y-%m-%d"));
    let mut keys = store
        .list_prefix(&chain_prefix)
        .await
        .expect("audit mutation chain listing must succeed");
    keys.sort();
    let record_key = keys
        .first()
        .expect("audit mutation chain must contain a record object")
        .clone();
    let original = store
        .get(&record_key)
        .await
        .expect("audit mutation record read must succeed");
    store
        .put(&record_key, Bytes::new())
        .await
        .expect("audit mutation record drop must succeed");
    let broken = verify_audit_day(store, day, &node_id)
        .await
        .expect("audit mutation verification must execute");
    store
        .put(&record_key, original)
        .await
        .expect("audit mutation record restore must succeed");
    store
        .delete_prefix(&chain_prefix)
        .await
        .expect("audit mutation chain cleanup must succeed");
    store
        .delete(&format!(
            "_audit/anchors/{}/{node_id}.json",
            day.format("%Y-%m-%d")
        ))
        .await
        .expect("audit mutation anchor cleanup must succeed");
    broken
}

fn canonicalize_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonicalize_json).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonicalize_json(value)))
                .collect(),
        ),
        scalar => scalar,
    }
}

#[derive(Default)]
struct DurableAuditEvidence {
    request_ids: BTreeSet<String>,
    records: BTreeMap<String, AuditRecord>,
    verified_terminal_streams: BTreeSet<(String, String)>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalAuditSeal {
    format: String,
    day: String,
    node_id: String,
    last_hash: Option<String>,
    record_count: u64,
}

/// Collect durable records for this test run while validating every sealed
/// historical stream that crash retirement left behind.
async fn collect_durable_audit_evidence(
    store: &ZeppelinStore,
    prefix: &str,
) -> DurableAuditEvidence {
    const TERMINAL_SEAL_FORMAT: &str = "zeppelin_audit_terminal_seal_v1";

    let node_marker = format!("/test-node-{prefix}-");
    let mut evidence = DurableAuditEvidence::default();
    for key in store
        .list_prefix("_audit/")
        .await
        .unwrap_or_else(|error| panic!("quiet audit LIST failed: {error}"))
    {
        if !key.contains(&node_marker) || !key.ends_with(".jsonl") {
            continue;
        }
        let bytes = store
            .get(&key)
            .await
            .unwrap_or_else(|error| panic!("quiet audit GET failed for {key}: {error}"));
        let seal = serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|value| value.get("format").is_some().then_some(value))
            .map(|value| {
                serde_json::from_value::<TerminalAuditSeal>(value)
                    .unwrap_or_else(|error| panic!("invalid terminal audit seal in {key}: {error}"))
            });
        if let Some(seal) = seal {
            assert_eq!(
                seal.format, TERMINAL_SEAL_FORMAT,
                "unexpected terminal audit seal format in {key}"
            );
            let day =
                chrono::NaiveDate::parse_from_str(&seal.day, "%Y-%m-%d").unwrap_or_else(|error| {
                    panic!("invalid terminal audit seal day in {key}: {error}")
                });
            assert_eq!(
                seal.day,
                day.format("%Y-%m-%d").to_string(),
                "terminal audit seal day was not canonical in {key}"
            );
            assert_eq!(
                seal.record_count == 0,
                seal.last_hash.is_none(),
                "terminal audit seal hash presence disagreed with record count in {key}"
            );
            let stream_prefix = format!("_audit/{}/{}/", seal.day, seal.node_id);
            assert!(
                key.starts_with(&stream_prefix),
                "terminal audit seal identity did not match its object key {key}"
            );
            assert!(
                evidence
                    .verified_terminal_streams
                    .insert((seal.day.clone(), seal.node_id.clone())),
                "duplicate terminal audit seal for {}/{}",
                seal.day,
                seal.node_id
            );
            let verification = verify_audit_day(store, day, &seal.node_id)
                .await
                .unwrap_or_else(|error| {
                    panic!("terminal audit seal verification failed for {key}: {error}")
                });
            assert!(
                verification.valid,
                "terminal audit seal verification failed for {key}: {verification:?}"
            );
            continue;
        }

        let body = String::from_utf8(bytes.to_vec())
            .unwrap_or_else(|error| panic!("quiet audit object {key} was not UTF-8: {error}"));
        for line in body.lines().filter(|line| !line.is_empty()) {
            let record: AuditRecord = serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("quiet audit record in {key} was invalid: {error}"));
            evidence.request_ids.insert(record.request_id.clone());
            evidence.records.insert(record.request_id.clone(), record);
        }
    }
    evidence
}

async fn run_security_refresh_checks(
    server: &FullTestServer,
    model: &mut Model,
    coverage: &mut Coverage,
    prefix: &str,
    op_index: u64,
    mutation: Option<OracleMutation>,
) -> Vec<Violation> {
    if !model.security.enabled() {
        return Vec::new();
    }

    server.force_policy_refresh().await;
    let mut findings = Vec::new();
    server.flush_audit().await;
    let audit_prefix = "_audit/";
    let durable_evidence = collect_durable_audit_evidence(&server.store, prefix).await;
    let mut durable_request_ids = durable_evidence.request_ids;
    let durable_audit_records = durable_evidence.records;
    if mutation == Some(OracleMutation::AuditRecordDeletion) {
        if let Some(request_id) = model.security.successful_audit_requests.iter().next() {
            durable_request_ids.remove(request_id);
        }
    }
    if let Some(finding) = check_i25_audit_evidence(
        &model.security.successful_audit_requests,
        &durable_request_ids,
    ) {
        findings.push(finding);
    }

    if mutation == Some(OracleMutation::AuditChainRecordDrop) {
        let broken = exercise_audit_record_drop(
            &server.store,
            server.clock.now().date_naive(),
            prefix,
            durable_audit_records.values().next().cloned(),
        )
        .await;
        assert!(
            !broken.valid,
            "production audit verifier accepted a dropped persisted record"
        );
        findings.push(SecurityFinding {
            id: ViolationId::I25AuditEvidence,
            detail: "production audit-chain verifier detected one dropped persisted record"
                .to_string(),
            evidence: json!({
                "mutation": mutation.map(OracleMutation::key),
                "first_divergence": broken.first_divergence,
                "verified_records": broken.verified_records,
            }),
        });
    }

    let preservation_resolution = async {
        let preservation = server.security.preservation_service().ok_or_else(|| {
            "security profile omitted the preservation authority at quiescence".to_string()
        })?;
        preservation
            .refresh_once()
            .await
            .map_err(|error| format!("authoritative preservation refresh failed: {error}"))?;
        let active = preservation
            .list_active()
            .map_err(|error| format!("authoritative preservation list failed: {error}"))?
            .into_iter()
            .map(|record| {
                (
                    record.lock_id.as_str().to_string(),
                    (
                        modeled_preservation_scope(&record.scope),
                        record.reason_text,
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        model
            .security
            .resolve_preservation_authoritative(&active, &durable_audit_records)
    }
    .await;
    let preservation_resolved = match preservation_resolution {
        Ok(installs) => {
            for (selector, lock_id) in installs {
                install_preservation_lock(&server.base_url, selector, lock_id);
            }
            ensure_quiet_preservation_lock(server, model).await;
            true
        }
        Err(detail) => {
            findings.push(SecurityFinding {
                id: ViolationId::I18IndeterminateResolution,
                detail,
                evidence: json!({
                    "pending_preservation_mutations": &model
                        .security
                        .indeterminate_preservation_mutations,
                }),
            });
            false
        }
    };

    let head_key = format!("{prefix}/_security/heads/policy.json");
    let head_bytes = server.store.get(&head_key).await.ok();
    let head = head_bytes
        .as_deref()
        .and_then(|bytes| serde_json::from_slice::<PolicyHead>(bytes).ok());
    let mut checksum_valid = false;
    let mut observed_version = 0;
    let mut authoritative_snapshot = None;
    if let Some(head) = &head {
        observed_version = head.version().get();
        let snapshot_key = format!("{prefix}/{}", head.object_key());
        if let Ok(snapshot_bytes) = server.store.get(&snapshot_key).await {
            if let Ok(snapshot) = serde_json::from_slice::<PolicySnapshot>(&snapshot_bytes) {
                checksum_valid = snapshot.verify_checksum().is_ok()
                    && snapshot.version() == head.version()
                    && snapshot.checksum() == head.checksum();
                if checksum_valid {
                    authoritative_snapshot = Some(snapshot);
                }
            }
        }
    }

    let mut secrets = server.workload_credentials.all_secrets();
    secrets.extend(delegated_token_secrets(&server.base_url));
    let mut leaked_secret_locations = Vec::new();
    for state_prefix in [format!("{prefix}/_security/"), audit_prefix.to_string()] {
        for key in server
            .store
            .list_prefix(&state_prefix)
            .await
            .unwrap_or_else(|error| {
                panic!("security-state LIST failed for {state_prefix}: {error}")
            })
        {
            if state_prefix == audit_prefix && !key.contains(&format!("/test-node-{prefix}-")) {
                continue;
            }
            let bytes = server
                .store
                .get(&key)
                .await
                .unwrap_or_else(|error| panic!("security-state GET failed for {key}: {error}"));
            if bytes_contain_secret(&bytes, &secrets) {
                leaked_secret_locations.push(key);
            }
        }
    }
    if mutation == Some(OracleMutation::SecuritySecretLeak) {
        leaked_secret_locations.push("mutation://plaintext-secret".to_string());
    }
    let observation = SecurityStateObservation {
        head_parsed: head.is_some(),
        checksum_valid,
        observed_version,
        minimum_version: model.security.policy_version,
        leaked_secret_locations,
    };
    if let Some(finding) = check_i26_security_state(&observation) {
        findings.push(finding);
    } else {
        let resolution = model.security.resolve_authoritative(
            authoritative_snapshot
                .as_ref()
                .expect("valid security state omitted authoritative snapshot"),
            server.clock.now(),
            op_index,
            &durable_audit_records,
        );
        match resolution {
            Ok(()) if preservation_resolved => model.security.close_staleness_windows(),
            Ok(()) => {}
            Err(detail) => findings.push(SecurityFinding {
                id: ViolationId::I18IndeterminateResolution,
                detail,
                evidence: json!({
                    "authoritative_policy_version": observed_version,
                    "pending_security_mutations": &model.security.indeterminate_mutations,
                }),
            }),
        }
    }

    coverage.record_security_oracle("I24");
    for (position, key) in model
        .security
        .revoked_credentials
        .clone()
        .into_iter()
        .enumerate()
    {
        let client = server.workload_credentials.client(key.actor.0, key.retired);
        let response = client
            .get(format!("{}/readyz", server.base_url))
            .header(
                "x-request-id",
                format!("adv-quiet-revocation-{}-{}", key.actor.0, key.retired),
            )
            .send()
            .await
            .unwrap_or_else(|error| panic!("quiet revocation probe failed: {error}"));
        let mut status = response.status().as_u16();
        if mutation == Some(OracleMutation::RevocationMisclassification) && position == 0 {
            status = 200;
        }
        if let Some(finding) = check_i24_revocation_freshness(true, status) {
            findings.push(finding);
        }
    }
    coverage.record_security_oracle("I25");
    coverage.record_security_oracle("I26");

    findings
        .into_iter()
        .map(|finding| security_finding_to_violation(finding, op_index, "_security"))
        .collect()
}

fn modeled_preservation_scope(
    scope: &zeppelin::security::PreservationScope,
) -> PreservationScopeSpec {
    match scope {
        zeppelin::security::PreservationScope::Global => PreservationScopeSpec::Global,
        zeppelin::security::PreservationScope::Namespace { namespace } => {
            PreservationScopeSpec::Namespace {
                namespace: namespace.as_str().to_string(),
            }
        }
        zeppelin::security::PreservationScope::NamespaceFilter { namespace, filter } => {
            PreservationScopeSpec::NamespaceFilter {
                namespace: namespace.as_str().to_string(),
                filter: serde_json::to_value(filter).unwrap_or_else(|error| {
                    panic!("authoritative preservation filter failed to serialize: {error}")
                }),
            }
        }
    }
}

async fn ensure_quiet_preservation_lock(server: &FullTestServer, model: &mut Model) {
    if model
        .security
        .preservation_locks
        .values()
        .any(|lock| lock.active)
    {
        return;
    }
    assert!(
        model
            .namespaces
            .values()
            .any(|namespace| !namespace.live.is_empty()),
        "security quiet period requires one live namespace for preservation validation"
    );
    let selector = LockSel(1);
    assert!(
        !model.security.preservation_locks.contains_key(&selector),
        "quiet preservation selector was already used"
    );
    let admin = client_with_bearer(&server.admin_bearer);
    let path = "/v1/security/preservation";
    let (status, response) = request_json(
        &admin,
        Method::POST,
        &format!("{}{}", server.base_url, path),
        Some(json!({
            "scope": {"kind": "global"},
            "reason_kind": "investigation",
            "reason_text": "adversarial quiet-period global preservation lock"
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED.as_u16(),
        "quiet preservation lock creation failed: {response}"
    );
    let lock_id = response
        .get("lock_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("quiet preservation response omitted lock_id: {response}"))
        .to_string();
    install_preservation_lock(&server.base_url, selector, lock_id.clone());
    model.security.preservation_locks.insert(
        selector,
        super::security_program::ModeledPreservationLock {
            lock_id,
            scope: PreservationScopeSpec::Global,
            active: true,
        },
    );
}

fn security_finding_to_violation(
    finding: SecurityFinding,
    op_index: u64,
    namespace: &str,
) -> Violation {
    Violation {
        id: finding.id,
        op_index,
        namespace: namespace.to_string(),
        detail: finding.detail,
        evidence: finding.evidence,
    }
}

async fn restore_misdirected_write_artifacts(
    store: &ZeppelinStore,
    scheduler: Option<&FaultScheduler>,
    mutation: Option<OracleMutation>,
) -> String {
    let mut proven_keys = BTreeSet::new();
    if let Some(scheduler) = scheduler {
        let schedule = scheduler.schedule();
        for event in scheduler.timeline() {
            let is_scheduled_misdirected_write = schedule.events.iter().any(|scheduled| {
                scheduled.id == event.event_id
                    && scheduled.boundary == Boundary::ObjectStore
                    && matches!(
                        scheduled.kind,
                        FaultKind::Content(ContentFault::MisdirectedWrite)
                    )
            });
            if !is_scheduled_misdirected_write {
                continue;
            }
            assert_eq!(event.boundary, Boundary::ObjectStore);
            assert_eq!(event.semantics, FaultSemantics::PostCommit);
            assert_eq!(event.observed, ObservedResult::Corrupted);
            assert!(
                event.action.starts_with("Content(MisdirectedWrite) call="),
                "misdirected-write timeline action lost its exact fault identity: {}",
                event.action
            );
            let original = event
                .key
                .as_deref()
                .expect("misdirected-write timeline evidence omitted the original key");
            let persisted = event
                .recovery
                .as_deref()
                .and_then(|recovery| recovery.strip_prefix("payload persisted at "))
                .expect("misdirected-write timeline evidence omitted the persisted key");
            assert_eq!(
                persisted,
                format!("{original}.misdirected"),
                "misdirected-write timeline evidence named an unexpected artifact"
            );
            proven_keys.insert(persisted.to_string());
        }
    }

    let key_list = proven_keys.iter().cloned().collect::<Vec<_>>().join(",");
    if mutation == Some(OracleMutation::MisdirectedWriteReachability) {
        return format!(
            "misdirected_artifacts_proven={}; cleanup_deferred_for_selftest=true; keys=[{}]",
            proven_keys.len(),
            key_list
        );
    }

    let mut deleted = 0usize;
    let mut already_absent = 0usize;
    for key in &proven_keys {
        if store
            .exists(key)
            .await
            .unwrap_or_else(|error| panic!("misdirected artifact existence check failed: {error}"))
        {
            store
                .delete(key)
                .await
                .unwrap_or_else(|error| panic!("misdirected artifact cleanup failed: {error}"));
            deleted += 1;
        } else {
            already_absent += 1;
        }
        assert!(
            !store.exists(key).await.unwrap_or_else(|error| panic!(
                "misdirected artifact cleanup verification failed: {error}"
            )),
            "misdirected artifact remained after canonical quiet restoration: {key}"
        );
    }

    format!(
        "misdirected_artifacts_proven={}; deleted={deleted}; already_absent={already_absent}; \
         verified_absent={}; keys=[{key_list}]",
        proven_keys.len(),
        proven_keys.len()
    )
}

async fn stop_background_compaction(server: &mut FullTestServer) {
    if let Some(shutdown) = server.shutdown_compaction.take() {
        let _ = shutdown.send(true);
    }
    if let Some(task) = server.compaction_loop_task.take() {
        task.await
            .unwrap_or_else(|error| panic!("background compaction task failed: {error}"));
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_quiescent_checks(
    client: &Client,
    server: &FullTestServer,
    artifacts: &mut SeedArtifacts,
    model: &mut Model,
    coverage: &mut Coverage,
    s3_tracker: &mut S3Tracker,
    corruption_tracker: &mut CorruptionTracker,
    op_index: &mut u64,
    compactions: &mut u64,
    started: Instant,
    dual_writer_stale_fencing_token: Option<u64>,
    mutation: Option<OracleMutation>,
    mode: RunMode,
    exact_vector_count: bool,
    quiet_timeline: &mut Vec<TimelineEvent>,
    scheduler: Option<&FaultScheduler>,
) -> Vec<Violation> {
    let mut violations = resolve_indeterminates(
        client,
        server,
        model,
        artifacts,
        corruption_tracker,
        mutation,
        *op_index,
    )
    .await;
    push_quiet_event(
        quiet_timeline,
        scheduler,
        started,
        *op_index,
        8,
        Boundary::Runner,
        "resolve-indeterminates",
        quiet_observed(&violations),
        Some(if violations.is_empty() {
            "ambiguity drained".to_string()
        } else {
            format!("violations={}", violations.len())
        }),
    );
    if !violations.is_empty() {
        push_skipped_quiet_steps(quiet_timeline, scheduler, started, *op_index, 9);
        return violations;
    }

    let namespaces = model.namespace_names();
    'compactions: for ns in &namespaces {
        let compact = Op::CompactInline {
            actor: ActorSel::ADMIN,
            ns: ns.clone(),
        };
        let step = execute_recorded_op(
            client,
            server,
            artifacts,
            model,
            coverage,
            s3_tracker,
            corruption_tracker,
            &compact,
            *op_index,
            started,
            mutation,
            mode,
            ExecutionPhase::Quiescence,
            true,
            0,
            None,
            false,
        )
        .await;
        *op_index += 1;
        *compactions += u64::from((200..300).contains(&step.status));
        if !(200..300).contains(&step.status) {
            if accept_loud_tainted_quiescence(
                step.status,
                &step.violations,
                corruption_tracker.tainted_keys(ns),
            ) {
                eprintln!(
                    "accepted loud quiescence failure for known-tainted namespace {ns}: \
                     status={}",
                    step.status
                );
                continue;
            }
            if !step.violations.is_empty() {
                violations = step.violations;
                break 'compactions;
            }
            violations = vec![Violation {
                id: ViolationId::I16Quiescence,
                op_index: *op_index,
                namespace: ns.clone(),
                detail: "quiescence compaction failed".to_string(),
                evidence: serde_json::json!({ "status": step.status }),
            }];
            break 'compactions;
        }
        if !step.violations.is_empty() {
            violations = step.violations;
            break 'compactions;
        }
    }
    push_quiet_event(
        quiet_timeline,
        scheduler,
        started,
        *op_index,
        9,
        Boundary::Runner,
        "force-compaction",
        quiet_observed(&violations),
        Some(format!(
            "namespaces={}; violations={}",
            namespaces.len(),
            violations.len()
        )),
    );
    if !violations.is_empty() {
        push_skipped_quiet_steps(quiet_timeline, scheduler, started, *op_index, 10);
        return violations;
    }

    'gc: for ns in &namespaces {
        for _ in 0..2 {
            let gc = Op::GcCycle {
                actor: ActorSel::ADMIN,
                ns: ns.clone(),
                keep_count: 1,
            };
            let step = execute_recorded_op(
                client,
                server,
                artifacts,
                model,
                coverage,
                s3_tracker,
                corruption_tracker,
                &gc,
                *op_index,
                started,
                mutation,
                mode,
                ExecutionPhase::Quiescence,
                true,
                0,
                None,
                false,
            )
            .await;
            *op_index += 1;
            if !step.violations.is_empty() {
                violations = step.violations;
                break 'gc;
            }
        }
    }
    push_quiet_event(
        quiet_timeline,
        scheduler,
        started,
        *op_index,
        10,
        Boundary::Runner,
        "gc-twice",
        quiet_observed(&violations),
        Some(format!(
            "namespaces={}; cycles={}; violations={}",
            namespaces.len(),
            namespaces.len().saturating_mul(2),
            violations.len()
        )),
    );
    if !violations.is_empty() {
        push_skipped_quiet_steps(quiet_timeline, scheduler, started, *op_index, 11);
        return violations;
    }

    if mutation == Some(OracleMutation::DualWriterFencing) {
        let namespace = namespaces
            .iter()
            .find(|namespace| {
                model
                    .namespaces
                    .get(*namespace)
                    .is_some_and(|ns_model| ns_model.spec.is_exact())
            })
            .unwrap_or_else(|| {
                panic!("dual-writer fencing mutation requires a live exact namespace")
            });
        violations = inject_dual_writer_fencing_mutation(
            &server.store,
            s3_tracker,
            namespace,
            *op_index,
            dual_writer_stale_fencing_token.unwrap_or_else(|| {
                panic!("dual-writer fencing mutation requires node A's stale fencing token")
            }),
        )
        .await;
    }
    if violations.is_empty() && mutation == Some(OracleMutation::MisdirectedWriteReachability) {
        'misdirected: for ns in &namespaces {
            let Some(ns_model) = model.namespaces.get(ns) else {
                continue;
            };
            if !ns_model.spec.is_exact() {
                continue;
            }
            let status = match Manifest::read(&server.store, ns).await {
                Ok(Some(manifest)) => json!({
                    "manifest_generation": manifest.version(),
                }),
                Ok(None) => json!({ "manifest_generation": null }),
                Err(error) => {
                    violations.push(Violation {
                        id: ViolationId::I15ManifestLineage,
                        op_index: *op_index,
                        namespace: ns.clone(),
                        detail: "selftest manifest read-failed at quiescence".to_string(),
                        evidence: json!({ "error": error.to_string() }),
                    });
                    break 'misdirected;
                }
            };
            violations = s3_tracker
                .check_namespace(&server.store, ns, *op_index, &status, false)
                .await;
            if !violations.is_empty() {
                break 'misdirected;
            }
        }
    }
    'oracles: for ns in &namespaces {
        if !violations.is_empty() {
            break;
        }
        let Some(status) = quiescent_s3_oracle_status(
            client,
            &server.base_url,
            ns,
            corruption_tracker.durably_tainted_keys(ns),
        )
        .await
        else {
            continue;
        };
        let expected_live = model
            .namespaces
            .get(ns)
            .map_or(0, |ns_model| ns_model.live.len());
        let preservation_locked = model.security.preservation_locks.values().any(|lock| {
            lock.active
                && match &lock.scope {
                    PreservationScopeSpec::Global => true,
                    PreservationScopeSpec::Namespace { namespace }
                    | PreservationScopeSpec::NamespaceFilter { namespace, .. } => namespace == ns,
                }
        });
        let mut oracle_violations = if preservation_locked {
            s3_oracle::check_quiescent_namespace_under_preservation(
                &server.store,
                ns,
                expected_live,
                &status,
                *op_index,
                exact_vector_count,
            )
            .await
        } else if exact_vector_count {
            s3_oracle::check_quiescent_namespace_after_second_node(
                &server.store,
                ns,
                expected_live,
                &status,
                *op_index,
            )
            .await
        } else {
            s3_oracle::check_quiescent_namespace(
                &server.store,
                ns,
                expected_live,
                &status,
                *op_index,
            )
            .await
        };
        oracle_violations
            .extend(s3_oracle::check_v4_sketch_publication(&server.store, ns, *op_index).await);
        oracle_violations.extend(
            if model
                .namespaces
                .get(ns)
                .is_some_and(|ns_model| ns_model.spec.is_exact())
            {
                s3_tracker
                    .check_namespace(
                        &server.store,
                        ns,
                        *op_index,
                        &status,
                        matches!(
                            mutation,
                            Some(OracleMutation::GcEatsLiveKey | OracleMutation::ClockGcEatsLive)
                        ),
                    )
                    .await
            } else {
                Vec::new()
            },
        );
        violations.extend(oracle_violations);
        if !violations.is_empty() {
            break 'oracles;
        }
    }
    push_quiet_event(
        quiet_timeline,
        scheduler,
        started,
        *op_index,
        11,
        Boundary::ObjectStore,
        "s3-oracles",
        quiet_observed(&violations),
        Some(format!(
            "namespaces={}; violations={}",
            namespaces.len(),
            violations.len()
        )),
    );
    if !violations.is_empty() {
        push_skipped_quiet_steps(quiet_timeline, scheduler, started, *op_index, 12);
        return violations;
    }

    'sweep: for ns in &namespaces {
        let ns_model = model
            .namespaces
            .get(ns)
            .unwrap_or_else(|| panic!("quiescence namespace {ns} disappeared from the model"));
        if ns_model.spec.late_interaction.is_some() {
            let Some(query) = ns_model
                .late_live
                .values()
                .next()
                .map(|record| record.values.clone())
            else {
                continue;
            };
            let query = Op::LateQuery {
                actor: ActorSel::ADMIN,
                ns: ns.clone(),
                query,
                top_k: ns_model.late_live.len().min(8),
                filter: None,
                consistency: ConsistencyLevel::Strong,
            };
            let step = execute_recorded_op(
                client,
                server,
                artifacts,
                model,
                coverage,
                s3_tracker,
                corruption_tracker,
                &query,
                *op_index,
                started,
                mutation,
                mode,
                ExecutionPhase::Quiescence,
                true,
                0,
                None,
                false,
            )
            .await;
            *op_index += 1;
            if !step.violations.is_empty() {
                violations = step.violations;
                break 'sweep;
            }
            continue;
        }

        let ids = ns_model.live.keys().cloned().collect::<Vec<_>>();
        let fetch = Op::FetchVectors {
            actor: ActorSel::ADMIN,
            ns: ns.clone(),
            ids,
            consistency: ConsistencyLevel::Strong,
        };
        let step = execute_recorded_op(
            client,
            server,
            artifacts,
            model,
            coverage,
            s3_tracker,
            corruption_tracker,
            &fetch,
            *op_index,
            started,
            mutation,
            mode,
            ExecutionPhase::Quiescence,
            true,
            0,
            None,
            false,
        )
        .await;
        *op_index += 1;
        if !step.violations.is_empty() {
            violations = step.violations;
            break 'sweep;
        }

        let q = exhaustive_query_from_model(model, ns);
        let query = Op::Query {
            actor: ActorSel::ADMIN,
            ns: ns.clone(),
            q,
            as_of: None,
        };
        let step = execute_recorded_op(
            client,
            server,
            artifacts,
            model,
            coverage,
            s3_tracker,
            corruption_tracker,
            &query,
            *op_index,
            started,
            mutation,
            mode,
            ExecutionPhase::Quiescence,
            true,
            0,
            None,
            false,
        )
        .await;
        *op_index += 1;
        if !step.violations.is_empty() {
            violations = step.violations;
            break 'sweep;
        }
    }
    if violations.is_empty() {
        violations.extend(
            check_quiet_security_visibility(server, model, coverage, *op_index, mutation).await,
        );
    }
    push_quiet_event(
        quiet_timeline,
        scheduler,
        started,
        *op_index,
        12,
        Boundary::ClientHttp,
        "exhaustive-sweep",
        quiet_observed(&violations),
        Some(format!(
            "namespaces={}; violations={}",
            namespaces.len(),
            violations.len()
        )),
    );
    violations
}

async fn check_quiet_security_visibility(
    server: &FullTestServer,
    model: &Model,
    coverage: &mut Coverage,
    op_index: u64,
    mutation: Option<OracleMutation>,
) -> Vec<Violation> {
    let Some(config) = model.security.config.clone() else {
        return Vec::new();
    };
    coverage.record_security_oracle("I23");
    coverage.record_security_oracle("I27");
    coverage.record_security_oracle("I28");
    let mut violations = Vec::new();
    for actor in [ActorSel(2), ActorSel(3)] {
        let namespace = config
            .tenant_namespace(actor)
            .unwrap_or_else(|| panic!("tenant actor {} has no namespace grant", actor.0));
        let namespace_model = model
            .namespaces
            .get(namespace)
            .unwrap_or_else(|| panic!("tenant namespace {namespace} is absent from the model"));
        let expected = model.security.expected_visible_ids(model, actor);
        let client = server.workload_credentials.client(actor.0, 0);
        let path = format!("/v1/namespaces/{namespace}/query");
        let body = json!({
            "sources": [{
                "type": "ann",
                "vector": vec![0.0_f32; namespace_model.spec.dims],
                "nprobe": namespace_model.spec.num_centroids
            }],
            "fusion": {"type": "none"},
            "top_k": expected.len().max(1),
            "consistency": "strong",
            "include_attributes": true
        });
        let response = client
            .post(format!("{}{}", server.base_url, path))
            .header("x-request-id", format!("adv-quiet-tenant-{}", actor.0))
            .json(&body)
            .send()
            .await
            .unwrap_or_else(|error| {
                panic!("quiet tenant query failed for actor {}: {error}", actor.0)
            });
        let status = response.status().as_u16();
        let body = response
            .json::<serde_json::Value>()
            .await
            .unwrap_or_else(|error| panic!("quiet tenant query returned non-JSON: {error}"));
        if let Some(finding) = check_i22_authz_decision(ExpectedDecision::Allow, status) {
            violations.push(security_finding_to_violation(finding, op_index, namespace));
            continue;
        }
        let mut observed = oracle::security_response_ids(&body);
        if mutation == Some(OracleMutation::ConstraintDrop) && actor == ActorSel(2) {
            if let Some(id) = observed.iter().next().cloned() {
                observed.remove(&id);
            } else {
                observed.insert("mutation-constraint-drop".to_string());
            }
        }
        if let Some(finding) = check_i23_tenant_leak(&expected, &observed) {
            violations.push(security_finding_to_violation(finding, op_index, namespace));
        }
        if let Some(finding) = check_i27_constraint_drop(&expected, &observed) {
            violations.push(security_finding_to_violation(finding, op_index, namespace));
        }
    }

    let admin = client_with_bearer(&server.admin_bearer);
    let destruction_keys = server
        .store
        .list_prefix("_audit/destruction/")
        .await
        .unwrap_or_else(|error| panic!("quiet destruction evidence LIST failed: {error}"))
        .into_iter()
        .collect::<BTreeSet<_>>();
    for key in &destruction_keys {
        let bytes = server
            .store
            .get(key)
            .await
            .unwrap_or_else(|error| panic!("quiet destruction evidence GET failed: {error}"));
        let _: serde_json::Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("quiet destruction record was invalid: {error}"));
    }
    let mut injected_bypass = false;
    for (selector, lock) in model
        .security
        .preservation_locks
        .iter()
        .filter(|(_, lock)| lock.active)
    {
        let locked_namespaces = match &lock.scope {
            PreservationScopeSpec::Global => model.namespaces.keys().cloned().collect::<Vec<_>>(),
            PreservationScopeSpec::Namespace { namespace }
            | PreservationScopeSpec::NamespaceFilter { namespace, .. } => {
                vec![namespace.clone()]
            }
        };
        for namespace in locked_namespaces {
            // Destruction evidence is incarnation-specific even though its JSON
            // names only the reusable namespace string. Consult the current
            // authoritative tombstone binding instead of treating an older
            // incarnation's valid evidence as a bypass of a later global lock.
            let current_meta = match server.store.get(&format!("{namespace}/meta.json")).await {
                Ok(bytes) => Some(
                    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_else(|error| {
                        panic!("quiet namespace metadata was invalid for {namespace}: {error}")
                    }),
                ),
                Err(ZeppelinError::NotFound { .. }) => None,
                Err(error) => {
                    panic!("quiet namespace metadata GET failed for {namespace}: {error}")
                }
            };
            let current_destruction_record =
                current_destruction_record_present(current_meta.as_ref(), &destruction_keys);
            let current_incarnation_destroyed =
                current_incarnation_destroyed(current_meta.as_ref());
            let expected = model
                .namespaces
                .get(&namespace)
                .map(|namespace| namespace.live.keys().cloned().collect::<BTreeSet<_>>())
                .unwrap_or_default();
            let mut observed = BTreeSet::new();
            let mut status = 200;
            let mut body = serde_json::Value::Null;
            if !expected.is_empty() {
                let path = format!("/v1/namespaces/{namespace}/vectors/get");
                let response = admin
                    .post(format!("{}{}", server.base_url, path))
                    .header(
                        "x-request-id",
                        format!("adv-quiet-preservation-{}-{namespace}", selector.0),
                    )
                    .json(&json!({
                        "ids": expected.iter().cloned().collect::<Vec<_>>(),
                        "consistency": "strong",
                        "include_vector": false,
                        "include_attributes": false
                    }))
                    .send()
                    .await
                    .unwrap_or_else(|error| panic!("quiet preservation fetch failed: {error}"));
                status = response.status().as_u16();
                body = response
                    .json::<serde_json::Value>()
                    .await
                    .unwrap_or_else(|error| {
                        panic!("quiet preservation fetch was non-JSON: {error}")
                    });
                observed = oracle::security_response_ids(&body);
            }
            if mutation == Some(OracleMutation::PreservationBypass) && !injected_bypass {
                injected_bypass = true;
                observed.clear();
            }
            if status != 200
                || observed != expected
                || current_destruction_record
                || current_incarnation_destroyed
            {
                violations.push(Violation {
                    id: ViolationId::I28PreservationBypass,
                    op_index,
                    namespace: namespace.clone(),
                    detail: "active preservation lock did not retain every modeled row without destruction evidence"
                        .to_string(),
                    evidence: json!({
                        "lock": selector.0,
                        "lock_id": lock.lock_id,
                        "scope": lock.scope,
                        "status": status,
                        "expected": expected,
                        "observed": observed,
                        "destruction_record": current_destruction_record,
                        "namespace_metadata_missing": current_incarnation_destroyed,
                        "body": body,
                    }),
                });
            }
        }
    }
    violations
}

fn current_destruction_record_present(
    current_meta: Option<&serde_json::Value>,
    destruction_keys: &BTreeSet<String>,
) -> bool {
    current_meta
        .and_then(|meta| meta.get("destruction_record_key"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|key| destruction_keys.contains(key))
}

fn current_incarnation_destroyed(current_meta: Option<&serde_json::Value>) -> bool {
    current_meta.is_none()
}

fn push_skipped_quiet_steps(
    timeline: &mut Vec<TimelineEvent>,
    scheduler: Option<&FaultScheduler>,
    started: Instant,
    op_index: u64,
    first_step: u8,
) {
    for (step, boundary, action) in [
        (9, Boundary::Runner, "force-compaction"),
        (10, Boundary::Runner, "gc-twice"),
        (11, Boundary::ObjectStore, "s3-oracles"),
        (12, Boundary::ClientHttp, "exhaustive-sweep"),
    ]
    .into_iter()
    .filter(|(step, _, _)| *step >= first_step)
    {
        push_quiet_event(
            timeline,
            scheduler,
            started,
            op_index,
            step,
            boundary,
            action,
            ObservedResult::DefiniteNotApplied,
            Some("skipped after an earlier quiet-period violation".to_string()),
        );
    }
}

async fn resolve_indeterminates(
    client: &Client,
    server: &FullTestServer,
    model: &mut Model,
    artifacts: &SeedArtifacts,
    corruption_tracker: &CorruptionTracker,
    mutation: Option<OracleMutation>,
    op_index: u64,
) -> Vec<Violation> {
    let mut resolutions = model
        .security
        .take_resolved_mutations()
        .into_iter()
        .map(|resolution| {
            serde_json::to_value(resolution).expect("security mutation resolution must serialize")
        })
        .collect::<Vec<_>>();
    let mut violations = Vec::new();

    for ns in model.namespace_names() {
        let entries = model
            .namespaces
            .get_mut(&ns)
            .map(|ns_model| std::mem::take(&mut ns_model.indeterminate_ns))
            .unwrap_or_default();
        for entry in entries {
            match entry {
                NsIndeterminate::MaybeCreatedNs => {
                    let exists = server
                        .store
                        .exists(&format!("{ns}/manifest.json"))
                        .await
                        .unwrap_or_else(|error| {
                            panic!("manifest existence probe failed for {ns}: {error}")
                        });
                    if exists {
                        resolutions.push(json!({
                            "namespace": ns,
                            "effect": "maybe_created_namespace",
                            "resolved": "applied"
                        }));
                    } else {
                        model.namespaces.remove(&ns);
                        resolutions.push(json!({
                            "namespace": ns,
                            "effect": "maybe_created_namespace",
                            "resolved": "not_applied"
                        }));
                        break;
                    }
                }
                NsIndeterminate::MaybeDeletedNs => {
                    // A metadata tombstone or a manifest destruction fence is
                    // authoritative terminal state. A failed or lost delete
                    // response can leave `meta.json` active while the fence is
                    // already durable, or leave it deleting while the manifest
                    // survives until governed cleanup resumes. Manifest
                    // presence alone kept both states in the live model and
                    // sent quiet compaction back into the product's deletion
                    // fence (seed 113, F1).
                    let metadata = match server.store.get(&format!("{ns}/meta.json")).await {
                        Ok(bytes) => Some(NamespaceMetadata::from_bytes(&bytes).unwrap_or_else(
                            |error| {
                                panic!("deletion resolution metadata was invalid for {ns}: {error}")
                            },
                        )),
                        Err(ZeppelinError::NotFound { .. }) => None,
                        Err(error) => {
                            panic!("deletion resolution metadata GET failed for {ns}: {error}")
                        }
                    };
                    let manifest_fenced = if metadata
                        .as_ref()
                        .is_some_and(|metadata| metadata.deletion_intent.is_some())
                    {
                        Manifest::read(&server.store, &ns)
                            .await
                            .unwrap_or_else(|error| {
                                panic!("deletion resolution manifest GET failed for {ns}: {error}")
                            })
                            .is_some_and(|manifest| manifest.is_deletion_fenced())
                    } else {
                        false
                    };
                    let lifecycle_disposition = match metadata.as_ref() {
                        None => "absent",
                        Some(metadata) if metadata.state == NamespaceState::Deleting => "deleting",
                        Some(_) if manifest_fenced => "deletion_fenced",
                        Some(metadata) if metadata.deletion_intent.is_some() => "active_unfenced",
                        Some(metadata) => metadata.state.as_str(),
                    };
                    if metadata.as_ref().is_none_or(|metadata| {
                        metadata.state == NamespaceState::Deleting || manifest_fenced
                    }) {
                        model.namespaces.remove(&ns);
                        resolutions.push(json!({
                            "namespace": ns,
                            "effect": "maybe_deleted_namespace",
                            "resolved": "applied",
                            "lifecycle_disposition": lifecycle_disposition
                        }));
                        break;
                    }
                    resolutions.push(json!({
                        "namespace": ns,
                        "effect": "maybe_deleted_namespace",
                        "resolved": "not_applied",
                        "lifecycle_disposition": lifecycle_disposition
                    }));
                }
                NsIndeterminate::MaybeSnapshot { name } => {
                    let path = format!("/v1/namespaces/{ns}/snapshots/{name}");
                    let (status, response) = request_json(
                        client,
                        Method::GET,
                        &format!("{}{}", server.base_url, path),
                        None,
                    )
                    .await;
                    if (200..300).contains(&status) {
                        let generation = response["generation"]
                            .as_u64()
                            .or_else(|| {
                                model.namespaces.get(&ns).map(|model| model.live_generation)
                            })
                            .unwrap_or(0);
                        if let Some(ns_model) = model.namespaces.get_mut(&ns) {
                            ns_model.snapshots.insert(name.clone(), generation);
                        }
                        resolutions.push(json!({
                            "namespace": ns,
                            "effect": "maybe_snapshot",
                            "name": name,
                            "resolved": "applied"
                        }));
                    } else if matches!(status, 404 | 410) {
                        resolutions.push(json!({
                            "namespace": ns,
                            "effect": "maybe_snapshot",
                            "name": name,
                            "resolved": "not_applied"
                        }));
                    } else {
                        violations.push(indeterminate_violation(
                            op_index,
                            &ns,
                            "snapshot creation resolution returned an unexpected status",
                            json!({ "name": name, "status": status, "response": response }),
                        ));
                    }
                }
                NsIndeterminate::MaybeSnapshotDeleted { name } => {
                    let path = format!("/v1/namespaces/{ns}/snapshots/{name}");
                    let (status, response) = request_json(
                        client,
                        Method::GET,
                        &format!("{}{}", server.base_url, path),
                        None,
                    )
                    .await;
                    if matches!(status, 404 | 410) {
                        if let Some(ns_model) = model.namespaces.get_mut(&ns) {
                            ns_model.snapshots.remove(&name);
                        }
                        resolutions.push(json!({
                            "namespace": ns,
                            "effect": "maybe_snapshot_deleted",
                            "name": name,
                            "resolved": "applied"
                        }));
                    } else if (200..300).contains(&status) {
                        resolutions.push(json!({
                            "namespace": ns,
                            "effect": "maybe_snapshot_deleted",
                            "name": name,
                            "resolved": "not_applied"
                        }));
                    } else {
                        violations.push(indeterminate_violation(
                            op_index,
                            &ns,
                            "snapshot deletion resolution returned an unexpected status",
                            json!({ "name": name, "status": status, "response": response }),
                        ));
                    }
                }
                NsIndeterminate::MaybeCloned { target, as_of } => {
                    // The copy-clone endpoint creates an empty target manifest
                    // before it publishes the copied source view. Therefore
                    // manifest existence proves only that target reservation
                    // succeeded, not that the ambiguous clone applied. Resolve
                    // against the candidate's complete authoritative contents.
                    let expected = clone_source_records(model, &ns, &as_of);
                    let manifest =
                        Manifest::read(&server.store, &target)
                            .await
                            .unwrap_or_else(|error| {
                                panic!("clone manifest resolution failed for {target}: {error}")
                            });
                    let manifest_vector_count = manifest.as_ref().map_or(0, Manifest::vector_count);
                    let count_matches = manifest.is_some()
                        && manifest_vector_count
                            == u64::try_from(expected.len())
                                .expect("clone candidate size must fit in u64");
                    let content_matches = if count_matches && !expected.is_empty() {
                        let path = format!("/v1/namespaces/{target}/vectors/get");
                        let ids = expected.keys().cloned().collect::<Vec<_>>();
                        let (status, response) = request_json(
                            client,
                            Method::POST,
                            &format!("{}{}", server.base_url, path),
                            Some(json!({
                                "ids": ids,
                                "include_vector": true,
                                "include_attributes": true,
                                "consistency": ConsistencyLevel::Strong,
                            })),
                        )
                        .await;
                        if !(200..300).contains(&status) {
                            violations.push(indeterminate_violation(
                                op_index,
                                &ns,
                                "strong fetch failed while proving ambiguous clone publication",
                                json!({
                                    "target": target,
                                    "status": status,
                                    "response": response,
                                }),
                            ));
                            false
                        } else {
                            expected.iter().all(|(id, candidate)| {
                                observed_fetch_record(&response, id)
                                    .ok()
                                    .flatten()
                                    .is_some_and(|observed| observed.semantically_eq(candidate))
                            })
                        }
                    } else {
                        count_matches
                    };
                    let publication_matches = count_matches && content_matches;
                    if publication_matches {
                        let generation = clone_source_generation(model, &ns, &as_of);
                        let later_target = model.namespaces.remove(&target);
                        model.apply(
                            &Op::CloneNamespace {
                                actor: ActorSel::ADMIN,
                                source: ns.clone(),
                                target: target.clone(),
                                as_of,
                            },
                            200,
                            None,
                            &json!({ "generation": generation }),
                            None,
                        );
                        if let Some(later_target) = later_target {
                            let resolved_target = model
                                .namespaces
                                .get_mut(&target)
                                .expect("resolved clone target must exist");
                            resolved_target.indeterminate = later_target.indeterminate;
                            resolved_target
                                .indeterminate_ns
                                .extend(later_target.indeterminate_ns);
                        }
                        resolutions.push(json!({
                            "namespace": ns,
                            "target": target,
                            "effect": "maybe_cloned",
                            "resolved": "applied",
                            "publication_disposition": "candidate_match",
                            "manifest_vector_count": manifest_vector_count,
                            "expected_vector_count": expected.len(),
                        }));
                    } else {
                        resolutions.push(json!({
                            "namespace": ns,
                            "target": target,
                            "effect": "maybe_cloned",
                            "resolved": "not_applied",
                            "publication_disposition": if manifest.is_some() {
                                "candidate_mismatch"
                            } else {
                                "manifest_absent"
                            },
                            "manifest_vector_count": manifest_vector_count,
                            "expected_vector_count": expected.len(),
                        }));
                    }
                }
                NsIndeterminate::MaybeCompacted => resolutions.push(json!({
                    "namespace": ns,
                    "effect": "maybe_compacted",
                    "resolved": "deferred_to_forced_compaction"
                })),
            }
        }
    }

    for ns in model.namespace_names() {
        let ids = model
            .namespaces
            .get(&ns)
            .map(|ns_model| ns_model.indeterminate.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        if ids.is_empty() {
            continue;
        }
        let path = format!("/v1/namespaces/{ns}/vectors/get");
        let (status, response) = request_json(
            client,
            Method::POST,
            &format!("{}{}", server.base_url, path),
            Some(json!({
                "ids": ids,
                "include_vector": true,
                "include_attributes": true,
                "consistency": ConsistencyLevel::Strong,
            })),
        )
        .await;
        if !(200..300).contains(&status) {
            if accept_loud_durable_manifest_resolution(
                status,
                &response,
                &ns,
                corruption_tracker.durably_tainted_keys(&ns),
            ) {
                eprintln!(
                    "accepted loud indeterminate-resolution failure for exact durable manifest \
                     taint in {ns}: status={status}"
                );
                resolutions.push(json!({
                    "namespace": ns,
                    "ids": ids,
                    "effect": "indeterminate_vectors",
                    "resolved": "deferred_due_to_durable_manifest_taint",
                    "status": status,
                }));
                continue;
            }
            violations.push(indeterminate_violation(
                op_index,
                &ns,
                "strong fetch failed while resolving indeterminate vectors",
                json!({ "status": status, "response": response }),
            ));
            continue;
        }

        let pending_ids = model
            .namespaces
            .get(&ns)
            .map(|ns_model| ns_model.indeterminate.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for id in pending_ids {
            let observed = match observed_fetch_record(&response, &id) {
                Ok(observed) => observed,
                Err(detail) => {
                    violations.push(indeterminate_violation(
                        op_index,
                        &ns,
                        &detail,
                        json!({ "id": id, "response": response }),
                    ));
                    continue;
                }
            };
            let pending = model.namespaces[&ns].indeterminate[&id].clone();
            if mutation == Some(OracleMutation::DroppedResponseLostWrite) {
                // Deliberately claim the ambiguous upsert was applied. The
                // ordinary resolver must reject that claim when the strong
                // fetch proves the WAL write was lost.
                let IndetEffect::MaybeUpserted(candidate) = &pending.effect else {
                    panic!(
                        "dropped-response lost-write mutation reached a non-upsert effect for {ns}/{id}"
                    );
                };
                model
                    .namespaces
                    .get_mut(&ns)
                    .expect("indeterminate namespace disappeared before selftest mutation")
                    .live
                    .insert(id.clone(), candidate.clone());
            }
            let resolved = match &pending.effect {
                IndetEffect::MaybeUpserted(candidate)
                    if observed
                        .as_ref()
                        .is_some_and(|observed| observed.semantically_eq(candidate)) =>
                {
                    "applied"
                }
                IndetEffect::MaybeDeleted if observed.is_none() => "applied",
                _ => "not_applied",
            };
            match model.resolve_indeterminate_record(&ns, &id, observed.clone()) {
                Ok(()) => resolutions.push(json!({
                    "namespace": ns,
                    "id": id,
                    "op_index": pending.op_index,
                    "reason": pending.reason,
                    "resolved": resolved,
                    "observed": observed,
                })),
                Err(detail) => violations.push(indeterminate_violation(
                    op_index,
                    &ns,
                    &detail,
                    json!({
                        "id": id,
                        "pending": pending,
                        "observed": observed,
                    }),
                )),
            }
        }
    }

    artifacts.write_resolutions(&resolutions);
    violations
}

fn clone_source_generation(model: &Model, source: &str, as_of: &super::ops::AsOfTarget) -> u64 {
    let source = model
        .namespaces
        .get(source)
        .unwrap_or_else(|| panic!("missing clone source model during resolution: {source}"));
    match as_of {
        super::ops::AsOfTarget::Generation(generation) => *generation,
        super::ops::AsOfTarget::Snapshot(name) => source
            .snapshots
            .get(name)
            .copied()
            .unwrap_or(source.live_generation),
        super::ops::AsOfTarget::Timestamp(_) => source.live_generation,
    }
}

fn clone_source_records(
    model: &Model,
    source: &str,
    as_of: &super::ops::AsOfTarget,
) -> BTreeMap<String, ModelRecord> {
    let source_model = model
        .namespaces
        .get(source)
        .unwrap_or_else(|| panic!("missing clone source model during resolution: {source}"));
    let generation = clone_source_generation(model, source, as_of);
    source_model
        .checkpoints
        .get(&generation)
        .cloned()
        .unwrap_or_else(|| source_model.live.clone())
}

fn observed_fetch_record(
    response: &serde_json::Value,
    id: &str,
) -> Result<Option<ModelRecord>, String> {
    if response["missing"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|value| value.as_str() == Some(id))
    {
        return Ok(None);
    }
    let Some(record) = response["results"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|record| record["id"].as_str() == Some(id))
    else {
        return Err(format!(
            "fetch resolution omitted {id} from both results and missing"
        ));
    };
    let values = record["values"]
        .as_array()
        .ok_or_else(|| format!("fetch resolution omitted values for {id}"))?
        .iter()
        .map(|value| {
            value
                .as_f64()
                .map(|value| value as f32)
                .ok_or_else(|| format!("fetch resolution returned non-numeric values for {id}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let attributes = record
        .get("attributes")
        .filter(|value| !value.is_null())
        .map(|value| {
            serde_json::from_value(value.clone()).map_err(|error| {
                format!("fetch resolution returned invalid attributes for {id}: {error}")
            })
        })
        .transpose()?;
    Ok(Some(ModelRecord { values, attributes }))
}

fn indeterminate_violation(
    op_index: u64,
    namespace: &str,
    detail: &str,
    evidence: serde_json::Value,
) -> Violation {
    Violation {
        id: ViolationId::I18IndeterminateResolution,
        op_index,
        namespace: namespace.to_string(),
        detail: detail.to_string(),
        evidence,
    }
}

fn exhaustive_query_from_model(model: &Model, ns: &str) -> GeneratedQuery {
    let ns_model = model
        .namespaces
        .get(ns)
        .unwrap_or_else(|| panic!("missing namespace model for quiescence query: {ns}"));
    let top_k = (ns_model.live.len() + ns_model.wal_tombstones.len()).max(1);
    let vector = ns_model.live.values().next().map_or_else(
        || vec![0.0f32; ns_model.spec.dims],
        |record| record.values.clone(),
    );
    let body = json!({
        "sources": [{
            "type": "ann",
            "vector": vector,
            "nprobe": ns_model.spec.num_centroids
        }],
        "fusion": { "type": "none" },
        "top_k": top_k,
        "candidate_k": top_k,
        "consistency": ConsistencyLevel::Strong,
        "include_attributes": true
    });
    let class = if ns_model.spec.is_exact()
        && !ns.ends_with("-sketch")
        && ns_model.wal_tombstones.is_empty()
    {
        QueryOracleClass::ExactAnn {
            top_k,
            consistency: ConsistencyLevel::Strong,
            filter: None,
        }
    } else {
        QueryOracleClass::Membership {
            consistency: ConsistencyLevel::Strong,
        }
    };
    GeneratedQuery {
        body,
        class,
        pattern_tags: Vec::new(),
    }
}

fn selftest_probe_op(
    probe: OracleMutation,
    last_op: &Op,
    model: &Model,
    generator: &mut AdversarialGenerator,
) -> Option<Op> {
    match (probe, last_op) {
        (OracleMutation::DropDelete, Op::DeleteVectors { ns, .. })
        | (OracleMutation::PhantomId, Op::Upsert { ns, .. }) => {
            let ids = model.namespaces.get(ns)?.live.keys().cloned().collect();
            Some(Op::FetchVectors {
                actor: ActorSel::ADMIN,
                ns: ns.clone(),
                ids,
                consistency: ConsistencyLevel::Strong,
            })
        }
        (OracleMutation::SkewScore, Op::Upsert { ns, .. }) => {
            let q = generator.exhaustive_query(model, ns, None);
            if matches!(&q.class, QueryOracleClass::ExactAnn { .. }) {
                Some(Op::Query {
                    actor: ActorSel::ADMIN,
                    ns: ns.clone(),
                    q,
                    as_of: None,
                })
            } else {
                None
            }
        }
        (OracleMutation::SwallowCorruption, Op::Upsert { ns, .. }) => {
            let ns_model = model.namespaces.get(ns)?;
            if ns_model.compacted_live.is_empty() {
                Some(Op::CompactInline {
                    actor: ActorSel::ADMIN,
                    ns: ns.clone(),
                })
            } else {
                let q = generator.exhaustive_query(model, ns, None);
                matches!(&q.class, QueryOracleClass::ExactAnn { .. }).then(|| Op::Query {
                    actor: ActorSel::ADMIN,
                    ns: ns.clone(),
                    q,
                    as_of: None,
                })
            }
        }
        (OracleMutation::MisdirectedWriteReachability, Op::Upsert { ns, .. }) => {
            Some(Op::CompactInline {
                actor: ActorSel::ADMIN,
                ns: ns.clone(),
            })
        }
        (
            OracleMutation::LateSkewScore
            | OracleMutation::LateHiddenGet
            | OracleMutation::LateTruncatedResultSuccess,
            Op::LateUpsert { ns, records, .. },
        ) => records.first().map(|record| Op::LateQuery {
            actor: ActorSel::ADMIN,
            ns: ns.clone(),
            query: record.values.clone(),
            top_k: records.len().min(8),
            filter: None,
            consistency: ConsistencyLevel::Strong,
        }),
        _ => None,
    }
}

fn deterministic_config() -> Config {
    let mut config = Config::default();
    config.cache.manifest_cache_ttl_ms = 0;
    config.cache.namespace_registry_ttl_ms = 0;
    config.cache.hydration_enabled = false;
    config.compaction.max_wal_fragments_before_compact = 2;
    config.indexing.default_num_centroids = 4;
    config.indexing.default_nprobe = 4;
    config.indexing.max_nprobe = 64;
    config.gc.horizon_secs = 0;
    config.gc.skew_slop_secs = 0;
    config.gc.allow_unsafe_short_horizon = true;
    config.gc.manifest_history_keep_count = 8;
    config.gc.pitr_retention_secs = 0;
    config.server.rate_limit_rps = 1_000_000;
    config.server.rate_limit_burst = 1_000_000;
    config.server.write_rate_limit_rps = 1_000_000;
    config.server.write_rate_limit_burst = 1_000_000;
    config
}

fn apply_late_namespace_config(config: &mut Config, specs: &BTreeMap<String, NamespaceSpec>) {
    let mut late_specs = specs
        .values()
        .filter_map(|spec| spec.late_interaction.as_ref());
    let Some(late) = late_specs.next() else {
        return;
    };
    assert!(
        late_specs.next().is_none(),
        "one adversarial seed may contain at most one late namespace"
    );
    assert_eq!(
        late.candidate_kind,
        zeppelin::wal::LateCandidateKind::FlatSq8,
        "phase-1 adversarial late namespace must use flat-SQ8 candidates"
    );
    config.mmli.allow_dev_encoder = true;
    config.mmli.segment.nlist = late.nlist;
    config.mmli.segment.probe_budget = late.probe_budget;
    config.mmli.segment.candidate_k = late.candidate_k;
    config.mmli.segment.kmeans_max_iterations = late.kmeans_max_iterations;
    config.mmli.segment.max_matrix_object_bytes = late.max_matrix_object_bytes;
    config.mmli.segment.max_cluster_object_bytes = late.max_cluster_object_bytes;
    config.mmli.segment.max_resident_bootstrap_bytes = late.max_resident_bootstrap_bytes;
    config.mmli.segment.read_gap_budget_bytes = late.read_gap_budget_bytes;
    config.mmli.segment.read_max_request_bytes = late.read_max_request_bytes;
    config.mmli.segment.read_max_concurrency = late.read_max_concurrency;
}

fn inspection_config() -> Config {
    let mut config = deterministic_config();
    config.security.mode = zeppelin::config::SecurityMode::OpenUnsafe;
    config
}

fn should_cleanup(preserve: PreserveMode, failed: bool) -> bool {
    match preserve {
        PreserveMode::Always => false,
        PreserveMode::OnFailure => !failed,
        PreserveMode::Never => true,
    }
}

#[cfg(test)]
mod outcome_tests {
    use axum::http::{HeaderMap, HeaderValue};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use object_store::memory::InMemory;
    use zeppelin::index::quantization::QuantizationType;
    use zeppelin::storage::read_plan::{ReadPlan, ReadPlanConfig, ReadRequest};
    use zeppelin::time::TimeSource;
    use zeppelin::types::DistanceMetric;

    use super::*;
    use crate::adversarial::faults::{Direction, FaultEvent, InjectedErrorKind, TargetSelector};
    use crate::adversarial::model::NsModel;
    use crate::adversarial::ops::AsOfTarget;

    #[test]
    fn mmli_default_read_concurrency_reaches_the_built_read_plan() {
        let config = Config::default();
        let segment = &config.mmli.segment;
        assert_eq!(segment.read_max_concurrency, 16);
        let bounds = ReadPlanConfig::new(
            segment.read_gap_budget_bytes,
            segment.read_max_request_bytes,
            segment.read_max_concurrency,
        )
        .expect("default MMLI read-plan bounds must validate");
        let plan = ReadPlan::build(
            &[ReadRequest {
                object_key: "late/segments/default-probe/matrix_0.bin".to_string(),
                range: 0..1,
            }],
            &bounds,
        )
        .expect("default MMLI read plan must build");

        // ReadPlan intentionally keeps this execution bound private. Its
        // derived Debug view lets this test pin the copied field without
        // adding a production getter solely for test access.
        assert!(
            format!("{plan:?}").contains("max_concurrent_requests: 16"),
            "MMLI config default did not reach ReadPlan.max_concurrent_requests"
        );
    }

    #[tokio::test]
    async fn seed_watchdog_fails_loudly_and_retires_a_hung_server_owner() {
        struct DropFlag(Arc<std::sync::atomic::AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let harness = TestHarness::new().await;
        let server = start_test_server_full(
            harness.store.clone(),
            Some(harness.prefix.clone()),
            deterministic_config(),
            false,
            None,
        )
        .await;
        let artifact_parent = tempfile::tempdir().expect("watchdog artifact tempdir");
        let env = RunnerEnv {
            seconds: 1,
            seeds: vec![2006],
            max_ops: Some(500),
            artifacts: artifact_parent.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Mixed,
            profile: None,
            env_echo: BTreeMap::from([("TEST_BACKEND".to_string(), "memory".to_string())]),
        };
        let artifacts = RunArtifacts::create(&env);
        let seed_dir = artifacts.root().join("seed-2006");
        let watchdog = SeedWatchdogContext::new(2006, seed_dir.clone());
        watchdog.register_server(&server);
        let owner_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let owner_drop = Arc::clone(&owner_dropped);
        let task = async move {
            let _drop_flag = DropFlag(owner_drop);
            let _server = server;
            std::future::pending::<()>().await;
        };

        let started = Instant::now();
        let OwnedSeedTask::Expired(expiration) = run_owned_seed_task(
            task,
            watchdog,
            Duration::from_millis(20),
            Duration::from_secs(1),
        )
        .await
        else {
            panic!("synthetic hung seed unexpectedly completed")
        };

        assert_eq!(expiration.owner_task_join, "cancelled-and-joined");
        assert_eq!(expiration.cleanup_result, "completed");
        assert!(owner_dropped.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(expiration.progress.runner_phase, "watchdog-expired");
        assert_eq!(
            expiration.progress.server_lifecycle["http_task_finished"],
            true
        );
        assert_eq!(
            expiration.progress.server_lifecycle["compaction_task_finished"],
            true
        );
        let outcome = seed_watchdog_outcome(&env, &artifacts, 2006, None, started, *expiration);
        assert!(outcome.failed);
        assert!(outcome.blocking_v1);
        assert_eq!(outcome.violations[0].id, ViolationId::I19CrashRecovery);
        let failure = read_failure_manifest(&seed_dir).expect("watchdog failure.json missing");
        assert_eq!(failure.seed, 2006);
        assert_eq!(failure.violations[0].id, ViolationId::I19CrashRecovery);
        let watchdog_json: serde_json::Value = serde_json::from_slice(
            &fs::read(seed_dir.join("watchdog.json")).expect("watchdog.json missing"),
        )
        .expect("watchdog.json must parse");
        assert_eq!(watchdog_json["owner_task_join"], "cancelled-and-joined");
        assert_eq!(watchdog_json["cleanup_result"], "completed");
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn operational_query_returns_a_typed_transport_outcome() {
        let result = operational_query(
            raw_adversarial_client(),
            1,
            "operational-transport".to_string(),
            "http://127.0.0.1:1".to_string(),
            json!({"sources": [], "top_k": 1}),
        )
        .await;

        assert_eq!(result.target_node, 1);
        assert_eq!(result.namespace, "operational-transport");
        assert!(matches!(
            result.exchange.outcome,
            OpOutcome::Ambiguous {
                reason: AmbiguityReason::ConnectionError,
                status: None,
            }
        ));
        let failure =
            operational_query_failure("fill-disk-cache", 17, &result, "unarmed transport failure");
        assert_eq!(failure.id, ViolationId::I19CrashRecovery);
        assert_eq!(failure.op_index, 17);
        assert_eq!(failure.namespace, "operational-transport");
        assert_eq!(failure.evidence["event_id"], "fill-disk-cache");
        assert_eq!(failure.evidence["node"], 1);
        assert_eq!(
            failure.evidence["request_outcome"],
            "ambiguous:connection_error"
        );
    }

    #[test]
    fn i28_matches_destruction_evidence_only_through_the_current_tombstone() {
        let historical_key = "_audit/destruction/old-incarnation.json".to_string();
        let current_key = "_audit/destruction/current-incarnation.json".to_string();
        let mut destruction_keys = BTreeSet::from([historical_key]);
        let active_meta = json!({"name": "reused", "state": "active"});

        assert!(!current_destruction_record_present(
            Some(&active_meta),
            &destruction_keys
        ));

        destruction_keys.insert(current_key.clone());
        let deleting_meta = json!({
            "name": "reused",
            "state": "deleting",
            "destruction_record_key": current_key,
        });
        assert!(current_destruction_record_present(
            Some(&deleting_meta),
            &destruction_keys
        ));
        assert!(current_incarnation_destroyed(None));
    }

    #[test]
    fn i26_scans_current_server_delegated_bearers_without_cross_server_bleed() {
        let left_url = format!("http://delegation-left-{}", uuid::Uuid::new_v4());
        let right_url = format!("http://delegation-right-{}", uuid::Uuid::new_v4());
        let left_token = TokenSel {
            parent: ActorSel(1),
            slot: 1,
        };
        let right_token = TokenSel {
            parent: ActorSel(2),
            slot: 1,
        };
        let left_secret = "zpt1_left.payload.signature".to_string();
        let right_secret = "zpt1_right.payload.signature".to_string();
        install_delegated_token(&left_url, left_token, left_secret.clone());
        install_delegated_token(&right_url, right_token, right_secret.clone());

        let secrets = delegated_token_secrets(&left_url);
        assert_eq!(secrets, vec![left_secret.clone()]);
        assert!(bytes_contain_secret(
            format!("artifact contains {left_secret}").as_bytes(),
            &secrets
        ));
        assert!(!bytes_contain_secret(
            format!("artifact contains {right_secret}").as_bytes(),
            &secrets
        ));
        assert!(!bytes_contain_secret(
            b"artifact contains <redacted>",
            &secrets
        ));

        DELEGATED_TOKEN_BEARERS
            .write()
            .unwrap_or_else(|_| panic!("delegated token registry lock poisoned"))
            .retain(|(server_url, _), _| server_url != &left_url && server_url != &right_url);
    }

    #[test]
    fn object_store_delta_reconstructs_every_metric_by_typed_class() {
        let in_run = ObjectStoreCensus::from([
            (
                ArtifactClass::Manifest,
                ClassStats {
                    get_ops: 3,
                    get_bytes: 30,
                    put_ops: 1,
                    put_bytes: 10,
                },
            ),
            (
                ArtifactClass::Sketch,
                ClassStats {
                    get_ops: 0,
                    get_bytes: 0,
                    put_ops: 2,
                    put_bytes: 200,
                },
            ),
        ]);
        let total = ObjectStoreCensus::from([
            (
                ArtifactClass::Manifest,
                ClassStats {
                    get_ops: 8,
                    get_bytes: 80,
                    put_ops: 3,
                    put_bytes: 30,
                },
            ),
            (
                ArtifactClass::Sketch,
                ClassStats {
                    get_ops: 4,
                    get_bytes: 400,
                    put_ops: 5,
                    put_bytes: 500,
                },
            ),
        ]);

        let quiet_period = object_store_delta(&total, &in_run);

        for (class, total) in total {
            let in_run = in_run[&class];
            let quiet_period = quiet_period[&class];
            assert_eq!(in_run.get_ops + quiet_period.get_ops, total.get_ops);
            assert_eq!(in_run.get_bytes + quiet_period.get_bytes, total.get_bytes);
            assert_eq!(in_run.put_ops + quiet_period.put_ops, total.put_ops);
            assert_eq!(in_run.put_bytes + quiet_period.put_bytes, total.put_bytes);
        }
    }

    #[test]
    #[should_panic(expected = "object-store counter regressed for manifest.get_ops")]
    fn object_store_delta_fails_loudly_when_a_counter_regresses() {
        let in_run = ObjectStoreCensus::from([(
            ArtifactClass::Manifest,
            ClassStats {
                get_ops: 2,
                ..ClassStats::default()
            },
        )]);
        let total = ObjectStoreCensus::from([(
            ArtifactClass::Manifest,
            ClassStats {
                get_ops: 1,
                ..ClassStats::default()
            },
        )]);

        let _ = object_store_delta(&total, &in_run);
    }

    #[test]
    fn mixed_mode_reserves_only_legacy_seed_zero_and_two_as_deterministic() {
        let expected = [
            (RunMode::Deterministic, None),
            (RunMode::Chaos, Some(FaultProfile::LegacyChaos)),
            (RunMode::Deterministic, None),
            (RunMode::Chaos, Some(FaultProfile::PostCommit)),
            (RunMode::Chaos, Some(FaultProfile::Network)),
            (RunMode::Chaos, Some(FaultProfile::Crash)),
            (RunMode::Chaos, Some(FaultProfile::Clock)),
            (RunMode::Chaos, Some(FaultProfile::Sched)),
            (RunMode::Chaos, Some(FaultProfile::SupportedFull)),
        ];
        for (seed, (mode, profile)) in expected.into_iter().enumerate() {
            let seed = u64::try_from(seed).unwrap();
            let assignment = effective_seed_assignment(RunMode::Mixed, None, seed);
            assert_eq!(assignment.mode, mode, "mixed residue {seed}");
            assert_eq!(assignment.profile, profile, "mixed residue {seed}");
            assert_eq!(effective_seed_mode(RunMode::Mixed, seed), mode);
        }
        assert_eq!(
            effective_seed_assignment(RunMode::Mixed, Some(FaultProfile::Content), 0).profile,
            Some(FaultProfile::Content)
        );
    }

    #[test]
    fn crash_schedule_quiescence_clock_outlives_a_stranded_compaction_lease() {
        let scheduler = FaultScheduler::for_seed(149, FaultProfile::Crash);
        let clock = test_clock_for_scheduler(Some(&scheduler))
            .expect("crash schedules need a shared clock for quiescence recovery");
        let config = deterministic_config();
        let before = clock.now();

        let advance_ms = advance_quiescence_clock_past_lease(&clock, &config);

        assert_eq!(
            advance_ms,
            i64::try_from((config.compaction.lease_duration_secs + 1) * 1_000).unwrap()
        );
        assert!(
            clock.now()
                > before
                    + chrono::Duration::seconds(
                        i64::try_from(config.compaction.lease_duration_secs).unwrap(),
                    )
        );
    }

    #[test]
    fn inspection_server_is_explicitly_open_unsafe() {
        assert_eq!(
            inspection_config().security.mode,
            zeppelin::config::SecurityMode::OpenUnsafe
        );
    }

    #[test]
    fn legacy_replay_config_without_security_decodes_with_implicit_admin() {
        let fixture = include_bytes!("fixtures/legacy_pre_security_config.json");
        let dir = tempfile::TempDir::new().unwrap();
        fs::write(dir.path().join("config.json"), fixture).unwrap();

        let replay = replay_seed_config(dir.path());

        assert_eq!(replay.seed, 83);
        assert_eq!(
            replay.config.security.mode,
            zeppelin::config::SecurityMode::Enforced
        );
        assert!(replay.config.security.api_keys.is_empty());
        assert!(replay.security_program.is_none());

        let legacy_op: Op = serde_json::from_value(json!({
            "GetNamespace": { "ns": "legacy" }
        }))
        .unwrap();
        assert_eq!(legacy_op.actor(), ActorSel::ADMIN);
    }

    #[test]
    fn ordinary_execution_uses_the_declared_non_admin_actor() {
        let tenant_read = Op::GetNamespace {
            actor: ActorSel(2),
            ns: "tenant-a".to_string(),
        };
        let admin_read = Op::GetNamespace {
            actor: ActorSel::ADMIN,
            ns: "tenant-a".to_string(),
        };
        let security_probe = Op::SecurityAdminProbe { actor: ActorSel(2) };

        assert_eq!(ordinary_execution_actor(&tenant_read), Some(ActorSel(2)));
        assert_eq!(ordinary_execution_actor(&admin_read), None);
        assert_eq!(ordinary_execution_actor(&security_probe), None);
    }

    #[test]
    fn overnight_seed_rotation_preserves_history_and_reaches_every_mixed_slot() {
        let configured = [0, 1, 2];
        let emitted = (0..9)
            .map(|index| overnight_seed(&configured, index))
            .collect::<Vec<_>>();

        assert_eq!(&emitted[..3], &configured);
        assert_eq!(emitted.iter().copied().collect::<BTreeSet<_>>().len(), 9);
        assert_eq!(
            emitted.iter().map(|seed| seed % 9).collect::<Vec<_>>(),
            (0..9).collect::<Vec<_>>()
        );
        assert_eq!(
            emitted
                .iter()
                .map(|seed| effective_seed_assignment(RunMode::Mixed, None, *seed).profile)
                .collect::<Vec<_>>(),
            vec![
                None,
                Some(FaultProfile::LegacyChaos),
                None,
                Some(FaultProfile::PostCommit),
                Some(FaultProfile::Network),
                Some(FaultProfile::Crash),
                Some(FaultProfile::Clock),
                Some(FaultProfile::Sched),
                Some(FaultProfile::SupportedFull),
            ]
        );
    }

    #[tokio::test]
    async fn injected_manifest_divergence_explains_internal_missing_expected_error() {
        let inner = ZeppelinStore::new(Arc::new(InMemory::new()));
        inner
            .put("ns/manifest.json", bytes::Bytes::from_static(b"manifest"))
            .await
            .unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-head-get-diverge".to_string(),
                start_op: 43,
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
        scheduler.advance_to(43);
        let faulted = store_fault_proxy(&inner, scheduler.clone());
        let timeline_start = scheduler.timeline().len();
        assert!(faulted.get("ns/manifest.json").await.is_err());

        let mut rec = OpRecord {
            index: 43,
            wall_ms: 0,
            op: Op::Query {
                actor: ActorSel::ADMIN,
                ns: "ns".to_string(),
                q: GeneratedQuery {
                    body: json!({}),
                    class: QueryOracleClass::ExpectError {
                        status: 410,
                        code: "POINT_IN_TIME_NOT_RETAINED".to_string(),
                    },
                    pattern_tags: vec!["as-of-410".to_string()],
                },
                as_of: Some(super::super::ops::AsOfTarget::Generation(10_004)),
            },
            method: "POST".to_string(),
            path: "/v1/namespaces/ns/query?as_of=10004".to_string(),
            status: 500,
            response: json!({
                "code": "INTERNAL_DATA_MISSING",
                "error": "an internal data object is missing; this is a server-side error",
                "request_id": "request-id",
                "retryable": false,
                "status": 500,
            }),
            outcome: "not_applied".to_string(),
            target_node: 0,
            execution: ExecutionMetadata::default(),
            gen_after: None,
            duration_ms: 0,
            violations: Vec::new(),
        };

        mark_injected_store_failure(&mut rec, 43, Some(&scheduler), Some(timeline_start));
        assert_eq!(rec.response[STORE_FAULT_MARKER], true);
        assert!(oracle::check_op(&Model::default(), &rec, RunMode::Chaos, None).is_empty());

        rec.response
            .as_object_mut()
            .unwrap()
            .remove(STORE_FAULT_MARKER);
        assert!(
            oracle::check_op(&Model::default(), &rec, RunMode::Chaos, None)
                .iter()
                .any(|violation| violation.id == ViolationId::I11ErrorEnvelope)
        );
    }

    #[tokio::test]
    async fn periodic_s3_oracle_classifies_corrupt_manifest_without_panicking() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let manifest_key = Manifest::s3_key("ns");
        store
            .put(&manifest_key, bytes::Bytes::from_static(b"truncated"))
            .await
            .unwrap();

        let status = periodic_s3_oracle_status(&store, "ns").await;
        assert_eq!(status["manifest_generation"], serde_json::Value::Null);
        assert!(status["manifest_read_error"]
            .as_str()
            .is_some_and(|error| error.contains("manifest msgpack deserialize")));

        let unrelated_taint = BTreeSet::from(["ns/wal/tainted.wal".to_string()]);
        let outside_window = S3Tracker::default()
            .check_namespace_with_fault_context(
                &store,
                "ns",
                225,
                &status,
                false,
                false,
                &unrelated_taint,
            )
            .await;
        assert_eq!(outside_window.len(), 1, "{outside_window:#?}");
        assert_eq!(outside_window[0].id, ViolationId::I15ManifestLineage);
        assert!(outside_window[0].detail.contains("read-failed"));

        let inside_window = S3Tracker::default()
            .check_namespace_with_fault_context(
                &store,
                "ns",
                208,
                &status,
                false,
                true,
                &BTreeSet::new(),
            )
            .await;
        assert!(inside_window.is_empty(), "{inside_window:#?}");

        let mut corruption_tracker = CorruptionTracker::default();
        corruption_tracker.observe(
            &[TimelineEvent {
                event_id: "content-00".to_string(),
                op_index: 208,
                wall_ms: 0,
                boundary: Boundary::ObjectStore,
                action: "Content(TornWrite { keep_bytes: 30 }) call=1".to_string(),
                key: Some(manifest_key),
                semantics: FaultSemantics::PostCommit,
                observed: ObservedResult::Corrupted,
                recovery: None,
            }],
            &["ns".to_string()],
        );
        let durable_manifest_taint = corruption_tracker
            .durably_tainted_keys("ns")
            .expect("torn manifest write must remain durably tainted");
        let explained_durable_corruption = S3Tracker::default()
            .check_namespace_with_fault_context(
                &store,
                "ns",
                225,
                &status,
                false,
                false,
                durable_manifest_taint,
            )
            .await;
        assert!(
            explained_durable_corruption.is_empty(),
            "{explained_durable_corruption:#?}"
        );
    }

    #[tokio::test]
    async fn periodic_lineage_without_http_context_absorbs_exact_durable_manifest_taint() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let namespace = format!("{prefix}-periodic-lineage");
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let config = deterministic_config();
        let server = start_test_server_full(
            harness.store.clone(),
            Some(prefix),
            config.clone(),
            false,
            None,
        )
        .await;
        let client = adversarial_client(&server);
        let (create_status, _) = request_json(
            &client,
            Method::POST,
            &format!("{}/v1/namespaces", server.base_url),
            Some(spec.create_body(&namespace)),
        )
        .await;
        assert_eq!(create_status, StatusCode::CREATED.as_u16());

        let artifact_root = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 1,
            seeds: vec![0],
            max_ops: Some(1),
            artifacts: artifact_root.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Deterministic,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let run_artifacts = RunArtifacts::create(&env);
        let mut artifacts = run_artifacts.seed(
            0,
            &config,
            &BTreeMap::from([(namespace.clone(), spec.clone())]),
            RunMode::Deterministic,
            None,
            None,
            None,
            None,
        );
        let mut model = Model::default();
        model.apply(
            &Op::CreateNamespace {
                actor: ActorSel::ADMIN,
                ns: namespace.clone(),
                spec,
            },
            StatusCode::CREATED.as_u16(),
            Some(1),
            &json!({}),
            None,
        );
        let manifest_key = Manifest::s3_key(&namespace);
        server
            .store
            .put(&manifest_key, bytes::Bytes::from_static(b"truncated"))
            .await
            .unwrap();
        server.manifest_cache.invalidate(&namespace);
        let mut corruption_tracker = CorruptionTracker::default();
        corruption_tracker.observe(
            &[TimelineEvent {
                event_id: "content-00".to_string(),
                op_index: 24,
                wall_ms: 0,
                boundary: Boundary::ObjectStore,
                action: "Content(TornWrite { keep_bytes: 30 }) call=1".to_string(),
                key: Some(manifest_key),
                semantics: FaultSemantics::PostCommit,
                observed: ObservedResult::Corrupted,
                recovery: None,
            }],
            std::slice::from_ref(&namespace),
        );

        let step = execute_recorded_op(
            &client,
            &server,
            &mut artifacts,
            &mut model,
            &mut Coverage::default(),
            &mut S3Tracker::default(),
            &mut corruption_tracker,
            &Op::FetchVectors {
                actor: ActorSel::ADMIN,
                ns: namespace.clone(),
                ids: vec!["missing".to_string()],
                consistency: ConsistencyLevel::Strong,
            },
            25,
            Instant::now(),
            None,
            RunMode::Deterministic,
            ExecutionPhase::Quiescence,
            true,
            0,
            None,
            false,
        )
        .await;

        assert_eq!(step.status, StatusCode::INTERNAL_SERVER_ERROR.as_u16());
        assert!(step.violations.is_empty(), "{:?}", step.violations);
        server.shutdown().await;
        harness.cleanup().await;
    }

    fn replay_trace_record(index: u64, phase: ExecutionPhase, op: Op) -> OpRecord {
        OpRecord {
            index,
            wall_ms: index,
            op,
            method: "GET".to_string(),
            path: "/fixture".to_string(),
            status: StatusCode::OK.as_u16(),
            response: json!({}),
            outcome: "applied".to_string(),
            target_node: 0,
            execution: ExecutionMetadata { phase, hold: None },
            gen_after: None,
            duration_ms: 0,
            violations: Vec::new(),
        }
    }

    fn quiet_failure_manifest(op_index: u64) -> FailureManifest {
        FailureManifest {
            seed: 7,
            mode: RunMode::Chaos,
            op_index,
            violations: vec![Violation {
                id: ViolationId::I16Quiescence,
                op_index,
                namespace: "source-prefix-ns".to_string(),
                detail: "quiet fixture failure".to_string(),
                evidence: json!({}),
            }],
            preserved_prefix: "source-prefix".to_string(),
            fault_plan: None,
            repro_cmd: "fixture replay".to_string(),
            inspect_cmd: "fixture inspect".to_string(),
        }
    }

    fn replay_trace_fixture(prefix: &str, include_sweep: bool) -> Vec<OpRecord> {
        let ns = format!("{prefix}-ns");
        let mut records = vec![
            replay_trace_record(
                0,
                ExecutionPhase::Workload,
                Op::GetNamespace {
                    actor: ActorSel::ADMIN,
                    ns: ns.clone(),
                },
            ),
            replay_trace_record(
                1,
                ExecutionPhase::Quiescence,
                Op::CompactInline {
                    actor: ActorSel::ADMIN,
                    ns: ns.clone(),
                },
            ),
            replay_trace_record(
                2,
                ExecutionPhase::Quiescence,
                Op::GcCycle {
                    actor: ActorSel::ADMIN,
                    ns: ns.clone(),
                    keep_count: 1,
                },
            ),
        ];
        if include_sweep {
            records.push(replay_trace_record(
                3,
                ExecutionPhase::Quiescence,
                Op::FetchVectors {
                    actor: ActorSel::ADMIN,
                    ns,
                    ids: vec![format!("{prefix}-v0")],
                    consistency: ConsistencyLevel::Strong,
                },
            ));
        }
        records
    }

    #[test]
    fn clean_replay_requires_an_exact_completed_quiet_trace() {
        let source = replay_trace_fixture("source-prefix", true);
        let replay = replay_trace_fixture("replay-prefix", true);

        assert_normalized_full_replay_structure(
            &source,
            "source-prefix",
            &replay,
            "replay-prefix",
            None,
            &BTreeSet::new(),
        );

        let mut extended = replay;
        extended.push(replay_trace_record(
            4,
            ExecutionPhase::Quiescence,
            Op::Query {
                actor: ActorSel::ADMIN,
                ns: "replay-prefix-ns".to_string(),
                q: GeneratedQuery {
                    body: json!({}),
                    class: QueryOracleClass::Membership {
                        consistency: ConsistencyLevel::Strong,
                    },
                    pattern_tags: Vec::new(),
                },
                as_of: None,
            },
        ));
        assert!(std::panic::catch_unwind(|| {
            assert_normalized_full_replay_structure(
                &source,
                "source-prefix",
                &extended,
                "replay-prefix",
                None,
                &BTreeSet::new(),
            );
        })
        .is_err());
    }

    #[test]
    fn repaired_quiet_failure_replay_accepts_only_a_longer_exact_prefix() {
        let source = replay_trace_fixture("source-prefix", false);
        let replay = replay_trace_fixture("replay-prefix", true);
        let failure = quiet_failure_manifest(3);

        assert_normalized_full_replay_structure(
            &source,
            "source-prefix",
            &replay,
            "replay-prefix",
            Some(&failure),
            &BTreeSet::new(),
        );
    }

    #[test]
    fn terminal_lifecycle_replay_divergence_requires_typed_disposition_and_omission() {
        let source = vec![
            replay_trace_record(
                0,
                ExecutionPhase::Workload,
                Op::GetNamespace {
                    actor: ActorSel::ADMIN,
                    ns: "source-prefix-ns".to_string(),
                },
            ),
            replay_trace_record(
                1,
                ExecutionPhase::Quiescence,
                Op::CompactInline {
                    actor: ActorSel::ADMIN,
                    ns: "source-prefix-ns".to_string(),
                },
            ),
        ];
        let replay = vec![
            replay_trace_record(
                0,
                ExecutionPhase::Workload,
                Op::GetNamespace {
                    actor: ActorSel::ADMIN,
                    ns: "replay-prefix-ns".to_string(),
                },
            ),
            replay_trace_record(
                1,
                ExecutionPhase::Quiescence,
                Op::GcCycle {
                    actor: ActorSel::ADMIN,
                    ns: "replay-prefix-other".to_string(),
                    keep_count: 1,
                },
            ),
        ];
        let failure = quiet_failure_manifest(1);
        let terminal = BTreeSet::from(["replay-prefix-ns".to_string()]);

        assert_normalized_full_replay_structure(
            &source,
            "source-prefix",
            &replay,
            "replay-prefix",
            Some(&failure),
            &terminal,
        );
        assert!(std::panic::catch_unwind(|| {
            assert_normalized_full_replay_structure(
                &source,
                "source-prefix",
                &replay,
                "replay-prefix",
                Some(&failure),
                &BTreeSet::new(),
            );
        })
        .is_err());

        let mut retained = replay;
        retained.push(replay_trace_record(
            2,
            ExecutionPhase::Quiescence,
            Op::GcCycle {
                actor: ActorSel::ADMIN,
                ns: "replay-prefix-ns".to_string(),
                keep_count: 1,
            },
        ));
        assert!(std::panic::catch_unwind(|| {
            assert_normalized_full_replay_structure(
                &source,
                "source-prefix",
                &retained,
                "replay-prefix",
                Some(&failure),
                &terminal,
            );
        })
        .is_err());
    }

    #[test]
    fn repaired_clone_publication_requires_typed_non_applied_resolution() {
        let root = tempfile::TempDir::new().unwrap();
        fs::write(
            root.path().join("resolutions.json"),
            serde_json::to_vec(&json!([
                {
                    "effect": "maybe_cloned",
                    "resolved": "not_applied",
                    "publication_disposition": "candidate_mismatch",
                    "target": "candidate-mismatch",
                },
                {
                    "effect": "maybe_cloned",
                    "resolved": "not_applied",
                    "publication_disposition": "manifest_absent",
                    "target": "manifest-absent",
                },
                {
                    "effect": "maybe_cloned",
                    "resolved": "applied",
                    "publication_disposition": "candidate_match",
                    "target": "applied",
                },
                {
                    "effect": "maybe_deleted_namespace",
                    "resolved": "applied",
                    "lifecycle_disposition": "deletion_fenced",
                    "target": "different-effect",
                }
            ]))
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            non_applied_clone_resolution_targets(root.path()),
            BTreeSet::from([
                "candidate-mismatch".to_string(),
                "manifest-absent".to_string(),
            ])
        );
    }

    #[test]
    fn terminal_workload_failure_does_not_add_unrecorded_quiet_verification() {
        let source = vec![
            replay_trace_record(
                0,
                ExecutionPhase::Workload,
                Op::GetNamespace {
                    actor: ActorSel::ADMIN,
                    ns: "source-prefix-ns".to_string(),
                },
            ),
            replay_trace_record(
                1,
                ExecutionPhase::Workload,
                Op::GetNamespace {
                    actor: ActorSel::ADMIN,
                    ns: "source-prefix-ns".to_string(),
                },
            ),
        ];
        let failure = quiet_failure_manifest(1);

        assert!(source_failure_precedes_unrecorded_quiet_period(
            &source,
            Some(&failure),
        ));
        assert!(!source_failure_precedes_unrecorded_quiet_period(
            &source, None,
        ));

        let quiet_source = replay_trace_fixture("source-prefix", false);
        let quiet_failure = quiet_failure_manifest(3);
        assert!(!source_failure_precedes_unrecorded_quiet_period(
            &quiet_source,
            Some(&quiet_failure),
        ));
    }

    #[test]
    fn overnight_seed_rotation_skips_arbitrary_configured_seeds_without_collisions() {
        let configured = [u64::MAX, 17, 0, 12, 5];
        let emitted = (0..64)
            .map(|index| overnight_seed(&configured, index))
            .collect::<Vec<_>>();

        assert_eq!(&emitted[..configured.len()], &configured);
        assert_eq!(
            emitted.iter().copied().collect::<BTreeSet<_>>().len(),
            emitted.len()
        );
        assert!(emitted[configured.len()..]
            .iter()
            .all(|seed| !configured.contains(seed)));
        assert_eq!(
            emitted.iter().map(|seed| seed % 9).collect::<BTreeSet<_>>(),
            (0..9).collect()
        );
    }

    #[test]
    fn overnight_emitted_seed_is_the_artifact_and_replay_identity() {
        let root = tempfile::TempDir::new().unwrap();
        let configured = [0, 1, 2];
        let emitted_seed = overnight_seed(&configured, 11);
        let env = RunnerEnv {
            seconds: 1,
            seeds: configured.to_vec(),
            max_ops: Some(1),
            artifacts: root.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Mixed,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let run_artifacts = RunArtifacts::create(&env);
        let seed_artifacts = run_artifacts.seed(
            emitted_seed,
            &deterministic_config(),
            &BTreeMap::new(),
            RunMode::Chaos,
            None,
            None,
            None,
            None,
        );
        drop(seed_artifacts);

        let seed_dir = run_artifacts.root().join(format!("seed-{emitted_seed}"));
        assert!(seed_dir.exists());
        assert_eq!(replay_seed_config(&seed_dir).seed, emitted_seed);
    }

    #[test]
    fn reproduction_environment_includes_the_active_profile() {
        assert_eq!(
            reproduction_environment("memory", None, Some(FaultProfile::Semantic)),
            "TEST_BACKEND=memory ZEPPELIN_ADVERSARIAL_PROFILE=semantic"
        );
        assert_eq!(
            reproduction_environment(
                "minio",
                Some(OracleMutation::DualWriterFencing),
                Some(FaultProfile::Ops),
            ),
            "TEST_BACKEND=minio ZEPPELIN_ADVERSARIAL_SELFTEST=dual-writer-fencing \
             ZEPPELIN_ADVERSARIAL_PROFILE=ops"
        );
    }

    #[tokio::test]
    async fn deterministic_seed_uses_the_canonical_quiet_period_order() {
        let root = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 30,
            seeds: vec![0],
            max_ops: Some(4),
            artifacts: root.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Deterministic,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let artifacts = RunArtifacts::create(&env);
        let seed_dir = artifacts.root().join("seed-0");
        let outcome = Box::pin(run_seed(
            &env,
            &artifacts,
            0,
            Instant::now() + Duration::from_secs(30),
            None,
            None,
        ))
        .await;
        assert!(!outcome.failed, "{:?}", outcome.violations);

        let timeline = fs::read_to_string(seed_dir.join("timeline.jsonl")).unwrap();
        let actions = timeline
            .lines()
            .map(|line| serde_json::from_str::<TimelineEvent>(line).unwrap().action)
            .filter(|action| action.starts_with("quiet:"))
            .collect::<Vec<_>>();
        assert_eq!(
            actions,
            [
                "quiet:scheduler-quiesce",
                "quiet:restore-network",
                "quiet:release-held",
                "quiet:stop-second-node",
                "quiet:primary-health",
                "quiet:security-refresh",
                "quiet:stop-background",
                "quiet:resolve-indeterminates",
                "quiet:force-compaction",
                "quiet:gc-twice",
                "quiet:s3-oracles",
                "quiet:exhaustive-sweep",
            ]
        );
        assert!(seed_dir.join("timeline.jsonl").exists());
        assert!(!seed_dir.join("faults.jsonl").exists());
    }

    #[tokio::test]
    async fn legacy_chaos_resolves_failed_initial_manifest_before_quiescence() {
        let root = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 30,
            seeds: vec![49],
            max_ops: Some(30),
            artifacts: root.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Chaos,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let artifacts = RunArtifacts::create(&env);
        let seed_dir = artifacts.root().join("seed-49");
        let outcome = Box::pin(run_seed(
            &env,
            &artifacts,
            49,
            Instant::now() + Duration::from_secs(30),
            None,
            None,
        ))
        .await;
        assert!(!outcome.failed, "{:?}", outcome.violations);

        let records = read_ops(&seed_dir);
        let probe = records
            .iter()
            .find(|record| record.index == 29)
            .expect("seed 49 must reach the write-shaped invalid probe");
        assert!(matches!(
            probe.op,
            Op::InvalidProbe {
                probe: InvalidProbe::WrongDims,
                ..
            }
        ));
        assert_eq!(probe.status, StatusCode::BAD_REQUEST.as_u16());
        let recovered = json!({
            "namespace": probe.op.namespace(),
            "manifest_generation": 4,
            "ready": false,
            "uncompacted_fragments": 3,
            "segment_count": 0,
            "active_segment": null,
            "active_segment_vector_count": 0,
        });
        assert_eq!(probe.response["compact_status_before"], recovered);
        assert_eq!(probe.response["compact_status_after"], recovered);

        let resolutions = fs::read_to_string(seed_dir.join("resolutions.json")).unwrap();
        let resolutions: Vec<serde_json::Value> = serde_json::from_str(&resolutions).unwrap();
        assert!(
            resolutions.iter().any(|resolution| {
                resolution["effect"] == "maybe_created_namespace"
                    && resolution["resolved"] == "applied"
            }),
            "{resolutions:#?}"
        );
    }

    // F1 (seed 113): an ambiguous `DeleteNamespace` can leave metadata active
    // after the manifest destruction fence commits, or metadata deleting
    // before the live manifest is removed. Both are terminal quiet-period
    // states even though `manifest.json` exists. An active but unfenced intent
    // remains live. The barriers below pin every state without sleeps.
    #[tokio::test]
    async fn ambiguous_delete_resolution_uses_authoritative_lifecycle_state() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let ns_fenced = format!("{prefix}-f1-fenced");
        let ns_deleting = format!("{prefix}-f1-deleting");
        let ns_unfenced = format!("{prefix}-f1-unfenced");
        let ns_live = format!("{prefix}-f1-live");
        let ns_gone = format!("{prefix}-f1-gone");

        let setup_server = start_test_server_full(
            harness.store.clone(),
            Some(prefix.clone()),
            deterministic_config(),
            false,
            None,
        )
        .await;
        let admin_bearer = setup_server.admin_bearer.clone();
        let client = adversarial_client(&setup_server);
        for ns in [&ns_fenced, &ns_deleting, &ns_unfenced, &ns_live] {
            let create = client
                .post(format!("{}/v1/namespaces", setup_server.base_url))
                .json(&spec.create_body(ns))
                .send()
                .await
                .unwrap();
            assert_eq!(create.status(), StatusCode::CREATED);
            let upsert = client
                .post(format!(
                    "{}/v1/namespaces/{ns}/vectors",
                    setup_server.base_url
                ))
                .json(&json!({
                    "vectors": [{ "id": "one", "values": [1.0, 0.0] }]
                }))
                .send()
                .await
                .unwrap();
            assert_eq!(upsert.status(), StatusCode::OK);
        }
        setup_server.shutdown().await;

        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Network,
            events: vec![
                // Exact seed-113 crash shape: the manifest fence commits, but
                // the request fails before metadata records the generation.
                FaultEvent {
                    id: "f1-manifest-fence-post-commit".to_string(),
                    start_op: 0,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector {
                        store_op: Some(StoreOp::Put),
                        key_substring: Some(format!("{ns_fenced}/manifest.json")),
                        ..TargetSelector::default()
                    },
                    kind: FaultKind::PostCommitFail {
                        error: InjectedErrorKind::Generic,
                    },
                },
                // Later governed-deletion state: the tombstone commits, but
                // manifest removal fails.
                FaultEvent {
                    id: "f1-manifest-delete-pre-fail".to_string(),
                    start_op: 0,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector {
                        store_op: Some(StoreOp::Delete),
                        key_substring: Some(format!("{ns_deleting}/manifest.json")),
                        ..TargetSelector::default()
                    },
                    kind: FaultKind::PreFail {
                        error: InjectedErrorKind::Generic,
                    },
                },
                // A deletion intent without a committed manifest fence remains
                // active and must not be classified terminal.
                FaultEvent {
                    id: "f1-lease-pre-fail".to_string(),
                    start_op: 0,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector {
                        store_op: Some(StoreOp::Put),
                        key_substring: Some(format!("{ns_unfenced}/lease.json")),
                        ..TargetSelector::default()
                    },
                    kind: FaultKind::PreFail {
                        error: InjectedErrorKind::Generic,
                    },
                },
            ],
        });
        let faulted_store = store_fault_proxy(&harness.store, scheduler.clone());
        let server = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
            faulted_store,
            Some(prefix),
            deterministic_config(),
            false,
            None,
            100 * 1024 * 1024,
            &admin_bearer,
        )
        .await;
        let client = adversarial_client(&server);
        for ns in [&ns_fenced, &ns_deleting, &ns_unfenced] {
            let delete = client
                .delete(format!("{}/v1/namespaces/{ns}", server.base_url))
                .send()
                .await
                .unwrap();
            let status = delete.status().as_u16();
            let body = delete.text().await.unwrap_or_default();
            assert!(
                (500..600).contains(&status),
                "the interrupted delete for {ns} must fail ambiguously, got {status}: {body}"
            );
        }
        let fired = scheduler
            .timeline()
            .into_iter()
            .map(|event| event.event_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            fired,
            BTreeSet::from([
                "f1-lease-pre-fail".to_string(),
                "f1-manifest-delete-pre-fail".to_string(),
                "f1-manifest-fence-post-commit".to_string(),
            ])
        );

        let fenced_meta = NamespaceMetadata::from_bytes(
            &harness
                .store
                .get(&format!("{ns_fenced}/meta.json"))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(fenced_meta.state, NamespaceState::Active);
        assert!(fenced_meta.deletion_intent.is_some());
        let fenced_manifest = Manifest::read(&harness.store, &ns_fenced)
            .await
            .unwrap()
            .expect("post-commit fault must retain the fenced manifest");
        assert!(fenced_manifest.is_deletion_fenced());

        let deleting_meta = NamespaceMetadata::from_bytes(
            &harness
                .store
                .get(&format!("{ns_deleting}/meta.json"))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(deleting_meta.state, NamespaceState::Deleting);
        assert!(harness
            .store
            .exists(&format!("{ns_deleting}/manifest.json"))
            .await
            .unwrap());

        let unfenced_meta = NamespaceMetadata::from_bytes(
            &harness
                .store
                .get(&format!("{ns_unfenced}/meta.json"))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(unfenced_meta.state, NamespaceState::Active);
        assert!(unfenced_meta.deletion_intent.is_some());
        let unfenced_manifest = Manifest::read(&harness.store, &ns_unfenced)
            .await
            .unwrap()
            .expect("lease failure must retain the live manifest");
        assert!(!unfenced_manifest.is_deletion_fenced());

        let mut model = Model::default();
        for ns in [&ns_fenced, &ns_deleting, &ns_unfenced, &ns_live, &ns_gone] {
            let mut ns_model = NsModel::new(spec.clone(), 1);
            ns_model
                .indeterminate_ns
                .push(NsIndeterminate::MaybeDeletedNs);
            model.namespaces.insert(ns.clone(), ns_model);
        }
        let root = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 30,
            seeds: vec![0],
            max_ops: None,
            artifacts: root.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Chaos,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let run_artifacts = RunArtifacts::create(&env);
        let seed_dir = run_artifacts.root().join("seed-0");
        let artifacts = run_artifacts.seed(
            0,
            &deterministic_config(),
            &BTreeMap::new(),
            RunMode::Chaos,
            None,
            None,
            None,
            None,
        );
        let violations = resolve_indeterminates(
            &client,
            &server,
            &mut model,
            &artifacts,
            &CorruptionTracker::default(),
            None,
            0,
        )
        .await;
        assert!(violations.is_empty(), "{violations:#?}");

        assert!(
            !model.namespaces.contains_key(&ns_fenced),
            "a durably fenced namespace must leave the live model set"
        );
        assert!(
            !model.namespaces.contains_key(&ns_deleting),
            "a durably deleting namespace must leave the live model set"
        );
        assert!(
            !model.namespaces.contains_key(&ns_gone),
            "a fully tombstoned namespace must leave the live model set"
        );
        assert!(
            model.namespaces.contains_key(&ns_unfenced),
            "an active namespace with no durable fence must stay live"
        );
        assert!(
            model.namespaces.contains_key(&ns_live),
            "a namespace whose delete intent never landed must stay live"
        );

        let resolutions: Vec<serde_json::Value> =
            serde_json::from_str(&fs::read_to_string(seed_dir.join("resolutions.json")).unwrap())
                .unwrap();
        let disposition = |ns: &str| {
            resolutions
                .iter()
                .find(|resolution| resolution["namespace"] == ns)
                .cloned()
        };
        let fenced = disposition(&ns_fenced).expect("missing fenced resolution");
        assert_eq!(fenced["effect"], "maybe_deleted_namespace");
        assert_eq!(fenced["resolved"], "applied");
        assert_eq!(fenced["lifecycle_disposition"], "deletion_fenced");
        let deleting = disposition(&ns_deleting).expect("missing deleting resolution");
        assert_eq!(deleting["effect"], "maybe_deleted_namespace");
        assert_eq!(deleting["resolved"], "applied");
        assert_eq!(deleting["lifecycle_disposition"], "deleting");
        let gone = disposition(&ns_gone).expect("missing tombstoned resolution");
        assert_eq!(gone["resolved"], "applied");
        assert_eq!(gone["lifecycle_disposition"], "absent");
        let unfenced = disposition(&ns_unfenced).expect("missing unfenced resolution");
        assert_eq!(unfenced["resolved"], "not_applied");
        assert_eq!(unfenced["lifecycle_disposition"], "active_unfenced");
        let live = disposition(&ns_live).expect("missing live resolution");
        assert_eq!(live["resolved"], "not_applied");
        assert_eq!(live["lifecycle_disposition"], "active");

        // The product fence is unchanged: compaction against either terminal
        // lifecycle shape still fails loudly instead of being swallowed.
        for ns in [&ns_fenced, &ns_deleting] {
            let fence = server.compactor.compact(ns).await;
            assert!(
                matches!(fence, Err(ZeppelinError::NamespaceDeleting { .. })),
                "compaction must still reject terminal namespace {ns}: {fence:?}"
            );
        }

        server.shutdown().await;
        harness.cleanup().await;
    }

    // F5 (seed 1475): an ambiguous copy-clone can reserve an empty target
    // manifest and then fail before publishing the source view. A later
    // ambiguous delete and idempotent create leave that empty target active.
    // Quiet resolution must prove the candidate contents rather than treating
    // the bootstrap manifest's existence as proof that all source rows landed.
    #[tokio::test]
    async fn ambiguous_clone_resolution_requires_published_candidate_contents() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let source = format!("{prefix}-f5-source");
        let target = format!("{prefix}-f5-target");
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let vectors = (0..8)
            .map(|index| GenVector {
                id: format!("{source}-v{index}"),
                values: vec![index as f32, 1.0],
                attributes: None,
            })
            .collect::<Vec<_>>();

        let setup_server = start_test_server_full(
            harness.store.clone(),
            Some(prefix.clone()),
            deterministic_config(),
            false,
            None,
        )
        .await;
        let admin_bearer = setup_server.admin_bearer.clone();
        let setup_client = adversarial_client(&setup_server);
        let create = setup_client
            .post(format!("{}/v1/namespaces", setup_server.base_url))
            .json(&spec.create_body(&source))
            .send()
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);
        let upsert = setup_client
            .post(format!(
                "{}/v1/namespaces/{source}/vectors",
                setup_server.base_url
            ))
            .json(&json!({ "vectors": vectors }))
            .send()
            .await
            .unwrap();
        assert_eq!(upsert.status(), StatusCode::OK);
        let compacted = setup_server.compactor.compact(&source).await.unwrap();
        assert_eq!(compacted.vectors_compacted, 8);
        let source_manifest = Manifest::read(&harness.store, &source)
            .await
            .unwrap()
            .expect("compacted source manifest must exist");
        let source_generation = source_manifest.version();
        assert_eq!(source_manifest.vector_count(), 8);
        setup_server.shutdown().await;

        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Network,
            events: vec![
                FaultEvent {
                    id: "f5-clone-copy-pre-fail".to_string(),
                    start_op: 0,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector {
                        store_op: Some(StoreOp::Copy),
                        key_substring: Some(format!("->{target}/")),
                        ..TargetSelector::default()
                    },
                    kind: FaultKind::PreFail {
                        error: InjectedErrorKind::Generic,
                    },
                },
                FaultEvent {
                    id: "f5-delete-lease-pre-fail".to_string(),
                    start_op: 0,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector {
                        store_op: Some(StoreOp::Put),
                        key_substring: Some(format!("{target}/lease.json")),
                        ..TargetSelector::default()
                    },
                    kind: FaultKind::PreFail {
                        error: InjectedErrorKind::Generic,
                    },
                },
                FaultEvent {
                    id: "f5-target-wal-pre-fail".to_string(),
                    start_op: 0,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector {
                        store_op: Some(StoreOp::Put),
                        key_substring: Some(format!("{target}/wal/")),
                        ..TargetSelector::default()
                    },
                    kind: FaultKind::PreFail {
                        error: InjectedErrorKind::Generic,
                    },
                },
            ],
        });
        let server = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
            store_fault_proxy(&harness.store, scheduler.clone()),
            Some(prefix),
            deterministic_config(),
            false,
            None,
            100 * 1024 * 1024,
            &admin_bearer,
        )
        .await;
        let client = adversarial_client(&server);

        let clone = client
            .post(format!("{}/v1/namespaces/{source}/clone", server.base_url))
            .json(&json!({
                "target": target,
                "as_of": source_generation.to_string(),
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(clone.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let empty_target = Manifest::read(&harness.store, &target)
            .await
            .unwrap()
            .expect("failed clone must retain its reserved target manifest");
        assert_eq!(empty_target.vector_count(), 0);

        let delete = client
            .delete(format!("{}/v1/namespaces/{target}", server.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(delete.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let target_meta = NamespaceMetadata::from_bytes(
            &harness
                .store
                .get(&format!("{target}/meta.json"))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(target_meta.state, NamespaceState::Active);
        assert!(target_meta.deletion_intent.is_some());

        let recreate = client
            .post(format!("{}/v1/namespaces", server.base_url))
            .json(&spec.create_body(&target))
            .send()
            .await
            .unwrap();
        assert_eq!(recreate.status(), StatusCode::OK);
        let rejected = client
            .post(format!(
                "{}/v1/namespaces/{target}/vectors",
                server.base_url
            ))
            .json(&json!({ "vectors": vectors }))
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let compacted = server.compactor.compact(&target).await.unwrap();
        assert_eq!(compacted.vectors_compacted, 0);
        assert!(compacted.segment_id.is_none());

        let fetch = client
            .post(format!(
                "{}/v1/namespaces/{target}/vectors/get",
                server.base_url
            ))
            .json(&json!({
                "ids": vectors.iter().map(|vector| &vector.id).collect::<Vec<_>>(),
                "include_vector": true,
                "include_attributes": true,
                "consistency": ConsistencyLevel::Strong,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(fetch.status(), StatusCode::OK);
        let fetch: serde_json::Value = fetch.json().await.unwrap();
        assert_eq!(
            fetch["missing"].as_array().map(Vec::len),
            Some(vectors.len())
        );
        assert_eq!(
            Manifest::read(&harness.store, &target)
                .await
                .unwrap()
                .expect("target manifest must remain present")
                .vector_count(),
            0
        );

        let fired = scheduler
            .timeline()
            .into_iter()
            .map(|event| event.event_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            fired,
            BTreeSet::from([
                "f5-clone-copy-pre-fail".to_string(),
                "f5-delete-lease-pre-fail".to_string(),
                "f5-target-wal-pre-fail".to_string(),
            ])
        );

        let mut model = Model::default();
        model.apply(
            &Op::CreateNamespace {
                actor: ActorSel::ADMIN,
                ns: source.clone(),
                spec: spec.clone(),
            },
            StatusCode::CREATED.as_u16(),
            Some(1),
            &json!({}),
            None,
        );
        model.apply(
            &Op::Upsert {
                actor: ActorSel::ADMIN,
                ns: source.clone(),
                vectors: vectors.clone(),
            },
            StatusCode::OK.as_u16(),
            Some(source_generation),
            &json!({}),
            None,
        );
        model.apply(
            &Op::CompactInline {
                actor: ActorSel::ADMIN,
                ns: source.clone(),
            },
            StatusCode::OK.as_u16(),
            Some(source_generation),
            &json!({}),
            None,
        );
        model.apply_outcome(
            &Op::CloneNamespace {
                actor: ActorSel::ADMIN,
                source: source.clone(),
                target: target.clone(),
                as_of: AsOfTarget::Generation(source_generation),
            },
            &OpOutcome::Ambiguous {
                reason: AmbiguityReason::ServerError { status: 500 },
                status: Some(500),
            },
            None,
            None,
            51,
        );
        model.apply_outcome(
            &Op::DeleteNamespace {
                actor: ActorSel::ADMIN,
                ns: target.clone(),
            },
            &OpOutcome::Ambiguous {
                reason: AmbiguityReason::ServerCrashed,
                status: None,
            },
            None,
            None,
            57,
        );
        model.apply(
            &Op::CreateNamespace {
                actor: ActorSel::ADMIN,
                ns: target.clone(),
                spec: spec.clone(),
            },
            StatusCode::OK.as_u16(),
            Some(empty_target.version()),
            &json!({}),
            None,
        );
        model.apply_outcome(
            &Op::Upsert {
                actor: ActorSel::ADMIN,
                ns: target.clone(),
                vectors: vectors.clone(),
            },
            &OpOutcome::Ambiguous {
                reason: AmbiguityReason::ServerError { status: 500 },
                status: Some(500),
            },
            None,
            None,
            67,
        );
        model.apply(
            &Op::CompactInline {
                actor: ActorSel::ADMIN,
                ns: target.clone(),
            },
            StatusCode::OK.as_u16(),
            Some(empty_target.version()),
            &json!({}),
            None,
        );

        let root = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 30,
            seeds: vec![1475],
            max_ops: None,
            artifacts: root.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Chaos,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let run_artifacts = RunArtifacts::create(&env);
        let seed_dir = run_artifacts.root().join("seed-1475");
        let artifacts = run_artifacts.seed(
            1475,
            &deterministic_config(),
            &BTreeMap::new(),
            RunMode::Chaos,
            None,
            None,
            None,
            None,
        );
        let violations = resolve_indeterminates(
            &client,
            &server,
            &mut model,
            &artifacts,
            &CorruptionTracker::default(),
            None,
            515,
        )
        .await;
        assert!(violations.is_empty(), "{violations:#?}");
        assert!(model.namespaces[&target].live.is_empty());
        assert!(model.namespaces[&target].indeterminate.is_empty());

        let resolutions: Vec<serde_json::Value> =
            serde_json::from_str(&fs::read_to_string(seed_dir.join("resolutions.json")).unwrap())
                .unwrap();
        let clone_resolution = resolutions
            .iter()
            .find(|resolution| resolution["effect"] == "maybe_cloned")
            .expect("missing ambiguous clone resolution");
        assert_eq!(clone_resolution["resolved"], "not_applied");
        assert_eq!(
            clone_resolution["publication_disposition"],
            "candidate_mismatch"
        );
        assert_eq!(clone_resolution["manifest_vector_count"], 0);
        assert_eq!(clone_resolution["expected_vector_count"], 8);

        server.shutdown().await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn acknowledged_torn_manifest_put_fails_reads_until_memo_cas_repairs_it() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let namespace = format!("{prefix}-acknowledged-torn-manifest");
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let setup_server = start_test_server_full(
            harness.store.clone(),
            Some(prefix.clone()),
            deterministic_config(),
            false,
            None,
        )
        .await;
        let admin_bearer = setup_server.admin_bearer.clone();
        let client = adversarial_client(&setup_server);
        let create = client
            .post(format!("{}/v1/namespaces", setup_server.base_url))
            .json(&spec.create_body(&namespace))
            .send()
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);
        setup_server.shutdown().await;

        let manifest_key = Manifest::s3_key(&namespace);
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Content,
            events: vec![FaultEvent {
                id: "focused-torn-manifest".to_string(),
                start_op: 1,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some(manifest_key.clone()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::Content(ContentFault::TornWrite { keep_bytes: 30 }),
            }],
        });
        let faulted_store = store_fault_proxy(&harness.store, scheduler.clone());
        let server = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
            faulted_store,
            Some(prefix),
            deterministic_config(),
            false,
            None,
            100 * 1024 * 1024,
            &admin_bearer,
        )
        .await;
        assert!(scheduler.advance_to(1).is_empty());

        let upsert = client
            .post(format!(
                "{}/v1/namespaces/{namespace}/vectors",
                server.base_url
            ))
            .json(&json!({
                "vectors": [{ "id": "one", "values": [1.0, 0.0] }]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(upsert.status(), StatusCode::OK);
        assert_eq!(
            tainted_aware_authoritative_generation(
                &harness.store,
                &scheduler,
                &namespace,
                "upsert"
            )
            .await,
            None
        );
        assert!(Manifest::read(&harness.store, &namespace).await.is_err());

        let get = client
            .get(format!("{}/v1/namespaces/{namespace}", server.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(get.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let timeline = scheduler.timeline();
        assert_eq!(timeline.len(), 1, "{timeline:#?}");
        assert_eq!(timeline[0].key.as_deref(), Some(manifest_key.as_str()));
        assert_eq!(timeline[0].semantics, FaultSemantics::PostCommit);
        assert_eq!(timeline[0].observed, ObservedResult::Corrupted);

        // TornWrite violates provider assumption A1: the backend acknowledged
        // a different body than the caller supplied. The same writer may still
        // repair that object safely: its returned ETag identifies the corrupt
        // live object, while its memo retains the fully published history
        // candidate. A conditional replacement must preserve both acknowledged
        // fragments; reads before that replacement remain fail-loud.
        let repair = client
            .post(format!(
                "{}/v1/namespaces/{namespace}/vectors",
                server.base_url
            ))
            .json(&json!({
                "vectors": [{ "id": "two", "values": [0.0, 1.0] }]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(repair.status(), StatusCode::OK);

        let repaired = Manifest::read(&harness.store, &namespace)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(repaired.fragments.len(), 2);
        let visible_ids = WalReader::new(harness.store.clone())
            .read_uncompacted_fragments(&namespace)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|fragment| fragment.vectors.into_iter().map(|vector| vector.id))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            visible_ids,
            BTreeSet::from(["one".to_string(), "two".to_string()])
        );
        let repaired_get = client
            .get(format!("{}/v1/namespaces/{namespace}", server.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(repaired_get.status(), StatusCode::OK);
        assert_eq!(scheduler.timeline().len(), 1);

        server.shutdown().await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn content_seed_127_classifies_durable_torn_manifest_loudly() {
        let root = tempfile::TempDir::new().unwrap();
        // The default mixed rotation no longer selects the legacy content
        // profile; pin it explicitly to keep this seed's recorded timeline.
        let env = RunnerEnv {
            seconds: 60,
            seeds: vec![127],
            max_ops: Some(520),
            artifacts: root.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Mixed,
            profile: Some(FaultProfile::Content),
            env_echo: BTreeMap::new(),
        };
        let artifacts = RunArtifacts::create(&env);
        let seed_dir = artifacts.root().join("seed-127");
        let outcome = Box::pin(run_seed(
            &env,
            &artifacts,
            127,
            Instant::now() + Duration::from_secs(60),
            None,
            None,
        ))
        .await;
        assert!(!outcome.failed, "{:?}", outcome.violations);
        assert!(
            outcome.ops >= 520,
            "seed 127 stopped at {} ops",
            outcome.ops
        );

        // Attribute the durable corruption to the exact persisted timeline
        // event. On slower targets the background compactor can consume this
        // store-fault window concurrently with the foreground operation, so
        // a hard-coded operation record does not prove ownership of the PUT.
        let timeline = fs::read_to_string(seed_dir.join("timeline.jsonl")).unwrap();
        let torn = timeline
            .lines()
            .map(|line| serde_json::from_str::<TimelineEvent>(line).unwrap())
            .find(|event| {
                event.action.starts_with("Content(TornWrite")
                    && event
                        .key
                        .as_deref()
                        .is_some_and(|key| key.ends_with("/manifest.json"))
            })
            .expect("seed 127 must persist one torn live manifest write");
        assert_eq!(torn.semantics, FaultSemantics::PostCommit);
        assert_eq!(torn.observed, ObservedResult::Corrupted);
        let ns = torn
            .key
            .as_deref()
            .and_then(|key| key.strip_suffix("/manifest.json"))
            .expect("torn live-manifest key must contain its namespace")
            .to_string();

        // Read-time integrity stays authoritative until a writer holding the
        // exact acknowledged candidate repairs the provider-abuse write with
        // an ETag-guarded CAS. Before that repair every data-path operation
        // fails loudly; the repair itself must be a successful mutation, after
        // which reads may succeed again.
        let records = read_ops(&seed_dir);
        let later: Vec<_> = records
            .iter()
            .filter(|record| {
                record.index > torn.op_index
                    && record.op.namespace() == ns
                    && matches!(
                        record.op,
                        Op::Upsert { .. }
                            | Op::DeleteVectors { .. }
                            | Op::Query { .. }
                            | Op::FetchVectors { .. }
                            | Op::GetNamespace { .. }
                    )
            })
            .collect();
        assert!(
            later.len() >= 20,
            "seed 127 must keep exercising the corrupted namespace, saw {}",
            later.len()
        );
        let repair_index = later
            .iter()
            .position(|record| record.status < 500)
            .expect("the same writer must eventually repair the torn manifest");
        for record in &later[..repair_index] {
            assert!(
                record.status >= 500,
                "op {} ({}) before manifest repair must fail loudly, got {}",
                record.index,
                record.op.kind(),
                record.status
            );
        }
        let repair = later[repair_index];
        assert!(
            matches!(repair.op, Op::Upsert { .. } | Op::DeleteVectors { .. }),
            "only a manifest mutation may repair the torn live object: {repair:?}"
        );
        assert!(
            (200..300).contains(&repair.status),
            "manifest repair must complete successfully: {repair:?}"
        );
        assert!(
            later[repair_index + 1..].iter().any(|record| {
                (200..300).contains(&record.status)
                    && matches!(
                        record.op,
                        Op::Query { .. } | Op::FetchVectors { .. } | Op::GetNamespace { .. }
                    )
            }),
            "seed 127 must prove that a read succeeds after the memo CAS repair"
        );
    }

    #[tokio::test]
    async fn quiet_restore_deletes_only_a_fired_misdirected_write_artifact() {
        let harness = TestHarness::new().await;
        let original = harness.key("ns/wal/fired.wal");
        let redirected = format!("{original}.misdirected");
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Content,
            events: vec![FaultEvent {
                id: "content-fired-misdirected".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some(original.clone()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::Content(super::super::faults::ContentFault::MisdirectedWrite),
            }],
        });
        let faulted = store_fault_proxy(&harness.store, scheduler.clone());

        faulted
            .put(&original, bytes::Bytes::from_static(b"wal"))
            .await
            .unwrap();
        assert!(!harness.store.exists(&original).await.unwrap());
        assert!(harness.store.exists(&redirected).await.unwrap());
        scheduler.quiesce();

        let recovery =
            restore_misdirected_write_artifacts(&harness.store, Some(&scheduler), None).await;

        assert!(!harness.store.exists(&original).await.unwrap());
        assert!(!harness.store.exists(&redirected).await.unwrap());
        assert_eq!(
            recovery,
            format!(
                "misdirected_artifacts_proven=1; deleted=1; already_absent=0; \
                 verified_absent=1; keys=[{redirected}]"
            )
        );
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn quiet_restore_preserves_an_unproven_misdirected_suffix_object() {
        let harness = TestHarness::new().await;
        let arbitrary = harness.key("ns/wal/arbitrary.wal.misdirected");
        harness
            .store
            .put(&arbitrary, bytes::Bytes::from_static(b"stray"))
            .await
            .unwrap();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Content,
            events: Vec::new(),
        });
        scheduler.quiesce();

        let recovery =
            restore_misdirected_write_artifacts(&harness.store, Some(&scheduler), None).await;

        assert!(harness.store.exists(&arbitrary).await.unwrap());
        assert_eq!(
            recovery,
            "misdirected_artifacts_proven=0; deleted=0; already_absent=0; \
             verified_absent=0; keys=[]"
        );
        harness.cleanup().await;
    }

    #[test]
    fn replay_op_rewrite_updates_all_prefix_bearing_strings() {
        let old_prefix = "source-prefix";
        let new_prefix = "replay-prefix";

        let upsert = rewrite_replayed_op(
            &Op::Upsert {
                actor: ActorSel::ADMIN,
                ns: "source-prefix-exact".to_string(),
                vectors: vec![GenVector {
                    id: "source-prefix-exact-v1".to_string(),
                    values: vec![1.0, 0.0],
                    attributes: None,
                }],
            },
            old_prefix,
            new_prefix,
        );
        let Op::Upsert { ns, vectors, .. } = upsert else {
            panic!("rewritten upsert changed operation kind")
        };
        assert_eq!(ns, "replay-prefix-exact");
        assert_eq!(vectors[0].id, "replay-prefix-exact-v1");

        let query = rewrite_replayed_op(
            &Op::Query {
                actor: ActorSel::ADMIN,
                ns: "source-prefix-exact".to_string(),
                q: GeneratedQuery {
                    body: json!({
                        "filter": {
                            "$or": [
                                { "id": "source-prefix-exact-v1" },
                                { "id": "unrelated-id" }
                            ]
                        },
                        "cursor": "source-prefix-cursor"
                    }),
                    class: QueryOracleClass::ExpectError {
                        status: 400,
                        code: "source-prefix-error".to_string(),
                    },
                    pattern_tags: vec![
                        "source-prefix-pattern".to_string(),
                        "stable-pattern".to_string(),
                    ],
                },
                as_of: None,
            },
            old_prefix,
            new_prefix,
        );
        let Op::Query { ns, q, .. } = query else {
            panic!("rewritten query changed operation kind")
        };
        assert_eq!(ns, "replay-prefix-exact");
        assert_eq!(
            q.body,
            json!({
                "filter": {
                    "$or": [
                        { "id": "replay-prefix-exact-v1" },
                        { "id": "unrelated-id" }
                    ]
                },
                "cursor": "replay-prefix-cursor"
            })
        );
        assert!(matches!(
            q.class,
            QueryOracleClass::ExpectError { ref code, .. } if code == "replay-prefix-error"
        ));
        assert_eq!(q.pattern_tags, ["replay-prefix-pattern", "stable-pattern"]);
    }

    #[test]
    fn foreground_hold_call_footprints_are_deliberate_and_bounded() {
        assert_eq!(
            foreground_hold_calls(&Op::DeleteVectors {
                actor: ActorSel::ADMIN,
                ns: "ns".to_string(),
                ids: vec!["one".to_string()],
            }),
            vec![(StoreOp::Get, "ns/manifest.json".to_string())]
        );
        assert_eq!(
            foreground_hold_calls(&Op::Hydrate {
                actor: ActorSel::ADMIN,
                ns: "ns".to_string(),
            }),
            vec![
                (StoreOp::Get, "ns/manifest.json".to_string()),
                (StoreOp::Get, "ns/segments/cluster_".to_string()),
            ]
        );
        assert_eq!(
            foreground_hold_calls(&Op::GcCycle {
                actor: ActorSel::ADMIN,
                ns: "ns".to_string(),
                keep_count: 4,
            }),
            vec![
                (StoreOp::Get, "ns/manifest.json".to_string()),
                (StoreOp::List, "ns/".to_string()),
            ]
        );
        assert_eq!(
            foreground_hold_calls(&Op::GetNamespace {
                actor: ActorSel::ADMIN,
                ns: "ns".to_string(),
            }),
            vec![(StoreOp::Get, "ns/manifest.json".to_string())]
        );
    }

    #[test]
    fn held_namespace_blocks_only_another_harness_mutation() {
        let mutation = Op::DeleteVectors {
            actor: ActorSel::ADMIN,
            ns: "held".to_string(),
            ids: vec!["one".to_string()],
        };
        let read = Op::GetNamespace {
            actor: ActorSel::ADMIN,
            ns: "held".to_string(),
        };
        let other_mutation = Op::DeleteVectors {
            actor: ActorSel::ADMIN,
            ns: "other".to_string(),
            ids: vec!["one".to_string()],
        };

        assert!(mutation_conflicts_with_held_namespace(&mutation, "held"));
        assert!(!mutation_conflicts_with_held_namespace(&read, "held"));
        assert!(!mutation_conflicts_with_held_namespace(
            &other_mutation,
            "held"
        ));
    }

    #[test]
    fn pending_hold_isolates_reads_that_can_stall_its_logical_release() {
        let held_read = Op::GetNamespace {
            actor: ActorSel::ADMIN,
            ns: "held".to_string(),
        };
        let other_read = Op::GetNamespace {
            actor: ActorSel::ADMIN,
            ns: "other".to_string(),
        };

        assert!(op_conflicts_with_held_namespace(&held_read, "held"));
        assert!(!op_conflicts_with_held_namespace(&other_read, "held"));
    }

    #[test]
    fn pending_hold_advances_with_reads_but_never_with_another_mutation() {
        let held_read = Op::GetNamespace {
            actor: ActorSel::ADMIN,
            ns: "held".to_string(),
        };
        let other_read = Op::GetNamespace {
            actor: ActorSel::ADMIN,
            ns: "other".to_string(),
        };
        let other_mutation = Op::DeleteVectors {
            actor: ActorSel::ADMIN,
            ns: "other".to_string(),
            ids: vec!["one".to_string()],
        };

        assert!(!op_can_run_while_hold_is_pending(&held_read, "held"));
        assert!(op_can_run_while_hold_is_pending(&other_read, "held"));
        assert!(!op_can_run_while_hold_is_pending(&other_mutation, "held"));
    }

    #[tokio::test]
    async fn pending_query_hold_defers_exact_error_probe_but_not_ordinary_query() {
        let exact_error = Op::InvalidProbe {
            actor: ActorSel::ADMIN,
            ns: "other".to_string(),
            probe: InvalidProbe::WeightsLenMismatch,
        };
        let ordinary_query = Op::Query {
            actor: ActorSel::ADMIN,
            ns: "other".to_string(),
            q: GeneratedQuery {
                body: json!({}),
                class: QueryOracleClass::Membership {
                    consistency: ConsistencyLevel::Strong,
                },
                pattern_tags: Vec::new(),
            },
            as_of: None,
        };
        let exact_error_query = Op::Query {
            actor: ActorSel::ADMIN,
            ns: "other".to_string(),
            q: GeneratedQuery {
                body: json!({}),
                class: QueryOracleClass::ExpectError {
                    status: 400,
                    code: "DIMENSION_MISMATCH".to_string(),
                },
                pattern_tags: Vec::new(),
            },
            as_of: None,
        };
        let non_query_probe = Op::InvalidProbe {
            actor: ActorSel::ADMIN,
            ns: "other".to_string(),
            probe: InvalidProbe::WrongDims,
        };
        let pending = PendingHeldOp {
            event_id: "held-query".to_string(),
            window_op: 5,
            scheduled_release_op: 12,
            release_op: 12,
            release_cause: HoldReleaseCause::LogicalOp,
            op_index: 5,
            namespace: "held".to_string(),
            holds_query_admission: true,
            task: tokio::spawn(std::future::pending()),
        };

        assert!(op_conflicts_with_pending_hold(
            &exact_error,
            6,
            None,
            &pending
        ));
        assert!(!op_conflicts_with_pending_hold(
            &ordinary_query,
            6,
            None,
            &pending
        ));
        assert!(op_conflicts_with_pending_hold(
            &exact_error_query,
            6,
            None,
            &pending
        ));
        assert!(!op_conflicts_with_pending_hold(
            &non_query_probe,
            6,
            None,
            &pending
        ));

        pending.task.abort();
    }

    #[test]
    fn sched_fifo_deferral_preserves_every_baseline_generated_operation() {
        fn upsert(ns: &str, id: &str) -> Op {
            Op::Upsert {
                actor: ActorSel::ADMIN,
                ns: ns.to_string(),
                vectors: vec![GenVector {
                    id: id.to_string(),
                    values: vec![1.0, 0.0],
                    attributes: None,
                }],
            }
        }

        fn id(op: &Op) -> String {
            match op {
                Op::Upsert { vectors, .. } => vectors[0].id.clone(),
                other => panic!("unexpected test op: {other:?}"),
            }
        }

        let baseline = vec![
            upsert("held", "held-1"),
            upsert("other", "other-1"),
            upsert("held", "held-2"),
            upsert("other", "other-2"),
        ];
        let mut generated = VecDeque::from(baseline.clone());
        let mut deferred = VecDeque::new();
        let mut scheduled = Vec::new();

        for _ in 0..2 {
            scheduled.push(next_fifo_deferred_op(
                &mut deferred,
                || generated.pop_front().expect("baseline stream exhausted"),
                |op, _| op.namespace() == "held",
            ));
        }
        for _ in 0..2 {
            scheduled.push(next_fifo_deferred_op(
                &mut deferred,
                || generated.pop_front().expect("unexpected fresh generation"),
                |_, _| false,
            ));
        }

        assert!(generated.is_empty());
        assert!(deferred.is_empty());
        assert_eq!(
            scheduled.iter().map(id).collect::<Vec<_>>()[2..],
            ["held-1", "held-2"]
        );
        let mut baseline_ids = baseline.iter().map(id).collect::<Vec<_>>();
        let mut scheduled_ids = scheduled.iter().map(id).collect::<Vec<_>>();
        baseline_ids.sort();
        scheduled_ids.sort();
        assert_eq!(scheduled_ids, baseline_ids);
        assert_eq!(scheduled.len(), baseline.len());
    }

    #[test]
    fn late_stream_window_selects_cached_unfiltered_late_query() {
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::LateStream,
            events: vec![FaultEvent {
                id: "late-stream-window".to_string(),
                start_op: 7,
                end_op: Some(8),
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("/matrix_".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::PreFail {
                    error: InjectedErrorKind::Http500,
                },
            }],
        });
        let cached = Op::LateQuery {
            actor: ActorSel::ADMIN,
            ns: "namespace-late".to_string(),
            query: vec![vec![1.0, 0.0]],
            top_k: 1,
            filter: None,
            consistency: ConsistencyLevel::Strong,
        };

        let (event_id, selected) =
            late_stream_fault_probe_for_window(Some(&scheduler), 7, Some(&cached))
                .expect("active late-stream window must select its cached query");
        assert_eq!(event_id, "late-stream-window");
        assert!(matches!(selected, Op::LateQuery { filter: None, .. }));
        assert!(late_stream_fault_probe_for_window(Some(&scheduler), 8, Some(&cached)).is_none());
    }

    #[test]
    fn sched_fifo_boundary_drain_preserves_every_generated_operation() {
        fn upsert(ns: &str, id: &str) -> Op {
            Op::Upsert {
                actor: ActorSel::ADMIN,
                ns: ns.to_string(),
                vectors: vec![GenVector {
                    id: id.to_string(),
                    values: vec![1.0, 0.0],
                    attributes: None,
                }],
            }
        }

        fn id(op: &Op) -> String {
            match op {
                Op::Upsert { vectors, .. } => vectors[0].id.clone(),
                other => panic!("unexpected test op: {other:?}"),
            }
        }

        let max_ops = 5u64;
        let baseline = vec![
            upsert("held", "held-start"),
            upsert("held", "held-deferred-1"),
            upsert("other", "other-1"),
            upsert("held", "held-deferred-2"),
            upsert("other", "other-2"),
        ];
        let mut generated = VecDeque::from(baseline.clone());
        let mut deferred = VecDeque::new();
        let mut recorded = vec![generated
            .pop_front()
            .expect("held boundary op must be generated first")];

        loop {
            let reserved = u64::try_from(recorded.len() + deferred.len())
                .expect("test workload size must fit in u64");
            let generation_budget = max_ops.saturating_sub(reserved);
            let Some(op) = next_fifo_deferred_op_with_budget(
                &mut deferred,
                generation_budget,
                || generated.pop_front().expect("baseline stream exhausted"),
                |op, _| op.namespace() == "held",
            ) else {
                assert_eq!(
                    reserved, max_ops,
                    "hold must end exactly at the finite workload boundary"
                );
                break;
            };
            recorded.push(op);
        }

        while let Some(op) = next_fifo_deferred_op_with_budget(
            &mut deferred,
            0,
            || panic!("boundary drain must not generate replacement operations"),
            |_, _| false,
        ) {
            recorded.push(op);
        }

        assert!(generated.is_empty());
        assert!(deferred.is_empty());
        assert_eq!(recorded.len(), max_ops as usize);
        assert_eq!(
            recorded.iter().map(id).collect::<Vec<_>>(),
            [
                "held-start",
                "other-1",
                "other-2",
                "held-deferred-1",
                "held-deferred-2",
            ]
        );
        let mut baseline_ids = baseline.iter().map(id).collect::<Vec<_>>();
        let mut recorded_ids = recorded.iter().map(id).collect::<Vec<_>>();
        baseline_ids.sort();
        recorded_ids.sort();
        assert_eq!(recorded_ids, baseline_ids);
    }

    #[test]
    fn terminal_held_operation_remains_selected_until_quiesce_join() {
        let scheduler = FaultScheduler::for_seed(691, FaultProfile::Sched);
        let holds = scheduler
            .schedule()
            .events
            .iter()
            .filter_map(|event| {
                let FaultKind::HoldCall { for_ops } = event.kind else {
                    return None;
                };
                Some((
                    event.start_op,
                    for_ops,
                    event.target.store_op,
                    event.target.key_substring.as_deref(),
                ))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            holds,
            [
                (210, 5, Some(StoreOp::Get), Some("manifest.json")),
                (370, 7, Some(StoreOp::Get), Some("cluster_")),
            ],
            "seed 691's two max-boundary holds drifted"
        );

        let normal_join = WorkloadAccountingSnapshot {
            selected_operation_ids: (0..500).collect(),
            completed_operation_ids: (0..500).collect(),
            held_operation_ids: Vec::new(),
            quiet_drain_operation_ids: Vec::new(),
        };
        assert_pre_quiet_workload_accounting(&normal_join, "normal join");

        let quiesce_join = WorkloadAccountingSnapshot {
            selected_operation_ids: (0..500).collect(),
            completed_operation_ids: (0..499).collect(),
            held_operation_ids: vec![499],
            quiet_drain_operation_ids: Vec::new(),
        };
        assert_pre_quiet_workload_accounting(&quiesce_join, "quiesce join");
    }

    #[test]
    fn corruption_tracker_records_new_timeline_taint_per_namespace_once() {
        let event = TimelineEvent {
            event_id: "content-00".to_string(),
            op_index: 7,
            wall_ms: 0,
            boundary: Boundary::ObjectStore,
            action: "Content(BitFlip)".to_string(),
            key: Some("ns-a/segments/cluster_0.bin".to_string()),
            semantics: FaultSemantics::PostCommit,
            observed: ObservedResult::Corrupted,
            recovery: None,
        };
        let mut tracker = CorruptionTracker::default();
        let namespaces = vec!["ns-a".to_string(), "ns-b".to_string()];

        tracker.observe(std::slice::from_ref(&event), &namespaces);
        tracker.observe(std::slice::from_ref(&event), &namespaces);

        assert_eq!(
            tracker.tainted_keys("ns-a"),
            Some(&BTreeSet::from(["ns-a/segments/cluster_0.bin".to_string()]))
        );
        assert!(tracker.durably_tainted_keys("ns-a").is_none());
        assert!(tracker.tainted_keys("ns-b").is_none());
        assert_eq!(tracker.seen_timeline_events, 1);
    }

    #[test]
    fn corruption_tracker_clears_taint_after_artifact_is_rewritten() {
        let mut tracker = CorruptionTracker {
            seen_timeline_events: 0,
            tainted: BTreeMap::from([(
                "ns".to_string(),
                BTreeSet::from([
                    "ns/segments/old.bin".to_string(),
                    "ns/segments/live.bin".to_string(),
                ]),
            )]),
            durably_tainted: BTreeMap::from([(
                "ns".to_string(),
                BTreeSet::from([
                    "ns/segments/old.bin".to_string(),
                    "ns/segments/live.bin".to_string(),
                ]),
            )]),
        };

        tracker.retain_reachable("ns", &BTreeSet::from(["ns/segments/live.bin".to_string()]));

        assert_eq!(
            tracker.tainted_keys("ns"),
            Some(&BTreeSet::from(["ns/segments/live.bin".to_string()]))
        );
        assert_eq!(
            tracker.durably_tainted_keys("ns"),
            Some(&BTreeSet::from(["ns/segments/live.bin".to_string()]))
        );
    }

    #[test]
    fn quiescence_accepts_only_clean_loud_failure_for_known_taint() {
        let tainted = BTreeSet::from(["ns/wal/missing.wal".to_string()]);
        let malformed_error = vec![Violation {
            id: ViolationId::I11ErrorEnvelope,
            op_index: 7,
            namespace: "ns".to_string(),
            detail: "malformed error".to_string(),
            evidence: json!({}),
        }];

        assert!(accept_loud_tainted_quiescence(500, &[], Some(&tainted)));
        assert!(!accept_loud_tainted_quiescence(500, &[], None));
        assert!(!accept_loud_tainted_quiescence(
            500,
            &malformed_error,
            Some(&tainted)
        ));
        assert!(!accept_loud_tainted_quiescence(200, &[], Some(&tainted)));
    }

    #[test]
    fn indeterminate_resolution_accepts_only_exact_durable_manifest_taint() {
        let namespace = "ns";
        let clean_loud = json!({
            "code": "INTERNAL_ERROR",
            "error": "an internal error occurred",
            "request_id": "request-id",
            "retryable": false,
            "status": 500,
        });
        let exact_manifest = BTreeSet::from([Manifest::s3_key(namespace)]);
        let unrelated_key = BTreeSet::from([format!("{namespace}/wal/tainted.wal")]);
        let other_manifest = BTreeSet::from([Manifest::s3_key("other")]);

        assert!(accept_loud_durable_manifest_resolution(
            500,
            &clean_loud,
            namespace,
            Some(&exact_manifest),
        ));
        assert!(!accept_loud_durable_manifest_resolution(
            500,
            &clean_loud,
            namespace,
            Some(&unrelated_key),
        ));
        assert!(!accept_loud_durable_manifest_resolution(
            500,
            &clean_loud,
            namespace,
            Some(&other_manifest),
        ));
        assert!(!accept_loud_durable_manifest_resolution(
            500,
            &json!({ "status": 500 }),
            namespace,
            Some(&exact_manifest),
        ));
        assert!(!accept_loud_durable_manifest_resolution(
            200,
            &json!({}),
            namespace,
            Some(&exact_manifest),
        ));
    }

    #[tokio::test]
    async fn quiet_s3_status_absorbs_exact_durable_manifest_failure() {
        let app = Router::new().route(
            "/v1/namespaces/ns/compact/status",
            get(|| async {
                let mut headers = HeaderMap::new();
                headers.insert("x-request-id", HeaderValue::from_static("request-id"));
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    headers,
                    Json(json!({
                        "code": "INTERNAL_ERROR",
                        "error": "an internal error occurred",
                        "request_id": "request-id",
                        "retryable": false,
                        "status": 500,
                    })),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let exact_manifest = BTreeSet::from([Manifest::s3_key("ns")]);

        let status = quiescent_s3_oracle_status(
            &raw_adversarial_client(),
            &format!("http://{address}"),
            "ns",
            Some(&exact_manifest),
        )
        .await;

        assert!(status.is_none(), "{status:#?}");
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn periodic_lineage_keeps_strict_http_status_without_exact_taint() {
        let app = Router::new().route(
            "/v1/namespaces/ns/compact/status",
            get(|| async {
                let mut headers = HeaderMap::new();
                headers.insert("x-request-id", HeaderValue::from_static("request-id"));
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    headers,
                    Json(json!({
                        "code": "INTERNAL_ERROR",
                        "error": "an internal error occurred",
                        "request_id": "request-id",
                        "retryable": false,
                        "status": 500,
                    })),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        Manifest::new().write(&store, "ns").await.unwrap();
        let client = raw_adversarial_client();
        let base_url = format!("http://{address}");
        let strict = tokio::spawn(async move {
            periodic_server_lineage_status(&client, &store, &base_url, "ns", &BTreeSet::new()).await
        })
        .await;

        assert!(strict.is_err_and(|error| error.is_panic()));
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn operational_queries_fall_back_to_a_live_modeled_vector() {
        let namespace = "ops-query".to_string();
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let mut model = Model::default();
        model.apply(
            &Op::CreateNamespace {
                actor: ActorSel::ADMIN,
                ns: namespace.clone(),
                spec,
            },
            201,
            Some(1),
            &json!({}),
            None,
        );
        model.apply(
            &Op::Upsert {
                actor: ActorSel::ADMIN,
                ns: namespace.clone(),
                vectors: vec![GenVector {
                    id: "live-vector".to_string(),
                    values: vec![1.0, 0.0],
                    attributes: None,
                }],
            },
            200,
            Some(2),
            &json!({}),
            None,
        );
        assert!(model.namespaces[&namespace].canonical_queries.is_empty());

        assert_eq!(
            operational_queries(&model),
            vec![(
                namespace,
                json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [1.0, 0.0],
                        "nprobe": 4
                    }],
                    "fusion": { "type": "none" },
                    "top_k": 1,
                    "candidate_k": 1,
                    "consistency": "strong",
                    "include_attributes": true
                })
            )]
        );
    }

    #[test]
    fn cache_fill_accepts_only_success_or_canonical_concurrency_limit() {
        let canonical = json!({
            "code": "CONCURRENCY_LIMIT",
            "error": "query concurrency limit reached, try again later",
            "status": 503,
            "retryable": true,
            "request_id": "ops-burst"
        });
        let altered = |key: &str, value: serde_json::Value| {
            let mut response = canonical.clone();
            response[key] = value;
            response
        };

        assert!(is_expected_cache_fill_response(200, &json!({})));
        assert!(is_expected_cache_fill_response(503, &canonical));
        assert!(!is_expected_cache_fill_response(
            503,
            &altered("code", json!("INDEX_UNAVAILABLE"))
        ));
        assert!(!is_expected_cache_fill_response(
            503,
            &altered("retryable", json!(false))
        ));
        assert!(!is_expected_cache_fill_response(
            503,
            &altered("request_id", json!(""))
        ));
        assert!(!is_expected_cache_fill_response(
            503,
            &altered("status", json!(500))
        ));
        assert!(!is_expected_cache_fill_response(503, &json!(null)));
        assert!(!is_expected_cache_fill_response(500, &canonical));

        let partitioned = json!({
            "code": "STORAGE_ERROR",
            "error": "a transient storage error occurred; please retry",
            "status": 500,
            "retryable": true,
            "request_id": "partitioned-read"
        });
        assert!(!is_expected_cache_fill_response(500, &partitioned));
        assert!(is_expected_partitioned_cache_fill_response(
            500,
            &partitioned
        ));
        assert!(!is_expected_partitioned_cache_fill_response(
            503,
            &partitioned
        ));
        let mut non_retryable = partitioned.clone();
        non_retryable["retryable"] = json!(false);
        assert!(!is_expected_partitioned_cache_fill_response(
            500,
            &non_retryable
        ));
    }

    #[tokio::test]
    async fn dual_writer_fencing_mutation_isolated_from_prior_same_generation_observation() {
        let harness = TestHarness::new().await;
        let store = harness.store.clone();
        let namespace = harness.artifact_origin_namespace("fenced");
        Manifest::new().write(&store, &namespace).await.unwrap();
        let key = Manifest::s3_key(&namespace);
        let original = store.get(&key).await.unwrap();
        let original_manifest = Manifest::from_bytes(&original).unwrap();
        assert_eq!(original_manifest.fencing_token, 0);
        let mut tracker = S3Tracker::default();
        let prior = tracker
            .check_namespace(
                &store,
                &namespace,
                6,
                &json!({ "manifest_generation": 1 }),
                false,
            )
            .await;
        assert!(prior.is_empty(), "{prior:#?}");

        let violations =
            inject_dual_writer_fencing_mutation(&store, &mut tracker, &namespace, 7, 1).await;

        assert_eq!(violations.len(), 1, "{violations:#?}");
        assert_eq!(violations[0].id, ViolationId::I21FencingViolation);
        assert!(violations[0].detail.contains("fork"));
        let authoritative_winner = store.get(&key).await.unwrap();
        let authoritative_manifest = Manifest::from_bytes(&authoritative_winner).unwrap();
        assert_eq!(authoritative_manifest.version(), 2);
        assert_eq!(
            authoritative_manifest.fencing_token, 2,
            "the committed winner must carry node B's exact token 2"
        );
        assert_ne!(authoritative_winner, original);

        let clean_advance = tracker
            .check_namespace(
                &store,
                &namespace,
                8,
                &json!({ "manifest_generation": 2 }),
                false,
            )
            .await;
        assert!(clean_advance.is_empty(), "{clean_advance:#?}");

        let mut later_fork = Manifest::read(&store, &namespace).await.unwrap().unwrap();
        later_fork.fencing_token += 1;
        store
            .put(&key, later_fork.to_bytes().unwrap())
            .await
            .unwrap();
        let workload_tracker_violations = tracker
            .check_namespace(
                &store,
                &namespace,
                9,
                &json!({ "manifest_generation": 2 }),
                false,
            )
            .await;
        assert_eq!(
            workload_tracker_violations.len(),
            1,
            "{workload_tracker_violations:#?}"
        );
        assert_eq!(
            workload_tracker_violations[0].id,
            ViolationId::I21FencingViolation
        );
        store.put(&key, authoritative_winner.clone()).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), authoritative_winner);
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn dual_writer_lease_activation_fires_and_releases_pinned_hold() {
        let harness = TestHarness::new().await;
        let bookkeeping_store = harness.store.clone();
        let namespace = harness.artifact_origin_namespace("fenced");
        let scheduler =
            FaultScheduler::from_schedule(FaultSchedule::dual_writer_fencing_selftest());
        let instrumented_store = store_fault_proxy(&bookkeeping_store, scheduler.clone());
        let clock = Arc::new(TestClock::default());
        scheduler.advance_to(0);

        let activation = begin_dual_writer_lease_hold(
            &scheduler,
            &bookkeeping_store,
            instrumented_store.clone(),
            instrumented_store,
            Arc::clone(&clock),
            &namespace,
            0,
        )
        .await;

        assert_eq!(activation.release_op, 8);
        assert_eq!(activation.stale_fencing_token, 1);
        assert_eq!(activation.node_b_lease.fencing_token, 2);
        assert!(!activation.node_a_renew.is_finished());
        scheduler.advance_to(7);
        assert!(!activation.node_a_renew.is_finished());
        scheduler.advance_to(8);
        let stale_fencing_token = finish_dual_writer_lease_hold(&scheduler, activation).await;
        assert_eq!(stale_fencing_token, 1);

        let timeline = dual_writer_lease_hold_timeline(&scheduler);
        assert_eq!(timeline.len(), 2, "{timeline:#?}");
        assert_eq!(timeline[0].semantics, FaultSemantics::WindowActive);
        assert_eq!(timeline[1].semantics, FaultSemantics::WindowEnd);
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn mutating_server_error_is_ambiguous_when_faults_are_active() {
        let app = Router::new().route(
            "/v1/namespaces/test/vectors",
            post(|| async {
                let mut headers = HeaderMap::new();
                headers.insert("x-request-id", HeaderValue::from_static("test-request"));
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    headers,
                    Json(json!({
                        "code": "STORAGE_ERROR",
                        "error": "injected",
                        "status": 500,
                        "retryable": true,
                        "request_id": "test-request"
                    })),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let outcome = request_outcome(
            &raw_adversarial_client(),
            Method::POST,
            &format!("http://{address}/v1/namespaces/test/vectors"),
            Some(json!({ "vectors": [] })),
            true,
        )
        .await;
        server.abort();

        assert!(matches!(
            outcome,
            OpOutcome::Ambiguous {
                reason: AmbiguityReason::ServerError { status: 500 },
                status: Some(500),
            }
        ));
    }

    #[tokio::test]
    async fn unmatched_http_requests_bypass_fault_proxy() {
        let app = Router::new().route(
            "/pass",
            post(|| async { Json(json!({ "relayed": "direct" })) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let direct_base_url = format!("http://{address}");
        let injector = start_http_fault_injector(&direct_base_url).await;
        let context = HttpFaultContext {
            scheduler: FaultScheduler::from_schedule(FaultSchedule {
                profile: FaultProfile::Network,
                events: Vec::new(),
            }),
            injector: injector.request_handle(),
            bookkeeping_store: ZeppelinStore::new(Arc::new(InMemory::new())),
            direct_base_url: direct_base_url.clone(),
            proxy_base_url: "http://127.0.0.1:1".to_string(),
        };

        let outcome = HTTP_FAULT_CONTEXT
            .scope(
                Some(context),
                REQUEST_IS_MUTATION.scope(
                    false,
                    request_outcome(
                        &raw_adversarial_client(),
                        Method::POST,
                        &format!("{direct_base_url}/pass"),
                        None,
                        true,
                    ),
                ),
            )
            .await;

        assert!(matches!(outcome, OpOutcome::Applied { status: 200, .. }));
        Arc::try_unwrap(injector).unwrap().shutdown().await;
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn full_test_server_accepts_an_exact_disk_cache_budget() {
        let harness = TestHarness::new().await;
        let server = crate::common::server::start_test_server_full_with_disk_cache_max_bytes(
            harness.store.clone(),
            Some(harness.prefix.clone()),
            deterministic_config(),
            false,
            None,
            2 * 1024 * 1024,
        )
        .await;

        assert_eq!(server.cache.max_size_bytes(), 2 * 1024 * 1024);

        server.shutdown().await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn ops_server_uses_the_requested_tiny_disk_cache_budget() {
        let harness = TestHarness::new().await;
        let server = crate::common::server::start_test_server_full_with_disk_cache_max_bytes(
            harness.store.clone(),
            Some(harness.prefix.clone()),
            deterministic_config(),
            false,
            None,
            2 * 1024 * 1024,
        )
        .await;

        assert_eq!(server.cache.max_size_bytes(), 2 * 1024 * 1024);

        server.shutdown().await;
        harness.cleanup().await;
    }

    #[test]
    fn ops_compaction_config_bounds_background_work_and_keeps_exact_count_observable() {
        let full_campaign = FaultScheduler::for_seed(7, FaultProfile::Ops);
        let config = config_for_mode(RunMode::Chaos, 7, Some(full_campaign.schedule()));

        assert_eq!(config.server.max_concurrent_queries, 1);
        assert_eq!(config.compaction.interval_secs, 1);
        assert_eq!(config.compaction.max_wal_fragments_before_compact, 2);
        assert_eq!(config.compaction.max_old_segments, 0);

        let dual_writer_control = FaultSchedule::dual_writer_fencing_selftest();
        let config = config_for_mode(RunMode::Chaos, 7, Some(&dual_writer_control));

        assert_eq!(config.server.max_concurrent_queries, 1);
        assert_eq!(config.compaction.interval_secs, 5);
        assert_eq!(config.compaction.max_wal_fragments_before_compact, 2);
    }

    #[test]
    fn chaos_network_gc_horizon_covers_inflight_wal_publication() {
        let scheduler = FaultScheduler::for_seed(742, FaultProfile::Network);
        let config = config_for_mode(RunMode::Chaos, 742, Some(scheduler.schedule()));
        let floor = config
            .gc_horizon_floor_secs()
            .expect("adversarial GC horizon floor must not overflow");

        assert_eq!(config.gc.horizon_secs, floor);
        assert!(!config.gc.allow_unsafe_short_horizon);
    }

    #[test]
    fn standalone_ops_requires_strict_compaction_proof_but_full_overlap_does_not() {
        let standalone_ops = FaultScheduler::for_seed(7, FaultProfile::Ops);
        assert!(requires_two_node_compaction_evidence_for_schedule(Some(
            standalone_ops.schedule()
        )));

        let full_overlap = FaultSchedule {
            profile: FaultProfile::Full,
            events: vec![
                FaultEvent {
                    id: "full-network-00".to_string(),
                    start_op: 40,
                    end_op: Some(80),
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector::default(),
                    kind: FaultKind::Partition {
                        direction: Direction::All,
                    },
                },
                FaultEvent {
                    id: "full-ops-01".to_string(),
                    start_op: 50,
                    end_op: Some(70),
                    boundary: Boundary::Runner,
                    target: TargetSelector::default(),
                    kind: FaultKind::StartSecondNode { for_ops: 20 },
                },
            ],
        };
        assert!(!requires_two_node_compaction_evidence_for_schedule(Some(
            &full_overlap
        )));
    }

    #[tokio::test]
    async fn ops_second_node_window_proves_both_background_workers_and_activity() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let namespace = format!("{prefix}-ops-compaction-workers");
        let scheduler = FaultScheduler::for_seed(7, FaultProfile::Ops);
        let config = config_for_mode(RunMode::Chaos, 7, Some(scheduler.schedule()));
        let observer = OperationalStoreObserver::default();
        let primary_store = operational_store_proxy(&harness.store, observer.clone(), 0);
        let primary = start_test_server_full(
            primary_store,
            Some(prefix.clone()),
            config.clone(),
            true,
            None,
        )
        .await;
        let client = adversarial_client(&primary);
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let (create_status, _) = request_json(
            &client,
            Method::POST,
            &format!("{}/v1/namespaces", primary.base_url),
            Some(spec.create_body(&namespace)),
        )
        .await;
        assert_eq!(create_status, StatusCode::CREATED.as_u16());
        let background_start = successful_background_compaction_metric(&namespace);

        let mut state = OperationalState::default();
        let remaining = state
            .apply_node_commands(
                vec![SchedulerCommand::StartSecondNode {
                    event_id: "ops-focused-second-node".to_string(),
                    for_ops: 20,
                }],
                NodeCommandContext {
                    scheduler: Some(&scheduler),
                    store: &harness.store,
                    shared_clock: None,
                    operational_observer: Some(&observer),
                    require_compaction_evidence: true,
                    prefix: &prefix,
                    config: &config,
                    admin_bearer: &primary.admin_bearer,
                    disk_cache_max_bytes: 2 * 1024 * 1024,
                    op_index: 40,
                },
            )
            .await;
        assert!(remaining.is_empty());

        let mut routed_nodes = Vec::new();
        for _ in 0..4 {
            let node = state.choose_target_node();
            let target = state.target(&primary, node);
            let (status, _) = request_json(
                &client,
                Method::GET,
                &format!("{}/v1/namespaces/{namespace}", target.base_url),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK.as_u16());
            routed_nodes.push(node);
        }
        assert_eq!(routed_nodes, vec![0, 1, 0, 1]);

        for fragment in 0..2 {
            let vectors = (0..2)
                .map(|index| {
                    let vector_index = fragment * 2 + index;
                    json!({
                        "id": format!("worker-proof-{vector_index}"),
                        "values": [1.0, vector_index as f32]
                    })
                })
                .collect::<Vec<_>>();
            let (upsert_status, _) = request_json(
                &client,
                Method::POST,
                &format!("{}/v1/namespaces/{namespace}/vectors", primary.base_url),
                Some(json!({ "vectors": vectors })),
            )
            .await;
            assert_eq!(upsert_status, StatusCode::OK.as_u16());
        }

        tokio::time::timeout(Duration::from_secs(30), async {
            while successful_background_compaction_metric(&namespace) == background_start {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect(
            "the focused two-node window published a fenced manifest but did not finish a \
             background compaction",
        );

        let remaining = state
            .apply_node_commands(
                vec![SchedulerCommand::StopSecondNode {
                    event_id: "ops-focused-second-node".to_string(),
                }],
                NodeCommandContext {
                    scheduler: Some(&scheduler),
                    store: &harness.store,
                    shared_clock: None,
                    operational_observer: Some(&observer),
                    require_compaction_evidence: true,
                    prefix: &prefix,
                    config: &config,
                    admin_bearer: &primary.admin_bearer,
                    disk_cache_max_bytes: 2 * 1024 * 1024,
                    op_index: 60,
                },
            )
            .await;
        assert!(remaining.is_empty());
        assert!(
            successful_background_compaction_metric(&namespace) > background_start,
            "the focused two-node window recorded no background compaction"
        );
        let stop = scheduler
            .timeline()
            .into_iter()
            .find(|event| event.semantics == FaultSemantics::WindowEnd)
            .expect("focused two-node window did not record its stop proof");
        let expected_recovery = format!(
            "namespace={namespace}; lease_attempt_nodes=[0,1]; lease_publication=true; \
             fenced_manifest=true; background_activity=true"
        );
        assert_eq!(stop.recovery.as_deref(), Some(expected_recovery.as_str()));

        primary.shutdown().await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn ops_exhaustion_burst_completes_eight_requests_and_records_limits() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let namespace = format!("{prefix}-ops-exhaustion");
        let scheduler = FaultScheduler::for_seed(7, FaultProfile::Ops);
        let config = config_for_mode(RunMode::Chaos, 7, Some(scheduler.schedule()));
        let disk_cache_max_bytes = 2 * 1024 * 1024;
        let server = start_test_server_full_with_disk_cache_max_bytes(
            harness.store.clone(),
            Some(prefix.clone()),
            config.clone(),
            false,
            None,
            disk_cache_max_bytes,
        )
        .await;
        let client = adversarial_client(&server);
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let (create_status, _) = request_json(
            &client,
            Method::POST,
            &format!("{}/v1/namespaces", server.base_url),
            Some(spec.create_body(&namespace)),
        )
        .await;
        assert_eq!(create_status, StatusCode::CREATED.as_u16());
        let mut model = Model::default();
        model.apply(
            &Op::CreateNamespace {
                actor: ActorSel::ADMIN,
                ns: namespace,
                spec,
            },
            StatusCode::CREATED.as_u16(),
            Some(1),
            &json!({}),
            None,
        );
        let mut state = OperationalState::default();
        let remaining = state
            .apply_node_commands(
                vec![SchedulerCommand::ResourceExhaustion {
                    event_id: "ops-focused-resource-limits".to_string(),
                    max_concurrent_queries: 1,
                    disk_cache_max_bytes,
                }],
                NodeCommandContext {
                    scheduler: Some(&scheduler),
                    store: &harness.store,
                    shared_clock: None,
                    operational_observer: None,
                    require_compaction_evidence: false,
                    prefix: &prefix,
                    config: &config,
                    admin_bearer: &server.admin_bearer,
                    disk_cache_max_bytes,
                    op_index: 8,
                },
            )
            .await;
        assert!(remaining.is_empty());
        state
            .apply_environment_commands(
                vec![SchedulerCommand::FillDiskCache {
                    event_id: "ops-focused-exhaustion-burst".to_string(),
                }],
                EnvironmentCommandContext {
                    scheduler: Some(&scheduler),
                    operational_observer: None,
                    client: &client,
                    primary: &server,
                    model: &mut model,
                    op_index: 9,
                },
            )
            .await;

        assert_eq!(server.cache.max_size_bytes(), disk_cache_max_bytes);
        assert_eq!(config.server.max_concurrent_queries, 1);
        let timeline = scheduler.timeline();
        assert_eq!(timeline.len(), 2, "{timeline:#?}");
        assert!(timeline[0].action.contains("queries=1"));
        assert!(timeline[0].action.contains("disk_cache_bytes=2097152"));
        let burst = timeline[1]
            .recovery
            .as_deref()
            .expect("focused exhaustion burst metadata missing");
        assert!(burst.contains("completed=8"), "{burst}");
        assert!(burst.contains("nodes={0}"), "{burst}");

        server.shutdown().await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn supported_full_cache_fill_joins_an_armed_crash_and_restarts() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let namespace = format!("{prefix}-armed-cache-fill");
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::SupportedFull,
            events: vec![FaultEvent {
                id: "supported-full-cache-fill-crash".to_string(),
                start_op: 9,
                end_op: None,
                boundary: Boundary::Process,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("cluster_".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::CrashAt {
                    point: crate::adversarial::faults::process::CrashPoint::HydrationGet,
                    position: TriggerPosition::Pre,
                },
            }],
        });
        let store = store_fault_proxy(&harness.store, scheduler.clone());
        let config = deterministic_config();
        let mut server = RestartableFullTestServer::new(
            start_test_server_full(
                store.clone(),
                Some(prefix.clone()),
                config.clone(),
                false,
                None,
            )
            .await,
        );
        let client = adversarial_client(&server);
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let (create_status, _) = request_json(
            &client,
            Method::POST,
            &format!("{}/v1/namespaces", server.base_url),
            Some(spec.create_body(&namespace)),
        )
        .await;
        assert_eq!(create_status, StatusCode::CREATED.as_u16());
        let mut model = Model::default();
        model.apply(
            &Op::CreateNamespace {
                actor: ActorSel::ADMIN,
                ns: namespace,
                spec,
            },
            StatusCode::CREATED.as_u16(),
            Some(1),
            &json!({}),
            None,
        );

        let controller = scheduler
            .process_controller()
            .expect("crash schedule must install a process controller");
        controller.request_crash(CrashRequest {
            event_id: "supported-full-cache-fill-crash".to_string(),
            op_index: 9,
            point: crate::adversarial::faults::process::CrashPoint::HydrationGet,
            position: TriggerPosition::Pre,
            key: format!("{prefix}/segments/pinned/cluster_0.bin"),
        });
        scheduler.advance_to(9);
        let environment = OperationalState::default()
            .apply_environment_commands(
                vec![SchedulerCommand::FillDiskCache {
                    event_id: "supported-full-cache-fill".to_string(),
                }],
                EnvironmentCommandContext {
                    scheduler: Some(&scheduler),
                    operational_observer: None,
                    client: &client,
                    primary: &server,
                    model: &mut model,
                    op_index: 9,
                },
            )
            .await;
        assert!(
            environment.violations.is_empty(),
            "{:?}",
            environment
                .violations
                .iter()
                .map(|violation| &violation.detail)
                .collect::<Vec<_>>()
        );
        let crash = environment
            .crash
            .expect("armed operational crash was not surfaced");
        let mut injector = None;
        let mut http_fault_context = None;
        let recovery = restart_after_crash(
            &mut server,
            &controller,
            &scheduler,
            &mut injector,
            &mut http_fault_context,
            &store,
            &harness.store,
            &prefix,
            &config,
            false,
            &client,
            &model,
            9,
            crash,
        )
        .await;
        assert!(recovery.is_empty(), "{recovery:#?}");

        let timeline = scheduler.timeline();
        let burst = timeline
            .iter()
            .find(|event| event.event_id == "supported-full-cache-fill")
            .and_then(|event| event.recovery.as_deref())
            .expect("cache-fill completion proof missing");
        assert!(burst.contains("completed=8"), "{burst}");
        assert!(timeline.iter().any(|event| {
            event.event_id == "supported-full-cache-fill-crash"
                && event.recovery.as_deref() == Some("restart+health-wait")
        }));

        server.into_inner().shutdown().await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn supported_full_cache_fill_records_scheduled_read_partition_failures() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let namespace = format!("{prefix}-partitioned-cache-fill");
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::SupportedFull,
            events: vec![
                FaultEvent {
                    id: "supported-full-network-01".to_string(),
                    start_op: 5,
                    end_op: Some(10),
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector::default(),
                    kind: FaultKind::Partition {
                        direction: Direction::ReadsFail,
                    },
                },
                FaultEvent {
                    id: "supported-full-ops-06".to_string(),
                    start_op: 5,
                    end_op: None,
                    boundary: Boundary::Runner,
                    target: TargetSelector::default(),
                    kind: FaultKind::FillDiskCache,
                },
            ],
        });
        let server = start_test_server_full(
            store_fault_proxy(&harness.store, scheduler.clone()),
            Some(prefix),
            deterministic_config(),
            false,
            None,
        )
        .await;
        let client = adversarial_client(&server);
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let (create_status, _) = request_json(
            &client,
            Method::POST,
            &format!("{}/v1/namespaces", server.base_url),
            Some(spec.create_body(&namespace)),
        )
        .await;
        assert_eq!(create_status, StatusCode::CREATED.as_u16());
        let mut model = Model::default();
        model.apply(
            &Op::CreateNamespace {
                actor: ActorSel::ADMIN,
                ns: namespace,
                spec,
            },
            StatusCode::CREATED.as_u16(),
            Some(1),
            &json!({}),
            None,
        );

        let commands = scheduler.advance_to(5);
        assert!(commands
            .iter()
            .any(|command| matches!(command, SchedulerCommand::FillDiskCache { .. })));
        OperationalState::default()
            .apply_environment_commands(
                commands,
                EnvironmentCommandContext {
                    scheduler: Some(&scheduler),
                    operational_observer: None,
                    client: &client,
                    primary: &server,
                    model: &mut model,
                    op_index: 5,
                },
            )
            .await;

        let timeline = scheduler.timeline();
        let burst = timeline
            .iter()
            .find(|event| event.event_id == "supported-full-ops-06")
            .and_then(|event| event.recovery.as_deref())
            .expect("partitioned cache-fill burst metadata missing");
        assert!(burst.contains("completed=8"), "{burst}");
        assert!(burst.contains("successful=0"), "{burst}");
        assert!(burst.contains("storage_faulted=8"), "{burst}");

        let _ = scheduler.advance_to(10);
        server.shutdown().await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn ops_inflight_delete_reconciles_the_modeled_namespace() {
        let harness = TestHarness::new().await;
        let operational_observer = OperationalStoreObserver::default();
        let prefix = harness.prefix.clone();
        let namespace = format!("{prefix}-ops-delete-race");
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let server = start_test_server_full(
            operational_store_proxy(&harness.store, operational_observer.clone(), 0),
            Some(prefix),
            deterministic_config(),
            false,
            None,
        )
        .await;
        let client = adversarial_client(&server);
        let create = client
            .post(format!("{}/v1/namespaces", server.base_url))
            .json(&spec.create_body(&namespace))
            .send()
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);

        let mut model = Model::default();
        model.apply(
            &Op::CreateNamespace {
                actor: ActorSel::ADMIN,
                ns: namespace.clone(),
                spec,
            },
            StatusCode::CREATED.as_u16(),
            Some(1),
            &json!({}),
            None,
        );
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Ops,
            events: Vec::new(),
        });
        let mut state = OperationalState::default();
        state
            .apply_environment_commands(
                vec![SchedulerCommand::DeleteNamespaceInFlight {
                    event_id: "ops-delete-race".to_string(),
                }],
                EnvironmentCommandContext {
                    scheduler: Some(&scheduler),
                    operational_observer: Some(&operational_observer),
                    client: &client,
                    primary: &server,
                    model: &mut model,
                    op_index: 17,
                },
            )
            .await;

        assert!(
            !model.namespaces.contains_key(&namespace),
            "an accepted operational delete must remove the namespace from the model"
        );
        let timeline = scheduler.timeline();
        assert_eq!(timeline.len(), 2, "{timeline:#?}");
        assert_eq!(timeline[0].semantics, FaultSemantics::WindowActive);
        assert_eq!(
            timeline[0].recovery.as_deref(),
            Some(
                "barrier=wal_put_entered; delete_joined=false; \
                 barrier_released=false; upsert_joined=false"
            )
        );
        assert_eq!(timeline[1].semantics, FaultSemantics::WindowEnd);
        let joined = timeline[1]
            .recovery
            .as_deref()
            .expect("delete rendezvous completion metadata missing");
        assert!(joined.contains("delete_joined=true"));
        assert!(joined.contains("barrier_released=true"));
        assert!(joined.contains("upsert_joined=true"));

        server.shutdown().await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn ops_delete_rendezvous_replay_reenacts_recorded_causality() {
        let source_root = tempfile::TempDir::new().unwrap();
        let source_env = RunnerEnv {
            seconds: 30,
            seeds: vec![13],
            max_ops: Some(2),
            artifacts: source_root.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Chaos,
            profile: Some(FaultProfile::Ops),
            env_echo: BTreeMap::new(),
        };
        let source_artifacts = RunArtifacts::create(&source_env);
        let source_namespace = "source-prefix-adv-13-0".to_string();
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let config = deterministic_config();
        let schedule = FaultSchedule {
            profile: FaultProfile::Ops,
            events: vec![FaultEvent {
                id: "ops-focused-delete-race".to_string(),
                start_op: 1,
                end_op: None,
                boundary: Boundary::Runner,
                target: TargetSelector::default(),
                kind: FaultKind::DeleteNamespaceInFlight,
            }],
        };
        let specs = BTreeMap::from([(source_namespace.clone(), spec.clone())]);
        let mut source_seed = source_artifacts.seed(
            13,
            &config,
            &specs,
            RunMode::Chaos,
            None,
            None,
            None,
            Some(&schedule),
        );
        for record in [
            OpRecord {
                index: 0,
                wall_ms: 0,
                op: Op::CreateNamespace {
                    actor: ActorSel::ADMIN,
                    ns: source_namespace.clone(),
                    spec,
                },
                method: "POST".to_string(),
                path: "/v1/namespaces".to_string(),
                status: StatusCode::CREATED.as_u16(),
                response: json!({}),
                outcome: "applied".to_string(),
                target_node: 0,
                execution: ExecutionMetadata::default(),
                gen_after: Some(1),
                duration_ms: 0,
                violations: Vec::new(),
            },
            OpRecord {
                index: 1,
                wall_ms: 1,
                op: Op::GetNamespace {
                    actor: ActorSel::ADMIN,
                    ns: source_namespace.clone(),
                },
                method: "GET".to_string(),
                path: format!("/v1/namespaces/{source_namespace}"),
                status: StatusCode::NOT_FOUND.as_u16(),
                response: json!({}),
                outcome: "definite_not_applied".to_string(),
                target_node: 0,
                execution: ExecutionMetadata::default(),
                gen_after: None,
                duration_ms: 0,
                violations: Vec::new(),
            },
        ] {
            source_seed.write_op(&record);
        }
        let source_dir = source_seed.dir.clone();
        drop(source_seed);

        let replay_root = tempfile::TempDir::new().unwrap();
        let replay_env = RunnerEnv {
            artifacts: replay_root.path().to_path_buf(),
            max_ops: None,
            profile: None,
            ..source_env
        };
        let outcome = Box::pin(run_replay(&replay_env, &source_dir)).await;
        assert!(!outcome.failed, "{:?}", outcome.violations);
        let replay_dir = fs::read_dir(replay_root.path())
            .unwrap()
            .find_map(|entry| {
                let path = entry.unwrap().path().join("seed-13");
                path.join("timeline.jsonl").exists().then_some(path)
            })
            .expect("focused delete replay did not write a seed timeline");
        let timeline = fs::read_to_string(replay_dir.join("timeline.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<TimelineEvent>(line).unwrap())
            .filter(|event| event.event_id == "ops-focused-delete-race")
            .collect::<Vec<_>>();
        assert_eq!(timeline.len(), 2, "{timeline:#?}");
        assert_eq!(timeline[0].semantics, FaultSemantics::WindowActive);
        assert_eq!(
            timeline[0].recovery.as_deref(),
            Some(
                "barrier=wal_put_entered; delete_joined=false; \
                 barrier_released=false; upsert_joined=false"
            )
        );
        assert_eq!(timeline[1].semantics, FaultSemantics::WindowEnd);
        let joined = timeline[1]
            .recovery
            .as_deref()
            .expect("focused replay delete join metadata missing");
        for proof in [
            "delete_joined=true",
            "barrier_released=true",
            "upsert_joined=true",
        ] {
            assert!(joined.contains(proof), "missing {proof}: {joined}");
        }
    }

    #[tokio::test]
    async fn ops_inflight_join_restores_strict_checkpointing_after_second_node_stop() {
        let harness = TestHarness::new().await;
        let operational_observer = OperationalStoreObserver::default();
        let prefix = harness.prefix.clone();
        let namespace = format!("{prefix}-ops-delete-rejected");
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let server = start_test_server_full(
            operational_store_proxy(&harness.store, operational_observer.clone(), 0),
            Some(prefix),
            deterministic_config(),
            false,
            None,
        )
        .await;
        let client = adversarial_client(&server);
        let create = client
            .post(format!("{}/v1/namespaces", server.base_url))
            .json(&spec.create_body(&namespace))
            .send()
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);

        let mut model = Model::default();
        model.apply(
            &Op::CreateNamespace {
                actor: ActorSel::ADMIN,
                ns: namespace.clone(),
                spec,
            },
            StatusCode::CREATED.as_u16(),
            Some(1),
            &json!({}),
            None,
        );
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Network,
            events: vec![FaultEvent {
                id: "reject-operational-delete".to_string(),
                start_op: 17,
                end_op: None,
                boundary: Boundary::ClientHttp,
                target: TargetSelector {
                    path_substring: Some(format!("/v1/namespaces/{namespace}")),
                    methods: Some(vec!["DELETE".to_string()]),
                    ..TargetSelector::default()
                },
                kind: FaultKind::DropRequest,
            }],
        });
        scheduler.advance_to(17);
        let injector = start_http_fault_injector(&server.base_url).await;
        let context = HttpFaultContext {
            scheduler,
            injector: injector.request_handle(),
            bookkeeping_store: harness.store.clone(),
            direct_base_url: server.base_url.clone(),
            proxy_base_url: injector.base_url(),
        };
        let mut state = OperationalState::default();
        state.record_second_node_started();
        assert!(!state.generation_checkpoints_enabled());
        state.record_second_node_stopped();
        assert!(state.generation_checkpoints_enabled());

        HTTP_FAULT_CONTEXT
            .scope(
                Some(context),
                state.apply_environment_commands(
                    vec![SchedulerCommand::DeleteNamespaceInFlight {
                        event_id: "ops-delete-race".to_string(),
                    }],
                    EnvironmentCommandContext {
                        scheduler: None,
                        operational_observer: Some(&operational_observer),
                        client: &client,
                        primary: &server,
                        model: &mut model,
                        op_index: 17,
                    },
                ),
            )
            .await;

        let namespace_model = &model.namespaces[&namespace];
        assert!(namespace_model.live.contains_key("operational-race-17"));
        assert!(
            namespace_model.checkpoints[&1].contains_key("operational-race-17"),
            "joined operational writes must resume strict checkpoints after node 2 stops"
        );

        Arc::try_unwrap(injector).unwrap().shutdown().await;
        server.shutdown().await;
        harness.cleanup().await;
    }

    #[test]
    fn second_node_activity_is_explicit_and_generation_strictness_resumes_after_stop() {
        let mut state = OperationalState::default();
        assert!(!state.second_node_active());
        assert!(!state.second_node_ever_active());
        assert!(state.generation_checkpoints_enabled());
        assert!(!state.quiescent_vector_count_must_be_exact());

        state.record_second_node_started();
        assert!(state.second_node_active());
        assert!(state.second_node_ever_active());
        assert!(!state.generation_checkpoints_enabled());
        assert!(state.quiescent_vector_count_must_be_exact());

        state.record_second_node_stopped();
        assert!(!state.second_node_active());
        assert!(state.second_node_ever_active());
        assert!(state.generation_checkpoints_enabled());
        assert!(state.quiescent_vector_count_must_be_exact());
    }

    #[test]
    fn read_only_secondary_router_never_routes_mutations_to_node_one() {
        let mut state = OperationalState::default();
        state.record_read_only_node_started();
        assert!(state.generation_checkpoints_enabled());
        assert!(
            !state.quiescent_vector_count_must_be_exact(),
            "a read-only secondary must preserve the single-writer count policy"
        );

        let read = Op::GetNamespace {
            actor: ActorSel::ADMIN,
            ns: "catalog".to_string(),
        };
        let write = Op::Upsert {
            actor: ActorSel::ADMIN,
            ns: "catalog".to_string(),
            vectors: Vec::new(),
        };
        assert_eq!(state.choose_target_node_for_op(&read), 0);
        assert_eq!(state.choose_target_node_for_op(&read), 1);
        for _ in 0..8 {
            assert_eq!(state.choose_target_node_for_op(&write), 0);
        }

        state.record_second_node_stopped();
        assert!(!state.second_node_active());
        assert!(!state.quiescent_vector_count_must_be_exact());
    }

    #[tokio::test]
    async fn supported_read_only_node_has_no_background_writer_and_rejects_mutation_routing() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let mut config = deterministic_config();
        let admin_bearer = crate::common::server::test_admin_bearer(&mut config);
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::SupportedFull,
            events: vec![FaultEvent {
                id: "supported-read-only-node".to_string(),
                start_op: 0,
                end_op: Some(20),
                boundary: Boundary::Runner,
                target: TargetSelector::default(),
                kind: FaultKind::StartReadOnlyNode { for_ops: 20 },
            }],
        });
        let mut state = OperationalState::default();
        let commands = scheduler.advance_to(0);
        let remaining = state
            .apply_node_commands(
                commands,
                NodeCommandContext {
                    scheduler: Some(&scheduler),
                    store: &harness.store,
                    shared_clock: None,
                    operational_observer: None,
                    require_compaction_evidence: false,
                    prefix: &prefix,
                    config: &config,
                    admin_bearer: &admin_bearer,
                    disk_cache_max_bytes: 4 * 1024 * 1024,
                    op_index: 0,
                },
            )
            .await;
        assert!(remaining.is_empty());
        assert!(state
            .second_node
            .as_ref()
            .expect("read-only node must be running")
            .compaction_loop_task
            .is_none());

        let read = Op::GetNamespace {
            actor: ActorSel::ADMIN,
            ns: "catalog".to_string(),
        };
        let write = Op::Upsert {
            actor: ActorSel::ADMIN,
            ns: "catalog".to_string(),
            vectors: Vec::new(),
        };
        assert_eq!(state.choose_target_node_for_op(&read), 0);
        assert_eq!(state.choose_target_node_for_op(&read), 1);
        assert_eq!(state.choose_target_node_for_op(&write), 0);

        let remaining = state
            .apply_node_commands(
                scheduler.advance_to(20),
                NodeCommandContext {
                    scheduler: Some(&scheduler),
                    store: &harness.store,
                    shared_clock: None,
                    operational_observer: None,
                    require_compaction_evidence: false,
                    prefix: &prefix,
                    config: &config,
                    admin_bearer: &admin_bearer,
                    disk_cache_max_bytes: 4 * 1024 * 1024,
                    op_index: 20,
                },
            )
            .await;
        assert!(remaining.is_empty());
        assert!(state.second_node.is_none());
        harness.cleanup().await;
    }

    #[test]
    fn ops_lineage_observation_runs_before_during_and_after_the_real_window() {
        for op_index in [39, 40, 59, 60] {
            assert!(should_observe_lineage(
                RunMode::Chaos,
                Some(FaultProfile::Ops),
                op_index,
            ));
        }
        assert!(!should_observe_lineage(
            RunMode::Chaos,
            Some(FaultProfile::Network),
            40,
        ));
        assert!(should_observe_lineage(RunMode::Deterministic, None, 25,));
        assert!(!should_observe_lineage(RunMode::Deterministic, None, 26,));
    }

    #[tokio::test]
    async fn quiet_period_restarts_finished_primary_after_terminal_hold_and_drains_reserved_ops() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let held_ns = format!("{prefix}-held");
        let other_ns = format!("{prefix}-other");
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Sched,
            events: vec![FaultEvent {
                id: "sched-held-manifest-get".to_string(),
                start_op: 2,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some(format!("{held_ns}/manifest.json")),
                    ..TargetSelector::default()
                },
                kind: FaultKind::HoldCall { for_ops: 5 },
            }],
        });
        let config = deterministic_config();
        let mut server = RestartableFullTestServer::new(
            start_test_server_full(
                store_fault_proxy(&harness.store, scheduler.clone()),
                Some(prefix.clone()),
                config.clone(),
                false,
                None,
            )
            .await,
        );
        let artifacts_dir = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 1,
            seeds: vec![0],
            max_ops: Some(5),
            artifacts: artifacts_dir.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Chaos,
            profile: Some(FaultProfile::Sched),
            env_echo: BTreeMap::new(),
        };
        let run_artifacts = RunArtifacts::create(&env);
        let specs = BTreeMap::from([
            (held_ns.clone(), spec.clone()),
            (other_ns.clone(), spec.clone()),
        ]);
        let mut artifacts = run_artifacts.seed(
            0,
            &config,
            &specs,
            RunMode::Chaos,
            None,
            None,
            None,
            Some(scheduler.schedule()),
        );
        let client = adversarial_client(&server);
        let started = Instant::now();
        let mut model = Model::default();
        let mut coverage = Coverage::default();
        let mut s3_tracker = S3Tracker::default();
        let mut corruption_tracker = CorruptionTracker::default();

        for (index, ns) in [(0, held_ns.clone()), (1, other_ns.clone())] {
            scheduler.advance_to(index);
            let step = execute_recorded_op(
                &client,
                &server,
                &mut artifacts,
                &mut model,
                &mut coverage,
                &mut s3_tracker,
                &mut corruption_tracker,
                &Op::CreateNamespace {
                    actor: ActorSel::ADMIN,
                    ns,
                    spec: spec.clone(),
                },
                index,
                started,
                None,
                RunMode::Chaos,
                ExecutionPhase::Workload,
                true,
                0,
                None,
                false,
            )
            .await;
            assert!((200..300).contains(&step.status), "{step:?}");
            assert!(step.violations.is_empty(), "{:?}", step.violations);
        }

        let held_op = Op::Upsert {
            actor: ActorSel::ADMIN,
            ns: held_ns.clone(),
            vectors: vec![GenVector {
                id: "held-vector".to_string(),
                values: vec![1.0, 0.0],
                attributes: None,
            }],
        };
        scheduler.advance_to(2);
        let hold = foreground_hold_for_op(Some(&scheduler), &held_op, 2)
            .expect("pinned manifest GET must nominate the held upsert");
        assert_eq!(hold.release_op, 7);
        let pending = match execute_hold_candidate(
            &scheduler,
            hold,
            client.clone(),
            OpExecutionTarget::from(&*server),
            held_op,
            2,
            started,
            None,
            RunMode::Chaos,
            true,
            0,
            None,
            false,
            None,
            &mut model,
        )
        .await
        {
            HoldCandidateOutcome::Held(pending) => pending,
            HoldCandidateOutcome::Completed(_) => {
                panic!("pinned manifest GET upsert completed without parking")
            }
        };
        assert_eq!(
            model.namespaces[&held_ns].indeterminate["held-vector"].reason,
            "held_in_flight"
        );

        for (index, id) in [(3, "other-one"), (4, "other-two")] {
            scheduler.advance_to(index);
            assert!(
                !pending.task.is_finished(),
                "held op released before {index}"
            );
            let step = execute_recorded_op(
                &client,
                &server,
                &mut artifacts,
                &mut model,
                &mut coverage,
                &mut s3_tracker,
                &mut corruption_tracker,
                &Op::Upsert {
                    actor: ActorSel::ADMIN,
                    ns: other_ns.clone(),
                    vectors: vec![GenVector {
                        id: id.to_string(),
                        values: vec![0.0, 1.0],
                        attributes: None,
                    }],
                },
                index,
                started,
                None,
                RunMode::Chaos,
                ExecutionPhase::Workload,
                true,
                0,
                None,
                false,
            )
            .await;
            assert!((200..300).contains(&step.status), "{step:?}");
            assert!(step.violations.is_empty(), "{:?}", step.violations);
        }

        let mut pending = Some(pending);
        let mut drain_ops = VecDeque::from([QuietDrainOp::Generated {
            op: Op::Upsert {
                actor: ActorSel::ADMIN,
                ns: held_ns.clone(),
                vectors: vec![GenVector {
                    id: "deferred-vector".to_string(),
                    values: vec![0.5, 0.5],
                    attributes: None,
                }],
            },
            inject_post_commit_ack_loss: false,
        }]);
        let mut operational_state = OperationalState::default();
        let mut injector = None;
        let mut http_fault_context = None;
        let mut dual_writer_lease_hold = None;
        let mut created_namespaces = vec![held_ns.clone(), other_ns.clone()];
        let mut background_compaction_starts = BTreeMap::new();
        let mut op_index = 5;
        let mut compactions = 0;
        let quiet = QuietPeriod {
            client: &client,
            server: &mut server,
            scheduler: Some(&scheduler),
            test_clock: None,
            injector: &mut injector,
            http_fault_context: &mut http_fault_context,
            chaos: None,
            operational_state: &mut operational_state,
            operational_observer: None,
            pending_held_op: &mut pending,
            dual_writer_lease_hold: &mut dual_writer_lease_hold,
            initial_dual_writer_stale_fencing_token: None,
            artifacts: &mut artifacts,
            model: &mut model,
            coverage: &mut coverage,
            s3_tracker: &mut s3_tracker,
            corruption_tracker: &mut corruption_tracker,
            created_namespaces: &mut created_namespaces,
            background_compaction_starts: &mut background_compaction_starts,
            op_index: &mut op_index,
            compactions: &mut compactions,
            started,
            mutation: None,
            mode: RunMode::Chaos,
            exact_vector_count: false,
            verify: false,
            preserve_recorded_holds: false,
            prefix: &prefix,
            config: &config,
            disk_cache_max_bytes: 100 * 1024 * 1024,
            drain_ops: &mut drain_ops,
        }
        .run()
        .await;
        assert!(quiet.violations.is_empty(), "{:?}", quiet.violations);
        assert_eq!(quiet.drained_ops, 1);
        assert!(pending.is_none());
        assert!(drain_ops.is_empty());
        assert!(model.namespaces[&held_ns].indeterminate.is_empty());
        assert!(model.namespaces[&held_ns].live.contains_key("held-vector"));
        assert!(model.namespaces[&held_ns]
            .live
            .contains_key("deferred-vector"));
        assert_eq!(artifacts.op_count(), 6);
        let recorded = read_ops(&artifacts.dir);
        assert_eq!(
            recorded
                .iter()
                .map(|record| record.index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5]
        );
        assert_eq!(
            recorded[2].execution,
            ExecutionMetadata {
                phase: ExecutionPhase::Workload,
                hold: Some(HeldExecutionMetadata {
                    event_id: "sched-held-manifest-get".to_string(),
                    window_op: 2,
                    scheduled_release_op: Some(7),
                    actual_join_op: 5,
                    release_cause: HoldReleaseCause::Quiesce,
                }),
            }
        );
        assert_eq!(
            recorded[5].execution,
            ExecutionMetadata {
                phase: ExecutionPhase::DeferredDrain,
                hold: None,
            }
        );
        let timeline = scheduler.timeline();
        let hold_timeline = timeline
            .iter()
            .filter(|event| event.event_id == "sched-held-manifest-get")
            .collect::<Vec<_>>();
        assert_eq!(hold_timeline.len(), 2, "{hold_timeline:#?}");
        assert_eq!(hold_timeline[0].op_index, 2);
        assert_eq!(hold_timeline[0].semantics, FaultSemantics::WindowActive);
        assert_eq!(hold_timeline[1].op_index, 2);
        assert_eq!(hold_timeline[1].semantics, FaultSemantics::WindowEnd);
        let quiet_start = timeline
            .iter()
            .position(|event| event.event_id == "quiet-01")
            .unwrap();
        let environment_restored = timeline
            .iter()
            .position(|event| event.event_id == "quiet-02")
            .unwrap();
        let held_release = timeline
            .iter()
            .position(|event| {
                event.event_id == "sched-held-manifest-get"
                    && event.semantics == FaultSemantics::WindowEnd
            })
            .unwrap();
        let release_complete = timeline
            .iter()
            .position(|event| event.event_id == "quiet-03")
            .unwrap();
        assert!(quiet_start < environment_restored, "{timeline:#?}");
        assert!(environment_restored < held_release, "{timeline:#?}");
        assert!(held_release < release_complete, "{timeline:#?}");

        // A graceful primary exit before a second quiet period must take the
        // runner's primary-finished branch, not leave its old runtime behind.
        let _ = server.shutdown_http.send_replace(true);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !server.server_task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("primary server did not finish before quiet-period recovery");

        let mut empty_pending: Option<PendingHeldOp> = None;
        let mut empty_drain_ops = VecDeque::new();
        let recovered_quiet = QuietPeriod {
            client: &client,
            server: &mut server,
            scheduler: None,
            test_clock: None,
            injector: &mut injector,
            http_fault_context: &mut http_fault_context,
            chaos: None,
            operational_state: &mut operational_state,
            operational_observer: None,
            pending_held_op: &mut empty_pending,
            dual_writer_lease_hold: &mut dual_writer_lease_hold,
            initial_dual_writer_stale_fencing_token: None,
            artifacts: &mut artifacts,
            model: &mut model,
            coverage: &mut coverage,
            s3_tracker: &mut s3_tracker,
            corruption_tracker: &mut corruption_tracker,
            created_namespaces: &mut created_namespaces,
            background_compaction_starts: &mut background_compaction_starts,
            op_index: &mut op_index,
            compactions: &mut compactions,
            started,
            mutation: None,
            mode: RunMode::Chaos,
            exact_vector_count: false,
            verify: false,
            preserve_recorded_holds: false,
            prefix: &prefix,
            config: &config,
            disk_cache_max_bytes: 100 * 1024 * 1024,
            drain_ops: &mut empty_drain_ops,
        }
        .run()
        .await;
        assert!(
            recovered_quiet.violations.is_empty(),
            "{:?}",
            recovered_quiet.violations
        );
        assert!(recovered_quiet.timeline.iter().any(|event| {
            event.event_id == "quiet-05"
                && event.recovery.as_deref() == Some("primary restarted and healthy")
        }));

        cleanup_ns(&harness.store, &held_ns).await;
        cleanup_ns(&harness.store, &other_ns).await;
        server.into_inner().shutdown().await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn restart_after_crash_retires_then_replaces_primary() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Crash,
            events: vec![FaultEvent {
                id: "runner-direct-crash-recovery".to_string(),
                start_op: u64::MAX,
                end_op: None,
                boundary: Boundary::Process,
                target: TargetSelector {
                    store_op: Some(StoreOp::Put),
                    key_substring: Some("never-trigger-direct-recovery".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::CrashAt {
                    point: crate::adversarial::faults::process::CrashPoint::ManifestCas,
                    position: TriggerPosition::Pre,
                },
            }],
        });
        let config = deterministic_config();
        let mut server = RestartableFullTestServer::new(
            start_test_server_full(
                store_fault_proxy(&harness.store, scheduler.clone()),
                Some(prefix.clone()),
                config.clone(),
                false,
                None,
            )
            .await,
        );
        let old_base_url = server.base_url.clone();
        let server_store = server.store.clone();
        let client = adversarial_client(&server);
        let controller = scheduler
            .process_controller()
            .expect("crash schedule must own a process controller");
        let mut injector = None;
        let mut http_fault_context = None;
        let model = Model::default();

        scheduler.advance_to(7);
        let recovery = restart_after_crash(
            &mut server,
            &controller,
            &scheduler,
            &mut injector,
            &mut http_fault_context,
            &server_store,
            &harness.store,
            &prefix,
            &config,
            false,
            &client,
            &model,
            7,
            CrashRequest {
                event_id: "runner-direct-crash-recovery".to_string(),
                op_index: 7,
                point: crate::adversarial::faults::process::CrashPoint::ManifestCas,
                position: TriggerPosition::Pre,
                key: format!("{prefix}/namespace/manifest.json"),
            },
        )
        .await;

        assert!(recovery.is_empty(), "{recovery:?}");
        assert_ne!(server.base_url, old_base_url);
        let crash_events = scheduler
            .timeline()
            .into_iter()
            .filter(|event| event.event_id == "runner-direct-crash-recovery")
            .collect::<Vec<_>>();
        assert_eq!(crash_events.len(), 1, "{crash_events:#?}");
        assert_eq!(
            crash_events[0].recovery.as_deref(),
            Some("restart+health-wait")
        );

        drop(http_fault_context.take());
        shutdown_http_fault_injector(&mut injector).await;
        drop(client);
        server.into_inner().shutdown().await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn crash_retirement_releases_an_accepted_list_hold_without_disarming_later_holds() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::SupportedFull,
            events: vec![
                FaultEvent {
                    id: "accepted-ready-list-before-crash".to_string(),
                    start_op: 0,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector {
                        store_op: Some(StoreOp::List),
                        key_substring: Some("__healthcheck__".to_string()),
                        ..TargetSelector::default()
                    },
                    kind: FaultKind::HoldCall { for_ops: 100 },
                },
                FaultEvent {
                    id: "ready-list-after-restart".to_string(),
                    start_op: 10,
                    end_op: None,
                    boundary: Boundary::ObjectStore,
                    target: TargetSelector {
                        store_op: Some(StoreOp::List),
                        key_substring: Some("__healthcheck__".to_string()),
                        ..TargetSelector::default()
                    },
                    kind: FaultKind::HoldCall { for_ops: 5 },
                },
                FaultEvent {
                    id: "list-hold-crash".to_string(),
                    start_op: u64::MAX,
                    end_op: None,
                    boundary: Boundary::Process,
                    target: TargetSelector {
                        store_op: Some(StoreOp::Put),
                        key_substring: Some("never-trigger-list-hold-crash".to_string()),
                        ..TargetSelector::default()
                    },
                    kind: FaultKind::CrashAt {
                        point: crate::adversarial::faults::process::CrashPoint::StagingDrop,
                        position: TriggerPosition::Pre,
                    },
                },
            ],
        });
        let store = store_fault_proxy(&harness.store, scheduler.clone());
        let config = deterministic_config();
        let mut server = RestartableFullTestServer::new(
            start_test_server_full(
                store.clone(),
                Some(prefix.clone()),
                config.clone(),
                false,
                None,
            )
            .await,
        );
        let client = adversarial_client(&server);
        let controller = scheduler
            .process_controller()
            .expect("crash schedule must own a process controller");
        let mut injector = None;
        let mut http_fault_context = None;
        let model = Model::default();

        scheduler.advance_to(0);
        let first_client = client.clone();
        let first_ready_url = format!("{}/readyz", server.base_url);
        let first_scheduler = scheduler.clone();
        let first_ready = tokio::spawn(async move {
            first_scheduler
                .with_armed_hold(
                    "accepted-ready-list-before-crash".to_string(),
                    first_client.get(first_ready_url).send(),
                )
                .await
        });
        scheduler
            .wait_for_hold_window_active("accepted-ready-list-before-crash", 0)
            .await;
        assert!(!first_ready.is_finished());

        let recovery = tokio::time::timeout(
            Duration::from_secs(1),
            restart_after_crash(
                &mut server,
                &controller,
                &scheduler,
                &mut injector,
                &mut http_fault_context,
                &store,
                &harness.store,
                &prefix,
                &config,
                false,
                &client,
                &model,
                0,
                CrashRequest {
                    event_id: "list-hold-crash".to_string(),
                    op_index: 0,
                    point: crate::adversarial::faults::process::CrashPoint::StagingDrop,
                    position: TriggerPosition::Pre,
                    key: format!("{prefix}/segments/staged"),
                },
            ),
        )
        .await
        .expect("crash retirement waited on an accepted LIST hold");
        assert!(recovery.is_empty(), "{recovery:?}");
        first_ready
            .await
            .expect("accepted readiness task failed while joining")
            .expect("accepted readiness request failed during retirement");

        scheduler.advance_to(10);
        let second_client = client.clone();
        let second_ready_url = format!("{}/readyz", server.base_url);
        let second_scheduler = scheduler.clone();
        let second_ready = tokio::spawn(async move {
            second_scheduler
                .with_armed_hold(
                    "ready-list-after-restart".to_string(),
                    second_client.get(second_ready_url).send(),
                )
                .await
        });
        scheduler
            .wait_for_hold_window_active("ready-list-after-restart", 10)
            .await;
        assert!(
            !second_ready.is_finished(),
            "retirement release permanently disarmed a later scheduled hold"
        );
        scheduler.advance_to(15);
        let response = second_ready
            .await
            .expect("post-restart readiness task failed while joining")
            .expect("post-restart readiness request failed");
        assert_eq!(response.status(), StatusCode::OK);

        drop(http_fault_context.take());
        shutdown_http_fault_injector(&mut injector).await;
        drop(client);
        server.into_inner().shutdown().await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn durable_audit_evidence_accepts_verified_terminal_seal() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let server = start_test_server_full(
            harness.store.clone(),
            Some(prefix.clone()),
            deterministic_config(),
            false,
            None,
        )
        .await;
        let client = client_with_bearer(&server.admin_bearer);
        let _ = crate::common::server::create_ns_api(&client, &server.base_url, 2).await;
        let audit_store = server.store.clone();
        drop(client);
        server.shutdown().await;

        let evidence = collect_durable_audit_evidence(&audit_store, &prefix).await;

        assert!(
            !evidence.verified_terminal_streams.is_empty(),
            "graceful audit shutdown must produce one verified terminal stream"
        );
        assert!(
            !evidence.records.is_empty(),
            "durable audit evidence must retain the namespace request"
        );
        harness.cleanup().await;
    }

    async fn durable_audit_evidence_panic_message(store: ZeppelinStore, prefix: String) -> String {
        let error = tokio::spawn(async move {
            let _ = collect_durable_audit_evidence(&store, &prefix).await;
        })
        .await
        .expect_err("malformed audit evidence must panic loudly");
        let payload = error.into_panic();
        if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else {
            "non-string panic payload".to_string()
        }
    }

    #[tokio::test]
    async fn durable_audit_evidence_rejects_malformed_and_unknown_terminal_seals() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let node_id = format!("test-node-{prefix}-malformed");
        let malformed_key = format!("_audit/2026-07-16/{node_id}/00000000000000000000000000.jsonl");
        let malformed = serde_json::to_vec(&json!({
            "format": "zeppelin_audit_terminal_seal_v1",
            "day": "2026-07-16",
            "node_id": node_id.clone(),
            "last_hash": null,
            "record_count": 0,
            "unexpected": true,
        }))
        .expect("malformed terminal seal fixture must encode");
        harness
            .store
            .put(&malformed_key, Bytes::from(malformed))
            .await
            .expect("malformed terminal seal fixture write must succeed");

        let malformed_message =
            durable_audit_evidence_panic_message(harness.store.clone(), prefix.clone()).await;
        assert!(
            malformed_message.contains("invalid terminal audit seal in"),
            "{malformed_message}"
        );
        harness
            .store
            .delete(&malformed_key)
            .await
            .expect("malformed terminal seal fixture cleanup must succeed");

        let unknown_format_key =
            format!("_audit/2026-07-16/{node_id}/00000000000000000000000001.jsonl");
        let unknown_format = serde_json::to_vec(&json!({
            "format": "unexpected_terminal_seal_format",
            "day": "2026-07-16",
            "node_id": node_id.clone(),
            "last_hash": null,
            "record_count": 0,
        }))
        .expect("unknown-format terminal seal fixture must encode");
        harness
            .store
            .put(&unknown_format_key, Bytes::from(unknown_format))
            .await
            .expect("unknown-format terminal seal fixture write must succeed");

        let unknown_format_message =
            durable_audit_evidence_panic_message(harness.store.clone(), prefix).await;
        assert!(
            unknown_format_message.contains("unexpected terminal audit seal format"),
            "{unknown_format_message}"
        );
        harness
            .store
            .delete(&unknown_format_key)
            .await
            .expect("unknown-format terminal seal fixture cleanup must succeed");
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn replay_preserves_terminal_join_when_hold_candidate_completes_early() {
        let source_root = tempfile::TempDir::new().unwrap();
        let source_env = RunnerEnv {
            seconds: 30,
            seeds: vec![61],
            max_ops: Some(4),
            artifacts: source_root.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Chaos,
            profile: Some(FaultProfile::Sched),
            env_echo: BTreeMap::new(),
        };
        let source_artifacts = RunArtifacts::create(&source_env);
        let held_ns = "source-prefix-adv-61-held".to_string();
        let other_ns = "source-prefix-adv-61-other".to_string();
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let config = deterministic_config();
        let schedule = FaultSchedule {
            profile: FaultProfile::Sched,
            events: vec![FaultEvent {
                id: "sched-terminal-hold".to_string(),
                start_op: 2,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(StoreOp::Get),
                    key_substring: Some("never-matches-recorded-terminal-hold".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::HoldCall { for_ops: 4 },
            }],
        };
        let specs = BTreeMap::from([
            (held_ns.clone(), spec.clone()),
            (other_ns.clone(), spec.clone()),
        ]);
        let mut source_seed = source_artifacts.seed(
            61,
            &config,
            &specs,
            RunMode::Chaos,
            None,
            None,
            None,
            Some(&schedule),
        );
        let source_records = vec![
            OpRecord {
                index: 0,
                wall_ms: 0,
                op: Op::CreateNamespace {
                    actor: ActorSel::ADMIN,
                    ns: held_ns.clone(),
                    spec: spec.clone(),
                },
                method: "POST".to_string(),
                path: "/v1/namespaces".to_string(),
                status: StatusCode::CREATED.as_u16(),
                response: json!({}),
                outcome: "applied".to_string(),
                target_node: 0,
                execution: ExecutionMetadata::workload(),
                gen_after: Some(1),
                duration_ms: 0,
                violations: Vec::new(),
            },
            OpRecord {
                index: 1,
                wall_ms: 1,
                op: Op::CreateNamespace {
                    actor: ActorSel::ADMIN,
                    ns: other_ns.clone(),
                    spec,
                },
                method: "POST".to_string(),
                path: "/v1/namespaces".to_string(),
                status: StatusCode::CREATED.as_u16(),
                response: json!({}),
                outcome: "applied".to_string(),
                target_node: 0,
                execution: ExecutionMetadata::workload(),
                gen_after: Some(1),
                duration_ms: 0,
                violations: Vec::new(),
            },
            OpRecord {
                index: 2,
                wall_ms: 2,
                op: Op::Upsert {
                    actor: ActorSel::ADMIN,
                    ns: held_ns,
                    vectors: vec![GenVector {
                        id: "terminal-held-vector".to_string(),
                        values: vec![1.0, 0.0],
                        attributes: None,
                    }],
                },
                method: "POST".to_string(),
                path: "/vectors".to_string(),
                status: StatusCode::OK.as_u16(),
                response: json!({}),
                outcome: "applied".to_string(),
                target_node: 0,
                execution: ExecutionMetadata {
                    phase: ExecutionPhase::Workload,
                    hold: Some(HeldExecutionMetadata {
                        event_id: "sched-terminal-hold".to_string(),
                        window_op: 2,
                        scheduled_release_op: Some(6),
                        actual_join_op: 4,
                        release_cause: HoldReleaseCause::Quiesce,
                    }),
                },
                gen_after: Some(2),
                duration_ms: 0,
                violations: Vec::new(),
            },
            OpRecord {
                index: 3,
                wall_ms: 3,
                op: Op::GetNamespace {
                    actor: ActorSel::ADMIN,
                    ns: other_ns.clone(),
                },
                method: "GET".to_string(),
                path: "/v1/namespaces/other".to_string(),
                status: StatusCode::OK.as_u16(),
                response: json!({}),
                outcome: "applied".to_string(),
                target_node: 0,
                execution: ExecutionMetadata::workload(),
                gen_after: None,
                duration_ms: 0,
                violations: Vec::new(),
            },
            OpRecord {
                index: 4,
                wall_ms: 4,
                op: Op::GetNamespace {
                    actor: ActorSel::ADMIN,
                    ns: other_ns,
                },
                method: "GET".to_string(),
                path: "/v1/namespaces/other".to_string(),
                status: StatusCode::OK.as_u16(),
                response: json!({}),
                outcome: "applied".to_string(),
                target_node: 0,
                execution: ExecutionMetadata {
                    phase: ExecutionPhase::DeferredDrain,
                    hold: None,
                },
                gen_after: None,
                duration_ms: 0,
                violations: Vec::new(),
            },
        ];
        for record in &source_records {
            source_seed.write_op(record);
        }
        let source_dir = source_seed.dir.clone();
        drop(source_seed);

        let replay_root = tempfile::TempDir::new().unwrap();
        let replay_env = RunnerEnv {
            artifacts: replay_root.path().to_path_buf(),
            max_ops: None,
            profile: None,
            ..source_env
        };
        let outcome = Box::pin(run_replay(&replay_env, &source_dir)).await;
        assert!(!outcome.failed, "{:?}", outcome.violations);

        let replay_dir = fs::read_dir(replay_root.path())
            .unwrap()
            .find_map(|entry| {
                let path = entry.unwrap().path().join("seed-61");
                path.join("timeline.jsonl").exists().then_some(path)
            })
            .expect("terminal-hold replay did not write artifacts");
        let replay_records = read_ops(&replay_dir);
        let (_, replay_workload) = replay_workload_records(&replay_records);
        assert_eq!(replay_workload.len(), 5);
        assert_eq!(
            normalized_op_execution_structure(
                &replay_workload,
                &recorded_namespace_prefix(61, &replay_seed_config(&replay_dir).namespace_specs,)
            ),
            normalized_op_execution_structure(&source_records, "source-prefix")
        );
        assert_eq!(
            replay_workload[2].execution.hold,
            source_records[2].execution.hold
        );
        assert_eq!(
            replay_workload[2].gen_after,
            Some(2),
            "the terminal-held mutation must advance the authoritative manifest exactly once"
        );
        assert!(replay_records.len() > 5);
        assert!(replay_records[5..]
            .iter()
            .all(|record| record.execution.phase == ExecutionPhase::Quiescence));

        let timeline = fs::read_to_string(replay_dir.join("timeline.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<TimelineEvent>(line).unwrap())
            .collect::<Vec<_>>();
        let first_quiet = timeline
            .iter()
            .find(|event| event.event_id == "quiet-01")
            .expect("terminal-hold replay omitted canonical quiet period");
        assert_eq!(first_quiet.op_index, 4);
        let quiet_start = timeline
            .iter()
            .position(|event| event.event_id == "quiet-01")
            .unwrap();
        let environment_restored = timeline
            .iter()
            .position(|event| event.event_id == "quiet-02")
            .unwrap();
        assert!(
            timeline
                .iter()
                .all(|event| event.event_id != "sched-terminal-hold"),
            "the non-matching store selector must force the recorded hold's \
             early-completion replay path: {timeline:#?}"
        );
        let release_complete = timeline
            .iter()
            .position(|event| event.event_id == "quiet-03")
            .unwrap();
        assert!(quiet_start < environment_restored, "{timeline:#?}");
        assert!(environment_restored < release_complete, "{timeline:#?}");
    }

    #[tokio::test]
    async fn sched_replay_preserves_recorded_hold_and_exact_trace_structure() {
        fn normalized_structure(records: &[OpRecord], prefix: &str) -> Vec<serde_json::Value> {
            records
                .iter()
                .map(|record| {
                    let mut op = serde_json::to_value(&record.op).unwrap();
                    rewrite_json_strings(&mut op, prefix, "<run-prefix>");
                    json!({
                        "index": record.index,
                        "op": op,
                        "target_node": record.target_node,
                        "execution": record.execution,
                    })
                })
                .collect()
        }

        fn phase_indices(records: &[OpRecord], phase: ExecutionPhase) -> Vec<u64> {
            records
                .iter()
                .filter(|record| record.execution.phase == phase)
                .map(|record| record.index)
                .collect()
        }

        fn normalized_timeline(path: &Path, prefix: &str) -> Vec<serde_json::Value> {
            fs::read_to_string(path.join("timeline.jsonl"))
                .unwrap()
                .lines()
                .map(|line| {
                    let mut event = serde_json::from_str::<serde_json::Value>(line).unwrap();
                    event
                        .as_object_mut()
                        .expect("timeline event must be an object")
                        .remove("wall_ms");
                    rewrite_json_strings(&mut event, prefix, "<run-prefix>");
                    event
                })
                .collect()
        }

        let source_root = tempfile::TempDir::new().unwrap();
        let source_env = RunnerEnv {
            seconds: 60,
            seeds: vec![3],
            max_ops: Some(20),
            artifacts: source_root.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Chaos,
            profile: Some(FaultProfile::Sched),
            env_echo: BTreeMap::new(),
        };
        let source_artifacts = RunArtifacts::create(&source_env);
        let source_dir = source_artifacts.root().join("seed-3");
        let source_outcome = Box::pin(run_seed(
            &source_env,
            &source_artifacts,
            3,
            Instant::now() + Duration::from_secs(60),
            None,
            None,
        ))
        .await;
        assert!(!source_outcome.failed, "{:?}", source_outcome.violations);

        let source_records = read_ops(&source_dir);
        // The warm delete at op 10 now validates its writer memo through the
        // manifest CAS and issues no manifest GET. The pinned HoldCall remains
        // pending until the fetch at op 11 performs the next matching GET.
        let expected_workload_indices = (0..12).collect::<Vec<_>>();
        let expected_drain_indices = (12..20).collect::<Vec<_>>();
        assert_eq!(
            source_records
                .iter()
                .filter(|record| record.execution.phase == ExecutionPhase::Workload)
                .count(),
            12
        );
        assert_eq!(
            source_records
                .iter()
                .filter(|record| record.execution.phase == ExecutionPhase::DeferredDrain)
                .count(),
            8
        );
        assert_eq!(
            phase_indices(&source_records, ExecutionPhase::Workload),
            expected_workload_indices
        );
        assert_eq!(
            phase_indices(&source_records, ExecutionPhase::DeferredDrain),
            expected_drain_indices
        );
        assert!(source_records
            .iter()
            .any(|record| record.execution.phase == ExecutionPhase::Quiescence));
        let held = source_records
            .iter()
            .find(|record| record.execution.hold.is_some())
            .expect("pinned seed 3 must record one held foreground op");
        assert_eq!(held.index, 11);
        assert_eq!(
            held.execution.hold,
            Some(HeldExecutionMetadata {
                event_id: "sched-01".to_string(),
                window_op: 11,
                scheduled_release_op: Some(15),
                actual_join_op: 12,
                release_cause: HoldReleaseCause::Quiesce,
            })
        );

        let replay_root = tempfile::TempDir::new().unwrap();
        let replay_env = RunnerEnv {
            artifacts: replay_root.path().to_path_buf(),
            max_ops: None,
            profile: None,
            ..source_env.clone()
        };
        let replay_outcome = Box::pin(run_replay(&replay_env, &source_dir)).await;
        assert!(!replay_outcome.failed, "{:?}", replay_outcome.violations);
        let replay_dirs = fs::read_dir(replay_root.path())
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path().join("seed-3");
                (path.join("config.json").exists() && path.join("ops.jsonl").exists())
                    .then_some(path)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            replay_dirs.len(),
            1,
            "replay must emit exactly one seed artifact directory: {replay_dirs:#?}"
        );
        let replay_dir = &replay_dirs[0];
        let replay_records = read_ops(replay_dir);
        assert_eq!(replay_records.len(), source_records.len());
        assert_eq!(
            phase_indices(&replay_records, ExecutionPhase::Workload),
            expected_workload_indices
        );
        assert_eq!(
            phase_indices(&replay_records, ExecutionPhase::DeferredDrain),
            expected_drain_indices
        );
        let replay_held = replay_records
            .iter()
            .find(|record| record.execution.hold.is_some())
            .expect("replay must preserve the held foreground op");
        assert_eq!(replay_held.index, held.index);
        assert_eq!(
            (
                replay_held.status,
                replay_held.outcome.as_str(),
                replay_held.gen_after
            ),
            (held.status, held.outcome.as_str(), held.gen_after),
            "replayed held op must preserve its recorded completion outcome"
        );

        let source_prefix =
            recorded_namespace_prefix(3, &replay_seed_config(&source_dir).namespace_specs);
        let replay_prefix =
            recorded_namespace_prefix(3, &replay_seed_config(replay_dir).namespace_specs);
        assert_eq!(
            normalized_structure(&replay_records, &replay_prefix),
            normalized_structure(&source_records, &source_prefix)
        );
        assert_eq!(
            normalized_timeline(replay_dir, &replay_prefix),
            normalized_timeline(&source_dir, &source_prefix),
            "replay must preserve the exact normalized fault timeline"
        );
    }

    #[tokio::test]
    async fn duplicate_retry_is_idempotent() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let ns = format!("{prefix}-duplicate-retry");
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let mut server = start_test_server_full(
            harness.store.clone(),
            Some(prefix),
            deterministic_config(),
            false,
            None,
        )
        .await;
        let client = adversarial_client(&server);
        let create = client
            .post(format!("{}/v1/namespaces", server.base_url))
            .json(&spec.create_body(&ns))
            .send()
            .await
            .unwrap();
        assert!(create.status().is_success());

        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
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
                kind: FaultKind::DuplicateRetry,
            }],
        });
        let _ = scheduler.advance_to(0);
        let injector = start_http_fault_injector(&server.base_url).await;
        let context = HttpFaultContext {
            scheduler: scheduler.clone(),
            injector: injector.request_handle(),
            bookkeeping_store: harness.store.clone(),
            direct_base_url: server.base_url.clone(),
            proxy_base_url: injector.base_url(),
        };
        let outcome = HTTP_FAULT_CONTEXT
            .scope(
                Some(context),
                REQUEST_AMBIGUITY_ALLOWED.scope(
                    true,
                    REQUEST_IS_MUTATION.scope(
                        true,
                        request_outcome(
                            &client,
                            Method::POST,
                            &format!("{}/v1/namespaces/{ns}/vectors", server.base_url),
                            Some(json!({
                                "vectors": [{
                                    "id": "one",
                                    "values": [1.0, 0.0]
                                }]
                            })),
                            true,
                        ),
                    ),
                ),
            )
            .await;
        assert!(matches!(outcome, OpOutcome::Applied { .. }));

        let response = client
            .post(format!(
                "{}/v1/namespaces/{ns}/vectors/get",
                server.base_url
            ))
            .json(&json!({
                "ids": ["one"],
                "include_vector": true,
                "consistency": "strong"
            }))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let body = response.json::<serde_json::Value>().await.unwrap();
        assert_eq!(body["results"].as_array().unwrap().len(), 1);
        assert_eq!(body["results"][0]["id"], "one");
        assert_eq!(scheduler.timeline().len(), 1);
        assert!(scheduler.timeline()[0].action.contains("DuplicateRetry"));

        Arc::try_unwrap(injector).unwrap().shutdown().await;
        stop_background_compaction(&mut server).await;
        cleanup_ns(&harness.store, &ns).await;
        harness.cleanup().await;
    }

    #[tokio::test]
    async fn copy_source_vanish_clone_fails_loudly_and_retains_safe_target() {
        let harness = TestHarness::new().await;
        let prefix = harness.prefix.clone();
        let source = format!("{prefix}-copy-vanish-source");
        let target = format!("{prefix}-copy-vanish-target");
        let spec = NamespaceSpec {
            dims: 2,
            metric: DistanceMetric::Cosine,
            quantization: QuantizationType::None,
            num_centroids: 4,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: None,
        };
        let setup_server = start_test_server_full(
            harness.store.clone(),
            Some(prefix.clone()),
            deterministic_config(),
            false,
            None,
        )
        .await;
        let admin_bearer = setup_server.admin_bearer.clone();
        let client = adversarial_client(&setup_server);
        let create = client
            .post(format!("{}/v1/namespaces", setup_server.base_url))
            .json(&spec.create_body(&source))
            .send()
            .await
            .unwrap();
        assert!(create.status().is_success());
        let upsert = client
            .post(format!(
                "{}/v1/namespaces/{source}/vectors",
                setup_server.base_url
            ))
            .json(&json!({
                "vectors": [{ "id": "one", "values": [1.0, 0.0] }]
            }))
            .send()
            .await
            .unwrap();
        assert!(upsert.status().is_success());
        setup_server.compactor.compact(&source).await.unwrap();
        let generation = Manifest::read(&harness.store, &source)
            .await
            .unwrap()
            .expect("source manifest must exist")
            .version();
        setup_server.shutdown().await;

        let scheduler = FaultScheduler::from_schedule(FaultSchedule {
            profile: FaultProfile::Semantic,
            events: vec![FaultEvent {
                id: "semantic-copy-source-vanish".to_string(),
                start_op: 0,
                end_op: None,
                boundary: Boundary::ObjectStore,
                target: TargetSelector {
                    store_op: Some(super::super::chaos::StoreOp::Copy),
                    key_substring: Some("segments/".to_string()),
                    ..TargetSelector::default()
                },
                kind: FaultKind::CopySourceVanish,
            }],
        });
        let faulted_store = store_fault_proxy(&harness.store, scheduler.clone());
        let server = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
            faulted_store,
            Some(prefix),
            deterministic_config(),
            false,
            None,
            100 * 1024 * 1024,
            &admin_bearer,
        )
        .await;

        let clone = client
            .post(format!("{}/v1/namespaces/{source}/clone", server.base_url))
            .json(&json!({
                "target": target,
                "as_of": generation.to_string(),
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(clone.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let clone_body = clone.json::<serde_json::Value>().await.unwrap();
        assert_eq!(clone_body["code"], "STORAGE_ERROR");
        assert_eq!(clone_body["status"], 500);
        assert!(clone_body["request_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()));
        assert_eq!(scheduler.timeline().len(), 1);
        assert_eq!(scheduler.timeline()[0].observed, ObservedResult::Corrupted);

        server.compactor.compact(&source).await.unwrap();
        let source_manifest = Manifest::read(&harness.store, &source)
            .await
            .unwrap()
            .expect("source manifest must survive failed clone");
        let quiescent = s3_oracle::check_quiescent_namespace(
            &harness.store,
            &source,
            1,
            &json!({
                "ready": true,
                "uncompacted_fragments": 0,
                "manifest_generation": source_manifest.version()
            }),
            1,
        )
        .await;
        assert!(quiescent.is_empty(), "{quiescent:#?}");
        assert_eq!(source_manifest.vector_count(), 1);
        let target_status = client
            .get(format!("{}/v1/namespaces/{target}", server.base_url))
            .send()
            .await
            .unwrap();
        assert!(target_status.status().is_success());
        let target_status = target_status.json::<serde_json::Value>().await.unwrap();
        assert_eq!(target_status["vector_count"], 0);

        let target_fetch = client
            .post(format!(
                "{}/v1/namespaces/{target}/vectors/get",
                server.base_url
            ))
            .json(&json!({
                "ids": ["one"],
                "include_vector": true,
                "consistency": "strong"
            }))
            .send()
            .await
            .unwrap();
        assert!(target_fetch.status().is_success());
        let target_fetch = target_fetch.json::<serde_json::Value>().await.unwrap();
        assert!(target_fetch["results"].as_array().unwrap().is_empty());
        assert_eq!(target_fetch["missing"], json!(["one"]));

        let fetch = client
            .post(format!(
                "{}/v1/namespaces/{source}/vectors/get",
                server.base_url
            ))
            .json(&json!({
                "ids": ["one"],
                "include_vector": true,
                "consistency": "strong"
            }))
            .send()
            .await
            .unwrap();
        assert!(fetch.status().is_success());
        let fetch_body = fetch.json::<serde_json::Value>().await.unwrap();
        assert_eq!(fetch_body["results"][0]["id"], "one");

        server.shutdown().await;
        cleanup_ns(&harness.store, &source).await;
        cleanup_ns(&harness.store, &target).await;
        harness.cleanup().await;
    }
}
