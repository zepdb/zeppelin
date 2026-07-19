//! Schedules namespace maintenance and wraps compaction in lease ownership.
//!
//! This module is the long-running orchestration layer around
//! [`Compactor`][crate::compaction::Compactor]. It does not build IVF indexes,
//! merge WAL rows, or publish manifests itself. Instead, it discovers namespaces,
//! resumes durable namespace deletion, runs storage garbage collection, asks the
//! compactor whether work is due, and gives each actual compaction a
//! per-namespace lease and renewal heartbeat.
//!
//! Production enters through
//! [`start_compaction_thread`][crate::compaction::background::start_compaction_thread],
//! which creates a
//! dedicated Tokio runtime so synchronous index training and this runtime's
//! blocking pool do not occupy query-runtime workers. Tests may call
//! [`compaction_loop`][crate::compaction::background::compaction_loop] directly
//! on their runtime. The HTTP manual-compaction handler enters through
//! [`run_compaction_with_reserved_lease`][crate::compaction::background::run_compaction_with_reserved_lease]
//! after reserving lifecycle admission and acquiring a lease; the periodic loop
//! uses
//! [`compact_namespace_under_lease_with_lifecycle`][crate::compaction::background::compact_namespace_under_lease_with_lifecycle]
//! to reserve, acquire, and run in one operation.
//!
//! S3 or MinIO remains authoritative throughout this file. The namespace
//! registry is only a discovery hint, the periodic loop invalidates its manifest
//! cache entry after authoritative publication or completed deletion, and
//! post-compaction index warming reads the manifest from object storage before
//! selecting an immutable key. A warm-cache
//! failure never changes query semantics: a later query still fetches the key
//! named by its manifest and reports a missing or corrupt object as an error.
//!
//! ## Reading map
//!
//! 1. Start with
//!    [`CompactionThreadOptions`][crate::compaction::background::CompactionThreadOptions]
//!    and [`CompactionLoopOptions`][crate::compaction::background::CompactionLoopOptions]
//!    to see which boot-time values are moved into the scheduler.
//! 2. Read
//!    [`compact_namespace_under_lease_with_lifecycle`][crate::compaction::background::compact_namespace_under_lease_with_lifecycle]
//!    and [`run_compaction_with_reserved_lease`][crate::compaction::background::run_compaction_with_reserved_lease]
//!    for lifecycle admission, lease acquisition, heartbeat, fenced commit, and
//!    best-effort release.
//! 3. Read
//!    [`start_compaction_thread`][crate::compaction::background::start_compaction_thread]
//!    for the OS-thread and Tokio-runtime
//!    boundary.
//! 4. Finish with
//!    [`compaction_loop`][crate::compaction::background::compaction_loop] for
//!    namespace discovery, deletion, GC,
//!    trigger evaluation, metrics, health recording, invalidation, and warming.
//!
//! ## Supervisor and maintenance flow
//!
//! ```text
//! server startup
//!      |
//!      v
//! compaction-runtime OS thread
//!      |
//!      +--> dedicated Tokio workers
//!              |
//!              +-- sleep until interval -- shutdown change --> return
//!              |
//!              v
//!        discover namespaces
//!        (fresh on tick 1 and every 12th tick;
//!         cached registry hint between refreshes)
//!              |
//!              +-- Deleting --> bounded delete continuation --> next namespace
//!              |
//!              v
//!             GC
//!              |
//!              | GC failure is logged; compaction check still runs
//!              v
//!        should_compact?
//!              |
//!              +-- false --> next namespace
//!              +-- error --> metric + health failure --> next namespace
//!              v
//! acquire lease --> heartbeat renews concurrently --> build immutable segment
//!              |                                      |
//!              | lease lost                           | fencing + manifest CAS
//!              +----------------> no stale commit     v
//!                                             manifest is authoritative
//!                                                      |
//!                              record health + invalidate manifest cache
//!                                                      |
//!                                         spawn best-effort metadata warm
//! ```
//!
//! Namespace work is sequential within a tick. A lease heartbeat and an index
//! warm are separate Tokio tasks, but the loop does not compact multiple
//! namespaces concurrently. A shutdown notification is observed only at the
//! next top-level `select!`; it does not interrupt deletion, GC, a compaction,
//! or the rest of the current namespace scan.
//!
//! ## Configuration and retry boundaries
//!
//! [`crate::config::CompactionConfig`], [`GcConfig`][crate::config::GcConfig],
//! the worker count, and the lease duration are startup snapshots. The runtime
//! query-knob API does not mutate them. Per-namespace metadata is refreshed by
//! authoritative namespace discovery on the first maintenance tick and every
//! twelfth tick; same-process metadata updates also refresh the manager's local
//! registry. Full-text settings passed into a background compaction therefore
//! come from that discovery snapshot, while the compactor independently reloads
//! its current manifest and effective index settings before building.
//!
//! Individual namespace failures are logged and retried by a later tick rather
//! than terminating the supervisor. That is failure isolation, not silent data
//! fallback: failed publication never becomes visible, and cached namespace
//! entries are used only to find possible work whose storage operations still
//! revalidate authoritative state.
//!
//! ## Rust concepts used here
//!
//! [`Arc`][std::sync::Arc] gives the OS thread, loop, heartbeat, and warm task
//! shared ownership of clients without copying the clients themselves. In Java,
//! this resembles sharing references to thread-safe services; in C it requires
//! explicit reference counting and a lifetime protocol. Rust releases the value
//! after the final `Arc` owner is dropped.
//!
//! The heartbeat and compactor share an
//! [`AtomicBool`][std::sync::atomic::AtomicBool] rather than a lock. A
//! sequentially consistent store/load communicates only “publication is now
//! forbidden”; it does not cancel CPU or I/O already in progress. Owned strings,
//! leases, and client handles are moved into spawned `async move` tasks so those
//! futures cannot borrow stack frames that may return first. Tokio
//! [`JoinHandle`][tokio::task::JoinHandle] cancellation is explicit: calling
//! `abort` requests cancellation, while merely dropping a handle detaches the
//! task.
//! The coordinator itself holds no mutex guard across object-store `.await`
//! points; `Arc` expresses shared lifetime, not automatic locking.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use thiserror::Error;
use tracing::{debug, error, info, warn};

use tokio::task::JoinHandle;

use crate::cache::manifest_cache::ManifestCache;
use crate::cache::DiskCache;
use crate::config::{Config, GcConfig};
use crate::error::{Result, ZeppelinError};
use crate::fts::FtsFieldConfig;
use crate::namespace::branching::activation::BranchActivationRecovery;
use crate::namespace::branching::deletion::DeletionGovernance;
use crate::namespace::branching::NamespaceDeleteOutcome;
use crate::namespace::graph::NamespaceGraph;
use crate::namespace::manager::{NamespaceMetadata, NamespaceState};
use crate::namespace::{NamespaceId, NamespaceManager};
use crate::security::SecurityKernel;
use crate::storage::ZeppelinStore;
use crate::wal::FragmentCachePolicy;
use crate::wal::Lease;
use crate::wal::LeaseManager;
use crate::wal::Manifest;

use super::gc::{GcNamespaceIncarnation, GcRunner};
use super::{CompactionResult, Compactor};

const NAMESPACE_DISCOVERY_REFRESH_TICKS: u64 = 12;

/// Failure while admitting or retiring leased-compaction heartbeat work.
#[derive(Debug, Error)]
pub enum CompactionLifecycleError {
    /// Crash retirement closed admission before a run could register.
    #[error("compaction lifecycle retired before leased compaction registration")]
    RetiredBeforeRegistration,

    /// Crash retirement won after reservation but before heartbeat ownership.
    #[error("compaction lifecycle retired during heartbeat registration")]
    RetiredDuringHeartbeatRegistration,

    /// A heartbeat task failed for a reason other than requested cancellation.
    #[error("compaction heartbeat failed during crash retirement: {failures}")]
    HeartbeatFailed {
        /// Joined task failures, preserved for operator diagnostics.
        failures: String,
    },
}

/// Process-local owner for leased-compaction heartbeats.
///
/// A heartbeat is a child task of a periodic or manual compaction run, but it
/// owns the capability to keep that run's S3 lease alive. It must therefore be
/// retired independently of the outer task: aborting an outer Tokio task only
/// drops its local handle, which would otherwise detach the heartbeat.
///
/// One application instance shares one lifecycle between its periodic loop and
/// manual HTTP compactions. Crash retirement closes admission, marks every
/// active run as publication-aborted, and aborts plus joins every heartbeat
/// before a replacement is allowed to begin. Normal runs stop and join their
/// own heartbeat before best-effort lease release.
#[derive(Clone)]
pub struct CompactionLifecycle {
    inner: Arc<Mutex<CompactionLifecycleState>>,
}

struct CompactionLifecycleState {
    accepting: bool,
    next_run_id: u64,
    runs: HashMap<u64, ActiveCompactionState>,
}

struct ActiveCompactionState {
    publication_aborted: Arc<AtomicBool>,
    heartbeat: Option<JoinHandle<()>>,
    heartbeat_completion: Option<HeartbeatCompletion>,
}

/// One registered compaction run.
///
/// This stays private to the orchestration module so callers cannot detach a
/// heartbeat from the lifecycle which must retire it.
struct ActiveCompaction {
    lifecycle: CompactionLifecycle,
    run_id: u64,
    publication_aborted: Arc<AtomicBool>,
    finished: bool,
}

/// Admission reservation for one future leased compaction.
///
/// Periodic and manual callers reserve before acquiring S3 ownership. A crash
/// close can therefore mark the reservation before acquisition completes; the
/// caller then releases any lease it acquired rather than creating an
/// untracked heartbeat or stranding a live lease across replacement.
pub struct CompactionRunReservation {
    active: Option<ActiveCompaction>,
}

/// Completion latch retained even while a normal run is joining its heartbeat.
///
/// Crash retirement may race a normal completion after that completion has
/// removed the `JoinHandle` from the registry. The latch prevents crash
/// retirement from returning until that in-flight join's task has actually
/// stopped; merely knowing that it was asked to abort is not sufficient.
#[derive(Clone)]
struct HeartbeatCompletion {
    exited: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

struct HeartbeatExitGuard(HeartbeatCompletion);

impl HeartbeatCompletion {
    fn new() -> Self {
        Self {
            exited: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    async fn wait_until_exited(&self) {
        loop {
            let notified = self.notify.notified();
            if self.exited.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for HeartbeatExitGuard {
    fn drop(&mut self) {
        self.0.exited.store(true, Ordering::SeqCst);
        self.0.notify.notify_waiters();
    }
}

impl CompactionLifecycle {
    /// Construct an accepting lifecycle with no active lease heartbeats.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CompactionLifecycleState {
                accepting: true,
                next_run_id: 0,
                runs: HashMap::new(),
            })),
        }
    }

    /// Close compaction admission, abort publication, and join every heartbeat.
    ///
    /// Crash retirement must call this before it aborts its periodic-loop or
    /// request-task owners. Closing the gate first makes a registration race
    /// loud rather than allowing a new untracked lease renewer to escape. Every
    /// active publication flag is set before any heartbeat is aborted, so a
    /// compactor that reaches a manifest CAS while retirement is in progress
    /// fails closed even if its outer future has not yet been cancelled.
    pub async fn close_and_abort_heartbeats(
        &self,
    ) -> std::result::Result<(), CompactionLifecycleError> {
        let (tasks, completions) = {
            let mut state = self
                .inner
                .lock()
                .unwrap_or_else(|_| panic!("compaction lifecycle lock poisoned"));
            state.accepting = false;

            let mut tasks = Vec::new();
            let mut completions = Vec::new();
            for run in state.runs.values_mut() {
                run.publication_aborted.store(true, Ordering::SeqCst);
                if let Some(task) = run.heartbeat.take() {
                    tasks.push(task);
                }
                if let Some(completion) = &run.heartbeat_completion {
                    completions.push(completion.clone());
                }
            }
            (tasks, completions)
        };

        for task in &tasks {
            task.abort();
        }

        let mut failures = Vec::new();
        for task in tasks {
            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => failures.push(error.to_string()),
            }
        }
        for completion in completions {
            completion.wait_until_exited().await;
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(CompactionLifecycleError::HeartbeatFailed {
                failures: failures.join("; "),
            })
        }
    }

    /// Reserve admission for a leased compaction before acquiring its S3 lease.
    ///
    /// A closed lifecycle returns a loud error without touching storage. The
    /// returned reservation removes itself if acquisition fails before work
    /// starts.
    pub fn reserve(&self) -> Result<CompactionRunReservation> {
        self.begin_run().map(|active| CompactionRunReservation {
            active: Some(active),
        })
    }

    /// Return whether crash retirement has closed new leased-compaction admission.
    #[must_use]
    pub fn is_retired(&self) -> bool {
        !self
            .inner
            .lock()
            .unwrap_or_else(|_| panic!("compaction lifecycle lock poisoned"))
            .accepting
    }

    fn begin_run(&self) -> Result<ActiveCompaction> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|_| panic!("compaction lifecycle lock poisoned"));
        if !state.accepting {
            return Err(CompactionLifecycleError::RetiredBeforeRegistration.into());
        }
        let run_id = state.next_run_id;
        state.next_run_id = state
            .next_run_id
            .checked_add(1)
            .expect("compaction lifecycle run ID overflow");
        let publication_aborted = Arc::new(AtomicBool::new(false));
        state.runs.insert(
            run_id,
            ActiveCompactionState {
                publication_aborted: Arc::clone(&publication_aborted),
                heartbeat: None,
                heartbeat_completion: None,
            },
        );
        Ok(ActiveCompaction {
            lifecycle: self.clone(),
            run_id,
            publication_aborted,
            finished: false,
        })
    }
}

impl Default for CompactionLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactionRunReservation {
    fn into_active(mut self) -> ActiveCompaction {
        self.active
            .take()
            .expect("compaction run reservation consumed more than once")
    }
}

impl Drop for CompactionRunReservation {
    fn drop(&mut self) {
        if let Some(mut active) = self.active.take() {
            active.finish();
        }
    }
}

impl ActiveCompaction {
    fn publication_abort_signal(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.publication_aborted)
    }

    fn spawn_heartbeat<F, Make>(&self, make_future: Make) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
        Make: FnOnce(Arc<AtomicBool>) -> F,
    {
        let mut state = self
            .lifecycle
            .inner
            .lock()
            .unwrap_or_else(|_| panic!("compaction lifecycle lock poisoned"));
        if !state.accepting || self.publication_aborted.load(Ordering::SeqCst) {
            return Err(CompactionLifecycleError::RetiredDuringHeartbeatRegistration.into());
        }
        let run = state.runs.get_mut(&self.run_id).unwrap_or_else(|| {
            panic!("compaction lifecycle lost active run before heartbeat registration")
        });
        assert!(
            run.heartbeat.is_none() && run.heartbeat_completion.is_none(),
            "compaction lifecycle registered more than one heartbeat for one run"
        );

        let completion = HeartbeatCompletion::new();
        let exit_guard = HeartbeatExitGuard(completion.clone());
        let future = make_future(Arc::clone(&self.publication_aborted));
        run.heartbeat = Some(tokio::spawn(async move {
            let _exit_guard = exit_guard;
            future.await;
        }));
        run.heartbeat_completion = Some(completion);
        Ok(())
    }

    async fn stop_and_join_heartbeat(&self) {
        let (task, completion) = {
            let mut state = self
                .lifecycle
                .inner
                .lock()
                .unwrap_or_else(|_| panic!("compaction lifecycle lock poisoned"));
            let run = state.runs.get_mut(&self.run_id).unwrap_or_else(|| {
                panic!("compaction lifecycle lost active run before heartbeat shutdown")
            });
            (run.heartbeat.take(), run.heartbeat_completion.clone())
        };

        if let Some(task) = task {
            task.abort();
            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => panic!("compaction heartbeat failed during normal shutdown: {error}"),
            }
        }
        if let Some(completion) = completion {
            completion.wait_until_exited().await;
        }
    }

    fn finish(&mut self) {
        let removed = self
            .lifecycle
            .inner
            .lock()
            .unwrap_or_else(|_| panic!("compaction lifecycle lock poisoned"))
            .runs
            .remove(&self.run_id);
        let Some(run) = removed else {
            panic!("compaction lifecycle lost active run before completion");
        };
        assert!(
            run.heartbeat.is_none(),
            "compaction lifecycle completed a run before joining its heartbeat"
        );
        self.finished = true;
    }
}

impl Drop for ActiveCompaction {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let state = self
            .lifecycle
            .inner
            .lock()
            .unwrap_or_else(|_| panic!("compaction lifecycle lock poisoned"));
        if let Some(run) = state.runs.get(&self.run_id) {
            run.publication_aborted.store(true, Ordering::SeqCst);
            if let Some(task) = &run.heartbeat {
                // This is a last-resort drop-path request only. Ordered crash
                // retirement calls close_and_abort_heartbeats(), which also
                // joins this task before replacement may begin.
                task.abort();
            }
        }
    }
}

#[must_use]
fn is_fresh_namespace_discovery_tick(tick: u64) -> bool {
    tick == 1 || tick % NAMESPACE_DISCOVERY_REFRESH_TICKS == 0
}

#[must_use]
fn changed_namespace_names(
    known: &BTreeSet<GcNamespaceIncarnation>,
    active: &BTreeSet<GcNamespaceIncarnation>,
) -> BTreeSet<String> {
    known
        .symmetric_difference(active)
        .map(|incarnation| incarnation.name().to_string())
        .collect()
}

/// Boot-time settings moved into the dedicated compaction runtime thread.
///
/// These settings are resolved from process startup configuration before the
/// thread starts. They are not connected to Zeppelin's runtime-mutable query
/// knobs and do not change while the thread is running.
///
/// # Examples
///
/// A process can reserve two Tokio workers for maintenance and move its GC
/// horizon into this value. Every later tick then uses that same GC snapshot.
///
/// # Rust Notes for Java/C Engineers
///
/// `Clone` performs field-wise cloning: the worker count is copied and
/// [`GcConfig`] is cloned into an independent owned value. Passing this struct
/// by value to [`start_compaction_thread`] then moves that snapshot across the
/// OS-thread boundary; no borrowed startup stack data can outlive its owner.
#[derive(Debug, Clone)]
pub struct CompactionThreadOptions {
    /// Number of Tokio worker threads assigned to background maintenance.
    ///
    /// Production derives this from [`crate::config::CpuBudget`]. It must be
    /// nonzero; an invalid value makes the runtime builder fail inside the
    /// spawned supervisor thread.
    pub compaction_workers: usize,
    /// Garbage-collection safety horizons and retention policy used every tick.
    ///
    /// This same snapshot also constrains which unreachable immutable artifacts
    /// the background loop may physically delete.
    pub gc_config: GcConfig,
}

/// Settings consumed by one invocation of the asynchronous maintenance loop.
///
/// Production constructs this inside [`start_compaction_thread`]. Integration
/// tests call [`compaction_loop`] directly and use `namespace_prefix` to keep a
/// shared bucket's unrelated namespaces out of their maintenance scan.
///
/// # Examples
///
/// A test whose isolated namespaces all start with `test-42` supplies that
/// prefix; the loop discovers and mutates only matching namespaces while using
/// the supplied GC horizon.
#[derive(Debug, Clone)]
pub struct CompactionLoopOptions {
    /// Fixed GC safety and retention snapshot applied to every active namespace.
    pub gc_config: GcConfig,
    /// Optional lexical namespace-name prefix used to restrict discovery.
    ///
    /// `None` scans all top-level namespaces. Production always uses `None`;
    /// tests use `Some(prefix)` for bucket isolation. This filters namespace
    /// names, not arbitrary S3 object-key descendants.
    pub namespace_prefix: Option<String>,
}

/// Boot-composed graph continuation capability for periodic namespace
/// deletion.
///
/// The worker carries no principal and cannot authorize new deletion requests.
/// It can only continue an already durable, evidence-bound intent through the
/// same graph seam used by request-spawned cleanup.
#[derive(Clone)]
pub struct GovernedDeletionWorker {
    graph: Arc<NamespaceGraph>,
    governance: Arc<dyn DeletionGovernance>,
    activation_recovery: Arc<dyn BranchActivationRecovery>,
}

impl std::fmt::Debug for GovernedDeletionWorker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GovernedDeletionWorker")
            .finish_non_exhaustive()
    }
}

impl GovernedDeletionWorker {
    /// Compose periodic deletion from the same boot snapshots as the server.
    #[must_use]
    pub fn new(
        store: ZeppelinStore,
        namespace_manager: Arc<NamespaceManager>,
        lease_manager: Arc<LeaseManager>,
        clock: crate::time::Clock,
        manifest_cache: Arc<ManifestCache>,
        config: &Config,
        security: Arc<SecurityKernel>,
    ) -> Self {
        let governance =
            security.namespace_delete_maintenance_governance(store.clone(), clock.clone());
        let activation_recovery = security.branch_activation_recovery();
        let graph = NamespaceGraph::new(
            store,
            namespace_manager,
            lease_manager,
            clock,
            manifest_cache,
            config.branching.clone(),
            config.indexing.clone(),
            config.gc_horizon_floor_secs(),
        );
        Self {
            graph: Arc::new(graph),
            governance,
            activation_recovery,
        }
    }

    async fn resume(
        &self,
        namespace: &NamespaceId,
        budget: Duration,
    ) -> Result<NamespaceDeleteOutcome> {
        self.graph
            .resume_delete(
                namespace,
                Arc::clone(&self.governance),
                Arc::clone(&self.activation_recovery),
                budget,
            )
            .await
    }

    async fn confirm_missing_after_resume(
        &self,
        namespace: &NamespaceId,
        metadata: &NamespaceMetadata,
    ) -> Result<()> {
        self.graph
            .confirm_missing_after_resume(namespace, metadata)
            .await
    }
}

/// Owns one lifecycle-registered renewal task for one leased compaction.
///
/// The task renews at one third of the configured lease duration. Successful
/// renewal extends expiry without changing the fencing token. A definite
/// takeover, or a renewal error after the last confirmed expiry, sets the
/// shared flag so the compactor refuses its next manifest CAS.
///
/// ```text
/// acquired lease token N
///        |
///        +-- every duration/3 --> renew with ETag CAS --> same token, new expiry
///        |                              |
///        |                              +-- transient error before expiry
///        |                                      `--> retry next heartbeat tick
///        |                              |
///        |                              `-- takeover / error past expiry
///        |                                      `--> lease_lost = true; stop
///        v
/// compactor checks lease_lost before each manifest CAS
/// ```
///
/// The flag is a publication guard, not cooperative cancellation for the whole
/// build. Immutable uploads completed before the check may remain as unreferenced
/// objects for GC, but they never become visible through this stale compaction's
/// manifest update.
struct LeaseHeartbeat {
    /// Sequentially consistent publication-abort signal shared with compaction.
    lease_lost: Arc<AtomicBool>,
    /// Registration whose lifecycle owns and joins the Tokio task.
    active: ActiveCompaction,
}

impl LeaseHeartbeat {
    /// Starts renewing an already-acquired namespace lease in a Tokio task.
    ///
    /// The heartbeat owns a clone-independent lease snapshot and updates that
    /// snapshot after every successful renewal. It signals loss only when the
    /// lease manager proves holder/token mismatch or when another renewal error
    /// arrives after the last expiry it successfully confirmed. Before that
    /// expiry, storage and CAS errors are logged and retried on the next tick.
    ///
    /// # Parameters
    ///
    /// - `lease_manager`: Shared manager whose holder identity acquired the
    ///   lease and whose configured duration determines the renewal interval.
    /// - `namespace`: Owned namespace name moved into logs, metric labels, and
    ///   object-store renewal calls.
    /// - `lease`: Most recently acquired snapshot. Its holder and fencing token
    ///   must correspond to `lease_manager` and `namespace`.
    ///
    /// # Returns
    ///
    /// A heartbeat containing the shared loss flag and a handle that can request
    /// task cancellation. The flag starts as `false`.
    ///
    /// # Panics
    ///
    /// Panics if called without an active Tokio runtime because
    /// [`tokio::spawn`] needs a runtime on which to schedule the task.
    ///
    /// # Side Effects
    ///
    /// Spawns one task, issues periodic lease CAS operations, increments
    /// lease-renewal or lease-loss metrics, and emits structured diagnostics.
    /// Normal ETag-bearing renewals issue no GET; conflicts and missing backend
    /// ETags use the lease manager's bounded authoritative-read paths. The task
    /// never changes the manifest or the fencing token.
    ///
    /// # Consistency
    ///
    /// Renewal proves lease-object ownership only. The compactor must still
    /// carry the fencing token into manifest CAS. If a conditional PUT succeeds
    /// without returning an ETag and its fallback GET fails, the heartbeat
    /// conservatively retains the older local expiry and may later declare loss
    /// even though S3 received the extension; failing closed is safer than a
    /// stale publication.
    ///
    /// # Performance
    ///
    /// Each normal successful heartbeat performs one remote conditional PUT.
    /// Renewal runs concurrently with compaction and performs no busy polling
    /// for a normal positive lease duration.
    ///
    /// # Examples
    ///
    /// With a 30-second lease, the task wakes about every 10 seconds. If token
    /// 7 renews successfully it stays token 7 with a later expiry. If another
    /// node takes over with token 8, the next renewal sets `lease_lost`, and the
    /// compactor rejects its next commit attempt.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The `async move` closure owns `lease_manager`, `namespace`, `lease`, and
    /// its cloned `Arc<AtomicBool>`. Java would capture heap references in a
    /// runnable; C would allocate an explicit context struct and define its
    /// cleanup. Rust statically prevents this detached future from borrowing
    /// local stack variables. Cloning `Arc` increments a reference count; it
    /// does not duplicate the atomic value.
    fn spawn(
        mut active: ActiveCompaction,
        lease_manager: Arc<LeaseManager>,
        namespace: String,
        lease: Lease,
    ) -> Result<Self> {
        let lease_lost = active.publication_abort_signal();
        // One missed renewal still leaves two nominal intervals before expiry,
        // giving a transient object-store failure another scheduled attempt.
        let interval = lease_manager.lease_duration() / 3;

        let heartbeat_result = active.spawn_heartbeat(move |flag| async move {
            let mut current = lease;
            loop {
                if flag.load(Ordering::SeqCst) {
                    return;
                }
                tokio::time::sleep(interval).await;
                if flag.load(Ordering::SeqCst) {
                    return;
                }

                match lease_manager.renew(&namespace, &current).await {
                    Ok(renewed) => {
                        if flag.load(Ordering::SeqCst) {
                            return;
                        }
                        crate::metrics::COMPACTION_LEASE_RENEWALS_TOTAL
                            .with_label_values(&[namespace.as_str()])
                            .inc();
                        debug!(
                            namespace = %namespace,
                            fencing_token = renewed.fencing_token,
                            expires_at = %renewed.expires_at,
                            "compaction lease renewed (heartbeat)"
                        );
                        current = renewed;
                    }
                    Err(ZeppelinError::LeaseExpired { .. }) => {
                        // A different holder/token is authoritative. Tell the
                        // compactor that publication is now forbidden.
                        crate::metrics::COMPACTION_LEASE_LOST_TOTAL
                            .with_label_values(&[namespace.as_str()])
                            .inc();
                        error!(
                            namespace = %namespace,
                            fencing_token = current.fencing_token,
                            "compaction lease lost mid-flight (taken over by another node); \
                             signaling compaction abort"
                        );
                        flag.store(true, Ordering::SeqCst);
                        return;
                    }
                    Err(e) => {
                        // A failed renewal does not prove loss before the last
                        // confirmed expiry. After it, fail closed rather than
                        // letting an unproven holder reach manifest CAS.
                        if current.expires_at <= lease_manager.clock().now() {
                            crate::metrics::COMPACTION_LEASE_LOST_TOTAL
                                .with_label_values(&[namespace.as_str()])
                                .inc();
                            error!(
                                namespace = %namespace,
                                error = %e,
                                expires_at = %current.expires_at,
                                "compaction lease renewal failed past expiry; \
                                 signaling compaction abort"
                            );
                            flag.store(true, Ordering::SeqCst);
                            return;
                        }
                        warn!(
                            namespace = %namespace,
                            error = %e,
                            "compaction lease renewal failed (transient); retrying next tick"
                        );
                    }
                }
            }
        });
        if let Err(error) = heartbeat_result {
            active.finish();
            return Err(error);
        }

        Ok(Self { lease_lost, active })
    }

    /// Stops and joins the renewal task after compaction finishes.
    ///
    /// The lifecycle run is consumed exactly once at final completion, so its
    /// heartbeat cannot be stopped or reused twice.
    ///
    /// # Side Effects
    ///
    /// Calls [`tokio::task::JoinHandle::abort`] and awaits termination. It does
    /// not release the lease; the wrapper performs release separately after this
    /// join.
    ///
    /// # Examples
    ///
    /// Once compaction has returned either success or error, stopping prevents
    /// later sleep ticks from extending a lease whose useful work has ended.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// The private lifecycle run is a compiler-enforced one-shot ownership
    /// transition. Java code would rely on an idempotent state flag, while C
    /// would conventionally invalidate a handle after cancellation. Tokio abort
    /// is cooperative at async yield points; awaiting the owned handle proves
    /// the task reached cancellation before release proceeds.
    async fn stop_and_join(&self) {
        self.active.stop_and_join_heartbeat().await;
    }

    fn finish(&mut self) {
        self.active.finish();
    }
}

/// Acquires a namespace lease and runs one fenced compaction lifecycle.
///
/// The caller must retain `lifecycle` until every future using it has completed
/// or cancellation has been followed by
/// [`CompactionLifecycle::close_and_abort_heartbeats`]. A successful lease
/// renews while immutable artifacts are built and carries its token into
/// manifest publication.
///
/// # Parameters
///
/// - `compactor`: Stateless coordinator that reads the authoritative manifest,
///   builds immutable segment artifacts, and attempts fenced manifest CAS.
/// - `lease_manager`: Shared process holder used to acquire, renew, and release
///   this namespace's lease object.
/// - `namespace`: Namespace to compact. The caller has normally obtained it from
///   namespace discovery, but the compactor re-reads authoritative state.
/// - `fts_configs`: Full-text field definitions from the namespace metadata
///   snapshot used for this cycle.
/// - `fragment_cache`: Immutable WAL-byte cache behavior for the compaction
///   read. Production supplies read-only access; direct tools may bypass it.
/// - `lifecycle`: Caller-owned admission and heartbeat join authority shared by
///   every leased compaction in the same server or test lifecycle.
///
/// # Returns
///
/// The [`CompactionResult`] whose manifest update succeeded, including a
/// `None` segment for a no-op or all-deleted result.
///
/// # Errors
///
/// Propagates lease acquisition errors, including [`ZeppelinError::LeaseHeld`]
/// when another node owns an unexpired lease, and all compaction errors. Lease
/// acquisition or compaction can leave remote lease or immutable candidate
/// objects behind; unsuccessful manifest CAS does not make those candidates
/// visible.
///
/// # Side Effects
///
/// Acquires and renews the lease object, runs all compaction I/O and CPU work,
/// updates heartbeat metrics, and attempts best-effort release. Loop-level
/// compaction counters, namespace health, cache invalidation, and warming are
/// deliberately the caller's responsibility.
///
/// # Consistency
///
/// Lease ownership avoids duplicate work, while the fencing token plus ETag CAS
/// prevent a stale holder from publishing. The manifest, not the lease or the
/// existence of uploaded objects, defines what readers can see.
///
/// # Performance
///
/// Adds lease acquisition and periodic renewal roundtrips around the underlying
/// compaction. A `LeaseHeld` result stops before reading WAL or building an
/// index.
///
/// # Examples
///
/// If namespace `catalog` has 100 visible WAL fragments and no live lease, this
/// call acquires token 12, keeps it renewed during index construction, and
/// returns the segment made visible by manifest CAS. If node B already owns the
/// lease, it returns `LeaseHeld` without doing duplicate compaction work.
///
/// # Rust Notes for Java/C Engineers
///
/// The compactor and manager are borrowed, so this future does not take them
/// away from the caller. `fts_configs` is also a shared borrow and is not cloned
/// here. The `?` operator forwards acquisition failure immediately, analogous to
/// propagating a Java exception or returning a checked C status, while Rust
/// still drops all local owned values on that path.
pub async fn compact_namespace_under_lease_with_lifecycle(
    compactor: &Compactor,
    lease_manager: &Arc<LeaseManager>,
    namespace: &str,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    fragment_cache: FragmentCachePolicy<'_>,
    lifecycle: &CompactionLifecycle,
) -> Result<CompactionResult> {
    let reservation = lifecycle.reserve()?;
    let lease = lease_manager.acquire(namespace).await?;
    run_compaction_with_reserved_lease(
        compactor,
        lease_manager,
        namespace,
        lease,
        fts_configs,
        fragment_cache,
        reservation,
    )
    .await
}

/// Runs fenced compaction after the caller has already acquired the lease.
///
/// This seam is shared by the periodic scheduler and the manual HTTP endpoint.
/// It starts renewal, passes the original fencing token plus a shared loss flag
/// to the compactor, stops renewal after the compactor returns, and then attempts
/// release before returning the compactor's original result.
///
/// ```text
/// caller owns acquired Lease
///        |
///        +-- clone --> heartbeat owns renewable snapshot
///        |                 |
///        |                 `--> Arc<AtomicBool> shared with compactor
///        v
/// compactor uses original fencing token
///        |
///        v
/// Result ready --> abort heartbeat --> best-effort release --> return Result
/// ```
///
/// # Parameters
///
/// - `compactor`: Coordinator that will build and conditionally publish the
///   compacted segment.
/// - `lease_manager`: Manager whose holder identity must match `lease`.
/// - `namespace`: Namespace to which the acquired lease and FTS settings apply.
/// - `lease`: Owned lease snapshot acquired for this namespace. Its original
///   token is used for compaction and final release.
/// - `fts_configs`: Borrowed full-text field definitions to materialize.
/// - `fragment_cache`: Immutable WAL-byte cache behavior used while building
///   the segment.
/// - `lifecycle`: Caller-owned heartbeat authority that must outlive this
///   future and be explicitly retired after cancellation before replacement.
///
/// # Returns
///
/// Returns exactly the compactor's [`CompactionResult`] on success. Heartbeat
/// stop and release do not replace that value.
///
/// # Errors
///
/// Returns the compactor's storage, indexing, fencing, lease-loss, upload-window,
/// or manifest-CAS error. Best-effort release errors are logged and suppressed,
/// so they never replace the primary result. Immutable uploads may exist even
/// when the result is an error, but failed publication leaves them invisible.
///
/// # Side Effects
///
/// Spawns and aborts one heartbeat, may renew the lease many times, runs
/// compaction, and calls lease release on every normal `Result` path.
///
/// # Consistency
///
/// The atomic flag is checked before each compactor manifest-CAS attempt. It is
/// an additional fail-closed signal; authoritative lease checks, fencing, and
/// manifest ETag CAS remain necessary for races inside one heartbeat interval.
/// Release preserves the lease object and fencing-token history.
///
/// # Cancellation
///
/// Normal cleanup is explicit: it stops and joins renewal before release. If
/// the outer future is cancelled or panics while awaiting compaction,
/// [`ActiveCompaction::drop`] requests heartbeat cancellation but cannot join
/// it. The shared [`CompactionLifecycle`] retains that join responsibility, so
/// ordered crash retirement must call
/// [`CompactionLifecycle::close_and_abort_heartbeats`] before replacing the
/// node.
///
/// # Performance
///
/// Cloning the lease duplicates its owned strings, while cloning the atomic
/// `Arc` only increments a reference count. All index and object-store costs
/// belong to the delegated compaction.
///
/// # Examples
///
/// A manual endpoint can acquire token 5, return HTTP 202, and run this function
/// in a spawned task. A takeover detected during a long build flips the flag;
/// the build may leave candidate objects, but this function returns an error
/// after the compactor refuses manifest publication and then attempts release.
///
/// # Rust Notes for Java/C Engineers
///
/// The original `Lease` is retained for token use and cleanup while a deep
/// `clone` is moved into the heartbeat. Both tasks share one atomic allocation
/// through `Arc`. Java's garbage collection gives shared lifetime but not this
/// explicit ownership split; C needs a refcount, atomic memory-order choices,
/// and cleanup on every exit path. Rust cleans up ordinary owned values, but a
/// detached Tokio task still requires explicit cancellation as described above.
pub async fn run_compaction_with_lease_and_lifecycle(
    compactor: &Compactor,
    lease_manager: &Arc<LeaseManager>,
    namespace: &str,
    lease: Lease,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    fragment_cache: FragmentCachePolicy<'_>,
    lifecycle: &CompactionLifecycle,
) -> Result<CompactionResult> {
    let reservation = match lifecycle.reserve() {
        Ok(reservation) => reservation,
        Err(error) => {
            // Compatibility callers may already have acquired their lease.
            // Do not leave it live when retirement beats registration.
            if let Err(release_error) = lease_manager.release(namespace, &lease).await {
                warn!(
                    namespace = %namespace,
                    error = %release_error,
                    "lease release failed after compaction lifecycle retirement"
                );
            }
            return Err(error);
        }
    };
    run_compaction_with_reserved_lease(
        compactor,
        lease_manager,
        namespace,
        lease,
        fts_configs,
        fragment_cache,
        reservation,
    )
    .await
}

/// Runs an already-acquired lease using a reservation made before acquisition.
///
/// Server paths call [`CompactionLifecycle::reserve`] while they still hold no
/// S3 lease, then pass that reservation here after successful acquisition.
/// This makes lifecycle close atomic with both periodic and manual admission.
pub async fn run_compaction_with_reserved_lease(
    compactor: &Compactor,
    lease_manager: &Arc<LeaseManager>,
    namespace: &str,
    lease: Lease,
    fts_configs: &HashMap<String, FtsFieldConfig>,
    fragment_cache: FragmentCachePolicy<'_>,
    reservation: CompactionRunReservation,
) -> Result<CompactionResult> {
    info!(
        namespace = %namespace,
        fencing_token = lease.fencing_token,
        lease_expires_at = %lease.expires_at,
        "starting compaction with acquired lease"
    );

    let mut heartbeat = match LeaseHeartbeat::spawn(
        reservation.into_active(),
        Arc::clone(lease_manager),
        namespace.to_string(),
        lease.clone(),
    ) {
        Ok(heartbeat) => heartbeat,
        Err(error) => {
            // The lifecycle may have closed after this caller acquired its
            // lease but before it could register a heartbeat. Do not strand a
            // short-lived lease over a replacement: release best-effort, then
            // surface the retired lifecycle loudly to the caller.
            if let Err(release_error) = lease_manager.release(namespace, &lease).await {
                warn!(
                    namespace = %namespace,
                    error = %release_error,
                    "lease release failed after compaction lifecycle retirement"
                );
            }
            return Err(error);
        }
    };

    let result = compactor
        .compact_with_fts_signaled(
            namespace,
            Some(lease.fencing_token),
            fts_configs,
            Some(Arc::clone(&heartbeat.lease_lost)),
            fragment_cache,
        )
        .await;

    heartbeat.stop_and_join().await;

    // Best-effort release; never blocks or fails the cycle. If the lease
    // was taken over, release() detects the holder/token mismatch and
    // returns Ok without touching the thief's lease.
    if let Err(e) = lease_manager.release(namespace, &lease).await {
        warn!(namespace = %namespace, error = %e, "lease release failed (best-effort)");
    }
    heartbeat.finish();

    result
}

/// Fetches and pins the current active segment's routing metadata best-effort.
///
/// A successful compaction spawns this helper after manifest-cache invalidation.
/// The helper deliberately reads the manifest from object storage rather than
/// trusting the compaction result: another compaction may already have advanced
/// the active segment before this task is scheduled. Hierarchical segments warm
/// `tree_meta.json`; ordinary IVF-Flat segments warm their centroid artifact.
///
/// # Parameters
///
/// - `store`: Owned cloneable store client used for the authoritative manifest
///   and immutable metadata GETs.
/// - `cache`: Shared tiered cache into which bytes are fetched and scope-pinned.
/// - `namespace`: Owned namespace name used for manifest lookup, artifact-key
///   construction, and the scoped pin.
///
/// # Returns
///
/// Returns unit after logging the outcome. A missing manifest, no active segment,
/// or an active ID absent from the segment list results in no pin; fetch and
/// decode/storage failures are logged rather than returned.
///
/// # Side Effects
///
/// Performs a fresh manifest GET and, on a cache miss, one immutable metadata
/// GET. It may fill memory and disk cache tiers, rotate the namespace's scoped
/// pin, and emit debug or warning diagnostics.
///
/// # Consistency
///
/// Warming is never a visibility boundary. The manifest read chooses the key,
/// and that immutable key remains safe to cache. A newer manifest can win after
/// this read and before pinning; a later warm may rotate the pin, while query
/// execution still follows its own manifest snapshot. A missing/corrupt object
/// remains a query-path error even though this helper suppresses its prefetch
/// failure.
///
/// # Performance
///
/// Runs outside the serial namespace loop in a spawned task. A warm cache hit
/// costs local lookup; a miss reads and writes the full routing-metadata object.
/// The helper does not fetch vector clusters or wait for any query.
///
/// # Examples
///
/// After segment `S9` becomes active, an IVF-Flat namespace fetches
/// `S9/centroids.bin` and pins it before the next query. If the GET fails, the
/// task logs a warning; the first query retries through its fail-loud cache-miss
/// path. If `S10` became active first, the fresh manifest causes this task to
/// warm `S10`, not the stale result that scheduled it.
///
/// # Rust Notes for Java/C Engineers
///
/// All parameters are owned because this future is passed to [`tokio::spawn`]
/// and may outlive the loop iteration. The inner `async` block creates a local
/// [`Result`] boundary so `?` can short-circuit the warm attempt while the outer
/// `match` converts every outcome into logging. `let Some(...) = ... else`
/// makes the no-active-segment branch explicit and prevents nullable access;
/// Java would use a null/optional check, while C would use a pointer plus status.
async fn warm_segment_index_meta(store: ZeppelinStore, cache: Arc<DiskCache>, namespace: String) {
    let result = async {
        let manifest = Manifest::read(&store, &namespace)
            .await?
            .unwrap_or_default();
        let seg_ref = manifest.active_segment.as_ref().and_then(|segment_id| {
            manifest
                .segments
                .iter()
                .find(|s| s.id == *segment_id)
                .cloned()
        });
        let Some(seg_ref) = seg_ref else {
            return Ok::<Option<String>, ZeppelinError>(None);
        };
        let key = if seg_ref.hierarchical {
            crate::index::hierarchical::tree_meta_key(&namespace, &seg_ref.id)
        } else {
            crate::index::ivf_flat::build::centroids_key(&namespace, &seg_ref.id)
        };
        cache.get_or_fetch(&key, || store.get(&key)).await?;
        cache.pin_scoped(&namespace, &key).await;
        Ok(Some(key))
    }
    .await;

    match result {
        Ok(Some(key)) => {
            debug!(namespace = %namespace, key = %key, "warmed segment index metadata post-compaction");
        }
        Ok(None) => {
            debug!(namespace = %namespace, "no active segment to warm post-compaction");
        }
        Err(e) => {
            warn!(
                namespace = %namespace,
                error = %e,
                "post-compaction cache warming failed (non-fatal — first query pays the cold fetch)"
            );
        }
    }
}

/// Starts the production maintenance loop on its own OS thread and Tokio runtime.
///
/// The returned thread owns the runtime until [`compaction_loop`] observes a
/// shutdown-channel change and returns. `Runtime::block_on` drives the top-level
/// loop on the supervisor thread, so its synchronous k-means work executes
/// there rather than on a query-runtime worker. Tasks spawned by the loop use
/// this runtime's workers, and FTS `spawn_blocking` work uses this runtime's
/// blocking pool. Object-store operations remain asynchronous and can overlap
/// while waiting.
///
/// ```text
/// calling/query runtime
///        |
///        | moves Arc owners + shutdown receiver
///        v
/// "compaction-runtime" OS supervisor thread
///        |
///        +--> block_on(compaction_loop)
///        |          `--> synchronous compaction work is driven here
///        |
///        +--> "compaction-worker" Tokio workers --> spawned heartbeat/warm
///        |
///        +--> Tokio blocking pool --> spawned FTS construction
///        |
///        `--> compaction_loop returns --> runtime drops --> OS thread exits
/// ```
///
/// # Parameters
///
/// - `compactor`: Shared stateless compactor moved into the runtime.
/// - `namespace_manager`: Shared namespace discovery, deletion, and health
///   coordinator.
/// - `shutdown`: Watch receiver whose next change or closed channel asks the loop
///   to stop. A lifecycle-retired admission error also reads its current value
///   to distinguish shutdown control flow from an operational failure.
/// - `manifest_cache`: Process-local query-manifest cache invalidated after
///   successful deletion or compaction.
/// - `lease_manager`: Shared holder used for all namespace compaction leases.
/// - `cache`: Shared tiered object cache used for post-publication metadata warm.
/// - `options`: Worker allocation and fixed GC policy for this runtime.
///
/// # Returns
///
/// A [`std::thread::JoinHandle`] for shutdown code to join. Returning the handle
/// means only that the OS thread was created; runtime construction happens
/// inside that thread and may fail afterward.
///
/// # Panics
///
/// Panics synchronously if the operating system cannot spawn the supervisor
/// thread. If Tokio cannot build its runtime—for example, because the worker
/// count is zero—the spawned thread panics, and that panic is observed later
/// when the caller joins the returned handle.
///
/// # Side Effects
///
/// Creates one named OS supervisor thread, the configured number of Tokio worker
/// threads, and Tokio's supporting driver/blocking resources. It immediately
/// starts logging and then waits one compaction interval before the first tick.
///
/// # Consistency
///
/// Thread isolation changes scheduling, not authority. Every storage mutation in
/// the loop still uses the namespace manager, GC implementation, lease manager,
/// compactor fencing, and manifest CAS contracts.
///
/// # Performance
///
/// The worker count caps how many spawned runtime tasks execute on ordinary
/// worker threads at once. It does not include the supervisor thread driving
/// the top-level future, the blocking pool, the number of pending async tasks,
/// or remote S3 concurrency inside delegated operations. Production normally
/// derives the count from
/// [`crate::config::CpuBudget::auto`].
///
/// # Examples
///
/// Startup can pass two compaction workers on an eight-core host. Query serving
/// continues on its separate runtime while the returned handle represents the
/// maintenance thread; graceful shutdown sends on the watch channel and joins
/// that handle with a timeout.
///
/// # Rust Notes for Java/C Engineers
///
/// The `move` closure transfers owned `Arc` handles and the watch receiver into
/// the OS thread. Unlike a Java reference capture or C pointer handoff, Rust
/// requires every captured value to be safe to send across threads. Dropping a
/// Rust thread `JoinHandle` detaches the thread; production retains it and
/// attempts a bounded join during shutdown. A timeout is reported loudly and
/// process exit, rather than same-process replacement, contains any remaining
/// OS-thread work.
#[allow(clippy::expect_used)]
pub fn start_compaction_thread(
    compactor: Arc<Compactor>,
    namespace_manager: Arc<NamespaceManager>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    manifest_cache: Arc<ManifestCache>,
    lease_manager: Arc<LeaseManager>,
    cache: Arc<DiskCache>,
    deletion_worker: GovernedDeletionWorker,
    lifecycle: CompactionLifecycle,
    options: CompactionThreadOptions,
) -> std::thread::JoinHandle<()> {
    let compaction_workers = options.compaction_workers;
    let gc_config = options.gc_config;
    info!(compaction_workers, "starting compaction runtime");
    std::thread::Builder::new()
        .name("compaction-runtime".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(compaction_workers)
                .thread_name("compaction-worker")
                .enable_all()
                .build()
                .expect("failed to build compaction runtime");

            rt.block_on(compaction_loop_with_governed_deletion(
                compactor,
                namespace_manager,
                shutdown,
                manifest_cache,
                lease_manager,
                cache,
                CompactionLoopOptions {
                    gc_config,
                    namespace_prefix: None,
                },
                deletion_worker,
                &lifecycle,
            ));
        })
        .expect("failed to spawn compaction thread")
}

/// Strongly revalidates one live manifest and evaluates its advisory trigger.
///
/// This is the single scheduler seam shared by the production loop and the
/// dedicated performance harness. A resident manifest permits a conditional
/// GET, but cache presence never substitutes for successful object-store
/// verification. Namespace metadata is the bounded-staleness discovery hint;
/// an actual lease-protected compaction reloads both manifest and metadata
/// before publishing anything.
///
/// # Errors
///
/// Maps a missing required live manifest to [`ZeppelinError::ManifestNotFound`]
/// using the discovered namespace name. Storage, conditional-read, binding,
/// and decoding errors propagate unchanged; no stale cached snapshot is used
/// after a failed refresh.
pub async fn evaluate_compaction_trigger(
    compactor: &Compactor,
    manifest_cache: &ManifestCache,
    namespace: &NamespaceMetadata,
) -> Result<bool> {
    let manifest = manifest_cache
        .get_strong_required(compactor.store(), &namespace.name)
        .await
        .map_err(|error| match error {
            ZeppelinError::NotFound { .. } => ZeppelinError::ManifestNotFound {
                namespace: namespace.name.clone(),
            },
            error => error,
        })?;

    compactor.should_compact(&namespace.name, &manifest, namespace)
}

/// Repeatedly discovers namespaces and performs deletion, GC, and compaction.
///
/// The first maintenance pass occurs after one configured interval. Tick 1 and
/// every twelfth tick call authoritative namespace discovery; intervening ticks
/// use the manager's process-local registry. A failed discovery also uses that
/// registry for this tick, so known namespaces continue maintenance while newly
/// created remote namespaces wait for a later successful refresh.
///
/// For each discovered namespace, processing is deliberately ordered and
/// serial. A durable `Deleting` tombstone or an `Active` namespace carrying a
/// governed deletion intent gets a bounded continuation and skips all other
/// work. An ordinary active namespace runs GC first, then trigger evaluation,
/// then lease-protected compaction if due. Failures are recorded at their own
/// boundary and the loop proceeds to the next namespace.
///
/// # Parameters
///
/// - `compactor`: Shared coordinator containing fixed trigger/index defaults and
///   access to authoritative object storage.
/// - `namespace_manager`: Shared registry plus authoritative namespace metadata
///   operations for discovery, deletion, and health updates.
/// - `shutdown`: Watch receiver. Any observed version change, or closure after
///   all senders are dropped, ends the loop at its next top-level wait.
/// - `manifest_cache`: Query-facing cache invalidated after completed deletion or
///   successful compaction publication/no-op observation.
/// - `lease_manager`: Shared per-process holder for namespace lease lifecycles.
/// - `cache`: Tiered immutable-object cache used only by spawned index-metadata
///   warming in this function.
/// - `options`: Fixed GC policy and optional lexical namespace discovery scope.
///
/// # Returns
///
/// Returns unit after shutdown is observed. Per-namespace errors are logged and
/// represented in metrics/health where applicable rather than returned from this
/// long-running supervisor.
///
/// # Side Effects
///
/// Sleeps, lists namespaces, resumes object deletion, runs manifest history and
/// immutable-object GC, reads compaction status, acquires/renews/releases leases,
/// builds and publishes segments, updates Prometheus counters and namespace
/// health metadata, invalidates manifest cache entries, and spawns best-effort
/// routing-metadata warms.
///
/// `COMPACTIONS_TOTAL` counts trigger-evaluation failures and attempts that
/// acquired a lease and returned from the compactor. `LeaseHeld` is a quiet
/// skip. A successful result records health and invalidates the manifest cache
/// even when a race turned the run into a no-op; only a result containing a
/// segment ID spawns a warm.
///
/// # Consistency
///
/// Registry snapshots decide what to inspect, never what is authoritative.
/// Deletion, GC, trigger checks, leases, and compaction each re-enter their
/// object-store-backed contracts. GC runs before compaction so it reasons from
/// the manifest state present at the start of the tick; newly retired objects
/// are considered by later cycles. Cache invalidation follows successful
/// publication and cannot make an unpublished segment visible.
///
/// A GC failure is logged but does not authorize a weaker cleanup or prevent an
/// independent compaction attempt. Trigger-evaluation and compaction failures
/// increment failure metrics and attempt a CAS-protected namespace health
/// update; failure of that health write is logged without hiding the original
/// maintenance failure.
///
/// # Cancellation
///
/// Shutdown is cooperative at the outer interval boundary. Once namespace work
/// starts, a signal does not cancel a 25-second deletion pass, remote GC calls,
/// lease-protected compaction, or remaining namespaces in the current snapshot.
/// After the loop returns, dropping the dedicated runtime cancels any spawned
/// warm tasks still running. The per-compaction heartbeat is normally stopped by
/// [`run_compaction_with_reserved_lease`] before control returns here.
///
/// # Performance
///
/// Work is `O(discovered namespaces)` per tick and namespaces are processed one
/// at a time, so one slow namespace delays later namespaces and shutdown
/// observation. Fresh discovery performs delimiter listing plus metadata reads
/// on tick 1 and every 12 ticks. Each active namespace runs a GC cycle and at
/// least a manifest-based trigger check; an actual compaction adds its artifact
/// I/O and CPU cost. `interval_secs = 0` creates an immediately-ready sleep and
/// therefore no intentional idle delay between scans; production callers should
/// provide a positive interval.
///
/// # Examples
///
/// Suppose `catalog` is deleting, `photos` has 120 WAL fragments, and `archive`
/// is idle. One tick spends at most 25 seconds continuing `catalog` deletion,
/// runs GC for `photos`, acquires its lease, publishes a segment, records health,
/// invalidates its manifest cache, and spawns a centroid warm. It then runs GC
/// and a cheap trigger check for `archive` without compacting it.
///
/// If namespace listing temporarily fails, already registered namespaces still
/// run through authoritative maintenance operations. If another node holds the
/// `photos` lease, this node logs a debug skip, leaves compaction health and the
/// success/failure counter unchanged, and retries discovery on later ticks.
///
/// # Rust Notes for Java/C Engineers
///
/// [`tokio::select!`] waits for either the interval future or the watch-channel
/// change without dedicating an OS thread to each wait. It resembles a Java
/// `CompletableFuture.anyOf` loop or C event-loop multiplexing, but the compiler
/// checks that borrowed inputs remain alive across every `.await`.
///
/// The loop borrows each namespace from an owned vector while cloning only the
/// namespace string moved into the detached warm task. `match` exhaustively
/// separates `LeaseHeld`, success, and other errors, preventing a new error
/// variant from accidentally taking a success path. Saturating tick arithmetic
/// keeps a long-lived debug counter from wrapping to zero.
pub async fn compaction_loop(
    compactor: Arc<Compactor>,
    namespace_manager: Arc<NamespaceManager>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    manifest_cache: Arc<ManifestCache>,
    lease_manager: Arc<LeaseManager>,
    cache: Arc<DiskCache>,
    options: CompactionLoopOptions,
) {
    let lifecycle = CompactionLifecycle::new();
    compaction_loop_with_lifecycle(
        compactor,
        namespace_manager,
        shutdown,
        manifest_cache,
        lease_manager,
        cache,
        options,
        &lifecycle,
    )
    .await;
}

/// Repeatedly runs maintenance under a shared server compaction lifecycle.
///
/// The periodic loop and manual HTTP endpoint must receive the same lifecycle
/// from server construction so one crash-retirement barrier owns every lease
/// heartbeat. Direct callers that do not have a server owner use
/// [`compaction_loop`], which creates an isolated lifecycle.
pub async fn compaction_loop_with_lifecycle(
    compactor: Arc<Compactor>,
    namespace_manager: Arc<NamespaceManager>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    manifest_cache: Arc<ManifestCache>,
    lease_manager: Arc<LeaseManager>,
    cache: Arc<DiskCache>,
    options: CompactionLoopOptions,
    lifecycle: &CompactionLifecycle,
) {
    compaction_loop_with_lifecycle_inner(
        compactor,
        namespace_manager,
        shutdown,
        manifest_cache,
        lease_manager,
        cache,
        options,
        None,
        lifecycle,
    )
    .await;
}

/// Run maintenance with an explicitly boot-composed governed deletion worker.
///
/// Production and production-like server tests use this entry point. Callers
/// that omit the worker can still exercise compaction/GC, but a discovered
/// deletion tombstone fails loudly and remains untouched.
pub async fn compaction_loop_with_governed_deletion(
    compactor: Arc<Compactor>,
    namespace_manager: Arc<NamespaceManager>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    manifest_cache: Arc<ManifestCache>,
    lease_manager: Arc<LeaseManager>,
    cache: Arc<DiskCache>,
    options: CompactionLoopOptions,
    deletion_worker: GovernedDeletionWorker,
    lifecycle: &CompactionLifecycle,
) {
    compaction_loop_with_lifecycle_inner(
        compactor,
        namespace_manager,
        shutdown,
        manifest_cache,
        lease_manager,
        cache,
        options,
        Some(&deletion_worker),
        lifecycle,
    )
    .await;
}

async fn compaction_loop_with_lifecycle_inner(
    compactor: Arc<Compactor>,
    namespace_manager: Arc<NamespaceManager>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    manifest_cache: Arc<ManifestCache>,
    lease_manager: Arc<LeaseManager>,
    cache: Arc<DiskCache>,
    options: CompactionLoopOptions,
    deletion_worker: Option<&GovernedDeletionWorker>,
    lifecycle: &CompactionLifecycle,
) {
    let CompactionLoopOptions {
        gc_config,
        namespace_prefix,
    } = options;
    info!(
        interval_secs = compactor.config().interval_secs,
        gc_horizon_secs = gc_config.horizon_secs,
        namespace_prefix = namespace_prefix.as_deref().unwrap_or("<all>"),
        "background compaction loop started"
    );

    let mut gc_runner = GcRunner::new(compactor.store().clone(), gc_config.clone())
        .with_preservation_service(compactor.preservation_service().cloned());
    // The registry and manifest cache are warmed by startup before this loop is
    // spawned. Seed matching lifecycle identities so tick one's authoritative
    // discovery does not evict an unchanged resident manifest. A missing or
    // stale registry entry still differs from the fresh S3 listing below and
    // therefore preserves delete/recreate invalidation safety.
    let mut known_incarnations = namespace_manager
        .cached_namespaces(namespace_prefix.as_deref())
        .into_iter()
        .filter(|namespace| namespace.state == NamespaceState::Active)
        .map(|namespace| GcNamespaceIncarnation::from_metadata(&namespace))
        .collect::<BTreeSet<_>>();
    let mut tick: u64 = 0;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(compactor.config().interval_secs)) => {},
            _ = shutdown.changed() => {
                info!("background compaction loop shutting down");
                break;
            }
        }

        tick = tick.saturating_add(1);
        let (namespaces, fresh_discovery) = if is_fresh_namespace_discovery_tick(tick) {
            match namespace_manager.list(namespace_prefix.as_deref()).await {
                Ok(ns) => (ns, true),
                Err(e) => {
                    warn!(error = %e, "failed to list namespaces for compaction");
                    (
                        namespace_manager.cached_namespaces(namespace_prefix.as_deref()),
                        false,
                    )
                }
            }
        } else {
            (
                namespace_manager.cached_namespaces(namespace_prefix.as_deref()),
                false,
            )
        };

        if fresh_discovery {
            let active = namespaces
                .iter()
                .filter(|namespace| namespace.state == NamespaceState::Active)
                .map(GcNamespaceIncarnation::from_metadata)
                .collect::<BTreeSet<_>>();

            // A name can disappear or return with a new per-create identity
            // while this process still holds its old manifest generation floor.
            // Reconcile every successful discovery before any strong trigger
            // read so a removed/replaced incarnation starts cold.
            let changed_names = changed_namespace_names(&known_incarnations, &active);
            for namespace in changed_names {
                manifest_cache.invalidate_at(&namespace, compactor.clock().now());
            }
            known_incarnations = active.clone();
            gc_runner.retain_namespaces(&active);
        }

        debug!(
            namespace_count = namespaces.len(),
            tick, "compaction loop tick"
        );

        for ns in &namespaces {
            let deletion_recovery_required = ns.state == NamespaceState::Deleting
                || ((ns.state == NamespaceState::Active
                    || ns.state == NamespaceState::Creating)
                    && ns.deletion_intent.is_some());
            if deletion_recovery_required {
                gc_runner.forget_namespace(&ns.name);
                let Some(deletion_worker) = deletion_worker else {
                    error!(
                        namespace = %ns.name,
                        "governed deletion worker is not configured; deletion intent left untouched"
                    );
                    continue;
                };
                let namespace = match NamespaceId::new(ns.name.clone()) {
                    Ok(namespace) => namespace,
                    Err(error) => {
                        warn!(
                            namespace = %ns.name,
                            error = %error,
                            "invalid namespace identity in deletion maintenance"
                        );
                        continue;
                    }
                };
                match deletion_worker
                    .resume(&namespace, Duration::from_secs(25))
                    .await
                {
                    Ok(NamespaceDeleteOutcome::Deleted) => info!(
                        namespace = %namespace,
                        "resumed namespace delete completed"
                    ),
                    Ok(NamespaceDeleteOutcome::AlreadyDeleting) => warn!(
                        namespace = %namespace,
                        "resumed namespace delete remains in progress"
                    ),
                    Ok(NamespaceDeleteOutcome::BranchGraceWait { not_before }) => info!(
                        namespace = %namespace,
                        not_before = %not_before,
                        "branch delete retains its root through reader-safety grace"
                    ),
                    Err(ZeppelinError::NamespaceNotFound { .. }) => {
                        match deletion_worker
                            .confirm_missing_after_resume(&namespace, ns)
                            .await
                        {
                            Ok(()) => debug!(
                                namespace = %namespace,
                                "namespace delete already completed"
                            ),
                            Err(error) => error!(
                                namespace = %namespace,
                                error = %error,
                                "namespace delete completion could not be proven"
                            ),
                        }
                    }
                    Err(e) => {
                        warn!(
                            namespace = %namespace,
                            error = %e,
                            "failed to resume namespace delete"
                        );
                    }
                }
                continue;
            }
            if ns.state == NamespaceState::Creating {
                gc_runner.forget_namespace(&ns.name);
                warn!(
                    namespace = %ns.name,
                    "skipping namespace whose initial manifest is not yet active"
                );
                continue;
            }
            match gc_runner
                .run_cycle_at(
                    GcNamespaceIncarnation::from_metadata(ns),
                    compactor.clock().now(),
                )
                .await
            {
                Ok(report) => {
                    if report.objects_deleted > 0
                        || report.candidates_marked > 0
                        || report.candidates_skipped > 0
                        || report.pending_deletes_pruned > 0
                        || report.pending_deletes_retained > 0
                    {
                        info!(
                            namespace = %ns.name,
                            objects_deleted = report.objects_deleted,
                            pending_deletes_pruned = report.pending_deletes_pruned,
                            pending_deletes_retained = report.pending_deletes_retained,
                            candidates_marked = report.candidates_marked,
                            candidates_skipped = report.candidates_skipped,
                            bytes_reclaimed = report.bytes_reclaimed,
                            "storage gc cycle completed"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        namespace = %ns.name,
                        error = %e,
                        "storage gc cycle failed"
                    );
                }
            }

            let trigger =
                evaluate_compaction_trigger(compactor.as_ref(), manifest_cache.as_ref(), ns).await;

            match trigger {
                Ok(true) => {
                    // Compact under the per-namespace lease (acquire →
                    // heartbeat → compact → release). LeaseHeld means
                    // another node is on it — skip quietly, not a failure.
                    match compact_namespace_under_lease_with_lifecycle(
                        &compactor,
                        &lease_manager,
                        &ns.name,
                        &ns.full_text_search,
                        FragmentCachePolicy::ReadOnly(&cache),
                        lifecycle,
                    )
                    .await
                    {
                        Err(ZeppelinError::LeaseHeld { holder, .. }) => {
                            debug!(
                                namespace = %ns.name,
                                holder = %holder,
                                "compaction lease held by another node, skipping"
                            );
                        }
                        Ok(result) => {
                            crate::metrics::COMPACTIONS_TOTAL
                                .with_label_values(&[ns.name.as_str(), "success"])
                                .inc();
                            if let Err(e) =
                                namespace_manager.record_compaction_success(&ns.name).await
                            {
                                warn!(
                                    namespace = %ns.name,
                                    error = %e,
                                    "failed to record compaction success health"
                                );
                            }
                            // Invalidate manifest cache so queries see new segment.
                            manifest_cache.invalidate_at(&ns.name, compactor.clock().now());
                            info!(
                                namespace = %ns.name,
                                vectors_compacted = result.vectors_compacted,
                                fragments_removed = result.fragments_removed,
                                "compaction completed"
                            );
                            // Warm the new segment's index metadata (centroids /
                            // tree_meta) into the cache eagerly, so the first
                            // query after compaction doesn't pay the cold fetch.
                            // Background + best-effort: warming is an
                            // optimization; its failure must never affect the
                            // compaction loop (queries keep fail-loud fetches).
                            if result.segment_id.is_some() {
                                tokio::spawn(warm_segment_index_meta(
                                    compactor.store().clone(),
                                    cache.clone(),
                                    ns.name.clone(),
                                ));
                            }
                        }
                        Err(e) if lifecycle.is_retired() && *shutdown.borrow() => {
                            // Shutdown can race a periodic tick between its
                            // trigger read and lease reservation. Retirement is
                            // not an operational compaction failure: the
                            // process already withdrew the authority to begin
                            // another run, so do not persist a false health
                            // failure before leaving the loop.
                            debug!(
                                namespace = %ns.name,
                                "compaction admission retired during shutdown"
                            );
                            return;
                        }
                        Err(e) => {
                            crate::metrics::COMPACTIONS_TOTAL
                                .with_label_values(&[ns.name.as_str(), "failure"])
                                .inc();
                            if let Err(health_error) = namespace_manager
                                .record_compaction_failure(&ns.name, &e)
                                .await
                            {
                                warn!(
                                    namespace = %ns.name,
                                    error = %health_error,
                                    "failed to record compaction failure health"
                                );
                            }
                            warn!(namespace = %ns.name, error = %e, "compaction failed");
                        }
                    }
                }
                Ok(false) => {
                    debug!(namespace = %ns.name, "compaction not needed");
                }
                Err(e) => {
                    crate::metrics::COMPACTIONS_TOTAL
                        .with_label_values(&[ns.name.as_str(), "failure"])
                        .inc();
                    if let Err(health_error) = namespace_manager
                        .record_compaction_failure(&ns.name, &e)
                        .await
                    {
                        warn!(
                            namespace = %ns.name,
                            error = %health_error,
                            "failed to record compaction trigger failure health"
                        );
                    }
                    warn!(namespace = %ns.name, error = %e, "failed to check compaction status");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::{DateTime, Utc};

    use super::{changed_namespace_names, is_fresh_namespace_discovery_tick};
    use crate::compaction::gc::GcNamespaceIncarnation;
    use crate::namespace::manager::NamespaceIncarnationId;

    #[test]
    fn namespace_discovery_refreshes_on_first_and_every_twelfth_tick() {
        let refresh_ticks = (1..=36)
            .filter(|tick| is_fresh_namespace_discovery_tick(*tick))
            .collect::<Vec<_>>();

        assert_eq!(refresh_ticks, vec![1, 12, 24, 36]);
    }

    #[test]
    fn namespace_incarnation_diff_deduplicates_replacements_and_tracks_removals() {
        let Some(old_created) = DateTime::<Utc>::from_timestamp(1, 0) else {
            panic!("one second after the Unix epoch must be representable");
        };
        let old_incarnation = NamespaceIncarnationId::new();
        let new_incarnation = NamespaceIncarnationId::new();
        let known = BTreeSet::from([
            GcNamespaceIncarnation::with_incarnation_id(
                "recreated".to_string(),
                old_created,
                old_incarnation,
            ),
            GcNamespaceIncarnation::new("removed".to_string(), old_created),
            GcNamespaceIncarnation::new("unchanged".to_string(), old_created),
        ]);
        let active = BTreeSet::from([
            GcNamespaceIncarnation::with_incarnation_id(
                "recreated".to_string(),
                old_created,
                new_incarnation,
            ),
            GcNamespaceIncarnation::new("unchanged".to_string(), old_created),
        ]);

        assert_eq!(
            changed_namespace_names(&known, &active),
            BTreeSet::from(["recreated".to_string(), "removed".to_string()])
        );
        assert!(changed_namespace_names(&active, &active).is_empty());
    }
}
