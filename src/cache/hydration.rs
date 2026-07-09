//! Proactive warm-set hydration for manifest-visible segment artifacts.
//!
//! Normal queries fill [`crate::cache::DiskCache`] reactively after an
//! object-store miss.
//! This module can instead observe namespace heat or an administrative request,
//! plan the active segment's cluster/attribute/bitmap/global-FTS objects, and
//! fetch them in the background before later queries need them. Hydration is
//! only a performance optimization: it never changes manifest visibility and a
//! failed, refused, or dropped job cannot make an otherwise valid query wrong.
//!
//! [`crate::cache::hydration::SegmentHydrator`] owns a bounded non-blocking job
//! sender and one spawned worker. Jobs run sequentially, while the objects
//! inside one accepted job are fetched concurrently up to
//! [`HydrationConfig::parallelism`][crate::cache::hydration::HydrationConfig::parallelism].
//! Query threads never await this work; enqueue pressure and background
//! failures are recorded in metrics/logs instead.
//!
//! Current hydration deliberately refuses incremental segments because
//! carried clusters may be owned by older segment identities. It also refuses
//! a segment whose planned warm set exceeds the configured fraction of the
//! entire cache. Both refusals return success to the worker because retrying the
//! same plan would not change the precondition.
//!
//! ## Reading map
//!
//! 1. Start with [`crate::cache::hydration::HeatPolicy`],
//!    [`crate::cache::hydration::HeatDecision`], and
//!    [`crate::cache::hydration::SessionWindowPolicy`] for query-to-job
//!    decisions.
//! 2. Read [`crate::cache::hydration::HydrationConfig`] and
//!    [`SegmentHydrator::start`][crate::cache::hydration::SegmentHydrator::start]
//!    for worker setup.
//! 3. Follow
//!    [`SegmentHydrator::observe_query`][crate::cache::hydration::SegmentHydrator::observe_query]
//!    and
//!    [`SegmentHydrator::request_hydration`][crate::cache::hydration::SegmentHydrator::request_hydration]
//!    to the non-blocking queue.
//! 4. Read `worker_loop`, `run_job_with_retries`, and `hydrate_segment_once` for
//!    the background lifecycle.
//! 5. Finish with `plan_hydration_items`, `sidecar_keys`, and `hydrate_item` for
//!    artifact discovery, capacity accounting, and cache population.
//!
//! ## Job and authority flow
//!
//! ```text
//! query heat or admin request
//!             |
//!             | try_send; query never waits
//!             v
//! bounded channel ---- full/closed ----> log + metric; stop
//!             |
//!             v
//! single background worker
//!             |
//!             +-- incremental segment --> refuse; no retry
//!             +-- warm set too large ---> refuse; no retry
//!             |
//!             v
//! HEAD sidecars + use manifest cluster-object sizes
//!             |
//!             v
//! concurrent get_or_fetch calls (bounded by parallelism)
//!             |
//!             v
//! memory/disk cache populated; S3/MinIO and manifest remain authoritative
//! ```
//!
//! ## Invariants
//!
//! - The supplied [`crate::wal::manifest::SegmentRef`] must come from an
//!   authoritative manifest.
//! - Hydration never publishes a segment or mutates immutable objects.
//! - Capacity is checked before downloads using grouped-object manifest sizes
//!   plus object-store HEAD sizes for sidecars.
//! - A grouped cluster object whose fetched length differs from its published
//!   size is invalidated and fails the attempt.
//! - One job receives the initial attempt plus `max_retries`; retries preserve
//!   later channel order by blocking the single worker during backoff.
//! - The session policy emits at most one heat trigger per segment per active
//!   window, but a later window or segment rotation may trigger again.
//!
//! TODO(doc): Verify whether hydration must support legacy segments whose
//! `cluster_objects` list is empty. The current planner warms sidecars but does
//! not synthesize legacy one-object-per-cluster vector keys.
//!
//! ## Rust concepts used here
//!
//! `Arc<dyn HeatPolicy>` is trait-object dispatch: similar to a Java
//! interface reference or a C `(void *, vtable)` pair, with compiler-checked
//! thread safety from `Send + Sync`. The bounded Tokio
//! [`mpsc`][tokio::sync::mpsc] channel moves each owned `HydrationJob` from
//! query code to the worker, so the sender cannot mutate a queued job afterward.
//!
//! [`futures::StreamExt::buffer_unordered`] runs up to `parallelism` owned
//! download futures and yields results as they finish. [`DashMap`] lets the heat
//! policy update independent namespace windows concurrently. RAII through
//! `GaugeGuard` decrements the inflight metric even when `?` or an early return
//! leaves a retry path.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::config::{CacheConfig, HydrationPolicyKind};
use crate::error::{Result, ZeppelinError};
use crate::fts::global_index::global_fts_key;
use crate::index::bitmap::bitmap_key;
use crate::index::ivf_flat::build::attrs_key;
use crate::storage::ZeppelinStore;
use crate::wal::manifest::SegmentRef;

use super::DiskCache;

/// Maximum number of accepted jobs waiting behind the single worker.
const HYDRATION_JOB_QUEUE_CAPACITY: usize = 1024;
/// Operator-facing reason incremental segment hydration is currently gated.
const INCREMENTAL_GATE_MESSAGE: &str =
    "blocked on incremental carry-over bug, see main todo.md Task 2C precondition";

/// Result of observing query heat or an explicit hydration request.
///
/// The enum makes the only two policy outcomes exhaustive. It carries no job
/// data; [`SegmentHydrator`] combines `Hydrate` with the current manifest
/// segment supplied by its caller.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeatDecision {
    /// Leave the namespace on normal reactive cache behavior for this event.
    Stay,
    /// Attempt to enqueue hydration for the supplied active segment.
    Hydrate,
}

/// Synchronous policy boundary between query observations and background work.
///
/// Implementations must be cheap and thread-safe because query handlers call
/// them inline. They decide only whether to enqueue; they never read S3/MinIO,
/// inspect cache capacity, or perform hydration themselves.
///
/// # Rust Notes for Java/C Engineers
///
/// `Send + Sync` lets an `Arc<dyn HeatPolicy>` be shared among Tokio tasks.
/// Rust verifies that each implementation's fields support concurrent access;
/// Java would rely on the implementation to synchronize correctly, while C
/// would encode the same contract in documentation around function pointers.
pub trait HeatPolicy: Send + Sync {
    /// Observes one query against a namespace's active segment.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose heat state should be updated.
    /// - `segment_id`: Current manifest-visible segment. A rotation may reset
    ///   implementation-specific state.
    /// - `now`: Caller-supplied monotonic time, enabling deterministic policy
    ///   tests and avoiding wall-clock jumps.
    ///
    /// # Returns
    ///
    /// [`HeatDecision::Hydrate`] when this observation crosses the policy's
    /// trigger condition; otherwise [`HeatDecision::Stay`].
    ///
    /// # Examples
    ///
    /// A session policy with threshold three returns `Stay`, `Stay`, then
    /// `Hydrate` for three queries within its window.
    fn observe_query(&self, namespace: &str, segment_id: &str, now: Instant) -> HeatDecision;

    /// Decides whether an explicit external request should enqueue hydration.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace named by the administrative request.
    ///
    /// # Returns
    ///
    /// A policy decision. [`SessionWindowPolicy`] always returns `Hydrate` and
    /// does not alter its query-window counters.
    ///
    /// # Examples
    ///
    /// An administrator can request hydration for a cold namespace without
    /// manufacturing query observations.
    fn request_hydration(&self, namespace: &str) -> HeatDecision;
}

/// Constructs the configured query-heat policy as a shared trait object.
///
/// # Parameters
///
/// - `config`: Validated cache configuration containing policy kind, threshold,
///   and window duration.
///
/// # Returns
///
/// A reference-counted [`HeatPolicy`] implementation suitable for sharing with
/// query handlers and [`SegmentHydrator`].
///
/// # Errors
///
/// Returns a configuration error when the selected policy's threshold or
/// window is zero.
///
/// # Examples
///
/// A `SessionWindow` configuration with five queries in 60 seconds produces a
/// [`SessionWindowPolicy`] hidden behind `Arc<dyn HeatPolicy>`.
///
/// # Rust Notes for Java/C Engineers
///
/// The `match` is exhaustive over [`HydrationPolicyKind`], so adding a new enum
/// variant forces this factory to handle it at compile time.
pub fn heat_policy_from_config(config: &CacheConfig) -> Result<Arc<dyn HeatPolicy>> {
    match config.hydration_policy {
        HydrationPolicyKind::SessionWindow => Ok(Arc::new(SessionWindowPolicy::new(
            config.hydration_heat_queries,
            Duration::from_secs(config.hydration_heat_window_secs),
        )?)),
    }
}

/// Validated worker limits for proactive segment hydration.
///
/// This owned snapshot separates runtime behavior from later configuration
/// mutation. Retry count and backoff currently use fixed values established by
/// [`Self::from_cache_config`].
#[derive(Debug, Clone)]
pub struct HydrationConfig {
    /// Maximum in-flight object downloads within one job; always positive.
    pub parallelism: usize,
    /// Maximum fraction in `(0, 1]` of total cache bytes one job may plan.
    pub max_segment_fraction: f64,
    /// Number of retry attempts after the initial attempt.
    pub max_retries: usize,
    /// Delay before each retry; the single worker processes no later job then.
    pub retry_backoff: Duration,
}

impl HydrationConfig {
    /// Builds validated worker limits from the global cache configuration.
    ///
    /// # Parameters
    ///
    /// - `config`: Cache configuration containing hydration parallelism and the
    ///   maximum per-segment cache fraction.
    ///
    /// # Returns
    ///
    /// An owned runtime snapshot with the configured limits plus two retries
    /// and a 250 ms retry backoff.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when parallelism is zero or when the
    /// fraction is non-finite, non-positive, or greater than one.
    ///
    /// # Examples
    ///
    /// Parallelism eight and fraction `0.25` allow eight simultaneous object
    /// fetches for a segment whose planned warm set uses at most one quarter of
    /// the cache budget.
    pub fn from_cache_config(config: &CacheConfig) -> Result<Self> {
        if config.hydration_parallelism == 0 {
            return Err(ZeppelinError::Config(
                "cache.hydration_parallelism must be greater than zero".into(),
            ));
        }
        if !config.hydration_max_segment_fraction.is_finite()
            || config.hydration_max_segment_fraction <= 0.0
            || config.hydration_max_segment_fraction > 1.0
        {
            return Err(ZeppelinError::Config(
                "cache.hydration_max_segment_fraction must be finite and in (0, 1]".into(),
            ));
        }
        Ok(Self {
            parallelism: config.hydration_parallelism,
            max_segment_fraction: config.hydration_max_segment_fraction,
            max_retries: 2,
            retry_backoff: Duration::from_millis(250),
        })
    }
}

/// Origin recorded for an accepted hydration job and its metrics.
#[derive(Debug, Clone, Copy)]
pub enum HydrationTrigger {
    /// Query observations crossed the configured heat threshold.
    Heat,
    /// An administrator explicitly requested the active segment.
    Admin,
}

impl HydrationTrigger {
    /// Returns the stable low-cardinality label used by metrics and logs.
    ///
    /// # Returns
    ///
    /// Either `"heat"` or `"admin"`, borrowed from static program data.
    ///
    /// # Examples
    ///
    /// An administrative job increments metrics under the `admin` label.
    fn as_str(self) -> &'static str {
        match self {
            Self::Heat => "heat",
            Self::Admin => "admin",
        }
    }
}

/// Owned message moved from a query/admin caller into the worker channel.
#[derive(Debug)]
struct HydrationJob {
    /// Namespace used for sidecar keys, logs, and metrics.
    namespace: String,
    /// Cloned manifest segment snapshot used to plan immutable objects.
    segment: SegmentRef,
    /// Source label retained across retries.
    trigger: HydrationTrigger,
}

/// Metric category and planning role for a hydrated object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HydrationObjectKind {
    /// Grouped immutable vector/ID cluster payload.
    Cluster,
    /// Per-cluster attribute sidecar.
    Attrs,
    /// Per-cluster metadata bitmap sidecar.
    Bitmap,
    /// Segment-global lexical index sidecar.
    Fts,
}

impl HydrationObjectKind {
    /// Returns the stable metric label for this artifact category.
    ///
    /// # Returns
    ///
    /// One of `cluster`, `attrs`, `bitmap`, or `fts`.
    ///
    /// # Examples
    ///
    /// A warmed bitmap sidecar increments the `bitmap` object/byte counters.
    fn as_str(self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
            Self::Attrs => "attrs",
            Self::Bitmap => "bitmap",
            Self::Fts => "fts",
        }
    }
}

/// One object key and size evidence in a hydration plan.
#[derive(Debug, Clone)]
struct HydrationItem {
    /// Full object-store/cache key.
    key: String,
    /// Category used for success metrics.
    kind: HydrationObjectKind,
    /// Bytes charged to the pre-download capacity budget.
    size_bytes: u64,
    /// Published length that fetched bytes must match, when available.
    ///
    /// Grouped cluster objects carry manifest sizes. Sidecars use HEAD only for
    /// planning and therefore leave this as `None`.
    expected_size: Option<u64>,
}

/// Non-blocking query/admin handle for one background hydration worker.
///
/// The handle owns a shared policy and bounded channel sender. The spawned
/// worker owns the store, cache, runtime configuration, and receiver. Dropping
/// all handles closes the channel; the worker exits after draining accepted jobs.
pub struct SegmentHydrator {
    /// Shared synchronous heat policy used on the caller path.
    policy: Arc<dyn HeatPolicy>,
    /// Bounded sender used with `try_send` so callers never await capacity.
    jobs: mpsc::Sender<HydrationJob>,
}

impl SegmentHydrator {
    /// Spawns one background worker and returns its shared enqueue handle.
    ///
    /// # Parameters
    ///
    /// - `store`: Owned object-store client moved into the worker.
    /// - `cache`: Shared disk/memory cache populated by the worker.
    /// - `policy`: Shared synchronous heat policy used by caller tasks.
    /// - `config`: Validated concurrency, capacity, and retry limits.
    ///
    /// # Returns
    ///
    /// An [`Arc`] containing the caller-facing handle. The worker begins running
    /// on the current Tokio runtime immediately.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime because [`tokio::spawn`] needs
    /// an active executor.
    ///
    /// # Side Effects
    ///
    /// Allocates a 1,024-slot channel and spawns a detached task. Dropping the
    /// returned handle closes the sender only after all other `Arc` clones drop.
    ///
    /// # Examples
    ///
    /// Startup creates one hydrator and shares its `Arc` in server state. Query
    /// handlers submit observations while the worker owns all download awaits.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// `tokio::spawn` requires an owned `Send + 'static` future. Moving `store`,
    /// `cache`, `config`, and the receiver into `worker_loop` proves the task
    /// cannot outlive borrowed stack data. Java captures heap references; C
    /// would require manual lifetime coordination for the thread context.
    pub fn start(
        store: ZeppelinStore,
        cache: Arc<DiskCache>,
        policy: Arc<dyn HeatPolicy>,
        config: HydrationConfig,
    ) -> Arc<Self> {
        let (jobs, rx) = mpsc::channel(HYDRATION_JOB_QUEUE_CAPACITY);
        tokio::spawn(worker_loop(store, cache, config, rx));
        Arc::new(Self { policy, jobs })
    }

    /// Records query heat and attempts a non-blocking enqueue when triggered.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace whose heat metric/policy state should advance.
    /// - `segment`: Current manifest-visible segment to clone into a triggered
    ///   job.
    ///
    /// # Returns
    ///
    /// Returns unit immediately. Policy `Stay`, a full channel, a closed worker,
    /// and later job failure do not become query errors.
    ///
    /// # Side Effects
    ///
    /// Increments namespace heat, mutates policy state, and may enqueue an owned
    /// job or record an enqueue failure.
    ///
    /// # Consistency
    ///
    /// The caller must supply the segment selected from the same manifest
    /// snapshot as the query. Hydration never changes that snapshot.
    ///
    /// # Performance
    ///
    /// Performs no object-store I/O and never awaits. A triggered call clones
    /// the segment metadata before `try_send`.
    ///
    /// # Examples
    ///
    /// The third query in a hot window may enqueue the active `seg-42`. If the
    /// queue is full, that query still executes normally on the cold path.
    pub fn observe_query(&self, namespace: &str, segment: &SegmentRef) {
        crate::metrics::NAMESPACE_HEAT
            .with_label_values(&[namespace])
            .inc();
        if self
            .policy
            .observe_query(namespace, &segment.id, Instant::now())
            == HeatDecision::Hydrate
        {
            self.enqueue(namespace, segment, HydrationTrigger::Heat);
        }
    }

    /// Applies policy to an explicit request and attempts a non-blocking enqueue.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace named by the administrative request.
    /// - `segment`: Current manifest-visible segment to hydrate.
    ///
    /// # Returns
    ///
    /// Returns unit whether the policy stays, the enqueue succeeds, or the
    /// bounded channel refuses it.
    ///
    /// # Side Effects
    ///
    /// May clone/enqueue a job and update job/failure metrics and logs.
    ///
    /// # Examples
    ///
    /// An operator can request `catalog` immediately after startup; the session
    /// policy returns `Hydrate` without waiting for its query threshold.
    pub fn request_hydration(&self, namespace: &str, segment: &SegmentRef) {
        if self.policy.request_hydration(namespace) == HeatDecision::Hydrate {
            self.enqueue(namespace, segment, HydrationTrigger::Admin);
        }
    }

    /// Clones one job and submits it with `try_send`.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace copied into the owned channel message.
    /// - `segment`: Manifest segment cloned for planning after the caller returns.
    /// - `trigger`: Heat/admin source used by metrics and structured logs.
    ///
    /// # Returns
    ///
    /// Returns unit. Success increments the accepted-job counter; full/closed
    /// channel errors increment failures and are logged.
    ///
    /// # Side Effects
    ///
    /// Allocates owned namespace/segment state and mutates channel/metrics/logs.
    ///
    /// # Examples
    ///
    /// A heat-triggered `seg-42` job enters the queue with label `heat`. If all
    /// 1,024 slots are occupied, it is dropped and recorded rather than awaited.
    fn enqueue(&self, namespace: &str, segment: &SegmentRef, trigger: HydrationTrigger) {
        let job = HydrationJob {
            namespace: namespace.to_string(),
            segment: segment.clone(),
            trigger,
        };
        match self.jobs.try_send(job) {
            Ok(()) => {
                crate::metrics::HYDRATION_JOBS_TOTAL
                    .with_label_values(&[trigger.as_str()])
                    .inc();
            }
            Err(error) => {
                crate::metrics::HYDRATION_FAILURES_TOTAL.inc();
                error!(
                    namespace,
                    segment_id = %segment.id,
                    trigger = trigger.as_str(),
                    error = %error,
                    "failed to enqueue hydration job"
                );
            }
        }
    }
}

/// Receives jobs serially and runs each through retry handling.
///
/// # Parameters
///
/// - `store`: Owned object-store client used by all jobs.
/// - `cache`: Shared destination cache.
/// - `config`: Owned worker limits.
/// - `rx`: Sole receiver for the bounded job channel.
///
/// # Returns
///
/// Returns when every sender is dropped and all accepted messages are drained.
/// Job failures are logged/metricized inside retry handling and do not stop the
/// loop.
///
/// # Performance
///
/// Jobs are processed one at a time. Object downloads within the current job
/// may run concurrently; retry backoff delays later queued jobs.
///
/// # Examples
///
/// If `catalog` fails its first object GET, its retry/backoff completes before
/// the worker starts the next queued `inventory` job.
async fn worker_loop(
    store: ZeppelinStore,
    cache: Arc<DiskCache>,
    config: HydrationConfig,
    mut rx: mpsc::Receiver<HydrationJob>,
) {
    let mut capacity_refusals_logged = HashMap::new();
    while let Some(job) = rx.recv().await {
        run_job_with_retries(&store, &cache, &config, job, &mut capacity_refusals_logged).await;
    }
}

/// Runs one job until success or its retry budget is exhausted.
///
/// # Parameters
///
/// - `store`: Shared worker-owned object-store client.
/// - `cache`: Shared destination cache.
/// - `config`: Retry and hydration limits.
/// - `job`: Owned job retained unchanged across attempts.
/// - `capacity_refusals_logged`: Per-worker namespace/segment suppression map
///   for repeated capacity warnings.
///
/// # Returns
///
/// Returns unit. Final failure is observable through metrics/logs rather than
/// propagated because no query awaits the worker.
///
/// # Side Effects
///
/// Increments/decrements the inflight gauge, records every failed attempt, logs
/// errors, and sleeps between retryable attempts.
///
/// # Examples
///
/// With `max_retries = 2`, a missing sidecar can produce three attempts. A
/// capacity refusal returns `Ok(())` from one attempt and is not retried.
///
/// # Rust Notes for Java/C Engineers
///
/// `GaugeGuard` is RAII, comparable to Java `try/finally` or a C cleanup label:
/// its destructor restores the gauge on every return path. The owned `job`
/// remains usable across loop iterations because each attempt only borrows it.
async fn run_job_with_retries(
    store: &ZeppelinStore,
    cache: &Arc<DiskCache>,
    config: &HydrationConfig,
    job: HydrationJob,
    capacity_refusals_logged: &mut HashMap<String, String>,
) {
    crate::metrics::HYDRATION_INFLIGHT.inc();
    let _inflight_guard = crate::metrics::GaugeGuard(&crate::metrics::HYDRATION_INFLIGHT);
    let mut attempt = 0usize;
    loop {
        match hydrate_segment_once(store, cache, config, &job, capacity_refusals_logged).await {
            Ok(()) => return,
            Err(error) => {
                crate::metrics::HYDRATION_FAILURES_TOTAL.inc();
                error!(
                    namespace = %job.namespace,
                    segment_id = %job.segment.id,
                    trigger = job.trigger.as_str(),
                    attempt,
                    error = %error,
                    "hydration job failed"
                );
                if attempt >= config.max_retries {
                    return;
                }
                attempt += 1;
                tokio::time::sleep(config.retry_backoff).await;
            }
        }
    }
}

/// Plans, capacity-checks, and hydrates one segment attempt.
///
/// # Parameters
///
/// - `store`: Object-store client used for HEAD/GET requests.
/// - `cache`: Destination cache and total byte budget source.
/// - `config`: Per-job concurrency and capacity-fraction limits.
/// - `job`: Borrowed namespace, segment snapshot, and trigger.
/// - `capacity_refusals_logged`: Warning-suppression state updated on refusal or
///   cleared after success.
///
/// # Returns
///
/// `Ok(())` after successful warming **or** a deliberate incremental/capacity
/// refusal. Other planning/download/validation failures return `Err` for retry.
///
/// # Errors
///
/// Propagates sidecar HEAD failures, byte-sum/capacity arithmetic errors,
/// cache/object-store fetch failures, and grouped-object length mismatches.
/// Some earlier items may already be cached when a later concurrent item fails.
///
/// # Side Effects
///
/// Reads object metadata/data, populates cache tiers, invalidates a grouped
/// object on length mismatch, updates hydration metrics, and logs refusals.
///
/// # Consistency
///
/// Hydration uses exactly the immutable references in `job.segment`; it does
/// not re-read or publish a manifest. Cache population cannot make the segment
/// visible independently of that manifest.
///
/// # Performance
///
/// Planning performs one HEAD per sidecar. Accepted items then execute at most
/// `parallelism` `get_or_fetch` futures concurrently. Capacity is checked before
/// those data fetches.
///
/// # Examples
///
/// A 2 GiB warm set under a 10 GiB cache and fraction `0.25` is accepted. A
/// 3 GiB set is refused without downloads. If one grouped object returns a
/// different length, its cache entry is invalidated and the attempt fails.
async fn hydrate_segment_once(
    store: &ZeppelinStore,
    cache: &Arc<DiskCache>,
    config: &HydrationConfig,
    job: &HydrationJob,
    capacity_refusals_logged: &mut HashMap<String, String>,
) -> Result<()> {
    if is_incremental_segment(&job.segment) {
        crate::metrics::HYDRATION_SKIPPED_TOTAL
            .with_label_values(&["incremental_segment"])
            .inc();
        warn!(
            namespace = %job.namespace,
            segment_id = %job.segment.id,
            reason = "incremental_segment",
            tracked_bug = INCREMENTAL_GATE_MESSAGE,
            "warm-set hydration refused for incremental segment"
        );
        return Ok(());
    }

    let items = plan_hydration_items(store, job).await?;
    let required_bytes = hydration_items_bytes(&items, &job.segment)?;
    crate::metrics::HYDRATION_REQUIRED_BYTES
        .with_label_values(&[&job.namespace])
        .set(required_bytes as f64);
    let capacity_limit = hydration_capacity_limit(cache.max_size_bytes(), config)?;
    if required_bytes > capacity_limit {
        crate::metrics::HYDRATION_SKIPPED_TOTAL
            .with_label_values(&["capacity"])
            .inc();
        record_capacity_refusal(
            job,
            required_bytes,
            cache.max_size_bytes(),
            config,
            capacity_refusals_logged,
        );
        return Ok(());
    }

    let mut stream = futures::stream::iter(items)
        .map(|item| hydrate_item(store.clone(), Arc::clone(cache), item))
        .buffer_unordered(config.parallelism);
    while let Some(result) = stream.next().await {
        result?;
    }
    crate::metrics::HYDRATION_REFUSED
        .with_label_values(&[job.namespace.as_str(), "capacity"])
        .set(0);
    capacity_refusals_logged.remove(&job.namespace);
    Ok(())
}

/// Records and rate-limits an operator-visible capacity refusal.
///
/// # Parameters
///
/// - `job`: Refused namespace and segment.
/// - `required_bytes`: Planned warm-set size.
/// - `cache_max_size_bytes`: Entire cache byte budget.
/// - `config`: Fraction used to calculate the refusal boundary.
/// - `capacity_refusals_logged`: Namespace-to-segment map suppressing duplicate
///   warning logs for the same active segment.
///
/// # Returns
///
/// Returns unit after setting the refusal gauge. It logs and increments the
/// refusal-log counter only when the namespace's refused segment changes.
///
/// # Side Effects
///
/// Mutates metrics, the suppression map, and possibly structured logs.
///
/// # Examples
///
/// Repeated hot queries for refused `seg-42` keep the gauge set but produce one
/// warning. After the namespace rotates to `seg-43`, the first refusal logs the
/// new segment and remediation again.
fn record_capacity_refusal(
    job: &HydrationJob,
    required_bytes: u64,
    cache_max_size_bytes: u64,
    config: &HydrationConfig,
    capacity_refusals_logged: &mut HashMap<String, String>,
) {
    crate::metrics::HYDRATION_REFUSED
        .with_label_values(&[job.namespace.as_str(), "capacity"])
        .set(1);

    let should_log = capacity_refusals_logged
        .get(&job.namespace)
        .map(|segment_id| segment_id != &job.segment.id)
        .unwrap_or(true);
    if !should_log {
        return;
    }

    capacity_refusals_logged.insert(job.namespace.clone(), job.segment.id.clone());
    crate::metrics::HYDRATION_REFUSAL_LOGS_TOTAL
        .with_label_values(&[job.namespace.as_str(), "capacity"])
        .inc();
    warn!(
        namespace = %job.namespace,
        segment_id = %job.segment.id,
        segment_bytes = required_bytes,
        cache_max_bytes = cache_max_size_bytes,
        max_fraction = config.max_segment_fraction,
        remediation = "raise cache.hydration_max_segment_fraction, raise cache.max_size_gb/cache.max_size_bytes with a larger NVMe/node, or deliberately leave this namespace on the cold path",
        "warm-set hydration refused: segment exceeds capacity fraction"
    );
}

/// Detects whether any logical cluster is owned by an older segment.
///
/// # Parameters
///
/// - `segment`: Manifest segment whose cluster-owner map should be inspected.
///
/// # Returns
///
/// `true` when at least one owner differs from `segment.id`; `false` for a full
/// rebuild or an empty legacy owner map.
///
/// # Examples
///
/// Owners `["seg-42", "seg-40"]` on segment `seg-42` are incremental. An empty
/// owner list is treated as non-incremental.
fn is_incremental_segment(segment: &SegmentRef) -> bool {
    segment
        .cluster_owners
        .iter()
        .any(|owner| owner != &segment.id)
}

/// Builds the complete capacity/download plan for a non-incremental segment.
///
/// Grouped cluster objects already carry published sizes in the manifest.
/// Attribute, optional bitmap, and optional global-FTS sidecars require HEAD
/// requests to discover their sizes before the capacity decision.
///
/// # Parameters
///
/// - `store`: Object-store client used for sidecar HEAD requests.
/// - `job`: Namespace and manifest segment supplying keys and object refs.
///
/// # Returns
///
/// Ordered items: manifest cluster objects first, followed by generated
/// sidecars. Each sidecar carries its HEAD size for budgeting.
///
/// # Errors
///
/// Returns a storage error if any required sidecar cannot be headed. No data GET
/// occurs in this phase, but earlier HEAD requests may already have completed.
///
/// # Side Effects
///
/// Performs one object-store HEAD for every generated sidecar key.
///
/// # Performance
///
/// Sidecar HEAD requests currently run sequentially. Item memory grows with
/// grouped-object count plus roughly one or two sidecars per cluster and one
/// optional global FTS object.
///
/// # Examples
///
/// A ten-cluster bitmap segment with two grouped data objects and global FTS
/// plans two manifest-sized clusters, ten attributes, ten bitmaps, and one FTS
/// item.
async fn plan_hydration_items(
    store: &ZeppelinStore,
    job: &HydrationJob,
) -> Result<Vec<HydrationItem>> {
    let mut items: Vec<HydrationItem> = job
        .segment
        .cluster_objects
        .iter()
        .map(|object| HydrationItem {
            key: object.key.clone(),
            kind: HydrationObjectKind::Cluster,
            size_bytes: object.size_bytes,
            expected_size: (object.size_bytes != 0).then_some(object.size_bytes),
        })
        .collect();

    for (key, kind) in sidecar_keys(&job.namespace, &job.segment) {
        let meta = store.head(&key).await?;
        items.push(HydrationItem {
            key,
            kind,
            size_bytes: meta.size as u64,
            expected_size: None,
        });
    }

    Ok(items)
}

/// Generates required attribute, bitmap, and global-FTS sidecar keys.
///
/// # Parameters
///
/// - `namespace`: Namespace prefix for all generated keys.
/// - `segment`: Manifest metadata containing cluster count, owner routing,
///   bitmap-field presence, and global-FTS presence.
///
/// # Returns
///
/// Attributes for every cluster, bitmaps for every cluster when any bitmap
/// field exists, and one global FTS key when advertised. Ordering follows
/// cluster index, with attributes before bitmap, then global FTS last.
///
/// # Consistency
///
/// Per-cluster keys route through [`SegmentRef::cluster_owner`]. Incremental
/// segments are currently refused before this helper is called.
///
/// # Examples
///
/// A two-cluster segment with bitmap fields and no FTS returns four keys:
/// attrs/bitmap for cluster 0, then attrs/bitmap for cluster 1.
fn sidecar_keys(namespace: &str, segment: &SegmentRef) -> Vec<(String, HydrationObjectKind)> {
    let mut keys = Vec::with_capacity(segment.cluster_count * 2 + 1);
    for cluster_idx in 0..segment.cluster_count {
        let owner = segment.cluster_owner(cluster_idx);
        keys.push((
            attrs_key(namespace, owner, cluster_idx),
            HydrationObjectKind::Attrs,
        ));
        if !segment.bitmap_fields.is_empty() {
            keys.push((
                bitmap_key(namespace, owner, cluster_idx),
                HydrationObjectKind::Bitmap,
            ));
        }
    }
    if segment.has_global_fts {
        keys.push((
            global_fts_key(namespace, &segment.id),
            HydrationObjectKind::Fts,
        ));
    }
    keys
}

/// Sums planned object sizes with overflow detection.
///
/// # Parameters
///
/// - `items`: Borrowed hydration plan whose sizes should be charged.
/// - `segment`: Segment used to identify an overflow error.
///
/// # Returns
///
/// Total planned bytes when every checked addition succeeds.
///
/// # Errors
///
/// Returns a cache error if the `u64` sum would overflow. No partial total is
/// returned or used for the capacity decision.
///
/// # Examples
///
/// Items of 100 and 250 bytes return 350. An artificial plan exceeding
/// `u64::MAX` fails rather than wrapping to a small accepted budget.
fn hydration_items_bytes(items: &[HydrationItem], segment: &SegmentRef) -> Result<u64> {
    items.iter().try_fold(0u64, |acc, item| {
        acc.checked_add(item.size_bytes).ok_or_else(|| {
            ZeppelinError::Cache(format!(
                "hydration byte budget overflows for segment {}",
                segment.id
            ))
        })
    })
}

/// Calculates the maximum warm-set bytes allowed for one segment.
///
/// # Parameters
///
/// - `cache_max_size_bytes`: Total configured cache byte budget.
/// - `config`: Validated fraction in `(0, 1]`.
///
/// # Returns
///
/// `floor(cache bytes * fraction)` as `u64`.
///
/// # Errors
///
/// Returns a cache error if floating-point conversion/multiplication produces
/// a non-finite or negative value.
///
/// # Examples
///
/// A 1,000-byte cache and fraction `0.25` allow a 250-byte planned segment.
fn hydration_capacity_limit(cache_max_size_bytes: u64, config: &HydrationConfig) -> Result<u64> {
    let limit = (cache_max_size_bytes as f64) * config.max_segment_fraction;
    if !limit.is_finite() || limit < 0.0 {
        return Err(ZeppelinError::Cache(
            "hydration capacity limit is not finite".into(),
        ));
    }
    Ok(limit.floor() as u64)
}

/// Populates one cache key and validates a published length when available.
///
/// # Parameters
///
/// - `store`: Owned clone used by the fetch closure after this future starts.
/// - `cache`: Shared cache serving a hit or coalescing/fetching a miss.
/// - `item`: Owned key, kind, planned size, and optional expected length.
///
/// # Returns
///
/// `Ok(())` after the key is present and any expected length matches.
///
/// # Errors
///
/// Propagates cache/object-store failures. A length mismatch invalidates the key
/// and returns a cache error; invalidation failure is returned first through `?`.
///
/// # Side Effects
///
/// May perform one object GET and write cache tiers, may invalidate a bad entry,
/// and increments per-kind object/byte metrics after success.
///
/// # Consistency
///
/// [`DiskCache::get_or_fetch`] is an optimization around the immutable remote
/// object. Published grouped-object size remains the validation boundary.
///
/// # Performance
///
/// Cloning the store and [`Arc`] is cheap shared-handle work. The returned bytes
/// may come from memory/disk without network I/O or from one coalesced GET.
///
/// # Examples
///
/// A grouped object advertised as 4 MiB is accepted only at exactly 4 MiB. A
/// cached 3 MiB value is invalidated and causes this attempt to fail.
///
/// # Rust Notes for Java/C Engineers
///
/// The `move` closure owns `fetch_key` and a store clone so it can safely run
/// after the outer stack frame suspends. Rust prevents it from borrowing a
/// temporary key that might disappear during `.await`.
async fn hydrate_item(
    store: ZeppelinStore,
    cache: Arc<DiskCache>,
    item: HydrationItem,
) -> Result<()> {
    let key = item.key.clone();
    let fetch_key = key.clone();
    let bytes = cache
        .get_or_fetch(&key, move || {
            let store = store.clone();
            async move { store.get(&fetch_key).await }
        })
        .await?;
    let actual = bytes.len() as u64;
    if let Some(expected) = item.expected_size {
        if actual != expected {
            cache.invalidate(&key).await?;
            return Err(ZeppelinError::Cache(format!(
                "hydrated object length mismatch for {key}: expected={expected}, actual={actual}",
            )));
        }
    }
    crate::metrics::HYDRATION_OBJECTS_TOTAL
        .with_label_values(&[item.kind.as_str()])
        .inc();
    crate::metrics::HYDRATION_BYTES_TOTAL
        .with_label_values(&[item.kind.as_str()])
        .inc_by(actual);
    Ok(())
}

/// Per-namespace policy that triggers after N queries within a fixed window.
///
/// State resets when the active segment changes or when an observation arrives
/// strictly later than `window` after `window_start`. Once triggered, later
/// observations of that segment stay quiet for the rest of the window; a new
/// window may trigger it again.
///
/// # Examples
///
/// With threshold three and a 60-second window, observations at seconds 0, 10,
/// and 20 return `Stay`, `Stay`, and `Hydrate`. A fourth at second 30 stays. An
/// observation after the window begins a new count.
#[derive(Debug)]
pub struct SessionWindowPolicy {
    /// Query count required to trigger; always greater than zero.
    threshold: u64,
    /// Maximum duration from window start accepted into one count window.
    window: Duration,
    /// Mutable heat window keyed by namespace.
    states: DashMap<String, WindowState>,
}

/// Mutable state for one namespace's current segment/window.
#[derive(Debug, Clone)]
struct WindowState {
    /// Active segment for which the current count was observed.
    segment_id: String,
    /// Monotonic start of this fixed window.
    window_start: Instant,
    /// Saturating number of observations in the current window.
    count: u64,
    /// Segment already triggered in this window, if any.
    triggered_segment_id: Option<String>,
}

impl WindowState {
    /// Starts a namespace window with its first observation already counted.
    ///
    /// # Parameters
    ///
    /// - `segment_id`: Active segment copied into policy state.
    /// - `now`: Monotonic time used as the fixed window start.
    ///
    /// # Returns
    ///
    /// State with `count = 1` and no prior trigger.
    ///
    /// # Examples
    ///
    /// The first `seg-42` query creates a window that can immediately trigger a
    /// threshold-one policy.
    fn new(segment_id: &str, now: Instant) -> Self {
        Self {
            segment_id: segment_id.to_string(),
            window_start: now,
            count: 1,
            triggered_segment_id: None,
        }
    }
}

impl SessionWindowPolicy {
    /// Creates an empty validated session-window policy.
    ///
    /// # Parameters
    ///
    /// - `threshold`: Positive query count required within one window.
    /// - `window`: Positive fixed window duration.
    ///
    /// # Returns
    ///
    /// A policy with no namespace state.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when either parameter is zero.
    ///
    /// # Examples
    ///
    /// `SessionWindowPolicy::new(3, Duration::from_secs(60))` triggers on the
    /// third observation inside each namespace/segment window.
    pub fn new(threshold: u64, window: Duration) -> Result<Self> {
        if threshold == 0 {
            return Err(ZeppelinError::Config(
                "session-window hydration threshold must be greater than zero".into(),
            ));
        }
        if window.is_zero() {
            return Err(ZeppelinError::Config(
                "session-window hydration window must be greater than zero".into(),
            ));
        }
        Ok(Self {
            threshold,
            window,
            states: DashMap::new(),
        })
    }

    /// Converts current count/trigger state into one idempotent decision.
    ///
    /// # Parameters
    ///
    /// - `state`: Exclusively borrowed namespace state after count/reset logic.
    ///
    /// # Returns
    ///
    /// `Hydrate` once when the threshold is reached for a segment; all later
    /// calls for the same state return `Stay` until reset.
    ///
    /// # Side Effects
    ///
    /// Records `triggered_segment_id` on the first hydrate decision.
    ///
    /// # Examples
    ///
    /// At threshold two, count two records the current segment and returns
    /// `Hydrate`; count three for that segment returns `Stay`.
    fn decision_for_state(&self, state: &mut WindowState) -> HeatDecision {
        if state.count >= self.threshold
            && state.triggered_segment_id.as_deref() != Some(state.segment_id.as_str())
        {
            state.triggered_segment_id = Some(state.segment_id.clone());
            HeatDecision::Hydrate
        } else {
            HeatDecision::Stay
        }
    }
}

impl HeatPolicy for SessionWindowPolicy {
    /// Updates one namespace window and emits a one-shot heat decision.
    ///
    /// A segment change resets count to one. An observation strictly outside
    /// the fixed window also resets count and trigger state. Otherwise the count
    /// saturates upward before decision evaluation.
    ///
    /// # Parameters
    ///
    /// - `namespace`: Namespace key whose DashMap entry is updated atomically.
    /// - `segment_id`: Current active segment, used to detect rotation.
    /// - `now`: Monotonic observation time.
    ///
    /// # Returns
    ///
    /// One [`HeatDecision`] for this observation.
    ///
    /// # Side Effects
    ///
    /// Creates or mutates one DashMap entry.
    ///
    /// # Examples
    ///
    /// After `seg-a` has triggered, the first `seg-b` query resets state and
    /// stays; enough subsequent `seg-b` queries can trigger again.
    ///
    /// # Rust Notes for Java/C Engineers
    ///
    /// DashMap's entry API holds exclusive access to this namespace shard entry
    /// while the closure mutates it. The mutable borrow cannot escape the guard,
    /// preventing use after another thread could replace the state.
    fn observe_query(&self, namespace: &str, segment_id: &str, now: Instant) -> HeatDecision {
        match self.states.entry(namespace.to_string()) {
            Entry::Vacant(entry) => {
                let mut state = WindowState::new(segment_id, now);
                let decision = self.decision_for_state(&mut state);
                entry.insert(state);
                decision
            }
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                if state.segment_id != segment_id {
                    *state = WindowState::new(segment_id, now);
                } else if now.saturating_duration_since(state.window_start) > self.window {
                    state.window_start = now;
                    state.count = 1;
                    state.triggered_segment_id = None;
                } else {
                    state.count = state.count.saturating_add(1);
                }
                self.decision_for_state(state)
            }
        }
    }

    /// Always accepts an explicit administrative request.
    ///
    /// # Parameters
    ///
    /// - `_namespace`: Unused because this policy does not throttle admin jobs.
    ///
    /// # Returns
    ///
    /// Always [`HeatDecision::Hydrate`]. Query-window state is unchanged.
    ///
    /// # Examples
    ///
    /// An admin request may hydrate a namespace whose query count is still zero.
    fn request_hydration(&self, _namespace: &str) -> HeatDecision {
        HeatDecision::Hydrate
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for deterministic session-window transitions and trait use.

    use std::time::{Duration, Instant};

    use super::{HeatDecision, HeatPolicy, SessionWindowPolicy};

    /// Constructs a policy or fails the test with the configuration error.
    ///
    /// # Parameters
    ///
    /// - `threshold`: Test query threshold.
    /// - `window`: Test window duration.
    ///
    /// # Returns
    ///
    /// A validated policy. Invalid fixtures panic with a useful message.
    fn test_policy(threshold: u64, window: Duration) -> SessionWindowPolicy {
        match SessionWindowPolicy::new(threshold, window) {
            Ok(policy) => policy,
            Err(error) => panic!("test policy construction failed: {error}"),
        }
    }

    /// Triggers exactly on the threshold and suppresses repeats in the window.
    #[test]
    fn test_session_window_triggers_at_threshold() {
        let policy = test_policy(3, Duration::from_secs(60));
        let now = Instant::now();

        assert_eq!(policy.observe_query("ns", "seg-a", now), HeatDecision::Stay);
        assert_eq!(
            policy.observe_query("ns", "seg-a", now + Duration::from_secs(1)),
            HeatDecision::Stay
        );
        assert_eq!(
            policy.observe_query("ns", "seg-a", now + Duration::from_secs(2)),
            HeatDecision::Hydrate
        );
        assert_eq!(
            policy.observe_query("ns", "seg-a", now + Duration::from_secs(3)),
            HeatDecision::Stay,
            "same segment should not enqueue repeatedly after the first trigger"
        );
    }

    /// Resets a sparse observation stream instead of accumulating across windows.
    #[test]
    fn test_session_window_resets_outside_window() {
        let policy = test_policy(2, Duration::from_secs(10));
        let now = Instant::now();

        assert_eq!(policy.observe_query("ns", "seg-a", now), HeatDecision::Stay);
        assert_eq!(
            policy.observe_query("ns", "seg-a", now + Duration::from_secs(11)),
            HeatDecision::Stay
        );
        assert_eq!(
            policy.observe_query("ns", "seg-a", now + Duration::from_secs(22)),
            HeatDecision::Stay
        );
    }

    /// Resets triggered state when the manifest rotates to a new segment.
    #[test]
    fn test_segment_rotation_retriggers() {
        let policy = test_policy(2, Duration::from_secs(60));
        let now = Instant::now();

        assert_eq!(policy.observe_query("ns", "seg-a", now), HeatDecision::Stay);
        assert_eq!(
            policy.observe_query("ns", "seg-a", now + Duration::from_secs(1)),
            HeatDecision::Hydrate
        );
        assert_eq!(
            policy.observe_query("ns", "seg-b", now + Duration::from_secs(2)),
            HeatDecision::Stay
        );
        assert_eq!(
            policy.observe_query("ns", "seg-b", now + Duration::from_secs(3)),
            HeatDecision::Hydrate
        );
    }

    /// Proves the policy remains callable through the `dyn HeatPolicy` boundary.
    #[test]
    fn test_policy_trait_object_safety() {
        let policy = test_policy(1, Duration::from_secs(60));
        let policy: &dyn HeatPolicy = &policy;

        assert_eq!(
            policy.observe_query("ns", "seg-a", Instant::now()),
            HeatDecision::Hydrate
        );
        assert_eq!(policy.request_hydration("ns"), HeatDecision::Hydrate);
    }
}
