//! Warm-set hydration policy and worker support.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::config::{CacheConfig, HydrationPolicyKind};
use crate::error::{Result, ZeppelinError};
use crate::storage::ZeppelinStore;
use crate::wal::manifest::{ClusterDataObjectRef, SegmentRef};

use super::DiskCache;

const HYDRATION_JOB_QUEUE_CAPACITY: usize = 1024;
const INCREMENTAL_GATE_MESSAGE: &str =
    "blocked on incremental carry-over bug, see main todo.md Task 2C precondition";

/// Decision returned by a heat policy observation.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeatDecision {
    /// Do not hydrate the namespace yet.
    Stay,
    /// Enqueue hydration for the namespace's active segment.
    Hydrate,
}

/// Query-heat policy surface consumed by the hydration core.
pub trait HeatPolicy: Send + Sync {
    /// Observe one namespace query for the active segment.
    ///
    /// This method is intentionally synchronous so query-path wiring can call
    /// it cheaply without awaiting hydration work.
    fn observe_query(&self, namespace: &str, segment_id: &str, now: Instant) -> HeatDecision;

    /// Record an explicit external hydration request.
    fn request_hydration(&self, namespace: &str) -> HeatDecision;
}

/// Build the globally configured heat policy.
pub fn heat_policy_from_config(config: &CacheConfig) -> Result<Arc<dyn HeatPolicy>> {
    match config.hydration_policy {
        HydrationPolicyKind::SessionWindow => Ok(Arc::new(SessionWindowPolicy::new(
            config.hydration_heat_queries,
            Duration::from_secs(config.hydration_heat_window_secs),
        )?)),
    }
}

/// Runtime configuration for background segment hydration.
#[derive(Debug, Clone)]
pub struct HydrationConfig {
    /// Maximum concurrent object downloads per hydration job.
    pub parallelism: usize,
    /// Maximum fraction of the cache byte budget one segment may require.
    pub max_segment_fraction: f64,
    /// Number of retries after the first failed job attempt.
    pub max_retries: usize,
    /// Backoff between failed job attempts.
    pub retry_backoff: Duration,
}

impl HydrationConfig {
    /// Build hydrator runtime configuration from cache configuration.
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

/// Trigger source for a hydration job.
#[derive(Debug, Clone, Copy)]
pub enum HydrationTrigger {
    /// Heat policy inferred the namespace is hot.
    Heat,
    /// Explicit administrative request.
    Admin,
}

impl HydrationTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Heat => "heat",
            Self::Admin => "admin",
        }
    }
}

#[derive(Debug)]
struct HydrationJob {
    namespace: String,
    segment: SegmentRef,
    trigger: HydrationTrigger,
}

/// Background worker that hydrates active segment objects into [`DiskCache`].
pub struct SegmentHydrator {
    policy: Arc<dyn HeatPolicy>,
    jobs: mpsc::Sender<HydrationJob>,
}

impl SegmentHydrator {
    /// Start a background segment hydrator worker.
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

    /// Observe a query and enqueue hydration if the policy fires.
    ///
    /// This function never awaits; query handling remains independent of
    /// hydration success or failure.
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

    /// Enqueue an explicit administrative hydration request.
    pub fn request_hydration(&self, namespace: &str, segment: &SegmentRef) {
        if self.policy.request_hydration(namespace) == HeatDecision::Hydrate {
            self.enqueue(namespace, segment, HydrationTrigger::Admin);
        }
    }

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

async fn worker_loop(
    store: ZeppelinStore,
    cache: Arc<DiskCache>,
    config: HydrationConfig,
    mut rx: mpsc::Receiver<HydrationJob>,
) {
    while let Some(job) = rx.recv().await {
        run_job_with_retries(&store, &cache, &config, job).await;
    }
}

async fn run_job_with_retries(
    store: &ZeppelinStore,
    cache: &Arc<DiskCache>,
    config: &HydrationConfig,
    job: HydrationJob,
) {
    crate::metrics::HYDRATION_INFLIGHT.inc();
    let _inflight_guard = crate::metrics::GaugeGuard(&crate::metrics::HYDRATION_INFLIGHT);
    let mut attempt = 0usize;
    loop {
        match hydrate_segment_once(store, cache, config, &job).await {
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

async fn hydrate_segment_once(
    store: &ZeppelinStore,
    cache: &Arc<DiskCache>,
    config: &HydrationConfig,
    job: &HydrationJob,
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

    let required_bytes = segment_cluster_object_bytes(&job.segment)?;
    let capacity_limit = hydration_capacity_limit(cache.max_size_bytes(), config)?;
    if required_bytes > capacity_limit {
        crate::metrics::HYDRATION_SKIPPED_TOTAL
            .with_label_values(&["capacity"])
            .inc();
        warn!(
            namespace = %job.namespace,
            segment_id = %job.segment.id,
            required_bytes,
            capacity_limit,
            cache_max_size_bytes = cache.max_size_bytes(),
            max_segment_fraction = config.max_segment_fraction,
            "warm-set hydration refused: segment exceeds capacity fraction"
        );
        return Ok(());
    }

    let objects = job.segment.cluster_objects.clone();
    let mut stream = futures::stream::iter(objects)
        .map(|object| hydrate_cluster_object(store.clone(), Arc::clone(cache), object))
        .buffer_unordered(config.parallelism);
    while let Some(result) = stream.next().await {
        result?;
    }
    Ok(())
}

fn is_incremental_segment(segment: &SegmentRef) -> bool {
    segment
        .cluster_owners
        .iter()
        .any(|owner| owner != &segment.id)
}

fn segment_cluster_object_bytes(segment: &SegmentRef) -> Result<u64> {
    segment
        .cluster_objects
        .iter()
        .try_fold(0u64, |acc, object| {
            acc.checked_add(object.size_bytes).ok_or_else(|| {
                ZeppelinError::Cache(format!(
                    "hydration byte budget overflows for segment {}",
                    segment.id
                ))
            })
        })
}

fn hydration_capacity_limit(cache_max_size_bytes: u64, config: &HydrationConfig) -> Result<u64> {
    let limit = (cache_max_size_bytes as f64) * config.max_segment_fraction;
    if !limit.is_finite() || limit < 0.0 {
        return Err(ZeppelinError::Cache(
            "hydration capacity limit is not finite".into(),
        ));
    }
    Ok(limit.floor() as u64)
}

async fn hydrate_cluster_object(
    store: ZeppelinStore,
    cache: Arc<DiskCache>,
    object: ClusterDataObjectRef,
) -> Result<()> {
    let key = object.key.clone();
    let fetch_key = key.clone();
    let bytes = cache
        .get_or_fetch(&key, move || {
            let store = store.clone();
            async move { store.get(&fetch_key).await }
        })
        .await?;
    let actual = bytes.len() as u64;
    if object.size_bytes != 0 && actual != object.size_bytes {
        cache.invalidate(&key).await?;
        return Err(ZeppelinError::Cache(format!(
            "hydrated object length mismatch for {key}: expected={}, actual={actual}",
            object.size_bytes
        )));
    }
    crate::metrics::HYDRATION_OBJECTS_TOTAL
        .with_label_values(&["cluster"])
        .inc();
    crate::metrics::HYDRATION_BYTES_TOTAL
        .with_label_values(&["cluster"])
        .inc_by(actual);
    Ok(())
}

/// Session-window policy: hydrate after N observations within a fixed window.
#[derive(Debug)]
pub struct SessionWindowPolicy {
    threshold: u64,
    window: Duration,
    states: DashMap<String, WindowState>,
}

#[derive(Debug, Clone)]
struct WindowState {
    segment_id: String,
    window_start: Instant,
    count: u64,
    triggered_segment_id: Option<String>,
}

impl WindowState {
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
    /// Create a session-window heat policy.
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

    fn request_hydration(&self, _namespace: &str) -> HeatDecision {
        HeatDecision::Hydrate
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{HeatDecision, HeatPolicy, SessionWindowPolicy};

    fn test_policy(threshold: u64, window: Duration) -> SessionWindowPolicy {
        match SessionWindowPolicy::new(threshold, window) {
            Ok(policy) => policy,
            Err(error) => panic!("test policy construction failed: {error}"),
        }
    }

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
