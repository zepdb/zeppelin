//! Warm-set hydration policy and worker support.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use crate::config::{CacheConfig, HydrationPolicyKind};
use crate::error::{Result, ZeppelinError};

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
