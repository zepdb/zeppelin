//! Isolated direct-domain measurements for ideal-analysis cases that do not
//! require an HTTP server or a background scheduler.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::error::ZeppelinError;
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::{LeaseManager, Manifest};

use crate::common::counting::{counting_store, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::perf_contract::depth::{
    depth_store, DepthTracker, PhysicalRequest, SpanKind, SpanOutcome,
};
use crate::perf_contract::scenario::RepeatCounters;

use super::artifacts::IdealSample;
use super::catalog::{IdealCase, IdealOperation, LeaseCase, ManifestCacheCase, OperationalCase};

/// Execute one safe direct-domain catalog case.
///
/// `None` means this module does not own the case. Supported cases always use
/// real storage and fail loudly on an unexpected production result.
#[must_use]
pub(crate) fn supports(case: &IdealCase) -> bool {
    matches!(
        case.operation,
        IdealOperation::Operational(OperationalCase::StartupStorageProbe)
            | IdealOperation::Lease(_)
            | IdealOperation::ManifestCache(_)
    )
}

pub(crate) async fn execute(case: &IdealCase) -> Option<IdealSample> {
    match case.operation {
        IdealOperation::Operational(OperationalCase::StartupStorageProbe) => {
            Some(execute_operational(case, OperationalCase::StartupStorageProbe).await)
        }
        IdealOperation::Lease(operation) => Some(execute_lease(case, operation).await),
        IdealOperation::ManifestCache(operation) => {
            Some(execute_manifest_cache(case, operation).await)
        }
        _ => None,
    }
}

struct DirectWorld {
    harness: TestHarness,
    store: ZeppelinStore,
    counter: GetCounter,
    tracker: DepthTracker,
}

impl DirectWorld {
    async fn new() -> Self {
        let harness = TestHarness::new().await;
        let (depth_wrapped, tracker) = depth_store(&harness.store);
        let (store, counter) = counting_store(&depth_wrapped);
        Self {
            harness,
            store,
            counter,
            tracker,
        }
    }

    fn namespace(&self, suffix: &str) -> String {
        self.harness.key(suffix)
    }

    async fn begin_measurement(&self) {
        await_tracker_idle(&self.tracker).await;
        self.counter.reset();
        self.tracker.reset();
    }

    async fn finish(self, case: &IdealCase) -> IdealSample {
        let sample = self.snapshot(case).await;
        self.cleanup(sample).await
    }

    async fn snapshot(&self, case: &IdealCase) -> IdealSample {
        await_tracker_idle(&self.tracker).await;
        let repeat = snapshot_repeat(&self.counter, &self.tracker);
        IdealSample::from_repeat(case.id.as_str(), &repeat)
    }

    async fn cleanup(self, sample: IdealSample) -> IdealSample {
        self.harness.cleanup().await;
        sample
    }
}

async fn execute_operational(case: &IdealCase, operation: OperationalCase) -> IdealSample {
    let world = DirectWorld::new().await;
    world.begin_measurement().await;
    match operation {
        OperationalCase::StartupStorageProbe => {
            world
                .store
                .list_common_prefixes("")
                .await
                .expect("ideal startup storage probe failed");
        }
        OperationalCase::HealthCheckStorageList => {
            world
                .store
                .list_prefix("__healthcheck__")
                .await
                .expect("ideal readiness storage list failed");
        }
    }
    world.finish(case).await
}

fn assert_manifest_cache_shape(operation: ManifestCacheCase, sample: &IdealSample) {
    let expected = match operation {
        ManifestCacheCase::EventualExpired | ManifestCacheCase::StrongWriteThroughWithoutEtag => {
            vec![(PhysicalRequest::GetFull, SpanOutcome::Success)]
        }
        ManifestCacheCase::StrongEtagChanged => {
            vec![(PhysicalRequest::GetConditional, SpanOutcome::Success)]
        }
        ManifestCacheCase::StrongConcurrentCoalesced => {
            vec![(PhysicalRequest::GetConditional, SpanOutcome::NotModified)]
        }
        ManifestCacheCase::StrongRequiredMissing => {
            vec![(PhysicalRequest::GetFull, SpanOutcome::NotFound)]
        }
        ManifestCacheCase::StrongOptionalConditionalNotFound => vec![
            (PhysicalRequest::GetConditional, SpanOutcome::NotFound),
            (PhysicalRequest::GetFull, SpanOutcome::NotFound),
        ],
    };
    assert_eq!(
        sample.total_get_ops,
        u64::try_from(expected.len()).expect("cache GET expectation count does not fit u64"),
        "{} issued the wrong cache GET count",
        sample.scenario_id
    );
    assert_eq!(
        sample.physical_operations.len(),
        expected.len(),
        "{} leaked unrelated object-store work: {:?}",
        sample.scenario_id,
        sample.physical_operations
    );
    for (observed, (request, outcome)) in sample.physical_operations.iter().zip(expected) {
        assert_eq!(observed.verb, "get");
        assert_eq!(observed.request, request);
        assert_eq!(observed.outcome, outcome);
        assert_eq!(observed.key, "manifest.json");
        if outcome == SpanOutcome::Success {
            assert!(
                observed.successful_bytes > 0,
                "{} successful cache GET returned no bytes",
                sample.scenario_id
            );
        } else {
            assert_eq!(observed.successful_bytes, 0);
        }
    }
}

async fn execute_lease(case: &IdealCase, operation: LeaseCase) -> IdealSample {
    let world = DirectWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    let owned = LeaseManager::new(
        world.store.clone(),
        "ideal-holder-a".to_string(),
        Duration::from_secs(3_600),
    );

    match operation {
        LeaseCase::AcquireNew => {
            world.begin_measurement().await;
            let lease = owned
                .acquire(&namespace)
                .await
                .expect("ideal new lease acquisition failed");
            assert_eq!(lease.fencing_token, 1, "first lease token must be one");
        }
        LeaseCase::AcquireLiveHeld => {
            owned
                .acquire(&namespace)
                .await
                .expect("ideal live-lease setup acquisition failed");
            let contender = LeaseManager::new(
                world.store.clone(),
                "ideal-holder-b".to_string(),
                Duration::from_secs(3_600),
            );
            world.begin_measurement().await;
            assert!(matches!(
                contender.acquire(&namespace).await,
                Err(ZeppelinError::LeaseHeld { .. })
            ));
        }
        LeaseCase::AcquireExpiredTakeover => {
            let expired = LeaseManager::new(
                world.store.clone(),
                "ideal-holder-expired".to_string(),
                Duration::ZERO,
            );
            expired
                .acquire(&namespace)
                .await
                .expect("ideal expired-lease setup failed");
            let contender = LeaseManager::new(
                world.store.clone(),
                "ideal-holder-b".to_string(),
                Duration::from_secs(3_600),
            );
            world.begin_measurement().await;
            let lease = contender
                .acquire(&namespace)
                .await
                .expect("ideal expired lease takeover failed");
            assert_eq!(lease.fencing_token, 2, "takeover must advance the token");
        }
        LeaseCase::RenewOwned => {
            let lease = owned
                .acquire(&namespace)
                .await
                .expect("ideal lease-renewal setup failed");
            world.begin_measurement().await;
            let renewed = owned
                .renew(&namespace, &lease)
                .await
                .expect("ideal owned lease renewal failed");
            assert_eq!(renewed.fencing_token, lease.fencing_token);
        }
        LeaseCase::ReleaseOwned => {
            let lease = owned
                .acquire(&namespace)
                .await
                .expect("ideal owned-release setup failed");
            world.begin_measurement().await;
            owned
                .release(&namespace, &lease)
                .await
                .expect("ideal owned lease release failed");
        }
        LeaseCase::ReleaseTakenOver => {
            let expired = LeaseManager::new(
                world.store.clone(),
                "ideal-holder-a".to_string(),
                Duration::ZERO,
            );
            let stale = expired
                .acquire(&namespace)
                .await
                .expect("ideal stale-release setup acquisition failed");
            let successor = LeaseManager::new(
                world.store.clone(),
                "ideal-holder-b".to_string(),
                Duration::from_secs(3_600),
            );
            successor
                .acquire(&namespace)
                .await
                .expect("ideal stale-release takeover setup failed");
            world.begin_measurement().await;
            expired
                .release(&namespace, &stale)
                .await
                .expect("taken-over lease release must remain best-effort");
        }
        LeaseCase::ReleaseMissing => {
            let lease = owned
                .acquire(&namespace)
                .await
                .expect("ideal missing-release setup acquisition failed");
            world
                .store
                .delete(&format!("{namespace}/lease.json"))
                .await
                .expect("ideal missing-release setup delete failed");
            world.begin_measurement().await;
            owned
                .release(&namespace, &lease)
                .await
                .expect("missing lease release must remain best-effort");
        }
    }

    world.finish(case).await
}

async fn execute_manifest_cache(case: &IdealCase, operation: ManifestCacheCase) -> IdealSample {
    let world = DirectWorld::new().await;
    let namespace = world.namespace(case.id.as_str());

    match operation {
        ManifestCacheCase::EventualExpired => {
            persist_manifest(&world.store, &namespace).await;
            let cache = ManifestCache::new(Duration::ZERO);
            cache
                .get_required(&world.store, &namespace)
                .await
                .expect("ideal expired-cache prime failed");
            world.begin_measurement().await;
            cache
                .get_required(&world.store, &namespace)
                .await
                .expect("ideal expired eventual cache refresh failed");
        }
        ManifestCacheCase::StrongEtagChanged => {
            let mut manifest = persist_manifest(&world.store, &namespace).await;
            let cache = ManifestCache::new(Duration::from_secs(3_600));
            cache
                .get_strong_required(&world.store, &namespace)
                .await
                .expect("ideal changed-ETag cache prime failed");
            manifest
                .write(&world.store, &namespace)
                .await
                .expect("ideal changed-ETag remote update failed");
            world.begin_measurement().await;
            let refreshed = cache
                .get_strong_required(&world.store, &namespace)
                .await
                .expect("ideal changed-ETag strong refresh failed");
            assert_eq!(refreshed.version(), manifest.version());
        }
        ManifestCacheCase::StrongWriteThroughWithoutEtag => {
            let manifest = persist_manifest(&world.store, &namespace).await;
            let cache = ManifestCache::new(Duration::from_secs(3_600));
            cache.insert(&namespace, manifest.clone());
            world.begin_measurement().await;
            let refreshed = cache
                .get_strong_required(&world.store, &namespace)
                .await
                .expect("ideal write-through strong refresh failed");
            assert_eq!(refreshed.version(), manifest.version());
        }
        ManifestCacheCase::StrongConcurrentCoalesced => {
            persist_manifest(&world.store, &namespace).await;
            let cache = Arc::new(ManifestCache::new(Duration::from_secs(3_600)));
            cache
                .get_strong_required(&world.store, &namespace)
                .await
                .expect("ideal concurrent-cache prime failed");
            world.begin_measurement().await;
            let (left, right) = tokio::join!(
                cache.get_strong_required(&world.store, &namespace),
                cache.get_strong_required(&world.store, &namespace),
            );
            let left = left.expect("first ideal concurrent strong read failed");
            let right = right.expect("second ideal concurrent strong read failed");
            assert_eq!(left.version(), right.version());
        }
        ManifestCacheCase::StrongRequiredMissing => {
            let cache = ManifestCache::new(Duration::from_secs(3_600));
            world.begin_measurement().await;
            assert!(matches!(
                cache.get_strong_required(&world.store, &namespace).await,
                Err(ZeppelinError::NotFound { .. })
            ));
        }
        ManifestCacheCase::StrongOptionalConditionalNotFound => {
            persist_manifest(&world.store, &namespace).await;
            let cache = ManifestCache::new(Duration::from_secs(3_600));
            cache
                .get_strong(&world.store, &namespace)
                .await
                .expect("ideal optional-cache prime failed");
            world
                .store
                .delete(&Manifest::s3_key(&namespace))
                .await
                .expect("ideal optional-cache setup delete failed");
            world.begin_measurement().await;
            let observed = cache
                .get_strong(&world.store, &namespace)
                .await
                .expect("ideal optional conditional-not-found refresh failed");
            assert_eq!(observed.version(), 0);
        }
    }

    let sample = world.snapshot(case).await;
    assert_manifest_cache_shape(operation, &sample);
    world.cleanup(sample).await
}

async fn persist_manifest(store: &ZeppelinStore, namespace: &str) -> Manifest {
    let mut manifest = Manifest::new();
    manifest
        .write(store, namespace)
        .await
        .expect("ideal manifest setup publication failed");
    manifest
}

async fn await_tracker_idle(tracker: &DepthTracker) {
    const MAX_YIELDS: usize = 4_096;
    const REQUIRED_ZERO_STREAK: usize = 8;

    let mut zero_streak = 0;
    for _ in 0..MAX_YIELDS {
        tokio::task::yield_now().await;
        if tracker.active_operations() == 0 {
            zero_streak += 1;
            if zero_streak == REQUIRED_ZERO_STREAK {
                return;
            }
        } else {
            zero_streak = 0;
        }
    }
    panic!(
        "ideal direct measurement did not quiesce: active_operations={}",
        tracker.active_operations()
    );
}

fn snapshot_repeat(counter: &GetCounter, tracker: &DepthTracker) -> RepeatCounters {
    let classes = counter
        .class_breakdown()
        .into_iter()
        .map(|(class, stats)| (class.name().to_string(), stats))
        .collect::<BTreeMap<_, _>>();
    let totals = classes
        .values()
        .copied()
        .fold(ClassStats::default(), |mut total, stats| {
            total.get_ops = total
                .get_ops
                .checked_add(stats.get_ops)
                .expect("ideal direct GET operation total overflowed");
            total.get_bytes = total
                .get_bytes
                .checked_add(stats.get_bytes)
                .expect("ideal direct GET byte total overflowed");
            total.put_ops = total
                .put_ops
                .checked_add(stats.put_ops)
                .expect("ideal direct PUT operation total overflowed");
            total.put_bytes = total
                .put_bytes
                .checked_add(stats.put_bytes)
                .expect("ideal direct PUT byte total overflowed");
            total
        });
    let cutoff_us = tracker.elapsed_us();
    let spans = tracker.take_spans();
    let get_path = DepthTracker::critical_path(&spans, &[SpanKind::Get], Some(cutoff_us));
    let put_get_path =
        DepthTracker::critical_path(&spans, &[SpanKind::Get, SpanKind::Put], Some(cutoff_us));
    let op_counts = [
        ("head", SpanKind::Head),
        ("list", SpanKind::List),
        ("copy", SpanKind::Copy),
        ("delete", SpanKind::Delete),
    ]
    .into_iter()
    .map(|(name, kind)| {
        (
            name.to_string(),
            spans.iter().filter(|span| span.kind == kind).count() as u64,
        )
    })
    .collect();

    RepeatCounters {
        classes,
        totals,
        raw_get_path: get_path.clone(),
        raw_put_get_path: put_get_path.clone(),
        get_path,
        put_get_path,
        spans,
        op_counts,
        labeled: Vec::new(),
        wall_elapsed_us: 0,
        response_cutoff_us: cutoff_us,
    }
}

#[cfg(test)]
mod tests {
    use super::super::catalog;
    use super::*;

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn direct_operational_case_uses_real_storage() {
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "operational.startup_storage_probe")
            .expect("operational direct case must exist");

        let sample = execute(case)
            .await
            .expect("operational case must have a direct executor");

        assert!(sample
            .physical_verb_mode_totals
            .iter()
            .any(|total| total.verb == "list" && total.mode == "list_delimiter"));
    }
}
