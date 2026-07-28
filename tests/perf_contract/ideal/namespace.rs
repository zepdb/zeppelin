//! Isolated namespace-discovery, recovery, and deletion measurements.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use zeppelin::namespace::manager::{NamespaceMetadata, NamespaceState};
use zeppelin::namespace::NamespaceManager;
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::{Clock, TimeSource};
use zeppelin::types::DistanceMetric;
use zeppelin::wal::Manifest;

use crate::common::counting::{perf_counting_store, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::perf_contract::depth::{depth_store, DepthTracker, OpSpan, PhysicalRequest, SpanKind};
use crate::perf_contract::scenario::RepeatCounters;

use super::artifacts::IdealSample;
use super::catalog::{IdealCase, IdealOperation, NamespaceControlCase};

const ACTIVE_SCAN_NAMESPACES: usize = 3;
const COMPLETE_DELETE_OBJECTS: usize = 3;
const INCOMPLETE_DELETE_OBJECTS: usize = 1_001;

/// Whether this executor owns the supplied catalog row.
#[must_use]
pub(crate) fn supports(case: &IdealCase) -> bool {
    owned_case(case).is_some()
}

/// Execute one namespace case against real TestHarness storage.
pub(crate) async fn execute(case: &IdealCase) -> Option<IdealSample> {
    let operation = owned_case(case)?;
    Some(match operation {
        OwnedCase::ScanEmpty => execute_scan_empty(case).await,
        OwnedCase::ScanActiveMany => execute_scan_active_many(case).await,
        OwnedCase::ScanRecoverCreatingManifestPresent => {
            execute_creating_recovery(case, true).await
        }
        OwnedCase::ScanRecoverCreatingManifestMissing => {
            execute_creating_recovery(case, false).await
        }
        OwnedCase::DeletePublishTombstone => execute_delete_publish(case).await,
        OwnedCase::DeleteCleanupIncomplete => execute_delete_cleanup(case, false).await,
        OwnedCase::DeleteCleanupComplete => execute_delete_cleanup(case, true).await,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnedCase {
    ScanEmpty,
    ScanActiveMany,
    ScanRecoverCreatingManifestPresent,
    ScanRecoverCreatingManifestMissing,
    DeletePublishTombstone,
    DeleteCleanupIncomplete,
    DeleteCleanupComplete,
}

fn owned_case(case: &IdealCase) -> Option<OwnedCase> {
    Some(match case.operation {
        IdealOperation::NamespaceControl(NamespaceControlCase::ScanEmpty) => OwnedCase::ScanEmpty,
        IdealOperation::NamespaceControl(NamespaceControlCase::ScanActiveMany) => {
            OwnedCase::ScanActiveMany
        }
        IdealOperation::NamespaceControl(
            NamespaceControlCase::ScanRecoverCreatingManifestPresent,
        ) => OwnedCase::ScanRecoverCreatingManifestPresent,
        IdealOperation::NamespaceControl(
            NamespaceControlCase::ScanRecoverCreatingManifestMissing,
        ) => OwnedCase::ScanRecoverCreatingManifestMissing,
        IdealOperation::NamespaceControl(NamespaceControlCase::DeletePublishTombstone) => {
            OwnedCase::DeletePublishTombstone
        }
        IdealOperation::NamespaceControl(NamespaceControlCase::DeleteCleanupIncomplete) => {
            OwnedCase::DeleteCleanupIncomplete
        }
        IdealOperation::NamespaceControl(NamespaceControlCase::DeleteCleanupComplete) => {
            OwnedCase::DeleteCleanupComplete
        }
        _ => return None,
    })
}

struct NamespaceWorld {
    harness: TestHarness,
    store: ZeppelinStore,
    counter: GetCounter,
    tracker: DepthTracker,
}

#[derive(Debug)]
struct FixedNamespaceTime(DateTime<Utc>);

impl TimeSource for FixedNamespaceTime {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

impl NamespaceWorld {
    async fn new() -> Self {
        let harness = TestHarness::new().await;
        let (depth_wrapped, tracker) = depth_store(&harness.store);
        let (store, counter) = perf_counting_store(&depth_wrapped);
        Self {
            harness,
            store,
            counter,
            tracker,
        }
    }

    fn namespace(&self, suffix: &str) -> String {
        format!("{}-{suffix}", self.harness.prefix)
    }

    fn manager(&self) -> NamespaceManager {
        let now = DateTime::from_timestamp(1_750_000_000, 123_456_789)
            .expect("fixed namespace ideal timestamp must be representable");
        NamespaceManager::with_clock(
            self.store.clone(),
            Duration::from_secs(3_600),
            Clock::from_source(Arc::new(FixedNamespaceTime(now))),
        )
    }

    async fn begin_measurement(&self) {
        await_tracker_idle(&self.tracker).await;
        self.counter.reset();
        self.tracker.reset();
    }

    fn sample(&self, case: &IdealCase) -> IdealSample {
        let repeat = snapshot_repeat(&self.counter, &self.tracker);
        IdealSample::from_repeat(case.id.as_str(), &repeat)
    }

    async fn cleanup_prefix(&self, prefix: &str) {
        self.harness
            .store
            .delete_prefix(prefix)
            .await
            .unwrap_or_else(|error| panic!("failed to clean namespace ideal fixture: {error}"));
    }

    async fn finish(self) {
        self.harness.cleanup().await;
    }
}

async fn execute_scan_empty(case: &IdealCase) -> IdealSample {
    let world = NamespaceWorld::new().await;
    let scope = world.namespace("scan-empty");
    let manager = world.manager();
    world.begin_measurement().await;
    let found = manager
        .list(Some(&scope))
        .await
        .expect("empty namespace scan failed");
    assert!(found.is_empty(), "empty namespace scan found entries");
    await_tracker_idle(&world.tracker).await;
    let sample = world.sample(case);
    assert_shape(&sample, 0, 0, 1, 0);
    world.finish().await;
    sample
}

async fn execute_scan_active_many(case: &IdealCase) -> IdealSample {
    let world = NamespaceWorld::new().await;
    let scope = world.namespace("scan-active");
    let setup = world.manager();
    let names = (0..ACTIVE_SCAN_NAMESPACES)
        .map(|index| format!("{scope}-{index}"))
        .collect::<Vec<_>>();
    for name in &names {
        create_namespace(&setup, name).await;
    }

    let scanner = world.manager();
    world.begin_measurement().await;
    let mut found = scanner
        .list(Some(&scope))
        .await
        .expect("active namespace scan failed");
    found.sort_by(|left, right| left.name.cmp(&right.name));
    assert_eq!(
        found.iter().map(|meta| &meta.name).collect::<Vec<_>>(),
        names.iter().collect::<Vec<_>>()
    );
    assert!(found
        .iter()
        .all(|meta| meta.state == NamespaceState::Active));
    await_tracker_idle(&world.tracker).await;
    let sample = world.sample(case);
    assert_shape(&sample, ACTIVE_SCAN_NAMESPACES as u64, 0, 1, 0);

    for name in &names {
        world.cleanup_prefix(&format!("{name}/")).await;
    }
    world.finish().await;
    sample
}

async fn execute_creating_recovery(case: &IdealCase, manifest_present: bool) -> IdealSample {
    let world = NamespaceWorld::new().await;
    let namespace = world.namespace(if manifest_present {
        "recover-present"
    } else {
        "recover-missing"
    });
    let setup = world.manager();
    let mut meta = create_namespace(&setup, &namespace).await;
    meta.state = NamespaceState::Creating;

    if manifest_present {
        world
            .store
            .put(
                &NamespaceMetadata::s3_key(&namespace),
                meta.to_bytes().expect("encode creating metadata"),
            )
            .await
            .expect("write creating metadata");
    } else {
        world
            .store
            .delete_prefix(&format!("{namespace}/"))
            .await
            .expect("clear interrupted-create fixture");
        world
            .store
            .put(
                &NamespaceMetadata::s3_key(&namespace),
                meta.to_bytes().expect("encode creating metadata"),
            )
            .await
            .expect("write lone creating metadata");
    }

    let scanner = world.manager();
    world.begin_measurement().await;
    let found = scanner
        .list(Some(&namespace))
        .await
        .expect("creating namespace recovery scan failed");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].name, namespace);
    assert_eq!(found[0].state, NamespaceState::Active);
    await_tracker_idle(&world.tracker).await;
    let sample = world.sample(case);
    // The body-only fixture deliberately has no incarnation metadata. Recovery
    // first reads and migrates that legacy identity before repairing state.
    let expected_gets = 5;
    let expected_puts = if manifest_present { 2 } else { 4 };
    assert_shape(&sample, expected_gets, expected_puts, 1, 0);

    let recovered = NamespaceMetadata::from_bytes(
        &world
            .harness
            .store
            .get(&NamespaceMetadata::s3_key(&namespace))
            .await
            .expect("read recovered metadata"),
    )
    .expect("decode recovered metadata");
    assert_eq!(recovered.state, NamespaceState::Active);
    assert!(
        Manifest::read(&world.harness.store, &namespace)
            .await
            .expect("read recovered manifest")
            .is_some(),
        "recovery did not publish a live manifest"
    );
    world.cleanup_prefix(&format!("{namespace}/")).await;
    world.finish().await;
    sample
}

async fn execute_delete_publish(case: &IdealCase) -> IdealSample {
    let world = NamespaceWorld::new().await;
    let namespace = world.namespace("delete-publish");
    let manager = world.manager();
    create_namespace(&manager, &namespace).await;

    world.begin_measurement().await;
    let meta = manager
        .start_delete(&namespace)
        .await
        .expect("namespace tombstone publication failed");
    assert_eq!(meta.state, NamespaceState::Deleting);
    await_tracker_idle(&world.tracker).await;
    let sample = world.sample(case);
    // One branch-root guard manifest read (start_delete reads the live manifest
    // once and threads it into mark_deleting, which re-checks it without a
    // second fetch) plus one metadata GET guard the tombstone.
    assert_shape(&sample, 2, 1, 0, 1);

    assert!(Manifest::read(&world.harness.store, &namespace)
        .await
        .expect("verify deleted manifest")
        .is_none());
    let durable = NamespaceMetadata::from_bytes(
        &world
            .harness
            .store
            .get(&NamespaceMetadata::s3_key(&namespace))
            .await
            .expect("read deletion tombstone"),
    )
    .expect("decode deletion tombstone");
    assert_eq!(durable.state, NamespaceState::Deleting);
    manager
        .finish_delete(&namespace, Duration::MAX)
        .await
        .expect("cleanup published deletion tombstone");
    world.finish().await;
    sample
}

async fn execute_delete_cleanup(case: &IdealCase, complete: bool) -> IdealSample {
    let world = NamespaceWorld::new().await;
    let namespace = world.namespace(if complete {
        "delete-complete"
    } else {
        "delete-incomplete"
    });
    let manager = world.manager();
    create_namespace(&manager, &namespace).await;
    manager
        .start_delete(&namespace)
        .await
        .expect("prepare namespace deletion");
    world
        .store
        .delete_prefix(&format!("{namespace}/manifests/"))
        .await
        .expect("remove retained histories from delete fixture");
    let object_count = if complete {
        COMPLETE_DELETE_OBJECTS
    } else {
        INCOMPLETE_DELETE_OBJECTS
    };
    seed_objects(&world.store, &namespace, object_count).await;

    world.begin_measurement().await;
    let outcome = manager
        .finish_delete(
            &namespace,
            if complete {
                Duration::MAX
            } else {
                Duration::ZERO
            },
        )
        .await
        .expect("measured namespace cleanup failed");
    assert_eq!(outcome.complete, complete);
    assert_eq!(
        outcome.deleted,
        if complete {
            COMPLETE_DELETE_OBJECTS
        } else {
            1_000
        }
    );
    await_tracker_idle(&world.tracker).await;
    let sample = world.sample(case);
    // e232818 made legacy cleanup re-read authoritative metadata at each step
    // and re-check manifest absence before batch deletion and tombstone
    // removal: one read on entry, two per guarded step. An incomplete batch
    // returns before the second step, so it observes three GETs while the
    // complete path observes five.
    assert_shape(
        &sample,
        if complete { 5 } else { 3 },
        0,
        if complete { 2 } else { 1 },
        if complete { 2 } else { 1 },
    );
    assert_eq!(request_ops(&sample, PhysicalRequest::DeleteBatch), 1);
    assert_eq!(
        request_ops(&sample, PhysicalRequest::Delete),
        u64::from(complete),
        "only completed cleanup deletes meta.json separately"
    );

    let remaining = world
        .harness
        .store
        .list_prefix(&format!("{namespace}/"))
        .await
        .expect("verify namespace cleanup state");
    if complete {
        assert!(remaining.is_empty());
    } else {
        assert_eq!(remaining.len(), 2, "one object plus tombstone must remain");
        assert!(remaining.contains(&NamespaceMetadata::s3_key(&namespace)));
        let final_outcome = manager
            .finish_delete(&namespace, Duration::MAX)
            .await
            .expect("finish incomplete namespace cleanup");
        assert!(final_outcome.complete);
        assert_eq!(final_outcome.deleted, 1);
    }
    world.cleanup_prefix(&format!("{namespace}/")).await;
    world.finish().await;
    sample
}

async fn create_namespace(manager: &NamespaceManager, namespace: &str) -> NamespaceMetadata {
    let meta = manager
        .create(namespace, 4, DistanceMetric::Euclidean)
        .await
        .expect("create namespace ideal fixture");
    assert_eq!(meta.name, namespace);
    assert_eq!(meta.state, NamespaceState::Active);
    meta
}

async fn seed_objects(store: &ZeppelinStore, namespace: &str, count: usize) {
    let mut writes = futures::stream::iter((0..count).map(|index| {
        let store = store.clone();
        // Seed under `segments/` so the fixture is a target-owned immutable
        // artifact. Governed cleanup only enumerates owned artifacts, so a key
        // directly under `{namespace}/` was never deleted and the delete counts
        // this case asserts did not exercise the cleanup path at all.
        let key = format!("{namespace}/segments/delete-fixture/{index:04}.bin");
        async move { store.put(&key, Bytes::from_static(b"x")).await }
    }))
    .buffer_unordered(64);
    while let Some(result) = writes.next().await {
        result.expect("seed namespace delete fixture object");
    }
}

fn assert_shape(sample: &IdealSample, gets: u64, puts: u64, lists: u64, deletes: u64) {
    assert_eq!(
        sample.total_get_ops, gets,
        "unexpected namespace GET count for {}",
        sample.scenario_id
    );
    let actual_puts = mode_ops(sample, "put");
    assert_eq!(
        actual_puts, puts,
        "unexpected namespace PUT count for {}",
        sample.scenario_id
    );
    assert_eq!(
        mode_ops(sample, "list"),
        lists,
        "unexpected namespace LIST count for {}",
        sample.scenario_id
    );
    assert_eq!(
        mode_ops(sample, "delete"),
        deletes,
        "unexpected namespace DELETE count for {}",
        sample.scenario_id
    );
}

fn mode_ops(sample: &IdealSample, verb: &str) -> u64 {
    sample
        .physical_verb_mode_totals
        .iter()
        .filter(|total| total.verb == verb)
        .map(|total| total.ops)
        .sum()
}

fn request_ops(sample: &IdealSample, request: PhysicalRequest) -> u64 {
    sample
        .physical_operations
        .iter()
        .filter(|operation| operation.request == request)
        .count() as u64
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
        "ideal namespace measurement did not quiesce: active_operations={}",
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
            total.get_ops += stats.get_ops;
            total.get_bytes += stats.get_bytes;
            total.put_ops += stats.put_ops;
            total.put_bytes += stats.put_bytes;
            total
        });
    let cutoff_us = tracker.elapsed_us();
    let spans = tracker.take_spans();
    let get_path = DepthTracker::critical_path(&spans, &[SpanKind::Get], Some(cutoff_us));
    let put_get_path =
        DepthTracker::critical_path(&spans, &[SpanKind::Get, SpanKind::Put], Some(cutoff_us));
    RepeatCounters {
        classes,
        totals,
        raw_get_path: get_path.clone(),
        raw_put_get_path: put_get_path.clone(),
        get_path,
        put_get_path,
        op_counts: operation_counts(&spans),
        spans,
        labeled: Vec::new(),
        wall_elapsed_us: 0,
        response_cutoff_us: cutoff_us,
    }
}

fn operation_counts(spans: &[OpSpan]) -> BTreeMap<String, u64> {
    [
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
    .collect()
}

#[cfg(test)]
mod tests {
    use super::super::catalog;
    use super::*;

    #[test]
    fn owns_exactly_the_seven_remaining_namespace_cases() {
        let supported = catalog::all()
            .iter()
            .filter(|case| supports(case))
            .map(|case| case.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            supported,
            vec![
                "namespace.scan_empty",
                "namespace.scan_active_many",
                "namespace.scan_recover_creating_manifest_present",
                "namespace.scan_recover_creating_manifest_missing",
                "namespace.delete_publish_tombstone",
                "namespace.delete_cleanup_incomplete",
                "namespace.delete_cleanup_complete",
            ]
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn all_namespace_cases_use_real_storage() {
        for case in catalog::all().iter().filter(|case| supports(case)) {
            let sample = execute(case)
                .await
                .unwrap_or_else(|| panic!("namespace case {} was not executed", case.id.as_str()));
            assert_eq!(sample.scenario_id, case.id.as_str());
        }
    }
}
