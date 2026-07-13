//! Isolated direct-domain measurements for ideal-analysis cases that do not
//! require an HTTP server or a background scheduler.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::error::ZeppelinError;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::VectorEntry;
use zeppelin::wal::{Lease, LeaseManager, Manifest, WalWriter};

use crate::common::counting::{perf_counting_store, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::perf_contract::depth::{
    depth_store, DepthTracker, PhysicalRequest, SpanKind, SpanOutcome,
};
use crate::perf_contract::injection::{
    inject_lease_retry_conflict, inject_missing_lease_put_etag, inject_missing_manifest_put_etag,
    LeaseRetryConflictHandle, MissingLeasePutEtagHandle, MissingManifestPutEtagHandle,
};
use crate::perf_contract::scenario::RepeatCounters;

use super::artifacts::IdealSample;
use super::catalog::{
    IdealCase, IdealOperation, LeaseCase, ManifestCacheCase, OperationalCase, VectorWriteCase,
};

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
            | IdealOperation::VectorWrite(
                VectorWriteCase::GroupCommitConflict
                    | VectorWriteCase::GroupCommitMissingPutEtag
                    | VectorWriteCase::GroupCommitWarm
            )
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
        IdealOperation::VectorWrite(
            operation @ (VectorWriteCase::GroupCommitConflict
            | VectorWriteCase::GroupCommitMissingPutEtag
            | VectorWriteCase::GroupCommitWarm),
        ) => Some(execute_group_commit(case, operation).await),
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
        let store = harness.store.clone();
        Self::from_store(harness, &store)
    }

    async fn with_lease_retry_conflict() -> (Self, LeaseRetryConflictHandle) {
        let harness = TestHarness::new().await;
        let (store, handle) = inject_lease_retry_conflict(&harness.store);
        (Self::from_store(harness, &store), handle)
    }

    async fn with_missing_lease_put_etag() -> (Self, MissingLeasePutEtagHandle) {
        let harness = TestHarness::new().await;
        let (store, handle) = inject_missing_lease_put_etag(&harness.store);
        (Self::from_store(harness, &store), handle)
    }

    async fn with_missing_manifest_put_etag() -> (Self, MissingManifestPutEtagHandle) {
        let harness = TestHarness::new().await;
        let (store, handle) = inject_missing_manifest_put_etag(&harness.store);
        (Self::from_store(harness, &store), handle)
    }

    fn from_store(harness: TestHarness, store: &ZeppelinStore) -> Self {
        let (depth_wrapped, tracker) = depth_store(store);
        let (store, counter) = perf_counting_store(&depth_wrapped);
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

async fn execute_group_commit(case: &IdealCase, operation: VectorWriteCase) -> IdealSample {
    if operation == VectorWriteCase::GroupCommitMissingPutEtag {
        return execute_group_commit_missing_put_etag(case).await;
    }

    let world = DirectWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    Manifest::new()
        .write(&world.store, &namespace)
        .await
        .expect("ideal warm group-commit manifest setup failed");
    let writer = WalWriter::new(world.store.clone());
    writer
        .append(
            &namespace,
            vec![VectorEntry {
                id: "group-commit-prime".to_string(),
                values: vec![0.0, 1.0, 0.0, 1.0],
                attributes: None,
            }],
            vec![],
        )
        .await
        .expect("ideal warm group-commit prime failed");

    if operation == VectorWriteCase::GroupCommitConflict {
        let (mut external, version) = Manifest::read_versioned(&world.store, &namespace)
            .await
            .expect("ideal group-commit conflict setup read failed")
            .expect("ideal group-commit conflict setup manifest missing");
        external.fencing_token = 41;
        external
            .write_conditional(&world.store, &namespace, &version)
            .await
            .expect("ideal group-commit conflict setup publication failed");
    }

    world.begin_measurement().await;
    writer
        .append(
            &namespace,
            vec![VectorEntry {
                id: "group-commit-measured".to_string(),
                values: vec![1.0, 0.0, 1.0, 0.0],
                attributes: None,
            }],
            vec![],
        )
        .await
        .expect("ideal warm group-commit append failed");

    let sample = world.snapshot(case).await;
    match operation {
        VectorWriteCase::GroupCommitConflict => assert_group_commit_conflict_shape(&sample),
        VectorWriteCase::GroupCommitWarm => assert_group_commit_warm_shape(&sample),
        _ => unreachable!("missing-ETag group commit has a dedicated world"),
    }
    world.cleanup(sample).await
}

async fn execute_group_commit_missing_put_etag(case: &IdealCase) -> IdealSample {
    let (world, injection) = DirectWorld::with_missing_manifest_put_etag().await;
    let namespace = world.namespace(case.id.as_str());
    Manifest::new()
        .write(&world.store, &namespace)
        .await
        .expect("ideal missing-ETag group-commit manifest setup failed");
    let writer = WalWriter::new(world.store.clone());
    writer
        .append(
            &namespace,
            vec![VectorEntry {
                id: "group-commit-missing-etag-prime".to_string(),
                values: vec![0.0, 1.0, 0.0, 1.0],
                attributes: None,
            }],
            vec![],
        )
        .await
        .expect("ideal missing-ETag group-commit prime failed");

    world.begin_measurement().await;
    writer
        .append(
            &namespace,
            vec![VectorEntry {
                id: "group-commit-missing-etag-measured".to_string(),
                values: vec![1.0, 0.0, 1.0, 0.0],
                attributes: None,
            }],
            vec![],
        )
        .await
        .expect("ideal missing-ETag group-commit append failed");

    let sample = world.snapshot(case).await;
    assert_group_commit_missing_put_etag_shape(&sample);
    assert_eq!(
        injection.stripped(),
        2,
        "both successful group-commit CAS responses must omit their ETags"
    );
    world.cleanup(sample).await
}

fn assert_group_commit_warm_shape(sample: &IdealSample) {
    assert_eq!(
        sample.total_get_ops, 0,
        "a warm group-commit round must issue no manifest GET"
    );
    assert_eq!(sample.total_get_bytes, 0);
    assert_eq!(
        sample.physical_operations.len(),
        3,
        "warm group commit must write one WAL, one history, and one live manifest: {:?}",
        sample.physical_operations
    );
    let expected = [
        (
            "put",
            PhysicalRequest::PutOverwrite,
            SpanOutcome::Success,
            "<wal>.wal",
        ),
        (
            "put",
            PhysicalRequest::PutCreate,
            SpanOutcome::Success,
            "<generation>.msgpack",
        ),
        (
            "put",
            PhysicalRequest::PutUpdate,
            SpanOutcome::Success,
            "manifest.json",
        ),
    ];
    for (operation, (verb, request, outcome, key)) in
        sample.physical_operations.iter().zip(expected)
    {
        assert_eq!(operation.verb, verb);
        assert_eq!(operation.request, request);
        assert_eq!(operation.outcome, outcome);
        assert_eq!(operation.key, key);
    }
}

fn assert_group_commit_conflict_shape(sample: &IdealSample) {
    assert_eq!(
        sample.total_get_ops, 3,
        "a stale group-commit memo must validate history and rebuild from S3"
    );
    assert_eq!(sample.physical_operations.len(), 7);
    let expected = [
        (
            "put",
            PhysicalRequest::PutOverwrite,
            SpanOutcome::Success,
            "<wal>.wal",
        ),
        (
            "put",
            PhysicalRequest::PutCreate,
            SpanOutcome::Precondition,
            "<generation>.msgpack",
        ),
        (
            "get",
            PhysicalRequest::GetFull,
            SpanOutcome::Success,
            "<generation>.msgpack",
        ),
        (
            "get",
            PhysicalRequest::GetFull,
            SpanOutcome::Success,
            "manifest.json",
        ),
        (
            "get",
            PhysicalRequest::GetFull,
            SpanOutcome::Success,
            "manifest.json",
        ),
        (
            "put",
            PhysicalRequest::PutCreate,
            SpanOutcome::Success,
            "<generation>.msgpack",
        ),
        (
            "put",
            PhysicalRequest::PutUpdate,
            SpanOutcome::Success,
            "manifest.json",
        ),
    ];
    for (operation, (verb, request, outcome, key)) in
        sample.physical_operations.iter().zip(expected)
    {
        assert_eq!(operation.verb, verb);
        assert_eq!(operation.request, request);
        assert_eq!(operation.outcome, outcome);
        assert_eq!(operation.key, key);
    }
}

fn assert_group_commit_missing_put_etag_shape(sample: &IdealSample) {
    assert_eq!(
        sample.total_get_ops, 1,
        "a missing prior CAS ETag must keep the next group commit cold"
    );
    assert_eq!(sample.physical_operations.len(), 4);
    let expected = [
        (
            "put",
            PhysicalRequest::PutOverwrite,
            SpanOutcome::Success,
            "<wal>.wal",
        ),
        (
            "get",
            PhysicalRequest::GetFull,
            SpanOutcome::Success,
            "manifest.json",
        ),
        (
            "put",
            PhysicalRequest::PutCreate,
            SpanOutcome::Success,
            "<generation>.msgpack",
        ),
        (
            "put",
            PhysicalRequest::PutUpdate,
            SpanOutcome::Success,
            "manifest.json",
        ),
    ];
    for (operation, (verb, request, outcome, key)) in
        sample.physical_operations.iter().zip(expected)
    {
        assert_eq!(operation.verb, verb);
        assert_eq!(operation.request, request);
        assert_eq!(operation.outcome, outcome);
        assert_eq!(operation.key, key);
    }
}

fn assert_warm_lease_renewal_shape(sample: &IdealSample) {
    assert_eq!(
        sample.total_get_ops, 0,
        "a warm lease renewal must not issue an authoritative GET"
    );
    assert_eq!(sample.total_get_bytes, 0);
    assert_eq!(
        sample.physical_operations.len(),
        1,
        "a warm lease renewal must be exactly one conditional PUT: {:?}",
        sample.physical_operations
    );
    let operation = &sample.physical_operations[0];
    assert_eq!(operation.verb, "put");
    assert_eq!(operation.request, PhysicalRequest::PutUpdate);
    assert_eq!(operation.outcome, SpanOutcome::Success);
    assert_eq!(operation.key, "lease.json");
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
    let (world, retry_conflict, missing_put_etag) = match operation {
        LeaseCase::RenewDoubleConflict => {
            let (world, handle) = DirectWorld::with_lease_retry_conflict().await;
            (world, Some(handle), None)
        }
        LeaseCase::RenewPutEtagMissing => {
            let (world, handle) = DirectWorld::with_missing_lease_put_etag().await;
            (world, None, Some(handle))
        }
        _ => (DirectWorld::new().await, None, None),
    };
    let namespace = world.namespace(case.id.as_str());
    let owned = LeaseManager::new(
        world.store.clone(),
        "ideal-holder-a".to_string(),
        Duration::from_secs(3_600),
    );
    let mut etag_fallback_lease: Option<Lease> = None;
    let mut cold_renewed_lease: Option<Lease> = None;

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
        LeaseCase::RenewEtagDrift => {
            let stale = owned
                .acquire(&namespace)
                .await
                .expect("ideal ETag-drift setup acquisition failed");
            owned
                .renew(&namespace, &stale)
                .await
                .expect("ideal ETag-drift setup renewal failed");
            world.begin_measurement().await;
            let renewed = owned
                .renew(&namespace, &stale)
                .await
                .expect("same-owner ETag drift must retry once");
            assert_eq!(renewed.fencing_token, stale.fencing_token);
        }
        LeaseCase::RenewDoubleConflict => {
            let stale = owned
                .acquire(&namespace)
                .await
                .expect("ideal double-conflict setup acquisition failed");
            owned
                .renew(&namespace, &stale)
                .await
                .expect("ideal double-conflict setup renewal failed");
            let handle = retry_conflict
                .as_ref()
                .expect("double-conflict case must install its race decorator");
            handle.arm();
            world.begin_measurement().await;
            assert!(matches!(
                owned.renew(&namespace, &stale).await,
                Err(ZeppelinError::LeaseExpired { .. })
            ));
            assert_eq!(
                handle.injections(),
                1,
                "double-conflict scenario did not perturb the retry"
            );
        }
        LeaseCase::RenewMissing => {
            let lease = owned
                .acquire(&namespace)
                .await
                .expect("ideal missing-renewal setup acquisition failed");
            world
                .store
                .delete(&format!("{namespace}/lease.json"))
                .await
                .expect("ideal missing-renewal setup delete failed");
            world.begin_measurement().await;
            assert!(matches!(
                owned.renew(&namespace, &lease).await,
                Err(ZeppelinError::LeaseExpired { .. })
            ));
        }
        LeaseCase::RenewCold => {
            let acquired = owned
                .acquire(&namespace)
                .await
                .expect("ideal cold-renewal setup acquisition failed");
            let cold: Lease = serde_json::from_slice(
                &serde_json::to_vec(&acquired).expect("serialize cold-renewal snapshot"),
            )
            .expect("deserialize cold-renewal snapshot");
            world.begin_measurement().await;
            let renewed = owned
                .renew(&namespace, &cold)
                .await
                .expect("cold lease renewal failed");
            assert_eq!(renewed.fencing_token, acquired.fencing_token);
            cold_renewed_lease = Some(renewed);
        }
        LeaseCase::RenewPutEtagMissing => {
            let lease = owned
                .acquire(&namespace)
                .await
                .expect("ideal missing-PUT-ETag setup acquisition failed");
            world.begin_measurement().await;
            let renewed = owned
                .renew(&namespace, &lease)
                .await
                .expect("renewal must recover a missing PUT ETag");
            assert_eq!(renewed.fencing_token, lease.fencing_token);
            assert_eq!(
                missing_put_etag
                    .as_ref()
                    .expect("missing-PUT-ETag case must install its decorator")
                    .stripped(),
                1
            );
            etag_fallback_lease = Some(renewed);
        }
        LeaseCase::RenewTakenOver => {
            let expired = LeaseManager::new(
                world.store.clone(),
                "ideal-holder-a".to_string(),
                Duration::ZERO,
            );
            let stale = expired
                .acquire(&namespace)
                .await
                .expect("ideal stale-renewal setup acquisition failed");
            let successor = LeaseManager::new(
                world.store.clone(),
                "ideal-holder-b".to_string(),
                Duration::from_secs(3_600),
            );
            successor
                .acquire(&namespace)
                .await
                .expect("ideal stale-renewal takeover setup failed");
            world.begin_measurement().await;
            assert!(matches!(
                expired.renew(&namespace, &stale).await,
                Err(ZeppelinError::LeaseExpired { .. })
            ));
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

    let sample = world.snapshot(case).await;
    match operation {
        LeaseCase::RenewOwned => assert_warm_lease_renewal_shape(&sample),
        LeaseCase::RenewEtagDrift => assert_benign_lease_etag_drift_shape(&sample),
        LeaseCase::RenewDoubleConflict => assert_double_lease_conflict_shape(&sample),
        LeaseCase::RenewMissing => assert_missing_lease_renewal_shape(&sample),
        LeaseCase::RenewCold => assert_cold_lease_renewal_shape(&sample),
        LeaseCase::RenewPutEtagMissing => assert_missing_put_etag_renewal_shape(&sample),
        LeaseCase::RenewTakenOver => assert_taken_over_lease_renewal_shape(&sample),
        _ => {}
    }

    if let Some(renewed) = etag_fallback_lease {
        world.begin_measurement().await;
        owned
            .renew(&namespace, &renewed)
            .await
            .expect("fallback GET must return the ETag used by the next renewal");
        let proof = world.snapshot(case).await;
        assert_missing_put_etag_renewal_shape(&proof);
        assert_eq!(
            missing_put_etag
                .as_ref()
                .expect("missing-PUT-ETag proof lost its decorator")
                .stripped(),
            2
        );
    }
    if let Some(renewed) = cold_renewed_lease {
        world.begin_measurement().await;
        owned
            .renew(&namespace, &renewed)
            .await
            .expect("cold renewal must return a warm ETag-bearing lease");
        let proof = world.snapshot(case).await;
        assert_warm_lease_renewal_shape(&proof);
    }
    world.cleanup(sample).await
}

fn assert_cold_lease_renewal_shape(sample: &IdealSample) {
    assert_eq!(sample.total_get_ops, 1);
    assert_eq!(sample.physical_operations.len(), 2);
    let expected = [
        ("get", PhysicalRequest::GetFull, SpanOutcome::Success),
        ("put", PhysicalRequest::PutUpdate, SpanOutcome::Success),
    ];
    for (operation, (verb, request, outcome)) in sample.physical_operations.iter().zip(expected) {
        assert_eq!(operation.verb, verb);
        assert_eq!(operation.request, request);
        assert_eq!(operation.outcome, outcome);
        assert_eq!(operation.key, "lease.json");
    }
}

fn assert_missing_put_etag_renewal_shape(sample: &IdealSample) {
    assert_eq!(sample.total_get_ops, 1);
    assert_eq!(sample.physical_operations.len(), 2);
    let expected = [
        ("put", PhysicalRequest::PutUpdate, SpanOutcome::Success),
        ("get", PhysicalRequest::GetFull, SpanOutcome::Success),
    ];
    for (operation, (verb, request, outcome)) in sample.physical_operations.iter().zip(expected) {
        assert_eq!(operation.verb, verb);
        assert_eq!(operation.request, request);
        assert_eq!(operation.outcome, outcome);
        assert_eq!(operation.key, "lease.json");
    }
}

fn assert_missing_lease_renewal_shape(sample: &IdealSample) {
    assert_eq!(sample.total_get_ops, 1);
    assert_eq!(sample.physical_operations.len(), 2);
    let expected = [
        ("put", PhysicalRequest::PutUpdate, SpanOutcome::Precondition),
        ("get", PhysicalRequest::GetFull, SpanOutcome::NotFound),
    ];
    for (operation, (verb, request, outcome)) in sample.physical_operations.iter().zip(expected) {
        assert_eq!(operation.verb, verb);
        assert_eq!(operation.request, request);
        assert_eq!(operation.outcome, outcome);
        assert_eq!(operation.key, "lease.json");
    }
}

fn assert_double_lease_conflict_shape(sample: &IdealSample) {
    assert_eq!(
        sample.total_get_ops, 1,
        "a second CAS conflict must not start another classification loop"
    );
    assert_eq!(sample.physical_operations.len(), 3);
    let expected = [
        ("put", PhysicalRequest::PutUpdate, SpanOutcome::Precondition),
        ("get", PhysicalRequest::GetFull, SpanOutcome::Success),
        ("put", PhysicalRequest::PutUpdate, SpanOutcome::Precondition),
    ];
    for (operation, (verb, request, outcome)) in sample.physical_operations.iter().zip(expected) {
        assert_eq!(operation.verb, verb);
        assert_eq!(operation.request, request);
        assert_eq!(operation.outcome, outcome);
        assert_eq!(operation.key, "lease.json");
    }
}

fn assert_benign_lease_etag_drift_shape(sample: &IdealSample) {
    assert_eq!(
        sample.total_get_ops, 1,
        "benign ETag drift must use one authoritative classification GET"
    );
    assert_eq!(sample.physical_operations.len(), 3);
    let expected = [
        ("put", PhysicalRequest::PutUpdate, SpanOutcome::Precondition),
        ("get", PhysicalRequest::GetFull, SpanOutcome::Success),
        ("put", PhysicalRequest::PutUpdate, SpanOutcome::Success),
    ];
    for (operation, (verb, request, outcome)) in sample.physical_operations.iter().zip(expected) {
        assert_eq!(operation.verb, verb);
        assert_eq!(operation.request, request);
        assert_eq!(operation.outcome, outcome);
        assert_eq!(operation.key, "lease.json");
    }
}

fn assert_taken_over_lease_renewal_shape(sample: &IdealSample) {
    assert_eq!(
        sample.total_get_ops, 1,
        "a stale holder must classify one failed CAS with one authoritative GET"
    );
    assert_eq!(sample.physical_operations.len(), 2);
    assert_eq!(sample.physical_operations[0].verb, "put");
    assert_eq!(
        sample.physical_operations[0].request,
        PhysicalRequest::PutUpdate
    );
    assert_eq!(
        sample.physical_operations[0].outcome,
        SpanOutcome::Precondition
    );
    assert_eq!(sample.physical_operations[1].verb, "get");
    assert_eq!(
        sample.physical_operations[1].request,
        PhysicalRequest::GetFull
    );
    assert_eq!(sample.physical_operations[1].outcome, SpanOutcome::Success);
    assert!(sample
        .physical_operations
        .iter()
        .all(|operation| operation.key == "lease.json"));
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

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn warm_group_commit_is_three_puts_without_manifest_get() {
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "writer.group_commit_warm")
            .expect("warm group-commit direct case must exist");

        let sample = execute(case)
            .await
            .expect("warm group-commit case must have a direct executor");

        assert_group_commit_warm_shape(&sample);
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn stale_group_commit_memo_rebuilds_after_history_conflict() {
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "writer.group_commit_conflict")
            .expect("conflicting group-commit direct case must exist");

        let sample = execute(case)
            .await
            .expect("conflicting group-commit case must have a direct executor");

        assert_group_commit_conflict_shape(&sample);
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn missing_manifest_put_etag_keeps_next_group_commit_cold() {
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "writer.group_commit_missing_put_etag")
            .expect("missing-ETag group-commit direct case must exist");

        let sample = execute(case)
            .await
            .expect("missing-ETag group-commit case must have a direct executor");

        assert_group_commit_missing_put_etag_shape(&sample);
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn warm_lease_renewal_is_one_cas_put_without_get() {
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "lease.renew_owned")
            .expect("owned-renewal direct case must exist");

        let sample = execute(case)
            .await
            .expect("owned-renewal case must have a direct executor");

        assert_warm_lease_renewal_shape(&sample);
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn consecutive_warm_lease_renewals_never_regress_to_gets() {
        const RENEWALS: usize = 4;
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "lease.renew_owned")
            .expect("owned-renewal direct case must exist");
        let world = DirectWorld::new().await;
        let namespace = world.namespace("lease.renew_owned.repeated");
        let manager = LeaseManager::new(
            world.store.clone(),
            "ideal-holder-repeated".to_string(),
            Duration::from_secs(3_600),
        );
        let mut lease = manager
            .acquire(&namespace)
            .await
            .expect("repeated renewal setup acquisition failed");

        world.begin_measurement().await;
        for _ in 0..RENEWALS {
            lease = manager
                .renew(&namespace, &lease)
                .await
                .expect("repeated warm renewal failed");
        }
        let sample = world.snapshot(case).await;

        assert_eq!(sample.total_get_ops, 0);
        assert_eq!(sample.total_get_bytes, 0);
        assert_eq!(sample.physical_operations.len(), RENEWALS);
        assert!(sample.physical_operations.iter().all(|operation| {
            operation.verb == "put"
                && operation.request == PhysicalRequest::PutUpdate
                && operation.outcome == SpanOutcome::Success
                && operation.key == "lease.json"
        }));
        world.cleanup(sample).await;
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn taken_over_lease_renewal_classifies_once_and_expires() {
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "lease.renew_taken_over")
            .expect("taken-over renewal direct case must exist");

        let sample = execute(case)
            .await
            .expect("taken-over renewal case must have a direct executor");

        assert_taken_over_lease_renewal_shape(&sample);
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn benign_lease_etag_drift_classifies_once_and_retries_once() {
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "lease.renew_etag_drift")
            .expect("ETag-drift renewal direct case must exist");

        let sample = execute(case)
            .await
            .expect("ETag-drift renewal case must have a direct executor");

        assert_benign_lease_etag_drift_shape(&sample);
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn double_lease_conflict_is_bounded_and_fails_closed() {
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "lease.renew_double_conflict")
            .expect("double-conflict renewal direct case must exist");

        let sample = execute(case)
            .await
            .expect("double-conflict renewal case must have a direct executor");

        assert_double_lease_conflict_shape(&sample);
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn missing_lease_renewal_fails_closed_after_one_classification() {
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "lease.renew_missing")
            .expect("missing renewal direct case must exist");

        let sample = execute(case)
            .await
            .expect("missing renewal case must have a direct executor");

        assert_missing_lease_renewal_shape(&sample);
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn cold_lease_renewal_reads_once_then_returns_a_warm_memo() {
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "lease.renew_cold")
            .expect("cold renewal direct case must exist");

        let sample = execute(case)
            .await
            .expect("cold renewal case must have a direct executor");

        assert_cold_lease_renewal_shape(&sample);
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn missing_put_etag_falls_back_to_one_get_and_advances_the_memo() {
        let case = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "lease.renew_put_etag_missing")
            .expect("missing-PUT-ETag renewal direct case must exist");

        let sample = execute(case)
            .await
            .expect("missing-PUT-ETag renewal case must have a direct executor");

        assert_missing_put_etag_renewal_shape(&sample);
    }
}
