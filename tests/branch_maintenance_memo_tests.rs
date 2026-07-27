#![cfg(feature = "branching-test-support")]

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use common::counting::{counting_store, GetCounter};
use common::fault_injection::toggle_get_failure_matching;
use common::harness::TestHarness;
use futures::StreamExt;
use zeppelin::config::{Config, SecurityMode};
use zeppelin::namespace::branching::test_support::{
    activate_fork_for_test, delete_namespace_for_test, manifest_incarnation_for_test,
    BranchMaintenanceRunnerForTest,
};
use zeppelin::namespace::manager::NamespaceMetadata;
use zeppelin::namespace::{NamespaceId, NamespaceManager};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::{Clock, TimeSource};
use zeppelin::types::DistanceMetric;
use zeppelin::wal::Manifest;

const TICK_BUDGET: Duration = Duration::from_secs(25);

#[derive(Debug)]
struct AdjustableClock(Mutex<DateTime<Utc>>);

impl AdjustableClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self(Mutex::new(now))
    }

    fn set(&self, now: DateTime<Utc>) {
        *self
            .0
            .lock()
            .expect("maintenance clock mutex must not be poisoned") = now;
    }

    fn advance(&self, duration: ChronoDuration) {
        let mut now = self
            .0
            .lock()
            .expect("maintenance clock mutex must not be poisoned");
        *now += duration;
    }
}

impl TimeSource for AdjustableClock {
    fn now(&self) -> DateTime<Utc> {
        *self
            .0
            .lock()
            .expect("maintenance clock mutex must not be poisoned")
    }
}

struct MaintenanceFixture {
    harness: TestHarness,
    namespace: String,
    config: Config,
    clock: Clock,
    counted_store: ZeppelinStore,
    counter: GetCounter,
    runner: BranchMaintenanceRunnerForTest,
}

impl MaintenanceFixture {
    async fn new(branching_enabled: bool, interval_secs: u64) -> Self {
        Self::new_with_clock(branching_enabled, interval_secs, Clock::system()).await
    }

    async fn new_with_clock(branching_enabled: bool, interval_secs: u64, clock: Clock) -> Self {
        let harness = TestHarness::new().await;
        let namespace = harness.artifact_origin_namespace("branch-maintenance");
        let config = maintenance_config(branching_enabled, interval_secs);
        NamespaceManager::new(harness.store.clone())
            .create(&namespace, 4, DistanceMetric::Cosine)
            .await
            .expect("maintenance namespace creation must succeed");
        let (counted_store, counter) = counting_store(&harness.store);
        let runner = BranchMaintenanceRunnerForTest::new_scoped(
            counted_store.clone(),
            &config,
            clock.clone(),
            Some(harness.prefix.clone()),
        )
        .expect("maintenance runner construction must succeed");
        Self {
            harness,
            namespace,
            config,
            clock,
            counted_store,
            counter,
            runner,
        }
    }

    async fn prime(&mut self) {
        self.runner
            .run(TICK_BUDGET)
            .await
            .expect("cold maintenance tick must complete");
    }

    async fn cleanup(self) {
        self.harness.cleanup().await;
    }
}

fn maintenance_config(branching_enabled: bool, interval_secs: u64) -> Config {
    let mut config = Config::default();
    config.security.mode = SecurityMode::OpenUnsafe;
    config.branching.enabled = branching_enabled;
    config.branching.max_children_per_namespace = 8;
    config.branching.max_depth = 4;
    config.compaction.interval_secs = interval_secs;
    config.cache.manifest_cache_ttl_ms = 0;
    config.cache.namespace_registry_ttl_ms = 0;
    config.server.request_timeout_secs = 1;
    config.gc.compaction_upload_window_secs = 1;
    config.gc.skew_slop_secs = 0;
    config.gc.horizon_secs = 2;
    config
        .validate()
        .expect("maintenance test config must pass production validation");
    config
}

fn assert_idle_census(counter: &GetCounter) {
    assert_eq!(
        counter.list_calls_for_prefix(""),
        0,
        "an unchanged tick must not recursively list the bucket"
    );
    assert_eq!(
        counter.delimiter_list_calls_for_prefix(""),
        1,
        "an unchanged tick must issue one delimiter inventory LIST"
    );
    assert_eq!(
        counter.total_gets(),
        0,
        "an unchanged tick must issue no GETs"
    );
}

fn assert_cold_census(counter: &GetCounter) {
    assert_eq!(
        counter.list_calls_for_prefix(""),
        0,
        "a cold tick must not recursively list the bucket"
    );
    assert_eq!(
        counter.delimiter_list_calls_for_prefix(""),
        1,
        "a cold tick must begin with one delimiter inventory LIST"
    );
    assert!(
        counter.gets_matching("meta.json") > 0,
        "a cold tick must strongly read namespace metadata"
    );
    assert!(
        counter.gets_matching("manifest.json") > 0,
        "a cold tick must preserve active-manifest verification"
    );
}

async fn create_namespace(store: &ZeppelinStore, namespace: &str) {
    NamespaceManager::new(store.clone())
        .create(namespace, 4, DistanceMetric::Cosine)
        .await
        .expect("maintenance fixture namespace creation must succeed");
}

#[tokio::test]
async fn unchanged_second_tick_costs_one_list_and_zero_gets() {
    let mut fixture = MaintenanceFixture::new(false, 60).await;
    fixture.prime().await;

    fixture.counter.reset();
    fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_idle_census(&fixture.counter);

    fixture.cleanup().await;
}

#[tokio::test]
async fn many_namespace_objects_do_not_expand_idle_listing() {
    let mut fixture = MaintenanceFixture::new(false, 60).await;
    let puts = futures::stream::iter(0..1_025)
        .map(|index| {
            let store = fixture.harness.store.clone();
            let key = format!("{}/segments/dense-{index:04}.bin", fixture.namespace);
            async move { store.put(&key, Bytes::from_static(b"dense")).await }
        })
        .buffer_unordered(32)
        .collect::<Vec<_>>()
        .await;
    for result in puts {
        result.expect("dense namespace fixture PUT must succeed");
    }
    fixture.prime().await;

    fixture.counter.reset();
    fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_idle_census(&fixture.counter);

    fixture.cleanup().await;
}

#[tokio::test]
async fn branching_enabled_without_forks_still_uses_idle_fast_path() {
    let mut fixture = MaintenanceFixture::new(true, 60).await;
    fixture.prime().await;

    fixture.counter.reset();
    fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_idle_census(&fixture.counter);

    fixture.cleanup().await;
}

#[tokio::test]
async fn namespace_addition_invalidates_the_inventory() {
    let mut fixture = MaintenanceFixture::new(false, 60).await;
    fixture.prime().await;
    let added = fixture
        .harness
        .artifact_origin_namespace("branch-maintenance-added");
    create_namespace(&fixture.harness.store, &added).await;

    fixture.counter.reset();
    fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_cold_census(&fixture.counter);

    fixture.cleanup().await;
}

#[tokio::test]
async fn namespace_removal_invalidates_the_inventory() {
    let mut fixture = MaintenanceFixture::new(false, 60).await;
    let removed = fixture
        .harness
        .artifact_origin_namespace("branch-maintenance-removed");
    create_namespace(&fixture.harness.store, &removed).await;
    fixture.prime().await;
    fixture
        .harness
        .store
        .delete_prefix(&format!("{removed}/"))
        .await
        .unwrap();

    fixture.counter.reset();
    fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_cold_census(&fixture.counter);

    fixture.cleanup().await;
}

#[tokio::test]
async fn namespace_metadata_identity_change_is_bounded_by_maturity_deadline() {
    let source = Arc::new(AdjustableClock::new(Utc::now()));
    let mut fixture =
        MaintenanceFixture::new_with_clock(false, 5, Clock::from_source(source.clone())).await;
    fixture.prime().await;
    NamespaceManager::new(fixture.harness.store.clone())
        .record_compaction_success(&fixture.namespace)
        .await
        .unwrap();

    fixture.counter.reset();
    fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_idle_census(&fixture.counter);

    source.advance(ChronoDuration::seconds(5));
    fixture.counter.reset();
    fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_cold_census(&fixture.counter);

    fixture.cleanup().await;
}

#[tokio::test]
async fn namespace_recreation_under_the_same_name_is_bounded_by_maturity_deadline() {
    let source = Arc::new(AdjustableClock::new(Utc::now()));
    let mut fixture =
        MaintenanceFixture::new_with_clock(false, 5, Clock::from_source(source.clone())).await;
    fixture.prime().await;
    fixture
        .harness
        .store
        .delete_prefix(&format!("{}/", fixture.namespace))
        .await
        .unwrap();
    create_namespace(&fixture.harness.store, &fixture.namespace).await;

    fixture.counter.reset();
    fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_idle_census(&fixture.counter);

    source.advance(ChronoDuration::seconds(5));
    fixture.counter.reset();
    fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_cold_census(&fixture.counter);

    fixture.cleanup().await;
}

#[tokio::test]
async fn deleting_namespace_resumes_within_one_maturity_period() {
    let source = Arc::new(AdjustableClock::new(Utc::now()));
    let mut fixture =
        MaintenanceFixture::new_with_clock(false, 5, Clock::from_source(source.clone())).await;
    fixture.prime().await;
    let outcome = delete_namespace_for_test(
        fixture.harness.store.clone(),
        NamespaceId::new(fixture.namespace.clone()).unwrap(),
        fixture.config.indexing.clone(),
        fixture.config.branching.clone(),
    )
    .await
    .unwrap();
    assert!(
        !matches!(
            outcome,
            zeppelin::namespace::branching::NamespaceDeleteOutcome::Deleted
        ),
        "fixture must leave a crash-resumable deletion"
    );

    fixture.counter.reset();
    let report = fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_eq!(report.deletions_inspected, 0);
    assert_idle_census(&fixture.counter);

    source.advance(ChronoDuration::seconds(5));
    fixture.counter.reset();
    let report = fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert!(report.deletions_inspected > 0);
    assert!(
        matches!(
            fixture
                .harness
                .store
                .get(&NamespaceMetadata::s3_key(&fixture.namespace))
                .await,
            Err(zeppelin::error::ZeppelinError::NotFound { .. })
        ),
        "maintenance must finish metadata-last deletion"
    );

    fixture.cleanup().await;
}

#[tokio::test]
async fn fork_creation_invalidates_and_verifies_child_roots() {
    let mut fixture = MaintenanceFixture::new(true, 60).await;
    fixture.prime().await;
    let target = fixture
        .harness
        .artifact_origin_namespace("branch-maintenance-fork");
    activate_fork_for_test(
        fixture.harness.store.clone(),
        NamespaceId::new(fixture.namespace.clone()).unwrap(),
        NamespaceId::new(target).unwrap(),
        fixture.config.indexing.clone(),
        fixture.config.branching.clone(),
    )
    .await
    .unwrap();

    fixture.counter.reset();
    let report = fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert!(report.active_verified > 0);
    assert_cold_census(&fixture.counter);

    fixture.cleanup().await;
}

#[tokio::test]
async fn exhausted_budget_makes_the_next_tick_cold() {
    let mut fixture = MaintenanceFixture::new(false, 60).await;
    fixture.prime().await;
    fixture.runner.run(Duration::ZERO).await.unwrap();

    fixture.counter.reset();
    fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_cold_census(&fixture.counter);

    fixture.cleanup().await;
}

#[tokio::test]
async fn errored_pass_does_not_publish_a_warm_memo() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("branch-maintenance-error");
    let config = maintenance_config(false, 60);
    create_namespace(&harness.store, &namespace).await;
    let (counted, counter) = counting_store(&harness.store);
    let (faulted, fault) =
        toggle_get_failure_matching(&counted, NamespaceMetadata::s3_key(&namespace));
    let mut runner = BranchMaintenanceRunnerForTest::new_scoped(
        faulted,
        &config,
        Clock::system(),
        Some(harness.prefix.clone()),
    )
    .unwrap();
    runner.run(TICK_BUDGET).await.unwrap();
    let mut changed = config.clone();
    changed.branching.max_children_per_namespace += 1;
    changed.validate().unwrap();
    runner.update_config(&changed).unwrap();

    fault.enable();
    runner
        .run(TICK_BUDGET)
        .await
        .expect_err("metadata GET fault must fail the full pass");
    fault.disable();
    counter.reset();
    runner.run(TICK_BUDGET).await.unwrap();
    assert_cold_census(&counter);

    harness.cleanup().await;
}

#[tokio::test]
async fn process_restart_starts_cold() {
    let mut fixture = MaintenanceFixture::new(false, 60).await;
    fixture.prime().await;
    let mut restarted = BranchMaintenanceRunnerForTest::new_scoped(
        fixture.counted_store.clone(),
        &fixture.config,
        fixture.clock.clone(),
        Some(fixture.harness.prefix.clone()),
    )
    .unwrap();

    fixture.counter.reset();
    restarted.run(TICK_BUDGET).await.unwrap();
    assert_cold_census(&fixture.counter);

    fixture.cleanup().await;
}

#[tokio::test]
async fn maturity_deadline_forces_a_full_pass_with_identical_inventory() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("branch-maintenance-deadline");
    let config = maintenance_config(false, 5);
    create_namespace(&harness.store, &namespace).await;
    let source = Arc::new(AdjustableClock::new(Utc::now()));
    let clock = Clock::from_source(source.clone());
    let (counted, counter) = counting_store(&harness.store);
    let mut runner = BranchMaintenanceRunnerForTest::new_scoped(
        counted,
        &config,
        clock,
        Some(harness.prefix.clone()),
    )
    .unwrap();
    runner.run(TICK_BUDGET).await.unwrap();
    source.advance(ChronoDuration::seconds(5));

    counter.reset();
    runner.run(TICK_BUDGET).await.unwrap();
    assert_cold_census(&counter);

    harness.cleanup().await;
}

#[tokio::test]
async fn backward_clock_step_never_admits_a_false_warm_tick() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("branch-maintenance-backward-clock");
    let config = maintenance_config(false, 60);
    create_namespace(&harness.store, &namespace).await;
    let now = Utc::now();
    let source = Arc::new(AdjustableClock::new(now));
    let clock = Clock::from_source(source.clone());
    let (counted, counter) = counting_store(&harness.store);
    let mut runner = BranchMaintenanceRunnerForTest::new_scoped(
        counted,
        &config,
        clock,
        Some(harness.prefix.clone()),
    )
    .unwrap();
    runner.run(TICK_BUDGET).await.unwrap();
    source.set(now - ChronoDuration::seconds(1));

    counter.reset();
    runner.run(TICK_BUDGET).await.unwrap();
    assert_cold_census(&counter);

    harness.cleanup().await;
}

#[tokio::test]
async fn non_branching_warm_runner_resumes_delete_within_one_maturity_period() {
    let source = Arc::new(AdjustableClock::new(Utc::now()));
    let mut fixture =
        MaintenanceFixture::new_with_clock(false, 5, Clock::from_source(source.clone())).await;
    fixture.prime().await;
    assert!(!fixture.config.branching.enabled);
    delete_namespace_for_test(
        fixture.harness.store.clone(),
        NamespaceId::new(fixture.namespace.clone()).unwrap(),
        fixture.config.indexing.clone(),
        fixture.config.branching.clone(),
    )
    .await
    .unwrap();

    fixture.counter.reset();
    let report = fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_eq!(report.deletions_inspected, 0);
    assert_idle_census(&fixture.counter);

    source.advance(ChronoDuration::seconds(5));
    let report = fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert!(report.deletions_inspected > 0);
    assert!(
        matches!(
            fixture
                .harness
                .store
                .get(&NamespaceMetadata::s3_key(&fixture.namespace))
                .await,
            Err(zeppelin::error::ZeppelinError::NotFound { .. })
        ),
        "branching-disabled maintenance must finish the interrupted delete"
    );

    fixture.cleanup().await;
}

#[tokio::test]
async fn unbound_manifest_is_migrated_within_one_maturity_period() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("branch-maintenance-migration");
    let config = maintenance_config(false, 5);
    create_namespace(&harness.store, &namespace).await;
    let metadata = NamespaceManager::new(harness.store.clone())
        .get(&namespace)
        .await
        .unwrap();
    let incarnation = metadata.incarnation_id.unwrap();
    let now = Utc::now();
    let source = Arc::new(AdjustableClock::new(now));
    let clock = Clock::from_source(source.clone());
    let (counted, _) = counting_store(&harness.store);
    let mut runner = BranchMaintenanceRunnerForTest::new_scoped(
        counted,
        &config,
        clock,
        Some(harness.prefix.clone()),
    )
    .unwrap();
    runner.run(TICK_BUDGET).await.unwrap();

    harness
        .store
        .delete(&Manifest::s3_key(&namespace))
        .await
        .unwrap();
    harness
        .store
        .delete_prefix(&Manifest::history_prefix(&namespace))
        .await
        .unwrap();
    let mut legacy = Manifest::new();
    legacy.write(&harness.store, &namespace).await.unwrap();
    assert_eq!(
        manifest_incarnation_for_test(&harness.store, &namespace)
            .await
            .unwrap(),
        None
    );

    runner.run(TICK_BUDGET).await.unwrap();
    assert_eq!(
        manifest_incarnation_for_test(&harness.store, &namespace)
            .await
            .unwrap(),
        None,
        "the warm memo may defer migration only until its maturity deadline"
    );
    source.advance(ChronoDuration::seconds(5));
    runner.run(TICK_BUDGET).await.unwrap();
    assert_eq!(
        manifest_incarnation_for_test(&harness.store, &namespace)
            .await
            .unwrap(),
        Some(incarnation)
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn branching_or_gc_policy_change_invalidates_the_memo() {
    let mut fixture = MaintenanceFixture::new(false, 60).await;
    fixture.prime().await;
    let mut changed = fixture.config.clone();
    changed.branching.max_children_per_namespace += 1;
    changed.gc.horizon_secs += 1;
    changed.validate().unwrap();
    fixture.runner.update_config(&changed).unwrap();

    fixture.counter.reset();
    fixture.runner.run(TICK_BUDGET).await.unwrap();
    assert_cold_census(&fixture.counter);

    fixture.cleanup().await;
}
