//! Scenario lifecycle and performance-contract entry orchestration.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use object_store::path::Path as ObjectPath;
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use zeppelin::compaction::gc;
use zeppelin::config::{Config, GcConfig};
use zeppelin::fts::global_index::global_fts_key;
use zeppelin::index::bitmap::bitmap_key;
use zeppelin::index::ivf_flat::build::attrs_key;
use zeppelin::index::ivf_flat::membership::deserialize_membership;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::manager::NamespaceMetadata;
use zeppelin::time::{Clock, TimeSource};
use zeppelin::wal::manifest::SegmentRef;
use zeppelin::wal::Manifest;

use crate::common::counting::{perf_counting_store, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::common::server::{api_ns, start_test_server_full, FullTestServer};
use zeppelin::storage::ZeppelinStore;

use super::contract::{
    capture_contract, check_contract, load_contract, ContractSpec, CostViolation, DepthMode,
};
use super::dataset::{generate, DatasetExpectations, DatasetSpec, GenVector, GeneratedDataset};
use super::depth::{depth_store, CriticalPath, DepthTracker, OpSpan, SpanKind};
use super::injection::{inject_store, Injection};
use super::report::RunArtifacts;
use super::{require_minio, scenarios, PerfEnv};

const SETUP_BATCH_SIZE: usize = 256;
const COMPACTION_ATTEMPTS: usize = 4;
const STABILITY_REPEATS: usize = 100;
const MSGPACK_POSITIVE_FIXINT_MAX: u64 = 127;

/// Real wall time normalized to a stable six-digit RFC3339 fraction.
///
/// Chrono's `AutoSi` serializer emits 0, 3, 6, or 9 fractional digits. The
/// performance contracts count serialized bytes, so landing exactly on a
/// millisecond otherwise makes timestamp-bearing objects three bytes shorter.
/// Elapsed-time and critical-path measurements continue to use `Instant`.
#[derive(Debug)]
struct StableWidthPerfTime {
    last_micros: AtomicI64,
}

impl TimeSource for StableWidthPerfTime {
    fn now(&self) -> DateTime<Utc> {
        let mut previous = self.last_micros.load(Ordering::SeqCst);
        loop {
            let real_micros = Utc::now().timestamp_micros();
            let mut candidate = real_micros.max(previous.saturating_add(1));
            if candidate.rem_euclid(1_000) == 0 {
                candidate = candidate.saturating_add(1);
            }
            match self.last_micros.compare_exchange_weak(
                previous,
                candidate,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return DateTime::from_timestamp_micros(candidate)
                        .expect("performance timestamp must be representable");
                }
                Err(actual) => previous = actual,
            }
        }
    }
}

fn stable_width_perf_clock() -> Clock {
    Clock::from_source(Arc::new(StableWidthPerfTime {
        last_micros: AtomicI64::new(i64::MIN),
    }))
}

/// Namespace options passed as a raw HTTP creation body.
pub type NsConfig = Value;

/// Contract-pinned server values that affect measured storage costs.
#[derive(Debug, Clone)]
pub struct ServerKnobs {
    pub nprobe: usize,
    pub manifest_cache_ttl_ms: u64,
    pub memory_cache_max_mb: usize,
    pub max_wal_fragments_before_compact: usize,
    pub bitmap_index: bool,
    pub fts_index: bool,
    pub hydration_enabled: bool,
}

/// One explicit cache precondition.
#[derive(Debug, Clone)]
pub enum CacheState {
    Cold,
    Warm { prime: Vec<Step> },
    WarmHydrated,
}

/// A priming action performed outside measurement.
#[derive(Debug, Clone, Copy)]
pub enum Step {
    Measure,
    Query,
}

/// Deterministic setup lifecycle executed before the measured operation.
#[derive(Debug, Clone)]
pub enum SetupPlan {
    Compacted,
    EventualDeletes { fragments: usize },
    AsOfRetainedGeneration,
    CompactionFull { fragments: usize },
    CompactionIncremental { fragments: usize },
    GcNothingEligible,
    Hydration,
}

/// The one operation measured by a scenario.
#[derive(Debug, Clone)]
pub enum MeasureOp {
    Query {
        consistency: &'static str,
        top_k: usize,
        query_index: usize,
        filter: Option<(String, String)>,
    },
    FtsQuery {
        consistency: &'static str,
        top_k: usize,
        field: String,
        query: String,
    },
    HybridQuery {
        consistency: &'static str,
        top_k: usize,
        candidate_k: usize,
        query_index: usize,
        field: String,
        query: String,
    },
    AsOfQuery {
        consistency: &'static str,
        top_k: usize,
        query_index: usize,
    },
    QueryPages {
        consistency: &'static str,
        pages: usize,
        page_size: usize,
        query_index: usize,
    },
    Fetch {
        consistency: &'static str,
        ids: usize,
    },
    Upsert {
        batch: usize,
    },
    Delete {
        count: usize,
    },
    Compact {
        fragments: usize,
        incremental: bool,
    },
    Gc,
    Hydrate {
        attempts: usize,
    },
}

/// Complete execution specification for one isolated scenario world.
#[derive(Debug, Clone)]
pub struct ScenarioSpec {
    pub name: String,
    pub dataset: DatasetSpec,
    pub ns_config: NsConfig,
    pub server_config: ServerKnobs,
    pub setup: SetupPlan,
    pub cache_state: CacheState,
    pub measure: MeasureOp,
    pub repeats: usize,
}

/// Deterministic counters and diagnostic spans for one measured operation.
#[derive(Debug, Clone, Serialize)]
pub struct RepeatCounters {
    pub classes: BTreeMap<String, ClassStats>,
    pub totals: ClassStats,
    pub get_path: CriticalPath,
    pub put_get_path: CriticalPath,
    pub spans: Vec<OpSpan>,
    pub op_counts: BTreeMap<String, u64>,
    pub labeled: Vec<LabeledCounters>,
    #[serde(skip)]
    pub wall_elapsed_us: u64,
    #[serde(skip)]
    pub response_cutoff_us: u64,
    /// Unfiltered paths are diagnostic inputs for the stability study only.
    #[serde(skip)]
    pub raw_get_path: CriticalPath,
    #[serde(skip)]
    pub raw_put_get_path: CriticalPath,
}

/// Counters for one named sub-operation inside a compound measurement.
#[derive(Debug, Clone, Serialize)]
pub struct LabeledCounters {
    pub label: String,
    /// GET count used by labeled contract assertions. Compound operations may
    /// scope this to their documented data-plane classes while totals retain
    /// every physical object-store operation.
    pub contract_get_ops: u64,
    pub classes: BTreeMap<String, ClassStats>,
    pub totals: ClassStats,
    pub get_path: CriticalPath,
    pub put_get_path: CriticalPath,
    pub spans: Vec<OpSpan>,
    pub op_counts: BTreeMap<String, u64>,
    #[serde(skip)]
    pub response_cutoff_us: u64,
}

struct NamespaceCleanupGuard {
    store: ZeppelinStore,
    namespaces: Vec<Arc<str>>,
    armed: bool,
}

impl NamespaceCleanupGuard {
    fn new(store: ZeppelinStore, namespace: String) -> Self {
        Self {
            store,
            namespaces: vec![Arc::from(namespace)],
            armed: true,
        }
    }

    fn add(&mut self, namespace: String) {
        assert!(
            self.armed,
            "cannot add a namespace to a disarmed cleanup guard"
        );
        assert!(
            !self
                .namespaces
                .iter()
                .any(|known| known.as_ref() == namespace),
            "duplicate performance namespace cleanup registration: {namespace}"
        );
        self.namespaces.push(Arc::from(namespace));
    }

    async fn cleanup(mut self) {
        for namespace in &self.namespaces {
            let prefix = format!("{namespace}/");
            self.store
                .delete_prefix(&prefix)
                .await
                .unwrap_or_else(|error| {
                    panic!("failed to clean performance namespace {namespace}: {error}")
                });
            let remaining = self
                .store
                .list_prefix(&prefix)
                .await
                .unwrap_or_else(|error| {
                    panic!("failed to verify performance namespace cleanup {namespace}: {error}")
                });
            assert!(
                remaining.is_empty(),
                "performance namespace cleanup left objects under {namespace}: {remaining:?}"
            );
        }
        self.armed = false;
    }
}

impl Drop for NamespaceCleanupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let store = self.store.clone();
        let namespaces = self.namespaces.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new()
                .expect("failed to build panic-cleanup Tokio runtime");
            runtime.block_on(async move {
                for namespace in namespaces {
                    let prefix = format!("{namespace}/");
                    if let Err(error) = store.delete_prefix(&prefix).await {
                        eprintln!("[perf-contract] panic cleanup failed for {namespace}: {error}");
                    }
                }
            });
        });
    }
}

/// Measured output and the generator's closed-form expectations.
#[derive(Debug, Clone)]
pub struct ScenarioOutcome {
    pub per_repeat: Vec<RepeatCounters>,
    pub expected: DatasetExpectations,
}

/// Advisory closed-loop timing output. Depth is intentionally not asserted
/// because multiple client requests are in flight at once.
#[derive(Debug, Clone)]
pub struct ClosedLoopOutcome {
    pub request_wall_us: Vec<u64>,
    pub elapsed_us: u64,
}

#[derive(Debug, Default)]
struct WorldState {
    as_of_generation: Option<String>,
    hydration_keys: Vec<String>,
    eventual_wal_keys: Vec<String>,
    repeat_cache_keys: Vec<String>,
    measure_offset: usize,
}

/// Run and check the complete checked-in catalog (or an env-selected subset).
pub async fn run_contracts_entry() {
    let env = PerfEnv::from_env();
    require_minio();
    assert!(
        !env.capture,
        "contracts entry refuses ZEPPELIN_PERF_CAPTURE; use the capture entry"
    );
    let artifacts = RunArtifacts::create(&env, "contracts", &env.scenarios);
    let mut failures = Vec::new();

    for name in &env.scenarios {
        let contract = match load_contract(name) {
            Ok(contract) => contract,
            Err(error) => {
                artifacts.write_contract_error(name, error.clone());
                failures.push(format!("{name}: {error}"));
                continue;
            }
        };
        let spec = scenarios::build(&contract, env.repeats);
        let outcome = run_scenario(&spec, None).await;
        let violations = check_contract(&contract, &outcome);
        artifacts.write_scenario(name, &outcome, &violations);
        if !violations.is_empty() {
            failures.push(format!("{name}: {violations:#?}"));
        }
    }

    let report = artifacts.write_report();
    println!("performance-contract report: {}", report.display());
    assert!(
        failures.is_empty(),
        "performance contract failures: {failures:#?}"
    );
}

/// Capture complete proposal TOMLs without checking or granting approval.
pub async fn run_capture_entry() {
    let env = PerfEnv::from_env();
    require_minio();
    assert!(
        env.capture,
        "capture entry requires ZEPPELIN_PERF_CAPTURE=1"
    );
    let artifacts = RunArtifacts::create(&env, "capture", &env.scenarios);
    let captured = chrono::Utc::now().to_rfc3339();
    let mut failures = Vec::new();

    for name in &env.scenarios {
        let contract = match load_contract(name) {
            Ok(contract) => contract,
            Err(error) => {
                artifacts.write_contract_error(name, error.clone());
                failures.push(format!("{name}: {error}"));
                continue;
            }
        };
        let spec = scenarios::build(&contract, env.repeats);
        let outcome = run_scenario(&spec, None).await;
        artifacts.write_scenario(name, &outcome, &[]);
        let proposed = capture_contract(
            &contract,
            &outcome,
            artifacts.git_rev().to_string(),
            captured.clone(),
        );
        let toml = toml::to_string_pretty(&proposed)
            .unwrap_or_else(|error| panic!("failed to encode proposed contract {name}: {error}"));
        artifacts.write_proposed(name, &toml);
    }

    let report = artifacts.write_report();
    println!(
        "performance-contract proposals: {}/proposed",
        artifacts.root().display()
    );
    println!("performance-contract report: {}", report.display());
    assert!(
        failures.is_empty(),
        "performance contract capture configuration failures: {failures:#?}"
    );
}

/// Prove the checker detects every planned regression and accepts a clean run.
pub async fn run_selftest_entry() {
    let env = PerfEnv::from_env();
    require_minio();
    assert!(!env.capture, "perf_selftest cannot run in capture mode");

    let injections = match env.selftest.as_deref() {
        Some(key) => vec![Injection::parse(key)],
        None => vec![
            Injection::ExtraManifestGet,
            Injection::SerializeClusterGets,
            Injection::ExtraHistoryPut,
        ],
    };
    let mut labels = vec!["clean-control".to_string()];
    labels.extend(
        injections
            .iter()
            .map(|injection| injection.key().to_string()),
    );
    let artifacts = RunArtifacts::create(&env, "perf_selftest", &labels);

    let clean_contract = load_or_panic("warm_query_strong");
    let clean_spec = scenarios::build(&clean_contract, env.repeats);
    let clean = run_scenario(&clean_spec, None).await;
    let clean_violations = check_contract(&clean_contract, &clean);
    artifacts.write_scenario("clean-control", &clean, &clean_violations);
    assert!(
        clean_violations.is_empty(),
        "perf selftest clean control failed: {clean_violations:#?}"
    );

    for injection in injections {
        let scenario_name = match injection {
            Injection::ExtraManifestGet | Injection::SerializeClusterGets => "warm_query_strong",
            Injection::ExtraHistoryPut => "upsert_single",
        };
        let contract = load_or_panic(scenario_name);
        let spec = scenarios::build(&contract, env.repeats);
        let outcome = run_scenario(&spec, Some(injection)).await;
        let violations = check_contract(&contract, &outcome);
        artifacts.write_scenario(injection.key(), &outcome, &violations);
        assert_expected_injection(injection, &violations);
    }

    let report = artifacts.write_report();
    println!("performance selftest report: {}", report.display());
}

/// Execute the required 100-repeat depth stability study.
pub async fn run_stability_entry() {
    let env = PerfEnv::from_env();
    require_minio();
    assert!(!env.capture, "depth_stability cannot run in capture mode");
    let artifacts = RunArtifacts::create(&env, "depth_stability", &env.scenarios);
    let mut markdown = String::from("# Depth Stability Study\n\n");
    markdown.push_str(&format!("- repeats per scenario: {STABILITY_REPEATS}\n"));
    markdown.push_str(
        "- cold samples use fresh namespaces; warm/write samples reuse only their declared pinned world\n",
    );
    markdown.push_str("- every sample runs serially with no background loops\n\n");
    let mut failures = Vec::new();

    for name in &env.scenarios {
        let contract = load_or_panic(name);
        let spec = scenarios::build(&contract, 1);
        let outcome = stability_outcome(&spec).await;
        assert_eq!(
            outcome.per_repeat.len(),
            STABILITY_REPEATS,
            "stability scenario {name} did not produce exactly {STABILITY_REPEATS} repeats"
        );
        let mut raw = BTreeMap::<u32, usize>::new();
        let mut cutoff = BTreeMap::<u32, usize>::new();

        for (repeat, measured) in outcome.per_repeat.iter().enumerate() {
            let (raw_depth, cutoff_depth) = relevant_depths(&spec.measure, measured);
            *raw.entry(raw_depth).or_default() += 1;
            *cutoff.entry(cutoff_depth).or_default() += 1;
            println!(
                "depth stability {name}: repeat {}/{} raw={} cutoff={}",
                repeat + 1,
                STABILITY_REPEATS,
                raw_depth,
                cutoff_depth
            );
        }

        let violations = stability_contract_violations(&contract, &spec.measure, &raw, &cutoff);
        artifacts.write_scenario(name, &outcome, &violations);
        if !violations.is_empty() {
            failures.push((name.clone(), violations));
        }
        markdown.push_str(&format!("## `{name}`\n\n"));
        markdown.push_str(&format!("- raw depth distribution: `{raw:?}`\n"));
        markdown.push_str(&format!("- response-cutoff distribution: `{cutoff:?}`\n"));
        if raw.len() == 1 {
            markdown.push_str("- result: exact depth is stable without cutoff\n\n");
        } else if cutoff.len() == 1 {
            markdown.push_str(
                "- result: exact depth is stable after excluding post-response starts\n\n",
            );
        } else {
            let observed_max = cutoff
                .keys()
                .next_back()
                .copied()
                .expect("cutoff distribution is non-empty");
            markdown.push_str(&format!(
                "- result: max mode required; observed response-cutoff maximum `{observed_max}`\n\n"
            ));
        }
        markdown.push_str(&format!(
            "- checked-in contract: `{}`\n\n",
            if failures.iter().any(|(failed, _)| failed == name) {
                "violated"
            } else {
                "satisfied"
            }
        ));
    }

    artifacts.write_depth_stability(&markdown);
    let report = artifacts.write_report();
    println!(
        "depth stability findings: {}/depth-stability.md",
        artifacts.root().display()
    );
    println!("depth stability report: {}", report.display());
    assert!(
        failures.is_empty(),
        "depth stability observations violate checked-in contracts: {failures:#?}"
    );
}

async fn stability_outcome(spec: &ScenarioSpec) -> ScenarioOutcome {
    match (&spec.cache_state, &spec.measure) {
        (CacheState::Cold | CacheState::WarmHydrated, _)
        | (_, MeasureOp::Compact { .. } | MeasureOp::Gc | MeasureOp::Hydrate { .. }) => {
            let worlds = (0..STABILITY_REPEATS).map(|_| {
                let mut sample = spec.clone();
                sample.repeats = 1;
                sample
            });
            run_stability_worlds(worlds).await
        }
        (
            CacheState::Warm { .. },
            MeasureOp::Query { .. }
            | MeasureOp::FtsQuery { .. }
            | MeasureOp::HybridQuery { .. }
            | MeasureOp::AsOfQuery { .. }
            | MeasureOp::QueryPages { .. }
            | MeasureOp::Fetch { .. },
        ) => {
            let mut sample = spec.clone();
            sample.repeats = STABILITY_REPEATS;
            run_scenario(&sample, None).await
        }
        (CacheState::Warm { .. }, MeasureOp::Upsert { .. } | MeasureOp::Delete { .. }) => {
            let per_world = spec
                .server_config
                .max_wal_fragments_before_compact
                .checked_sub(2)
                .expect("write stability requires room for prime plus one measured fragment");
            assert!(per_world > 0, "write stability world cannot fit one repeat");
            let mut remaining = STABILITY_REPEATS;
            let mut worlds = Vec::new();
            while remaining > 0 {
                let repeats = remaining.min(per_world);
                let mut sample = spec.clone();
                sample.repeats = repeats;
                worlds.push(sample);
                remaining -= repeats;
            }
            run_stability_worlds(worlds).await
        }
    }
}

async fn run_stability_worlds(worlds: impl IntoIterator<Item = ScenarioSpec>) -> ScenarioOutcome {
    let mut expected = None;
    let mut per_repeat = Vec::new();
    for (world_index, world) in worlds.into_iter().enumerate() {
        let outcome = run_scenario(&world, None).await;
        match &expected {
            Some(previous) => assert_eq!(
                previous, &outcome.expected,
                "stability world changed closed-form dataset expectations"
            ),
            None => expected = Some(outcome.expected.clone()),
        }
        per_repeat.extend(outcome.per_repeat);
        println!(
            "depth stability {}: completed world {} with {} total repeats",
            world.name,
            world_index + 1,
            per_repeat.len()
        );
    }
    ScenarioOutcome {
        per_repeat,
        expected: expected.expect("stability study produced no worlds"),
    }
}

pub(crate) async fn run_scenario(
    spec: &ScenarioSpec,
    injection: Option<Injection>,
) -> ScenarioOutcome {
    run_scenario_inner(spec, injection, None, None, None, false).await
}

/// Run a serial scenario with a test-only store decorator outside depth and
/// counting instrumentation. Cold scenarios boot a fresh server per repeat.
pub(crate) async fn run_scenario_decorated(
    spec: &ScenarioSpec,
    decorator: &dyn Fn(&ZeppelinStore) -> ZeppelinStore,
    before_measure: &dyn Fn(),
    after_measure: &dyn Fn(),
) -> ScenarioOutcome {
    run_scenario_inner(
        spec,
        None,
        Some(decorator),
        Some(before_measure),
        Some(after_measure),
        true,
    )
    .await
}

async fn run_scenario_inner(
    spec: &ScenarioSpec,
    injection: Option<Injection>,
    decorator: Option<&dyn Fn(&ZeppelinStore) -> ZeppelinStore>,
    before_measure: Option<&dyn Fn()>,
    after_measure: Option<&dyn Fn()>,
    repeat_cold: bool,
) -> ScenarioOutcome {
    assert!(
        spec.repeats > 0,
        "scenario repeats must be greater than zero"
    );
    assert!(
        matches!(spec.server_config.manifest_cache_ttl_ms, 0 | 3_600_000),
        "manifest cache TTL must be pinned to 0 or 3600000"
    );
    assert!(
        !matches!(&spec.cache_state, CacheState::WarmHydrated)
            || matches!(&spec.setup, SetupPlan::Hydration),
        "WarmHydrated cache state requires the hydration setup"
    );

    let mut generated = generate(spec.dataset.clone());
    for probes in &mut generated.expected.probe_clusters {
        probes.truncate(spec.server_config.nprobe);
    }

    let harness = TestHarness::new().await;
    let config = scenario_config(spec);
    let clock = stable_width_perf_clock();
    let (depth_wrapped, tracker) = depth_store(&harness.store);
    let (instrumented_store, counter) = perf_counting_store(&depth_wrapped);
    let decorated_store = decorator.map_or_else(
        || instrumented_store.clone(),
        |decorate| decorate(&instrumented_store),
    );
    let server_store = injection.map_or_else(
        || decorated_store.clone(),
        |selected| inject_store(&decorated_store, selected),
    );
    let setup_server = start_test_server_full(
        server_store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        Some(clock.clone()),
    )
    .await;
    let client = Client::new();
    let namespace = api_ns(&harness, &spec.name);
    let mut cleanup_guard =
        NamespaceCleanupGuard::new(instrumented_store.clone(), namespace.clone());
    create_namespace(&client, &setup_server, &namespace, spec).await;
    let mut world = setup_world(&client, &setup_server, &namespace, &generated, spec).await;

    let mut cold_server = None;
    match &spec.cache_state {
        CacheState::Cold => {
            let setup_queries = zeppelin::metrics::QUERIES_TOTAL
                .with_label_values(&[&namespace])
                .get();
            assert_eq!(
                setup_queries, 0,
                "cold scenario setup issued a query before fresh-server boot"
            );
            cold_server = Some(
                start_test_server_full(
                    server_store.clone(),
                    Some(harness.prefix.clone()),
                    config.clone(),
                    false,
                    Some(clock.clone()),
                )
                .await,
            );
        }
        CacheState::Warm { prime } => {
            for step in prime {
                match step {
                    Step::Measure => {
                        execute_measure_operation(
                            &client,
                            &setup_server,
                            &namespace,
                            &generated,
                            &spec.measure,
                            &world,
                            world.measure_offset,
                            &tracker,
                        )
                        .await;
                        await_tracker_idle(&tracker).await;
                        if measure_mutates(&spec.measure) {
                            world.measure_offset += 1;
                        }
                    }
                    Step::Query => {
                        prime_query(&client, &setup_server, &namespace, &generated).await;
                        await_tracker_idle(&tracker).await;
                    }
                }
            }
        }
        CacheState::WarmHydrated => {
            cold_server = Some(
                start_test_server_full(
                    server_store.clone(),
                    Some(harness.prefix.clone()),
                    config.clone(),
                    false,
                    Some(clock.clone()),
                )
                .await,
            );
        }
    }
    post_prime_setup(
        &client,
        &setup_server,
        &namespace,
        &generated,
        spec,
        &mut world,
    )
    .await;

    let mut measured_namespaces = vec![namespace.clone()];
    if repeat_cold && matches!(&spec.cache_state, CacheState::Cold) {
        let source_manifest = live_manifest(&setup_server, &namespace).await;
        for repeat in 1..spec.repeats {
            let target = cold_namespace_name(&namespace, &spec.name, repeat);
            clone_namespace_for_cold(&setup_server, &namespace, &target, &source_manifest).await;
            cleanup_guard.add(target.clone());
            measured_namespaces.push(target);
        }
    }

    if let Some(reset) = before_measure {
        reset();
    }
    counter.reset();
    tracker.reset();
    let repeat_count = if matches!(&spec.cache_state, CacheState::Cold) && !repeat_cold {
        1
    } else {
        spec.repeats
    };
    let mut per_repeat = Vec::with_capacity(repeat_count);
    for repeat in 0..repeat_count {
        let fresh_server =
            if repeat_cold && repeat > 0 && matches!(&spec.cache_state, CacheState::Cold) {
                Some(
                    start_test_server_full(
                        server_store.clone(),
                        Some(harness.prefix.clone()),
                        config.clone(),
                        false,
                        Some(clock.clone()),
                    )
                    .await,
                )
            } else {
                None
            };
        let measured_server = fresh_server
            .as_ref()
            .or(cold_server.as_ref())
            .unwrap_or(&setup_server);
        let measured_namespace = measured_namespaces.get(repeat).unwrap_or(&namespace);
        if !world.eventual_wal_keys.is_empty() {
            // This frozen scenario explicitly invalidates the immutable WAL
            // byte entries before every repeat. Keep the new decoded tier on
            // the same repeat boundary so it cannot change the contracted S3
            // pre-state or leak observer state from a prior measurement.
            measured_server.clear_wal_fragment_cache();
        }
        if !world.repeat_cache_keys.is_empty() {
            // Frozen contracts that invalidate selected immutable byte-cache
            // entries define every repeat against that cold byte pre-state.
            // Mirror the same boundary in the decoded FTS tier so priming or a
            // prior repeat cannot hide a contracted object-store GET. The
            // ignored ideal catalog remains free to measure true warm reuse.
            measured_server.clear_decoded_artifact_cache();
        }
        for key in world
            .eventual_wal_keys
            .iter()
            .chain(&world.repeat_cache_keys)
        {
            measured_server
                .cache
                .invalidate(key)
                .await
                .unwrap_or_else(|error| {
                    panic!("failed to invalidate measured cache key {key}: {error}")
                });
        }
        if measure_mutates(&spec.measure) {
            // The checked-in write contracts are frozen cold writer rounds:
            // each repeat historically starts with one authoritative manifest
            // GET. Make that pre-state explicit now that production retains a
            // disposable post-CAS memo. The ignored ideal-analysis catalog owns
            // the separate warm 0-GET group-commit scenario.
            measured_server.reset_wal_writer_state(measured_namespace);
        }
        let started = Instant::now();
        let mut measured = execute_measure_once(
            &client,
            measured_server,
            measured_namespace,
            &generated,
            spec,
            &world,
            world.measure_offset + repeat,
            &counter,
            &tracker,
        )
        .await;
        measured.wall_elapsed_us = started.elapsed().as_micros() as u64;
        per_repeat.push(measured);
        counter.reset();
        tracker.reset();
    }

    if let Some(snapshot) = after_measure {
        snapshot();
    }

    let final_server = cold_server.as_ref().unwrap_or(&setup_server);
    if matches!(&spec.measure, MeasureOp::Compact { .. }) {
        verify_cluster_balance(final_server, &namespace, &generated.expected, spec).await;
    }

    assert_fragment_window(final_server, &namespace, spec).await;
    cleanup_guard.cleanup().await;
    harness.cleanup().await;

    ScenarioOutcome {
        per_repeat,
        expected: generated.expected,
    }
}

/// Run query-like traffic through one real server with multiple closed-loop
/// clients. This path records wall timing only; aggregate depth has no
/// single-request interpretation while requests overlap.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_closed_loop_scenario(
    spec: &ScenarioSpec,
    clients: usize,
    request_budget: usize,
    decorator: &dyn Fn(&ZeppelinStore) -> ZeppelinStore,
    before_measure: &dyn Fn(),
    after_measure: &dyn Fn(),
) -> ClosedLoopOutcome {
    assert!(clients > 0, "closed-loop clients must be nonzero");
    assert!(
        request_budget >= clients,
        "closed-loop request budget must cover every client"
    );
    assert!(
        matches!(
            &spec.measure,
            MeasureOp::Query { .. }
                | MeasureOp::FtsQuery { .. }
                | MeasureOp::HybridQuery { .. }
                | MeasureOp::AsOfQuery { .. }
                | MeasureOp::Fetch { .. }
        ),
        "closed-loop validation only supports query-like scenarios"
    );

    let mut generated = generate(spec.dataset.clone());
    for probes in &mut generated.expected.probe_clusters {
        probes.truncate(spec.server_config.nprobe);
    }

    let harness = TestHarness::new().await;
    let mut config = scenario_config(spec);
    let clock = stable_width_perf_clock();
    // Tier 1 pins this to one request. Tier 3 intentionally admits the
    // profile's closed-loop clients; the production middleware load-sheds
    // instead of queueing when this semaphore is exhausted.
    config.server.max_concurrent_queries = clients;
    let (depth_wrapped, tracker) = depth_store(&harness.store);
    let (instrumented_store, counter) = perf_counting_store(&depth_wrapped);
    let decorated_store = decorator(&instrumented_store);
    let setup_server = start_test_server_full(
        decorated_store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        Some(clock.clone()),
    )
    .await;
    let client = Client::new();
    let namespace = api_ns(&harness, &spec.name);
    let cleanup_guard = NamespaceCleanupGuard::new(instrumented_store.clone(), namespace.clone());
    create_namespace(&client, &setup_server, &namespace, spec).await;
    let mut world = setup_world(&client, &setup_server, &namespace, &generated, spec).await;

    let mut cold_server = None;
    match &spec.cache_state {
        CacheState::Cold => {
            let setup_queries = zeppelin::metrics::QUERIES_TOTAL
                .with_label_values(&[&namespace])
                .get();
            assert_eq!(
                setup_queries, 0,
                "cold closed-loop setup issued a query before fresh-server boot"
            );
            cold_server = Some(
                start_test_server_full(
                    decorated_store.clone(),
                    Some(harness.prefix.clone()),
                    config,
                    false,
                    Some(clock),
                )
                .await,
            );
        }
        CacheState::Warm { prime } => {
            for step in prime {
                match step {
                    Step::Measure => {
                        execute_measure_operation(
                            &client,
                            &setup_server,
                            &namespace,
                            &generated,
                            &spec.measure,
                            &world,
                            world.measure_offset,
                            &tracker,
                        )
                        .await;
                        await_tracker_idle(&tracker).await;
                        assert!(
                            !measure_mutates(&spec.measure),
                            "closed-loop prime unexpectedly mutates state"
                        );
                    }
                    Step::Query => {
                        prime_query(&client, &setup_server, &namespace, &generated).await;
                        await_tracker_idle(&tracker).await;
                    }
                }
            }
        }
        CacheState::WarmHydrated => {
            panic!("closed-loop latency validation does not support hydration")
        }
    }
    post_prime_setup(
        &client,
        &setup_server,
        &namespace,
        &generated,
        spec,
        &mut world,
    )
    .await;
    let measured_server = cold_server.as_ref().unwrap_or(&setup_server);

    before_measure();
    counter.reset();
    tracker.reset();
    let next = AtomicUsize::new(0);
    let started = Instant::now();
    let workers = futures::future::join_all((0..clients).map(|_| {
        let client = client.clone();
        let next = &next;
        let server = measured_server;
        let namespace = &namespace;
        let generated = &generated;
        let measure = &spec.measure;
        let world = &world;
        let tracker = &tracker;
        async move {
            let mut timings = Vec::new();
            loop {
                let request_index = next.fetch_add(1, Ordering::SeqCst);
                if request_index >= request_budget {
                    break;
                }
                let request_started = Instant::now();
                let _ = execute_measure_operation(
                    &client,
                    server,
                    namespace,
                    generated,
                    measure,
                    world,
                    request_index,
                    tracker,
                )
                .await;
                timings.push(request_started.elapsed().as_micros() as u64);
            }
            timings
        }
    }))
    .await;
    let elapsed_us = started.elapsed().as_micros() as u64;
    await_tracker_idle(&tracker).await;
    let request_wall_us = workers.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(
        request_wall_us.len(),
        request_budget,
        "closed-loop workers did not exhaust the fixed request budget"
    );
    after_measure();

    assert_fragment_window(measured_server, &namespace, spec).await;
    cleanup_guard.cleanup().await;
    harness.cleanup().await;
    ClosedLoopOutcome {
        request_wall_us,
        elapsed_us,
    }
}

async fn setup_world(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    generated: &GeneratedDataset,
    spec: &ScenarioSpec,
) -> WorldState {
    match &spec.setup {
        SetupPlan::CompactionFull { fragments } => {
            upsert_dataset_in_fragments(client, server, namespace, generated, *fragments).await;
            assert_manifest_fragment_count(server, namespace, *fragments).await;
            WorldState::default()
        }
        SetupPlan::Compacted
        | SetupPlan::EventualDeletes { .. }
        | SetupPlan::AsOfRetainedGeneration
        | SetupPlan::CompactionIncremental { .. }
        | SetupPlan::GcNothingEligible
        | SetupPlan::Hydration => {
            upsert_dataset(client, server, namespace, generated).await;
            compact_until_ready(client, server, namespace, spec).await;
            verify_cluster_balance(server, namespace, &generated.expected, spec).await;

            let mut world = WorldState::default();
            match &spec.setup {
                SetupPlan::AsOfRetainedGeneration => {
                    let manifest = live_manifest(server, namespace).await;
                    world.as_of_generation = Some(manifest.version().to_string());
                    let vector = GenVector {
                        id: "perf-as-of-live-advance".to_string(),
                        values: generated.queries[0].vector.clone(),
                        attributes: generated.vectors[0].attributes.clone(),
                    };
                    upsert_rows(client, server, namespace, &[vector]).await;
                }
                SetupPlan::CompactionIncremental { fragments } => {
                    write_incremental_fragments(client, server, namespace, generated, *fragments)
                        .await;
                    assert_manifest_fragment_count(server, namespace, *fragments).await;
                }
                SetupPlan::Hydration => {
                    let manifest = live_manifest(server, namespace).await;
                    let segment = active_segment(&manifest, namespace);
                    world.hydration_keys = hydration_keys(namespace, segment);
                    assert!(
                        !world.hydration_keys.is_empty(),
                        "hydration setup produced no cacheable artifacts"
                    );
                }
                SetupPlan::Compacted
                | SetupPlan::EventualDeletes { .. }
                | SetupPlan::GcNothingEligible => {}
                SetupPlan::CompactionFull { .. } => unreachable!(),
            }
            let manifest = live_manifest(server, namespace).await;
            let segment = active_segment(&manifest, namespace);
            world.repeat_cache_keys = measured_sidecar_cache_keys(namespace, segment, spec);
            world
        }
    }
}

async fn post_prime_setup(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    generated: &GeneratedDataset,
    spec: &ScenarioSpec,
    world: &mut WorldState,
) {
    if let SetupPlan::EventualDeletes { fragments } = &spec.setup {
        for index in 0..*fragments {
            delete_rows(
                client,
                server,
                namespace,
                &[generated.vectors[index].id.clone()],
            )
            .await;
        }
        assert_manifest_fragment_count(server, namespace, *fragments).await;
        let manifest = live_manifest(server, namespace).await;
        world.eventual_wal_keys = manifest
            .fragments
            .iter()
            .filter(|fragment| fragment.delete_count > 0)
            .map(|fragment| format!("wal_fragments/{}.wal", fragment.id))
            .collect();
        assert_eq!(
            world.eventual_wal_keys.len(),
            *fragments,
            "eventual setup did not publish exactly the requested tombstone fragments"
        );
        world.measure_offset = *fragments;
    }
}

fn load_or_panic(name: &str) -> ContractSpec {
    load_contract(name).unwrap_or_else(|error| panic!("{error}"))
}

async fn create_namespace(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    spec: &ScenarioSpec,
) {
    assert!(
        !namespace.contains('/'),
        "API namespace must be one URL path segment: {namespace}"
    );
    let mut body = spec.ns_config.clone();
    let object = body
        .as_object_mut()
        .expect("scenario namespace config must be a JSON object");
    object.insert("name".to_string(), json!(namespace));
    assert_eq!(
        object.get("dimensions").and_then(Value::as_u64),
        Some(spec.dataset.dims as u64),
        "scenario namespace dimensions must match the dataset"
    );

    let response = client
        .post(format!("{}/v1/namespaces", server.base_url))
        .json(&body)
        .send()
        .await
        .unwrap_or_else(|error| panic!("create namespace request failed: {error}"));
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("create namespace response read failed: {error}"));
    assert_eq!(
        status.as_u16(),
        201,
        "create namespace failed: {}",
        String::from_utf8_lossy(&bytes)
    );
    let response_body: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("create namespace returned invalid JSON: {error}"));
    assert_eq!(
        response_body["name"].as_str(),
        Some(namespace),
        "create namespace returned a different name"
    );
}

async fn clone_namespace_for_cold(
    server: &FullTestServer,
    source: &str,
    target: &str,
    source_manifest: &Manifest,
) {
    let source_meta_key = NamespaceMetadata::s3_key(source);
    let mut metadata = NamespaceMetadata::from_bytes(
        &server
            .store
            .get(&source_meta_key)
            .await
            .unwrap_or_else(|error| panic!("failed to read cold source metadata: {error}")),
    )
    .unwrap_or_else(|error| panic!("failed to decode cold source metadata: {error}"));
    metadata.name = target.to_string();
    let target_meta_key = NamespaceMetadata::s3_key(target);
    server
        .store
        .put(
            &target_meta_key,
            metadata
                .to_bytes()
                .unwrap_or_else(|error| panic!("failed to encode cold target metadata: {error}")),
        )
        .await
        .unwrap_or_else(|error| panic!("failed to write cold target metadata: {error}"));

    let source_prefix = format!("{source}/");
    let immutable_prefix = format!("{source}/segments/");
    let immutable_keys = server
        .store
        .list_prefix(&immutable_prefix)
        .await
        .unwrap_or_else(|error| panic!("failed to list cold clone artifacts: {error}"));
    assert!(
        !immutable_keys.is_empty(),
        "cold clone source has no immutable segment artifacts"
    );
    for from in immutable_keys {
        let suffix = from.strip_prefix(&source_prefix).unwrap_or_else(|| {
            panic!("cold clone source key {from:?} escaped prefix {source_prefix:?}")
        });
        let to = format!("{target}/{suffix}");
        let from_path = ObjectPath::parse(&from)
            .unwrap_or_else(|error| panic!("invalid cold clone source key {from}: {error}"));
        let to_path = ObjectPath::parse(&to)
            .unwrap_or_else(|error| panic!("invalid cold clone target key {to}: {error}"));
        server
            .store
            .inner()
            .copy(&from_path, &to_path)
            .await
            .unwrap_or_else(|error| panic!("cold clone copy {from} -> {to} failed: {error}"));
    }

    let mut manifest = source_manifest.clone();
    manifest.fencing_token = 0;
    for key in &mut manifest.pending_deletes {
        *key = rewrite_cold_key(source, target, key);
    }
    for segment in &mut manifest.segments {
        if let Some(sketch) = &mut segment.sketch {
            sketch.key = rewrite_cold_key(source, target, &sketch.key);
        }
        if let Some(bootstrap) = &mut segment.bootstrap {
            bootstrap.key = rewrite_cold_key(source, target, &bootstrap.key);
        }
        if let Some(membership) = &mut segment.membership {
            membership.key = rewrite_cold_key(source, target, &membership.key);
        }
        for object in &mut segment.cluster_objects {
            object.key = rewrite_cold_key(source, target, &object.key);
        }
    }
    reset_cold_clone_manifest_version(&mut manifest);
    manifest
        .write(&server.store, target)
        .await
        .unwrap_or_else(|error| panic!("failed to publish cold clone manifest: {error}"));
    server.manifest_cache.invalidate(target);
}

fn reset_cold_clone_manifest_version(manifest: &mut Manifest) {
    assert!(
        manifest.version() <= MSGPACK_POSITIVE_FIXINT_MAX,
        "cold clone byte stability requires source manifest version <= \
         {MSGPACK_POSITIVE_FIXINT_MAX}; generation {} uses a wider MessagePack \
         integer than the reset clone",
        manifest.version()
    );
    manifest.reset_version_for_clone();
}

fn rewrite_cold_key(source: &str, target: &str, key: &str) -> String {
    let prefix = format!("{source}/");
    let suffix = key
        .strip_prefix(&prefix)
        .unwrap_or_else(|| panic!("cold clone embedded key {key:?} escaped prefix {prefix:?}"));
    format!("{target}/{suffix}")
}

fn cold_namespace_name(source: &str, scenario: &str, repeat: usize) -> String {
    assert!(
        (1..=999).contains(&repeat),
        "cold namespace repeat must fit three digits"
    );
    let suffix = format!("-{scenario}");
    let prefix = source
        .strip_suffix(&suffix)
        .unwrap_or_else(|| panic!("cold namespace {source:?} lacks scenario suffix {suffix:?}"));
    assert!(
        prefix.len() >= 3,
        "cold namespace prefix is too short to encode a repeat"
    );
    let split = prefix.len() - 3;
    let target = format!("{}{:03}{suffix}", &prefix[..split], repeat);
    assert_eq!(
        target.len(),
        source.len(),
        "cold namespace alias changed serialized key length"
    );
    target
}

async fn upsert_dataset(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    generated: &GeneratedDataset,
) {
    for batch in generated.vectors.chunks(SETUP_BATCH_SIZE) {
        upsert_rows(client, server, namespace, batch).await;
    }
}

async fn upsert_dataset_in_fragments(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    generated: &GeneratedDataset,
    fragments: usize,
) {
    assert!(fragments > 0, "setup fragment count must be nonzero");
    assert_eq!(
        generated.vectors.len() % fragments,
        0,
        "dataset rows must divide evenly across setup fragments"
    );
    let rows_per_fragment = generated.vectors.len() / fragments;
    for rows in generated.vectors.chunks(rows_per_fragment) {
        upsert_rows(client, server, namespace, rows).await;
    }
}

async fn write_incremental_fragments(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    generated: &GeneratedDataset,
    fragments: usize,
) {
    assert!(
        fragments <= generated.vectors.len(),
        "incremental fragment count exceeds generated rows"
    );
    for vector in generated.vectors.iter().take(fragments) {
        upsert_rows(client, server, namespace, std::slice::from_ref(vector)).await;
    }
}

async fn upsert_rows(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    vectors: &[GenVector],
) {
    assert!(!vectors.is_empty(), "upsert batch cannot be empty");
    let response = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap_or_else(|error| panic!("upsert request failed for {namespace}: {error}"));
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("upsert response read failed for {namespace}: {error}"));
    assert_eq!(
        status.as_u16(),
        200,
        "upsert failed for {namespace}: {}",
        String::from_utf8_lossy(&bytes)
    );
    let body: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("upsert returned invalid JSON for {namespace}: {error}"));
    assert_eq!(
        body["upserted"].as_u64(),
        Some(vectors.len() as u64),
        "upsert response count mismatch for {namespace}"
    );
}

async fn delete_rows(client: &Client, server: &FullTestServer, namespace: &str, ids: &[String]) {
    assert!(!ids.is_empty(), "delete batch cannot be empty");
    let response = client
        .delete(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({ "ids": ids }))
        .send()
        .await
        .unwrap_or_else(|error| panic!("delete request failed for {namespace}: {error}"));
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("delete response read failed for {namespace}: {error}"));
    assert_eq!(
        status.as_u16(),
        204,
        "delete failed for {namespace}: {}",
        String::from_utf8_lossy(&bytes)
    );
    assert!(
        bytes.is_empty(),
        "204 delete response unexpectedly had a body"
    );
}

async fn assert_manifest_fragment_count(server: &FullTestServer, namespace: &str, expected: usize) {
    let manifest = live_manifest(server, namespace).await;
    assert_eq!(
        manifest.fragments.len(),
        expected,
        "manifest fragment count mismatch for {namespace}"
    );
}

async fn live_manifest(server: &FullTestServer, namespace: &str) -> Manifest {
    Manifest::read(&server.store, namespace)
        .await
        .unwrap_or_else(|error| panic!("failed to read manifest for {namespace}: {error}"))
        .unwrap_or_else(|| panic!("manifest disappeared for {namespace}"))
}

fn active_segment<'a>(manifest: &'a Manifest, namespace: &str) -> &'a SegmentRef {
    let active_id = manifest
        .active_segment
        .as_deref()
        .unwrap_or_else(|| panic!("manifest has no active segment for {namespace}"));
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == active_id)
        .unwrap_or_else(|| panic!("active segment {active_id} missing for {namespace}"))
}

fn hydration_keys(namespace: &str, segment: &SegmentRef) -> Vec<String> {
    let mut keys = segment
        .cluster_objects
        .iter()
        .map(|object| object.key.clone())
        .collect::<Vec<_>>();
    for cluster in 0..segment.cluster_count {
        let owner = segment.cluster_owner(cluster);
        keys.push(attrs_key(namespace, owner, cluster));
        if !segment.bitmap_fields.is_empty() {
            keys.push(bitmap_key(namespace, owner, cluster));
        }
    }
    if segment.has_global_fts {
        keys.push(global_fts_key(namespace, &segment.id));
    }
    keys.sort();
    keys.dedup();
    keys
}

fn measured_sidecar_cache_keys(
    namespace: &str,
    segment: &SegmentRef,
    spec: &ScenarioSpec,
) -> Vec<String> {
    let mut keys = Vec::new();
    match &spec.measure {
        MeasureOp::Query {
            filter: Some(_), ..
        } => {
            for cluster in 0..segment.cluster_count {
                let owner = segment.cluster_owner(cluster);
                if spec.server_config.bitmap_index {
                    keys.push(bitmap_key(namespace, owner, cluster));
                } else {
                    keys.push(attrs_key(namespace, owner, cluster));
                }
            }
        }
        MeasureOp::FtsQuery { .. } if segment.has_global_fts => {
            keys.push(global_fts_key(namespace, &segment.id));
        }
        MeasureOp::HybridQuery { .. } if segment.has_global_fts => {
            keys.extend(
                segment
                    .cluster_objects
                    .iter()
                    .map(|object| object.key.clone()),
            );
            keys.push(global_fts_key(namespace, &segment.id));
        }
        _ => {}
    }
    keys
}

async fn compact_until_ready(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    spec: &ScenarioSpec,
) {
    let mut last_status = Value::Null;
    for _ in 0..COMPACTION_ATTEMPTS {
        last_status = compaction_status(client, server, namespace).await;
        let ready = last_status["ready"].as_bool().unwrap_or_else(|| {
            panic!("compact/status missing boolean ready for {namespace}: {last_status}")
        });
        let fragments = last_status["uncompacted_fragments"]
            .as_u64()
            .unwrap_or_else(|| {
                panic!(
                    "compact/status missing uncompacted_fragments for {namespace}: {last_status}"
                )
            });
        if ready && fragments == 0 {
            return;
        }

        let result = if spec.server_config.fts_index {
            let configs = std::collections::HashMap::from([(
                "content".to_string(),
                zeppelin::fts::FtsFieldConfig {
                    stemming: true,
                    remove_stopwords: true,
                    ..Default::default()
                },
            )]);
            server
                .compactor
                .compact_with_fts(namespace, None, &configs)
                .await
        } else {
            server.compactor.compact(namespace).await
        }
        .unwrap_or_else(|error| panic!("inline compaction failed for {namespace}: {error}"));
        assert!(
            result.segment_id.is_some(),
            "inline compaction with pending fragments produced no segment for {namespace}"
        );
        server.manifest_cache.invalidate(namespace);
    }
    panic!(
        "compaction did not reach ready with zero fragments for {namespace} after {COMPACTION_ATTEMPTS} attempts; last status: {last_status}"
    );
}

async fn compaction_status(client: &Client, server: &FullTestServer, namespace: &str) -> Value {
    let response = client
        .get(format!(
            "{}/v1/namespaces/{namespace}/compact/status",
            server.base_url
        ))
        .send()
        .await
        .unwrap_or_else(|error| panic!("compact/status request failed for {namespace}: {error}"));
    let status = response.status();
    let bytes = response.bytes().await.unwrap_or_else(|error| {
        panic!("compact/status response read failed for {namespace}: {error}")
    });
    assert_eq!(
        status.as_u16(),
        200,
        "compact/status failed for {namespace}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("compact/status returned invalid JSON: {error}"))
}

async fn verify_cluster_balance(
    server: &FullTestServer,
    namespace: &str,
    expected: &DatasetExpectations,
    spec: &ScenarioSpec,
) {
    let manifest = Manifest::read(&server.store, namespace)
        .await
        .unwrap_or_else(|error| panic!("failed to read manifest for {namespace}: {error}"))
        .unwrap_or_else(|| panic!("manifest disappeared for {namespace}"));
    assert!(
        manifest.fragments.is_empty(),
        "manifest still contains uncompacted fragments for {namespace}"
    );
    let active_id = manifest
        .active_segment
        .as_deref()
        .unwrap_or_else(|| panic!("manifest has no active segment for {namespace}"));
    let segment = manifest
        .segments
        .iter()
        .find(|segment| segment.id == active_id)
        .unwrap_or_else(|| panic!("active segment {active_id} missing for {namespace}"));
    assert_eq!(
        segment.cluster_count, spec.dataset.nlist,
        "k-means did not produce the requested cluster count for {namespace}"
    );
    assert_eq!(
        segment.vector_count, spec.dataset.vectors,
        "active segment vector count mismatch for {namespace}"
    );
    let membership_ref = segment.membership.as_ref().unwrap_or_else(|| {
        panic!("active segment {active_id} has no membership artifact for {namespace}")
    });
    let bytes = server
        .store
        .get(&membership_ref.key)
        .await
        .unwrap_or_else(|error| panic!("failed to read membership for {namespace}: {error}"));
    assert_eq!(
        bytes.len() as u64,
        membership_ref.size_bytes,
        "membership byte size disagrees with manifest for {namespace}"
    );
    let membership = deserialize_membership(&bytes)
        .unwrap_or_else(|error| panic!("invalid membership for {namespace}: {error}"));
    assert_eq!(
        membership.cluster_count as usize, segment.cluster_count,
        "membership cluster count disagrees with manifest for {namespace}"
    );
    assert_eq!(
        membership.entries.len() as u64,
        membership_ref.entry_count,
        "membership entry count disagrees with manifest for {namespace}"
    );
    let mut rows = vec![0usize; segment.cluster_count];
    for (_, cluster) in membership.entries {
        rows[cluster as usize] += 1;
    }
    assert!(
        rows.iter().all(|rows| *rows == expected.rows_per_cluster),
        "seeded blobs were not recovered as equal clusters for {namespace}: expected {} rows each, got {rows:?}",
        expected.rows_per_cluster
    );
}

fn scenario_config(spec: &ScenarioSpec) -> Config {
    let mut config = Config::load(None).expect("failed to load base Config");
    config.cache.manifest_cache_ttl_ms = spec.server_config.manifest_cache_ttl_ms;
    config.cache.namespace_registry_ttl_ms = 3_600_000;
    config.cache.memory_cache_max_mb = spec.server_config.memory_cache_max_mb;
    config.cache.hydration_enabled = spec.server_config.hydration_enabled;
    config.cache.hydration_parallelism = 1;
    config.server.max_concurrent_queries = 1;
    config.indexing.default_num_centroids = spec.dataset.nlist;
    config.indexing.default_nprobe = spec.server_config.nprobe;
    config.indexing.max_nprobe = spec.dataset.nlist.max(spec.server_config.nprobe);
    config.indexing.quantization = QuantizationType::Scalar;
    config.indexing.hierarchical = false;
    config.indexing.bitmap_index = spec.server_config.bitmap_index;
    config.indexing.fts_index = spec.server_config.fts_index;
    config.query.rerank_coalesce_gap_bytes =
        Some(zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES);
    config.query.cost_latency_profile = None;
    config.compaction.max_wal_fragments_before_compact =
        spec.server_config.max_wal_fragments_before_compact;
    config
}

async fn prime_query(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    generated: &GeneratedDataset,
) {
    let body = json!({
        "vector": generated.queries[0].vector,
        "top_k": 10,
        "consistency": "strong",
        "include_attributes": false,
    });
    let _ = post_query(client, server, namespace, body, None).await;
}

fn measure_mutates(measure: &MeasureOp) -> bool {
    matches!(measure, MeasureOp::Upsert { .. } | MeasureOp::Delete { .. })
}

#[allow(clippy::too_many_arguments)]
async fn execute_measure_once(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    generated: &GeneratedDataset,
    spec: &ScenarioSpec,
    world: &WorldState,
    repeat_index: usize,
    counter: &GetCounter,
    tracker: &DepthTracker,
) -> RepeatCounters {
    match &spec.measure {
        MeasureOp::QueryPages {
            consistency,
            pages,
            page_size,
            query_index,
        } => {
            execute_query_pages(
                client,
                server,
                namespace,
                generated,
                consistency,
                *pages,
                *page_size,
                *query_index,
                counter,
                tracker,
            )
            .await
        }
        MeasureOp::Hydrate { attempts } => {
            execute_hydration(
                client,
                server,
                namespace,
                *attempts,
                &world.hydration_keys,
                counter,
                tracker,
            )
            .await
        }
        measure => {
            let cutoff_us = execute_measure_operation(
                client,
                server,
                namespace,
                generated,
                measure,
                world,
                repeat_index,
                tracker,
            )
            .await;
            await_tracker_idle(tracker).await;
            snapshot_repeat(counter, tracker, cutoff_us, Vec::new())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_measure_operation(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    generated: &GeneratedDataset,
    measure: &MeasureOp,
    world: &WorldState,
    repeat_index: usize,
    tracker: &DepthTracker,
) -> u64 {
    match measure {
        MeasureOp::Query {
            consistency,
            top_k,
            query_index,
            filter,
        } => {
            let query = generated.queries.get(*query_index).unwrap_or_else(|| {
                panic!(
                    "query_index {query_index} outside generated query count {}",
                    generated.queries.len()
                )
            });
            let mut body = json!({
                "vector": query.vector,
                "top_k": top_k,
                "consistency": consistency,
                "include_attributes": false,
            });
            if let Some((field, value)) = filter {
                body["filter"] = json!({
                    "op": "eq",
                    "field": field,
                    "value": value,
                });
            }
            let _ = post_query(client, server, namespace, body, None).await;
        }
        MeasureOp::FtsQuery {
            consistency,
            top_k,
            field,
            query,
        } => {
            let body = json!({
                "rank_by": [field, "BM25", query],
                "top_k": top_k,
                "consistency": consistency,
                "include_attributes": false,
            });
            let _ = post_query(client, server, namespace, body, None).await;
        }
        MeasureOp::HybridQuery {
            consistency,
            top_k,
            candidate_k,
            query_index,
            field,
            query,
        } => {
            let planned = generated.queries.get(*query_index).unwrap_or_else(|| {
                panic!("hybrid query_index {query_index} outside generated query count")
            });
            let body = json!({
                "sources": [
                    {"type": "ann", "vector": planned.vector},
                    {"type": "bm25", "rank_by": [field, "BM25", query]},
                ],
                "fusion": {"type": "rrf", "k": 60},
                "candidate_k": candidate_k,
                "top_k": top_k,
                "consistency": consistency,
                "projection": {"include_attributes": false},
            });
            let _ = post_query(client, server, namespace, body, None).await;
        }
        MeasureOp::AsOfQuery {
            consistency,
            top_k,
            query_index,
        } => {
            let planned = generated.queries.get(*query_index).unwrap_or_else(|| {
                panic!("as-of query_index {query_index} outside generated query count")
            });
            let generation = world
                .as_of_generation
                .as_deref()
                .expect("as-of measure missing retained generation setup");
            let body = json!({
                "vector": planned.vector,
                "top_k": top_k,
                "consistency": consistency,
                "include_attributes": false,
            });
            let _ = post_query(client, server, namespace, body, Some(generation)).await;
        }
        MeasureOp::Fetch { consistency, ids } => {
            assert!(*ids > 0, "fetch id count must be nonzero");
            assert!(
                *ids <= generated.vectors.len(),
                "fetch id count exceeds generated rows"
            );
            let requested = generated
                .vectors
                .iter()
                .take(*ids)
                .map(|vector| vector.id.as_str())
                .collect::<Vec<_>>();
            let response = client
                .post(format!(
                    "{}/v1/namespaces/{namespace}/vectors/get",
                    server.base_url
                ))
                .json(&json!({
                    "ids": requested,
                    "include_vector": false,
                    "include_attributes": false,
                    "consistency": consistency,
                }))
                .send()
                .await
                .unwrap_or_else(|error| panic!("fetch request failed for {namespace}: {error}"));
            let status = response.status();
            let body: Value = response
                .json()
                .await
                .unwrap_or_else(|error| panic!("fetch response was invalid JSON: {error}"));
            assert_eq!(status.as_u16(), 200, "fetch failed for {namespace}: {body}");
            assert_eq!(
                body["results"].as_array().map(Vec::len),
                Some(*ids),
                "fetch returned the wrong number of rows"
            );
        }
        MeasureOp::Upsert { batch } => {
            assert!(*batch > 0, "measured upsert batch must be nonzero");
            let values = &generated.queries[0].vector;
            let vectors = (0..*batch)
                .map(|index| GenVector {
                    id: if *batch == 1 {
                        format!("perf-measure-{index:08}")
                    } else {
                        format!("perf-measure-{repeat_index:04}-{index:08}")
                    },
                    values: values.clone(),
                    attributes: None,
                })
                .collect::<Vec<_>>();
            upsert_rows(client, server, namespace, &vectors).await;
        }
        MeasureOp::Delete { count } => {
            assert!(*count > 0, "measured delete count must be nonzero");
            let start = repeat_index
                .checked_mul(*count)
                .expect("measured delete index overflowed");
            assert!(
                start + count <= generated.vectors.len(),
                "measured deletes exhausted generated ids"
            );
            let ids = generated.vectors[start..start + count]
                .iter()
                .map(|vector| vector.id.clone())
                .collect::<Vec<_>>();
            delete_rows(client, server, namespace, &ids).await;
        }
        MeasureOp::Compact {
            fragments,
            incremental,
        } => {
            assert_manifest_fragment_count(server, namespace, *fragments).await;
            let result = server
                .compactor
                .compact(namespace)
                .await
                .unwrap_or_else(|error| panic!("measured compaction failed: {error}"));
            assert!(
                result.segment_id.is_some(),
                "measured compaction produced no segment"
            );
            assert!(
                world.as_of_generation.is_none(),
                "compaction world unexpectedly carried an as-of generation"
            );
            if *incremental {
                assert!(
                    matches!(
                        measure,
                        MeasureOp::Compact {
                            incremental: true,
                            ..
                        }
                    ),
                    "incremental compaction flag changed during execution"
                );
            }
            server.manifest_cache.invalidate(namespace);
        }
        MeasureOp::Gc => {
            let config = GcConfig {
                horizon_secs: 0,
                compaction_upload_window_secs: 0,
                skew_slop_secs: 0,
                allow_unsafe_short_horizon: true,
                manifest_history_keep_count: 1_024,
                pitr_retention_secs: 0,
            };
            let report = gc::run_gc_cycle(&server.store, namespace, &config)
                .await
                .unwrap_or_else(|error| panic!("measured GC cycle failed: {error}"));
            assert_eq!(
                report.objects_deleted, 0,
                "nothing-eligible GC deleted objects"
            );
            assert_eq!(
                report.pending_deletes_deleted, 0,
                "nothing-eligible GC deleted pending objects"
            );
            server.manifest_cache.invalidate(namespace);
        }
        MeasureOp::QueryPages { .. } | MeasureOp::Hydrate { .. } => {
            unreachable!("compound measures are executed by dedicated paths")
        }
    }
    tracker.elapsed_us()
}

async fn post_query(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    body: Value,
    as_of: Option<&str>,
) -> Value {
    let mut request = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&body);
    if let Some(generation) = as_of {
        request = request.query(&[("as_of", generation)]);
    }
    let response = request
        .send()
        .await
        .unwrap_or_else(|error| panic!("query request failed for {namespace}: {error}"));
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .unwrap_or_else(|error| panic!("query response was invalid JSON: {error}"));
    assert_eq!(status.as_u16(), 200, "query failed for {namespace}: {body}");
    assert!(
        body["results"].is_array(),
        "query response omitted results for {namespace}: {body}"
    );
    body
}

#[allow(clippy::too_many_arguments)]
async fn execute_query_pages(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    generated: &GeneratedDataset,
    consistency: &str,
    pages: usize,
    page_size: usize,
    query_index: usize,
    counter: &GetCounter,
    tracker: &DepthTracker,
) -> RepeatCounters {
    assert_eq!(pages, 2, "pagination contract requires exactly two pages");
    assert!(page_size > 0, "pagination page size must be nonzero");
    let query = generated.queries.get(query_index).unwrap_or_else(|| {
        panic!("pagination query_index {query_index} outside generated query count")
    });
    let mut cursor = json!({"type": "none"});
    let mut labeled = Vec::with_capacity(pages);
    for page in 0..pages {
        let body = json!({
            "sources": [{"type": "ann", "vector": query.vector}],
            "top_k": page_size,
            "cursor": cursor,
            "consistency": consistency,
            "projection": {"include_attributes": false},
        });
        let response = post_query(client, server, namespace, body, None).await;
        if page + 1 < pages {
            let token = response["next_cursor"]
                .as_str()
                .unwrap_or_else(|| panic!("page {} omitted next_cursor", page + 1));
            cursor = json!({"type": "after", "token": token});
        }
        let cutoff_us = tracker.elapsed_us();
        await_tracker_idle(tracker).await;
        let snapshot = snapshot_repeat(counter, tracker, cutoff_us, Vec::new());
        labeled.push(labeled_snapshot(format!("page_{}", page + 1), snapshot));
        counter.reset();
        tracker.reset();
    }
    combine_labeled(labeled)
}

async fn execute_hydration(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    attempts: usize,
    hydration_keys: &[String],
    counter: &GetCounter,
    tracker: &DepthTracker,
) -> RepeatCounters {
    assert_eq!(attempts, 2, "hydration contract requires two attempts");
    assert!(!hydration_keys.is_empty(), "hydration key plan is empty");
    let mut labeled = Vec::with_capacity(attempts);
    for attempt in 0..attempts {
        let response = client
            .post(format!(
                "{}/v1/namespaces/{namespace}/hydrate",
                server.base_url
            ))
            .send()
            .await
            .unwrap_or_else(|error| panic!("hydrate request failed: {error}"));
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .unwrap_or_else(|error| panic!("hydrate response was invalid JSON: {error}"));
        assert_eq!(status.as_u16(), 202, "hydrate request failed: {body}");
        await_hydration_cache(server, hydration_keys).await;
        await_tracker_idle(tracker).await;
        let cutoff_us = tracker.elapsed_us();
        let snapshot = snapshot_repeat(counter, tracker, cutoff_us, Vec::new());
        let hydrated_gets = ["cluster", "attrs", "bitmap", "fts"]
            .into_iter()
            .map(|class| snapshot.classes[class].get_ops)
            .sum::<u64>();
        if attempt == 0 {
            assert_eq!(
                hydrated_gets,
                hydration_keys.len() as u64,
                "hydrate GET count did not match the planned warm-set artifacts"
            );
        } else {
            assert_eq!(
                hydrated_gets, 0,
                "repeat hydration fetched reachable artifact data"
            );
        }
        labeled.push(labeled_snapshot_with_get_ops(
            format!("hydrate_{}", attempt + 1),
            snapshot,
            hydrated_gets,
        ));
        counter.reset();
        tracker.reset();
    }
    combine_labeled(labeled)
}

async fn await_hydration_cache(server: &FullTestServer, keys: &[String]) {
    const MAX_YIELDS: usize = 65_536;
    for _ in 0..MAX_YIELDS {
        let mut complete = true;
        for key in keys {
            if server.cache.get(key).await.is_none() {
                complete = false;
                break;
            }
        }
        if complete {
            return;
        }
        tokio::task::yield_now().await;
    }
    let missing = futures::future::join_all(keys.iter().map(|key| server.cache.get(key)))
        .await
        .into_iter()
        .zip(keys)
        .filter_map(|(value, key)| value.is_none().then_some(key.clone()))
        .collect::<Vec<_>>();
    panic!("hydration did not populate its deterministic cache plan: {missing:?}");
}

fn snapshot_repeat(
    counter: &GetCounter,
    tracker: &DepthTracker,
    cutoff_us: u64,
    labeled: Vec<LabeledCounters>,
) -> RepeatCounters {
    let classes = class_snapshot(counter);
    let totals = sum_stats(classes.values().copied());
    let spans = tracker.take_spans();
    let raw_get_path = DepthTracker::critical_path(&spans, &[SpanKind::Get, SpanKind::Head], None);
    let raw_put_get_path = DepthTracker::critical_path(
        &spans,
        &[SpanKind::Get, SpanKind::Head, SpanKind::Put],
        None,
    );
    let get_path =
        DepthTracker::critical_path(&spans, &[SpanKind::Get, SpanKind::Head], Some(cutoff_us));
    let put_get_path = DepthTracker::critical_path(
        &spans,
        &[SpanKind::Get, SpanKind::Head, SpanKind::Put],
        Some(cutoff_us),
    );
    let op_counts = operation_counts(&spans);
    RepeatCounters {
        classes,
        totals,
        get_path,
        put_get_path,
        spans,
        op_counts,
        labeled,
        wall_elapsed_us: 0,
        response_cutoff_us: cutoff_us,
        raw_get_path,
        raw_put_get_path,
    }
}

fn labeled_snapshot(label: String, snapshot: RepeatCounters) -> LabeledCounters {
    let contract_get_ops = snapshot.totals.get_ops;
    labeled_snapshot_with_get_ops(label, snapshot, contract_get_ops)
}

fn labeled_snapshot_with_get_ops(
    label: String,
    snapshot: RepeatCounters,
    contract_get_ops: u64,
) -> LabeledCounters {
    LabeledCounters {
        label,
        contract_get_ops,
        classes: snapshot.classes,
        totals: snapshot.totals,
        get_path: snapshot.get_path,
        put_get_path: snapshot.put_get_path,
        spans: snapshot.spans,
        op_counts: snapshot.op_counts,
        response_cutoff_us: snapshot.response_cutoff_us,
    }
}

fn combine_labeled(labeled: Vec<LabeledCounters>) -> RepeatCounters {
    assert!(!labeled.is_empty(), "compound measure produced no labels");
    let mut classes = class_snapshot(&GetCounter::default());
    let mut spans = Vec::new();
    let mut cutoff_us = 0;
    for part in &labeled {
        for (class, stats) in &part.classes {
            let total = classes
                .get_mut(class)
                .expect("labeled class missing from complete class map");
            total.get_ops += stats.get_ops;
            total.get_bytes += stats.get_bytes;
            total.put_ops += stats.put_ops;
            total.put_bytes += stats.put_bytes;
        }
        spans.extend(part.spans.clone());
        cutoff_us = cutoff_us.max(part.response_cutoff_us);
    }
    let totals = sum_stats(classes.values().copied());
    let raw_get_path = DepthTracker::critical_path(&spans, &[SpanKind::Get, SpanKind::Head], None);
    let raw_put_get_path = DepthTracker::critical_path(
        &spans,
        &[SpanKind::Get, SpanKind::Head, SpanKind::Put],
        None,
    );
    let get_path =
        DepthTracker::critical_path(&spans, &[SpanKind::Get, SpanKind::Head], Some(cutoff_us));
    let put_get_path = DepthTracker::critical_path(
        &spans,
        &[SpanKind::Get, SpanKind::Head, SpanKind::Put],
        Some(cutoff_us),
    );
    let op_counts = operation_counts(&spans);
    RepeatCounters {
        classes,
        totals,
        get_path,
        put_get_path,
        spans,
        op_counts,
        labeled,
        wall_elapsed_us: 0,
        response_cutoff_us: cutoff_us,
        raw_get_path,
        raw_put_get_path,
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

async fn await_tracker_idle(tracker: &DepthTracker) {
    const MAX_YIELDS: usize = 4096;
    const REQUIRED_ZERO_STREAK: usize = 8;

    let mut zero_streak = 0usize;
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
        "measured request did not reach object-store quiescence: active_operations={}",
        tracker.active_operations()
    );
}

fn class_snapshot(counter: &GetCounter) -> BTreeMap<String, ClassStats> {
    counter
        .class_breakdown()
        .into_iter()
        .map(|(class, stats)| (class.name().to_string(), stats))
        .collect()
}

fn sum_stats(stats: impl Iterator<Item = ClassStats>) -> ClassStats {
    stats.fold(ClassStats::default(), |mut total, class| {
        total.get_ops += class.get_ops;
        total.get_bytes += class.get_bytes;
        total.put_ops += class.put_ops;
        total.put_bytes += class.put_bytes;
        total
    })
}

async fn assert_fragment_window(server: &FullTestServer, namespace: &str, spec: &ScenarioSpec) {
    let manifest = Manifest::read(&server.store, namespace)
        .await
        .unwrap_or_else(|error| panic!("post-run manifest read failed for {namespace}: {error}"))
        .unwrap_or_else(|| panic!("post-run manifest missing for {namespace}"));
    assert!(
        manifest.fragments.len() < spec.server_config.max_wal_fragments_before_compact,
        "scenario {} crossed its pinned compaction threshold: fragments={}, threshold={}",
        spec.name,
        manifest.fragments.len(),
        spec.server_config.max_wal_fragments_before_compact
    );
}

fn assert_expected_injection(injection: Injection, violations: &[CostViolation]) {
    let found = violations
        .iter()
        .any(|violation| match (injection, violation) {
            (
                Injection::ExtraManifestGet,
                CostViolation::OpCount {
                    class,
                    kind: SpanKind::Get,
                    ..
                },
            ) => class == "manifest",
            (Injection::SerializeClusterGets, CostViolation::Depth { kinds, .. }) => kinds == "get",
            (
                Injection::ExtraHistoryPut,
                CostViolation::KeyCount {
                    substring,
                    kind: SpanKind::Put,
                    ..
                },
            ) => substring.contains("manifests/"),
            _ => false,
        });
    assert!(
        found,
        "selftest injection {} did not fire its required violation; got {violations:#?}",
        injection.key()
    );
}

fn relevant_depths(measure: &MeasureOp, repeat: &RepeatCounters) -> (u32, u32) {
    match measure {
        MeasureOp::Query { .. }
        | MeasureOp::FtsQuery { .. }
        | MeasureOp::HybridQuery { .. }
        | MeasureOp::AsOfQuery { .. }
        | MeasureOp::QueryPages { .. }
        | MeasureOp::Fetch { .. }
        | MeasureOp::Gc
        | MeasureOp::Hydrate { .. } => (repeat.raw_get_path.depth, repeat.get_path.depth),
        MeasureOp::Upsert { .. } | MeasureOp::Delete { .. } | MeasureOp::Compact { .. } => {
            (repeat.raw_put_get_path.depth, repeat.put_get_path.depth)
        }
    }
}

fn stability_contract_violations(
    contract: &ContractSpec,
    measure: &MeasureOp,
    raw: &BTreeMap<u32, usize>,
    cutoff: &BTreeMap<u32, usize>,
) -> Vec<CostViolation> {
    let name = match measure {
        MeasureOp::Query { .. }
        | MeasureOp::FtsQuery { .. }
        | MeasureOp::HybridQuery { .. }
        | MeasureOp::AsOfQuery { .. }
        | MeasureOp::QueryPages { .. }
        | MeasureOp::Fetch { .. }
        | MeasureOp::Gc
        | MeasureOp::Hydrate { .. } => "get",
        MeasureOp::Upsert { .. } | MeasureOp::Delete { .. } | MeasureOp::Compact { .. } => {
            "put_get"
        }
    };
    let assertion =
        contract.assertions.depth.get(name).unwrap_or_else(|| {
            panic!("contract {} omitted assert.depth.{name}", contract.scenario)
        });
    let (recommended_mode, recommended_value, cutoff_required) = if raw.len() == 1 {
        (
            DepthMode::Exact,
            *raw.keys()
                .next()
                .expect("raw depth distribution is non-empty"),
            false,
        )
    } else if cutoff.len() == 1 {
        (
            DepthMode::Exact,
            *cutoff
                .keys()
                .next()
                .expect("cutoff depth distribution is non-empty"),
            true,
        )
    } else {
        (
            DepthMode::Max,
            *cutoff
                .keys()
                .next_back()
                .expect("cutoff depth distribution is non-empty"),
            false,
        )
    };

    let mut violations = Vec::new();
    match assertion.mode {
        DepthMode::Exact
            if recommended_mode != DepthMode::Exact || assertion.value != recommended_value =>
        {
            violations.push(CostViolation::BaselineDrift {
                field: format!("assert.depth.{name}"),
                detail: format!(
                    "stability recommends mode={recommended_mode:?}, value={recommended_value}; checked contract has mode=Exact, value={}",
                    assertion.value
                ),
            });
        }
        DepthMode::Max if recommended_value > assertion.value => {
            violations.push(CostViolation::BaselineDrift {
                field: format!("assert.depth.{name}"),
                detail: format!(
                    "observed response-cutoff depth {recommended_value} exceeds checked max {}",
                    assertion.value
                ),
            });
        }
        DepthMode::Exact | DepthMode::Max => {}
    }
    if cutoff_required
        && !assertion
            .why
            .as_deref()
            .is_some_and(|why| why.to_ascii_lowercase().contains("response"))
    {
        violations.push(CostViolation::BaselineDrift {
            field: format!("assert.depth.{name}.why"),
            detail: "stable exact depth requires documenting the response-cutoff filter"
                .to_string(),
        });
    }
    if recommended_mode == DepthMode::Max
        && assertion
            .why
            .as_deref()
            .is_none_or(|why| why.trim().is_empty())
    {
        violations.push(CostViolation::BaselineDrift {
            field: format!("assert.depth.{name}.why"),
            detail: "max depth requires a named source of scheduler variation".to_string(),
        });
    }
    violations
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn cold_clone_uses_the_manifest_version_reset_api() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let mut manifest = Manifest::new();
        manifest.write(&store, "cold-clone-reset").await.unwrap();
        assert_eq!(manifest.version(), 1);

        manifest.reset_version_for_clone();

        assert_eq!(manifest.version(), 0);
    }

    #[tokio::test]
    #[should_panic(expected = "cold clone byte stability requires source manifest version <= 127")]
    async fn cold_clone_rejects_multibyte_source_generation() {
        let store = ZeppelinStore::new(Arc::new(InMemory::new()));
        let mut manifest = Manifest::new();
        for _ in 0..=MSGPACK_POSITIVE_FIXINT_MAX {
            manifest
                .write(&store, "cold-clone-multibyte")
                .await
                .unwrap();
        }
        assert_eq!(manifest.version(), MSGPACK_POSITIVE_FIXINT_MAX + 1);

        reset_cold_clone_manifest_version(&mut manifest);
    }

    #[test]
    fn approved_max_accepts_samples_below_the_bound() {
        let contract = load_or_panic("cold_query_strong");
        let measure = scenarios::build(&contract, 1).measure;
        let distribution = BTreeMap::from([(6, STABILITY_REPEATS)]);

        assert!(
            stability_contract_violations(&contract, &measure, &distribution, &distribution,)
                .is_empty()
        );
    }

    #[test]
    fn approved_max_rejects_samples_above_the_bound() {
        let contract = load_or_panic("cold_query_strong");
        let measure = scenarios::build(&contract, 1).measure;
        let distribution = BTreeMap::from([(6, STABILITY_REPEATS - 1), (8, 1)]);

        let violations =
            stability_contract_violations(&contract, &measure, &distribution, &distribution);
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            CostViolation::BaselineDrift { field, detail }
                if field == "assert.depth.get" && detail.contains("exceeds checked max 7")
        ));
    }
}
