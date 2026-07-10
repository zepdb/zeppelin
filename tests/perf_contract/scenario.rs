//! Scenario lifecycle and Phase-1 entry orchestration.

use std::collections::BTreeMap;
use std::sync::Arc;

use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::index::ivf_flat::membership::deserialize_membership;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::wal::Manifest;

use crate::common::counting::{counting_store, ClassStats, GetCounter};
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
use super::{scenarios, PerfEnv};

const SETUP_BATCH_SIZE: usize = 256;
const COMPACTION_ATTEMPTS: usize = 4;
const STABILITY_REPEATS: usize = 100;

/// Namespace options passed as a raw HTTP creation body.
pub type NsConfig = Value;

/// Contract-pinned server values that affect Phase-1 measurements.
#[derive(Debug, Clone)]
pub struct ServerKnobs {
    pub nprobe: usize,
    pub manifest_cache_ttl_ms: u64,
    pub memory_cache_max_mb: usize,
    pub max_wal_fragments_before_compact: usize,
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
}

/// The one operation measured by a scenario.
#[derive(Debug, Clone)]
pub enum MeasureOp {
    Query {
        consistency: &'static str,
        top_k: usize,
        query_index: usize,
    },
    Upsert {
        batch: usize,
    },
}

/// Complete execution specification for one isolated scenario world.
#[derive(Debug, Clone)]
pub struct ScenarioSpec {
    pub name: String,
    pub dataset: DatasetSpec,
    pub ns_config: NsConfig,
    pub server_config: ServerKnobs,
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
    #[serde(skip)]
    pub response_cutoff_us: u64,
    /// Unfiltered paths are diagnostic inputs for the stability study only.
    #[serde(skip)]
    pub raw_get_path: CriticalPath,
    #[serde(skip)]
    pub raw_put_get_path: CriticalPath,
}

struct NamespaceCleanupGuard {
    store: ZeppelinStore,
    namespace: Arc<str>,
    armed: bool,
}

impl NamespaceCleanupGuard {
    fn new(store: ZeppelinStore, namespace: String) -> Self {
        Self {
            store,
            namespace: Arc::from(namespace),
            armed: true,
        }
    }

    async fn cleanup(mut self) {
        let prefix = format!("{}/", self.namespace);
        self.store
            .delete_prefix(&prefix)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "failed to clean performance namespace {}: {error}",
                    self.namespace
                )
            });
        let remaining = self
            .store
            .list_prefix(&prefix)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "failed to verify performance namespace cleanup {}: {error}",
                    self.namespace
                )
            });
        assert!(
            remaining.is_empty(),
            "performance namespace cleanup left objects under {}: {remaining:?}",
            self.namespace
        );
        self.armed = false;
    }
}

impl Drop for NamespaceCleanupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let store = self.store.clone();
        let namespace = Arc::clone(&self.namespace);
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new()
                .expect("failed to build panic-cleanup Tokio runtime");
            runtime.block_on(async move {
                let prefix = format!("{namespace}/");
                if let Err(error) = store.delete_prefix(&prefix).await {
                    eprintln!("[perf-contract] panic cleanup failed for {namespace}: {error}");
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

/// Run and check the three checked-in contracts (or an env-selected subset).
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
        let contract = load_or_panic(name);
        let spec = scenarios::build(&contract, env.repeats);
        let outcome = run_scenario(&spec, None).await;
        let violations = check_contract(&contract, &outcome);
        artifacts.write_scenario(name, &outcome, &violations);
        if !violations.is_empty() {
            failures.push((name.clone(), violations));
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

    for name in &env.scenarios {
        let contract = load_or_panic(name);
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
        (CacheState::Cold, _) => {
            let worlds = (0..STABILITY_REPEATS).map(|_| {
                let mut sample = spec.clone();
                sample.repeats = 1;
                sample
            });
            run_stability_worlds(worlds).await
        }
        (CacheState::Warm { .. }, MeasureOp::Query { .. }) => {
            let mut sample = spec.clone();
            sample.repeats = STABILITY_REPEATS;
            run_scenario(&sample, None).await
        }
        (CacheState::Warm { .. }, MeasureOp::Upsert { .. }) => {
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
        (CacheState::WarmHydrated, _) => panic!("WarmHydrated cache state requires Phase 2"),
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

async fn run_scenario(spec: &ScenarioSpec, injection: Option<Injection>) -> ScenarioOutcome {
    assert!(
        spec.repeats > 0,
        "scenario repeats must be greater than zero"
    );
    assert!(
        matches!(spec.server_config.manifest_cache_ttl_ms, 0 | 3_600_000),
        "manifest cache TTL must be pinned to 0 or 3600000"
    );
    if matches!(&spec.cache_state, CacheState::WarmHydrated) {
        panic!("WarmHydrated cache state requires Phase 2");
    }

    let mut generated = generate(spec.dataset.clone());
    for probes in &mut generated.expected.probe_clusters {
        probes.truncate(spec.server_config.nprobe);
    }

    let harness = TestHarness::new().await;
    let config = scenario_config(spec);
    let (depth_wrapped, tracker) = depth_store(&harness.store);
    let (instrumented_store, counter) = counting_store(&depth_wrapped);
    let server_store = injection.map_or_else(
        || instrumented_store.clone(),
        |selected| inject_store(&instrumented_store, selected),
    );
    let setup_server = start_test_server_full(
        server_store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
    )
    .await;
    let client = Client::new();
    let namespace = api_ns(&harness, &spec.name);
    let cleanup_guard = NamespaceCleanupGuard::new(instrumented_store.clone(), namespace.clone());
    create_namespace(&client, &setup_server, &namespace, spec).await;
    upsert_dataset(&client, &setup_server, &namespace, &generated).await;
    compact_until_ready(&client, &setup_server, &namespace).await;
    verify_cluster_balance(&setup_server, &namespace, &generated.expected, spec).await;

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
                start_test_server_full(server_store, Some(harness.prefix.clone()), config, false)
                    .await,
            );
        }
        CacheState::Warm { prime } => {
            for step in prime {
                match step {
                    Step::Measure => {
                        execute_measure(
                            &client,
                            &setup_server,
                            &namespace,
                            &generated,
                            &spec.measure,
                            &tracker,
                        )
                        .await;
                        await_tracker_idle(&tracker).await;
                    }
                }
            }
        }
        CacheState::WarmHydrated => unreachable!("checked before setup"),
    }
    let measured_server = cold_server.as_ref().unwrap_or(&setup_server);

    counter.reset();
    tracker.reset();
    let repeat_count = if matches!(&spec.cache_state, CacheState::Cold) {
        1
    } else {
        spec.repeats
    };
    let mut per_repeat = Vec::with_capacity(repeat_count);
    for _ in 0..repeat_count {
        let cutoff_us = execute_measure(
            &client,
            measured_server,
            &namespace,
            &generated,
            &spec.measure,
            &tracker,
        )
        .await;
        await_tracker_idle(&tracker).await;
        let classes = class_snapshot(&counter);
        let totals = sum_stats(classes.values().copied());
        let spans = tracker.take_spans();
        let raw_get_path =
            DepthTracker::critical_path(&spans, &[SpanKind::Get, SpanKind::Head], None);
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
        per_repeat.push(RepeatCounters {
            classes,
            totals,
            get_path,
            put_get_path,
            spans,
            response_cutoff_us: cutoff_us,
            raw_get_path,
            raw_put_get_path,
        });
        counter.reset();
        tracker.reset();
    }

    assert_fragment_window(measured_server, &namespace, spec).await;
    cleanup_guard.cleanup().await;
    harness.cleanup().await;

    ScenarioOutcome {
        per_repeat,
        expected: generated.expected,
    }
}

fn require_minio() {
    match std::env::var("TEST_BACKEND") {
        Ok(backend) if backend == "minio" => {}
        Ok(backend) => panic!("performance contracts require TEST_BACKEND=minio, got {backend:?}"),
        Err(std::env::VarError::NotPresent) => {
            panic!("performance contracts require TEST_BACKEND=minio")
        }
        Err(error) => panic!("failed to read TEST_BACKEND: {error}"),
    }
    match std::env::var("ZEPPELIN_MAX_CLUSTERS_PER_OBJECT") {
        Err(std::env::VarError::NotPresent) => {}
        Ok(value) => panic!(
            "performance contracts require ZEPPELIN_MAX_CLUSTERS_PER_OBJECT to be unset, got {value:?}"
        ),
        Err(error) => panic!("failed to read ZEPPELIN_MAX_CLUSTERS_PER_OBJECT: {error}"),
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

async fn compact_until_ready(client: &Client, server: &FullTestServer, namespace: &str) {
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

        let result = server
            .compactor
            .compact(namespace)
            .await
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
    config.cache.hydration_enabled = false;
    config.server.max_concurrent_queries = 1;
    config.indexing.default_num_centroids = spec.dataset.nlist;
    config.indexing.default_nprobe = spec.server_config.nprobe;
    config.indexing.max_nprobe = spec.dataset.nlist.max(spec.server_config.nprobe);
    config.indexing.quantization = QuantizationType::Scalar;
    config.indexing.hierarchical = false;
    config.indexing.bitmap_index = false;
    config.indexing.fts_index = false;
    config.query.rerank_coalesce_gap_bytes =
        Some(zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES);
    config.query.cost_latency_profile = None;
    config.compaction.max_wal_fragments_before_compact =
        spec.server_config.max_wal_fragments_before_compact;
    config
}

async fn execute_measure(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    generated: &GeneratedDataset,
    measure: &MeasureOp,
    tracker: &DepthTracker,
) -> u64 {
    let (status, bytes) = match measure {
        MeasureOp::Query {
            consistency,
            top_k,
            query_index,
        } => {
            let query = generated.queries.get(*query_index).unwrap_or_else(|| {
                panic!(
                    "query_index {query_index} outside generated query count {}",
                    generated.queries.len()
                )
            });
            let response = client
                .post(format!(
                    "{}/v1/namespaces/{namespace}/query",
                    server.base_url
                ))
                .json(&json!({
                    "vector": query.vector,
                    "top_k": top_k,
                    "consistency": consistency,
                    "include_attributes": false,
                }))
                .send()
                .await
                .unwrap_or_else(|error| panic!("query request failed for {namespace}: {error}"));
            let status = response.status();
            let bytes = response.bytes().await.unwrap_or_else(|error| {
                panic!("query response read failed for {namespace}: {error}")
            });
            (status, bytes)
        }
        MeasureOp::Upsert { batch } => {
            assert!(*batch > 0, "measured upsert batch must be nonzero");
            let values = &generated.queries[0].vector;
            let vectors = (0..*batch)
                .map(|index| GenVector {
                    id: format!("perf-measure-{index:08}"),
                    values: values.clone(),
                    attributes: None,
                })
                .collect::<Vec<_>>();
            let response = client
                .post(format!(
                    "{}/v1/namespaces/{namespace}/vectors",
                    server.base_url
                ))
                .json(&json!({ "vectors": vectors }))
                .send()
                .await
                .unwrap_or_else(|error| {
                    panic!("measured upsert request failed for {namespace}: {error}")
                });
            let status = response.status();
            let bytes = response.bytes().await.unwrap_or_else(|error| {
                panic!("measured upsert response read failed for {namespace}: {error}")
            });
            (status, bytes)
        }
    };
    let cutoff_us = tracker.elapsed_us();
    assert!(
        status.is_success(),
        "measured operation failed for {namespace}: status={status}, body={}",
        String::from_utf8_lossy(&bytes)
    );
    let body: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("measured response returned invalid JSON: {error}"));
    match measure {
        MeasureOp::Query { .. } => assert!(
            body["results"].is_array(),
            "query response omitted results for {namespace}: {body}"
        ),
        MeasureOp::Upsert { batch } => assert_eq!(
            body["upserted"].as_u64(),
            Some(*batch as u64),
            "measured upsert count mismatch for {namespace}"
        ),
    }
    cutoff_us
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
        MeasureOp::Query { .. } => (repeat.raw_get_path.depth, repeat.get_path.depth),
        MeasureOp::Upsert { .. } => (repeat.raw_put_get_path.depth, repeat.put_get_path.depth),
    }
}

fn stability_contract_violations(
    contract: &ContractSpec,
    measure: &MeasureOp,
    raw: &BTreeMap<u32, usize>,
    cutoff: &BTreeMap<u32, usize>,
) -> Vec<CostViolation> {
    let name = match measure {
        MeasureOp::Query { .. } => "get",
        MeasureOp::Upsert { .. } => "put_get",
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

    #[test]
    #[ignore = "focused helper test"]
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
    #[ignore = "focused helper test"]
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
