use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use reqwest::{Client, Method, StatusCode};
use serde_json::json;
use zeppelin::compaction::gc;
use zeppelin::config::{Config, GcConfig};
use zeppelin::types::ConsistencyLevel;
use zeppelin::wal::Manifest;

use crate::common::counting::{counting_store, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::common::server::{cleanup_ns, start_test_server_full, FullTestServer};

use super::artifacts::{
    read_ops, read_seed_config, FailureManifest, RunArtifacts, SeedArtifacts, SeedReport,
};
use super::chaos::{chaos_store, ChaosHandle, FaultPlan, FiredFault};
use super::generator::{AdversarialGenerator, Coverage};
use super::model::{Model, OracleMutation};
use super::ops::{GeneratedQuery, InvalidProbe, NamespaceSpec, Op, OpRecord, QueryOracleClass};
use super::oracle::{self, Violation, ViolationId};
use super::s3_oracle::{self, S3Tracker};
use super::{PreserveMode, RunMode, RunnerEnv};

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub seeds_run: u64,
    pub failed_seeds: u64,
    pub ops_total: u64,
    pub compactions_total: u64,
    pub background_compactions_total: u64,
    pub ops_per_sec: f64,
    pub coverage: Coverage,
}

#[derive(Debug)]
struct SeedOutcome {
    mode: RunMode,
    failed: bool,
    ops: u64,
    compactions: u64,
    background_compactions: u64,
    coverage: Coverage,
    violations: Vec<Violation>,
    wall_secs: f64,
    object_store: BTreeMap<String, ClassStats>,
    fired_faults: Vec<FiredFault>,
}

pub async fn run_smoke(env: RunnerEnv) -> RunSummary {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(env.seconds);
    let artifacts = RunArtifacts::create(&env);
    let artifact_root = artifacts.root().to_path_buf();
    let mut seed_reports = Vec::new();
    let mut summary = RunSummary {
        seeds_run: 0,
        failed_seeds: 0,
        ops_total: 0,
        compactions_total: 0,
        background_compactions_total: 0,
        ops_per_sec: 0.0,
        coverage: Coverage::default(),
    };

    for seed in &env.seeds {
        let outcome = run_seed(
            &env,
            &artifacts,
            *seed,
            deadline,
            env.selftest,
            env.selftest,
        )
        .await;
        summary.seeds_run += 1;
        summary.failed_seeds += u64::from(outcome.failed);
        summary.ops_total += outcome.ops;
        summary.compactions_total += outcome.compactions;
        summary.background_compactions_total += outcome.background_compactions;
        summary.coverage.merge(&outcome.coverage);
        seed_reports.push(SeedReport {
            seed: *seed,
            mode: outcome.mode,
            dir: artifact_root.join(format!("seed-{seed}")),
            failed: outcome.failed,
            ops: outcome.ops,
            compactions: outcome.compactions,
            background_compactions: outcome.background_compactions,
            violations: outcome.violations,
            wall_secs: outcome.wall_secs,
            object_store: outcome.object_store,
            fired_faults: outcome.fired_faults,
        });
    }
    summary.ops_per_sec = summary.ops_total as f64 / started.elapsed().as_secs_f64().max(0.001);
    artifacts.write_report(&env, &seed_reports, &summary.coverage, false);
    summary
}

pub async fn run_overnight(env: RunnerEnv) -> RunSummary {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(env.seconds);
    let artifacts = RunArtifacts::create(&env);
    let artifact_root = artifacts.root().to_path_buf();
    let mut seed_reports = Vec::new();
    let mut summary = RunSummary {
        seeds_run: 0,
        failed_seeds: 0,
        ops_total: 0,
        compactions_total: 0,
        background_compactions_total: 0,
        ops_per_sec: 0.0,
        coverage: Coverage::default(),
    };
    let mut seed_index = 0usize;

    while Instant::now() < deadline || summary.seeds_run == 0 {
        let base_seed = env.seeds[seed_index % env.seeds.len()];
        let round = (seed_index / env.seeds.len()) as u64;
        let seed = base_seed.wrapping_add(round << 32);
        let outcome = run_seed(&env, &artifacts, seed, deadline, env.selftest, env.selftest).await;
        summary.seeds_run += 1;
        summary.failed_seeds += u64::from(outcome.failed);
        summary.ops_total += outcome.ops;
        summary.compactions_total += outcome.compactions;
        summary.background_compactions_total += outcome.background_compactions;
        summary.coverage.merge(&outcome.coverage);
        seed_reports.push(SeedReport {
            seed,
            mode: outcome.mode,
            dir: artifact_root.join(format!("seed-{seed}")),
            failed: outcome.failed,
            ops: outcome.ops,
            compactions: outcome.compactions,
            background_compactions: outcome.background_compactions,
            violations: outcome.violations,
            wall_secs: outcome.wall_secs,
            object_store: outcome.object_store,
            fired_faults: outcome.fired_faults,
        });
        seed_index += 1;
    }

    summary.ops_per_sec = summary.ops_total as f64 / started.elapsed().as_secs_f64().max(0.001);
    artifacts.write_report(&env, &seed_reports, &summary.coverage, true);
    summary
}

pub async fn replay_seed_from_env() {
    let env = RunnerEnv::from_env();
    let replay = std::env::var("ZEPPELIN_ADVERSARIAL_REPLAY")
        .map(PathBuf::from)
        .expect("ZEPPELIN_ADVERSARIAL_REPLAY must point at a seed artifact dir");
    let expected_failure = read_failure_manifest(&replay);
    let outcome = run_replay(&env, &replay).await;

    if outcome.failed {
        if let Some(expected) = expected_failure {
            let actual = outcome
                .violations
                .first()
                .unwrap_or_else(|| panic!("replay failed without a recorded violation"));
            let expected_violation = expected
                .violations
                .first()
                .unwrap_or_else(|| panic!("failure.json had no violations"));
            assert_eq!(
                actual.op_index, expected_violation.op_index,
                "replay reproduced a violation at the wrong op index"
            );
            assert_eq!(
                actual.id, expected_violation.id,
                "replay reproduced the wrong violation id"
            );
            panic!(
                "replay reproduced {:?} at op {} in {}",
                actual.id,
                actual.op_index,
                replay.display()
            );
        }
        panic!(
            "replay produced unexpected violations: {:?}",
            outcome.violations
        );
    }

    if let Some(expected) = expected_failure {
        let limit = env.max_ops.unwrap_or(u64::MAX);
        let expected_index = expected
            .violations
            .first()
            .map_or(expected.op_index, |violation| violation.op_index);
        assert!(
            limit <= expected_index,
            "replay did not reproduce expected violation {:?} at op {}",
            expected.violations.first().map(|violation| violation.id),
            expected_index
        );
    }

    println!(
        "replay clean: dir={} ops={} compactions={} background_compactions={}",
        replay.display(),
        outcome.ops,
        outcome.compactions,
        outcome.background_compactions
    );
}

pub async fn inspect_from_env() {
    let target = std::env::var("ZEPPELIN_ADVERSARIAL_INSPECT")
        .expect("ZEPPELIN_ADVERSARIAL_INSPECT must be a seed dir or namespace prefix");
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let namespaces = inspect_namespaces(&store, &target).await;
    assert!(
        !namespaces.is_empty(),
        "inspect target {target:?} did not resolve to any namespaces"
    );
    let server = start_test_server_full(store.clone(), None, deterministic_config(), false).await;
    println!("inspect server: {}", server.base_url);
    for ns in &namespaces {
        print_namespace_inspection(&store, ns).await;
    }
    let hold_secs = std::env::var("ZEPPELIN_ADVERSARIAL_INSPECT_HOLD_SECS")
        .ok()
        .map(|value| {
            value.parse::<u64>().unwrap_or_else(|error| {
                panic!("invalid ZEPPELIN_ADVERSARIAL_INSPECT_HOLD_SECS={value}: {error}")
            })
        })
        .unwrap_or(0);
    if hold_secs > 0 {
        println!("holding inspect server for {hold_secs}s");
        tokio::time::sleep(Duration::from_secs(hold_secs)).await;
    }
    if let Some(shutdown) = server.shutdown_compaction.as_ref() {
        let _ = shutdown.send(true);
    }
}

async fn run_replay(env: &RunnerEnv, replay: &Path) -> SeedOutcome {
    let seed_config = replay_seed_config(replay);
    let replay_mutation = env.selftest.or(seed_config.fault_plan);
    let mode = effective_seed_mode(seed_config.mode, seed_config.seed);
    let chaos_plan = if mode == RunMode::Chaos {
        Some(
            seed_config
                .chaos_plan
                .clone()
                .unwrap_or_else(|| FaultPlan::for_seed(seed_config.seed)),
        )
    } else {
        None
    };
    let chaos_plan_json = chaos_plan
        .as_ref()
        .map(|plan| serde_json::to_value(plan).expect("FaultPlan must serialize"));
    let old_prefix = recorded_namespace_prefix(seed_config.seed, &seed_config.namespace_specs);
    let harness = TestHarness::new().await;
    let prefix = harness.prefix.clone();
    let (instrumented_store, chaos_handle) = wrap_chaos_store(&harness.store, chaos_plan.clone());
    let (store, counter) = counting_store(&instrumented_store);
    let config = seed_config.config.clone();
    let specs = seed_config
        .namespace_specs
        .iter()
        .map(|(ns, spec)| (rewrite_prefix(ns, &old_prefix, &prefix), spec.clone()))
        .collect::<BTreeMap<_, _>>();
    let run_artifacts = RunArtifacts::create(env);
    let mut artifacts = run_artifacts.seed(
        seed_config.seed,
        &config,
        &specs,
        mode,
        replay_mutation.map(OracleMutation::key),
        seed_config.selftest_probe.map(OracleMutation::key),
        chaos_plan_json.as_ref(),
    );
    let server = start_test_server_full(
        store.clone(),
        Some(prefix.clone()),
        config.clone(),
        mode == RunMode::Chaos,
    )
    .await;
    let client = adversarial_client();
    let mut model = Model::default();
    let mut coverage = Coverage::default();
    let mut s3_tracker = S3Tracker::default();
    let mut created_namespaces = Vec::new();
    let mut background_compaction_starts = BTreeMap::new();
    let mut failed = false;
    let mut failure_violations = Vec::new();
    let mut compactions = 0u64;
    let started = Instant::now();
    let max_ops = env.max_ops.unwrap_or(u64::MAX);

    let records = read_ops(replay);
    for source in records.into_iter().take(max_ops as usize) {
        let op = source.op.rewrite_namespace_prefix(&old_prefix, &prefix);
        let step = execute_recorded_op(
            &client,
            &server,
            &mut artifacts,
            &mut model,
            &mut coverage,
            &mut s3_tracker,
            &op,
            source.index,
            started,
            replay_mutation,
            mode,
        )
        .await;
        if matches!(op, Op::CreateNamespace { .. }) && (200..300).contains(&step.status) {
            let ns = op.namespace().to_string();
            note_background_compaction_namespace(&mut background_compaction_starts, &ns);
            created_namespaces.push(ns);
        }
        if let Op::CloneNamespace { target, .. } = &op {
            if (200..300).contains(&step.status) {
                note_background_compaction_namespace(&mut background_compaction_starts, target);
                created_namespaces.push(target.clone());
            }
        }
        if let Op::DeleteNamespace { ns } = &op {
            if (200..300).contains(&step.status) {
                created_namespaces.retain(|created| created != ns);
            }
        }
        if matches!(
            op,
            Op::CompactInline { .. } | Op::CompactEndpoint { .. } | Op::ProbeSandwich { .. }
        ) && (200..300).contains(&step.status)
        {
            compactions += 1;
        }
        if !step.violations.is_empty() {
            failed = true;
            failure_violations = step.violations;
            break;
        }
    }

    let mut op_count = artifacts.op_count();
    stop_chaos_and_background(&server, chaos_handle.as_ref()).await;
    if !failed {
        let quiescence = quiesce_and_verify(
            &client,
            &server,
            &mut artifacts,
            &mut model,
            &mut coverage,
            &mut s3_tracker,
            &created_namespaces,
            &mut op_count,
            &mut compactions,
            started,
            replay_mutation,
            RunMode::Deterministic,
        )
        .await;
        if !quiescence.is_empty() {
            failed = true;
            failure_violations = quiescence;
        }
    }

    artifacts.write_model_final(&model);
    artifacts.write_s3_final(&store, &created_namespaces).await;
    artifacts.write_coverage(&coverage);
    let fired_faults = chaos_handle
        .as_ref()
        .map(ChaosHandle::fired)
        .unwrap_or_default();
    artifacts.write_faults(&fired_faults);
    let object_store = object_store_breakdown(&counter);
    let background_compactions = background_compactions_since(&background_compaction_starts);
    if should_cleanup(env.preserve, failed) {
        for ns in &created_namespaces {
            cleanup_ns(&store, ns).await;
        }
        harness.cleanup().await;
    } else {
        println!("preserved replay prefix {prefix}");
    }
    if let Some(shutdown) = server.shutdown_compaction.as_ref() {
        let _ = shutdown.send(true);
    }
    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    SeedOutcome {
        mode,
        failed,
        ops: op_count,
        compactions,
        background_compactions,
        coverage,
        violations: failure_violations,
        wall_secs: elapsed,
        object_store,
        fired_faults,
    }
}

#[derive(Debug, serde::Deserialize)]
struct ReplaySeedConfig {
    seed: u64,
    mode: RunMode,
    #[serde(default)]
    fault_plan: Option<OracleMutation>,
    #[serde(default)]
    selftest_probe: Option<OracleMutation>,
    #[serde(default)]
    chaos_plan: Option<FaultPlan>,
    config: Config,
    namespace_specs: BTreeMap<String, NamespaceSpec>,
}

fn replay_seed_config(path: &Path) -> ReplaySeedConfig {
    serde_json::from_value(read_seed_config(path))
        .unwrap_or_else(|error| panic!("failed to parse replay seed config: {error}"))
}

fn read_failure_manifest(path: &Path) -> Option<FailureManifest> {
    let failure_path = path.join("failure.json");
    if !failure_path.exists() {
        return None;
    }
    let bytes = fs::read(&failure_path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", failure_path.display()));
    Some(serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!(
            "failed to parse failure manifest {}: {error}",
            failure_path.display()
        )
    }))
}

fn recorded_namespace_prefix(
    seed: u64,
    namespace_specs: &BTreeMap<String, NamespaceSpec>,
) -> String {
    let marker = format!("-adv-{seed}-");
    for ns in namespace_specs.keys() {
        if let Some(index) = ns.find(&marker) {
            return ns[..index].to_string();
        }
    }
    let first = namespace_specs
        .keys()
        .next()
        .unwrap_or_else(|| panic!("replay config contained no namespace specs"));
    first
        .rsplit_once('-')
        .map_or_else(|| first.clone(), |(prefix, _)| prefix.to_string())
}

fn rewrite_prefix(value: &str, old_prefix: &str, new_prefix: &str) -> String {
    value.strip_prefix(old_prefix).map_or_else(
        || value.to_string(),
        |suffix| format!("{new_prefix}{suffix}"),
    )
}

fn effective_seed_mode(mode: RunMode, seed: u64) -> RunMode {
    match mode {
        RunMode::Deterministic => RunMode::Deterministic,
        RunMode::Chaos => RunMode::Chaos,
        RunMode::Mixed => {
            if seed % 3 == 1 {
                RunMode::Chaos
            } else {
                RunMode::Deterministic
            }
        }
    }
}

fn config_for_mode(mode: RunMode, seed: u64) -> Config {
    let mut config = deterministic_config();
    if mode == RunMode::Chaos {
        config.cache.manifest_cache_ttl_ms = 500;
        config.compaction.interval_secs = 2 + (seed % 4);
        config.gc.compaction_upload_window_secs = 2;
    }
    config
}

fn wrap_chaos_store(
    store: &zeppelin::storage::ZeppelinStore,
    plan: Option<FaultPlan>,
) -> (zeppelin::storage::ZeppelinStore, Option<ChaosHandle>) {
    if let Some(plan) = plan {
        let (store, handle) = chaos_store(store, plan);
        (store, Some(handle))
    } else {
        (store.clone(), None)
    }
}

fn adversarial_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build adversarial reqwest client")
}

async fn stop_chaos_and_background(server: &FullTestServer, chaos: Option<&ChaosHandle>) {
    if let Some(chaos) = chaos {
        chaos.disable();
    }
    if let Some(shutdown) = server.shutdown_compaction.as_ref() {
        let _ = shutdown.send(true);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn sanitize_op_for_mode(op: Op, mode: RunMode) -> Op {
    if mode != RunMode::Chaos {
        return op;
    }
    match op {
        // Chaos mode injects S3 faults while foreground APIs are running.
        // Manual maintenance probes can fail for injected-storage reasons
        // that drown out foreground invariants; keep explicit maintenance out
        // of chaos mode and use the live background loop for compaction
        // coverage instead.
        Op::CompactInline { ns }
        | Op::CompactEndpoint { ns }
        | Op::GcCycle { ns, .. }
        | Op::ProbeSandwich { ns, .. } => Op::GetNamespace { ns },
        Op::FetchVectors { ns, ids, .. } => Op::FetchVectors {
            ns,
            ids,
            consistency: ConsistencyLevel::Strong,
        },
        Op::Query { ns, mut q, as_of } => {
            if let Some(object) = q.body.as_object_mut() {
                object.insert("consistency".to_string(), json!(ConsistencyLevel::Strong));
            }
            q.class = match q.class {
                QueryOracleClass::ExactAnn { top_k, filter, .. } => QueryOracleClass::ExactAnn {
                    top_k,
                    consistency: ConsistencyLevel::Strong,
                    filter,
                },
                QueryOracleClass::Membership { .. } => QueryOracleClass::Membership {
                    consistency: ConsistencyLevel::Strong,
                },
                QueryOracleClass::ExpectError { status, code } => {
                    QueryOracleClass::ExpectError { status, code }
                }
            };
            Op::Query { ns, q, as_of }
        }
        other => other,
    }
}

fn note_background_compaction_namespace(starts: &mut BTreeMap<String, u64>, ns: &str) {
    starts
        .entry(ns.to_string())
        .or_insert_with(|| background_compaction_metric(ns));
}

fn background_compactions_since(starts: &BTreeMap<String, u64>) -> u64 {
    starts
        .iter()
        .map(|(ns, start)| background_compaction_metric(ns).saturating_sub(*start))
        .sum()
}

fn background_compaction_metric(ns: &str) -> u64 {
    ["success", "failure"]
        .into_iter()
        .map(|status| {
            zeppelin::metrics::COMPACTIONS_TOTAL
                .with_label_values(&[ns, status])
                .get()
        })
        .sum()
}

fn object_store_breakdown(counter: &GetCounter) -> BTreeMap<String, ClassStats> {
    counter
        .class_breakdown()
        .into_iter()
        .map(|(class, stats)| (class.name().to_string(), stats))
        .collect()
}

fn recorded_seed_ops_if_requested(env: &RunnerEnv, seed: u64, prefix: &str) -> Option<Vec<Op>> {
    if std::env::var_os("ZEPPELIN_ADVERSARIAL_SEED").is_none()
        || std::env::var_os("ZEPPELIN_ADVERSARIAL_REPLAY").is_some()
    {
        return None;
    }

    let recorded = latest_recorded_seed_dir(env, seed)?;
    let seed_config = replay_seed_config(&recorded);
    let old_prefix = recorded_namespace_prefix(seed_config.seed, &seed_config.namespace_specs);
    let ops = read_ops(&recorded)
        .into_iter()
        .map(|record| rewrite_op_strings(&record.op, &old_prefix, prefix))
        .collect::<Vec<_>>();
    eprintln!(
        "determinism guard: comparing generated seed {} to {}",
        seed,
        recorded.display()
    );
    Some(ops)
}

fn latest_recorded_seed_dir(env: &RunnerEnv, seed: u64) -> Option<PathBuf> {
    let entries = match fs::read_dir(&env.artifacts) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!(
            "failed to read adversarial artifact root {}: {error}",
            env.artifacts.display()
        ),
    };
    let min_ops = env.max_ops.unwrap_or(0) as usize;
    let mut dirs = Vec::new();
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| {
            panic!(
                "failed to read entry under {}: {error}",
                env.artifacts.display()
            )
        });
        let path = entry.path().join(format!("seed-{seed}"));
        if !path.join("config.json").exists() || !path.join("ops.jsonl").exists() {
            continue;
        }
        let config = replay_seed_config(&path);
        if config.fault_plan == env.selftest && config.selftest_probe == env.selftest {
            let records = read_ops(&path);
            if !recorded_trace_uses_generated_ids(&records) {
                continue;
            }
            let op_count = records.len();
            if op_count >= min_ops {
                dirs.push((op_count, path));
            }
        }
    }
    dirs.sort();
    dirs.pop().map(|(_, path)| path)
}

fn recorded_trace_uses_generated_ids(records: &[OpRecord]) -> bool {
    for record in records {
        if let Op::Upsert { ns, vectors } = &record.op {
            return vectors.iter().all(|vector| vector.id.starts_with(ns));
        }
    }
    true
}

fn assert_recorded_op_matches(recorded_ops: Option<&[Op]>, seed: u64, index: u64, actual: &Op) {
    let Some(recorded_ops) = recorded_ops else {
        return;
    };
    let expected = recorded_ops.get(index as usize).unwrap_or_else(|| {
        panic!("determinism guard for seed {seed} ended before generated op {index}: {actual:#?}")
    });
    let expected_json =
        serde_json::to_value(expected).expect("recorded Op must serialize for comparison");
    let actual_json = serde_json::to_value(actual).expect("generated Op must serialize");
    assert!(
        json_values_equivalent(&actual_json, &expected_json),
        "determinism guard diverged for seed {seed} at op {index}\nexpected: {expected:#?}\nactual: {actual:#?}"
    );
}

fn json_values_equivalent(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (left, right) {
        (serde_json::Value::Null, serde_json::Value::Null) => true,
        (serde_json::Value::Bool(left), serde_json::Value::Bool(right)) => left == right,
        (serde_json::Value::String(left), serde_json::Value::String(right)) => left == right,
        (serde_json::Value::Number(left), serde_json::Value::Number(right)) => {
            json_numbers_equivalent(left, right)
        }
        (serde_json::Value::Array(left), serde_json::Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right.iter())
                    .all(|(left, right)| json_values_equivalent(left, right))
        }
        (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| json_values_equivalent(left, right))
                })
        }
        _ => false,
    }
}

fn json_numbers_equivalent(left: &serde_json::Number, right: &serde_json::Number) -> bool {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return left == right;
    }
    if let (Some(left), Some(right)) = (left.as_u64(), right.as_u64()) {
        return left == right;
    }
    let left = left
        .as_f64()
        .expect("serde_json number should fit in f64 for guard comparison");
    let right = right
        .as_f64()
        .expect("serde_json number should fit in f64 for guard comparison");
    (left - right).abs() <= 1e-12
}

fn rewrite_op_strings(op: &Op, old_prefix: &str, new_prefix: &str) -> Op {
    let mut value = serde_json::to_value(op).expect("Op must serialize for determinism guard");
    rewrite_json_strings(&mut value, old_prefix, new_prefix);
    serde_json::from_value(value).expect("rewritten Op must deserialize")
}

fn rewrite_json_strings(value: &mut serde_json::Value, old_prefix: &str, new_prefix: &str) {
    match value {
        serde_json::Value::String(string) => {
            if let Some(suffix) = string.strip_prefix(old_prefix) {
                *string = format!("{new_prefix}{suffix}");
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                rewrite_json_strings(value, old_prefix, new_prefix);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                rewrite_json_strings(value, old_prefix, new_prefix);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

async fn inspect_namespaces(store: &zeppelin::storage::ZeppelinStore, target: &str) -> Vec<String> {
    let path = Path::new(target);
    if path.exists() {
        let failure = read_failure_manifest(path);
        if let Some(failure) = failure {
            let discovered = discover_namespaces(store, &failure.preserved_prefix).await;
            if !discovered.is_empty() {
                return discovered;
            }
            let config = replay_seed_config(path);
            return config.namespace_specs.keys().cloned().collect();
        }
        let config = replay_seed_config(path);
        return config.namespace_specs.keys().cloned().collect();
    }
    discover_namespaces(store, target).await
}

async fn discover_namespaces(
    store: &zeppelin::storage::ZeppelinStore,
    prefix: &str,
) -> Vec<String> {
    let mut namespaces = store
        .list_common_prefixes("")
        .await
        .unwrap_or_else(|error| panic!("failed to list root namespace prefixes: {error}"))
        .into_iter()
        .filter_map(|key| key.strip_suffix('/').map(str::to_string))
        .filter(|namespace| namespace.starts_with(prefix))
        .collect::<Vec<_>>();
    namespaces.sort();
    namespaces
}

async fn print_namespace_inspection(store: &zeppelin::storage::ZeppelinStore, ns: &str) {
    println!("\n## namespace {ns}");
    let Some(manifest) = Manifest::read(store, ns)
        .await
        .unwrap_or_else(|error| panic!("failed to read manifest for {ns}: {error}"))
    else {
        println!("manifest: missing");
        return;
    };
    println!("manifest generation: {}", manifest.version());
    println!("fencing_token: {}", manifest.fencing_token);
    println!("next_sequence: {}", manifest.next_sequence);
    println!("active_segment: {:?}", manifest.active_segment);
    println!("compaction_watermark: {:?}", manifest.compaction_watermark);
    println!("updated_at: {}", manifest.updated_at);
    println!("pending_deletes: {}", manifest.pending_deletes.len());
    for key in &manifest.pending_deletes {
        println!("  pending {key}");
    }

    println!("fragments: {}", manifest.fragments.len());
    for fragment in &manifest.fragments {
        println!(
            "  {} seq={} vectors={} deletes={} bytes={}",
            fragment.id,
            fragment.sequence_number,
            fragment.vector_count,
            fragment.delete_count,
            fragment.size_bytes
        );
    }

    println!("segments: {}", manifest.segments.len());
    for segment in &manifest.segments {
        let carried = segment
            .cluster_owners
            .iter()
            .filter(|owner| *owner != &segment.id)
            .count();
        println!(
            "  {} vectors={} clusters={} quant={:?} hierarchical={} carried_clusters={} fts={:?} bitmap={:?} global_fts={}",
            segment.id,
            segment.vector_count,
            segment.cluster_count,
            segment.quantization,
            segment.hierarchical,
            carried,
            segment.fts_fields,
            segment.bitmap_fields,
            segment.has_global_fts
        );
    }

    let snapshots = zeppelin::wal::manifest::NamedSnapshot::list(store, ns)
        .await
        .unwrap_or_else(|error| panic!("failed to list snapshots for {ns}: {error}"));
    println!("snapshots: {}", snapshots.len());
    for snapshot in &snapshots {
        println!(
            "  {} -> generation {} at {}",
            snapshot.name, snapshot.generation, snapshot.created_at
        );
    }

    println!("history:");
    for entry in Manifest::list_history(store, ns)
        .await
        .unwrap_or_else(|error| panic!("failed to list history for {ns}: {error}"))
    {
        let history = Manifest::read_history(store, ns, entry.version)
            .await
            .unwrap_or_else(|error| panic!("failed to read history {}: {error}", entry.key))
            .unwrap_or_else(|| panic!("history key disappeared: {}", entry.key));
        let pins = snapshots
            .iter()
            .filter(|snapshot| snapshot.generation == entry.version)
            .map(|snapshot| snapshot.name.as_str())
            .collect::<Vec<_>>();
        println!(
            "  generation {} updated_at={} pins={:?}",
            history.version(),
            history.updated_at,
            pins
        );
    }

    let candidates = gc::load_gc_candidates(store, ns)
        .await
        .unwrap_or_else(|error| panic!("failed to load GC candidates for {ns}: {error}"));
    println!("gc candidates: {}", candidates.len());
    for candidate in &candidates {
        println!(
            "  {} first_seen={} since_generation={}",
            candidate.key,
            candidate.first_seen_unreachable_at,
            candidate.unreachable_since_manifest_version
        );
    }

    let reachable = gc::reachable_keys_with_retained_history_and_staging(
        store,
        ns,
        &manifest,
        &Default::default(),
    )
    .await
    .unwrap_or_else(|error| panic!("failed to compute reachability for {ns}: {error}"));
    let listed = store
        .list_prefix(&format!("{ns}/"))
        .await
        .unwrap_or_else(|error| panic!("failed to list namespace keys for {ns}: {error}"))
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let missing = reachable.difference(&listed).cloned().collect::<Vec<_>>();
    let extra = listed.difference(&reachable).cloned().collect::<Vec<_>>();
    println!("reach \\ listed: {}", missing.len());
    for key in missing {
        println!("  missing {key}");
    }
    println!("listed \\ reach: {}", extra.len());
    for key in extra {
        println!("  awaiting_gc {key}");
    }
}

pub async fn run_oracle_selftest(env: RunnerEnv) {
    let mutations = env.selftest.map_or_else(
        || {
            vec![
                OracleMutation::DropDelete,
                OracleMutation::SkewScore,
                OracleMutation::PhantomId,
                OracleMutation::LeakTombstone,
                OracleMutation::FilterSkew,
                OracleMutation::GcEatsLiveKey,
                OracleMutation::StaleCheckpoint,
                OracleMutation::ChaosLostWrite,
            ]
        },
        |mutation| vec![mutation],
    );

    for mutation in mutations {
        let seed = 7;
        let clean_env = env.for_oracle_selftest(seed);
        let clean_artifacts = RunArtifacts::create(&clean_env);
        let clean = run_seed(
            &clean_env,
            &clean_artifacts,
            seed,
            Instant::now() + Duration::from_secs(clean_env.seconds),
            None,
            Some(mutation),
        )
        .await;
        assert!(
            !clean.failed,
            "clean oracle selftest control for {} failed: {:?}",
            mutation.key(),
            clean.violations
        );

        let mutated_env = env.for_oracle_selftest(seed);
        let mutated_artifacts = RunArtifacts::create(&mutated_env);
        let mutated = run_seed(
            &mutated_env,
            &mutated_artifacts,
            seed,
            Instant::now() + Duration::from_secs(mutated_env.seconds),
            Some(mutation),
            Some(mutation),
        )
        .await;
        assert!(
            mutated.failed,
            "oracle selftest mutation {} did not fail",
            mutation.key()
        );
        let fired: Vec<ViolationId> = mutated
            .violations
            .iter()
            .map(|violation| violation.id)
            .collect();
        let accepted = match mutation {
            OracleMutation::DropDelete | OracleMutation::PhantomId => {
                fired.contains(&ViolationId::I4FetchExact)
            }
            OracleMutation::SkewScore => fired.contains(&ViolationId::I1StrongExact),
            OracleMutation::LeakTombstone => fired.contains(&ViolationId::I3EventualExact),
            OracleMutation::FilterSkew => fired.contains(&ViolationId::I1StrongExact),
            OracleMutation::GcEatsLiveKey => fired.contains(&ViolationId::I14S3Reachability),
            OracleMutation::StaleCheckpoint => fired.contains(&ViolationId::I8AsOfExact),
            OracleMutation::ChaosLostWrite => fired.contains(&ViolationId::I16Quiescence),
        };
        assert!(
            accepted,
            "oracle selftest mutation {} fired {:?}, not the expected violation",
            mutation.key(),
            fired
        );
        println!(
            "oracle selftest {} fired {:?} after {} ops",
            mutation.key(),
            fired,
            mutated.ops
        );
    }
}

async fn run_seed(
    env: &RunnerEnv,
    artifacts: &RunArtifacts,
    seed: u64,
    deadline: Instant,
    mutation: Option<OracleMutation>,
    selftest_probe: Option<OracleMutation>,
) -> SeedOutcome {
    let mode = if mutation == Some(OracleMutation::ChaosLostWrite) {
        RunMode::Chaos
    } else {
        effective_seed_mode(env.mode, seed)
    };
    let chaos_plan = if mode == RunMode::Chaos {
        Some(if mutation == Some(OracleMutation::ChaosLostWrite) {
            FaultPlan::lost_write_selftest()
        } else {
            FaultPlan::for_seed(seed)
        })
    } else {
        None
    };
    let chaos_plan_json = chaos_plan
        .as_ref()
        .map(|plan| serde_json::to_value(plan).expect("FaultPlan must serialize"));
    let harness = TestHarness::new().await;
    let prefix = harness.prefix.clone();
    let (instrumented_store, chaos_handle) = wrap_chaos_store(&harness.store, chaos_plan.clone());
    let (store, counter) = counting_store(&instrumented_store);
    let config = config_for_mode(mode, seed);
    let mut generator = AdversarialGenerator::new(seed, &prefix);
    let specs = generator.specs();
    let recorded_ops = recorded_seed_ops_if_requested(env, seed, &prefix);
    let mut artifacts = artifacts.seed(
        seed,
        &config,
        &specs,
        mode,
        mutation.map(OracleMutation::key),
        selftest_probe.map(OracleMutation::key),
        chaos_plan_json.as_ref(),
    );
    let server = start_test_server_full(
        store.clone(),
        Some(prefix.clone()),
        config.clone(),
        mode == RunMode::Chaos,
    )
    .await;
    let client = adversarial_client();
    let mut model = Model::default();
    let mut coverage = Coverage::default();
    let mut created_namespaces = Vec::new();
    let mut background_compaction_starts = BTreeMap::new();
    let mut s3_tracker = S3Tracker::default();
    let mut op_index = 0u64;
    let mut failed = false;
    let mut failure_violations = Vec::new();
    let mut compactions = 0u64;
    let started = Instant::now();
    let max_ops = env.max_ops.unwrap_or(500);

    while op_index < max_ops && (Instant::now() < deadline || op_index == 0) {
        let op = sanitize_op_for_mode(generator.next(&model), mode);
        assert_recorded_op_matches(recorded_ops.as_deref(), seed, op_index, &op);
        let step = execute_recorded_op(
            &client,
            &server,
            &mut artifacts,
            &mut model,
            &mut coverage,
            &mut s3_tracker,
            &op,
            op_index,
            started,
            mutation,
            mode,
        )
        .await;
        if matches!(op, Op::CreateNamespace { .. }) && (200..300).contains(&step.status) {
            let ns = op.namespace().to_string();
            note_background_compaction_namespace(&mut background_compaction_starts, &ns);
            created_namespaces.push(ns);
        }
        if let Op::CloneNamespace { target, .. } = &op {
            if (200..300).contains(&step.status) {
                note_background_compaction_namespace(&mut background_compaction_starts, target);
                created_namespaces.push(target.clone());
            }
        }
        if let Op::DeleteNamespace { ns } = &op {
            if (200..300).contains(&step.status) {
                created_namespaces.retain(|created| created != ns);
            }
        }
        if matches!(
            op,
            Op::CompactInline { .. } | Op::CompactEndpoint { .. } | Op::ProbeSandwich { .. }
        ) && (200..300).contains(&step.status)
        {
            compactions += 1;
        }
        op_index += 1;
        if !step.violations.is_empty() {
            failed = true;
            failure_violations = step.violations;
            break;
        }

        if let Some(probe) = selftest_probe {
            if let Some(probe_op) = selftest_probe_op(probe, &op, &model, &mut generator) {
                assert_recorded_op_matches(recorded_ops.as_deref(), seed, op_index, &probe_op);
                let step = execute_recorded_op(
                    &client,
                    &server,
                    &mut artifacts,
                    &mut model,
                    &mut coverage,
                    &mut s3_tracker,
                    &probe_op,
                    op_index,
                    started,
                    mutation,
                    mode,
                )
                .await;
                op_index += 1;
                if !step.violations.is_empty() {
                    failed = true;
                    failure_violations = step.violations;
                    break;
                }
            }
        }
    }

    stop_chaos_and_background(&server, chaos_handle.as_ref()).await;

    if !failed {
        let quiescence = quiesce_and_verify(
            &client,
            &server,
            &mut artifacts,
            &mut model,
            &mut coverage,
            &mut s3_tracker,
            &created_namespaces,
            &mut op_index,
            &mut compactions,
            started,
            mutation,
            RunMode::Deterministic,
        )
        .await;
        if !quiescence.is_empty() {
            failed = true;
            failure_violations = quiescence;
        }
    }

    artifacts.write_model_final(&model);
    artifacts.write_s3_final(&store, &created_namespaces).await;
    artifacts.write_coverage(&coverage);
    let fired_faults = chaos_handle
        .as_ref()
        .map(ChaosHandle::fired)
        .unwrap_or_default();
    artifacts.write_faults(&fired_faults);
    let object_store = object_store_breakdown(&counter);
    let background_compactions = background_compactions_since(&background_compaction_starts);

    if failed {
        let failure_op_index = failure_violations
            .first()
            .map_or(op_index, |violation| violation.op_index);
        artifacts
            .capture_s3_metadata(
                &store,
                &created_namespaces,
                std::env::var("ZEPPELIN_ADVERSARIAL_DUMP_S3").as_deref() == Ok("full"),
            )
            .await;
        let replay_max_ops = failure_op_index + 1;
        let backend = env
            .env_echo
            .get("TEST_BACKEND")
            .map(String::as_str)
            .unwrap_or("memory");
        let mut repro_env = format!("TEST_BACKEND={backend}");
        if let Some(mutation) = mutation {
            repro_env.push_str(&format!(
                " ZEPPELIN_ADVERSARIAL_SELFTEST={}",
                mutation.key()
            ));
        }
        artifacts.write_failure(&FailureManifest {
            seed,
            mode,
            op_index: failure_op_index,
            violations: failure_violations.clone(),
            preserved_prefix: prefix.clone(),
            fault_plan: mutation.map(|mutation| mutation.key().to_string()),
            repro_cmd: format!(
                "{repro_env} ZEPPELIN_ADVERSARIAL_REPLAY={} ZEPPELIN_ADVERSARIAL_MAX_OPS={} cargo test --test adversarial_workload_tests replay_seed -- --ignored --nocapture",
                artifacts.dir.display(),
                replay_max_ops
            ),
            inspect_cmd: format!(
                "TEST_BACKEND={} ZEPPELIN_ADVERSARIAL_INSPECT={} cargo test --test adversarial_workload_tests inspect -- --ignored --nocapture",
                backend,
                artifacts.dir.display()
            ),
        });
    }

    if should_cleanup(env.preserve, failed) {
        for ns in &created_namespaces {
            cleanup_ns(&store, ns).await;
        }
        harness.cleanup().await;
    } else {
        println!("preserved adversarial prefix {prefix}");
    }

    if let Some(shutdown) = server.shutdown_compaction.as_ref() {
        let _ = shutdown.send(true);
    }

    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    println!(
        "seed {}: failed={} ops={} compactions={} background_compactions={} ops/sec={:.2}",
        seed,
        failed,
        op_index,
        compactions,
        background_compactions,
        op_index as f64 / elapsed
    );

    SeedOutcome {
        mode,
        failed,
        ops: op_index,
        compactions,
        background_compactions,
        coverage,
        violations: failure_violations,
        wall_secs: elapsed,
        object_store,
        fired_faults,
    }
}

struct StepOutcome {
    status: u16,
    violations: Vec<Violation>,
}

#[allow(clippy::too_many_arguments)]
async fn execute_recorded_op(
    client: &Client,
    server: &FullTestServer,
    artifacts: &mut SeedArtifacts,
    model: &mut Model,
    coverage: &mut Coverage,
    s3_tracker: &mut S3Tracker,
    op: &Op,
    index: u64,
    started: Instant,
    mutation: Option<OracleMutation>,
    mode: RunMode,
) -> StepOutcome {
    let mut rec = execute_op(client, server, op, index, started).await;
    if (200..300).contains(&rec.status) {
        match op {
            Op::CloneNamespace { .. } => {
                rec.gen_after = rec
                    .response
                    .get("generation")
                    .and_then(serde_json::Value::as_u64);
            }
            Op::DeleteNamespace { .. } | Op::PatchIndexConfig { .. } => {}
            _ if op.is_mutating() => {
                rec.gen_after =
                    Some(compact_generation(client, &server.base_url, op.namespace()).await);
            }
            _ => {}
        }
    }
    coverage.record(op);
    artifacts.write_op(&rec);
    model.apply(op, rec.status, rec.gen_after, &rec.response, mutation);
    if (200..300).contains(&rec.status) {
        if let Op::DeleteNamespace { ns } = op {
            s3_tracker.forget_namespace(ns);
        }
    }
    let mut violations = oracle::check_op(model, &rec, mode, mutation);
    if mutation == Some(OracleMutation::ChaosLostWrite) && mode == RunMode::Chaos {
        violations.clear();
    }
    if (200..300).contains(&rec.status) {
        if let Op::CloneNamespace { target, .. } = op {
            violations.extend(
                s3_oracle::check_clone_manifest(&server.store, target, &rec.response, rec.index)
                    .await,
            );
        }
    }
    if mode == RunMode::Deterministic && rec.index % 25 == 0 {
        for (ns, ns_model) in &model.namespaces {
            if !ns_model.spec.is_exact() {
                continue;
            }
            let status = compact_status(client, &server.base_url, ns).await;
            violations.extend(
                s3_tracker
                    .check_namespace(
                        &server.store,
                        ns,
                        rec.index,
                        &status,
                        mutation == Some(OracleMutation::GcEatsLiveKey),
                    )
                    .await,
            );
        }
    }
    StepOutcome {
        status: rec.status,
        violations,
    }
}

async fn execute_op(
    client: &Client,
    server: &FullTestServer,
    op: &Op,
    index: u64,
    started: Instant,
) -> OpRecord {
    let before = Instant::now();
    let (method, path, status, response) = match op {
        Op::CreateNamespace { ns, spec } => {
            let path = "/v1/namespaces".to_string();
            let body = spec.create_body(ns);
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(body),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::GetNamespace { ns } => {
            let path = format!("/v1/namespaces/{ns}");
            let (status, response) = request_json(
                client,
                Method::GET,
                &format!("{}{}", server.base_url, path),
                None,
            )
            .await;
            ("GET".to_string(), path, status, response)
        }
        Op::Upsert { ns, vectors } => {
            let path = format!("/v1/namespaces/{ns}/vectors");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({ "vectors": vectors })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::DeleteVectors { ns, ids } => {
            let path = format!("/v1/namespaces/{ns}/vectors");
            let (status, response) = request_json(
                client,
                Method::DELETE,
                &format!("{}{}", server.base_url, path),
                Some(json!({ "ids": ids })),
            )
            .await;
            ("DELETE".to_string(), path, status, response)
        }
        Op::FetchVectors {
            ns,
            ids,
            consistency,
        } => {
            let path = format!("/v1/namespaces/{ns}/vectors/get");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({
                    "ids": ids,
                    "include_vector": true,
                    "include_attributes": true,
                    "consistency": consistency,
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::Query { ns, q, as_of } => {
            let path = if let Some(as_of) = as_of {
                format!("/v1/namespaces/{ns}/query?as_of={as_of}")
            } else {
                format!("/v1/namespaces/{ns}/query")
            };
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(q.body.clone()),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::BatchQuery { ns, qs } => {
            let path = format!("/v1/namespaces/{ns}/query/batch");
            let queries = qs.iter().map(|q| q.body.clone()).collect::<Vec<_>>();
            let (status, batch) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({ "queries": queries })),
            )
            .await;
            let mut individual = Vec::with_capacity(qs.len());
            for q in qs {
                let single_path = format!("/v1/namespaces/{ns}/query");
                let (single_status, single_body) = request_json(
                    client,
                    Method::POST,
                    &format!("{}{}", server.base_url, single_path),
                    Some(q.body.clone()),
                )
                .await;
                individual.push(json!({
                    "status": single_status,
                    "body": single_body
                }));
            }
            (
                "POST".to_string(),
                path,
                status,
                json!({
                    "batch": batch,
                    "individual": individual
                }),
            )
        }
        Op::PaginateAll { ns, q, page_size } => {
            let path = format!("/v1/namespaces/{ns}/query");
            let mut pages = Vec::new();
            let mut cursor = json!({ "type": "none" });
            let mut status = StatusCode::OK.as_u16();
            for _ in 0..50 {
                let mut page_body = q.body.clone();
                let page_object = page_body.as_object_mut().expect("query body is object");
                page_object.insert("top_k".to_string(), json!(page_size));
                page_object.insert("cursor".to_string(), cursor.clone());
                let (page_status, page_response) = request_json(
                    client,
                    Method::POST,
                    &format!("{}{}", server.base_url, path),
                    Some(page_body),
                )
                .await;
                if !(200..300).contains(&page_status) {
                    status = page_status;
                    pages.push(json!({ "status": page_status, "body": page_response }));
                    break;
                }
                let next = page_response
                    .get("next_cursor")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string);
                pages.push(json!({ "status": page_status, "body": page_response }));
                let Some(next) = next else {
                    break;
                };
                cursor = json!({ "type": "after", "token": next });
            }

            let mut big_body = q.body.clone();
            let paged_result_count = pages
                .iter()
                .filter_map(|page| page["body"]["results"].as_array())
                .map(Vec::len)
                .sum::<usize>()
                .max(*page_size);
            let big_object = big_body.as_object_mut().expect("query body is object");
            big_object.insert("top_k".to_string(), json!(paged_result_count));
            big_object.insert("cursor".to_string(), json!({ "type": "none" }));
            let (big_status, big_response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(big_body),
            )
            .await;
            if !(200..300).contains(&big_status) {
                status = big_status;
            }
            (
                "POST".to_string(),
                path,
                status,
                json!({
                    "pages": pages,
                    "big": {
                        "status": big_status,
                        "body": big_response
                    }
                }),
            )
        }
        Op::InvalidProbe { ns, probe } => {
            let status_before = if probe.is_write_shaped() {
                Some(compact_status(client, &server.base_url, ns).await)
            } else {
                None
            };
            let (method, path, status, mut response) =
                execute_invalid_probe(client, server, ns, *probe).await;
            if let Some(before) = status_before {
                let after = compact_status(client, &server.base_url, ns).await;
                let response_object = response
                    .as_object_mut()
                    .expect("invalid probe error response is object");
                response_object.insert("compact_status_before".to_string(), before);
                response_object.insert("compact_status_after".to_string(), after);
            }
            (method, path, status, response)
        }
        Op::CompactEndpoint { ns } => {
            let path = format!("/v1/namespaces/{ns}/compact");
            let (trigger_status, trigger) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                None,
            )
            .await;
            let mut final_status = trigger_status;
            let mut status_body = serde_json::Value::Null;
            if (200..300).contains(&trigger_status) {
                status_body = wait_compaction_ready(client, &server.base_url, ns).await;
                final_status = StatusCode::OK.as_u16();
            }
            (
                "POST".to_string(),
                path,
                final_status,
                json!({
                    "trigger_status": trigger_status,
                    "trigger": trigger,
                    "status": status_body
                }),
            )
        }
        Op::GcCycle { ns, keep_count } => {
            let path = format!("gc::run_gc_cycle({ns})");
            let config = gc_config(*keep_count);
            let report = gc::run_gc_cycle(&server.store, ns, &config)
                .await
                .unwrap_or_else(|error| panic!("gc cycle failed for {ns}: {error}"));
            let retained_generations = Manifest::list_history(&server.store, ns)
                .await
                .unwrap_or_else(|error| panic!("history list after gc failed for {ns}: {error}"))
                .into_iter()
                .map(|entry| entry.version)
                .collect::<Vec<_>>();
            (
                "IN_PROCESS".to_string(),
                path,
                StatusCode::OK.as_u16(),
                json!({
                    "candidates_marked": report.candidates_marked,
                    "objects_deleted": report.objects_deleted,
                    "pending_deletes_deleted": report.pending_deletes_deleted,
                    "pending_deletes_pruned": report.pending_deletes_pruned,
                    "pending_deletes_retained": report.pending_deletes_retained,
                    "bytes_reclaimed": report.bytes_reclaimed,
                    "candidates_skipped": report.candidates_skipped,
                    "retained_generations": retained_generations,
                    "keep_count": keep_count
                }),
            )
        }
        Op::CreateSnapshot { ns, name } => {
            let path = format!("/v1/namespaces/{ns}/snapshots/{name}");
            let (status, response) = request_json(
                client,
                Method::PUT,
                &format!("{}{}", server.base_url, path),
                None,
            )
            .await;
            ("PUT".to_string(), path, status, response)
        }
        Op::GetSnapshot { ns, name } => {
            let path = format!("/v1/namespaces/{ns}/snapshots/{name}");
            let (status, response) = request_json(
                client,
                Method::GET,
                &format!("{}{}", server.base_url, path),
                None,
            )
            .await;
            ("GET".to_string(), path, status, response)
        }
        Op::ListSnapshots { ns } => {
            let path = format!("/v1/namespaces/{ns}/snapshots");
            let (status, response) = request_json(
                client,
                Method::GET,
                &format!("{}{}", server.base_url, path),
                None,
            )
            .await;
            ("GET".to_string(), path, status, response)
        }
        Op::DeleteSnapshot { ns, name } => {
            let path = format!("/v1/namespaces/{ns}/snapshots/{name}");
            let (status, response) = request_json(
                client,
                Method::DELETE,
                &format!("{}{}", server.base_url, path),
                None,
            )
            .await;
            ("DELETE".to_string(), path, status, response)
        }
        Op::CloneNamespace {
            source,
            target,
            as_of,
        } => {
            let path = format!("/v1/namespaces/{source}/clone");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({
                    "target": target,
                    "as_of": as_of.to_string()
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::PatchIndexConfig { ns, patch } => {
            let path = format!("/v1/namespaces/{ns}/index_config");
            let (status, response) = request_json(
                client,
                Method::PATCH,
                &format!("{}{}", server.base_url, path),
                Some(patch.clone()),
            )
            .await;
            ("PATCH".to_string(), path, status, response)
        }
        Op::Hydrate { ns } => {
            let path = format!("/v1/namespaces/{ns}/hydrate");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                None,
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        Op::DeleteNamespace { ns } => {
            let path = format!("/v1/namespaces/{ns}");
            let (status, response) = request_json(
                client,
                Method::DELETE,
                &format!("{}{}", server.base_url, path),
                None,
            )
            .await;
            if (200..300).contains(&status) {
                wait_namespace_gone(client, &server.base_url, ns).await;
            }
            ("DELETE".to_string(), path, status, response)
        }
        Op::ProbeSandwich { ns, maintenance } => {
            let path = format!("probe_sandwich({ns},{maintenance:?})");
            let before = compact_status(client, &server.base_url, ns).await;
            let maintenance_response =
                execute_sandwich_maintenance(client, server, ns, *maintenance).await;
            let after = compact_status(client, &server.base_url, ns).await;
            (
                "COMPOSITE".to_string(),
                path,
                StatusCode::OK.as_u16(),
                json!({
                    "before": before,
                    "maintenance": maintenance_response,
                    "after": after
                }),
            )
        }
        Op::CompactInline { ns } => match server.compactor.compact(ns).await {
            Ok(result) => (
                "IN_PROCESS".to_string(),
                format!("compactor.compact({ns})"),
                StatusCode::OK.as_u16(),
                json!({
                    "segment_id": result.segment_id,
                    "vectors_compacted": result.vectors_compacted,
                    "fragments_removed": result.fragments_removed,
                    "old_segment_removed": result.old_segment_removed,
                }),
            ),
            Err(error) => (
                "IN_PROCESS".to_string(),
                format!("compactor.compact({ns})"),
                StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                json!({
                    "code": "INTERNAL_ERROR",
                    "error": error.to_string(),
                    "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    "retryable": true,
                    "request_id": "in-process",
                }),
            ),
        },
    };

    OpRecord {
        index,
        wall_ms: started.elapsed().as_millis() as u64,
        op: op.clone(),
        method,
        path,
        status,
        response,
        gen_after: None,
        duration_ms: before.elapsed().as_millis() as u64,
        violations: Vec::new(),
    }
}

async fn execute_invalid_probe(
    client: &Client,
    server: &FullTestServer,
    ns: &str,
    probe: InvalidProbe,
) -> (String, String, u16, serde_json::Value) {
    match probe {
        InvalidProbe::NanVector => {
            let path = format!("/v1/namespaces/{ns}/query");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [serde_json::Value::Null, json!(0.0)]
                    }],
                    "fusion": { "type": "none" },
                    "top_k": 1
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::WrongDims => {
            let path = format!("/v1/namespaces/{ns}/vectors");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({
                    "vectors": [{
                        "id": "wrong-dims",
                        "values": [0.0]
                    }]
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::BadIdCharset => {
            let path = format!("/v1/namespaces/{ns}/vectors");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({
                    "vectors": [{
                        "id": "bad/id",
                        "values": [0.0, 0.0]
                    }]
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::EmptyBatch => {
            let path = format!("/v1/namespaces/{ns}/vectors");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({ "vectors": [] })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::OversizedBatch => {
            let path = format!("/v1/namespaces/{ns}/query/batch");
            let queries = (0..257)
                .map(|_| {
                    json!({
                        "sources": [{
                            "type": "ann",
                            "vector": [0.0, 0.0]
                        }],
                        "fusion": { "type": "none" },
                        "top_k": 1
                    })
                })
                .collect::<Vec<_>>();
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({ "queries": queries })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::UnknownField => {
            let path = format!("/v1/namespaces/{ns}/query");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [0.0, 0.0]
                    }],
                    "fusion": { "type": "none" },
                    "top_k": 1,
                    "unexpected": true
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::BadCursorToken => {
            let path = format!("/v1/namespaces/{ns}/query");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [0.0, 0.0]
                    }],
                    "top_k": 1,
                    "cursor": { "type": "after", "token": "not-a-cursor" }
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::GroupingPlusCursor => {
            let path = format!("/v1/namespaces/{ns}/query");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [0.0, 0.0]
                    }],
                    "top_k": 1,
                    "grouping": { "type": "field", "field": "group", "max_per_group": 1 },
                    "cursor": { "type": "none" }
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::WeightsLenMismatch => {
            let path = format!("/v1/namespaces/{ns}/query");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [0.0, 0.0]
                    }, {
                        "type": "ann",
                        "vector": [1.0, 0.0]
                    }],
                    "fusion": { "type": "weighted", "weights": [1.0] },
                    "top_k": 1
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
        InvalidProbe::AsOfGenZero | InvalidProbe::AsOfGenFuture => {
            let generation = if probe == InvalidProbe::AsOfGenZero {
                0
            } else {
                compact_generation(client, &server.base_url, ns).await + 10_000
            };
            let path = format!("/v1/namespaces/{ns}/query?as_of={generation}");
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}{}", server.base_url, path),
                Some(json!({
                    "sources": [{
                        "type": "ann",
                        "vector": [0.0, 0.0]
                    }],
                    "fusion": { "type": "none" },
                    "top_k": 1
                })),
            )
            .await;
            ("POST".to_string(), path, status, response)
        }
    }
}

async fn request_json(
    client: &Client,
    method: Method,
    url: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    let mut request = client.request(method, url);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request
        .send()
        .await
        .unwrap_or_else(|error| panic!("HTTP request failed for {url}: {error}"));
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        assert!(
            response.headers().contains_key("x-request-id"),
            "non-2xx response missing x-request-id header for {url}"
        );
    }
    if status == StatusCode::NO_CONTENT.as_u16() {
        return (status, serde_json::Value::Null);
    }
    let body = response
        .json::<serde_json::Value>()
        .await
        .unwrap_or_else(|error| panic!("HTTP response JSON parse failed for {url}: {error}"));
    (status, body)
}

async fn compact_generation(client: &Client, base_url: &str, ns: &str) -> u64 {
    compact_status(client, base_url, ns).await["manifest_generation"]
        .as_u64()
        .unwrap_or_else(|| panic!("compact/status missing manifest_generation for {ns}"))
}

async fn compact_status(client: &Client, base_url: &str, ns: &str) -> serde_json::Value {
    let url = format!("{base_url}/v1/namespaces/{ns}/compact/status");
    let response = client
        .get(&url)
        .send()
        .await
        .unwrap_or_else(|error| panic!("compact/status request failed for {ns}: {error}"));
    assert_eq!(
        response.status().as_u16(),
        200,
        "compact/status failed for {ns}"
    );
    response
        .json::<serde_json::Value>()
        .await
        .unwrap_or_else(|error| panic!("compact/status JSON parse failed for {ns}: {error}"))
}

async fn wait_compaction_ready(client: &Client, base_url: &str, ns: &str) -> serde_json::Value {
    for _ in 0..300 {
        let status = compact_status(client, base_url, ns).await;
        if status["ready"].as_bool().unwrap_or(false) {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("compact endpoint did not reach ready for {ns}");
}

async fn wait_namespace_gone(client: &Client, base_url: &str, ns: &str) {
    for _ in 0..300 {
        let response = client
            .get(format!("{base_url}/v1/namespaces/{ns}"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("namespace delete poll failed for {ns}: {error}"));
        match response.status() {
            StatusCode::NOT_FOUND => return,
            StatusCode::OK | StatusCode::GONE | StatusCode::ACCEPTED => {}
            status => panic!("unexpected namespace delete poll status for {ns}: {status}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("namespace {ns} did not reach 404 after delete");
}

async fn execute_sandwich_maintenance(
    client: &Client,
    server: &FullTestServer,
    ns: &str,
    maintenance: super::ops::MaintenanceKind,
) -> serde_json::Value {
    match maintenance {
        super::ops::MaintenanceKind::CompactInline => {
            let result = server
                .compactor
                .compact(ns)
                .await
                .unwrap_or_else(|error| panic!("sandwich compaction failed for {ns}: {error}"));
            json!({
                "kind": "compact_inline",
                "segment_id": result.segment_id,
                "vectors_compacted": result.vectors_compacted,
            })
        }
        super::ops::MaintenanceKind::CompactEndpoint => {
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}/v1/namespaces/{ns}/compact", server.base_url),
                None,
            )
            .await;
            let ready = if (200..300).contains(&status) {
                Some(wait_compaction_ready(client, &server.base_url, ns).await)
            } else {
                None
            };
            json!({ "kind": "compact_endpoint", "status": status, "response": response, "ready": ready })
        }
        super::ops::MaintenanceKind::GcCycle => {
            let config = gc_config(4);
            let report = gc::run_gc_cycle(&server.store, ns, &config)
                .await
                .unwrap_or_else(|error| panic!("sandwich gc failed for {ns}: {error}"));
            json!({
                "kind": "gc_cycle",
                "candidates_marked": report.candidates_marked,
                "objects_deleted": report.objects_deleted,
            })
        }
        super::ops::MaintenanceKind::Hydrate => {
            let (status, response) = request_json(
                client,
                Method::POST,
                &format!("{}/v1/namespaces/{ns}/hydrate", server.base_url),
                None,
            )
            .await;
            json!({ "kind": "hydrate", "status": status, "response": response })
        }
    }
}

fn gc_config(keep_count: u64) -> GcConfig {
    GcConfig {
        horizon_secs: 0,
        compaction_upload_window_secs: 0,
        skew_slop_secs: 0,
        allow_unsafe_short_horizon: true,
        manifest_history_keep_count: keep_count as usize,
        pitr_retention_secs: 0,
    }
}

#[allow(clippy::too_many_arguments)]
async fn quiesce_and_verify(
    client: &Client,
    server: &FullTestServer,
    artifacts: &mut SeedArtifacts,
    model: &mut Model,
    coverage: &mut Coverage,
    s3_tracker: &mut S3Tracker,
    namespaces: &[String],
    op_index: &mut u64,
    compactions: &mut u64,
    started: Instant,
    mutation: Option<OracleMutation>,
    mode: RunMode,
) -> Vec<Violation> {
    for ns in namespaces {
        let compact = Op::CompactInline { ns: ns.clone() };
        let step = execute_recorded_op(
            client, server, artifacts, model, coverage, s3_tracker, &compact, *op_index, started,
            mutation, mode,
        )
        .await;
        *op_index += 1;
        *compactions += u64::from((200..300).contains(&step.status));
        if !(200..300).contains(&step.status) {
            return vec![Violation {
                id: ViolationId::I16Quiescence,
                op_index: *op_index,
                namespace: ns.clone(),
                detail: "quiescence compaction failed".to_string(),
                evidence: serde_json::json!({ "status": step.status }),
            }];
        }
        if !step.violations.is_empty() {
            return step.violations;
        }

        for _ in 0..2 {
            let gc = Op::GcCycle {
                ns: ns.clone(),
                keep_count: 0,
            };
            let step = execute_recorded_op(
                client, server, artifacts, model, coverage, s3_tracker, &gc, *op_index, started,
                mutation, mode,
            )
            .await;
            *op_index += 1;
            if !step.violations.is_empty() {
                return step.violations;
            }
        }

        let status = compact_status(client, &server.base_url, ns).await;
        let expected_live = model
            .namespaces
            .get(ns)
            .map_or(0, |ns_model| ns_model.live.len());
        let mut violations = s3_oracle::check_quiescent_namespace(
            &server.store,
            ns,
            expected_live,
            &status,
            *op_index,
        )
        .await;
        violations.extend(
            if model
                .namespaces
                .get(ns)
                .is_some_and(|ns_model| ns_model.spec.is_exact())
            {
                s3_tracker
                    .check_namespace(
                        &server.store,
                        ns,
                        *op_index,
                        &status,
                        mutation == Some(OracleMutation::GcEatsLiveKey),
                    )
                    .await
            } else {
                Vec::new()
            },
        );
        if !violations.is_empty() {
            return violations;
        }

        let ids = model
            .namespaces
            .get(ns)
            .map(|ns_model| ns_model.live.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let fetch = Op::FetchVectors {
            ns: ns.clone(),
            ids,
            consistency: ConsistencyLevel::Strong,
        };
        let step = execute_recorded_op(
            client, server, artifacts, model, coverage, s3_tracker, &fetch, *op_index, started,
            mutation, mode,
        )
        .await;
        *op_index += 1;
        if !step.violations.is_empty() {
            return step.violations;
        }

        let q = exhaustive_query_from_model(model, ns);
        let query = Op::Query {
            ns: ns.clone(),
            q,
            as_of: None,
        };
        let step = execute_recorded_op(
            client, server, artifacts, model, coverage, s3_tracker, &query, *op_index, started,
            mutation, mode,
        )
        .await;
        *op_index += 1;
        if !step.violations.is_empty() {
            return step.violations;
        }
    }
    Vec::new()
}

fn exhaustive_query_from_model(model: &Model, ns: &str) -> GeneratedQuery {
    let ns_model = model
        .namespaces
        .get(ns)
        .unwrap_or_else(|| panic!("missing namespace model for quiescence query: {ns}"));
    let top_k = (ns_model.live.len() + ns_model.wal_tombstones.len()).max(1);
    let vector = ns_model.live.values().next().map_or_else(
        || vec![0.0f32; ns_model.spec.dims],
        |record| record.values.clone(),
    );
    let body = json!({
        "sources": [{
            "type": "ann",
            "vector": vector,
            "nprobe": ns_model.spec.num_centroids
        }],
        "fusion": { "type": "none" },
        "top_k": top_k,
        "candidate_k": top_k,
        "consistency": ConsistencyLevel::Strong,
        "include_attributes": true
    });
    let class = if ns_model.spec.is_exact() && ns_model.wal_tombstones.is_empty() {
        QueryOracleClass::ExactAnn {
            top_k,
            consistency: ConsistencyLevel::Strong,
            filter: None,
        }
    } else {
        QueryOracleClass::Membership {
            consistency: ConsistencyLevel::Strong,
        }
    };
    GeneratedQuery {
        body,
        class,
        pattern_tags: Vec::new(),
    }
}

fn selftest_probe_op(
    probe: OracleMutation,
    last_op: &Op,
    model: &Model,
    generator: &mut AdversarialGenerator,
) -> Option<Op> {
    match (probe, last_op) {
        (OracleMutation::DropDelete, Op::DeleteVectors { ns, .. })
        | (OracleMutation::PhantomId, Op::Upsert { ns, .. }) => {
            let ids = model.namespaces.get(ns)?.live.keys().cloned().collect();
            Some(Op::FetchVectors {
                ns: ns.clone(),
                ids,
                consistency: ConsistencyLevel::Strong,
            })
        }
        (OracleMutation::SkewScore, Op::Upsert { ns, .. }) => {
            let q = generator.exhaustive_query(model, ns, None);
            if matches!(&q.class, QueryOracleClass::ExactAnn { .. }) {
                Some(Op::Query {
                    ns: ns.clone(),
                    q,
                    as_of: None,
                })
            } else {
                None
            }
        }
        _ => None,
    }
}

fn deterministic_config() -> Config {
    let mut config = Config::load(None).expect("failed to load base Config");
    config.cache.manifest_cache_ttl_ms = 0;
    config.cache.namespace_registry_ttl_ms = 0;
    config.cache.hydration_enabled = false;
    config.compaction.max_wal_fragments_before_compact = 2;
    config.indexing.default_num_centroids = 4;
    config.indexing.default_nprobe = 4;
    config.indexing.max_nprobe = 64;
    config.gc.horizon_secs = 0;
    config.gc.skew_slop_secs = 0;
    config.gc.allow_unsafe_short_horizon = true;
    config.gc.manifest_history_keep_count = 8;
    config.gc.pitr_retention_secs = 0;
    config.server.rate_limit_rps = 1_000_000;
    config.server.rate_limit_burst = 1_000_000;
    config.server.write_rate_limit_rps = 1_000_000;
    config.server.write_rate_limit_burst = 1_000_000;
    config
}

fn should_cleanup(preserve: PreserveMode, failed: bool) -> bool {
    match preserve {
        PreserveMode::Always => false,
        PreserveMode::OnFailure => !failed,
        PreserveMode::Never => true,
    }
}
