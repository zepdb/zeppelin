use std::time::{Duration, Instant};

use reqwest::{Client, Method, StatusCode};
use serde_json::json;
use zeppelin::config::Config;
use zeppelin::types::ConsistencyLevel;

use crate::common::harness::TestHarness;
use crate::common::server::{cleanup_ns, start_test_server_full, FullTestServer};

use super::artifacts::{FailureManifest, RunArtifacts, SeedArtifacts};
use super::generator::{AdversarialGenerator, Coverage};
use super::model::{Model, OracleMutation};
use super::ops::{Op, OpRecord, QueryOracleClass};
use super::oracle::{self, Violation, ViolationId};
use super::{PreserveMode, RunMode, RunnerEnv};

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub seeds_run: u64,
    pub failed_seeds: u64,
    pub ops_total: u64,
    pub compactions_total: u64,
    pub ops_per_sec: f64,
    pub coverage: Coverage,
}

#[derive(Debug)]
struct SeedOutcome {
    failed: bool,
    ops: u64,
    compactions: u64,
    coverage: Coverage,
    violations: Vec<Violation>,
}

pub async fn run_smoke(env: RunnerEnv) -> RunSummary {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(env.seconds);
    let artifacts = RunArtifacts::create(&env);
    let mut summary = RunSummary {
        seeds_run: 0,
        failed_seeds: 0,
        ops_total: 0,
        compactions_total: 0,
        ops_per_sec: 0.0,
        coverage: Coverage::default(),
    };

    for seed in &env.seeds {
        let outcome = run_seed(&env, &artifacts, *seed, deadline, None, None).await;
        summary.seeds_run += 1;
        summary.failed_seeds += u64::from(outcome.failed);
        summary.ops_total += outcome.ops;
        summary.compactions_total += outcome.compactions;
        summary.coverage.merge(&outcome.coverage);
    }
    summary.ops_per_sec = summary.ops_total as f64 / started.elapsed().as_secs_f64().max(0.001);
    summary
}

pub async fn run_oracle_selftest(env: RunnerEnv) {
    let mutations = env.selftest.map_or_else(
        || {
            vec![
                OracleMutation::DropDelete,
                OracleMutation::SkewScore,
                OracleMutation::PhantomId,
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
    let harness = TestHarness::new().await;
    let prefix = harness.prefix.clone();
    let store = harness.store.clone();
    let config = deterministic_config();
    let mut generator = AdversarialGenerator::new(seed, &prefix);
    let specs = generator.specs();
    let mut artifacts = artifacts.seed(seed, &config, &specs, env.mode);
    let server =
        start_test_server_full(store.clone(), Some(prefix.clone()), config.clone(), false).await;
    let client = Client::new();
    let mut model = Model::default();
    let mut coverage = Coverage::default();
    let mut created_namespaces = Vec::new();
    let mut op_index = 0u64;
    let mut failed = false;
    let mut failure_violations = Vec::new();
    let mut compactions = 0u64;
    let started = Instant::now();
    let max_ops = env.max_ops.unwrap_or(100);

    while op_index < max_ops && (Instant::now() < deadline || op_index == 0) {
        let op = generator.next(&model);
        let step = execute_recorded_op(
            &client,
            &server,
            &mut artifacts,
            &mut model,
            &mut coverage,
            &op,
            op_index,
            started,
            mutation,
        )
        .await;
        if matches!(op, Op::CreateNamespace { .. }) && (200..300).contains(&step.status) {
            created_namespaces.push(op.namespace().to_string());
        }
        if matches!(op, Op::CompactInline { .. }) && (200..300).contains(&step.status) {
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
                let step = execute_recorded_op(
                    &client,
                    &server,
                    &mut artifacts,
                    &mut model,
                    &mut coverage,
                    &probe_op,
                    op_index,
                    started,
                    mutation,
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

    if !failed {
        let quiescence = quiesce_and_verify(
            &client,
            &server,
            &mut artifacts,
            &mut model,
            &mut coverage,
            &mut generator,
            &created_namespaces,
            &mut op_index,
            &mut compactions,
            started,
            mutation,
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

    if failed {
        artifacts.write_failure(&FailureManifest {
            seed,
            mode: env.mode,
            op_index,
            violations: failure_violations.clone(),
            preserved_prefix: prefix.clone(),
            repro_cmd: format!(
                "TEST_BACKEND={} ZEPPELIN_ADVERSARIAL_SEED={} cargo test --test adversarial_workload_tests smoke -- --ignored --nocapture",
                env.env_echo
                    .get("TEST_BACKEND")
                    .map(String::as_str)
                    .unwrap_or("memory"),
                seed
            ),
            inspect_cmd: format!(
                "ZEPPELIN_ADVERSARIAL_INSPECT={} cargo test --test adversarial_workload_tests inspect -- --ignored --nocapture",
                prefix
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
        shutdown
            .send(true)
            .expect("failed to signal compaction loop shutdown");
    }

    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    println!(
        "seed {}: failed={} ops={} compactions={} ops/sec={:.2}",
        seed,
        failed,
        op_index,
        compactions,
        op_index as f64 / elapsed
    );

    SeedOutcome {
        failed,
        ops: op_index,
        compactions,
        coverage,
        violations: failure_violations,
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
    op: &Op,
    index: u64,
    started: Instant,
    mutation: Option<OracleMutation>,
) -> StepOutcome {
    let mut rec = execute_op(client, server, op, index, started).await;
    if op.is_mutating() && (200..300).contains(&rec.status) {
        rec.gen_after = Some(compact_generation(client, &server.base_url, op.namespace()).await);
    }
    coverage.record(op);
    artifacts.write_op(&rec);
    model.apply(op, rec.status, rec.gen_after, mutation);
    let violations = oracle::check_op(model, &rec, RunMode::Deterministic, mutation);
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
        Op::CompactInline { ns } => {
            let result = server
                .compactor
                .compact(ns)
                .await
                .unwrap_or_else(|error| panic!("inline compaction failed for {ns}: {error}"));
            (
                "IN_PROCESS".to_string(),
                format!("compactor.compact({ns})"),
                StatusCode::OK.as_u16(),
                json!({
                    "segment_id": result.segment_id,
                    "vectors_compacted": result.vectors_compacted,
                    "fragments_removed": result.fragments_removed,
                    "old_segment_removed": result.old_segment_removed,
                }),
            )
        }
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

#[allow(clippy::too_many_arguments)]
async fn quiesce_and_verify(
    client: &Client,
    server: &FullTestServer,
    artifacts: &mut SeedArtifacts,
    model: &mut Model,
    coverage: &mut Coverage,
    generator: &mut AdversarialGenerator,
    namespaces: &[String],
    op_index: &mut u64,
    compactions: &mut u64,
    started: Instant,
    mutation: Option<OracleMutation>,
) -> Vec<Violation> {
    for ns in namespaces {
        for _ in 0..20 {
            let status = compact_status(client, &server.base_url, ns).await;
            let ready = status["ready"].as_bool().unwrap_or(false);
            let uncompacted = status["uncompacted_fragments"].as_u64().unwrap_or(u64::MAX);
            if ready && uncompacted == 0 {
                break;
            }
            let op = Op::CompactInline { ns: ns.clone() };
            let step = execute_recorded_op(
                client, server, artifacts, model, coverage, &op, *op_index, started, mutation,
            )
            .await;
            *op_index += 1;
            *compactions += 1;
            if !step.violations.is_empty() {
                return step.violations;
            }
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
            client, server, artifacts, model, coverage, &fetch, *op_index, started, mutation,
        )
        .await;
        *op_index += 1;
        if !step.violations.is_empty() {
            return step.violations;
        }

        let q = generator.exhaustive_query(model, ns, None);
        let query = Op::Query {
            ns: ns.clone(),
            q,
            as_of: None,
        };
        let step = execute_recorded_op(
            client, server, artifacts, model, coverage, &query, *op_index, started, mutation,
        )
        .await;
        *op_index += 1;
        if !step.violations.is_empty() {
            return step.violations;
        }
    }
    Vec::new()
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
