use std::time::{Duration, Instant};

use reqwest::{Client, Method, StatusCode};
use serde_json::json;
use zeppelin::compaction::gc;
use zeppelin::config::{Config, GcConfig};
use zeppelin::types::ConsistencyLevel;
use zeppelin::wal::Manifest;

use crate::common::harness::TestHarness;
use crate::common::server::{cleanup_ns, start_test_server_full, FullTestServer};

use super::artifacts::{FailureManifest, RunArtifacts, SeedArtifacts};
use super::generator::{AdversarialGenerator, Coverage};
use super::model::{Model, OracleMutation};
use super::ops::{InvalidProbe, Op, OpRecord, QueryOracleClass};
use super::oracle::{self, Violation, ViolationId};
use super::s3_oracle::{self, S3Tracker};
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
                OracleMutation::LeakTombstone,
                OracleMutation::FilterSkew,
                OracleMutation::GcEatsLiveKey,
                OracleMutation::StaleCheckpoint,
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
    let mut s3_tracker = S3Tracker::default();
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
            &mut s3_tracker,
            &op,
            op_index,
            started,
            mutation,
        )
        .await;
        if matches!(op, Op::CreateNamespace { .. }) && (200..300).contains(&step.status) {
            created_namespaces.push(op.namespace().to_string());
        }
        if let Op::CloneNamespace { target, .. } = &op {
            if (200..300).contains(&step.status) {
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
            &mut s3_tracker,
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
    s3_tracker: &mut S3Tracker,
    op: &Op,
    index: u64,
    started: Instant,
    mutation: Option<OracleMutation>,
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
    let mut violations = oracle::check_op(model, &rec, RunMode::Deterministic, mutation);
    if (200..300).contains(&rec.status) {
        if let Op::CloneNamespace { target, .. } = op {
            violations.extend(
                s3_oracle::check_clone_manifest(&server.store, target, &rec.response, rec.index)
                    .await,
            );
        }
    }
    if rec.index % 25 == 0 {
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
    generator: &mut AdversarialGenerator,
    namespaces: &[String],
    op_index: &mut u64,
    compactions: &mut u64,
    started: Instant,
    mutation: Option<OracleMutation>,
) -> Vec<Violation> {
    for ns in namespaces {
        let compact = Op::CompactEndpoint { ns: ns.clone() };
        let step = execute_recorded_op(
            client, server, artifacts, model, coverage, s3_tracker, &compact, *op_index, started,
            mutation,
        )
        .await;
        *op_index += 1;
        *compactions += u64::from((200..300).contains(&step.status));
        if !step.violations.is_empty() {
            return step.violations;
        }

        for _ in 0..2 {
            let gc = Op::GcCycle {
                ns: ns.clone(),
                keep_count: 4,
            };
            let step = execute_recorded_op(
                client, server, artifacts, model, coverage, s3_tracker, &gc, *op_index, started,
                mutation,
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
            mutation,
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
            client, server, artifacts, model, coverage, s3_tracker, &query, *op_index, started,
            mutation,
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
