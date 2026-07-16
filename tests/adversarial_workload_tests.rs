mod adversarial;
mod common;

const REQUIRED_OP_KINDS: &[&str] = &[
    "create_namespace",
    "get_namespace",
    "upsert",
    "delete_vectors",
    "fetch_vectors",
    "query",
    "batch_query",
    "paginate_all",
    "invalid_probe",
    "compact_endpoint",
    "gc_cycle",
    "create_snapshot",
    "get_snapshot",
    "list_snapshots",
    "delete_snapshot",
    "clone_namespace",
    "patch_index_config",
    "hydrate",
    "delete_namespace",
    "probe_sandwich",
    "compact_inline",
];

const REQUIRED_TAGS: &[&str] = &[
    "delete-then-reupsert",
    "eventual-tombstone",
    "eventual",
    "batch",
    "pagination",
    "fts",
    "invalid-probe",
    "as-of-200",
    "as-of-410",
    "snapshot",
    "clone",
    "gc-cycle",
    "sandwich",
    "delete-recreate",
    "sketch-adc-v4",
];

const REQUIRED_SECURITY_OP_KINDS: &[&str] = &[
    "create_key",
    "rotate_key",
    "revoke_key",
    "publish_grant_change",
    "tenant_boundary_probe",
    "use_revoked_credential",
    "forbidden_write_probe",
    "export_probe",
    "security_admin_probe",
    "audit_barrier",
    "query_with_receipt",
    "verify_receipt",
    "tamper_artifact_then_verify",
    "audit_chain_check",
    "mint_token",
    "use_token",
    "token_exceed_scope_probe",
    "use_expired_token",
    "revoke_parent_then_use_token",
    "create_lock",
    "release_lock",
    "delete_under_lock",
    "gc_under_lock",
];

fn operation_coverage_required(mode: adversarial::RunMode, kind: &str) -> bool {
    mode != adversarial::RunMode::Chaos
        || !matches!(
            kind,
            "compact_endpoint" | "gc_cycle" | "probe_sandwich" | "compact_inline"
        )
}

fn tag_coverage_required(mode: adversarial::RunMode, tag: &str) -> bool {
    mode != adversarial::RunMode::Chaos || !matches!(tag, "gc-cycle" | "sandwich")
}

fn minimum_explicit_compactions(security_profile: bool, seed_count: u64) -> u64 {
    if security_profile {
        // The security profile runs in chaos mode, where foreground maintenance
        // operations are deliberately sanitized. Require the quiet-period proof
        // from every seed instead of the mixed-profile foreground floor.
        seed_count
    } else {
        20
    }
}

#[tokio::test]
#[ignore]
async fn smoke() {
    let env = adversarial::RunnerEnv::from_env();
    let security_profile = env.profile == Some(adversarial::faults::FaultProfile::Security);
    let configured_seed_count = env.seeds.len() as u64;
    let mode = if env.profile.is_some() {
        adversarial::RunMode::Chaos
    } else {
        env.mode
    };
    let summary = adversarial::runner::run_smoke(env).await;

    assert!(
        summary.failed_seeds == 0,
        "adversarial smoke had {} failed seed(s)",
        summary.failed_seeds
    );
    assert!(
        configured_seed_count >= 2,
        "adversarial smoke requires at least 2 configured seeds, got {configured_seed_count}"
    );
    assert_eq!(
        summary.seeds_run, configured_seed_count,
        "runner did not execute every configured smoke seed"
    );
    assert!(
        summary.ops_total >= 200,
        "expected at least 200 ops, ran {}",
        summary.ops_total
    );
    let required_compactions =
        minimum_explicit_compactions(security_profile, configured_seed_count);
    assert!(
        summary.compactions_total >= required_compactions,
        "expected at least {required_compactions} compactions, ran {}",
        summary.compactions_total
    );
    assert!(
        summary
            .coverage
            .tag_counts
            .get("delete-then-reupsert")
            .copied()
            .unwrap_or(0)
            > 0,
        "delete-then-reupsert scenario tag was not covered"
    );
    for kind in REQUIRED_OP_KINDS
        .iter()
        .copied()
        .filter(|kind| operation_coverage_required(mode, kind))
    {
        assert!(
            summary.coverage.op_counts.get(kind).copied().unwrap_or(0) > 0,
            "operation kind {kind} was not covered"
        );
    }
    for tag in REQUIRED_TAGS
        .iter()
        .copied()
        .filter(|tag| tag_coverage_required(mode, tag))
    {
        assert!(
            summary.coverage.tag_counts.get(tag).copied().unwrap_or(0) > 0,
            "scenario tag {tag} was not covered"
        );
    }
    if security_profile {
        for kind in REQUIRED_SECURITY_OP_KINDS {
            assert!(
                summary.coverage.op_counts.get(*kind).copied().unwrap_or(0) > 0,
                "security operation kind {kind} was not covered"
            );
        }
        for oracle in ["I22", "I23", "I24", "I25", "I26", "I27", "I28", "I29"] {
            assert!(
                summary
                    .coverage
                    .security_oracle_counts
                    .get(oracle)
                    .copied()
                    .unwrap_or(0)
                    > 0,
                "security oracle scenario {oracle} was not covered"
            );
        }
    }
    assert!(
        summary
            .coverage
            .tag_counts
            .get("sketch-adc-v4")
            .copied()
            .unwrap_or(0)
            >= summary.seeds_run * 2,
        "sketch-adc-v4 scenario tag must run twice per seed"
    );

    println!(
        "adversarial smoke: seeds={} ops={} compactions={} background_compactions={} failed={} non_blocking_findings={} ops/sec={:.2}",
        summary.seeds_run,
        summary.ops_total,
        summary.compactions_total,
        summary.background_compactions_total,
        summary.failed_seeds,
        summary.non_blocking_findings,
        summary.ops_per_sec
    );
}

#[test]
fn smoke_coverage_contract_is_mode_aware() {
    for mode in [
        adversarial::RunMode::Deterministic,
        adversarial::RunMode::Mixed,
    ] {
        for kind in REQUIRED_OP_KINDS {
            assert!(operation_coverage_required(mode, kind));
        }
        for tag in REQUIRED_TAGS {
            assert!(tag_coverage_required(mode, tag));
        }
    }

    for kind in [
        "compact_endpoint",
        "gc_cycle",
        "probe_sandwich",
        "compact_inline",
    ] {
        assert!(!operation_coverage_required(
            adversarial::RunMode::Chaos,
            kind
        ));
    }
    for tag in ["gc-cycle", "sandwich"] {
        assert!(!tag_coverage_required(adversarial::RunMode::Chaos, tag));
    }

    assert!(operation_coverage_required(
        adversarial::RunMode::Chaos,
        "upsert"
    ));
    assert!(tag_coverage_required(
        adversarial::RunMode::Chaos,
        "delete-then-reupsert"
    ));
}

#[test]
fn security_smoke_requires_quiet_period_compaction_per_seed() {
    assert_eq!(minimum_explicit_compactions(false, 2), 20);
    assert_eq!(minimum_explicit_compactions(true, 2), 2);
}

#[tokio::test]
#[ignore]
async fn oracle_selftest() {
    let env = adversarial::RunnerEnv::from_env();
    adversarial::runner::run_oracle_selftest(env).await;
}

#[tokio::test]
#[ignore]
async fn replay_seed() {
    adversarial::runner::replay_seed_from_env().await;
}

#[tokio::test]
#[ignore]
async fn inspect() {
    adversarial::runner::inspect_from_env().await;
}

#[tokio::test]
#[ignore]
async fn overnight() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let env = adversarial::RunnerEnv::from_env();
    let summary = adversarial::runner::run_overnight(env).await;
    println!(
        "adversarial overnight: seeds={} ops={} compactions={} background_compactions={} failed={} ops/sec={:.2}",
        summary.seeds_run,
        summary.ops_total,
        summary.compactions_total,
        summary.background_compactions_total,
        summary.failed_seeds,
        summary.ops_per_sec
    );
}

#[tokio::test]
#[ignore]
async fn crash_matrix() {
    adversarial::faults::store_proxy::run_crash_matrix().await;
}

#[tokio::test]
async fn restartable_server_exposes_hard_abort() {
    let harness = common::harness::TestHarness::new().await;
    let prefix = harness.prefix.clone();
    let namespace = format!("{prefix}-restart");
    let config = zeppelin::config::Config::default();
    let mut server = common::server::start_test_server_full(
        harness.store.clone(),
        Some(prefix.clone()),
        config.clone(),
        false,
        None,
    )
    .await;
    let admin_bearer = server.admin_bearer.clone();
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let create = client
        .post(format!("{}/v1/namespaces", server.base_url))
        .json(&serde_json::json!({
            "name": namespace,
            "dimensions": 2,
            "distance_metric": "cosine",
            "index_config": {
                "nlist": 4,
                "quantization": "none",
                "pq_m": 1,
                "hierarchical": false,
                "fts_index": false,
                "bitmap_index": false
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(create.status(), 201);
    let upsert = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&serde_json::json!({
            "vectors": [{ "id": "survivor", "values": [1.0, 0.0] }]
        }))
        .send()
        .await
        .unwrap();
    assert!(upsert.status().is_success());

    server.abort();
    drop(server);
    let mut replacement =
        common::server::start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
            harness.store.clone(),
            Some(prefix),
            config,
            false,
            None,
            100 * 1024 * 1024,
            &admin_bearer,
        )
        .await;
    let fetched = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors/get",
            replacement.base_url
        ))
        .json(&serde_json::json!({
            "ids": ["survivor"],
            "include_vector": true,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert!(fetched.status().is_success());
    let body = fetched.json::<serde_json::Value>().await.unwrap();
    assert_eq!(body["results"][0]["id"], "survivor");

    replacement.abort();
    common::server::cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
}

/// A completed full server must not retain policy-refresh work after its
/// harness deletes the scoped security objects.
#[tokio::test]
async fn full_server_shutdown_stops_policy_refresh_before_harness_cleanup() {
    use std::time::Duration;

    let harness = common::harness::TestHarness::new().await;
    let prefix = harness.prefix.clone();
    let (store, counter) = common::counting::counting_store(&harness.store);
    let mut config = zeppelin::config::Config::default();
    config.security.policy_refresh_secs = 1;
    let server =
        common::server::start_test_server_full(store, Some(prefix.clone()), config, false, None)
            .await;

    server.shutdown().await;
    harness.cleanup().await;
    counter.reset();

    let policy_head = format!("{prefix}/_security/heads/policy.json");
    let refresh_after_cleanup = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if counter.gets_matching(&policy_head) > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    assert!(
        refresh_after_cleanup.is_err(),
        "policy refresh survived server shutdown and accessed deleted {policy_head}"
    );
}

#[tokio::test]
async fn wall_clock_jump_does_not_expire_compaction_upload_window() {
    use std::sync::Arc;
    use std::time::Duration;

    use zeppelin::time::{Clock, TimeSource};

    let harness = common::harness::TestHarness::new().await;
    let namespace = harness.key("clock-upload-window");
    let test_clock = Arc::new(adversarial::faults::clock::TestClock::default());
    let source: Arc<dyn TimeSource> = test_clock.clone();
    let clock = Clock::from_source(source);
    common::write_active_namespace_metadata(
        &harness.store,
        &namespace,
        4,
        zeppelin::types::DistanceMetric::Euclidean,
    )
    .await;
    let mut manifest = zeppelin::wal::Manifest::new_at(clock.now());
    manifest.write(&harness.store, &namespace).await.unwrap();
    let writer = zeppelin::wal::WalWriter::with_clock(harness.store.clone(), clock.clone());
    writer
        .append(
            &namespace,
            common::vectors::random_vectors(8, 4),
            Vec::new(),
        )
        .await
        .unwrap();

    let mut config = zeppelin::config::Config::default();
    config.indexing.default_num_centroids = 4;
    config.indexing.default_nprobe = 4;
    let mut compactor = zeppelin::compaction::Compactor::with_clock(
        harness.store.clone(),
        zeppelin::wal::WalReader::new(harness.store.clone()),
        config.compaction,
        config.indexing,
        Duration::from_secs(2),
        clock,
    );
    compactor.set_test_pre_cas_delay(Duration::from_millis(100));
    let compactor = Arc::new(compactor);
    let ns = namespace.clone();
    let task = tokio::spawn(async move { compactor.compact(&ns).await });

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!task.is_finished(), "compaction finished before clock jump");
    test_clock.jump(60 * 60 * 1_000);
    let result = task.await.unwrap().unwrap();
    assert_eq!(result.vectors_compacted, 8);

    common::server::cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
}
