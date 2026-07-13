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

#[tokio::test]
#[ignore]
async fn smoke() {
    let env = adversarial::RunnerEnv::from_env();
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
        summary.seeds_run >= 3,
        "expected at least 3 seeds, ran {}",
        summary.seeds_run
    );
    assert!(
        summary.ops_total >= 200,
        "expected at least 200 ops, ran {}",
        summary.ops_total
    );
    assert!(
        summary.compactions_total >= 20,
        "expected at least 20 compactions, ran {}",
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
    let config = zeppelin::config::Config::load(None).unwrap();
    let mut server = common::server::start_test_server_full(
        harness.store.clone(),
        Some(prefix.clone()),
        config.clone(),
        false,
        None,
    )
    .await;
    let client = reqwest::Client::new();
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
    let mut replacement = common::server::start_test_server_full(
        harness.store.clone(),
        Some(prefix),
        config,
        false,
        None,
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

    let mut config = zeppelin::config::Config::load(None).unwrap();
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
