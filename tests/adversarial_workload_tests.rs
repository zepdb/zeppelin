mod adversarial;
mod common;

#[tokio::test]
#[ignore]
async fn smoke() {
    let env = adversarial::RunnerEnv::from_env();
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
    for kind in [
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
    ] {
        assert!(
            summary.coverage.op_counts.get(kind).copied().unwrap_or(0) > 0,
            "operation kind {kind} was not covered"
        );
    }
    for tag in [
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
    ] {
        assert!(
            summary.coverage.tag_counts.get(tag).copied().unwrap_or(0) > 0,
            "scenario tag {tag} was not covered"
        );
    }

    println!(
        "adversarial smoke: seeds={} ops={} compactions={} failed={} ops/sec={:.2}",
        summary.seeds_run,
        summary.ops_total,
        summary.compactions_total,
        summary.failed_seeds,
        summary.ops_per_sec
    );
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
    let env = adversarial::RunnerEnv::from_env();
    let summary = adversarial::runner::run_overnight(env).await;
    println!(
        "adversarial overnight: seeds={} ops={} compactions={} failed={} ops/sec={:.2}",
        summary.seeds_run,
        summary.ops_total,
        summary.compactions_total,
        summary.failed_seeds,
        summary.ops_per_sec
    );
}
