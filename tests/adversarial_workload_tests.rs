mod adversarial;
mod common;

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
        for oracle in ["I22", "I23", "I24", "I25", "I26", "I27", "I28"] {
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

/// Deterministic deletion-unification smoke. Active branch setup uses the
/// explicitly feature-gated test-support seam because production fork
/// activation is not yet exposed through HTTP; both deletion operations
/// themselves execute through the real HTTP namespace DELETE handler.
#[cfg(feature = "branching-test-support")]
#[tokio::test]
#[ignore]
async fn branching_delete_smoke() {
    let env = adversarial::RunnerEnv::from_env();
    assert_eq!(
        env.mode,
        adversarial::RunMode::Deterministic,
        "branching deletion smoke requires deterministic mode"
    );
    assert!(
        env.profile.is_none(),
        "branching deletion smoke does not accept a chaos profile"
    );
    assert!(
        env.seeds.len() >= 2,
        "branching deletion smoke requires at least two pinned seeds"
    );

    let expected_seeds = u64::try_from(env.seeds.len())
        .unwrap_or_else(|_| panic!("branching deletion smoke seed count does not fit u64"));
    let expected_source_conflicts = env
        .seeds
        .iter()
        .copied()
        .map(adversarial::generator::BranchingDeleteSchedule::for_seed)
        .map(adversarial::generator::BranchingDeleteSchedule::expected_source_conflicts)
        .try_fold(0_u64, |total, count| total.checked_add(count))
        .unwrap_or_else(|| panic!("branching deletion conflict count overflowed"));
    let expected_source_deletes = env
        .seeds
        .iter()
        .copied()
        .map(adversarial::generator::BranchingDeleteSchedule::for_seed)
        .map(adversarial::generator::BranchingDeleteSchedule::expected_source_delete_ops)
        .try_fold(0_u64, |total, count| total.checked_add(count))
        .unwrap_or_else(|| panic!("branching source-delete count overflowed"));
    let summary = adversarial::branching::run_branching_delete_smoke(env)
        .await
        .unwrap_or_else(|error| panic!("branching deletion smoke failed: {error}"));
    assert_eq!(summary.seeds_run, expected_seeds);
    assert_eq!(summary.failed_seeds, 0);
    assert_eq!(summary.delete_branch_ops, expected_seeds * 2);
    assert_eq!(
        summary.delete_source_with_branches_ops,
        expected_source_deletes
    );
    assert_eq!(summary.expected_source_conflicts, expected_source_conflicts);
    println!(
        "branching deletion smoke: seeds={} delete_branch={} delete_source_with_branches={} expected_409s={}",
        summary.seeds_run,
        summary.delete_branch_ops,
        summary.delete_source_with_branches_ops,
        summary.expected_source_conflicts
    );
}

/// Stable replayable Phase 10 profile. The normal runner executes the full
/// fork/list/diverge/materialize/delete vocabulary and writes canonical
/// `ops.jsonl` artifacts that can be replayed with the existing replay test.
#[tokio::test]
#[ignore]
async fn branching_profile() {
    let env = adversarial::RunnerEnv::from_env();
    assert_eq!(
        env.profile,
        Some(adversarial::faults::FaultProfile::Branching),
        "branching profile requires ZEPPELIN_ADVERSARIAL_PROFILE=branching"
    );
    let summary = adversarial::runner::run_smoke(env).await;
    assert_eq!(
        summary.failed_seeds, 0,
        "branching profile reported failures"
    );
    for kind in [
        "fork_namespace",
        "list_branches",
        "compact_branch",
        "delete_branch",
        "delete_source_with_branches",
    ] {
        assert!(
            summary.coverage.op_counts.get(kind).copied().unwrap_or(0) > 0,
            "branching profile did not cover {kind}"
        );
    }
    assert!(
        summary
            .coverage
            .tag_counts
            .get(adversarial::generator::BRANCHING_PROFILE_TAG)
            .copied()
            .unwrap_or(0)
            >= 5,
        "branching profile must query both source and target across divergence"
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
async fn restartable_server_composes_replacement_after_crash_retirement() {
    let harness = common::harness::TestHarness::new().await;
    let prefix = harness.prefix.clone();
    let namespace = format!("{prefix}-restart");
    let config = zeppelin::config::Config::default();
    let server = common::server::start_test_server_full(
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

    // A replacement may only compose after the old node has joined every
    // child that can retain authority or renew a lease. `abort()` is a raw
    // listener escape hatch; this fixture exercises the retired crash boundary.
    server
        .abort_and_drop()
        .await
        .expect("restart fixture must join the old HTTP task");
    let replacement =
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

    replacement.shutdown().await;
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

    // The adversarial crash/restart path retains a clone while it replaces the
    // old server. That clone must not keep the old server's policy cache alive.
    let retained_application_store = server.store.clone();

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

    drop(retained_application_store);
}

/// An aborted server must release its policy-refresh cache even while crash
/// recovery retains its application-store clone for the replacement server.
#[tokio::test]
async fn full_server_crash_retirement_stops_policy_refresh_with_retained_application_store() {
    use std::time::Duration;

    let harness = common::harness::TestHarness::new().await;
    let prefix = harness.prefix.clone();
    let (store, counter) = common::counting::counting_store(&harness.store);
    let mut config = zeppelin::config::Config::default();
    config.security.policy_refresh_secs = 1;
    let server =
        common::server::start_test_server_full(store, Some(prefix.clone()), config, false, None)
            .await;
    let retained_application_store = server.store.clone();

    server
        .abort_and_drop()
        .await
        .expect("policy-refresh crash retirement must join its HTTP task");
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
        "policy refresh survived server abort and accessed deleted {policy_head}"
    );

    drop(retained_application_store);
}

/// Open a completed HTTP/1.1 keep-alive health exchange and retain its socket.
///
/// The returned socket keeps the server's accepted connection task alive until
/// crash retirement closes that connection. This makes the test exercise the
/// state that a listener-only abort used to leak.
async fn open_keep_alive_health_connection(base_url: &str) -> TcpStream {
    let address = base_url
        .strip_prefix("http://")
        .unwrap_or_else(|| panic!("test server URL is not HTTP: {base_url}"));
    let mut connection = TcpStream::connect(address)
        .await
        .unwrap_or_else(|error| panic!("keep-alive connection failed: {error}"));
    let request =
        format!("GET /healthz HTTP/1.1\r\nHost: {address}\r\nConnection: keep-alive\r\n\r\n");
    connection
        .write_all(request.as_bytes())
        .await
        .unwrap_or_else(|error| panic!("keep-alive health request write failed: {error}"));

    let mut response = Vec::new();
    let mut buffer = [0_u8; 1024];
    let header_end = loop {
        let read = tokio::time::timeout(Duration::from_secs(2), connection.read(&mut buffer))
            .await
            .expect("keep-alive health response timed out")
            .unwrap_or_else(|error| panic!("keep-alive health response read failed: {error}"));
        assert!(
            read > 0,
            "server closed keep-alive health request before a response"
        );
        response.extend_from_slice(&buffer[..read]);
        assert!(
            response.len() <= 16 * 1024,
            "keep-alive health response headers exceeded 16 KiB"
        );
        if let Some(position) = response.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = std::str::from_utf8(&response[..header_end])
        .expect("keep-alive health response headers must be UTF-8");
    assert!(
        headers.starts_with("HTTP/1.1 200 "),
        "keep-alive health request failed: {headers:?}"
    );
    assert!(
        !headers.lines().any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.eq_ignore_ascii_case("connection")
                    && value.trim().eq_ignore_ascii_case("close")
            })
        }),
        "server declined the requested keep-alive connection: {headers:?}"
    );
    let content_length = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| {
            value
                .trim()
                .parse::<usize>()
                .expect("keep-alive health content length must be a usize")
        })
        .expect("keep-alive health response must have Content-Length");
    while response.len() < header_end + content_length {
        let read = tokio::time::timeout(Duration::from_secs(2), connection.read(&mut buffer))
            .await
            .expect("keep-alive health body timed out")
            .unwrap_or_else(|error| panic!("keep-alive health body read failed: {error}"));
        assert!(
            read > 0,
            "server closed keep-alive health response before its body"
        );
        response.extend_from_slice(&buffer[..read]);
    }

    connection
}

async fn peer_closed_keep_alive_connection(connection: &mut TcpStream) -> bool {
    let mut byte = [0_u8; 1];
    matches!(
        tokio::time::timeout(Duration::from_secs(1), connection.read(&mut byte)).await,
        Ok(Ok(0))
    )
}

/// A simulated crash must retire all old connection-owned preservation state
/// before recovery composes a replacement over the retained application store.
#[tokio::test]
async fn full_server_abort_drops_preservation_refresh_before_replacement_composition() {
    const OLD_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
    const OBSERVATION_WINDOW: Duration = Duration::from_millis(2_300);
    const HOLD_EVENT_ID: &str = "crash-retirement-held-manifest-read";

    let harness = common::harness::TestHarness::new().await;
    let prefix = harness.prefix.clone();
    let (store, counter) = common::counting::counting_store(&harness.store);
    let scheduler =
        adversarial::faults::FaultScheduler::from_schedule(adversarial::faults::FaultSchedule {
            profile: adversarial::faults::FaultProfile::Sched,
            events: vec![adversarial::faults::FaultEvent {
                id: HOLD_EVENT_ID.to_string(),
                start_op: 1,
                end_op: None,
                boundary: adversarial::faults::Boundary::ObjectStore,
                target: adversarial::faults::TargetSelector {
                    store_op: Some(adversarial::chaos::StoreOp::Get),
                    key_substring: Some("manifest.json".to_string()),
                    path_substring: None,
                    methods: None,
                },
                kind: adversarial::faults::FaultKind::HoldCall { for_ops: 1_000 },
            }],
        });
    let mut old_config = zeppelin::config::Config::default();
    old_config.security.policy_refresh_secs = 1;
    let server = common::server::start_test_server_full(
        adversarial::faults::store_proxy::store_fault_proxy(&store, scheduler.clone()),
        Some(prefix.clone()),
        old_config,
        false,
        None,
    )
    .await;

    // This clone matches the crash/restart runner: storage survives the old
    // node, but the old node's authority and refresh work must not.
    let retained_application_store = server.store.clone();
    let mut old_connection = open_keep_alive_health_connection(&server.base_url).await;
    let old_client = common::server::client_with_bearer(&server.admin_bearer);
    let namespace = common::server::create_ns_api(&old_client, &server.base_url, 2).await;
    let _ = scheduler.advance_to(1);
    let held_url = format!("{}/v1/namespaces/{namespace}", server.base_url);
    let held_scheduler = scheduler.clone();
    let held_request = tokio::spawn(async move {
        held_scheduler
            .with_armed_hold(HOLD_EVENT_ID.to_string(), async move {
                old_client.get(held_url).send().await
            })
            .await
    });
    scheduler
        .wait_for_hold_window_active(HOLD_EVENT_ID, 1)
        .await;

    let mut retirement = tokio::spawn(async move {
        server
            .abort_and_drop()
            .await
            .expect("held-request crash retirement must join its HTTP task");
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut retirement)
            .await
            .is_err(),
        "crash retirement must remain pending while an accepted old request owns AppState"
    );
    assert!(
        !retirement.is_finished(),
        "crash retirement must not finish before its accepted request is released"
    );

    let mut replacement_config = zeppelin::config::Config::default();
    replacement_config.security.policy_refresh_secs = 60;
    scheduler.release_held_calls();
    let _ = tokio::time::timeout(Duration::from_secs(2), held_request)
        .await
        .expect("held request did not finish after release")
        .expect("held request task panicked");
    tokio::time::timeout(Duration::from_secs(2), retirement)
        .await
        .expect("crash retirement did not join accepted HTTP connections")
        .expect("crash retirement task must not panic");
    let replacement = common::server::start_test_server_full(
        retained_application_store,
        Some(prefix.clone()),
        replacement_config,
        false,
        None,
    )
    .await;
    counter.reset();
    tokio::time::sleep(OBSERVATION_WINDOW).await;

    let replacement_health = reqwest::Client::new()
        .get(format!("{}/healthz", replacement.base_url))
        .send()
        .await
        .unwrap_or_else(|error| panic!("replacement health request failed: {error}"));
    assert!(replacement_health.status().is_success());

    let preservation_head = format!("{prefix}/_security/preservation/heads/locks.json");
    let stale_refreshes = counter.gets_matching(&preservation_head);
    let old_connection_closed = peer_closed_keep_alive_connection(&mut old_connection).await;
    drop(old_connection);
    replacement.shutdown().await;
    harness.cleanup().await;

    assert!(
        old_connection_closed,
        "crash retirement returned with the old keep-alive connection open; \
         observed {stale_refreshes} preservation refresh GET(s) over at least two \
         {OLD_REFRESH_INTERVAL:?} intervals"
    );
    assert_eq!(
        stale_refreshes, 0,
        "old preservation refresh survived while replacement was live for at least two \
         {OLD_REFRESH_INTERVAL:?} intervals"
    );
}

#[tokio::test]
async fn wall_clock_jump_does_not_expire_compaction_upload_window() {
    use std::sync::Arc;
    use std::time::Duration;

    use zeppelin::time::{Clock, TimeSource};

    let harness = common::harness::TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("clock-upload-window");
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
