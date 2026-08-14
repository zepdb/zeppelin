mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeppelin::config::Config;
use zeppelin::security::{
    verify_audit_day, AuditClient, AuditRecord, AuditRuntime, AuditSinkError, SecurityKernel,
};
use zeppelin::storage::{ConditionalPutOutcome, ZeppelinStore};
use zeppelin::time::Clock;

use common::counting::counting_store;
use common::fault_injection::{
    fail_cas_etag_reconciliation_once_matching, pause_first_create_matching,
};
use common::harness::TestHarness;
use common::server::{scoped_test_security_store, test_security_runtime};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuditWriterHeadFixture {
    signer_node: String,
    stream_id: String,
    open_day: String,
    lease_owner: Option<String>,
    lease_expires_at: Option<DateTime<Utc>>,
}

fn writer_head_key(signer: &str) -> String {
    format!("_security/audit-writers/{signer}.json")
}

fn stream_signer(stream_id: &str) -> &str {
    stream_id
        .split_once('.')
        .map(|(signer, _)| signer)
        .expect("production audit stream must contain its signer prefix")
}

async fn install_real_storage_signer_on(
    harness: &TestHarness,
    additional_stores: &[&ZeppelinStore],
) -> Arc<SecurityKernel> {
    let mut config = Config::default();
    let key_file = tempfile::NamedTempFile::new().expect("unique test signer file must create");
    let seed = Sha256::digest(harness.prefix.as_bytes());
    let seed_hex = seed
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    std::fs::write(key_file.path(), seed_hex).expect("unique test signer seed must write");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(key_file.path(), std::fs::Permissions::from_mode(0o600))
            .expect("unique test signer seed permissions must restrict");
    }
    config.security.token_signing_key_path = key_file.path().to_string_lossy().into_owned();
    let security_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let (security, _credentials, _admin) =
        test_security_runtime(&security_store, &mut config, &Clock::system()).await;
    security
        .install_object_signer(&harness.store)
        .expect("test signer must install on real object storage");
    for store in additional_stores {
        security
            .install_object_signer(store)
            .expect("test signer must install on the fault-wrapped real object storage");
    }
    security
}

async fn install_real_storage_signer(harness: &TestHarness) -> Arc<SecurityKernel> {
    install_real_storage_signer_on(harness, &[]).await
}

async fn mutate_writer_head(
    store: &ZeppelinStore,
    signer: &str,
    mutate: impl FnOnce(&mut AuditWriterHeadFixture),
) {
    let key = writer_head_key(signer);
    let (body, metadata) = store
        .get_with_object_metadata(&key)
        .await
        .expect("authoritative writer head must read with metadata");
    let mut head: AuditWriterHeadFixture =
        serde_json::from_slice(&body).expect("authoritative writer head must decode");
    mutate(&mut head);
    let version = metadata
        .version
        .as_ref()
        .expect("authoritative writer head must carry a version token");
    let outcome = store
        .put_if_match_outcome(
            &key,
            Bytes::from(serde_json::to_vec(&head).expect("writer head must encode")),
            version,
        )
        .await
        .expect("writer head CAS mutation must execute");
    assert!(matches!(outcome, ConditionalPutOutcome::Updated { .. }));
}

async fn expire_writer_head(store: &ZeppelinStore, signer: &str) {
    mutate_writer_head(store, signer, |head| {
        head.lease_expires_at = Some(Utc::now() - chrono::Duration::seconds(1));
    })
    .await;
}

async fn wait_until_audit_writer_is_unhealthy(client: &AuditClient) {
    wait_until_audit_writer_is_unhealthy_within(client, Duration::from_secs(2)).await;
}

async fn wait_until_audit_writer_is_unhealthy_within(client: &AuditClient, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        while client.is_healthy() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fatal audit authority failure must make readiness observe an unhealthy writer");
}

#[tokio::test]
async fn lease_timer_reconciliation_failure_fails_health_before_expiry() {
    let harness = TestHarness::new().await;
    let writer_head_prefix = "_security/audit-writers/";
    let (fault_store, fault) =
        fail_cas_etag_reconciliation_once_matching(&harness.store, writer_head_prefix);
    let _security = install_real_storage_signer_on(&harness, &[&fault_store]).await;
    let (client, runtime) =
        AuditRuntime::start_for_published_signer(fault_store, Duration::from_secs(1))
            .await
            .expect("lease-timer reconciliation fixture must claim its head");
    let stream_id = client.node_id().to_string();
    let signer = stream_signer(&stream_id).to_string();

    fault.enable();
    wait_until_audit_writer_is_unhealthy_within(&client, Duration::from_secs(15)).await;
    assert_eq!(fault.etags_stripped(), 1);
    assert_eq!(fault.failures_injected(), 1);
    assert!(matches!(
        client.submit_buffered(AuditRecord::open_unsafe_boot(Utc::now(), &stream_id)),
        Err(AuditSinkError::WriterUnavailable)
    ));
    let shutdown_error = runtime
        .shutdown()
        .await
        .expect_err("fatal lease reconciliation loss must terminate the actor");
    assert!(matches!(
        shutdown_error,
        AuditSinkError::WriterAuthorityLost(_)
    ));

    cleanup_production_audit(&harness.store, &signer, &[stream_id]).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn local_writer_uses_explicit_application_clock_day() {
    let harness = TestHarness::new().await;
    let _security = install_real_storage_signer(&harness).await;
    let node_id = format!("test-node-{}-explicit-audit-day", harness.prefix);
    let timestamp = Utc
        .with_ymd_and_hms(2020, 1, 14, 12, 0, 0)
        .single()
        .expect("historical audit timestamp must exist");
    let day = timestamp.date_naive();
    let (client, runtime) = AuditRuntime::start_at(
        harness.store.clone(),
        node_id.clone(),
        Duration::from_secs(60),
        timestamp,
    )
    .await
    .expect("explicit application-clock audit day must start");

    client
        .submit_durable(AuditRecord::open_unsafe_boot(timestamp, &node_id))
        .await
        .expect("matching historical audit evidence must become durable");
    runtime
        .shutdown()
        .await
        .expect("historical audit day must seal and anchor");

    let verification = verify_audit_day(&harness.store, day, &node_id)
        .await
        .expect("historical audit day verification must execute");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 1);

    harness.cleanup().await;
}

#[tokio::test]
async fn production_writer_uses_explicit_application_clock_day() {
    let harness = TestHarness::new().await;
    let _security = install_real_storage_signer(&harness).await;
    let timestamp = Utc
        .with_ymd_and_hms(2020, 1, 15, 12, 0, 0)
        .single()
        .expect("historical production audit timestamp must exist");
    let day = timestamp.date_naive();
    let (client, runtime) = AuditRuntime::start_for_published_signer_at(
        harness.store.clone(),
        Duration::from_secs(60),
        timestamp,
    )
    .await
    .expect("production writer must use the application-selected audit day");
    let stream_id = client.node_id().to_string();
    let signer = stream_signer(&stream_id).to_string();

    client
        .submit_durable(AuditRecord::open_unsafe_boot(timestamp, &stream_id))
        .await
        .expect("matching historical production evidence must become durable");
    runtime
        .shutdown()
        .await
        .expect("historical production audit day must seal and anchor");

    let verification = verify_audit_day(&harness.store, day, &stream_id)
        .await
        .expect("historical production audit day verification must execute");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 1);

    cleanup_production_audit(&harness.store, &signer, &[stream_id]).await;
    harness.cleanup().await;
}

fn canonicalize_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonicalize_json).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonicalize_json(value)))
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        scalar => scalar,
    }
}

fn record_hash(record: &AuditRecord) -> String {
    let value = serde_json::to_value(record).expect("audit record must convert to JSON");
    let bytes = serde_json::to_vec(&canonicalize_json(value))
        .expect("audit record must encode canonically");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn cleanup_production_audit(store: &ZeppelinStore, signer: &str, stream_ids: &[String]) {
    let keys = store
        .list_prefix("_audit/")
        .await
        .expect("production audit cleanup inventory must list");
    for key in keys
        .into_iter()
        .filter(|key| stream_ids.iter().any(|stream| key.contains(stream)))
    {
        store
            .delete(&key)
            .await
            .expect("production audit object cleanup must succeed");
    }
    match store.delete(&writer_head_key(signer)).await {
        Ok(()) | Err(zeppelin::error::ZeppelinError::NotFound { .. }) => {}
        Err(error) => panic!("production audit head cleanup failed: {error}"),
    }
}

#[tokio::test]
async fn production_head_uses_real_storage_for_exclusion_and_empty_rotation() {
    let harness = TestHarness::new().await;
    let _security = install_real_storage_signer(&harness).await;
    let (client, runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("production writer must claim its real-storage head");
    let first_stream = client.node_id().to_string();
    let signer = stream_signer(&first_stream).to_string();
    let concurrent =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await;
    assert!(matches!(
        concurrent,
        Err(AuditSinkError::WriterAlreadyActive)
    ));
    runtime
        .shutdown()
        .await
        .expect("empty production stream must seal and anchor");

    let day = Utc::now().date_naive();
    let verification = verify_audit_day(&harness.store, day, &first_stream)
        .await
        .expect("empty production chain verification must execute");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 0);

    let (next, next_runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("clean same-day restart must rotate immediately");
    let next_stream = next.node_id().to_string();
    assert_ne!(next_stream, first_stream);
    next_runtime
        .shutdown()
        .await
        .expect("empty successor must seal and anchor");

    cleanup_production_audit(&harness.store, &signer, &[first_stream, next_stream]).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn production_head_resumes_a_crash_then_rotates_on_real_storage() {
    let harness = TestHarness::new().await;
    let _security = install_real_storage_signer(&harness).await;
    let (client, runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("production crash fixture must start");
    let crashed_stream = client.node_id().to_string();
    let signer = stream_signer(&crashed_stream).to_string();
    client
        .submit_durable(AuditRecord::open_unsafe_boot(Utc::now(), &crashed_stream))
        .await
        .expect("pre-crash record must become durable");
    drop(runtime);
    drop(client);
    expire_writer_head(&harness.store, &signer).await;

    let (recovered, recovered_runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("expired crash stream must recover from real storage");
    assert_eq!(recovered.node_id(), crashed_stream);
    recovered_runtime
        .shutdown()
        .await
        .expect("recovered stream must seal and anchor");
    let verification = verify_audit_day(&harness.store, Utc::now().date_naive(), &crashed_stream)
        .await
        .expect("recovered production chain verification must execute");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 1);

    let (next, next_runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("sealed crash stream must rotate");
    let next_stream = next.node_id().to_string();
    assert_ne!(next_stream, crashed_stream);
    next_runtime
        .shutdown()
        .await
        .expect("rotated successor must seal");

    cleanup_production_audit(&harness.store, &signer, &[crashed_stream, next_stream]).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn delayed_stale_batch_takeover_fails_successor_health_and_recovers_evidence() {
    let harness = TestHarness::new().await;
    let (stale_store, stale_pause) = pause_first_create_matching(&harness.store, "_audit/");
    let (successor_store, successor_pause) = pause_first_create_matching(&harness.store, "_audit/");
    let _security =
        install_real_storage_signer_on(&harness, &[&stale_store, &successor_store]).await;

    let (stale_client, stale_runtime) =
        AuditRuntime::start_for_published_signer(stale_store, Duration::from_secs(60))
            .await
            .expect("stale writer fixture must claim the production head");
    let stream_id = stale_client.node_id().to_string();
    let signer = stream_signer(&stream_id).to_string();
    let stale_record = AuditRecord::open_unsafe_boot(Utc::now(), &stream_id);
    let stale_submitter = stale_client.clone();
    let stale_submission =
        tokio::spawn(async move { stale_submitter.submit_durable(stale_record).await });
    stale_pause.wait_until_paused().await;

    expire_writer_head(&harness.store, &signer).await;
    let (successor, successor_runtime) =
        AuditRuntime::start_for_published_signer(successor_store, Duration::from_secs(60))
            .await
            .expect("successor must claim the expired production head");
    assert_eq!(successor.node_id(), stream_id);
    let successor_record = AuditRecord::open_unsafe_boot(Utc::now(), &stream_id);
    let successor_submitter = successor.clone();
    let first_successor_attempt = successor_record.clone();
    let successor_submission = tokio::spawn(async move {
        successor_submitter
            .submit_durable(first_successor_attempt)
            .await
    });
    successor_pause.wait_until_paused().await;

    stale_pause.release();
    stale_submission
        .await
        .expect("stale submission task must join")
        .expect("already-refreshed stale batch must win its in-flight create");
    successor_pause.release();
    let successor_error = successor_submission
        .await
        .expect("successor submission task must join")
        .expect_err("divergent deterministic slot must fail the successor submission");
    assert_eq!(successor_error, AuditSinkError::ImmutableObjectConflict);

    wait_until_audit_writer_is_unhealthy(&successor).await;
    assert!(matches!(
        successor.submit_buffered(AuditRecord::open_unsafe_boot(Utc::now(), &stream_id)),
        Err(AuditSinkError::WriterUnavailable)
    ));
    drop(stale_runtime);
    drop(stale_client);
    let shutdown_error = successor_runtime
        .shutdown()
        .await
        .expect_err("fatal slot conflict must terminate the successor actor");
    assert_eq!(shutdown_error, AuditSinkError::ImmutableObjectConflict);
    drop(successor);

    expire_writer_head(&harness.store, &signer).await;
    let (recovered, recovered_runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("restart must adopt the stale winner from authoritative storage");
    assert_eq!(recovered.node_id(), stream_id);
    recovered
        .submit_durable(successor_record)
        .await
        .expect("caller retry must preserve the successor evidence after recovery");
    recovered_runtime
        .shutdown()
        .await
        .expect("recovered two-record chain must seal and anchor");

    let verification = verify_audit_day(&harness.store, Utc::now().date_naive(), &stream_id)
        .await
        .expect("recovered stale-winner chain verification must execute");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 2);

    cleanup_production_audit(&harness.store, &signer, &[stream_id]).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn writer_head_loss_during_day_rollover_fails_health_immediately() {
    let harness = TestHarness::new().await;
    let _security = install_real_storage_signer(&harness).await;
    let (client, runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("day-rollover writer fixture must claim its head");
    let stream_id = client.node_id().to_string();
    let signer = stream_signer(&stream_id).to_string();
    mutate_writer_head(&harness.store, &signer, |head| {
        head.lease_owner = Some("replacement-audit-writer".to_string());
        head.lease_expires_at = Some(Utc::now() + chrono::Duration::minutes(5));
    })
    .await;

    let tomorrow = Utc::now().date_naive() + chrono::Duration::days(1);
    let rollover_record = AuditRecord::open_unsafe_boot(
        tomorrow
            .and_hms_opt(0, 0, 1)
            .expect("tomorrow rollover timestamp must exist")
            .and_utc(),
        &stream_id,
    );
    let error = client
        .submit_durable(rollover_record)
        .await
        .expect_err("stale head ETag must reject the UTC-day transition");
    assert!(matches!(error, AuditSinkError::WriterAuthorityLost(_)));
    wait_until_audit_writer_is_unhealthy(&client).await;
    let shutdown_error = runtime
        .shutdown()
        .await
        .expect_err("writer-head loss must terminate the actor");
    assert!(matches!(
        shutdown_error,
        AuditSinkError::WriterAuthorityLost(_)
    ));

    cleanup_production_audit(&harness.store, &signer, &[stream_id]).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn invalid_occupied_terminal_slot_fails_health_immediately() {
    let harness = TestHarness::new().await;
    let _security = install_real_storage_signer(&harness).await;
    let (client, runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("invalid-terminal writer fixture must claim its head");
    let stream_id = client.node_id().to_string();
    let signer = stream_signer(&stream_id).to_string();
    let today = Utc::now().date_naive();
    client
        .submit_durable(AuditRecord::open_unsafe_boot(Utc::now(), &stream_id))
        .await
        .expect("first chain record must become durable");

    let invalid_terminal_key = format!(
        "_audit/{}/{stream_id}/{}.jsonl",
        today.format("%Y-%m-%d"),
        ulid::Ulid::from(2_u128)
    );
    harness
        .store
        .put_create(
            &invalid_terminal_key,
            Bytes::from_static(b"not a valid audit chain continuation"),
        )
        .await
        .expect("invalid terminal fixture must occupy the deterministic next slot");

    let tomorrow = today + chrono::Duration::days(1);
    let rollover_record = AuditRecord::open_unsafe_boot(
        tomorrow
            .and_hms_opt(0, 0, 1)
            .expect("tomorrow rollover timestamp must exist")
            .and_utc(),
        &stream_id,
    );
    let error = client
        .submit_durable(rollover_record)
        .await
        .expect_err("invalid occupied terminal slot must fail the rollover");
    assert_eq!(error, AuditSinkError::ImmutableObjectConflict);
    wait_until_audit_writer_is_unhealthy(&client).await;
    let shutdown_error = runtime
        .shutdown()
        .await
        .expect_err("invalid terminal slot must terminate the actor");
    assert_eq!(shutdown_error, AuditSinkError::ImmutableObjectConflict);

    cleanup_production_audit(&harness.store, &signer, &[stream_id]).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn corrupt_target_day_tail_fails_health_instead_of_retrying_storage() {
    let harness = TestHarness::new().await;
    let _security = install_real_storage_signer(&harness).await;
    let (client, runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("corrupt-target-tail writer fixture must claim its head");
    let stream_id = client.node_id().to_string();
    let signer = stream_signer(&stream_id).to_string();
    let tomorrow = Utc::now().date_naive() + chrono::Duration::days(1);
    let corrupt_tail_key = format!(
        "_audit/{}/{stream_id}/{}.jsonl",
        tomorrow.format("%Y-%m-%d"),
        ulid::Ulid::from(1_u128)
    );
    harness
        .store
        .put_create(
            &corrupt_tail_key,
            Bytes::from_static(b"not a valid audit record or terminal seal"),
        )
        .await
        .expect("corrupt target-day tail fixture must publish");

    let rollover_record = AuditRecord::open_unsafe_boot(
        tomorrow
            .and_hms_opt(0, 0, 1)
            .expect("tomorrow rollover timestamp must exist")
            .and_utc(),
        &stream_id,
    );
    let error = client
        .submit_durable(rollover_record)
        .await
        .expect_err("corrupt target-day tail must fail before head publication");
    assert!(matches!(error, AuditSinkError::Serialization(_)));
    wait_until_audit_writer_is_unhealthy(&client).await;
    let shutdown_error = runtime
        .shutdown()
        .await
        .expect_err("corrupt target-day tail must terminate the actor");
    assert!(matches!(shutdown_error, AuditSinkError::Serialization(_)));

    cleanup_production_audit(&harness.store, &signer, &[stream_id]).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn production_head_recovers_across_midnight_on_real_storage() {
    let harness = TestHarness::new().await;
    let _security = install_real_storage_signer(&harness).await;
    let (client, runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("cross-midnight production fixture must start");
    let stream_id = client.node_id().to_string();
    let signer = stream_signer(&stream_id).to_string();
    drop(runtime);
    drop(client);
    let yesterday = Utc::now().date_naive() - chrono::Duration::days(1);
    mutate_writer_head(&harness.store, &signer, |head| {
        head.open_day = yesterday.format("%Y-%m-%d").to_string();
        head.lease_expires_at = Some(Utc::now() - chrono::Duration::seconds(1));
    })
    .await;

    let (yesterday_client, yesterday_runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("expired prior-day head must recover");
    assert_eq!(yesterday_client.node_id(), stream_id);
    let yesterday_ts = yesterday
        .and_hms_opt(23, 59, 59)
        .expect("prior-day timestamp must exist")
        .and_utc();
    yesterday_client
        .submit_durable(AuditRecord::open_unsafe_boot(yesterday_ts, &stream_id))
        .await
        .expect("prior-day record must become durable");
    drop(yesterday_runtime);
    drop(yesterday_client);
    expire_writer_head(&harness.store, &signer).await;

    let (recovered, recovered_runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("cross-midnight crash stream must recover");
    recovered
        .submit_durable(AuditRecord::open_unsafe_boot(Utc::now(), &stream_id))
        .await
        .expect("today record must seal yesterday before publication");
    recovered_runtime
        .shutdown()
        .await
        .expect("today chain must seal");

    for day in [yesterday, Utc::now().date_naive()] {
        let verification = verify_audit_day(&harness.store, day, &stream_id)
            .await
            .expect("cross-midnight verification must execute");
        assert!(verification.valid, "{day}: {verification:?}");
        assert_eq!(verification.verified_records, 1);
    }

    cleanup_production_audit(&harness.store, &signer, &[stream_id]).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn production_head_repairs_a_terminal_slot_without_an_anchor_on_real_storage() {
    let harness = TestHarness::new().await;
    let _security = install_real_storage_signer(&harness).await;
    let (client, runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("terminal-repair production fixture must start");
    let crashed_stream = client.node_id().to_string();
    let signer = stream_signer(&crashed_stream).to_string();
    let day = Utc::now().date_naive();
    client
        .submit_durable(AuditRecord::open_unsafe_boot(Utc::now(), &crashed_stream))
        .await
        .expect("terminal-repair record must become durable");
    drop(runtime);
    drop(client);

    let chain_prefix = format!("_audit/{}/{crashed_stream}/", day.format("%Y-%m-%d"));
    let keys = harness
        .store
        .list_prefix(&chain_prefix)
        .await
        .expect("crashed chain must list");
    assert_eq!(keys.len(), 1, "crash must leave exactly one record batch");
    let record_body = harness
        .store
        .get(&keys[0])
        .await
        .expect("crashed record batch must read");
    let record: AuditRecord =
        serde_json::from_slice(&record_body).expect("single record batch must decode");
    let seal_key = format!("{chain_prefix}{}.jsonl", ulid::Ulid::from(2_u128));
    let seal = serde_json::json!({
        "format": "zeppelin_audit_terminal_seal_v1",
        "day": day.format("%Y-%m-%d").to_string(),
        "node_id": crashed_stream,
        "last_hash": record_hash(&record),
        "record_count": 1
    });
    harness
        .store
        .put_create(
            &seal_key,
            Bytes::from(serde_json::to_vec(&seal).expect("terminal seal must encode")),
        )
        .await
        .expect("test crash must leave a terminal slot before its anchor");
    let late_batch = harness
        .store
        .put_create(&seal_key, record_body.clone())
        .await;
    assert!(
        late_batch.is_err(),
        "create-only terminal slot must reject a late batch at the same chain position"
    );
    let anchor_key = format!(
        "_audit/anchors/{}/{crashed_stream}.json",
        day.format("%Y-%m-%d")
    );
    assert!(matches!(
        harness.store.get(&anchor_key).await,
        Err(zeppelin::error::ZeppelinError::NotFound { .. })
    ));
    expire_writer_head(&harness.store, &signer).await;

    let (successor, successor_runtime) =
        AuditRuntime::start_for_published_signer(harness.store.clone(), Duration::from_secs(60))
            .await
            .expect("startup must repair the sealed tail and rotate");
    let successor_stream = successor.node_id().to_string();
    assert_ne!(successor_stream, crashed_stream);
    let verification = verify_audit_day(&harness.store, day, &crashed_stream)
        .await
        .expect("repaired terminal chain verification must execute");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 1);
    successor_runtime
        .shutdown()
        .await
        .expect("successor stream must seal");

    cleanup_production_audit(&harness.store, &signer, &[crashed_stream, successor_stream]).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn deterministic_batch_slot_rejects_divergent_writers_on_real_storage() {
    let harness = TestHarness::new().await;
    let _security = install_real_storage_signer(&harness).await;
    let node_id = format!("test-node-{}-audit-divergent", harness.prefix);
    let (left, left_runtime) = AuditRuntime::start(
        harness.store.clone(),
        node_id.clone(),
        Duration::from_secs(60),
    )
    .await
    .expect("left divergent writer must start from an empty tail");
    let (right, right_runtime) = AuditRuntime::start(
        harness.store.clone(),
        node_id.clone(),
        Duration::from_secs(60),
    )
    .await
    .expect("right divergent writer must start from the same empty tail");
    let mut left_record = AuditRecord::open_unsafe_boot(Utc::now(), &node_id);
    left_record.request_id = "left-divergent".to_string();
    let mut right_record = AuditRecord::open_unsafe_boot(Utc::now(), &node_id);
    right_record.request_id = "right-divergent".to_string();
    let (left_result, right_result) = tokio::join!(
        left.submit_durable(left_record),
        right.submit_durable(right_record)
    );
    assert_ne!(
        left_result.is_ok(),
        right_result.is_ok(),
        "exactly one divergent body may create the deterministic first-position slot: left={left_result:?} right={right_result:?}"
    );
    if left_result.is_ok() {
        left_runtime
            .shutdown()
            .await
            .expect("winning left writer must seal the chain");
        assert!(
            right_runtime.shutdown().await.is_err(),
            "losing right writer must fail loudly on its conflicting staged body"
        );
    } else {
        right_runtime
            .shutdown()
            .await
            .expect("winning right writer must seal the chain");
        assert!(
            left_runtime.shutdown().await.is_err(),
            "losing left writer must fail loudly on its conflicting staged body"
        );
    }
    let verification = verify_audit_day(&harness.store, Utc::now().date_naive(), &node_id)
        .await
        .expect("divergent-writer chain verification must execute");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 1);

    harness.cleanup().await;
}

#[tokio::test]
async fn writer_start_recovers_one_tail_object_before_accepting_records() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let mut config = Config::default();
    let security_store = scoped_test_security_store(&store, &harness.prefix);
    let (security, _credentials, _admin) =
        test_security_runtime(&security_store, &mut config, &Clock::system()).await;
    security
        .install_object_signer(&store)
        .expect("test audit signer must install on the counted store");

    let node_id = format!("test-node-{}-audit-recovery", harness.prefix);
    let (client, runtime) =
        AuditRuntime::start(store.clone(), node_id.clone(), Duration::from_secs(60))
            .await
            .expect("empty audit stream must recover before writer start");
    for index in 0..257 {
        let mut record = AuditRecord::open_unsafe_boot(Utc::now(), &node_id);
        record.request_id = format!("before-crash-{index}");
        client
            .submit_buffered(record)
            .expect("audit record must enqueue");
    }
    client.flush().await.expect("two audit batches must flush");
    drop(client);
    drop(runtime);

    let day = Utc::now().date_naive();
    let prefix = format!("_audit/{}/{node_id}/", day.format("%Y-%m-%d"));
    let mut keys = store
        .list_prefix(&prefix)
        .await
        .expect("audit objects must list");
    keys.sort();
    assert_eq!(keys.len(), 2, "257 records must create two batches");

    counter.reset();
    let (recovered_client, recovered_runtime) =
        AuditRuntime::start(store.clone(), node_id.clone(), Duration::from_secs(60))
            .await
            .expect("writer restart must recover the persisted tail before returning");
    let normalized_prefix = Path::parse(&prefix)
        .expect("audit LIST prefix must be a valid object path")
        .to_string();
    assert_eq!(counter.list_calls_for_prefix(&normalized_prefix), 1);
    assert_eq!(counter.gets_matching(&prefix), 1);
    assert_eq!(counter.gets_matching(&keys[0]), 0);
    assert_eq!(counter.gets_matching(&keys[1]), 1);

    let mut record = AuditRecord::open_unsafe_boot(Utc::now(), &node_id);
    record.request_id = "after-crash".to_string();
    recovered_client
        .submit_durable(record)
        .await
        .expect("recovered writer must extend the prior batch");
    recovered_runtime
        .shutdown()
        .await
        .expect("recovered stream must publish its anchor");

    let verification = verify_audit_day(&store, day, &node_id)
        .await
        .expect("recovered stream verification must execute");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 258);

    harness.cleanup().await;
}

#[tokio::test]
async fn interval_flush_and_shutdown_use_real_test_storage() {
    let harness = TestHarness::new().await;
    let mut config = Config::default();
    let security_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let (security, _credentials, _admin) =
        test_security_runtime(&security_store, &mut config, &Clock::system()).await;
    security
        .install_object_signer(&harness.store)
        .expect("test audit signer must install");

    let node_id = format!("test-node-{}-audit-interval", harness.prefix);
    let (client, runtime) = AuditRuntime::start(
        harness.store.clone(),
        node_id.clone(),
        Duration::from_millis(10),
    )
    .await
    .expect("signed audit runtime must recover before start");
    let record = AuditRecord::open_unsafe_boot(Utc::now(), &node_id);
    client
        .submit_buffered(record)
        .expect("buffered audit record must enqueue");
    let prefix = format!(
        "_audit/{}/{node_id}/",
        Utc::now().date_naive().format("%Y-%m-%d")
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let keys = harness
                .store
                .list_prefix(&prefix)
                .await
                .expect("audit prefix must list");
            if !keys.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("flush interval must publish the partial batch");
    runtime
        .shutdown()
        .await
        .expect("shutdown must sign the terminal anchor");

    harness.cleanup().await;
}

#[tokio::test]
async fn day_rollover_restarts_positioning_and_anchors_both_days() {
    let harness = TestHarness::new().await;
    let mut config = Config::default();
    let security_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let (security, _credentials, _admin) =
        test_security_runtime(&security_store, &mut config, &Clock::system()).await;
    security
        .install_object_signer(&harness.store)
        .expect("test audit signer must install");

    let node_id = format!("test-node-{}-audit-rollover", harness.prefix);
    let (client, runtime) = AuditRuntime::start(
        harness.store.clone(),
        node_id.clone(),
        Duration::from_secs(60),
    )
    .await
    .expect("signed audit runtime must start");
    let first_ts = Utc::now();
    let second_ts = first_ts + chrono::Duration::days(1);
    client
        .submit_durable(AuditRecord::open_unsafe_boot(first_ts, &node_id))
        .await
        .expect("first-day record must become durable");
    client
        .submit_durable(AuditRecord::open_unsafe_boot(second_ts, &node_id))
        .await
        .expect("second-day record must roll over the chain");
    runtime
        .shutdown()
        .await
        .expect("second day must receive a terminal anchor");

    for day in [first_ts.date_naive(), second_ts.date_naive()] {
        let verification = verify_audit_day(&harness.store, day, &node_id)
            .await
            .expect("day verification must execute");
        assert!(verification.valid, "{day}: {verification:?}");
        assert_eq!(verification.verified_records, 1);
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn durable_shutdown_fails_loudly_without_anchor_signer_on_real_test_storage() {
    let harness = TestHarness::new().await;
    let node_id = format!("test-node-{}-audit-unsigned", harness.prefix);
    let (client, runtime) = AuditRuntime::start(
        harness.store.clone(),
        node_id.clone(),
        Duration::from_secs(60),
    )
    .await
    .expect("an empty unsigned stream can start before its terminal obligation");
    client
        .submit_buffered(AuditRecord::open_unsafe_boot(Utc::now(), &node_id))
        .expect("unsigned record must enqueue before shutdown checks the signer");
    let result = runtime.shutdown().await;
    assert!(
        matches!(result, Err(ref error) if error.to_string().contains("signing capability is unavailable")),
        "unsigned terminal anchor must fail loudly: {result:?}"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn writer_start_rejects_legacy_tail_without_chain_position() {
    let harness = TestHarness::new().await;
    let node_id = format!("test-node-{}-audit-legacy", harness.prefix);
    let day = Utc::now().date_naive();
    let record = AuditRecord::open_unsafe_boot(Utc::now(), &node_id);
    let mut body = serde_json::to_vec(&record).expect("legacy record must encode");
    body.push(b'\n');
    let key = format!(
        "_audit/{}/{node_id}/01J00000000000000000000000.jsonl",
        day.format("%Y-%m-%d")
    );
    harness
        .store
        .put_create(&key, Bytes::from(body))
        .await
        .expect("legacy tail fixture must publish");

    let result = AuditRuntime::start(harness.store.clone(), node_id, Duration::from_secs(60)).await;
    assert!(
        matches!(result, Err(ref error) if error.to_string().contains("explicit offline migration")),
        "legacy tail must fail before a writer handle is returned: {result:?}"
    );

    harness.cleanup().await;
}
