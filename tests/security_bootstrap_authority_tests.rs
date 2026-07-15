mod common;

use std::str::FromStr;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use zeppelin::config::Config;
use zeppelin::error::ZeppelinError;
use zeppelin::security::{PolicyStore, SecurityError};
use zeppelin::storage::ZeppelinStore;

use common::fault_injection::synchronize_create_pair_matching;
use common::harness::TestHarness;

const BOOTSTRAP_CONFIG: &str = r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_bootstrap"
name = "bootstrap-admin"
sha256_hex = "0000000000000000000000000000000000000000000000000000000000000000"
actions = ["*"]
namespaces = ["*"]
"#;

const DRIFTED_DIGEST: &str = "2222222222222222222222222222222222222222222222222222222222222222";

#[derive(Clone)]
struct CapturedLogWriter(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|_| panic!("captured log buffer lock poisoned"))
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn scoped_store(harness: &TestHarness) -> ZeppelinStore {
    let backend = PrefixStore::new(harness.store.inner(), Path::from(harness.prefix.clone()));
    ZeppelinStore::new(Arc::new(backend))
}

#[tokio::test]
async fn concurrent_first_boot_loser_reads_winners_authoritative_head() {
    let harness = TestHarness::new().await;
    let (store, race) =
        synchronize_create_pair_matching(&scoped_store(&harness), "_security/heads/policy.json");
    let first = PolicyStore::new(store.clone());
    let second = PolicyStore::new(store.clone());
    let config = Config::from_str(BOOTSTRAP_CONFIG).expect("valid bootstrap config");
    let boot_time = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();

    let (first, second) = tokio::join!(
        first.load_or_bootstrap(&config.security, boot_time),
        second.load_or_bootstrap(&config.security, boot_time),
    );
    let first = first.expect("first concurrent boot must resolve authority");
    let second = second.expect("second concurrent boot must resolve authority");

    assert_eq!(race.arrivals(), 2);
    assert_eq!(race.conflicts(), 1);
    assert_eq!(first.head(), second.head());
    assert_eq!(first.snapshot(), second.snapshot());
    assert_eq!(first.head().version().get(), 1);

    let policy_objects = store
        .list_prefix("_security/policies/")
        .await
        .expect("both immutable bootstrap candidates must remain listable");
    assert_eq!(policy_objects.len(), 2);
    assert!(policy_objects
        .iter()
        .any(|key| key == first.head().object_key()));

    harness.cleanup().await;
}

#[tokio::test]
async fn authoritative_policy_allows_enforced_restart_without_bootstrap_keys() {
    let harness = TestHarness::new().await;
    let policy_store = PolicyStore::new(scoped_store(&harness));
    let boot_time = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
    let bootstrap = Config::from_str(BOOTSTRAP_CONFIG).expect("valid bootstrap config");
    let published = policy_store
        .load_or_bootstrap(&bootstrap.security, boot_time)
        .await
        .expect("first boot must publish the authoritative policy");

    let restart = Config::from_str(
        "[security]\nmode = \"enforced\"\ncursor_hmac_key_hex = \"1111111111111111111111111111111111111111111111111111111111111111\"\n",
    )
        .expect("S3 authority makes bootstrap credentials optional after first boot");
    let loaded = policy_store
        .load_or_bootstrap(&restart.security, boot_time)
        .await
        .expect("existing S3 policy must load without bootstrap credentials");

    assert_eq!(loaded.head(), published.head());
    assert_eq!(loaded.snapshot(), published.snapshot());

    harness.cleanup().await;
}

#[tokio::test]
async fn first_boot_rejects_missing_bootstrap_credentials() {
    let harness = TestHarness::new().await;
    let store = scoped_store(&harness);
    let policy_store = PolicyStore::new(store.clone());
    let boot_time = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
    let empty = Config::from_str(
        "[security]\nmode = \"enforced\"\ncursor_hmac_key_hex = \"1111111111111111111111111111111111111111111111111111111111111111\"\n",
    )
        .expect("an empty recovery seam is syntactically valid");

    let error = policy_store
        .load_or_bootstrap(&empty.security, boot_time)
        .await
        .expect_err("first boot without any usable recovery credential must fail loud");

    assert!(matches!(
        error,
        ZeppelinError::Security(SecurityError::MissingBootstrapCredentials)
    ));
    assert!(store
        .list_prefix("_security/")
        .await
        .expect("failed first boot must leave the policy keyspace readable")
        .is_empty());
    harness.cleanup().await;
}

#[tokio::test]
async fn first_boot_rejects_only_expired_bootstrap_credentials() {
    let harness = TestHarness::new().await;
    let store = scoped_store(&harness);
    let policy_store = PolicyStore::new(store.clone());
    let boot_time = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
    let expired = Config::from_str(
        r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_expired"
name = "expired-bootstrap"
sha256_hex = "1111111111111111111111111111111111111111111111111111111111111111"
actions = ["*"]
namespaces = ["*"]
expires_at = "2000-01-01T00:00:00Z"
"#,
    )
    .expect("expired credentials remain syntactically valid recovery input");

    let error = policy_store
        .load_or_bootstrap(&expired.security, boot_time)
        .await
        .expect_err("first boot must not publish a policy with no usable credential");

    assert!(matches!(
        error,
        ZeppelinError::Security(SecurityError::MissingBootstrapCredentials)
    ));
    assert!(store
        .list_prefix("_security/")
        .await
        .expect("failed first boot must leave the policy keyspace readable")
        .is_empty());
    harness.cleanup().await;
}

#[tokio::test]
async fn existing_policy_ignores_expired_drifted_config_and_warns_redacted() {
    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_writer({
            let captured = Arc::clone(&captured);
            move || CapturedLogWriter(Arc::clone(&captured))
        })
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("bootstrap-authority test binary must not install another tracing subscriber");

    let harness = TestHarness::new().await;
    let policy_store = PolicyStore::new(scoped_store(&harness));
    let boot_time = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
    let bootstrap = Config::from_str(BOOTSTRAP_CONFIG).expect("valid bootstrap config");
    let published = policy_store
        .load_or_bootstrap(&bootstrap.security, boot_time)
        .await
        .expect("first boot must publish the authoritative policy");
    let drifted = Config::from_str(&format!(
        r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_drifted"
name = "ignored-expired-recovery-key"
sha256_hex = "{DRIFTED_DIGEST}"
actions = ["Query"]
namespaces = ["different-namespace"]
expires_at = "2000-01-01T00:00:00Z"
"#
    ))
    .expect("drifted recovery credentials remain syntactically valid");

    let loaded = policy_store
        .load_or_bootstrap(&drifted.security, boot_time)
        .await
        .expect("the authoritative S3 policy must ignore drifted recovery config");
    assert_eq!(loaded.snapshot(), published.snapshot());

    harness.cleanup().await;

    let output = String::from_utf8(
        captured
            .lock()
            .unwrap_or_else(|_| panic!("captured log buffer lock poisoned"))
            .clone(),
    )
    .expect("captured logs must be UTF-8");
    let warnings = output
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|event| {
            event["fields"]["message"]
                == "configured bootstrap credentials drift from S3-authoritative security policy and are ignored"
                && event["fields"]["policy_version"] == 1
                && event["fields"]["configured_bootstrap_key_count"] == 1
                && event["fields"]["authoritative_policy_key_count"] == 1
        })
        .collect::<Vec<_>>();
    assert_eq!(
        warnings.len(),
        1,
        "expired drifted bootstrap config must produce exactly one redacted warning"
    );
    let warning = &warnings[0];

    assert_eq!(warning["level"], "WARN");
    assert_eq!(warning["fields"]["policy_version"], 1);
    assert_eq!(warning["fields"]["configured_bootstrap_key_count"], 1);
    assert_eq!(warning["fields"]["authoritative_policy_key_count"], 1);
    assert!(!output.contains(DRIFTED_DIGEST));
    assert!(!output.contains("0000000000000000000000000000000000000000000000000000000000000000"));
    assert!(!output.contains("sha256_hex"));
}
