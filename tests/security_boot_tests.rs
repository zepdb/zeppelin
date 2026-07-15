use std::str::FromStr;
use std::time::Duration;

use zeppelin::config::{Config, StorageBackend};
use zeppelin::startup::{build_app, shutdown_background_tasks};

const VALID_DIGEST: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[test]
fn boot_fails_without_mode() {
    let error = Config::from_str("").expect_err("missing [security] must fail closed");

    assert_eq!(
        error.to_string(),
        "config error: missing required [security] section; set security.mode to \"enforced\" or \"open_unsafe\""
    );
}

#[test]
fn enforced_config_allows_no_bootstrap_keys_pending_s3_authority_check() {
    let config = Config::from_str(
        "[security]\nmode = \"enforced\"\ncursor_hmac_key_hex = \"1111111111111111111111111111111111111111111111111111111111111111\"\n",
    )
        .expect("credential availability is decided after checking S3 policy authority");

    assert!(config.security.api_keys.is_empty());
}

#[test]
fn enforced_config_requires_and_redacts_cursor_hmac_key() {
    let missing = Config::from_str("[security]\nmode = \"enforced\"\n")
        .expect_err("enforced configuration without cursor signing material must fail closed");
    assert!(missing
        .to_string()
        .contains("security.cursor_hmac_key_hex is required"));

    let malformed =
        Config::from_str("[security]\nmode = \"enforced\"\ncursor_hmac_key_hex = \"abcd\"\n")
            .expect_err("short cursor signing material must fail closed");
    assert!(malformed
        .to_string()
        .contains("must contain exactly 64 hexadecimal characters"));

    let secret = "12".repeat(32);
    let config = Config::from_str(&format!(
        "[security]\nmode = \"enforced\"\ncursor_hmac_key_hex = \"{secret}\"\n"
    ))
    .expect("valid cursor signing material must parse");
    let debug = format!("{config:?}");
    assert!(!debug.contains(&secret));
    assert!(debug.contains("[REDACTED]"));
}

#[test]
fn boot_fails_bad_action_name() {
    let error = Config::from_str(
        r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_admin"
name = "bootstrap-admin"
sha256_hex = "0000000000000000000000000000000000000000000000000000000000000000"
actions = ["Qrye"]
namespaces = ["*"]
"#,
    )
    .expect_err("unknown action names must fail loud");

    assert_eq!(
        error.to_string(),
        "config error: invalid configuration:\n- security.api_keys[0].actions contains unknown action \"Qrye\""
    );
}

#[test]
fn boot_fails_dup_key_id() {
    let error = Config::from_str(
        r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_duplicate"
name = "first"
sha256_hex = "0000000000000000000000000000000000000000000000000000000000000000"
actions = ["Query"]
namespaces = ["*"]

[[security.api_keys]]
key_id = "zpk1_duplicate"
name = "second"
sha256_hex = "1111111111111111111111111111111111111111111111111111111111111111"
actions = ["Query"]
namespaces = ["*"]
"#,
    )
    .expect_err("duplicate public key identifiers must be ambiguous and invalid");

    assert_eq!(
        error.to_string(),
        "config error: invalid configuration:\n- security.api_keys contains duplicate key_id \"zpk1_duplicate\""
    );
}

#[test]
fn boot_fails_bad_key_digest() {
    let error = Config::from_str(
        r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_bad_digest"
name = "bad-digest"
sha256_hex = "not-a-sha256-digest"
actions = ["Query"]
namespaces = ["*"]
"#,
    )
    .expect_err("API key digests must be exactly 32 encoded bytes");

    assert_eq!(
        error.to_string(),
        "config error: invalid configuration:\n- security.api_keys[0].sha256_hex must contain exactly 64 hexadecimal characters"
    );
}

#[test]
fn boot_fails_security_section_without_mode() {
    let error = Config::from_str("[security]\n")
        .expect_err("the required security mode must never be inferred");

    assert_eq!(
        error.to_string(),
        "config error: missing required security.mode in [security]; set it to \"enforced\" or \"open_unsafe\""
    );
}

#[test]
fn boot_fails_noncanonical_key_id() {
    let source = format!(
        r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_reader.with-dot"
name = "reader"
sha256_hex = "{VALID_DIGEST}"
actions = ["Query"]
namespaces = ["*"]
"#
    );
    let error = Config::from_str(&source)
        .expect_err("configured key IDs must match the bearer-token grammar");

    assert_eq!(
        error.to_string(),
        "config error: invalid configuration:\n- security.api_keys[0].key_id must start with \"zpk1_\", contain a nonempty alphanumeric, '-' or '_' suffix, and be at most 128 characters"
    );
}

#[test]
fn boot_fails_empty_key_name() {
    let source = format!(
        r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_reader"
name = "   "
sha256_hex = "{VALID_DIGEST}"
actions = ["Query"]
namespaces = ["*"]
"#
    );
    let error = Config::from_str(&source)
        .expect_err("a named bootstrap credential needs an audit-safe display identity");

    assert_eq!(
        error.to_string(),
        "config error: invalid configuration:\n- security.api_keys[0].name must not be empty or whitespace"
    );
}

#[test]
fn boot_fails_empty_action_grants() {
    let source = format!(
        r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_reader"
name = "reader"
sha256_hex = "{VALID_DIGEST}"
actions = []
namespaces = ["*"]
"#
    );
    let error =
        Config::from_str(&source).expect_err("a credential with no typed action grant is unusable");

    assert_eq!(
        error.to_string(),
        "config error: invalid configuration:\n- security.api_keys[0].actions must contain at least one Action name or \"*\""
    );
}

#[test]
fn boot_fails_empty_namespace_scopes() {
    let source = format!(
        r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_reader"
name = "reader"
sha256_hex = "{VALID_DIGEST}"
actions = ["Query"]
namespaces = []
"#
    );
    let error = Config::from_str(&source)
        .expect_err("a credential with no typed namespace scope is unusable");

    assert_eq!(
        error.to_string(),
        "config error: invalid configuration:\n- security.api_keys[0].namespaces must contain at least one namespace name or \"*\""
    );
}

#[test]
fn boot_fails_invalid_namespace_scope() {
    let source = format!(
        r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_reader"
name = "reader"
sha256_hex = "{VALID_DIGEST}"
actions = ["Query"]
namespaces = ["tenant/a"]
"#
    );
    let error = Config::from_str(&source)
        .expect_err("namespace grants must use the same typed grammar as namespace resources");

    assert_eq!(
        error.to_string(),
        "config error: invalid configuration:\n- security.api_keys[0].namespaces contains invalid namespace \"tenant/a\""
    );
}

#[test]
fn boot_fails_zero_policy_refresh_interval() {
    let error = Config::from_str(
        r#"
[security]
mode = "open_unsafe"
policy_refresh_secs = 0
"#,
    )
    .expect_err("a zero policy refresh interval cannot provide bounded revocation");

    assert_eq!(
        error.to_string(),
        "config error: invalid configuration:\n- security.policy_refresh_secs must be greater than zero"
    );
}

#[test]
fn enforced_config_allows_expired_recovery_key_pending_s3_authority_check() {
    let source = format!(
        r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_expired"
name = "expired"
sha256_hex = "{VALID_DIGEST}"
actions = ["Query"]
namespaces = ["*"]
expires_at = "2000-01-01T00:00:00Z"
"#
    );
    let config = Config::from_str(&source)
        .expect("S3 may already contain an authoritative policy with usable credentials");

    assert_eq!(config.security.api_keys.len(), 1);
    assert!(config.security.api_keys[0]
        .expires_at
        .is_some_and(|expires_at| expires_at < chrono::Utc::now()));
}

#[test]
fn load_without_path_fails_required_security_contract() {
    let error = Config::load(None)
        .expect_err("loading without an explicit [security] section must fail closed");

    assert_eq!(
        error.to_string(),
        "config error: missing required [security] section; set security.mode to \"enforced\" or \"open_unsafe\""
    );
}

#[test]
fn load_file_without_security_fails_required_security_contract() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "[server]\nport = 8080\n").unwrap();
    let path = file.path().to_str().unwrap();

    let error = Config::load(Some(path))
        .expect_err("file loading must enforce the same security contract as FromStr");

    assert_eq!(
        error.to_string(),
        "config error: missing required [security] section; set security.mode to \"enforced\" or \"open_unsafe\""
    );
}

#[tokio::test]
async fn startup_exports_open_unsafe_security_mode_gauge() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = Config::from_str(
        r#"
[security]
mode = "open_unsafe"
"#,
    )
    .unwrap();
    config.storage.backend = StorageBackend::Local;
    config.storage.bucket = temp.path().join("objects").to_string_lossy().to_string();
    config.cache.dir = temp.path().join("cache");
    config.compaction.interval_secs = 3_600;

    let (_router, background_tasks, audit_runtime) = build_app(config).await.unwrap();

    assert_eq!(
        zeppelin::metrics::SECURITY_MODE
            .with_label_values(&["open_unsafe"])
            .get(),
        1
    );
    assert_eq!(
        zeppelin::metrics::SECURITY_MODE
            .with_label_values(&["enforced"])
            .get(),
        0
    );

    audit_runtime.shutdown().await.unwrap();
    shutdown_background_tasks(background_tasks, Duration::from_secs(1))
        .await
        .unwrap();
}
