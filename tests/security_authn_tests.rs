use std::str::FromStr;

use axum::http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
use chrono::Utc;
use zeppelin::config::Config;
use zeppelin::security::{
    ApiKeyAdapter, AuthnFailure, CredentialAdapter, DenyReason, SecurityError,
};

fn adapter() -> ApiKeyAdapter {
    let config = Config::from_str(
        r#"
[security]
mode = "enforced"

[[security.api_keys]]
key_id = "zpk1_reader"
name = "reader"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["Query"]
namespaces = ["tenant-a"]
"#,
    )
    .unwrap();
    ApiKeyAdapter::from_config(&config.security).unwrap()
}

#[test]
fn canonical_bearer_authenticates_named_principal() {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer zpk1_reader.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
    );

    let principal = adapter().authenticate(&headers, Utc::now()).unwrap();

    assert_eq!(principal.id.as_str(), "zpk1_reader");
    assert_eq!(principal.display_name, "reader");
}

#[test]
fn wrong_secret_right_key_id_is_unknown() {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer zpk1_reader.BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
    );

    let failure = adapter().authenticate(&headers, Utc::now()).unwrap_err();

    assert_eq!(failure, AuthnFailure::CredentialUnknown);
}

#[test]
fn malformed_bearer_is_unknown() {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer zpk1_reader.too-short"),
    );

    let failure = adapter().authenticate(&headers, Utc::now()).unwrap_err();

    assert_eq!(failure, AuthnFailure::CredentialUnknown);
}

#[test]
fn expired_key_is_rejected() {
    let config = Config::from_str(
        r#"
[security]
mode = "open_unsafe"

[[security.api_keys]]
key_id = "zpk1_expired"
name = "expired"
sha256_hex = "0f007385b6f9d4b7eeb2748605afe1a984a0a3bfa3f014d09e2a784ce9e5cd1a"
actions = ["Query"]
namespaces = ["tenant-a"]
expires_at = "2000-01-01T00:00:00Z"
"#,
    )
    .unwrap();
    let adapter = ApiKeyAdapter::from_config(&config.security).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer zpk1_expired.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
    );

    let failure = adapter.authenticate(&headers, Utc::now()).unwrap_err();

    assert_eq!(failure, AuthnFailure::CredentialExpired);
}

#[test]
fn expired_authorization_decision_stays_an_authentication_failure() {
    let error = SecurityError::Authorization(DenyReason::CredentialExpired);

    assert_eq!(error.status_code(), 401);
    assert_eq!(error.code(), "credential_expired");
    assert_eq!(error.client_message(), "authentication required");
}
