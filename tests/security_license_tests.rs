mod common;

use std::fs;
use std::process::Command;
use std::sync::{Arc, Mutex};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{Duration, TimeZone, Utc};
use ed25519_dalek::{Signer, SigningKey};
use serde_json::json;
use zeppelin::config::Config;
use zeppelin::security::{
    canonical_payload_bytes, Entitlements, Feature, LicenseLimits, LicensePayload, SecurityError,
    SecurityKernel, SignedLicense,
};
use zeppelin::startup::build_app;
use zeppelin::time::{Clock, TimeSource};

use common::harness::TestHarness;
use common::server::{
    client_with_bearer, expired_test_entitlements, scoped_test_security_store,
    start_test_server_full_with_entitlements, start_test_server_with_entitlements,
    test_admin_bearer, test_entitlements,
};

#[derive(Debug)]
struct AdjustableLicenseClock(Mutex<chrono::DateTime<Utc>>);

impl AdjustableLicenseClock {
    fn advance(&self, duration: Duration) {
        let mut now = self
            .0
            .lock()
            .unwrap_or_else(|_| panic!("license test clock lock poisoned"));
        *now += duration;
    }
}

impl TimeSource for AdjustableLicenseClock {
    fn now(&self) -> chrono::DateTime<Utc> {
        *self
            .0
            .lock()
            .unwrap_or_else(|_| panic!("license test clock lock poisoned"))
    }
}

const TEST_LICENSE_SEED: [u8; 32] = [7_u8; 32];

fn signed_license(
    features: Vec<Feature>,
    issued_at: chrono::DateTime<Utc>,
    expires_at: chrono::DateTime<Utc>,
) -> SignedLicense {
    let payload = LicensePayload {
        customer_id: "customer:test".to_string(),
        customer_name: "Test Customer".to_string(),
        issued_at,
        expires_at,
        features,
        limits: LicenseLimits::default(),
    };
    let signing_key = SigningKey::from_bytes(&TEST_LICENSE_SEED);
    let signature = signing_key.sign(&canonical_payload_bytes(&payload).unwrap());
    SignedLicense::new(payload, URL_SAFE_NO_PAD.encode(signature.to_bytes()))
}

#[test]
fn community_entitlements_expose_no_licensed_features() {
    let entitlements = Entitlements::community();

    for feature in Feature::ALL {
        assert!(!entitlements.has(feature));
    }
    assert!(!entitlements.management_frozen(Utc::now()));
}

#[test]
fn expired_entitlements_keep_enforcement_features_but_freeze_management_after_grace() {
    let entitlements = expired_test_entitlements();
    let expires_at = entitlements.expires_at().unwrap();

    let within_grace = expires_at + Duration::days(14) - Duration::seconds(1);
    let after_grace = expires_at + Duration::days(14) + Duration::seconds(1);

    assert!(entitlements.has(Feature::Rbac));
    assert!(entitlements.has(Feature::Constraints));
    assert!(entitlements.has(Feature::AuditS3));
    assert!(!entitlements.management_frozen(within_grace));
    assert!(entitlements.management_frozen(after_grace));
}

#[test]
fn entitlements_gating_is_composition_only() {
    for path in [
        "src/server/handlers/mod.rs",
        "src/server/handlers/as_of.rs",
        "src/server/handlers/config.rs",
        "src/server/handlers/namespace.rs",
        "src/server/handlers/query.rs",
        "src/server/handlers/security.rs",
        "src/server/handlers/vectors.rs",
    ] {
        let source = fs::read_to_string(path).unwrap();
        for forbidden in [
            "Entitlements",
            "Feature::",
            "feature_not_licensed",
            "license_expired",
        ] {
            assert!(
                !source.contains(forbidden),
                "handler-level entitlement gate {forbidden:?} found in {path}"
            );
        }
    }
}

#[test]
fn release_builds_cannot_select_test_license_authority() {
    let manifest = fs::read_to_string("Cargo.toml").unwrap();
    let license_source = fs::read_to_string("src/security/license.rs").unwrap();
    let entitlement_source = fs::read_to_string("src/security/entitlements.rs").unwrap();
    let startup_source = fs::read_to_string("src/startup.rs").unwrap();
    let integration_server_source = fs::read_to_string("tests/common/server.rs").unwrap();

    assert!(
        !manifest.contains("test-support"),
        "test-support must not be a Cargo-selectable production feature"
    );
    assert!(
        !license_source.contains("feature = \"test-support\""),
        "the embedded verification key must not be feature-selectable"
    );
    assert!(
        !entitlement_source.contains("feature = \"test-support\""),
        "unchecked entitlement constructors must not be feature-selectable"
    );
    assert!(
        !license_source.contains("pub fn verify_signed_license_bytes_with_public_key"),
        "release code must not expose caller-selected license trust roots"
    );
    assert!(
        !startup_source.contains("pub async fn build_app_with_entitlement_resolver"),
        "release code must not expose caller-selected entitlement resolvers"
    );
    assert!(
        !integration_server_source.contains("verify_signed_license_bytes"),
        "integration support must not rely on a transferable production-key license"
    );
    assert!(
        !std::path::Path::new("tests/fixtures/licenses").exists(),
        "production-key-signed license fixtures must never be committed"
    );
}

#[tokio::test]
async fn community_enforced_boot_rejects_zero_or_expired_credentials() {
    let harness = TestHarness::new().await;
    let entitlements = Arc::new(Entitlements::community());
    let mut empty = Config::default();
    empty.security.set_cursor_hmac_key_hex("42".repeat(32));

    let empty_error = match SecurityKernel::from_resolved_entitlements(
        harness.store.clone(),
        &empty.security,
        Clock::system(),
        Arc::clone(&entitlements),
    )
    .await
    {
        Ok(_) => panic!("community enforced boot must reject zero credentials"),
        Err(error) => error,
    };
    assert!(matches!(
        empty_error,
        zeppelin::error::ZeppelinError::Security(SecurityError::MissingBootstrapCredentials)
    ));

    let mut expired = Config::default();
    let _bearer = test_admin_bearer(&mut expired);
    expired.security.api_keys[0].expires_at = Some(Utc::now() - Duration::seconds(1));
    let expired_error = match SecurityKernel::from_resolved_entitlements(
        harness.store.clone(),
        &expired.security,
        Clock::system(),
        entitlements,
    )
    .await
    {
        Ok(_) => panic!("community enforced boot must reject only-expired credentials"),
        Err(error) => error,
    };
    assert!(matches!(
        expired_error,
        zeppelin::error::ZeppelinError::Security(SecurityError::MissingBootstrapCredentials)
    ));

    harness.cleanup().await;
}

#[tokio::test]
async fn tampered_license_fails_boot_before_storage_composition() {
    let temp = tempfile::TempDir::new().unwrap();
    let license_path = temp.path().join("tampered.json");
    fs::write(
        &license_path,
        serde_json::to_vec(&json!({
            "customer_id": "customer:test",
            "customer_name": "Test Customer",
            "issued_at": "2030-01-01T00:00:00Z",
            "expires_at": "2031-01-01T00:00:00Z",
            "features": ["rbac"],
            "limits": {},
            "signature": "tampered"
        }))
        .unwrap(),
    )
    .unwrap();
    let mut config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut config);
    config.security.license_path = license_path.to_string_lossy().into_owned();

    let error = match build_app(config).await {
        Ok(_) => panic!("tampered configured license must fail boot"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("license signature verification failed"));
}

#[tokio::test]
async fn license_signed_by_wrong_key_fails_boot() {
    let payload = LicensePayload {
        customer_id: "customer:test".to_string(),
        customer_name: "Test Customer".to_string(),
        issued_at: Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap(),
        expires_at: Utc.with_ymd_and_hms(2031, 1, 1, 0, 0, 0).unwrap(),
        features: vec![Feature::Rbac],
        limits: LicenseLimits::default(),
    };
    let signing_key = SigningKey::from_bytes(&[42_u8; 32]);
    let signature = signing_key.sign(&canonical_payload_bytes(&payload).unwrap());
    let document = SignedLicense::new(payload, URL_SAFE_NO_PAD.encode(signature.to_bytes()));
    let temp = tempfile::TempDir::new().unwrap();
    let license_path = temp.path().join("wrong-key.json");
    fs::write(&license_path, serde_json::to_vec(&document).unwrap()).unwrap();
    let mut config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut config);
    config.security.license_path = license_path.to_string_lossy().into_owned();

    let error = match build_app(config).await {
        Ok(_) => panic!("license signed by a non-production key must fail boot"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("license signature verification failed"));
}

#[tokio::test]
async fn community_boot_keeps_data_plane_and_stubs_security_management() {
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_entitlements(Config::default(), Entitlements::community()).await;
    let admin = client_with_bearer(&admin_bearer);

    let create_namespace = admin
        .post(format!("{base_url}/v1/namespaces"))
        .json(&json!({
            "name": format!("community-data-{}", uuid::Uuid::new_v4()),
            "dimensions": 2,
            "distance_metric": "euclidean"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(create_namespace.status(), 201);

    let management = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": "zpk1_test_admin",
            "name": "community-key"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(management.status(), 403);
    let body: serde_json::Value = management.json().await.unwrap();
    assert_eq!(body["code"], "feature_not_licensed");
    assert_eq!(body["status"], 403);

    let security_store = scoped_test_security_store(&harness.store, &harness.prefix);
    assert!(
        security_store
            .list_prefix("_security/")
            .await
            .unwrap()
            .is_empty(),
        "community composition must not construct the licensed S3 policy registry"
    );

    assert!(
        harness
            .store
            .list_prefix("_audit/")
            .await
            .unwrap()
            .iter()
            .all(|path| !path.contains(&harness.prefix)),
        "community audit must stay tracing-only"
    );
}

#[test]
fn license_tool_rejects_wrong_key_and_never_clobbers_inputs() {
    let root = tempfile::TempDir::new().unwrap();
    let payload_path = root.path().join("payload.json");
    let key_path = root.path().join("signing-key.txt");
    let output_path = root.path().join("license.json");
    let payload = signed_license(
        vec![Feature::Rbac],
        Utc::now() - Duration::days(1),
        Utc::now() + Duration::days(30),
    )
    .payload();
    fs::write(&payload_path, serde_json::to_vec(&payload).unwrap()).unwrap();
    let key_text = "07".repeat(32);
    fs::write(&key_path, &key_text).unwrap();

    let tool = env!("CARGO_BIN_EXE_zeppelin_license");
    let signed = Command::new(tool)
        .args([
            "sign",
            "--payload",
            payload_path.to_str().unwrap(),
            "--private-key",
            key_path.to_str().unwrap(),
            "--output",
            output_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!signed.status.success());
    assert!(String::from_utf8_lossy(&signed.stderr)
        .contains("private key does not match the public key embedded in this binary"));
    assert!(!output_path.exists());

    let original_key = fs::read(&key_path).unwrap();
    let clobber = Command::new(tool)
        .args([
            "sign",
            "--payload",
            payload_path.to_str().unwrap(),
            "--private-key",
            key_path.to_str().unwrap(),
            "--output",
            key_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!clobber.status.success());
    assert_eq!(fs::read(&key_path).unwrap(), original_key);

    let first_license = b"existing-license-must-survive".to_vec();
    fs::write(&output_path, &first_license).unwrap();
    let overwrite = Command::new(tool)
        .args([
            "sign",
            "--payload",
            payload_path.to_str().unwrap(),
            "--private-key",
            key_path.to_str().unwrap(),
            "--output",
            output_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!overwrite.status.success());
    assert_eq!(fs::read(&output_path).unwrap(), first_license);
}

#[tokio::test]
async fn expired_grace_then_frozen_keeps_reads_and_enforcement() {
    let entitlements = expired_test_entitlements();
    let (base_url, _harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_entitlements(Config::default(), entitlements).await;
    let admin = client_with_bearer(&admin_bearer);

    let read = admin
        .get(format!("{base_url}/v1/security/keys"))
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), 200);

    let mutation = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": "zpk1_test_admin",
            "name": "frozen-key"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(mutation.status(), 403);
    let body: serde_json::Value = mutation.json().await.unwrap();
    assert_eq!(body["code"], "license_expired");

    let unauthenticated = reqwest::Client::new()
        .get(format!("{base_url}/v1/namespaces/not-visible"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), 401);
}

#[tokio::test]
async fn expired_license_never_disables_constraint_enforcement() {
    let entitlements = expired_test_entitlements();
    let expires_at = entitlements.expires_at().unwrap();
    let now = expires_at + Duration::days(13);
    let source = Arc::new(AdjustableLicenseClock(Mutex::new(now)));
    let clock = Clock::from_source(source.clone());
    let harness = TestHarness::new().await;
    let server = start_test_server_full_with_entitlements(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        clock,
        entitlements,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);

    let principal_id = "service:expired-license-reader";
    let principal = admin
        .post(format!("{}/v1/security/principals", server.base_url))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "expired-license-reader"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        principal.status(),
        201,
        "{}",
        principal.text().await.unwrap()
    );

    let key = admin
        .post(format!("{}/v1/security/keys", server.base_url))
        .json(&json!({
            "principal_id": principal_id,
            "name": "expired-license-reader-primary"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201, "{}", key.text().await.unwrap());
    let key_body: serde_json::Value = key.json().await.unwrap();
    let reader = client_with_bearer(key_body["api_key"].as_str().unwrap());

    let grant = admin
        .post(format!("{}/v1/security/grants", server.base_url))
        .json(&json!({
            "principal_id": principal_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["Query"]},
            "mandatory_filter": {"op": "eq", "field": "tenant", "value": "a"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());

    let namespace = common::server::create_ns_api(&admin, &server.base_url, 2).await;
    let upsert = admin
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({
            "vectors": [
                {"id": "tenant-a", "values": [1.0, 0.0], "attributes": {"tenant": "a"}},
                {"id": "tenant-b", "values": [1.0, 0.0], "attributes": {"tenant": "b"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), 200, "{}", upsert.text().await.unwrap());

    source.advance(Duration::days(2));

    let frozen = admin
        .post(format!("{}/v1/security/keys", server.base_url))
        .json(&json!({
            "principal_id": "zpk1_test_admin",
            "name": "too-late"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(frozen.status(), 403);
    assert_eq!(
        frozen.json::<serde_json::Value>().await.unwrap()["code"],
        "license_expired"
    );

    let denied_upsert = reader
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({
            "vectors": [{"id": "must-stay-denied", "values": [1.0, 0.0]}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied_upsert.status(), 403);
    assert_eq!(
        denied_upsert.json::<serde_json::Value>().await.unwrap()["code"],
        "forbidden"
    );

    let query = reader
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 10}))
        .send()
        .await
        .unwrap();
    assert_eq!(query.status(), 200);
    let body: serde_json::Value = query.json().await.unwrap();
    let ids = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["tenant-a"]);

    server.shutdown().await;
}

#[tokio::test]
async fn constraints_require_feature_when_loading_authoritative_policy() {
    let full = test_entitlements(Feature::ALL);
    let (base_url, harness, _cache, _cache_dir, admin_bearer) =
        start_test_server_with_entitlements(Config::default(), full).await;
    let admin = client_with_bearer(&admin_bearer);

    let grant = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": "zpk1_test_admin",
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["Query"]},
            "mandatory_filter": {"op": "eq", "field": "tenant", "value": "a"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());

    let mut config = Config::default();
    let _unused_bearer = test_admin_bearer(&mut config);
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let rbac_only = Arc::new(test_entitlements([Feature::Rbac]));
    let error = match SecurityKernel::from_resolved_entitlements(
        store,
        &config.security,
        Clock::system(),
        rbac_only,
    )
    .await
    {
        Ok(_) => panic!("constraint-carrying policy must fail without Constraints"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        zeppelin::error::ZeppelinError::Security(SecurityError::FeatureRequired(
            Feature::Constraints
        ))
    ));
}
