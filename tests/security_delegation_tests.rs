mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant};

use bytes::Bytes;
use chrono::{Duration, Utc};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use zeppelin::config::{Config, SecurityMode};
use zeppelin::security::{
    canonical_policy_checksum, verify_audit_day, Action, AuditRecord, AuditRuntime,
    DelegationNarrowing, Feature, NamespaceId, PolicyStore, SecurityKernel,
};
use zeppelin::time::{Clock, TimeSource};

use common::harness::TestHarness;
use common::server::{
    client_with_bearer, create_ns_api, scoped_test_security_store, start_test_server,
    start_test_server_full, start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer,
    start_test_server_full_with_entitlements,
    start_test_server_full_without_rate_limit_override_and_admin_bearer, test_admin_bearer,
    test_entitlements,
};

#[derive(Debug)]
struct AdjustableDelegationClock(Mutex<chrono::DateTime<chrono::Utc>>);

impl AdjustableDelegationClock {
    fn advance(&self, duration: Duration) {
        let mut now = self
            .0
            .lock()
            .unwrap_or_else(|_| panic!("delegation test clock poisoned"));
        *now += duration;
    }
}

impl TimeSource for AdjustableDelegationClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        *self
            .0
            .lock()
            .unwrap_or_else(|_| panic!("delegation test clock poisoned"))
    }
}

fn delegation_signing_key(seed_byte: u8) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().expect("delegation signing-key fixture");
    std::fs::write(file.path(), format!("{seed_byte:02x}").repeat(32))
        .expect("write delegation signing-key fixture");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o600))
            .expect("restrict delegation signing-key fixture");
    }
    file
}

#[derive(Serialize, Deserialize)]
struct PolicySnapshotWire {
    version: u64,
    created_at: chrono::DateTime<chrono::Utc>,
    created_by: String,
    checksum: String,
    principals: Vec<Value>,
    keys: Vec<Value>,
    grants: Vec<Value>,
}

#[derive(Serialize)]
struct PolicyChecksumWire<'a> {
    version: u64,
    created_at: chrono::DateTime<chrono::Utc>,
    created_by: &'a str,
    principals: &'a [Value],
    keys: &'a [Value],
    grants: &'a [Value],
}

fn recompute_policy_checksum(snapshot: &mut PolicySnapshotWire) {
    let content = PolicyChecksumWire {
        version: snapshot.version,
        created_at: snapshot.created_at,
        created_by: &snapshot.created_by,
        principals: &snapshot.principals,
        keys: &snapshot.keys,
        grants: &snapshot.grants,
    };
    let value = serde_json::to_value(content).expect("legacy policy checksum content must encode");
    snapshot.checksum =
        canonical_policy_checksum(&value).expect("production checksum seam must accept fixture");
}

#[tokio::test]
async fn legacy_all_policy_is_cas_migrated_before_serving() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut config);
    let entitlements = Arc::new(test_entitlements(Feature::ALL));
    let bootstrap_store = PolicyStore::new(store.clone(), Arc::clone(&entitlements));
    let now = chrono::Utc::now();
    let bootstrap = bootstrap_store
        .load_or_bootstrap(&config.security, now)
        .await
        .expect("Phase 7 bootstrap policy");

    let mut legacy: PolicySnapshotWire = serde_json::from_value(
        serde_json::to_value(bootstrap.snapshot()).expect("bootstrap snapshot must encode"),
    )
    .expect("bootstrap snapshot wire must decode");
    legacy.grants[0]["actions"] = json!({"kind": "all"});
    recompute_policy_checksum(&mut legacy);
    let legacy_checksum = legacy.checksum.clone();
    let legacy_key = format!("_security/policies/{}.json", ulid::Ulid::new());
    store
        .put(
            &legacy_key,
            Bytes::from(serde_json::to_vec(&legacy).expect("legacy snapshot must encode")),
        )
        .await
        .expect("legacy snapshot write");
    store
        .put(
            "_security/heads/policy.json",
            Bytes::from(
                serde_json::to_vec(&json!({
                    "version": legacy.version,
                    "object_key": legacy_key,
                    "checksum": legacy_checksum,
                }))
                .expect("legacy head must encode"),
            ),
        )
        .await
        .expect("legacy head write");

    let left_store = PolicyStore::new(store.clone(), Arc::clone(&entitlements));
    let right_store = PolicyStore::new(store.clone(), Arc::clone(&entitlements));
    let (left, right) = tokio::join!(
        left_store.load_or_bootstrap(&config.security, now),
        right_store.load_or_bootstrap(&config.security, now)
    );
    let left = left.expect("left migration contender");
    let right = right.expect("right migration contender");
    assert_eq!(left.head().checksum(), right.head().checksum());
    assert_eq!(left.snapshot().version().get(), legacy.version + 1);
    assert!(!left
        .snapshot()
        .grants()
        .iter()
        .any(|grant| matches!(grant.actions(), zeppelin::security::GrantActions::All)));
    let migrated_grants =
        serde_json::to_value(left.snapshot().grants()).expect("migrated grants must encode");
    let migrated_grants = migrated_grants
        .as_array()
        .expect("migrated grants must be an array");
    assert!(migrated_grants.iter().any(|grant| {
        grant["actions"]["actions"]
            .as_array()
            .is_some_and(|actions| actions.iter().any(|action| action == "SecurityAdminWrite"))
    }));
    assert!(migrated_grants.iter().all(|grant| {
        grant["actions"]["actions"]
            .as_array()
            .is_none_or(|actions| actions.iter().all(|action| action != "CredentialDelegate"))
    }));
    left.snapshot()
        .validate_for_use()
        .expect("migrated snapshot must compile");
    let migration_records = store
        .list_prefix("_security/migrations/phase7-safe-all-v2/")
        .await
        .expect("migration evidence list");
    assert_eq!(migration_records.len(), 1);
}

#[tokio::test]
async fn open_unsafe_with_delegation_entitlement_fails_boot() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    config.security.mode = SecurityMode::OpenUnsafe;
    let key = delegation_signing_key(0x31);
    config.security.token_signing_key_path = key.path().to_string_lossy().into_owned();

    let result = SecurityKernel::from_resolved_entitlements(
        store,
        &config.security,
        Clock::system(),
        Arc::new(test_entitlements([Feature::Rbac, Feature::Delegation])),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("delegation cannot compose without enforced parent authority"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("delegation requires security.mode = enforced"),
        "{error}"
    );
}

#[tokio::test]
async fn delegation_signing_key_contract_fails_loud_at_boot() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut config);
    let entitlements = Arc::new(test_entitlements([Feature::Rbac, Feature::Delegation]));

    let missing = SecurityKernel::from_resolved_entitlements(
        store.clone(),
        &config.security,
        Clock::system(),
        Arc::clone(&entitlements),
    )
    .await;
    let missing = match missing {
        Ok(_) => panic!("delegation boot must reject a missing signing key"),
        Err(error) => error,
    };
    assert!(
        missing
            .to_string()
            .contains("missing required security.token_signing_key_path"),
        "{missing}"
    );

    let loose = delegation_signing_key(0x51);
    #[cfg(unix)]
    std::fs::set_permissions(loose.path(), {
        use std::os::unix::fs::PermissionsExt;
        std::fs::Permissions::from_mode(0o644)
    })
    .unwrap();
    config.security.token_signing_key_path = loose.path().to_string_lossy().into_owned();
    let loose_result = SecurityKernel::from_resolved_entitlements(
        store.clone(),
        &config.security,
        Clock::system(),
        Arc::clone(&entitlements),
    )
    .await;
    #[cfg(unix)]
    {
        let loose_error = match loose_result {
            Ok(_) => panic!("delegation boot must reject a group-readable signing key"),
            Err(error) => error,
        };
        assert!(
            loose_error
                .to_string()
                .contains("must have 0600 permissions"),
            "{loose_error}"
        );
    }

    let invalid = delegation_signing_key(0x52);
    std::fs::write(invalid.path(), "not-a-32-byte-hex-seed").unwrap();
    config.security.token_signing_key_path = invalid.path().to_string_lossy().into_owned();
    let invalid_result = SecurityKernel::from_resolved_entitlements(
        store,
        &config.security,
        Clock::system(),
        entitlements,
    )
    .await;
    let invalid_error = match invalid_result {
        Ok(_) => panic!("delegation boot must reject malformed signing material"),
        Err(error) => error,
    };
    assert!(
        invalid_error
            .to_string()
            .contains("invalid delegation signing key"),
        "{invalid_error}"
    );
}

#[tokio::test]
async fn direct_kernel_composition_installs_delegation_signer_on_input_store() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut config);
    let key = delegation_signing_key(0x53);
    config.security.token_signing_key_path = key.path().to_string_lossy().into_owned();

    let (kernel, adapter) = SecurityKernel::from_resolved_entitlements(
        store.clone(),
        &config.security,
        Clock::system(),
        Arc::new(test_entitlements([Feature::Rbac, Feature::Delegation])),
    )
    .await
    .expect("direct kernel composition must publish a delegation signer");

    let (client, runtime) =
        AuditRuntime::start_for_published_signer(store.clone(), StdDuration::from_secs(60))
            .await
            .expect("direct kernel composition must install signing on its input store");
    let node_id = client.node_id().to_string();
    let now = Utc::now();
    client
        .submit_durable(AuditRecord::open_unsafe_boot(now, &node_id))
        .await
        .expect("input-store signer must durably sign audit evidence");
    runtime
        .shutdown()
        .await
        .expect("signed audit writer must seal its chain");

    let verification = verify_audit_day(&store, now.date_naive(), &node_id)
        .await
        .expect("signed audit chain must verify");
    assert!(verification.valid, "{verification:?}");
    assert_eq!(verification.verified_records, 1);

    drop(adapter);
    drop(kernel);
    drop(store);
    harness.cleanup().await;
}

#[tokio::test]
async fn verifier_discovers_signer_published_after_verifier_boot() {
    let harness = TestHarness::new().await;
    let verifier_key = delegation_signing_key(0x41);
    let signer_key = delegation_signing_key(0x42);

    let mut verifier_config = Config::default();
    verifier_config.security.policy_refresh_secs = 1;
    verifier_config.security.token_signing_key_path =
        verifier_key.path().to_string_lossy().into_owned();
    let verifier_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let verifier = start_test_server_full(verifier_store, None, verifier_config, false, None).await;

    let mut signer_config = Config::default();
    signer_config.security.policy_refresh_secs = 1;
    signer_config.security.token_signing_key_path =
        signer_key.path().to_string_lossy().into_owned();
    let signer_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let signer = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        signer_store,
        None,
        signer_config,
        false,
        None,
        100 * 1024 * 1024,
        &verifier.admin_bearer,
    )
    .await;
    let admin = client_with_bearer(&signer.admin_bearer);
    let namespace = create_ns_api(&admin, &signer.base_url, 2).await;

    let grant = admin
        .post(format!("{}/v1/security/grants", signer.base_url))
        .json(&json!({
            "principal_id": "zpk1_test_admin",
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());

    let minted = admin
        .post(format!("{}/v1/security/tokens", signer.base_url))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "cross-node signer refresh",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let agent = client_with_bearer(&token);

    let deadline = Instant::now() + StdDuration::from_secs(3);
    loop {
        let response = agent
            .post(format!(
                "{}/v1/namespaces/{namespace}/query",
                verifier.base_url
            ))
            .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
            .send()
            .await
            .unwrap();
        if response.status() == 200 {
            break;
        }
        assert_eq!(response.status(), 401, "{}", response.text().await.unwrap());
        assert!(
            Instant::now() <= deadline,
            "verifier did not discover the immutable signer within 2x refresh"
        );
        tokio::time::sleep(StdDuration::from_millis(25)).await;
    }

    signer.shutdown().await;
    verifier.shutdown().await;
}

#[tokio::test]
async fn concurrent_signer_registration_enforces_inventory_cap_atomically() {
    let harness = TestHarness::new().await;
    let mut base_config = Config::default();
    let _admin_bearer = test_admin_bearer(&mut base_config);
    let entitlements = Arc::new(test_entitlements([Feature::Rbac, Feature::Delegation]));
    let signing_keys = (0_u8..34)
        .map(|index| delegation_signing_key(0x60_u8 + index))
        .collect::<Vec<_>>();

    for (index, signing_key) in signing_keys.iter().take(31).enumerate() {
        let mut config = base_config.clone();
        config.security.token_signing_key_path = signing_key.path().to_string_lossy().into_owned();
        SecurityKernel::from_resolved_entitlements(
            scoped_test_security_store(&harness.store, &harness.prefix),
            &config.security,
            Clock::system(),
            Arc::clone(&entitlements),
        )
        .await
        .unwrap_or_else(|error| panic!("signer {index} must register below the cap: {error}"));
    }

    let mut left_config = base_config.clone();
    left_config.security.token_signing_key_path =
        signing_keys[31].path().to_string_lossy().into_owned();
    let mut right_config = base_config.clone();
    right_config.security.token_signing_key_path =
        signing_keys[32].path().to_string_lossy().into_owned();
    let (left, right) = tokio::join!(
        SecurityKernel::from_resolved_entitlements(
            scoped_test_security_store(&harness.store, &harness.prefix),
            &left_config.security,
            Clock::system(),
            Arc::clone(&entitlements),
        ),
        SecurityKernel::from_resolved_entitlements(
            scoped_test_security_store(&harness.store, &harness.prefix),
            &right_config.security,
            Clock::system(),
            Arc::clone(&entitlements),
        )
    );
    let successful_registrations = usize::from(left.is_ok()) + usize::from(right.is_ok());
    assert_eq!(
        successful_registrations,
        1,
        "exactly one contender may claim the final signer slot; left={:?}, right={:?}",
        left.as_ref().err(),
        right.as_ref().err()
    );

    let mut overflow_config = base_config;
    overflow_config.security.token_signing_key_path =
        signing_keys[33].path().to_string_lossy().into_owned();
    let overflow = SecurityKernel::from_resolved_entitlements(
        scoped_test_security_store(&harness.store, &harness.prefix),
        &overflow_config.security,
        Clock::system(),
        entitlements,
    )
    .await;
    let overflow = match overflow {
        Ok(_) => panic!("a distinct signer must not register above the inventory cap"),
        Err(error) => error,
    };
    assert!(
        overflow.to_string().contains("signer inventory is full"),
        "unexpected overflow error: {overflow}"
    );
}

proptest! {
    #[test]
    fn narrowing_effective_scope_never_exceeds_parent(
        parent_action_bits in any::<u32>(),
        narrowed_action_bits in any::<u32>(),
        parent_namespace_bits in any::<u8>(),
        narrowed_namespace_bits in any::<u8>(),
    ) {
        let actions = Action::ALL;
        let namespaces = (0..8)
            .map(|index| NamespaceId::new(format!("delegation-prop-{index}")).unwrap())
            .collect::<Vec<_>>();
        let narrowed_actions = actions
            .into_iter()
            .enumerate()
            .filter(|(index, action)|
                action.is_delegatable() && narrowed_action_bits & (1_u32 << index) != 0)
            .map(|(_, action)| action)
            .collect::<Vec<_>>();
        let narrowed_namespaces = namespaces
            .iter()
            .enumerate()
            .filter(|(index, _)| narrowed_namespace_bits & (1_u8 << index) != 0)
            .map(|(_, namespace)| namespace.clone())
            .collect::<Vec<_>>();
        prop_assume!(!narrowed_actions.is_empty() && !narrowed_namespaces.is_empty());
        let narrowing = DelegationNarrowing::new(
            narrowed_actions,
            narrowed_namespaces,
            None,
            "property narrowing".to_string(),
        ).unwrap();

        for (action_index, action) in actions.into_iter().enumerate() {
            for (namespace_index, namespace) in namespaces.iter().enumerate() {
                let parent_allows = parent_action_bits & (1_u32 << action_index) != 0
                    && parent_namespace_bits & (1_u8 << namespace_index) != 0;
                let effective = narrowing.effective_allows(action, namespace, parent_allows);
                prop_assert_eq!(effective, narrowing.allows(action, namespace) && parent_allows);
                prop_assert!(!effective || parent_allows);
            }
        }
    }
}

#[tokio::test]
async fn mint_narrow_use() {
    let (base_url, _harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let namespace_a = format!("delegation-a-{suffix}");
    let namespace_b = format!("delegation-b-{suffix}");

    for namespace in [&namespace_a, &namespace_b] {
        let response = admin
            .post(format!("{base_url}/v1/namespaces"))
            .json(&json!({"name": namespace, "dimensions": 2}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201, "{}", response.text().await.unwrap());
    }

    let principal_id = format!("service:delegating-parent:{suffix}");
    let principal = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "delegating-parent"
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
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": principal_id,
            "name": "delegating-parent-primary"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201, "{}", key.text().await.unwrap());
    let key_body: serde_json::Value = key.json().await.unwrap();
    let parent = client_with_bearer(key_body["api_key"].as_str().unwrap());

    for (scope, actions) in [
        (json!({"kind": "global"}), json!(["CredentialDelegate"])),
        (
            json!({"kind": "namespace", "namespace": namespace_a}),
            json!(["Query", "VectorUpsert"]),
        ),
        (
            json!({"kind": "namespace", "namespace": namespace_b}),
            json!(["Query", "VectorUpsert"]),
        ),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&json!({
                "principal_id": principal_id,
                "scope": scope,
                "actions": {"kind": "selected", "actions": actions}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }

    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace_a],
            "purpose": "answer one scoped retrieval task",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let minted_body: serde_json::Value = minted.json().await.unwrap();
    let token = minted_body["token"].as_str().unwrap();
    assert!(token.starts_with("zpt1_"));
    let agent = client_with_bearer(token);

    let mut tampered_token = token.to_string();
    let replacement = if tampered_token.ends_with('A') {
        'B'
    } else {
        'A'
    };
    tampered_token.pop();
    tampered_token.push(replacement);
    let tampered = client_with_bearer(&tampered_token)
        .post(format!("{base_url}/v1/namespaces/{namespace_a}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(tampered.status(), 401);
    assert_eq!(
        tampered.json::<serde_json::Value>().await.unwrap()["code"],
        "credential_unknown"
    );

    let query_a = agent
        .post(format!("{base_url}/v1/namespaces/{namespace_a}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(query_a.status(), 200, "{}", query_a.text().await.unwrap());

    let upsert_a = agent
        .post(format!("{base_url}/v1/namespaces/{namespace_a}/vectors"))
        .json(&json!({"vectors": [{"id": "denied", "values": [1.0, 0.0]}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert_a.status(), 403);

    let query_b = agent
        .post(format!("{base_url}/v1/namespaces/{namespace_b}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(query_b.status(), 403);
}

#[tokio::test]
async fn mint_beyond_parent_400() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let server = start_test_server_full(store.clone(), None, Config::default(), false, None).await;
    let base_url = server.base_url.clone();
    let admin = client_with_bearer(&server.admin_bearer);
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let namespace_a = format!("delegation-parent-a-{suffix}");
    let namespace_c = format!("delegation-outside-c-{suffix}");
    for namespace in [&namespace_a, &namespace_c] {
        let response = admin
            .post(format!("{base_url}/v1/namespaces"))
            .json(&json!({"name": namespace, "dimensions": 2}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201, "{}", response.text().await.unwrap());
    }

    let principal_id = format!("service:narrow-parent:{suffix}");
    let principal = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "narrow-parent"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(principal.status(), 201);
    let key = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({"principal_id": principal_id, "name": "narrow-parent-key"}))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let parent = client_with_bearer(
        key.json::<serde_json::Value>().await.unwrap()["api_key"]
            .as_str()
            .unwrap(),
    );

    for (scope, actions) in [
        (json!({"kind": "global"}), json!(["CredentialDelegate"])),
        (
            json!({"kind": "namespace", "namespace": namespace_a}),
            json!(["Query"]),
        ),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&json!({
                "principal_id": principal_id,
                "scope": scope,
                "actions": {"kind": "selected", "actions": actions}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }

    let duplicate_scope = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query", "Query"],
            "namespaces": [namespace_a, namespace_a],
            "purpose": "duplicates must fail strict parsing",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate_scope.status(), 400);
    assert_eq!(
        duplicate_scope.json::<serde_json::Value>().await.unwrap()["code"],
        "invalid_security_request"
    );

    for request in [
        json!({
            "actions": ["SecurityAdminWrite"],
            "namespaces": [namespace_a],
            "purpose": "control-plane authority cannot fit namespace narrowing",
            "expires_in_secs": 300
        }),
        json!({
            "actions": ["Query"],
            "namespaces": [namespace_a],
            "mandatory_filter": {"op": "and", "filters": []},
            "purpose": "invalid filters fail at mint",
            "expires_in_secs": 300
        }),
    ] {
        let response = parent
            .post(format!("{base_url}/v1/security/tokens"))
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["code"],
            "invalid_security_request"
        );
    }

    for request in [
        json!({
            "actions": ["Query"],
            "namespaces": [namespace_c],
            "purpose": "must not cross namespace scope",
            "expires_in_secs": 300
        }),
        json!({
            "actions": ["NamespaceDelete"],
            "namespaces": [namespace_a],
            "purpose": "must not gain destructive authority",
            "expires_in_secs": 300
        }),
    ] {
        let response = parent
            .post(format!("{base_url}/v1/security/tokens"))
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["code"], "delegation_scope_exceeds_parent");
        assert!(body.get("token").is_none());
    }

    server.flush_audit().await;
    let mut denied_mints = Vec::new();
    for key in store.list_prefix("_audit/").await.unwrap() {
        let body = store.get(&key).await.unwrap();
        denied_mints.extend(
            String::from_utf8(body.to_vec())
                .unwrap()
                .lines()
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|record| {
                    record["action"] == "CredentialDelegate"
                        && record["outcome"]["denied"]["reason"] == "action_not_granted"
                }),
        );
    }
    assert_eq!(denied_mints.len(), 2);
    let purposes = denied_mints
        .iter()
        .map(|record| {
            record["params"]["delegation_mint"]["purpose"]
                .as_str()
                .unwrap()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        purposes,
        std::collections::BTreeSet::from([
            "must not cross namespace scope",
            "must not gain destructive authority",
        ])
    );
    assert!(denied_mints
        .iter()
        .all(|record| record["params"]["delegation_mint"]
            .get("token_id")
            .is_none()));

    server.shutdown().await;
}

#[tokio::test]
async fn expired_token_401_and_backward_clock_jump_does_not_resurrect() {
    let harness = TestHarness::new().await;
    let source = Arc::new(AdjustableDelegationClock(Mutex::new(chrono::Utc::now())));
    let server = start_test_server_full_with_entitlements(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        Clock::from_source(source.clone()),
        test_entitlements(Feature::ALL),
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let principal_id = format!("service:expiring-parent:{}", uuid::Uuid::new_v4().simple());

    let principal = admin
        .post(format!("{}/v1/security/principals", server.base_url))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "expiring-parent"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(principal.status(), 201);
    let key = admin
        .post(format!("{}/v1/security/keys", server.base_url))
        .json(&json!({"principal_id": principal_id, "name": "expiring-parent-key"}))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let parent = client_with_bearer(
        key.json::<serde_json::Value>().await.unwrap()["api_key"]
            .as_str()
            .unwrap(),
    );
    for (scope, actions) in [
        (json!({"kind": "global"}), json!(["CredentialDelegate"])),
        (
            json!({"kind": "namespace", "namespace": namespace}),
            json!(["Query"]),
        ),
    ] {
        let grant = admin
            .post(format!("{}/v1/security/grants", server.base_url))
            .json(&json!({
                "principal_id": principal_id,
                "scope": scope,
                "actions": {"kind": "selected", "actions": actions}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }

    let minted = parent
        .post(format!("{}/v1/security/tokens", server.base_url))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "short-lived retrieval",
            "expires_in_secs": 2
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let agent = client_with_bearer(&token);

    source.advance(Duration::milliseconds(1_500));
    let first_observation = agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        first_observation.status(),
        200,
        "{}",
        first_observation.text().await.unwrap()
    );

    source.advance(Duration::seconds(-1));
    tokio::time::sleep(StdDuration::from_millis(600)).await;
    for _ in 0..2 {
        let response = agent
            .post(format!(
                "{}/v1/namespaces/{namespace}/query",
                server.base_url
            ))
            .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 401);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["code"],
            "credential_expired"
        );
        source.advance(Duration::seconds(-20));
    }

    let minted_after_backjump = parent
        .post(format!("{}/v1/security/tokens", server.base_url))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "mint uses the verifier monotonic floor",
            "expires_in_secs": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted_after_backjump.status(), 201);
    let fresh_token = minted_after_backjump
        .json::<serde_json::Value>()
        .await
        .unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let fresh_agent = client_with_bearer(&fresh_token);
    let immediate = fresh_agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        immediate.status(),
        200,
        "{}",
        immediate.text().await.unwrap()
    );

    source.advance(Duration::seconds(-20));
    tokio::time::sleep(StdDuration::from_millis(1_100)).await;
    let first_use_after_backjump = fresh_agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(first_use_after_backjump.status(), 401);
    assert_eq!(
        first_use_after_backjump
            .json::<serde_json::Value>()
            .await
            .unwrap()["code"],
        "credential_expired"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn parent_revocation_kills_tokens_on_second_node_within_freshness_bound() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let mut config = Config::default();
    config.security.policy_refresh_secs = 1;
    let writer = start_test_server_full(store.clone(), None, config.clone(), false, None).await;
    let admin = client_with_bearer(&writer.admin_bearer);
    let namespace = create_ns_api(&admin, &writer.base_url, 2).await;
    let principal_id = format!(
        "service:revocable-delegation:{}",
        uuid::Uuid::new_v4().simple()
    );

    let principal = admin
        .post(format!("{}/v1/security/principals", writer.base_url))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "revocable-delegation"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(principal.status(), 201);
    let key = admin
        .post(format!("{}/v1/security/keys", writer.base_url))
        .json(&json!({
            "principal_id": principal_id,
            "name": "revocable-delegation-key"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let key = key.json::<serde_json::Value>().await.unwrap();
    let parent_key_id = key["key_id"].as_str().unwrap().to_string();
    let parent = client_with_bearer(key["api_key"].as_str().unwrap());
    for (scope, actions) in [
        (json!({"kind": "global"}), json!(["CredentialDelegate"])),
        (
            json!({"kind": "namespace", "namespace": namespace}),
            json!(["Query"]),
        ),
    ] {
        let grant = admin
            .post(format!("{}/v1/security/grants", writer.base_url))
            .json(&json!({
                "principal_id": principal_id,
                "scope": scope,
                "actions": {"kind": "selected", "actions": actions}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let minted = parent
        .post(format!("{}/v1/security/tokens", writer.base_url))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "revocation-bound test",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let agent = client_with_bearer(&token);

    let reader = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        None,
        config,
        false,
        None,
        100 * 1024 * 1024,
        &writer.admin_bearer,
    )
    .await;
    let before = agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            reader.base_url
        ))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(before.status(), 200, "{}", before.text().await.unwrap());

    let revoked = admin
        .delete(format!(
            "{}/v1/security/keys/{parent_key_id}",
            writer.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(revoked.status(), 200, "{}", revoked.text().await.unwrap());

    let deadline = Instant::now() + StdDuration::from_secs(2);
    loop {
        let response = agent
            .post(format!(
                "{}/v1/namespaces/{namespace}/query",
                reader.base_url
            ))
            .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
            .send()
            .await
            .unwrap();
        assert!(
            Instant::now() <= deadline,
            "delegated credential survived parent key revocation beyond 2x policy refresh"
        );
        if response.status() == 401 {
            assert_eq!(
                response.json::<serde_json::Value>().await.unwrap()["code"],
                "credential_unknown"
            );
            break;
        }
        assert_eq!(response.status(), 200);
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }

    writer.shutdown().await;
    reader.shutdown().await;
}

#[tokio::test]
async fn parent_grant_removal_rechecks_current_authority() {
    let (base_url, _harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&admin, &base_url, 2).await;
    let principal_id = format!(
        "service:grant-rechecked-delegation:{}",
        uuid::Uuid::new_v4().simple()
    );
    let principal = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "grant-rechecked-delegation"
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
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": principal_id,
            "name": "grant-rechecked-delegation-key"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201, "{}", key.text().await.unwrap());
    let key = key.json::<serde_json::Value>().await.unwrap();
    let parent = client_with_bearer(key["api_key"].as_str().unwrap());
    for (scope, actions) in [
        (json!({"kind": "global"}), json!(["CredentialDelegate"])),
        (
            json!({"kind": "namespace", "namespace": namespace}),
            json!(["Query"]),
        ),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&json!({
                "principal_id": principal_id,
                "scope": scope,
                "actions": {"kind": "selected", "actions": actions}
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "current authority test",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let agent = client_with_bearer(&token);
    let before = agent
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(before.status(), 200, "{}", before.text().await.unwrap());

    let removed = admin
        .delete(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["Query"]}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(removed.status(), 200, "{}", removed.text().await.unwrap());
    let after = agent
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(after.status(), 403);
    assert_eq!(
        after.json::<serde_json::Value>().await.unwrap()["code"],
        "forbidden"
    );
}

#[tokio::test]
async fn token_filter_intersects_parent_filter() {
    let (base_url, _harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&admin, &base_url, 2).await;
    let principal_id = format!(
        "service:filtered-delegation:{}",
        uuid::Uuid::new_v4().simple()
    );
    assert_eq!(
        admin
            .post(format!("{base_url}/v1/security/principals"))
            .json(&json!({
                "principal_id": principal_id,
                "kind": "service",
                "display_name": "filtered-delegation"
            }))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let key = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({"principal_id": principal_id, "name": "filtered-key"}))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let parent = client_with_bearer(
        key.json::<serde_json::Value>().await.unwrap()["api_key"]
            .as_str()
            .unwrap(),
    );
    for body in [
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }),
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["Query"]},
            "mandatory_filter": {"op": "eq", "field": "tenant", "value": "a"}
        }),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let upsert = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "a-west", "values": [1.0, 0.0], "attributes": {"tenant": "a", "region": "west"}},
                {"id": "a-east", "values": [1.0, 0.0], "attributes": {"tenant": "a", "region": "east"}},
                {"id": "b-west", "values": [1.0, 0.0], "attributes": {"tenant": "b", "region": "west"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), 200, "{}", upsert.text().await.unwrap());

    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "mandatory_filter": {"op": "eq", "field": "region", "value": "west"},
            "purpose": "tenant and region scoped retrieval",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let response = client_with_bearer(&token)
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({"vector": [1.0, 0.0], "top_k": 10}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    let body: serde_json::Value = response.json().await.unwrap();
    let ids = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["a-west"]);
}

#[tokio::test]
async fn no_chained_minting() {
    let (base_url, _harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&admin, &base_url, 2).await;
    let principal_id = format!("service:no-chain-parent:{}", uuid::Uuid::new_v4().simple());
    assert_eq!(
        admin
            .post(format!("{base_url}/v1/security/principals"))
            .json(&json!({
                "principal_id": principal_id,
                "kind": "service",
                "display_name": "no-chain-parent"
            }))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let key = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({"principal_id": principal_id, "name": "no-chain-key"}))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let parent = client_with_bearer(
        key.json::<serde_json::Value>().await.unwrap()["api_key"]
            .as_str()
            .unwrap(),
    );
    for body in [
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }),
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["Query"]}
        }),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "one generation only",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();

    let chained = client_with_bearer(&token)
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "must be rejected",
            "expires_in_secs": 60
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(chained.status(), 403);
    assert_eq!(
        chained.json::<serde_json::Value>().await.unwrap()["code"],
        "forbidden"
    );
}

#[tokio::test]
async fn approval_two_person() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&admin, &base_url, 2).await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let parent_id = format!("service:approval-parent:{suffix}");
    let approver_id = format!("human:approval-reviewer:{suffix}");

    let mut credentials = Vec::new();
    for (principal_id, kind, display_name) in [
        (&parent_id, "service", "approval-parent"),
        (&approver_id, "human", "approval-reviewer"),
    ] {
        let principal = admin
            .post(format!("{base_url}/v1/security/principals"))
            .json(&json!({
                "principal_id": principal_id,
                "kind": kind,
                "display_name": display_name
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
            .post(format!("{base_url}/v1/security/keys"))
            .json(&json!({
                "principal_id": principal_id,
                "name": format!("{display_name}-key")
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(key.status(), 201, "{}", key.text().await.unwrap());
        credentials.push(
            key.json::<serde_json::Value>().await.unwrap()["api_key"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    let parent_bearer = credentials[0].clone();
    let approver_bearer = credentials[1].clone();
    let parent = client_with_bearer(&parent_bearer);

    for body in [
        json!({
            "principal_id": parent_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }),
        json!({
            "principal_id": parent_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["NamespaceDelete"]},
            "require_approval": ["NamespaceDelete"]
        }),
        json!({
            "principal_id": approver_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["NamespaceDelete"]}
        }),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }

    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["NamespaceDelete"],
            "namespaces": [namespace],
            "purpose": "delete only after independent approval",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let agent = client_with_bearer(&token);

    let missing = agent
        .delete(format!("{base_url}/v1/namespaces/{namespace}"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 403);
    assert_eq!(
        missing.json::<serde_json::Value>().await.unwrap()["code"],
        "approval_required"
    );

    let malformed_request_id = format!("approval-malformed-{suffix}");
    let malformed = agent
        .delete(format!("{base_url}/v1/namespaces/{namespace}"))
        .header("X-Request-Id", &malformed_request_id)
        .header("X-Zeppelin-Approval", "not-a-zpk1-credential")
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 403);
    assert_eq!(
        malformed.json::<serde_json::Value>().await.unwrap()["code"],
        "approval_required"
    );

    let parent_self_approval = agent
        .delete(format!("{base_url}/v1/namespaces/{namespace}"))
        .header("X-Zeppelin-Approval", &parent_bearer)
        .send()
        .await
        .unwrap();
    assert_eq!(parent_self_approval.status(), 403);
    assert_eq!(
        parent_self_approval
            .json::<serde_json::Value>()
            .await
            .unwrap()["code"],
        "approval_required"
    );

    let approved = agent
        .delete(format!("{base_url}/v1/namespaces/{namespace}"))
        .header("X-Zeppelin-Approval", &approver_bearer)
        .send()
        .await
        .unwrap();
    assert_eq!(approved.status(), 202, "{}", approved.text().await.unwrap());

    let mut records = Vec::new();
    for key in harness.store.list_prefix("_audit/").await.unwrap() {
        if !key.contains(&harness.prefix) {
            continue;
        }
        let body = harness.store.get(&key).await.unwrap();
        records.extend(
            String::from_utf8(body.to_vec())
                .unwrap()
                .lines()
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()),
        );
    }
    let deletion = records
        .iter()
        .find(|record| {
            record["action"] == "NamespaceDelete"
                && record["outcome"] == serde_json::Value::String("success".to_string())
        })
        .expect("approved delegated deletion audit record");
    assert!(deletion["principal_id"]
        .as_str()
        .unwrap()
        .starts_with("zdt1_"));
    assert_eq!(deletion["principal_kind"], "agent");
    assert_eq!(deletion["delegation_parent"], parent_id);
    assert_eq!(deletion["approval_principal_id"], approver_id);
    assert!(records.iter().any(|record| {
        record["request_id"] == malformed_request_id
            && record["action"] == "NamespaceDelete"
            && record["outcome"]["authn_failed"]["reason"] == "credential_unknown"
    }));
}

#[tokio::test]
async fn approval_constraints_narrow_delegated_vector_delete() {
    let (base_url, _harness, admin_bearer) = start_test_server().await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api(&admin, &base_url, 2).await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let parent_id = format!("service:delete-parent:{suffix}");
    let approver_id = format!("human:delete-approver:{suffix}");
    let mut credentials = Vec::new();
    for (principal_id, kind) in [(&parent_id, "service"), (&approver_id, "human")] {
        let principal = admin
            .post(format!("{base_url}/v1/security/principals"))
            .json(&json!({
                "principal_id": principal_id,
                "kind": kind,
                "display_name": principal_id
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
            .post(format!("{base_url}/v1/security/keys"))
            .json(&json!({"principal_id": principal_id, "name": "delete-key"}))
            .send()
            .await
            .unwrap();
        assert_eq!(key.status(), 201, "{}", key.text().await.unwrap());
        credentials.push(
            key.json::<serde_json::Value>().await.unwrap()["api_key"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    for body in [
        json!({
            "principal_id": parent_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }),
        json!({
            "principal_id": parent_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["VectorDelete"]}
        }),
        json!({
            "principal_id": approver_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["VectorDelete"]},
            "mandatory_filter": {"op": "eq", "field": "group", "value": "a"}
        }),
    ] {
        let grant = admin
            .post(format!("{base_url}/v1/security/grants"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let upsert = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "approved-row", "values": [1.0, 0.0], "attributes": {"group": "a"}},
                {"id": "outside-approval", "values": [0.9, 0.1], "attributes": {"group": "b"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), 200, "{}", upsert.text().await.unwrap());
    let parent = client_with_bearer(&credentials[0]);
    let minted = parent
        .post(format!("{base_url}/v1/security/tokens"))
        .json(&json!({
            "actions": ["VectorDelete"],
            "namespaces": [namespace],
            "purpose": "delete only inside independent approver slice",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let agent = client_with_bearer(&token);
    let deletion = agent
        .delete(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .header("X-Zeppelin-Approval", &credentials[1])
        .json(&json!({"ids": ["approved-row", "outside-approval"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(deletion.status(), 204, "{}", deletion.text().await.unwrap());

    let remaining = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({
            "vector": [1.0, 0.0],
            "top_k": 10,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        remaining.status(),
        200,
        "{}",
        remaining.text().await.unwrap()
    );
    let remaining = remaining.json::<serde_json::Value>().await.unwrap();
    let remaining_ids = remaining["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(remaining_ids, vec!["outside-approval"]);
}

#[tokio::test]
async fn agent_kind_in_audit_and_rate_limits() {
    let harness = TestHarness::new().await;
    let store = scoped_test_security_store(&harness.store, &harness.prefix);
    let writer = start_test_server_full(store.clone(), None, Config::default(), false, None).await;
    let admin = client_with_bearer(&writer.admin_bearer);
    let namespace = create_ns_api(&admin, &writer.base_url, 2).await;
    let principal_id = format!("service:rate-parent:{}", uuid::Uuid::new_v4().simple());
    assert_eq!(
        admin
            .post(format!("{}/v1/security/principals", writer.base_url))
            .json(&json!({
                "principal_id": principal_id,
                "kind": "service",
                "display_name": "rate-parent"
            }))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let key = admin
        .post(format!("{}/v1/security/keys", writer.base_url))
        .json(&json!({"principal_id": principal_id, "name": "rate-parent-key"}))
        .send()
        .await
        .unwrap();
    assert_eq!(key.status(), 201);
    let parent_bearer = key.json::<serde_json::Value>().await.unwrap()["api_key"]
        .as_str()
        .unwrap()
        .to_string();
    let parent = client_with_bearer(&parent_bearer);
    for body in [
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }),
        json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["Query"]}
        }),
    ] {
        let grant = admin
            .post(format!("{}/v1/security/grants", writer.base_url))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(grant.status(), 201, "{}", grant.text().await.unwrap());
    }
    let minted = parent
        .post(format!("{}/v1/security/tokens", writer.base_url))
        .json(&json!({
            "actions": ["Query"],
            "namespaces": [namespace],
            "purpose": "isolated rate bucket",
            "expires_in_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(minted.status(), 201, "{}", minted.text().await.unwrap());
    let token = minted.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();

    let mut limited = Config::default();
    limited.server.rate_limit_rps = 1_000;
    limited.server.rate_limit_burst = 1_000;
    limited.server.principal_rate_limit_rps = 1;
    limited.server.principal_rate_limit_burst = 1;
    limited.server.write_rate_limit_rps = 1_000;
    limited.server.write_rate_limit_burst = 1_000;
    limited.server.principal_write_rate_limit_rps = 1_000;
    limited.server.principal_write_rate_limit_burst = 1_000;
    let reader = start_test_server_full_without_rate_limit_override_and_admin_bearer(
        store,
        None,
        limited,
        &writer.admin_bearer,
    )
    .await;
    let agent = client_with_bearer(&token);
    let query = || {
        json!({
            "vector": [1.0, 0.0],
            "top_k": 1
        })
    };
    let first = agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            reader.base_url
        ))
        .json(&query())
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 200, "{}", first.text().await.unwrap());
    let exhausted = agent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            reader.base_url
        ))
        .json(&query())
        .send()
        .await
        .unwrap();
    assert_eq!(exhausted.status(), 429);

    let parent_after_agent_exhaustion = parent
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            reader.base_url
        ))
        .json(&query())
        .send()
        .await
        .unwrap();
    assert_eq!(
        parent_after_agent_exhaustion.status(),
        200,
        "{}",
        parent_after_agent_exhaustion.text().await.unwrap()
    );

    writer.shutdown().await;
    reader.shutdown().await;
}
