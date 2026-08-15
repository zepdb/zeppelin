mod common;

use bytes::Bytes;
use serde_json::{json, Value};
use std::time::Duration;
use ulid::Ulid;

use common::counting::counting_store;
use common::fault_injection::{
    fail_delete_once_matching, fail_get_after_successful_cas_matching, fail_put_once_matching,
    pause_first_cas_matching, toggle_get_failure_matching,
};
use common::harness::TestHarness;
use common::server::{
    client_with_bearer, create_ns_api, scoped_test_security_store, start_test_server_full,
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer,
};
use zeppelin::compaction::gc::{GcNamespaceIncarnation, GcRunner};
use zeppelin::config::Config;
use zeppelin::namespace::manager::{NamespaceMetadata, NamespaceState};
use zeppelin::namespace::NamespaceId;
use zeppelin::security::{
    CreatePreservationLock, PreservationReasonKind, PreservationScope, PreservationService,
    PrincipalId,
};
use zeppelin::storage::ObjectUserMetadata;
use zeppelin::time::Clock;
use zeppelin::wal::{Manifest, WalFragment};

async fn create_namespace_lock(client: &reqwest::Client, base_url: &str, namespace: &str) -> Value {
    let response = client
        .post(format!("{base_url}/v1/security/preservation"))
        .json(&json!({
            "scope": {"kind": "namespace", "namespace": namespace},
            "reason_kind": "litigation",
            "reason_text": "preserve the authoritative namespace state"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201, "{}", response.text().await.unwrap());
    response.json().await.unwrap()
}

#[tokio::test]
async fn strong_preservation_probe_sees_lock_missing_from_stale_cache() {
    let harness = TestHarness::new().await;
    let namespace =
        NamespaceId::new(harness.artifact_origin_namespace("strong-probe-target")).unwrap();
    let first = PreservationService::start(
        harness.store.clone(),
        Clock::system(),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    let second = PreservationService::start(
        harness.store.clone(),
        Clock::system(),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    second
        .create_lock(
            PrincipalId::new("human:strong-probe").unwrap(),
            CreatePreservationLock {
                scope: PreservationScope::Namespace {
                    namespace: namespace.clone(),
                },
                reason_kind: PreservationReasonKind::Litigation,
                reason_text: "strong probe regression".to_string(),
            },
        )
        .await
        .unwrap();
    assert!(!first.guard_namespace(&namespace).unwrap().is_locked());
    let (guard, proof) = first.guard_namespace_strong(&namespace).await.unwrap();
    assert!(guard.is_locked());
    assert!(proof.e_tag.is_some());
    harness.cleanup().await;
}

async fn create_release_approver(admin: &reqwest::Client, base_url: &str, suffix: &str) -> String {
    let principal_id = format!("human:preservation-approver:{suffix}");
    let response = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "human",
            "display_name": "preservation release approver"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201, "{}", response.text().await.unwrap());

    let response = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": principal_id,
            "name": "preservation approval key"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201, "{}", response.text().await.unwrap());
    let bearer = response.json::<Value>().await.unwrap()["api_key"]
        .as_str()
        .unwrap()
        .to_string();

    let response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": principal_id,
            "scope": {"kind": "global"},
            "actions": {
                "kind": "selected",
                "actions": ["PreservationRelease"]
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201, "{}", response.text().await.unwrap());
    bearer
}

async fn read_audit_records(store: &zeppelin::storage::ZeppelinStore, node_id: &str) -> Vec<Value> {
    let keys = store.list_prefix("_audit/").await.unwrap();
    let mut records = Vec::new();
    for key in keys
        .into_iter()
        .filter(|key| key.contains(&format!("/{node_id}/")))
    {
        let body = String::from_utf8(store.get(&key).await.unwrap().to_vec()).unwrap();
        records.extend(
            body.lines()
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_str(line).unwrap()),
        );
    }
    records
}

#[tokio::test]
async fn production_manager_rejects_ungoverned_delete_before_mutation() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;

    let error = server
        .namespace_manager
        .delete(&namespace)
        .await
        .expect_err("a production manager must require governed deletion");
    assert!(error.to_string().contains("requires governed deletion"));

    let metadata = server
        .namespace_manager
        .get_including_deleting(&namespace)
        .await
        .expect("rejected direct deletion must retain namespace metadata");
    assert_eq!(metadata.state, NamespaceState::Active);
    Manifest::read(&harness.store, &namespace)
        .await
        .expect("manifest read must succeed")
        .expect("rejected direct deletion must retain the live manifest");

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn lock_blocks_namespace_delete_and_release_needs_second_approver() {
    let harness = TestHarness::new().await;
    let security_store = scoped_test_security_store(&harness.store, &harness.prefix);
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let lock = create_namespace_lock(&admin, &server.base_url, &namespace).await;
    let lock_id = lock["lock_id"].as_str().unwrap();

    let blocked_request_id = "phase8-preservation-blocked-namespace";
    let blocked = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .header("x-request-id", blocked_request_id)
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), 409);
    let blocked_body = blocked.json::<Value>().await.unwrap();
    assert_eq!(blocked_body["code"], "preservation_locked");
    assert!(
        !blocked_body.to_string().contains(lock_id),
        "lock identifiers belong in audit evidence, not the response"
    );
    server.flush_audit().await;
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let blocked_audit = records
        .iter()
        .find(|record| record["request_id"] == blocked_request_id)
        .expect("blocked namespace delete must have durable audit evidence");
    assert_eq!(
        blocked_audit["params"]["preservation_blocked"]["surface"],
        "namespace_delete"
    );
    assert_eq!(
        blocked_audit["params"]["preservation_blocked"]["lock_ids"],
        json!([lock_id])
    );

    let unapproved = admin
        .post(format!(
            "{}/v1/security/preservation/{lock_id}/release",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(unapproved.status(), 403);
    assert_eq!(
        unapproved.json::<Value>().await.unwrap()["code"],
        "approval_required"
    );

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let approval_bearer = create_release_approver(&admin, &server.base_url, &suffix).await;
    let release_request_id = "phase8-preservation-release-success";
    let released = admin
        .post(format!(
            "{}/v1/security/preservation/{lock_id}/release",
            server.base_url
        ))
        .header("x-request-id", release_request_id)
        .header("x-zeppelin-approval", approval_bearer)
        .send()
        .await
        .unwrap();
    assert_eq!(released.status(), 200, "{}", released.text().await.unwrap());
    server.flush_audit().await;
    let records = read_audit_records(&harness.store, &server.audit_node_id).await;
    let release_audit = records
        .iter()
        .find(|record| record["request_id"] == release_request_id)
        .expect("committed release must have durable success audit evidence");
    assert_eq!(release_audit["outcome"], "success");
    assert_eq!(
        release_audit["params"]["preservation_release"]["lock_id"],
        lock_id
    );
    assert_eq!(
        release_audit["approval_principal_id"],
        format!("human:preservation-approver:{suffix}")
    );

    let head: Value = serde_json::from_slice(
        &security_store
            .get("_security/preservation/heads/locks.json")
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(!head["active_lock_ids"]
        .as_array()
        .unwrap()
        .iter()
        .any(|active| active == lock_id));
    let transition_key = head["last_transition_record"].as_str().unwrap();
    let transition: Value =
        serde_json::from_slice(&security_store.get(transition_key).await.unwrap()).unwrap();
    assert_eq!(transition["kind"], "release");
    assert_eq!(transition["lock_id"], lock_id);
    assert!(transition["previous_transition_record"].is_string());
    let released_record: Value = serde_json::from_slice(
        &security_store
            .get(transition["lock_record_key"].as_str().unwrap())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(released_record["state"], "released");

    let accepted = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202, "{}", accepted.text().await.unwrap());

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn successful_lock_cas_does_not_depend_on_a_post_commit_reread() {
    let harness = TestHarness::new().await;
    let (store, post_cas_get) = fail_get_after_successful_cas_matching(
        &harness.store,
        "_security/preservation/heads/locks.json",
        "_security/preservation/heads/locks.json",
    );
    let server = start_test_server_full(
        store,
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;

    let lock = create_namespace_lock(&admin, &server.base_url, &namespace).await;
    assert!(lock["lock_id"].is_string());
    assert_eq!(
        post_cas_get.failures_injected(),
        0,
        "the committed create must install its CAS-known state without a GET"
    );

    let blocked = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), 409);
    assert_eq!(post_cas_get.failures_injected(), 0);

    server
        .security
        .preservation_service()
        .unwrap()
        .refresh_once()
        .await
        .expect_err("the armed post-CAS GET fault must remain observable");
    assert_eq!(post_cas_get.failures_injected(), 1);

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn lock_blocks_snapshot_and_vector_delete_surfaces() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;

    let upsert = admin
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({
            "vectors": [{
                "id": "preserved-row",
                "values": [1.0, 0.0],
                "attributes": {"tenant": "a"}
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), 200, "{}", upsert.text().await.unwrap());
    let snapshot = admin
        .put(format!(
            "{}/v1/namespaces/{namespace}/snapshots/before-lock",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(snapshot.status(), 201, "{}", snapshot.text().await.unwrap());

    create_namespace_lock(&admin, &server.base_url, &namespace).await;

    let snapshot_delete = admin
        .delete(format!(
            "{}/v1/namespaces/{namespace}/snapshots/before-lock",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(snapshot_delete.status(), 409);
    assert_eq!(
        snapshot_delete.json::<Value>().await.unwrap()["code"],
        "preservation_locked"
    );

    for body in [
        json!({"ids": ["preserved-row"]}),
        json!({"filter": {"op": "eq", "field": "tenant", "value": "a"}}),
    ] {
        let vector_delete = admin
            .delete(format!(
                "{}/v1/namespaces/{namespace}/vectors",
                server.base_url
            ))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(vector_delete.status(), 409);
        assert_eq!(
            vector_delete.json::<Value>().await.unwrap()["code"],
            "preservation_locked"
        );
    }

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn global_lock_blocks_multiple_namespaces_and_destruction_surfaces() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace_a = create_ns_api(&admin, &server.base_url, 2).await;
    let namespace_b = create_ns_api(&admin, &server.base_url, 2).await;
    let snapshot = admin
        .put(format!(
            "{}/v1/namespaces/{namespace_b}/snapshots/global-lock",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(snapshot.status(), 201, "{}", snapshot.text().await.unwrap());

    let lock = admin
        .post(format!("{}/v1/security/preservation", server.base_url))
        .json(&json!({
            "scope": {"kind": "global"},
            "reason_kind": "regulatory",
            "reason_text": "retain every namespace during the global freeze"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(lock.status(), 201, "{}", lock.text().await.unwrap());

    let namespace_delete = admin
        .delete(format!("{}/v1/namespaces/{namespace_a}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(namespace_delete.status(), 409);

    let snapshot_delete = admin
        .delete(format!(
            "{}/v1/namespaces/{namespace_b}/snapshots/global-lock",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(snapshot_delete.status(), 409);

    let vector_delete = admin
        .delete(format!(
            "{}/v1/namespaces/{namespace_b}/vectors",
            server.base_url
        ))
        .json(&json!({"ids": ["global-preserved-row"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(vector_delete.status(), 409);

    let mut gc = GcRunner::new(server.store.clone(), Config::default().gc)
        .with_preservation_service(server.security.preservation_service().cloned());
    let report = gc
        .run_cycle_at(
            GcNamespaceIncarnation::new(namespace_b, server.clock.now()),
            server.clock.now(),
        )
        .await
        .unwrap();
    assert_eq!(report, Default::default());

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn destruction_record_is_durable_before_namespace_removal() {
    let harness = TestHarness::new().await;
    let (failing_store, failure) = fail_put_once_matching(&harness.store, "_audit/destruction/");
    let server = start_test_server_full(
        failing_store,
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;

    let failed = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(failed.status(), 500);
    assert_eq!(
        failed.json::<Value>().await.unwrap()["code"],
        "audit_unavailable"
    );
    assert_eq!(failure.failures_injected(), 1);

    let meta_bytes = harness
        .store
        .get(&NamespaceMetadata::object_store_key(&namespace))
        .await
        .unwrap();
    let meta = NamespaceMetadata::from_bytes(&meta_bytes).unwrap();
    assert_eq!(meta.state, NamespaceState::Active);
    assert!(meta.destruction_record_key.is_none());
    let destruction_key = meta
        .deletion_intent
        .as_ref()
        .expect("the active fenced state must retain its resumable intent")
        .destruction_record_key
        .clone();
    assert!(meta
        .deletion_intent
        .as_ref()
        .and_then(|intent| intent.fenced_generation)
        .is_some());
    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("manifest read must succeed")
        .expect("audit failure must leave the live manifest intact");
    let objects = harness
        .store
        .list_prefix_meta(&format!("{namespace}/"))
        .await
        .unwrap();
    let expected_object_count = objects.len() as u64;
    let expected_byte_count = objects.iter().map(|object| object.size).sum::<u64>();

    let accepted = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202, "{}", accepted.text().await.unwrap());

    let keys = harness
        .store
        .list_prefix("_audit/destruction/")
        .await
        .unwrap();
    let mut records = Vec::new();
    for key in keys {
        let record: Value =
            serde_json::from_slice(&harness.store.get(&key).await.unwrap()).unwrap();
        if record["namespace"] == namespace {
            records.push(record);
        }
    }
    assert_eq!(records.len(), 1);
    let record = &records[0];
    let bound_record: Value =
        serde_json::from_slice(&harness.store.get(&destruction_key).await.unwrap()).unwrap();
    assert_eq!(&bound_record, record);
    assert_eq!(record["namespace"], namespace);
    assert_eq!(record["manifest_version_destroyed"], manifest.version());
    assert_eq!(record["object_count"], expected_object_count);
    assert_eq!(record["byte_count"], expected_byte_count);
    assert_eq!(record["actor"], "zpk1_test_admin");
    assert!(record["decision_id"].is_string());
    assert!(record["ts"].is_string());

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn governed_delete_migrates_legacy_incarnation_before_binding_evidence() {
    let harness = TestHarness::new().await;
    let (server_store, metadata_delete_failure) =
        fail_delete_once_matching(&harness.store, "meta.json");
    let server = start_test_server_full(
        server_store,
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let metadata_key = NamespaceMetadata::object_store_key(&namespace);
    let (body, object_metadata) = harness
        .store
        .get_with_object_metadata(&metadata_key)
        .await
        .unwrap();
    let version = object_metadata
        .version
        .as_ref()
        .expect("real object storage must return a metadata version token");
    let original_incarnation = object_metadata
        .user_metadata
        .get("zeppelin-namespace-incarnation")
        .expect("new namespace metadata must carry an incarnation")
        .replace('-', "");
    harness
        .store
        .put_if_match_with_user_metadata(
            &metadata_key,
            body,
            version,
            &namespace,
            &ObjectUserMetadata::new(),
        )
        .await
        .unwrap();

    let accepted = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202, "{}", accepted.text().await.unwrap());

    let cleanup_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while metadata_delete_failure.failures_injected() == 0
        && tokio::time::Instant::now() < cleanup_deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        metadata_delete_failure.failures_injected(),
        1,
        "the fixture must retain the governed tombstone after background cleanup"
    );
    let tombstone =
        NamespaceMetadata::from_bytes(&harness.store.get(&metadata_key).await.unwrap()).unwrap();
    let expected_key = format!("_audit/destruction/{original_incarnation}.json");
    assert_eq!(
        tombstone.destruction_record_key.as_deref(),
        Some(expected_key.as_str()),
        "the tombstone must bind evidence to the migrated authoritative incarnation"
    );
    let evidence: Value = serde_json::from_slice(&harness.store.get(&expected_key).await.unwrap())
        .expect("destruction evidence must be valid JSON");
    assert_eq!(evidence["namespace"], namespace);

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn in_flight_manifest_recovery_write_cannot_resurrect_governed_delete() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let mut candidate = Manifest::read(&harness.store, &namespace)
        .await
        .unwrap()
        .expect("new namespace must have a manifest");
    let (paused_store, pause) =
        pause_first_cas_matching(&harness.store, format!("{namespace}/manifest.json"));
    let writer_namespace = namespace.clone();
    let writer =
        tokio::spawn(async move { candidate.write(&paused_store, &writer_namespace).await });
    pause.wait_until_paused().await;

    let accepted = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202, "{}", accepted.text().await.unwrap());
    pause.release();

    let error = writer
        .await
        .expect("recovery writer task must not panic")
        .expect_err("a writer paused before publication must lose to governed deletion");
    assert!(
        matches!(
            error,
            zeppelin::error::ZeppelinError::ManifestConflict { .. }
        ),
        "unexpected paused writer error: {error}"
    );
    assert!(
        Manifest::read(&harness.store, &namespace)
            .await
            .unwrap()
            .is_none(),
        "the stale recovery writer must not resurrect manifest visibility"
    );

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn destruction_evidence_survives_failure_after_record_before_manifest_removal() {
    let harness = TestHarness::new().await;
    let (store, delete_failure) = fail_delete_once_matching(&harness.store, "manifest.json");
    let server = start_test_server_full(
        store,
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let namespace_id = NamespaceId::new(namespace.clone()).unwrap();
    let (_, namespace_object_metadata) = harness
        .store
        .get_with_object_metadata(&NamespaceMetadata::object_store_key(&namespace))
        .await
        .unwrap();
    let expected_incarnation = namespace_object_metadata
        .user_metadata
        .get("zeppelin-namespace-incarnation")
        .expect("new namespace metadata must carry an incarnation")
        .to_string();
    let (preservation_guard, expected_preservation_head) = server
        .security
        .preservation_service()
        .expect("the full server must compose preservation governance")
        .guard_namespace_strong(&namespace_id)
        .await
        .unwrap();
    assert!(!preservation_guard.is_locked());
    let expected_preservation_head = serde_json::to_value(expected_preservation_head).unwrap();
    let (mut stale_manifest, stale_version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .unwrap()
        .expect("new namespace must have a live manifest");

    let failed = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(failed.status(), 500, "{}", failed.text().await.unwrap());
    assert_eq!(delete_failure.failures_injected(), 1);

    let meta_bytes = harness
        .store
        .get(&NamespaceMetadata::object_store_key(&namespace))
        .await
        .unwrap();
    let meta = NamespaceMetadata::from_bytes(&meta_bytes).unwrap();
    assert_eq!(meta.state, NamespaceState::Deleting);
    let evidence_key = meta
        .destruction_record_key
        .expect("the tombstone must retain the committed evidence reference");
    let evidence = harness.store.get(&evidence_key).await.unwrap();
    let record: Value = serde_json::from_slice(&evidence).unwrap();
    assert_eq!(record["namespace"], namespace);
    assert_eq!(
        record["incarnation"], expected_incarnation,
        "destruction evidence must bind the exact authoritative namespace incarnation"
    );
    assert_eq!(
        record["preservation_head"], expected_preservation_head,
        "destruction evidence must bind the exact strong preservation-head proof"
    );
    let fenced_manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("manifest read must succeed")
        .expect("the injected DELETE failure must leave the manifest intact");
    assert_eq!(
        record["manifest_version_destroyed"],
        fenced_manifest.version(),
        "destruction evidence must name the exact fenced manifest generation"
    );

    let cleanup_error = server
        .namespace_manager
        .finish_delete(&namespace, Duration::MAX)
        .await
        .expect_err("governed cleanup must not bypass the graph state machine");
    assert!(cleanup_error
        .to_string()
        .contains("must resume through NamespaceGraph"));

    let stale_error = stale_manifest
        .write_conditional(&harness.store, &namespace, &stale_version)
        .await
        .expect_err("the fence CAS must invalidate an in-flight writer's ETag");
    assert!(
        matches!(
            stale_error,
            zeppelin::error::ZeppelinError::ManifestConflict { .. }
        ),
        "unexpected stale writer error: {stale_error}"
    );

    let stale_unconditional_error = stale_manifest
        .write(&harness.store, &namespace)
        .await
        .expect_err("an unconditional stale writer must not overwrite the fence");
    assert!(
        matches!(
            stale_unconditional_error,
            zeppelin::error::ZeppelinError::NamespaceDeleting { .. }
        ),
        "unexpected unconditional stale writer error: {stale_unconditional_error}"
    );

    let (mut fenced_candidate, fenced_version) =
        Manifest::read_versioned(&harness.store, &namespace)
            .await
            .unwrap()
            .expect("failed manifest removal must retain its fenced generation");
    let mut unfenced_candidate = Manifest::new();
    let unfenced_error = unfenced_candidate
        .write_conditional(&harness.store, &namespace, &fenced_version)
        .await
        .expect_err("an unfenced candidate must not clear fenced current authority");
    assert!(
        matches!(
            unfenced_error,
            zeppelin::error::ZeppelinError::NamespaceDeleting { .. }
        ),
        "unexpected unfenced candidate error: {unfenced_error}"
    );
    let fenced_error = fenced_candidate
        .write_conditional(&harness.store, &namespace, &fenced_version)
        .await
        .expect_err("a writer that observes the fence must refuse publication");
    assert!(
        matches!(
            fenced_error,
            zeppelin::error::ZeppelinError::NamespaceDeleting { .. }
        ),
        "unexpected fenced writer error: {fenced_error}"
    );

    let accepted = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202, "{}", accepted.text().await.unwrap());
    let evidence_keys = harness
        .store
        .list_prefix("_audit/destruction/")
        .await
        .unwrap();
    assert_eq!(
        evidence_keys
            .iter()
            .filter(|key| key.as_str() == evidence_key)
            .count(),
        1,
        "retry must reuse the tombstone-bound immutable evidence"
    );

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn lock_created_after_evidence_blocks_graph_resume_until_release() {
    let harness = TestHarness::new().await;
    let (counted_store, counter) = counting_store(&harness.store);
    let (store, delete_failure) = fail_delete_once_matching(&counted_store, "manifest.json");
    let server = start_test_server_full(
        store,
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;

    let failed = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(failed.status(), 500, "{}", failed.text().await.unwrap());
    assert_eq!(delete_failure.failures_injected(), 1);

    let metadata = server
        .namespace_manager
        .get_including_deleting(&namespace)
        .await
        .unwrap();
    let evidence_key = metadata
        .destruction_record_key
        .expect("governed tombstone must bind its destruction evidence");
    let _: Value =
        serde_json::from_slice(&harness.store.get(&evidence_key).await.unwrap()).unwrap();

    let peer = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
        100 * 1024 * 1024,
        &server.admin_bearer,
    )
    .await;
    let peer_admin = client_with_bearer(&peer.admin_bearer);
    let lock = create_namespace_lock(&peer_admin, &peer.base_url, &namespace).await;
    let lock_id = lock["lock_id"].as_str().unwrap();

    let head_gets_before = counter.gets_matching("_security/preservation/heads/locks.json");
    let blocked = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), 409);
    assert_eq!(
        blocked.json::<Value>().await.unwrap()["code"],
        "preservation_locked"
    );
    assert!(
        counter.gets_matching("_security/preservation/heads/locks.json") > head_gets_before,
        "graph resume must refresh the authoritative preservation head"
    );
    Manifest::read(&harness.store, &namespace)
        .await
        .expect("manifest read must succeed")
        .expect("blocked governed commit must retain the fenced manifest");

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let approval_bearer = create_release_approver(&peer_admin, &peer.base_url, &suffix).await;
    let released = peer_admin
        .post(format!(
            "{}/v1/security/preservation/{lock_id}/release",
            peer.base_url
        ))
        .header("x-zeppelin-approval", approval_bearer)
        .send()
        .await
        .unwrap();
    assert_eq!(released.status(), 200, "{}", released.text().await.unwrap());

    let head_gets_before = counter.gets_matching("_security/preservation/heads/locks.json");
    let resumed = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resumed.status(), 202, "{}", resumed.text().await.unwrap());
    assert!(
        counter.gets_matching("_security/preservation/heads/locks.json") > head_gets_before,
        "resumed graph deletion must refresh released lock authority"
    );
    assert!(
        Manifest::read(&harness.store, &namespace)
            .await
            .expect("manifest read must succeed")
            .is_none(),
        "successful governed commit must remove the exact fenced manifest"
    );

    peer.shutdown().await;
    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn lock_created_after_tombstone_blocks_resumed_physical_cleanup() {
    let harness = TestHarness::new().await;
    let (store, delete_failure) = fail_delete_once_matching(&harness.store, "manifest.json");
    let server = start_test_server_full(
        store,
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let failed = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(failed.status(), 500, "{}", failed.text().await.unwrap());
    assert_eq!(delete_failure.failures_injected(), 1);

    create_namespace_lock(&admin, &server.base_url, &namespace).await;
    let blocked = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), 409);
    assert_eq!(
        blocked.json::<Value>().await.unwrap()["code"],
        "preservation_locked"
    );
    Manifest::read(&harness.store, &namespace)
        .await
        .expect("manifest read must succeed")
        .expect("blocked cleanup must retain the manifest");
    harness
        .store
        .get(&NamespaceMetadata::object_store_key(&namespace))
        .await
        .expect("blocked cleanup must retain the deletion tombstone");
    let deferrals = harness
        .store
        .list_prefix(&format!("{}/_audit/preservation/", harness.prefix))
        .await
        .unwrap();
    assert!(!deferrals.is_empty());

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn lock_survives_restart_and_remains_s3_authoritative() {
    let harness = TestHarness::new().await;
    let first = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&first.admin_bearer);
    let namespace = create_ns_api(&admin, &first.base_url, 2).await;
    create_namespace_lock(&admin, &first.base_url, &namespace).await;
    let admin_bearer = first.admin_bearer.clone();
    first.shutdown().await;

    let second = common::server::start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
        100 * 1024 * 1024,
        &admin_bearer,
    )
    .await;
    let admin = client_with_bearer(&second.admin_bearer);
    let blocked = admin
        .delete(format!("{}/v1/namespaces/{namespace}", second.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), 409);
    assert_eq!(
        blocked.json::<Value>().await.unwrap()["code"],
        "preservation_locked"
    );

    second.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn namespace_filter_lock_conservatively_blocks_uncertain_overlap() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;

    let lock = admin
        .post(format!("{}/v1/security/preservation", server.base_url))
        .json(&json!({
            "scope": {
                "kind": "namespace_filter",
                "namespace": namespace,
                "filter": {"op": "eq", "field": "tenant", "value": "a"}
            },
            "reason_kind": "investigation",
            "reason_text": "retain tenant a while filter disjointness remains conservative"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(lock.status(), 201, "{}", lock.text().await.unwrap());

    let blocked = admin
        .delete(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({
            "filter": {"op": "eq", "field": "tenant", "value": "b"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), 409);
    assert_eq!(
        blocked.json::<Value>().await.unwrap()["code"],
        "preservation_locked"
    );

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn compaction_defers_existing_tombstone_and_preserves_as_of_recovery_under_lock() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        Config::default(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let upsert = admin
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({"vectors": [{"id": "held", "values": [1.0, 0.0]}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert.status(), 200, "{}", upsert.text().await.unwrap());
    let before_delete = Manifest::read(&server.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    let historical_generation = before_delete.version().to_string();
    let delete = admin
        .delete(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({"ids": ["held"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(delete.status(), 204, "{}", delete.text().await.unwrap());
    create_namespace_lock(&admin, &server.base_url, &namespace).await;

    let before = Manifest::read(&server.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    assert!(
        before
            .uncompacted_fragments()
            .iter()
            .any(|fragment| fragment.delete_count > 0),
        "test setup must publish an existing tombstone before locking"
    );
    let result = server.compactor.compact(&namespace).await.unwrap();
    let after = Manifest::read(&server.store, &namespace)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.fragments_removed, 0);
    assert_eq!(result.vectors_compacted, 0);
    assert_eq!(after.version(), before.version());
    assert_eq!(
        after.uncompacted_fragments(),
        before.uncompacted_fragments(),
        "deferring the whole compaction preserves tombstones and all source fragments"
    );
    let deferrals = harness
        .store
        .list_prefix(&format!("{}/_audit/preservation/", harness.prefix))
        .await
        .unwrap();
    assert_eq!(deferrals.len(), 1);
    let deferral: Value =
        serde_json::from_slice(&harness.store.get(&deferrals[0]).await.unwrap()).unwrap();
    assert_eq!(deferral["event"], "compaction_deferred_preservation");
    assert_eq!(deferral["namespace"], namespace);

    let historical = admin
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .query(&[("as_of", historical_generation.as_str())])
        .json(&json!({
            "vector": [1.0, 0.0],
            "top_k": 1,
            "consistency": "strong"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(historical.status(), 200);
    let historical = historical.json::<Value>().await.unwrap();
    assert_eq!(historical["results"][0]["id"], "held");

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn gc_defers_before_listing_or_deleting_locked_namespace() {
    let harness = TestHarness::new().await;
    let mut config = Config::default();
    config.gc.horizon_secs = 0;
    config.gc.allow_unsafe_short_horizon = true;
    let (counted_store, counter) = counting_store(&harness.store);
    let server = start_test_server_full(
        counted_store,
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    let lock = create_namespace_lock(&admin, &server.base_url, &namespace).await;
    let lock_id = lock["lock_id"].as_str().unwrap();

    let garbage_key = WalFragment::object_store_key(&namespace, &Ulid::from_parts(1, 1));
    server
        .store
        .put_create(&garbage_key, Bytes::from_static(b"preserved"))
        .await
        .unwrap();
    let mut runner = GcRunner::new(server.store.clone(), config.gc)
        .with_preservation_service(server.security.preservation_service().cloned());
    let namespace_prefix = format!("{namespace}/");
    let lists_before = counter.list_calls_for_prefix(&namespace_prefix);
    let gets_before = counter.total_observed_gets();
    let report = runner
        .run_cycle_at(
            GcNamespaceIncarnation::new(namespace.clone(), server.clock.now()),
            server.clock.now(),
        )
        .await
        .unwrap();

    assert_eq!(report, Default::default());
    assert_eq!(
        counter.list_calls_for_prefix(&namespace_prefix),
        lists_before,
        "a locked GC cycle must consult the in-memory lock set before namespace LIST"
    );
    assert_eq!(
        counter.total_observed_gets(),
        gets_before,
        "a locked GC cycle must not perform object GETs before deferral"
    );
    assert_eq!(
        server.store.get(&garbage_key).await.unwrap(),
        Bytes::from_static(b"preserved")
    );
    let deferrals = harness
        .store
        .list_prefix(&format!("{}/_audit/preservation/", harness.prefix))
        .await
        .unwrap();
    assert_eq!(deferrals.len(), 1);
    let deferral: Value =
        serde_json::from_slice(&harness.store.get(&deferrals[0]).await.unwrap()).unwrap();
    assert_eq!(deferral["event"], "gc_deferred_preservation");

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let approval_bearer = create_release_approver(&admin, &server.base_url, &suffix).await;
    let released = admin
        .post(format!(
            "{}/v1/security/preservation/{lock_id}/release",
            server.base_url
        ))
        .header("x-zeppelin-approval", approval_bearer)
        .send()
        .await
        .unwrap();
    assert_eq!(released.status(), 200, "{}", released.text().await.unwrap());

    let cleanup = runner
        .run_cycle_at(
            GcNamespaceIncarnation::new(namespace, server.clock.now()),
            server.clock.now(),
        )
        .await
        .unwrap();
    assert!(
        cleanup.objects_deleted >= 1,
        "the first unlocked cycle must reclaim the retained orphan at a zero test horizon: {cleanup:?}"
    );
    assert!(
        matches!(
            server.store.get(&garbage_key).await,
            Err(zeppelin::error::ZeppelinError::NotFound { .. })
        ),
        "the retained orphan must become collectible after release"
    );

    server.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test]
async fn stale_lock_cache_fails_closed_while_reads_remain_available() {
    let harness = TestHarness::new().await;
    let (faulty_store, fault) =
        toggle_get_failure_matching(&harness.store, "_security/preservation/heads/locks.json");
    let mut config = Config::default();
    config.security.policy_refresh_secs = 1;
    let server = start_test_server_full(
        faulty_store,
        Some(harness.prefix.clone()),
        config,
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api(&admin, &server.base_url, 2).await;
    create_namespace_lock(&admin, &server.base_url, &namespace).await;

    fault.enable();
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert!(fault.failures_injected() >= 2);

    let read = admin
        .get(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), 200, "{}", read.text().await.unwrap());

    let blocked = admin
        .delete(format!("{}/v1/namespaces/{namespace}", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), 503);
    assert_eq!(
        blocked.json::<Value>().await.unwrap()["code"],
        "preservation_state_unavailable"
    );

    fault.disable();
    server.shutdown().await;
    harness.cleanup().await;
}
