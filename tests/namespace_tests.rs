mod common;

use common::assertions::{assert_s3_object_exists, assert_s3_object_not_exists};
use common::harness::TestHarness;

use chrono::Utc;
use zeppelin::config::IndexingConfig;
use zeppelin::error::ZeppelinError;
use zeppelin::namespace::manager::{
    CompactionHealth, NamespaceIndexConfig, NamespaceMetadata, NamespaceState,
};
use zeppelin::namespace::NamespaceManager;
use zeppelin::storage::ObjectUserMetadata;
use zeppelin::types::DistanceMetric;
use zeppelin::types::IndexType;
use zeppelin::wal::{LeaseManager, Manifest};

/// Create a URL-safe namespace name scoped to this test's prefix (no slashes).
fn ns(harness: &TestHarness, suffix: &str) -> String {
    format!("{}-{suffix}", harness.prefix)
}

/// Clean up all S3 objects under a namespace prefix.
async fn cleanup_ns(store: &zeppelin::storage::ZeppelinStore, ns: &str) {
    let prefix = format!("{ns}/");
    let _ = store.delete_prefix(&prefix).await;
}

#[tokio::test]
async fn test_create_namespace() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "ns-create");

    let manager = NamespaceManager::new(harness.store.clone());
    let meta = manager
        .create(&name, 128, DistanceMetric::Cosine)
        .await
        .unwrap();

    assert_eq!(meta.name, name);
    assert_eq!(meta.dimensions, 128);
    assert_eq!(meta.distance_metric, DistanceMetric::Cosine);
    assert_eq!(meta.vector_count, 0);

    // Verify meta.json exists on S3
    let meta_key = NamespaceMetadata::s3_key(&name);
    assert_s3_object_exists(&harness.store, &meta_key).await;

    // Verify manifest.json exists on S3
    let manifest_key = Manifest::s3_key(&name);
    assert_s3_object_exists(&harness.store, &manifest_key).await;

    cleanup_ns(&harness.store, &name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_namespace_incarnation_survives_metadata_cas_updates() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "incarnation-cas");
    let manager = NamespaceManager::new(harness.store.clone());
    let created = manager
        .create(&name, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    let incarnation = created
        .incarnation_id
        .clone()
        .expect("new namespaces must carry an incarnation ID");
    let mut body_without_runtime_identity = created.clone();
    body_without_runtime_identity.incarnation_id = None;
    assert_eq!(
        created.to_bytes().unwrap(),
        body_without_runtime_identity.to_bytes().unwrap(),
        "the runtime incarnation identity must not change meta.json bytes"
    );

    let configured = manager
        .update_index_config(
            &LeaseManager::new(
                harness.store.clone(),
                "incarnation-config-update".to_string(),
                std::time::Duration::from_secs(30),
            ),
            &name,
            NamespaceIndexConfig::from_indexing_config(&IndexingConfig::default()),
        )
        .await
        .unwrap();
    assert_eq!(configured.incarnation_id.as_ref(), Some(&incarnation));

    let failed = manager
        .record_compaction_failure(
            &name,
            &ZeppelinError::Index("incarnation preservation proof".to_string()),
        )
        .await
        .unwrap();
    assert_eq!(failed.incarnation_id.as_ref(), Some(&incarnation));

    let deleting = manager.start_delete(&name).await.unwrap();
    assert_eq!(deleting.incarnation_id.as_ref(), Some(&incarnation));

    let remote = NamespaceManager::new(harness.store.clone())
        .get_including_deleting(&name)
        .await
        .unwrap();
    assert_eq!(remote.incarnation_id.as_ref(), Some(&incarnation));

    manager
        .finish_delete(&name, std::time::Duration::MAX)
        .await
        .unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn test_index_config_update_waits_for_the_namespace_writer_lease() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "index-config-writer-lease");
    let manager = NamespaceManager::new(harness.store.clone());
    manager
        .create(&name, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();

    let blocker = LeaseManager::new(
        harness.store.clone(),
        "index-config-blocker".to_string(),
        std::time::Duration::from_secs(30),
    );
    let held = blocker.acquire(&name).await.unwrap();
    let updater = LeaseManager::new(
        harness.store.clone(),
        "index-config-updater".to_string(),
        std::time::Duration::from_secs(30),
    );
    let desired = NamespaceIndexConfig::from_indexing_config(&IndexingConfig::default());

    let error = manager
        .update_index_config(&updater, &name, desired.clone())
        .await
        .expect_err("index config must not publish while another writer owns the lease");
    assert!(matches!(error, ZeppelinError::LeaseHeld { .. }));

    blocker.release(&name, &held).await.unwrap();
    let updated = manager
        .update_index_config(&updater, &name, desired.clone())
        .await
        .unwrap();
    assert_eq!(updated.index_config.as_ref(), Some(&desired));

    cleanup_ns(&harness.store, &name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_legacy_namespace_incarnation_migrates_once_without_changing_body() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "legacy-incarnation-migration");
    let key = NamespaceMetadata::s3_key(&name);
    let creator = NamespaceManager::new(harness.store.clone());
    let created = creator
        .create(&name, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    let original_incarnation = created
        .incarnation_id
        .clone()
        .expect("new namespace must have an incarnation");

    let (original_body, original_object_metadata) =
        harness.store.get_with_object_metadata(&key).await.unwrap();
    assert!(
        original_object_metadata
            .user_metadata
            .get("zeppelin-namespace-incarnation")
            .is_some(),
        "new namespace setup must begin with an incarnation"
    );

    // Re-publish the exact body with an unrelated header but without the
    // incarnation to model a legacy namespace. Migration must preserve both.
    let original_etag = original_object_metadata
        .e_tag
        .as_deref()
        .expect("authoritative metadata fixture must have an ETag");
    let mut legacy_user_metadata = ObjectUserMetadata::new();
    legacy_user_metadata.insert("legacy-fixture-marker", "preserve-me");
    harness
        .store
        .put_if_match_with_user_metadata(
            &key,
            original_body.clone(),
            original_etag,
            &name,
            &legacy_user_metadata,
        )
        .await
        .unwrap();
    let (legacy_body, legacy_object_metadata) =
        harness.store.get_with_object_metadata(&key).await.unwrap();
    assert_eq!(legacy_body, original_body);
    assert!(legacy_object_metadata
        .user_metadata
        .get("zeppelin-namespace-incarnation")
        .is_none());
    assert_eq!(
        legacy_object_metadata
            .user_metadata
            .get("legacy-fixture-marker"),
        Some("preserve-me")
    );

    // Ordinary reads and lists remain read-only: only the explicit guarded-
    // write seam is allowed to migrate legacy metadata.
    let ordinary_reader = NamespaceManager::new(harness.store.clone());
    assert!(ordinary_reader
        .get(&name)
        .await
        .unwrap()
        .incarnation_id
        .is_none());
    let ordinary_lister = NamespaceManager::new(harness.store.clone());
    let listed = ordinary_lister
        .list(Some(&name))
        .await
        .unwrap()
        .into_iter()
        .find(|metadata| metadata.name == name)
        .expect("legacy namespace must be listed");
    assert!(listed.incarnation_id.is_none());
    let (body_after_reads, metadata_after_reads) =
        harness.store.get_with_object_metadata(&key).await.unwrap();
    assert_eq!(body_after_reads, original_body);
    assert!(metadata_after_reads
        .user_metadata
        .get("zeppelin-namespace-incarnation")
        .is_none());

    // Independent nodes may race to migrate. Exactly one CAS candidate wins;
    // every caller returns the same authoritative ID.
    let left = NamespaceManager::new(harness.store.clone());
    let right = NamespaceManager::new(harness.store.clone());
    let (left_meta, right_meta) = tokio::join!(
        left.get_active_metadata_for_guarded_write(&name),
        right.get_active_metadata_for_guarded_write(&name)
    );
    let left_meta = left_meta.unwrap();
    let right_meta = right_meta.unwrap();
    let left_id = left_meta
        .incarnation_id
        .expect("migrated metadata must carry an incarnation");
    let right_id = right_meta
        .incarnation_id
        .expect("racing migration must return an incarnation");
    assert_eq!(left_id, right_id);
    assert_eq!(
        left_id, original_incarnation,
        "metadata migration must adopt the incarnation already bound in the manifest"
    );

    let (migrated_body, migrated_object_metadata) =
        harness.store.get_with_object_metadata(&key).await.unwrap();
    assert_eq!(
        migrated_body, original_body,
        "migration must preserve the JSON body and updated_at byte-for-byte"
    );
    assert!(migrated_object_metadata
        .user_metadata
        .get("zeppelin-namespace-incarnation")
        .is_some());
    assert_eq!(
        migrated_object_metadata
            .user_metadata
            .get("legacy-fixture-marker"),
        Some("preserve-me")
    );

    let idempotent_reader = NamespaceManager::new(harness.store.clone());
    assert_eq!(
        idempotent_reader
            .get_active_metadata_for_guarded_write(&name)
            .await
            .unwrap()
            .incarnation_id
            .as_ref(),
        Some(&left_id)
    );
    assert_eq!(
        idempotent_reader
            .get(&name)
            .await
            .unwrap()
            .incarnation_id
            .as_ref(),
        Some(&left_id)
    );

    cleanup_ns(&harness.store, &name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_legacy_creating_namespace_recovers_manifest_incarnation() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "legacy-creating-incarnation");
    let key = NamespaceMetadata::s3_key(&name);
    let creator = NamespaceManager::new(harness.store.clone());
    let created = creator
        .create(&name, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    let expected_incarnation = created
        .incarnation_id
        .clone()
        .expect("new namespace must have an incarnation");

    // Model a legacy process that reserved meta.json as creating without the
    // user-metadata identity while the bootstrap manifest already carried it.
    let mut legacy_creating = created;
    legacy_creating.state = NamespaceState::Creating;
    legacy_creating.incarnation_id = None;
    harness
        .store
        .put(&key, legacy_creating.to_bytes().unwrap())
        .await
        .unwrap();

    let recovering_manager = NamespaceManager::new(harness.store.clone());
    let recovered = recovering_manager.get(&name).await.unwrap();
    assert_eq!(recovered.state, NamespaceState::Active);
    assert_eq!(
        recovered.incarnation_id.as_ref(),
        Some(&expected_incarnation),
        "creating recovery must adopt the bootstrap manifest identity"
    );

    let authoritative = NamespaceManager::new(harness.store.clone())
        .get_active_metadata_for_guarded_write(&name)
        .await
        .unwrap();
    assert_eq!(
        authoritative.incarnation_id.as_ref(),
        Some(&expected_incarnation)
    );

    cleanup_ns(&harness.store, &name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_legacy_creating_namespace_without_manifest_mints_incarnation() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "legacy-creating-missing-manifest");
    let metadata_key = NamespaceMetadata::s3_key(&name);
    let manifest_key = Manifest::s3_key(&name);
    let creator = NamespaceManager::new(harness.store.clone());
    let mut legacy_creating = creator
        .create(&name, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();
    legacy_creating.state = NamespaceState::Creating;
    legacy_creating.incarnation_id = None;
    harness
        .store
        .put(&metadata_key, legacy_creating.to_bytes().unwrap())
        .await
        .unwrap();
    harness.store.delete(&manifest_key).await.unwrap();

    let recovering_manager = NamespaceManager::new(harness.store.clone());
    let recovered = recovering_manager.get(&name).await.unwrap();
    assert_eq!(recovered.state, NamespaceState::Active);
    assert!(recovered.incarnation_id.is_some());
    assert_s3_object_exists(&harness.store, &manifest_key).await;

    let authoritative = NamespaceManager::new(harness.store.clone())
        .get_active_metadata_for_guarded_write(&name)
        .await
        .unwrap();
    assert_eq!(authoritative.incarnation_id, recovered.incarnation_id);

    cleanup_ns(&harness.store, &name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_legacy_creating_namespace_resumes_after_manifest_binding_crash() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "legacy-creating-post-bind-crash");
    let metadata_key = NamespaceMetadata::s3_key(&name);
    let manifest_key = Manifest::s3_key(&name);
    let creator = NamespaceManager::new(harness.store.clone());
    let created = creator
        .create(&name, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();

    // Replace the new-format bootstrap with a legacy unbound generation one.
    harness.store.delete(&manifest_key).await.unwrap();
    harness
        .store
        .delete_prefix(&Manifest::history_prefix(&name))
        .await
        .unwrap();
    let mut legacy_manifest = Manifest::new_at(created.created_at);
    legacy_manifest.write(&harness.store, &name).await.unwrap();
    assert_eq!(legacy_manifest.version(), 1);

    let mut legacy_creating = created;
    legacy_creating.state = NamespaceState::Creating;
    legacy_creating.incarnation_id = None;
    harness
        .store
        .put(&metadata_key, legacy_creating.to_bytes().unwrap())
        .await
        .unwrap();

    // The explicit migration CAS succeeds, then the active-only wrapper rejects
    // the still-creating state. Bind the manifest and model a crash before the
    // final metadata activation CAS.
    let migrating_manager = NamespaceManager::new(harness.store.clone());
    let migration_error = migrating_manager
        .get_active_metadata_for_guarded_write(&name)
        .await
        .expect_err("creating metadata must not be returned to a guarded write");
    assert!(matches!(
        migration_error,
        ZeppelinError::ManifestConflict { .. }
    ));
    let (_, migrated_metadata) = harness
        .store
        .get_with_object_metadata(&metadata_key)
        .await
        .unwrap();
    let incarnation = uuid::Uuid::parse_str(
        migrated_metadata
            .user_metadata
            .get("zeppelin-namespace-incarnation")
            .expect("metadata migration must publish an incarnation"),
    )
    .unwrap();
    let (bound_manifest, _) =
        Manifest::read_versioned_required_for_incarnation(&harness.store, &name, incarnation)
            .await
            .unwrap();
    assert_eq!(bound_manifest.version(), 2);

    let recovered = NamespaceManager::new(harness.store.clone())
        .get(&name)
        .await
        .unwrap();
    assert_eq!(recovered.state, NamespaceState::Active);
    assert_eq!(
        recovered.incarnation_id,
        NamespaceManager::new(harness.store.clone())
            .get_active_metadata_for_guarded_write(&name)
            .await
            .unwrap()
            .incarnation_id
    );

    cleanup_ns(&harness.store, &name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_active_legacy_metadata_without_manifest_fails_before_migration() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "active-legacy-missing-manifest");
    let metadata_key = NamespaceMetadata::s3_key(&name);
    let manifest_key = Manifest::s3_key(&name);
    let creator = NamespaceManager::new(harness.store.clone());
    creator
        .create(&name, 16, DistanceMetric::Euclidean)
        .await
        .unwrap();

    let (body, _) = harness
        .store
        .get_with_object_metadata(&metadata_key)
        .await
        .unwrap();
    harness
        .store
        .put(&metadata_key, body.clone())
        .await
        .unwrap();
    harness.store.delete(&manifest_key).await.unwrap();

    let manager = NamespaceManager::new(harness.store.clone());
    let error = manager
        .get_active_metadata_for_guarded_write(&name)
        .await
        .expect_err("active metadata without a manifest must fail as corrupt state");
    assert!(matches!(error, ZeppelinError::Serialization(_)));

    let (body_after_failure, object_metadata_after_failure) = harness
        .store
        .get_with_object_metadata(&metadata_key)
        .await
        .unwrap();
    assert_eq!(body_after_failure, body);
    assert!(
        object_metadata_after_failure
            .user_metadata
            .get("zeppelin-namespace-incarnation")
            .is_none(),
        "integrity failure must occur before metadata migration"
    );

    cleanup_ns(&harness.store, &name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_get_namespace_from_registry() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "ns-get-reg");

    let manager = NamespaceManager::new(harness.store.clone());
    manager
        .create(&name, 64, DistanceMetric::Euclidean)
        .await
        .unwrap();

    // Get should hit registry
    let meta = manager.get(&name).await.unwrap();
    assert_eq!(meta.name, name);
    assert_eq!(meta.dimensions, 64);

    cleanup_ns(&harness.store, &name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_get_namespace_s3_fallback() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "ns-get-s3");

    // Create with one manager
    let manager1 = NamespaceManager::new(harness.store.clone());
    manager1
        .create(&name, 32, DistanceMetric::DotProduct)
        .await
        .unwrap();

    // Get with a fresh manager (empty registry → falls back to S3)
    let manager2 = NamespaceManager::new(harness.store.clone());
    let meta = manager2.get(&name).await.unwrap();
    assert_eq!(meta.name, name);
    assert_eq!(meta.dimensions, 32);
    assert_eq!(meta.distance_metric, DistanceMetric::DotProduct);

    cleanup_ns(&harness.store, &name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_list_namespaces() {
    let harness = TestHarness::new().await;
    let ns1 = ns(&harness, "ns-list-a");
    let ns2 = ns(&harness, "ns-list-b");

    let manager = NamespaceManager::new(harness.store.clone());
    manager
        .create(&ns1, 16, DistanceMetric::Cosine)
        .await
        .unwrap();
    manager
        .create(&ns2, 32, DistanceMetric::Euclidean)
        .await
        .unwrap();

    // List all namespaces and filter by our test prefix
    let namespaces = manager.list(None).await.unwrap();
    let names: Vec<&str> = namespaces.iter().map(|m| m.name.as_str()).collect();
    assert!(names.contains(&ns1.as_str()), "expected {ns1} in {names:?}");
    assert!(names.contains(&ns2.as_str()), "expected {ns2} in {names:?}");

    cleanup_ns(&harness.store, &ns1).await;
    cleanup_ns(&harness.store, &ns2).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_list_namespaces_ignores_nested_meta_objects() {
    let harness = TestHarness::new().await;
    let valid_ns = ns(&harness, "ns-list-delimited");
    let nested_ns = format!("{}/segments/seg_cruft", harness.prefix);
    let nested_meta_key = NamespaceMetadata::s3_key(&nested_ns);

    let manager = NamespaceManager::new(harness.store.clone());
    manager
        .create(&valid_ns, 16, DistanceMetric::Cosine)
        .await
        .unwrap();

    let now = Utc::now();
    let nested_meta = NamespaceMetadata {
        name: nested_ns.clone(),
        dimensions: 16,
        distance_metric: DistanceMetric::Cosine,
        index_type: IndexType::default(),
        vector_count: 0,
        created_at: now,
        updated_at: now,
        state: NamespaceState::Active,
        destruction_record_key: None,
        full_text_search: std::collections::HashMap::new(),
        index_config: None,
        compaction_health: CompactionHealth::default(),
        creation_kind: zeppelin::namespace::branching::NamespaceCreationKind::Root,
        branch_identity: None,
        branch_prepare: None,
        incarnation_id: None,
    };
    harness
        .store
        .put(&nested_meta_key, nested_meta.to_bytes().unwrap())
        .await
        .unwrap();

    let namespaces = manager.list(None).await.unwrap();
    let names: Vec<&str> = namespaces.iter().map(|m| m.name.as_str()).collect();
    assert!(
        names.contains(&valid_ns.as_str()),
        "expected valid namespace {valid_ns} in {names:?}"
    );
    assert!(
        !names.contains(&nested_ns.as_str()),
        "recursive namespace listing must not treat nested metadata as a namespace: {names:?}"
    );

    cleanup_ns(&harness.store, &valid_ns).await;
    let _ = harness.store.delete(&nested_meta_key).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_delete_namespace() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "ns-delete");

    let manager = NamespaceManager::new(harness.store.clone());
    manager
        .create(&name, 64, DistanceMetric::Cosine)
        .await
        .unwrap();

    // Delete
    manager.delete(&name).await.unwrap();

    // Verify meta.json is gone
    let meta_key = NamespaceMetadata::s3_key(&name);
    assert_s3_object_not_exists(&harness.store, &meta_key).await;

    // Verify get fails
    let result = manager.get(&name).await;
    assert!(matches!(
        result,
        Err(ZeppelinError::NamespaceNotFound { .. })
    ));

    harness.cleanup().await;
}

#[tokio::test]
async fn test_duplicate_create() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "ns-dup");

    let manager = NamespaceManager::new(harness.store.clone());
    manager
        .create(&name, 64, DistanceMetric::Cosine)
        .await
        .unwrap();

    let result = manager.create(&name, 64, DistanceMetric::Cosine).await;
    assert!(matches!(
        result,
        Err(ZeppelinError::NamespaceAlreadyExists { .. })
    ));

    cleanup_ns(&harness.store, &name).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_get_nonexistent() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "ns-nonexistent");

    let manager = NamespaceManager::new(harness.store.clone());
    let result = manager.get(&name).await;
    assert!(matches!(
        result,
        Err(ZeppelinError::NamespaceNotFound { .. })
    ));

    harness.cleanup().await;
}

#[tokio::test]
async fn test_scan_and_register() {
    let harness = TestHarness::new().await;
    let ns1 = ns(&harness, "ns-scan-a");
    let ns2 = ns(&harness, "ns-scan-b");

    // Create namespaces with one manager
    let manager1 = NamespaceManager::new(harness.store.clone());
    manager1
        .create(&ns1, 16, DistanceMetric::Cosine)
        .await
        .unwrap();
    manager1
        .create(&ns2, 32, DistanceMetric::Euclidean)
        .await
        .unwrap();

    // Fresh manager with scan_and_register
    let manager2 = NamespaceManager::new(harness.store.clone());
    let count = manager2.scan_and_register().await.unwrap();
    assert!(count >= 2);

    // Both should be in registry now
    assert!(manager2.exists_in_registry(&ns1));
    assert!(manager2.exists_in_registry(&ns2));

    cleanup_ns(&harness.store, &ns1).await;
    cleanup_ns(&harness.store, &ns2).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn test_create_namespace_invalid_name_regex() {
    let harness = TestHarness::new().await;

    let manager = NamespaceManager::new(harness.store.clone());
    let result = manager
        .create("bad/name", 128, DistanceMetric::Cosine)
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        matches!(err, ZeppelinError::Validation(_)),
        "expected Validation error, got: {msg}"
    );
    assert!(msg.contains("namespace name"), "got: {msg}");

    harness.cleanup().await;
}

#[tokio::test]
async fn test_create_namespace_valid_name_formats() {
    let harness = TestHarness::new().await;

    let manager = NamespaceManager::new(harness.store.clone());
    let valid_names = [
        ns(&harness, "a"),
        ns(&harness, "my-ns"),
        ns(&harness, "ns_123"),
        ns(&harness, "ABC"),
    ];

    for name in &valid_names {
        let result = manager.create(name, 16, DistanceMetric::Cosine).await;
        assert!(
            result.is_ok(),
            "expected success for name '{name}', got: {result:?}"
        );
        cleanup_ns(&harness.store, name).await;
    }

    harness.cleanup().await;
}
