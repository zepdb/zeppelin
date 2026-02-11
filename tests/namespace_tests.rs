mod common;

use common::assertions::{assert_s3_object_exists, assert_s3_object_not_exists};
use common::harness::TestHarness;

use zeppelin::error::ZeppelinError;
use zeppelin::namespace::manager::NamespaceMetadata;
use zeppelin::namespace::NamespaceManager;
use zeppelin::types::DistanceMetric;
use zeppelin::wal::Manifest;

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
    let meta = manager.create(&name, 128, DistanceMetric::Cosine).await.unwrap();

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
async fn test_get_namespace_from_registry() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "ns-get-reg");

    let manager = NamespaceManager::new(harness.store.clone());
    manager.create(&name, 64, DistanceMetric::Euclidean).await.unwrap();

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
    manager1.create(&name, 32, DistanceMetric::DotProduct).await.unwrap();

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
    manager.create(&ns1, 16, DistanceMetric::Cosine).await.unwrap();
    manager.create(&ns2, 32, DistanceMetric::Euclidean).await.unwrap();

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
async fn test_delete_namespace() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "ns-delete");

    let manager = NamespaceManager::new(harness.store.clone());
    manager.create(&name, 64, DistanceMetric::Cosine).await.unwrap();

    // Delete
    manager.delete(&name).await.unwrap();

    // Verify meta.json is gone
    let meta_key = NamespaceMetadata::s3_key(&name);
    assert_s3_object_not_exists(&harness.store, &meta_key).await;

    // Verify get fails
    let result = manager.get(&name).await;
    assert!(matches!(result, Err(ZeppelinError::NamespaceNotFound { .. })));

    harness.cleanup().await;
}

#[tokio::test]
async fn test_duplicate_create() {
    let harness = TestHarness::new().await;
    let name = ns(&harness, "ns-dup");

    let manager = NamespaceManager::new(harness.store.clone());
    manager.create(&name, 64, DistanceMetric::Cosine).await.unwrap();

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
    manager1.create(&ns1, 16, DistanceMetric::Cosine).await.unwrap();
    manager1.create(&ns2, 32, DistanceMetric::Euclidean).await.unwrap();

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
    let result = manager.create("bad/name", 128, DistanceMetric::Cosine).await;
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
        assert!(result.is_ok(), "expected success for name '{name}', got: {result:?}");
        cleanup_ns(&harness.store, name).await;
    }

    harness.cleanup().await;
}
