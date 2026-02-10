mod common;

use common::assertions::{assert_s3_object_exists, assert_s3_object_not_exists};
use common::harness::TestHarness;

use zeppelin::error::ZeppelinError;
use zeppelin::namespace::manager::NamespaceMetadata;
use zeppelin::namespace::NamespaceManager;
use zeppelin::types::DistanceMetric;
use zeppelin::wal::Manifest;

#[tokio::test]
async fn test_create_namespace() {
    let harness = TestHarness::new().await;
    let ns = harness.key("ns-create");

    let manager = NamespaceManager::new(harness.store.clone());
    let meta = manager.create(&ns, 128, DistanceMetric::Cosine).await.unwrap();

    assert_eq!(meta.name, ns);
    assert_eq!(meta.dimensions, 128);
    assert_eq!(meta.distance_metric, DistanceMetric::Cosine);
    assert_eq!(meta.vector_count, 0);

    // Verify meta.json exists on S3
    let meta_key = NamespaceMetadata::s3_key(&ns);
    assert_s3_object_exists(&harness.store, &meta_key).await;

    // Verify manifest.json exists on S3
    let manifest_key = Manifest::s3_key(&ns);
    assert_s3_object_exists(&harness.store, &manifest_key).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn test_get_namespace_from_registry() {
    let harness = TestHarness::new().await;
    let ns = harness.key("ns-get-reg");

    let manager = NamespaceManager::new(harness.store.clone());
    manager.create(&ns, 64, DistanceMetric::Euclidean).await.unwrap();

    // Get should hit registry
    let meta = manager.get(&ns).await.unwrap();
    assert_eq!(meta.name, ns);
    assert_eq!(meta.dimensions, 64);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_get_namespace_s3_fallback() {
    let harness = TestHarness::new().await;
    let ns = harness.key("ns-get-s3");

    // Create with one manager
    let manager1 = NamespaceManager::new(harness.store.clone());
    manager1.create(&ns, 32, DistanceMetric::DotProduct).await.unwrap();

    // Get with a fresh manager (empty registry → falls back to S3)
    let manager2 = NamespaceManager::new(harness.store.clone());
    let meta = manager2.get(&ns).await.unwrap();
    assert_eq!(meta.name, ns);
    assert_eq!(meta.dimensions, 32);
    assert_eq!(meta.distance_metric, DistanceMetric::DotProduct);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_list_namespaces() {
    let harness = TestHarness::new().await;
    let ns1 = harness.key("ns-list-a");
    let ns2 = harness.key("ns-list-b");

    let manager = NamespaceManager::new(harness.store.clone());
    manager.create(&ns1, 16, DistanceMetric::Cosine).await.unwrap();
    manager.create(&ns2, 32, DistanceMetric::Euclidean).await.unwrap();

    // List with prefix scoped to this test's prefix
    let prefix = format!("{}/", harness.prefix);
    let namespaces = manager.list(Some(&prefix)).await.unwrap();
    assert!(namespaces.len() >= 2);

    let names: Vec<&str> = namespaces.iter().map(|m| m.name.as_str()).collect();
    assert!(names.contains(&ns1.as_str()));
    assert!(names.contains(&ns2.as_str()));

    harness.cleanup().await;
}

#[tokio::test]
async fn test_delete_namespace() {
    let harness = TestHarness::new().await;
    let ns = harness.key("ns-delete");

    let manager = NamespaceManager::new(harness.store.clone());
    manager.create(&ns, 64, DistanceMetric::Cosine).await.unwrap();

    // Delete
    manager.delete(&ns).await.unwrap();

    // Verify meta.json is gone
    let meta_key = NamespaceMetadata::s3_key(&ns);
    assert_s3_object_not_exists(&harness.store, &meta_key).await;

    // Verify get fails
    let result = manager.get(&ns).await;
    assert!(matches!(result, Err(ZeppelinError::NamespaceNotFound { .. })));

    harness.cleanup().await;
}

#[tokio::test]
async fn test_duplicate_create() {
    let harness = TestHarness::new().await;
    let ns = harness.key("ns-dup");

    let manager = NamespaceManager::new(harness.store.clone());
    manager.create(&ns, 64, DistanceMetric::Cosine).await.unwrap();

    let result = manager.create(&ns, 64, DistanceMetric::Cosine).await;
    assert!(matches!(
        result,
        Err(ZeppelinError::NamespaceAlreadyExists { .. })
    ));

    harness.cleanup().await;
}

#[tokio::test]
async fn test_get_nonexistent() {
    let harness = TestHarness::new().await;
    let ns = harness.key("ns-nonexistent");

    let manager = NamespaceManager::new(harness.store.clone());
    let result = manager.get(&ns).await;
    assert!(matches!(
        result,
        Err(ZeppelinError::NamespaceNotFound { .. })
    ));

    harness.cleanup().await;
}

#[tokio::test]
async fn test_scan_and_register() {
    let harness = TestHarness::new().await;
    let ns1 = harness.key("ns-scan-a");
    let ns2 = harness.key("ns-scan-b");

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

    harness.cleanup().await;
}
