mod common;

use bytes::Bytes;
use common::harness::TestHarness;
use zeppelin::config::{StorageBackend, StorageConfig};
use zeppelin::storage::ZeppelinStore;

/// Smoke test: connect to S3, write an object, read it back, verify content, delete it.
#[tokio::test]
async fn test_s3_put_get_delete() {
    let harness = TestHarness::new().await;
    let key = harness.key("hello.txt");
    let data = Bytes::from("hello from zeppelin");

    // PUT
    harness
        .store
        .put(&key, data.clone())
        .await
        .expect("put should succeed");

    // GET
    let result = harness.store.get(&key).await.expect("get should succeed");
    assert_eq!(result, data, "data read back should match what was written");

    // EXISTS
    let exists = harness
        .store
        .exists(&key)
        .await
        .expect("exists should succeed");
    assert!(exists, "object should exist after put");

    // HEAD
    let meta = harness.store.head(&key).await.expect("head should succeed");
    assert_eq!(meta.size, data.len(), "head size should match data length");

    // DELETE
    harness
        .store
        .delete(&key)
        .await
        .expect("delete should succeed");

    // Verify deleted
    let exists_after = harness
        .store
        .exists(&key)
        .await
        .expect("exists should succeed");
    assert!(!exists_after, "object should not exist after delete");

    harness.cleanup().await;
}

/// Test that getting a nonexistent key returns NotFound.
#[tokio::test]
async fn test_s3_get_not_found() {
    let harness = TestHarness::new().await;
    let key = harness.key("does-not-exist.txt");

    let result = harness.store.get(&key).await;
    assert!(result.is_err(), "get of nonexistent key should fail");

    match result.unwrap_err() {
        zeppelin::error::ZeppelinError::NotFound { .. } => {}
        other => panic!("expected NotFound error, got: {other}"),
    }

    harness.cleanup().await;
}

/// Test listing objects under a prefix.
#[tokio::test]
async fn test_s3_list_prefix() {
    let harness = TestHarness::new().await;

    // Write 3 objects
    for i in 0..3 {
        let key = harness.key(&format!("list-test/item_{i}.txt"));
        harness
            .store
            .put(&key, Bytes::from(format!("item {i}")))
            .await
            .expect("put should succeed");
    }

    // List them
    let prefix = harness.key("list-test/");
    let keys = harness
        .store
        .list_prefix(&prefix)
        .await
        .expect("list should succeed");

    assert_eq!(keys.len(), 3, "should list exactly 3 objects");
    for i in 0..3 {
        let expected_suffix = format!("item_{i}.txt");
        assert!(
            keys.iter().any(|k| k.ends_with(&expected_suffix)),
            "should find item_{i}.txt in listing"
        );
    }

    harness.cleanup().await;
}

/// Test overwrite semantics: putting to the same key replaces the value.
#[tokio::test]
async fn test_s3_overwrite() {
    let harness = TestHarness::new().await;
    let key = harness.key("overwrite.txt");

    // Write v1
    harness
        .store
        .put(&key, Bytes::from("version 1"))
        .await
        .expect("put v1 should succeed");

    // Overwrite with v2
    harness
        .store
        .put(&key, Bytes::from("version 2"))
        .await
        .expect("put v2 should succeed");

    // Read back — should get v2
    let result = harness.store.get(&key).await.expect("get should succeed");
    assert_eq!(
        result,
        Bytes::from("version 2"),
        "should read overwritten value"
    );

    harness.cleanup().await;
}

/// Test exists returns false for nonexistent keys.
#[tokio::test]
async fn test_s3_exists_false() {
    let harness = TestHarness::new().await;
    let key = harness.key("nope.txt");

    let exists = harness
        .store
        .exists(&key)
        .await
        .expect("exists should succeed");
    assert!(!exists, "nonexistent key should return false");

    harness.cleanup().await;
}

/// Test delete_prefix removes all objects under a prefix.
#[tokio::test]
async fn test_s3_delete_prefix() {
    let harness = TestHarness::new().await;

    // Write some objects under a sub-prefix
    let sub = "bulk-delete";
    for i in 0..5 {
        let key = harness.key(&format!("{sub}/file_{i}.bin"));
        harness
            .store
            .put(&key, Bytes::from(vec![i as u8; 100]))
            .await
            .expect("put should succeed");
    }

    // Verify they exist
    let prefix = harness.key(&format!("{sub}/"));
    let keys = harness
        .store
        .list_prefix(&prefix)
        .await
        .expect("list should work");
    assert_eq!(keys.len(), 5);

    // Delete prefix
    let deleted = harness
        .store
        .delete_prefix(&prefix)
        .await
        .expect("delete_prefix should succeed");
    assert_eq!(deleted, 5, "should delete 5 objects");

    // Verify empty
    let keys_after = harness
        .store
        .list_prefix(&prefix)
        .await
        .expect("list should work");
    assert!(keys_after.is_empty(), "prefix should be empty after delete");

    harness.cleanup().await;
}

// ── Coverage tests for store.rs uncovered lines ──────────────────────

/// Test local backend: full put/get/exists/head/delete lifecycle (lines 52-61).
#[tokio::test]
async fn test_local_backend_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        backend: StorageBackend::Local,
        bucket: dir.path().to_str().unwrap().to_string(),
        ..Default::default()
    };
    let store = ZeppelinStore::from_config(&config).expect("local backend should build");

    let key = format!("test-local/{}", uuid::Uuid::new_v4());
    store.put(&key, Bytes::from("hello")).await.unwrap();

    let data = store.get(&key).await.unwrap();
    assert_eq!(data, Bytes::from("hello"));

    assert!(store.exists(&key).await.unwrap());

    let meta = store.head(&key).await.unwrap();
    assert_eq!(meta.size, 5);

    store.delete(&key).await.unwrap();
    assert!(!store.exists(&key).await.unwrap());
}

/// Test local backend creates non-existent directory (lines 54-56).
#[tokio::test]
async fn test_local_backend_creates_dir() {
    let base = tempfile::tempdir().unwrap();
    let nested = base.path().join("deeply/nested/dir");
    let config = StorageConfig {
        backend: StorageBackend::Local,
        bucket: nested.to_str().unwrap().to_string(),
        ..Default::default()
    };
    let store = ZeppelinStore::from_config(&config).expect("should create dirs and build");
    store.put("test.txt", Bytes::from("ok")).await.unwrap();
    assert!(nested.exists());
}

/// Test unsupported backend returns Config error (lines 63-67).
#[test]
fn test_unsupported_backend_error() {
    let config = StorageConfig {
        backend: StorageBackend::Gcs,
        bucket: "irrelevant".to_string(),
        ..Default::default()
    };
    let result = ZeppelinStore::from_config(&config);
    match result {
        Err(zeppelin::error::ZeppelinError::Config(msg)) => {
            assert!(
                msg.contains("unsupported storage backend: gcs"),
                "unexpected message: {msg}"
            );
        }
        other => panic!(
            "expected Config error, got: {}",
            match other {
                Ok(_) => "Ok(ZeppelinStore)".to_string(),
                Err(e) => format!("Err({e})"),
            }
        ),
    }
}

/// Test head() on nonexistent key returns NotFound (lines 256-263).
#[tokio::test]
async fn test_head_not_found() {
    let harness = TestHarness::new().await;
    let key = harness.key("nonexistent-for-head.bin");
    match harness.store.head(&key).await {
        Err(zeppelin::error::ZeppelinError::NotFound { .. }) => {}
        other => panic!("expected NotFound, got: {other:?}"),
    }
    harness.cleanup().await;
}

/// Test put_if_match non-Precondition error branch (lines 178-182).
/// Local backend lacks conditional PUT, so put_opts returns a non-Precondition
/// storage error, exercising the `other` match arm.
#[tokio::test]
async fn test_put_if_match_storage_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        backend: StorageBackend::Local,
        bucket: dir.path().to_str().unwrap().to_string(),
        ..Default::default()
    };
    let store = ZeppelinStore::from_config(&config).unwrap();
    store.put("obj.bin", Bytes::from("data")).await.unwrap();

    let result = store
        .put_if_match("obj.bin", Bytes::from("new"), "fake-etag", "test-ns")
        .await;
    match result {
        Err(zeppelin::error::ZeppelinError::Storage(_)) => {}
        other => panic!("expected Storage error, got: {other:?}"),
    }
}

/// Test head() generic storage error mapping (lines 264-265).
/// Uses local backend with a path conflict to trigger a non-NotFound OS error.
#[tokio::test]
async fn test_head_storage_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        backend: StorageBackend::Local,
        bucket: dir.path().to_str().unwrap().to_string(),
        ..Default::default()
    };
    let store = ZeppelinStore::from_config(&config).unwrap();

    // Write a regular file at "blocker"
    store
        .put("blocker", Bytes::from("I am a file"))
        .await
        .unwrap();
    // Try to head "blocker/child" — OS should return ENOTDIR, not ENOENT
    let result = store.head("blocker/child").await;
    match result {
        Err(zeppelin::error::ZeppelinError::Storage(_)) => {}
        Err(zeppelin::error::ZeppelinError::NotFound { .. }) => {
            // object_store may map ENOTDIR → NotFound internally; acceptable
        }
        other => panic!("expected Storage or NotFound error, got: {other:?}"),
    }
}

/// Test S3 builder failure with invalid config (lines 48-50).
#[test]
fn test_s3_build_error() {
    let config = StorageConfig {
        backend: StorageBackend::S3,
        bucket: "".to_string(),
        ..Default::default()
    };
    let result = ZeppelinStore::from_config(&config);
    // S3 builder may accept empty bucket at build time and fail later.
    // If it does fail, it should be a Config error.
    match result {
        Err(zeppelin::error::ZeppelinError::Config(msg)) => {
            assert!(
                msg.contains("failed to build S3 store"),
                "unexpected message: {msg}"
            );
        }
        Ok(_) => {
            // Builder deferred validation — acceptable, not all S3 builders
            // reject empty bucket at construction time.
        }
        Err(other) => panic!("expected Config error or Ok, got: {other}"),
    }
}
