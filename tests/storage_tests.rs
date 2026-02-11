mod common;

use bytes::Bytes;
use common::harness::TestHarness;

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
