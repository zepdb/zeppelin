mod common;

use bytes::Bytes;
use common::harness::TestHarness;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use std::fmt;
use std::time::Duration;
use zeppelin::config::{StorageBackend, StorageConfig};
use zeppelin::storage::{StorageVersion, ZeppelinStore};
use zeppelin::wal::Manifest;

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

/// A successful ordinary or CAS PUT returns the exact ETag that a fresh
/// authoritative read observes for the newly written object.
#[tokio::test]
async fn test_put_and_put_if_match_return_authoritative_etags() {
    let harness = TestHarness::new().await;
    let key = harness.key("returned-put-etag.bin");

    let first_etag = harness
        .store
        .put(&key, Bytes::from_static(b"first"))
        .await
        .expect("initial PUT should succeed");
    let Some(first_etag) = first_etag else {
        eprintln!("backend omitted the initial PUT ETag; skipping ETag equality assertions");
        harness.cleanup().await;
        return;
    };
    let (_, observed_first_version) = harness
        .store
        .get_with_meta(&key)
        .await
        .expect("initial object should be readable with metadata");
    let observed_first_version =
        observed_first_version.expect("a backend that returned a PUT ETag must return a read one");
    assert_eq!(observed_first_version.etag(), Some(first_etag.as_str()));

    let second_version = harness
        .store
        .put_if_match(
            &key,
            Bytes::from_static(b"second"),
            &observed_first_version,
            "returned-put-etag",
        )
        .await
        .expect("matching conditional PUT should succeed");
    let Some(second_etag) = second_version.as_ref().and_then(StorageVersion::etag) else {
        eprintln!("backend omitted the conditional PUT ETag; skipping its equality assertion");
        harness.cleanup().await;
        return;
    };
    let (body, observed_second_version) = harness
        .store
        .get_with_meta(&key)
        .await
        .expect("updated object should be readable with metadata");

    assert_eq!(body, Bytes::from_static(b"second"));
    assert_eq!(
        observed_second_version
            .as_ref()
            .and_then(StorageVersion::etag),
        Some(second_etag)
    );

    harness.cleanup().await;
}

/// A fresh conditional manifest publication exposes the ETag returned by its
/// ordinary live-manifest PUT as the next CAS capability.
#[tokio::test]
async fn test_fresh_manifest_write_conditional_returns_authoritative_etag() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("returned-manifest-etag");
    let mut manifest = common::publish_bound_manifest(
        &harness.store,
        &namespace,
        Manifest::new(),
        uuid::Uuid::new_v4(),
    )
    .await;
    let (_, base_version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .expect("initial manifest should be readable")
        .expect("initial manifest should exist");

    let written_version = manifest
        .write_conditional(&harness.store, &namespace, &base_version)
        .await
        .expect("conditional manifest publication should succeed");
    let Some(written_etag) = written_version.e_tag() else {
        eprintln!("backend omitted the manifest PUT ETag; skipping its equality assertion");
        harness.cleanup().await;
        return;
    };
    let (_, observed_version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .expect("fresh manifest should be readable")
        .expect("fresh manifest should exist");

    assert_eq!(observed_version.e_tag(), Some(written_etag));
    assert_eq!(manifest.version(), 2);

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

#[tokio::test]
async fn test_delete_many_handles_one_full_s3_batch() {
    let harness = TestHarness::new().await;
    let prefix = harness.key("delete-many/");
    let keys = (0..1_000)
        .map(|index| format!("{prefix}object_{index:04}.bin"))
        .collect::<Vec<_>>();

    let puts = futures::stream::iter(keys.iter().cloned())
        .map(|key| {
            let store = harness.store.clone();
            async move { store.put(&key, Bytes::from_static(b"delete-many")).await }
        })
        .buffer_unordered(32)
        .collect::<Vec<_>>()
        .await;
    for result in puts {
        result.expect("delete-many fixture PUT failed");
    }

    assert_eq!(harness.store.delete_many(keys).await.unwrap(), 1_000);
    assert!(harness.store.list_prefix(&prefix).await.unwrap().is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_delete_many_parses_every_key_before_deleting() {
    let harness = TestHarness::new().await;
    let protected = harness.key("delete-many-validate/protected.bin");
    harness
        .store
        .put(&protected, Bytes::from_static(b"must remain"))
        .await
        .unwrap();
    let malformed = format!("{}//malformed.bin", harness.prefix);

    assert!(harness
        .store
        .delete_many(vec![protected.clone(), malformed])
        .await
        .is_err());
    assert!(harness.store.exists(&protected).await.unwrap());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_delete_many_rejects_more_than_one_s3_batch_before_deleting() {
    let harness = TestHarness::new().await;
    let protected = harness.key("delete-many-limit/protected.bin");
    harness
        .store
        .put(&protected, Bytes::from_static(b"must remain"))
        .await
        .unwrap();
    let mut keys = vec![protected.clone()];
    keys.extend((0..1_000).map(|index| harness.key(&format!("absent/{index}.bin"))));

    assert!(harness.store.delete_many(keys).await.is_err());
    assert!(harness.store.exists(&protected).await.unwrap());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_delete_many_rejects_duplicate_keys_before_deleting() {
    let harness = TestHarness::new().await;
    let protected = harness.key("delete-many-duplicate/protected.bin");
    harness
        .store
        .put(&protected, Bytes::from_static(b"must remain"))
        .await
        .unwrap();

    let error = harness
        .store
        .delete_many(vec![protected.clone(), protected.clone()])
        .await
        .expect_err("duplicate delete keys must fail before object-store I/O");
    assert!(
        matches!(&error, zeppelin::error::ZeppelinError::Validation(message)
            if message.contains("unique object keys")),
        "unexpected duplicate-key error: {error}"
    );
    assert!(harness.store.exists(&protected).await.unwrap());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_delete_many_is_idempotent_for_absent_keys_and_empty_batches() {
    let harness = TestHarness::new().await;
    let absent = harness.key("delete-many-absent/object.bin");

    assert_eq!(harness.store.delete_many(vec![absent]).await.unwrap(), 1);
    assert_eq!(harness.store.delete_many(Vec::new()).await.unwrap(), 0);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_delete_prefix_paged_excludes_key_and_resumes() {
    let harness = TestHarness::new().await;
    let prefix = harness.key("paged-delete/");
    let excluded = format!("{prefix}meta.json");

    harness
        .store
        .put(&excluded, Bytes::from_static(b"tombstone"))
        .await
        .unwrap();
    for i in 0..1001 {
        let key = format!("{prefix}object_{i}.bin");
        harness
            .store
            .put(&key, Bytes::from(vec![i as u8; 8]))
            .await
            .unwrap();
    }

    let first = harness
        .store
        .delete_prefix_paged(&prefix, Some(&excluded), Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(first.deleted, 1000);
    assert!(
        !first.complete,
        "zero budget should stop after the first delete chunk"
    );
    assert!(harness.store.exists(&excluded).await.unwrap());

    let second = harness
        .store
        .delete_prefix_paged(&prefix, Some(&excluded), Duration::MAX)
        .await
        .unwrap();
    assert!(second.complete);
    assert_eq!(second.deleted, 1);
    assert!(harness.store.exists(&excluded).await.unwrap());

    harness.store.delete(&excluded).await.unwrap();
    let remaining = harness.store.list_prefix(&prefix).await.unwrap();
    assert!(remaining.is_empty(), "remaining keys: {remaining:?}");

    harness.cleanup().await;
}

#[tokio::test]
#[should_panic(expected = "recursive root listing must use list_common_prefixes")]
async fn test_list_prefix_rejects_recursive_root_listing() {
    let harness = TestHarness::new().await;

    let _ = harness.store.list_prefix("").await;
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

/// Test local backend supports atomic create semantics used by namespace creation.
#[tokio::test]
async fn test_local_backend_put_if_not_exists() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        backend: StorageBackend::Local,
        bucket: dir.path().to_str().unwrap().to_string(),
        ..Default::default()
    };
    let store = ZeppelinStore::from_config(&config).expect("local backend should build");
    let key = format!("test-local/{}", uuid::Uuid::new_v4());

    store
        .put_if_not_exists(&key, Bytes::from("first"), "test-ns")
        .await
        .expect("first create should succeed");

    let result = store
        .put_if_not_exists(&key, Bytes::from("second"), "test-ns")
        .await;

    match result {
        Err(zeppelin::error::ZeppelinError::NamespaceAlreadyExists { namespace }) => {
            assert_eq!(namespace, "test-ns")
        }
        other => panic!("expected NamespaceAlreadyExists, got: {other:?}"),
    }

    let data = store.get(&key).await.unwrap();
    assert_eq!(data, Bytes::from("first"));
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

    let fake_version = StorageVersion::from_parts(Some("fake-etag".to_string()), None)
        .expect("a token with an ETag is constructible");
    let result = store
        .put_if_match("obj.bin", Bytes::from("new"), &fake_version, "test-ns")
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

/// An object store that reports its identity only as a backend generation.
///
/// This is the shape GCS presents: conditional operations key on the object
/// generation and the ETag is not the identity Zeppelin can send. No backend
/// Zeppelin builds today behaves this way, so the version-only revalidation path
/// in `get_if_none_match` is unreachable without modelling it here.
#[derive(Debug)]
struct GenerationOnlyStore {
    inner: std::sync::Arc<dyn ObjectStore>,
}

impl fmt::Display for GenerationOnlyStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "GenerationOnlyStore({})", self.inner)
    }
}

fn move_etag_to_generation(meta: &mut ObjectMeta) {
    meta.version = meta.e_tag.take();
}

#[async_trait::async_trait]
impl ObjectStore for GenerationOnlyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut options: PutOptions,
    ) -> OsResult<PutResult> {
        // The inner store keys conditions on the ETag, so lower a generation-only
        // condition back into the ETag field the generation was moved out of.
        if let PutMode::Update(ref mut version) = options.mode {
            if version.e_tag.is_none() {
                version.e_tag = version.version.take();
            }
        }
        let mut result = self.inner.put_opts(location, payload, options).await?;
        result.version = result.e_tag.take();
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let mut result = self.inner.get_opts(location, options).await?;
        move_etag_to_generation(&mut result.meta);
        Ok(result)
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        let mut meta = self.inner.head(location).await?;
        move_etag_to_generation(&mut meta);
        Ok(meta)
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// Revalidation against a generation-only backend compares tokens locally.
///
/// `If-None-Match` is ETag-defined, so a token with no ETag cannot express the
/// precondition. The seam degrades to a full authoritative GET and compares the
/// tokens itself — the one place a missing field costs more work instead of
/// raising an error, because revalidation is an optimization, not a correctness
/// gate. Both outcomes must still be exact.
#[tokio::test]
async fn generation_only_revalidation_falls_back_to_a_local_comparison() {
    let store = ZeppelinStore::new(std::sync::Arc::new(GenerationOnlyStore {
        inner: std::sync::Arc::new(object_store::memory::InMemory::new()),
    }));
    let key = "ns/manifest.json";

    store.put(key, Bytes::from_static(b"v1")).await.unwrap();
    let (body, first) = store.get_with_meta(key).await.unwrap();
    let first = first.expect("a generation-only backend still reports an identity");
    assert_eq!(body, Bytes::from_static(b"v1"));
    assert_eq!(first.etag(), None);
    assert!(first.backend_version().is_some());

    // Unchanged: no ETag to send, so the token comparison happens locally.
    assert_eq!(store.get_if_none_match(key, &first).await.unwrap(), None);

    // Changed: the same path must hand back the new body and the new identity.
    store.put(key, Bytes::from_static(b"v2")).await.unwrap();
    let (changed_body, second) = store
        .get_if_none_match(key, &first)
        .await
        .unwrap()
        .expect("a changed object must be returned, not reported unchanged");
    let second = second.expect("the fresh read reports the current identity");
    assert_eq!(changed_body, Bytes::from_static(b"v2"));
    assert_ne!(second, first);

    // And the CAS the whole refactor exists for: a generation alone authorizes it.
    store
        .put_if_match(key, Bytes::from_static(b"v3"), &second, "ns")
        .await
        .unwrap();
    assert_eq!(store.get(key).await.unwrap(), Bytes::from_static(b"v3"));
}
