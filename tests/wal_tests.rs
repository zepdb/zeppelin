mod common;

use async_trait::async_trait;
use common::counting::counting_store;
use common::fault_injection::{delay_get_matching, toggle_get_failure_matching};
use common::harness::TestHarness;
use common::vectors::random_vectors;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use zeppelin::error::ZeppelinError;
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::{Manifest, ManifestAppendGuard, WalFragment, WalReader, WalWriter};

/// Object-store decorator that replaces the ETag on one GET response.
///
/// The payload and every other metadata field still come from the real test
/// backend. This isolates the fail-closed behavior for a backend that returns
/// an existing manifest body without a usable conditional-write capability.
#[derive(Debug)]
struct ManifestGetEtagOverrideStore {
    inner: Arc<dyn ObjectStore>,
    manifest_key: String,
    replacement: Option<String>,
}

impl fmt::Display for ManifestGetEtagOverrideStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ManifestGetEtagOverrideStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ManifestGetEtagOverrideStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
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
        if location.as_ref() == self.manifest_key {
            result.meta.e_tag.clone_from(&self.replacement);
        }
        Ok(result)
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
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

fn override_manifest_get_etag(
    store: &ZeppelinStore,
    manifest_key: String,
    replacement: Option<String>,
) -> ZeppelinStore {
    ZeppelinStore::new(Arc::new(ManifestGetEtagOverrideStore {
        inner: store.inner(),
        manifest_key,
        replacement,
    }))
}

#[tokio::test]
async fn test_fragment_serialize_deserialize_roundtrip() {
    let vectors = random_vectors(5, 32);
    let deletes = vec!["del_1".to_string(), "del_2".to_string()];

    let fragment = WalFragment::new(vectors.clone(), deletes.clone());
    let bytes = fragment.to_bytes().unwrap();
    let restored = WalFragment::from_bytes(&bytes).unwrap();

    assert_eq!(restored.id, fragment.id);
    assert_eq!(restored.vectors.len(), 5);
    assert_eq!(restored.deletes.len(), 2);
    assert_eq!(restored.checksum, fragment.checksum);
}

#[tokio::test]
async fn guarded_append_rejects_a_legacy_unbound_manifest() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("legacy-unbound-append-guard");
    // Deliberately legacy: this test asserts the guard that rejects appends
    // against a manifest with no incarnation, so it must stay unbound.
    let mut manifest = Manifest::new();
    manifest.write(&harness.store, &namespace).await.unwrap();
    let (manifest, version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    let error = ManifestAppendGuard::new(&namespace, &manifest, version)
        .expect_err("an unbound pre-upgrade manifest must fail loud");
    assert!(matches!(error, ZeppelinError::Index(_)));
    assert!(error.to_string().contains("incarnation-bound manifest"));
    harness.cleanup().await;
}

#[tokio::test]
async fn test_fragment_checksum_corruption() {
    let vectors = random_vectors(3, 16);
    let fragment = WalFragment::new(vectors, vec![]);
    let mut bytes = fragment.to_bytes().unwrap().to_vec();

    // Corrupt a byte in the middle of the payload
    if bytes.len() > 10 {
        bytes[10] ^= 0xFF;
    }

    let result = WalFragment::from_bytes(&bytes);
    assert!(result.is_err());
    match result.unwrap_err() {
        ZeppelinError::ChecksumMismatch { .. }
        | ZeppelinError::Json(_)
        | ZeppelinError::Bincode(_)
        | ZeppelinError::Serialization(_) => {}
        other => {
            panic!("expected ChecksumMismatch, Json, Bincode, or Serialization error, got: {other}")
        }
    }
}

#[tokio::test]
async fn test_wal_writer_append_single_fragment() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-single");

    // Initialize namespace manifest so writer can read it
    common::seed_bound_manifest(&harness.store, &ns).await;

    let writer = WalWriter::new(harness.store.clone());
    let vectors = random_vectors(3, 16);
    let (fragment, _) = writer.append(&ns, vectors, vec![]).await.unwrap();

    // Verify fragment exists on S3
    let frag_key = WalFragment::s3_key(&ns, &fragment.id);
    assert!(harness.store.exists(&frag_key).await.unwrap());

    // Verify manifest has the fragment
    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.fragments.len(), 1);
    assert_eq!(manifest.fragments[0].id, fragment.id);
    assert_eq!(manifest.fragments[0].vector_count, 3);
    assert_eq!(manifest.fragments[0].delete_count, 0);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_wal_writer_append_multiple_fragments() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-multi");

    common::seed_bound_manifest(&harness.store, &ns).await;

    let writer = WalWriter::new(harness.store.clone());

    let (f1, _) = writer
        .append(&ns, random_vectors(2, 8), vec![])
        .await
        .unwrap();
    let (f2, _) = writer
        .append(&ns, random_vectors(3, 8), vec!["del_1".to_string()])
        .await
        .unwrap();
    let (f3, _) = writer
        .append(&ns, vec![], vec!["del_2".to_string(), "del_3".to_string()])
        .await
        .unwrap();

    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.fragments.len(), 3);
    assert_eq!(manifest.fragments[0].id, f1.id);
    assert_eq!(manifest.fragments[1].id, f2.id);
    assert_eq!(manifest.fragments[2].id, f3.id);
    assert_eq!(manifest.fragments[1].delete_count, 1);
    assert_eq!(manifest.fragments[2].delete_count, 2);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_wal_reader_read_uncompacted_fragments() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-read");

    common::seed_bound_manifest(&harness.store, &ns).await;

    let writer = WalWriter::new(harness.store.clone());
    let (f1, _) = writer
        .append(&ns, random_vectors(2, 8), vec![])
        .await
        .unwrap();
    let (f2, _) = writer
        .append(&ns, random_vectors(3, 8), vec![])
        .await
        .unwrap();

    let reader = WalReader::new(harness.store.clone());
    let fragments = reader.read_uncompacted_fragments(&ns).await.unwrap();

    assert_eq!(fragments.len(), 2);
    // Should be in ULID order (f1 before f2)
    assert_eq!(fragments[0].id, f1.id);
    assert_eq!(fragments[1].id, f2.id);
    assert_eq!(fragments[0].vectors.len(), 2);
    assert_eq!(fragments[1].vectors.len(), 3);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_wal_reader_empty_namespace() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-empty");

    // No manifest at all → should return empty
    let reader = WalReader::new(harness.store.clone());
    let fragments = reader.read_uncompacted_fragments(&ns).await.unwrap();
    assert!(fragments.is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_wal_fragment_key_listing() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-list");

    common::seed_bound_manifest(&harness.store, &ns).await;

    let writer = WalWriter::new(harness.store.clone());
    writer
        .append(&ns, random_vectors(1, 4), vec![])
        .await
        .unwrap();
    writer
        .append(&ns, random_vectors(1, 4), vec![])
        .await
        .unwrap();

    let reader = WalReader::new(harness.store.clone());
    let keys = reader.list_fragment_keys(&ns).await.unwrap();
    assert_eq!(keys.len(), 2);
    for key in &keys {
        assert!(key.ends_with(".wal"));
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn test_wal_writer_concurrent_appends() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-concurrent");

    common::seed_bound_manifest(&harness.store, &ns).await;

    let writer = Arc::new(WalWriter::new(harness.store.clone()));

    let mut handles = vec![];
    for i in 0..10 {
        let writer = writer.clone();
        let ns = ns.clone();
        handles.push(tokio::spawn(async move {
            let vectors = vec![zeppelin::types::VectorEntry {
                id: format!("concurrent_{i}"),
                values: vec![i as f32; 4],
                attributes: None,
            }];
            writer.append(&ns, vectors, vec![]).await.unwrap();
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(
        manifest.fragments.len(),
        10,
        "all 10 concurrent appends should be in manifest"
    );

    // Verify all fragments are readable and have valid checksums
    let reader = WalReader::new(harness.store.clone());
    let fragments = reader.read_uncompacted_fragments(&ns).await.unwrap();
    assert_eq!(fragments.len(), 10);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_wal_writer_sequential_consistency() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-sequential");

    common::seed_bound_manifest(&harness.store, &ns).await;

    let writer = WalWriter::new(harness.store.clone());

    // Append 1
    writer
        .append(&ns, random_vectors(2, 4), vec![])
        .await
        .unwrap();
    let m = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(m.fragments.len(), 1);

    // Append 2
    writer
        .append(&ns, random_vectors(3, 4), vec![])
        .await
        .unwrap();
    let m = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(m.fragments.len(), 2);

    // Append 3
    writer
        .append(&ns, random_vectors(1, 4), vec![])
        .await
        .unwrap();
    let m = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(m.fragments.len(), 3);

    harness.cleanup().await;
}

#[tokio::test]
async fn scoped_delete_append_rejects_a_changed_authority_manifest() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-scoped-delete-authority");

    let mut manifest = Manifest::new();
    manifest
        .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
        .unwrap();
    manifest.write(&harness.store, &ns).await.unwrap();
    let (observed_manifest, observed_storage_version) =
        Manifest::read_versioned(&harness.store, &ns)
            .await
            .unwrap()
            .unwrap();
    let guard = ManifestAppendGuard::new(&ns, &observed_manifest, observed_storage_version)
        .expect("MinIO manifest reads must provide an ETag");

    let writer = WalWriter::new(harness.store.clone());
    writer
        .append(&ns, random_vectors(1, 4), vec![])
        .await
        .unwrap();

    let error = writer
        .append_deletes_if_manifest_unchanged(
            &ns,
            vec!["tenant-a-visible-before-race".to_string()],
            guard,
        )
        .await
        .expect_err("a scoped delete must not publish against changed live data");
    assert!(matches!(error, ZeppelinError::ManifestConflict { .. }));

    let committed = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(committed.fragments.len(), 1);
    assert_eq!(committed.fragments[0].delete_count, 0);
    assert_eq!(
        harness
            .store
            .list_prefix(&format!("{ns}/wal/"))
            .await
            .unwrap()
            .len(),
        1,
        "failed guarded append must clean its orphan fragment"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn scoped_delete_append_ignores_an_older_process_memo() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-scoped-delete-stale-memo");

    let mut manifest = Manifest::new();
    manifest
        .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
        .unwrap();
    manifest.write(&harness.store, &ns).await.unwrap();

    let writer_a = WalWriter::new(harness.store.clone());
    writer_a
        .append(&ns, random_vectors(1, 4), vec![])
        .await
        .unwrap();

    let writer_b = WalWriter::new(harness.store.clone());
    writer_b
        .append(&ns, random_vectors(1, 4), vec![])
        .await
        .unwrap();
    let (observed_manifest, observed_storage_version) =
        Manifest::read_versioned(&harness.store, &ns)
            .await
            .unwrap()
            .unwrap();
    let guard = ManifestAppendGuard::new(&ns, &observed_manifest, observed_storage_version)
        .expect("MinIO manifest reads must provide an ETag");

    writer_a
        .append_deletes_if_manifest_unchanged(
            &ns,
            vec!["authorized-at-current-version".to_string()],
            guard,
        )
        .await
        .expect("an older process memo must not override authoritative S3 state");

    let committed = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(committed.fragments.len(), 3);
    assert_eq!(committed.fragments[2].delete_count, 1);

    harness.cleanup().await;
}

#[tokio::test]
async fn scoped_delete_append_rejects_recreated_namespace_incarnation() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-scoped-delete-incarnation");

    let fixed_time = chrono::Utc::now();
    let mut old_manifest = Manifest::new_at(fixed_time);
    old_manifest
        .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
        .unwrap();
    old_manifest.write(&harness.store, &ns).await.unwrap();
    let (observed_manifest, observed_storage_version) =
        Manifest::read_versioned(&harness.store, &ns)
            .await
            .unwrap()
            .unwrap();
    let old_guard =
        ManifestAppendGuard::new(&ns, &observed_manifest, observed_storage_version.clone())
            .expect("MinIO manifest reads must provide an ETag");

    harness.store.delete(&Manifest::s3_key(&ns)).await.unwrap();
    let mut recreated = Manifest::new_at(fixed_time);
    recreated
        .bind_namespace_incarnation(uuid::Uuid::from_u128(2))
        .unwrap();
    recreated.write(&harness.store, &ns).await.unwrap();
    let (recreated_manifest, recreated_storage_version) =
        Manifest::read_versioned(&harness.store, &ns)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(recreated_manifest.version(), observed_manifest.version());
    assert_ne!(
        recreated_storage_version, observed_storage_version,
        "incarnation-bound manifest bytes must differ across recreation"
    );

    let writer = WalWriter::new(harness.store.clone());
    let error = writer
        .append_deletes_if_manifest_unchanged(
            &ns,
            vec!["old-incarnation-id".to_string()],
            old_guard,
        )
        .await
        .expect_err("an old-incarnation ETag must not authorize a recreated namespace");
    assert!(matches!(error, ZeppelinError::ManifestConflict { .. }));
    assert!(Manifest::read(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap()
        .fragments
        .is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn legacy_manifest_incarnation_migration_is_cas_bound_and_idempotent() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-legacy-incarnation-migration");
    // Deliberately legacy: the migration under test is what binds this
    // manifest, so seeding it bound would erase the case.
    let mut legacy = Manifest::new();
    legacy.write(&harness.store, &ns).await.unwrap();
    let legacy_generation = legacy.version();
    let incarnation = uuid::Uuid::from_u128(0xfeed);

    let (migrated, migrated_storage_version) =
        Manifest::read_versioned_required_for_incarnation(&harness.store, &ns, incarnation)
            .await
            .expect("legacy manifest must acquire its metadata-backed incarnation through CAS");
    assert_eq!(migrated.version(), legacy_generation + 1);
    ManifestAppendGuard::new(&ns, &migrated, migrated_storage_version.clone())
        .expect("migrated manifest must be valid guarded-write authority");

    let (reloaded, reloaded_storage_version) =
        Manifest::read_versioned_required_for_incarnation(&harness.store, &ns, incarnation)
            .await
            .expect("re-reading the same incarnation must not publish another generation");
    assert_eq!(reloaded.version(), migrated.version());
    assert_eq!(reloaded_storage_version, migrated_storage_version);

    let error = Manifest::read_versioned_required_for_incarnation(
        &harness.store,
        &ns,
        uuid::Uuid::from_u128(0xbeef),
    )
    .await
    .expect_err("a different namespace lifetime must never reuse the migrated manifest");
    assert!(matches!(error, ZeppelinError::ManifestConflict { .. }));

    harness.cleanup().await;
}

#[tokio::test]
async fn bound_manifest_read_rejects_missing_or_empty_get_etags_before_any_put() {
    let harness = TestHarness::new().await;
    let incarnation = uuid::Uuid::from_u128(0xcafe);

    for (case, replacement) in [("missing", None), ("empty", Some(String::new()))] {
        let ns = harness.artifact_origin_namespace(&format!("wal-bound-{case}-get-etag"));
        let mut manifest = Manifest::new();
        manifest.bind_namespace_incarnation(incarnation).unwrap();
        manifest.write(&harness.store, &ns).await.unwrap();
        let expected_generation = manifest.version();
        let expected_bytes = manifest.to_bytes().unwrap();

        let manifest_key = Manifest::s3_key(&ns);
        let history_prefix = Manifest::history_prefix(&ns);
        let (counted_store, counter) = counting_store(&harness.store);
        let faulted_store =
            override_manifest_get_etag(&counted_store, manifest_key.clone(), replacement);

        let error =
            Manifest::read_versioned_required_for_incarnation(&faulted_store, &ns, incarnation)
                .await
                .expect_err(
                    "an existing bound manifest without a usable GET ETag must fail closed",
                );
        assert!(matches!(error, ZeppelinError::Index(_)));
        assert!(error.to_string().contains("requires an object-store ETag"));
        assert_eq!(counter.gets_matching(&manifest_key), 1);
        assert_eq!(counter.puts_matching(&manifest_key), 0);
        assert_eq!(counter.puts_matching(&history_prefix), 0);
        assert_eq!(
            counter.total_observed_puts(),
            0,
            "a rejected bound-manifest read must stop before every PUT"
        );

        let stored = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
        assert_eq!(stored.version(), expected_generation);
        assert_eq!(stored.to_bytes().unwrap(), expected_bytes);
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn legacy_manifest_migration_rejects_missing_or_empty_get_etags_before_any_put() {
    let harness = TestHarness::new().await;
    let incarnation = uuid::Uuid::from_u128(0xfeed);

    for (case, replacement) in [("missing", None), ("empty", Some(String::new()))] {
        let ns = harness.artifact_origin_namespace(&format!("wal-legacy-{case}-get-etag"));
        // Deliberately legacy: this exercises the unbound-manifest migration
        // path's ETag preconditions.
        let mut manifest = Manifest::new();
        manifest.write(&harness.store, &ns).await.unwrap();
        let expected_generation = manifest.version();
        let expected_bytes = manifest.to_bytes().unwrap();

        let manifest_key = Manifest::s3_key(&ns);
        let history_prefix = Manifest::history_prefix(&ns);
        let (counted_store, counter) = counting_store(&harness.store);
        let faulted_store =
            override_manifest_get_etag(&counted_store, manifest_key.clone(), replacement);

        let error =
            Manifest::read_versioned_required_for_incarnation(&faulted_store, &ns, incarnation)
                .await
                .expect_err("legacy migration without a usable GET ETag must fail closed");
        assert!(matches!(error, ZeppelinError::Index(_)));
        assert!(error.to_string().contains("requires an object-store ETag"));
        assert_eq!(counter.gets_matching(&manifest_key), 1);
        assert_eq!(counter.puts_matching(&manifest_key), 0);
        assert_eq!(counter.puts_matching(&history_prefix), 0);
        assert_eq!(
            counter.total_observed_puts(),
            0,
            "a rejected legacy migration must stop before history or live PUT"
        );

        let stored = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
        assert_eq!(stored.version(), expected_generation);
        assert_eq!(stored.to_bytes().unwrap(), expected_bytes);
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn guarded_appends_from_one_snapshot_never_coalesce() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("wal-guarded-no-coalesce");
    let mut manifest = Manifest::new();
    manifest
        .bind_namespace_incarnation(uuid::Uuid::from_u128(1))
        .unwrap();
    manifest.write(&harness.store, &ns).await.unwrap();
    let (observed_manifest, observed_storage_version) =
        Manifest::read_versioned(&harness.store, &ns)
            .await
            .unwrap()
            .unwrap();
    let guard = ManifestAppendGuard::new(&ns, &observed_manifest, observed_storage_version)
        .expect("MinIO manifest reads must provide an ETag");

    let manifest_key = Manifest::s3_key(&ns);
    let (failing_store, failure) =
        toggle_get_failure_matching(&harness.store, manifest_key.clone());
    let delayed_store = delay_get_matching(&failing_store, manifest_key, Duration::from_secs(2));
    let writer = Arc::new(WalWriter::new(delayed_store));
    failure.enable();

    let blocker_writer = Arc::clone(&writer);
    let blocker_ns = ns.clone();
    let blocker = tokio::spawn(async move {
        blocker_writer
            .append(&blocker_ns, random_vectors(1, 4), vec![])
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let first_writer = Arc::clone(&writer);
    let first_ns = ns.clone();
    let first_guard = guard.clone();
    let first = tokio::spawn(async move {
        first_writer
            .append_deletes_if_manifest_unchanged(
                &first_ns,
                vec!["guarded-first".to_string()],
                first_guard,
            )
            .await
    });
    let second_writer = Arc::clone(&writer);
    let second_ns = ns.clone();
    let second = tokio::spawn(async move {
        second_writer
            .append_deletes_if_manifest_unchanged(
                &second_ns,
                vec!["guarded-second".to_string()],
                guard,
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let uploaded = harness
                .store
                .list_prefix(&format!("{ns}/wal/"))
                .await
                .unwrap()
                .len();
            if uploaded >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all three fragments must upload while the first leader is blocked");
    tokio::time::sleep(Duration::from_millis(50)).await;

    tokio::time::timeout(Duration::from_secs(5), async {
        while failure.failures_injected() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the blocker manifest GET must fail");
    failure.disable();
    blocker
        .await
        .expect("blocker task must join")
        .expect_err("the first leader is only a deterministic lock blocker");

    let first = first.await.expect("first guarded task must join");
    let second = second.await.expect("second guarded task must join");
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let error = first.err().or_else(|| second.err()).expect("one must fail");
    assert!(matches!(error, ZeppelinError::ManifestConflict { .. }));

    let committed = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(committed.fragments.len(), 1);
    assert_eq!(committed.fragments[0].delete_count, 1);
    assert_eq!(
        harness
            .store
            .list_prefix(&format!("{ns}/wal/"))
            .await
            .unwrap()
            .len(),
        1,
        "the failed blocker and stale guarded append must clean their orphans"
    );

    harness.cleanup().await;
}
