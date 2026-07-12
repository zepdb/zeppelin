mod common;

use common::counting::counting_store;
use common::fault_injection::fail_put_once_matching;
use common::harness::TestHarness;
use ulid::Ulid;
use zeppelin::error::ZeppelinError;
use zeppelin::wal::manifest::FragmentRef;
use zeppelin::wal::Manifest;

fn fragment(id: u128, vector_count: usize) -> FragmentRef {
    FragmentRef {
        id: Ulid::from_parts(50_000, id),
        vector_count,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 16,
    }
}

async fn history_versions(store: &zeppelin::storage::ZeppelinStore, ns: &str) -> Vec<u64> {
    Manifest::list_history(store, ns)
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.version)
        .collect()
}

#[tokio::test]
async fn conditional_history_failure_does_not_advance_live_manifest_and_retry_is_clean() {
    let harness = TestHarness::new().await;
    let ns = harness.key("manifest-history-conditional");

    Manifest::new().write(&harness.store, &ns).await.unwrap();
    let (mut manifest, version) = Manifest::read_versioned(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.version(), 1);
    manifest.add_fragment(fragment(1, 3));

    let (failing_store, failures) =
        fail_put_once_matching(&harness.store, Manifest::history_prefix(&ns));
    let err = manifest
        .write_conditional(&failing_store, &ns, &version)
        .await
        .unwrap_err();
    assert!(matches!(err, ZeppelinError::Storage(_)));
    assert_eq!(failures.failures_injected(), 1);
    assert_eq!(manifest.version(), 1);

    let live = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(live.version(), 1);
    assert!(live.fragments.is_empty());
    assert!(Manifest::read_history(&harness.store, &ns, 2)
        .await
        .unwrap()
        .is_none());

    manifest
        .write_conditional(&failing_store, &ns, &version)
        .await
        .unwrap();
    assert_eq!(manifest.version(), 2);

    let live = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(live.version(), manifest.version());
    assert_eq!(live.fragments.len(), 1);
    assert_eq!(history_versions(&harness.store, &ns).await, vec![1, 2]);
    assert!(Manifest::read_history(&harness.store, &ns, 2)
        .await
        .unwrap()
        .is_some());

    harness.cleanup().await;
}

#[tokio::test]
async fn write_history_failure_does_not_advance_live_manifest_and_retry_is_clean() {
    let harness = TestHarness::new().await;
    let ns = harness.key("manifest-history-write");

    Manifest::new().write(&harness.store, &ns).await.unwrap();
    let mut manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(manifest.version(), 1);
    manifest.add_fragment(fragment(2, 5));

    let (failing_store, failures) =
        fail_put_once_matching(&harness.store, Manifest::history_prefix(&ns));
    let err = manifest.write(&failing_store, &ns).await.unwrap_err();
    assert!(matches!(err, ZeppelinError::Storage(_)));
    assert_eq!(failures.failures_injected(), 1);
    assert_eq!(manifest.version(), 1);

    let live = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(live.version(), 1);
    assert!(live.fragments.is_empty());
    assert!(Manifest::read_history(&harness.store, &ns, 2)
        .await
        .unwrap()
        .is_none());

    manifest.write(&failing_store, &ns).await.unwrap();
    assert_eq!(manifest.version(), 2);

    let live = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(live.version(), manifest.version());
    assert_eq!(live.fragments.len(), 1);
    assert_eq!(history_versions(&harness.store, &ns).await, vec![1, 2]);
    assert!(Manifest::read_history(&harness.store, &ns, 2)
        .await
        .unwrap()
        .is_some());

    harness.cleanup().await;
}

#[tokio::test]
async fn conditional_pointer_failure_orphan_history_can_be_overwritten_on_retry() {
    let harness = TestHarness::new().await;
    let ns = harness.key("manifest-history-pointer-failure");

    Manifest::new().write(&harness.store, &ns).await.unwrap();
    let (mut manifest, version) = Manifest::read_versioned(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.version(), 1);
    manifest.add_fragment(fragment(3, 7));

    let (failing_store, failures) = fail_put_once_matching(&harness.store, Manifest::s3_key(&ns));
    let err = manifest
        .write_conditional(&failing_store, &ns, &version)
        .await
        .unwrap_err();
    assert!(matches!(err, ZeppelinError::Storage(_)));
    assert_eq!(failures.failures_injected(), 1);
    assert_eq!(manifest.version(), 1);

    let live = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(live.version(), 1);
    assert!(live.fragments.is_empty());

    let orphan = Manifest::read_history(&harness.store, &ns, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(orphan.version(), 2);
    assert_eq!(orphan.fragments.len(), 1);
    assert_eq!(orphan.fragments[0].vector_count, 7);

    manifest.add_fragment(fragment(4, 11));
    manifest
        .write_conditional(&failing_store, &ns, &version)
        .await
        .unwrap();

    let live = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(live.version(), 2);
    assert_eq!(live.fragments.len(), 2);
    assert_eq!(live.fragments[1].vector_count, 11);

    let history = Manifest::read_history(&harness.store, &ns, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.version(), live.version());
    assert_eq!(history.fragments, live.fragments);

    harness.cleanup().await;
}

#[tokio::test]
async fn conditional_manifest_publication_has_no_success_readback() {
    let harness = TestHarness::new().await;
    let ns = harness.key("manifest-no-success-readback");
    Manifest::new().write(&harness.store, &ns).await.unwrap();
    let (mut manifest, version) = Manifest::read_versioned(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    manifest.add_fragment(fragment(9, 13));

    let (store, counter) = counting_store(&harness.store);
    manifest
        .write_conditional(&store, &ns, &version)
        .await
        .unwrap();

    assert_eq!(counter.puts_matching("/manifest.json"), 1);
    assert_eq!(
        counter.gets_matching("/manifest.json"),
        0,
        "a conformant successful conditional PUT must not be synchronously read back"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn successful_manifest_write_keeps_candidate_namespace_bound() {
    let harness = TestHarness::new().await;
    let source = harness.key("manifest-bound-candidate-source");
    let target = harness.key("manifest-bound-candidate-target");
    let mut manifest = Manifest::new();
    manifest.write(&harness.store, &source).await.unwrap();

    harness
        .store
        .put(&Manifest::s3_key(&target), manifest.to_bytes().unwrap())
        .await
        .unwrap();
    let result = Manifest::read(&harness.store, &target).await;
    assert!(matches!(
        result,
        Err(ZeppelinError::Serialization(message))
            if message.contains("manifest namespace binding mismatch")
    ));

    harness.cleanup().await;
}

#[tokio::test]
async fn live_manifest_rejects_bytes_bound_to_another_namespace() {
    let harness = TestHarness::new().await;
    let source = harness.key("manifest-binding-source");
    let target = harness.key("manifest-binding-target");
    Manifest::new()
        .write(&harness.store, &source)
        .await
        .unwrap();
    Manifest::new()
        .write(&harness.store, &target)
        .await
        .unwrap();

    let wrong = harness.store.get(&Manifest::s3_key(&source)).await.unwrap();
    harness
        .store
        .put(&Manifest::s3_key(&target), wrong)
        .await
        .unwrap();

    let result = Manifest::read(&harness.store, &target).await;
    assert!(matches!(
        result,
        Err(ZeppelinError::Serialization(message))
            if message.contains("manifest namespace binding mismatch")
    ));

    harness.cleanup().await;
}
