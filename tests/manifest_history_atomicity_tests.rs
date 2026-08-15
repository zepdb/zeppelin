mod common;

use common::counting::counting_store;
use common::fault_injection::{fail_put_once_matching, synchronize_cas_pair_matching};
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
        artifact_origin: None,
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
    let ns = harness.artifact_origin_namespace("manifest-history-conditional");

    common::seed_bound_manifest(&harness.store, &ns).await;
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
    let ns = harness.artifact_origin_namespace("manifest-history-write");

    common::seed_bound_manifest(&harness.store, &ns).await;
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
async fn failed_live_put_does_not_reserve_the_candidate_history_generation() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("manifest-history-pointer-failure");

    common::seed_bound_manifest(&harness.store, &ns).await;
    let (mut manifest, version) = Manifest::read_versioned(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.version(), 1);
    manifest.add_fragment(fragment(3, 7));

    let (failing_store, failures) =
        fail_put_once_matching(&harness.store, Manifest::object_store_key(&ns));
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

    let predecessor = Manifest::read_history(&harness.store, &ns, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(predecessor.to_bytes().unwrap(), live.to_bytes().unwrap());
    assert!(!harness
        .store
        .exists(&Manifest::history_key(&ns, 2))
        .await
        .unwrap());

    let mut divergent = live.clone();
    divergent.add_fragment(fragment(4, 11));
    let version2 = divergent
        .write_conditional(&failing_store, &ns, &version)
        .await
        .expect("a divergent retry must remain free to publish generation two");
    assert_eq!(divergent.version(), 2);
    assert_eq!(divergent.fragments.len(), 1);
    assert_eq!(divergent.fragments[0].vector_count, 11);

    divergent.add_fragment(fragment(5, 13));
    divergent
        .write_conditional(&failing_store, &ns, &version2)
        .await
        .expect("the next publication must retain generation two first");

    let live = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(live.version(), 3);
    assert_eq!(live.fragments.len(), 2);
    assert_eq!(live.fragments[1].vector_count, 13);

    let history = Manifest::read_history(&harness.store, &ns, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.version(), 2);
    assert_eq!(history.fragments.len(), 1);
    assert_eq!(history.fragments[0].vector_count, 11);

    harness.cleanup().await;
}

#[tokio::test]
async fn competing_candidates_share_predecessor_history_and_one_wins_live_cas() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("manifest-history-orphan-toctou");
    common::seed_bound_manifest(&harness.store, &ns).await;

    let (mut winner, winner_version) = Manifest::read_versioned(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    let (mut stale, stale_version) = Manifest::read_versioned(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    winner.add_fragment(fragment(30, 7));
    stale.add_fragment(fragment(31, 11));

    let base = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let predecessor_key = Manifest::history_key(&ns, 1);
    let (race_store, cas) =
        synchronize_cas_pair_matching(&harness.store, Manifest::object_store_key(&ns));
    cas.enable();
    let winner_store = race_store.clone();
    let stale_store = race_store;
    let winner_ns = ns.clone();
    let stale_ns = ns.clone();
    let ((winner_result, winner), (stale_result, stale)) = tokio::join!(
        async move {
            let result = winner
                .write_conditional(&winner_store, &winner_ns, &winner_version)
                .await;
            (result, winner)
        },
        async move {
            let result = stale
                .write_conditional(&stale_store, &stale_ns, &stale_version)
                .await;
            (result, stale)
        }
    );
    assert_eq!(cas.arrivals(), 2);
    assert_eq!(cas.conflicts(), 1);

    let (committed, rejected) = match (winner_result, stale_result) {
        (Ok(_), Err(ZeppelinError::ManifestConflict { .. })) => (winner, stale),
        (Err(ZeppelinError::ManifestConflict { .. }), Ok(_)) => (stale, winner),
        (winner_result, stale_result) => panic!(
            "exactly one candidate must win the live CAS: winner={winner_result:?}, stale={stale_result:?}"
        ),
    };

    let live = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let history = Manifest::read_history(&harness.store, &ns, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live.to_bytes().unwrap(), committed.to_bytes().unwrap());
    assert_eq!(
        history.to_bytes().unwrap(),
        base.to_bytes().unwrap(),
        "both candidates must share the immutable predecessor history"
    );
    assert_ne!(live.fragments, rejected.fragments);
    assert_eq!(
        harness.store.get(&predecessor_key).await.unwrap(),
        base.to_bytes().unwrap()
    );
    let committed_history = Manifest::read_history(&harness.store, &ns, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        committed_history.to_bytes().unwrap(),
        live.to_bytes().unwrap(),
        "only the winning candidate becomes immutable history generation two"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn conditional_manifest_publication_has_no_success_readback() {
    let harness = TestHarness::new().await;
    let ns = harness.artifact_origin_namespace("manifest-no-success-readback");
    common::seed_bound_manifest(&harness.store, &ns).await;
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
    let source = harness.artifact_origin_namespace("manifest-bound-candidate-source");
    let target = harness.artifact_origin_namespace("manifest-bound-candidate-target");
    let manifest = common::publish_bound_manifest(
        &harness.store,
        &source,
        Manifest::new(),
        uuid::Uuid::new_v4(),
    )
    .await;

    harness
        .store
        .put(
            &Manifest::object_store_key(&target),
            manifest.to_bytes().unwrap(),
        )
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
    let source = harness.artifact_origin_namespace("manifest-binding-source");
    let target = harness.artifact_origin_namespace("manifest-binding-target");
    common::seed_bound_manifest(&harness.store, &source).await;
    common::seed_bound_manifest(&harness.store, &target).await;

    let wrong = harness
        .store
        .get(&Manifest::object_store_key(&source))
        .await
        .unwrap();
    harness
        .store
        .put(&Manifest::object_store_key(&target), wrong)
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
