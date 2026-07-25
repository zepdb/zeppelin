mod common;

use chrono::Utc;
use common::counting::counting_store;
use common::fault_injection::{fail_after_put_once_matching, fail_put_once_matching};
use common::harness::TestHarness;
use zeppelin::types::VectorEntry;
use zeppelin::wal::{Manifest, WalFragment, WalReader, WalWriter};

#[tokio::test]
async fn lost_manifest_cas_acknowledgement_keeps_reachable_fragment() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("wal-post-commit-manifest");
    Manifest::new()
        .write(&harness.store, &namespace)
        .await
        .unwrap();

    let (faulted_store, failure) =
        fail_after_put_once_matching(&harness.store, Manifest::s3_key(&namespace));
    let (faulted_store, counter) = counting_store(&faulted_store);
    let writer = WalWriter::new(faulted_store);
    let result = writer
        .append(
            &namespace,
            vec![VectorEntry {
                id: "survives-lost-ack".to_string(),
                values: vec![1.0, 2.0, 3.0, 4.0],
                attributes: None,
            }],
            vec![],
        )
        .await;

    assert_eq!(failure.failures_injected(), 1);
    let live = Manifest::read(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live.fragments.len(), 1);
    let reachable_key = WalFragment::s3_key(&namespace, &live.fragments[0].id);
    assert!(
        harness.store.exists(&reachable_key).await.unwrap(),
        "a live manifest must never reference a deleted WAL fragment"
    );

    let (fragment, recovered) = result.expect("a committed manifest CAS must recover as success");
    assert_eq!(fragment.id, live.fragments[0].id);
    assert_eq!(recovered.version(), live.version());
    assert!(recovered
        .fragments
        .iter()
        .any(|fref| fref.id == fragment.id));

    let fragments = WalReader::new(harness.store.clone())
        .read_uncompacted_fragments(&namespace)
        .await
        .unwrap();
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].vectors[0].id, "survives-lost-ack");

    counter.reset();
    writer
        .append(
            &namespace,
            vec![VectorEntry {
                id: "cold-after-lost-ack".to_string(),
                values: vec![4.0, 3.0, 2.0, 1.0],
                attributes: None,
            }],
            vec![],
        )
        .await
        .expect("a recovered lost acknowledgement must leave the next round cold and writable");
    assert_eq!(
        counter.gets_matching("/manifest.json"),
        1,
        "ambiguous write recovery must not populate the group-commit memo"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn terminal_manifest_write_error_clears_group_commit_memo() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("wal-terminal-manifest-error");
    Manifest::new()
        .write(&harness.store, &namespace)
        .await
        .unwrap();

    let failed_history = Manifest::history_key(&namespace, 3);
    let (faulted_store, failure) = fail_put_once_matching(&harness.store, &failed_history);
    let (faulted_store, counter) = counting_store(&faulted_store);
    let writer = WalWriter::new(faulted_store);

    writer
        .append(
            &namespace,
            vec![VectorEntry {
                id: "memo-seed".to_string(),
                values: vec![1.0, 0.0, 0.0, 0.0],
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    // Advance authority outside the writer and remove that winner's history.
    // The writer's memo first conflicts, then its cold retry must repair the
    // exact predecessor before another CAS. Failing that repair is terminal
    // and must leave the group-commit memo empty.
    let (mut concurrent, version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    concurrent.updated_at = Utc::now();
    concurrent
        .write_conditional(&harness.store, &namespace, &version)
        .await
        .unwrap();
    harness.store.delete(&failed_history).await.unwrap();

    let failed = writer
        .append(
            &namespace,
            vec![VectorEntry {
                id: "terminal-error".to_string(),
                values: vec![0.0, 1.0, 0.0, 0.0],
                attributes: None,
            }],
            vec![],
        )
        .await;
    assert!(failed.is_err());
    assert_eq!(failure.failures_injected(), 1);

    counter.reset();
    writer
        .append(
            &namespace,
            vec![VectorEntry {
                id: "cold-after-terminal-error".to_string(),
                values: vec![0.0, 0.0, 1.0, 0.0],
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(
        counter.gets_matching("/manifest.json"),
        1,
        "a terminal publication error must leave the next group-commit round cold"
    );

    let live = Manifest::read(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live.fragments.len(), 2);

    harness.cleanup().await;
}

#[tokio::test]
async fn failed_live_manifest_put_does_not_wedge_a_divergent_wal_retry() {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("wal-divergent-history-retry");
    Manifest::new()
        .write(&harness.store, &namespace)
        .await
        .unwrap();

    let (faulted_store, failure) =
        fail_put_once_matching(&harness.store, Manifest::s3_key(&namespace));
    let writer = WalWriter::new(faulted_store);
    let first = writer
        .append(
            &namespace,
            vec![VectorEntry {
                id: "failed-candidate".to_string(),
                values: vec![1.0, 0.0, 0.0, 0.0],
                attributes: None,
            }],
            vec![],
        )
        .await;
    assert!(first.is_err(), "the injected live PUT must fail loudly");
    assert_eq!(failure.failures_injected(), 1);

    let live_one = Manifest::read(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_one.version(), 1);
    assert!(live_one.fragments.is_empty());
    let history_one = Manifest::read_history(&harness.store, &namespace, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        history_one.to_bytes().unwrap(),
        live_one.to_bytes().unwrap()
    );
    assert!(!harness
        .store
        .exists(&Manifest::history_key(&namespace, 2))
        .await
        .unwrap());

    let (fragment, live_two) = writer
        .append(
            &namespace,
            vec![VectorEntry {
                id: "divergent-retry".to_string(),
                values: vec![0.0, 1.0, 0.0, 0.0],
                attributes: None,
            }],
            vec![],
        )
        .await
        .expect("a rebuilt WAL candidate must publish at the unreserved generation");
    assert_eq!(live_two.version(), 2);
    assert_eq!(live_two.fragments.len(), 1);
    assert_eq!(live_two.fragments[0].id, fragment.id);
    assert_eq!(
        WalReader::new(harness.store.clone())
            .read_uncompacted_fragments(&namespace)
            .await
            .unwrap()[0]
            .vectors[0]
            .id,
        "divergent-retry"
    );

    harness.cleanup().await;
}
