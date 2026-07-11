mod common;

use common::fault_injection::fail_after_put_once_matching;
use common::harness::TestHarness;
use zeppelin::types::VectorEntry;
use zeppelin::wal::{Manifest, WalFragment, WalReader, WalWriter};

#[tokio::test]
async fn lost_manifest_cas_acknowledgement_keeps_reachable_fragment() {
    let harness = TestHarness::new().await;
    let namespace = harness.key("wal-post-commit-manifest");
    Manifest::new()
        .write(&harness.store, &namespace)
        .await
        .unwrap();

    let (faulted_store, failure) =
        fail_after_put_once_matching(&harness.store, Manifest::s3_key(&namespace));
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

    harness.cleanup().await;
}
