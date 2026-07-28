mod common;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use ulid::Ulid;
use zeppelin::error::ZeppelinError;
use zeppelin::wal::manifest::FragmentRef;
use zeppelin::wal::{Manifest, WalFragment};

use common::harness::TestHarness;

#[tokio::test]
async fn receipt_publication_reuses_successful_put_hash_without_readback() {
    let mut harness = TestHarness::new().await;
    harness.store = harness.store.clone().with_receipts_enabled(true);
    let namespace = harness.artifact_origin_namespace("receipt-put-hash");
    let fragment_id = Ulid::new();
    let key = WalFragment::s3_key(&namespace, &fragment_id);
    let body = Bytes::from_static(b"fresh immutable artifact");
    let expected_hash = <[u8; 32]>::from(Sha256::digest(&body));

    harness.store.put(&key, body).await.unwrap();
    // Removing the object makes an accidental hydration read fail. Publication
    // must use the exact hash retained beside the successful immutable PUT.
    harness.store.delete(&key).await.unwrap();

    let mut manifest = Manifest::new();
    manifest.add_fragment(FragmentRef {
        id: fragment_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 24,
        artifact_origin: None,
    });
    let manifest =
        common::publish_bound_manifest(&harness.store, &namespace, manifest, uuid::Uuid::new_v4())
            .await;

    let artifacts = manifest
        .receipt_artifacts(&namespace)
        .expect("published manifest must carry a complete receipt inventory");
    assert_eq!(artifacts.get(&key), Some(&expected_hash));

    harness.cleanup().await;
}

#[tokio::test]
async fn put_hash_survives_manifest_conflict_and_is_consumed_after_retry_commit() {
    let mut harness = TestHarness::new().await;
    harness.store = harness.store.clone().with_receipts_enabled(true);
    let namespace = harness.artifact_origin_namespace("receipt-put-hash-cas");
    common::seed_bound_manifest(&harness.store, &namespace).await;
    let (base, stale_version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();

    let fragment_id = Ulid::new();
    let key = WalFragment::s3_key(&namespace, &fragment_id);
    harness
        .store
        .put(&key, Bytes::from_static(b"retry-owned immutable artifact"))
        .await
        .unwrap();

    let mut competitor = base.clone();
    competitor
        .write_conditional(&harness.store, &namespace, &stale_version)
        .await
        .unwrap();

    let mut stale = base;
    stale.add_fragment(FragmentRef {
        id: fragment_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 30,
        artifact_origin: None,
    });
    assert!(matches!(
        stale
            .write_conditional(&harness.store, &namespace, &stale_version)
            .await,
        Err(ZeppelinError::ManifestConflict { .. })
    ));

    // If the failed candidate consumed the PUT-side hash, the retry below
    // cannot build a complete receipt inventory after this authoritative body
    // is removed.
    harness.store.delete(&key).await.unwrap();
    let (mut retry, retry_version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    retry.add_fragment(FragmentRef {
        id: fragment_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 30,
        artifact_origin: None,
    });
    retry
        .write_conditional(&harness.store, &namespace, &retry_version)
        .await
        .unwrap();
    assert!(retry
        .receipt_artifacts(&namespace)
        .expect("retry commit must consume the retained PUT hash")
        .contains_key(&key));

    harness.cleanup().await;
}
