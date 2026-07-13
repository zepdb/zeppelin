//! Installation/CI certification for the object-store assumptions Zeppelin
//! relies on instead of re-verifying every successful production write.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use common::harness::TestHarness;
use zeppelin::error::ZeppelinError;

#[tokio::test]
async fn list_metadata_preserves_version_token() {
    let harness = TestHarness::new().await;
    let prefix = harness.key("provider-conformance-list-meta");
    let key = format!("{prefix}/object.bin");
    let first = Bytes::from_static(b"first-version");
    let second = Bytes::from_static(b"other-version");

    harness.store.put(&key, first.clone()).await.unwrap();

    let listed = harness
        .store
        .list_prefix_meta(&format!("{prefix}/"))
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key, key);
    assert_eq!(listed[0].size, first.len() as u64);
    let first_etag = listed[0]
        .version
        .as_ref()
        .and_then(zeppelin::storage::StorageVersion::etag)
        .expect("supported backend LIST must preserve the object ETag");
    assert!(harness
        .store
        .get_if_none_match(&key, first_etag)
        .await
        .unwrap()
        .is_none());
    let head = harness.store.head(&key).await.unwrap();
    assert_eq!(head.e_tag.as_deref(), Some(first_etag));

    harness.store.put(&key, second.clone()).await.unwrap();
    let relisted = harness
        .store
        .list_prefix_meta(&format!("{prefix}/"))
        .await
        .unwrap();
    assert_eq!(relisted.len(), 1);
    assert_eq!(relisted[0].key, key);
    assert_eq!(relisted[0].size, second.len() as u64);
    let second_etag = relisted[0]
        .version
        .as_ref()
        .and_then(zeppelin::storage::StorageVersion::etag)
        .expect("supported backend LIST must preserve the replacement ETag");
    assert_ne!(second_etag, first_etag);

    harness.cleanup().await;
}

#[tokio::test]
async fn supported_backend_honors_exact_atomic_strong_object_semantics() {
    let harness = TestHarness::new().await;
    let store = harness.store.clone();
    let prefix = harness.key("provider-conformance");
    let key = format!("{prefix}/exact.bin");
    let sibling = format!("{prefix}/exact.bin.misdirected");
    let copy = format!("{prefix}/copy.bin");
    let created = format!("{prefix}/create-only.bin");
    let old = Bytes::from(vec![0x3c; 64 * 1024]);
    let new = Bytes::from(vec![0xa7; 64 * 1024]);

    store.put(&key, old.clone()).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), old);
    assert!(matches!(
        store.get(&sibling).await,
        Err(ZeppelinError::NotFound { .. })
    ));

    let (old_body, old_etag) = store.get_with_meta(&key).await.unwrap();
    let old_etag = old_etag.expect("supported backend must return an object version token");
    let old_head = store.head(&key).await.unwrap();
    assert_eq!(old_head.size, old_body.len());
    assert_eq!(old_head.e_tag.as_deref(), Some(old_etag.as_str()));

    store.put(&key, new.clone()).await.unwrap();
    let (new_body, new_etag) = store.get_with_meta(&key).await.unwrap();
    let new_etag = new_etag.expect("overwrite must return a version token");
    assert_eq!(new_body, new);
    assert_ne!(
        new_etag, old_etag,
        "overwrite must change the version token"
    );
    assert!(store
        .get_if_none_match(&key, &new_etag)
        .await
        .unwrap()
        .is_none());
    let new_head = store.head(&key).await.unwrap();
    assert_eq!(new_head.size, new.len());
    assert_eq!(new_head.e_tag.as_deref(), Some(new_etag.as_str()));

    let stale = store
        .put_if_match(&key, Bytes::from_static(b"stale"), &old_etag, &prefix)
        .await;
    assert!(matches!(stale, Err(ZeppelinError::ManifestConflict { .. })));
    assert_eq!(store.get(&key).await.unwrap(), new);

    let cas_body = Bytes::from_static(b"conditional replacement");
    store
        .put_if_match(&key, cas_body.clone(), &new_etag, &prefix)
        .await
        .unwrap();
    let (after_cas, after_cas_etag) = store.get_with_meta(&key).await.unwrap();
    assert_eq!(after_cas, cas_body);
    assert_ne!(after_cas_etag.as_deref(), Some(new_etag.as_str()));

    store
        .put_if_not_exists(&created, Bytes::from_static(b"created"), &prefix)
        .await
        .unwrap();
    assert!(store
        .put_if_not_exists(&created, Bytes::from_static(b"replacement"), &prefix)
        .await
        .is_err());
    assert_eq!(
        store.get(&created).await.unwrap(),
        Bytes::from_static(b"created")
    );

    store
        .copy_if_not_exists(&key, &copy, &prefix)
        .await
        .unwrap();
    assert_eq!(store.get(&copy).await.unwrap(), cas_body);
    assert!(store
        .copy_if_not_exists(&created, &copy, &prefix)
        .await
        .is_err());
    assert_eq!(store.get(&copy).await.unwrap(), cas_body);

    let listed = store.list_prefix(&format!("{prefix}/")).await.unwrap();
    let unique = listed.iter().cloned().collect::<BTreeSet<_>>();
    assert_eq!(unique.len(), listed.len(), "LIST returned a duplicate key");
    assert_eq!(
        unique,
        BTreeSet::from([key.clone(), copy.clone(), created.clone()])
    );

    store.delete(&copy).await.unwrap();
    assert!(matches!(
        store.get(&copy).await,
        Err(ZeppelinError::NotFound { .. })
    ));
    assert!(!store
        .list_prefix(&format!("{prefix}/"))
        .await
        .unwrap()
        .contains(&copy));

    harness.cleanup().await;
}

#[tokio::test]
async fn overwrite_readers_never_observe_torn_payloads() {
    let harness = TestHarness::new().await;
    let key = harness.key("provider-conformance-atomic-overwrite.bin");
    let first = Bytes::from(vec![0x11; 128 * 1024]);
    let second = Bytes::from(vec![0xee; 128 * 1024]);
    harness.store.put(&key, first.clone()).await.unwrap();

    let store = Arc::new(harness.store.clone());
    let writer_store = Arc::clone(&store);
    let writer_key = key.clone();
    let first_for_writer = first.clone();
    let second_for_writer = second.clone();
    let writer = tokio::spawn(async move {
        for index in 0..16 {
            let body = if index % 2 == 0 {
                second_for_writer.clone()
            } else {
                first_for_writer.clone()
            };
            writer_store.put(&writer_key, body).await.unwrap();
        }
    });

    for _ in 0..32 {
        let observed = store.get(&key).await.unwrap();
        assert!(
            observed == first || observed == second,
            "reader observed a partial or mixed successful overwrite"
        );
    }
    writer.await.unwrap();

    harness.cleanup().await;
}
