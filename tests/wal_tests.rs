mod common;

use common::harness::TestHarness;
use common::vectors::random_vectors;

use std::sync::Arc;

use zeppelin::error::ZeppelinError;
use zeppelin::wal::{Manifest, WalFragment, WalReader, WalWriter};

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
        | ZeppelinError::Bincode(_) => {}
        other => panic!("expected ChecksumMismatch, Json, or Bincode error, got: {other}"),
    }
}

#[tokio::test]
async fn test_wal_writer_append_single_fragment() {
    let harness = TestHarness::new().await;
    let ns = harness.key("wal-single");

    // Initialize namespace manifest so writer can read it
    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

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
    let ns = harness.key("wal-multi");

    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

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
    let ns = harness.key("wal-read");

    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

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
    let ns = harness.key("wal-empty");

    // No manifest at all → should return empty
    let reader = WalReader::new(harness.store.clone());
    let fragments = reader.read_uncompacted_fragments(&ns).await.unwrap();
    assert!(fragments.is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn test_wal_fragment_key_listing() {
    let harness = TestHarness::new().await;
    let ns = harness.key("wal-list");

    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

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
    let ns = harness.key("wal-concurrent");

    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

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
    let ns = harness.key("wal-sequential");

    let manifest = Manifest::new();
    manifest.write(&harness.store, &ns).await.unwrap();

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
