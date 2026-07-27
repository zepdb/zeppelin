mod common;

use std::time::Duration;

use common::harness::TestHarness;
use common::vectors::random_vectors;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::index::quantization::QuantizationType;
use zeppelin::types::DistanceMetric;
use zeppelin::wal::manifest::{CoarsePayloadEncoding, Manifest};
use zeppelin::wal::{WalReader, WalWriter};

const DIM: usize = 256;

async fn compact_with_mode(
    mode: QuantizationType,
) -> (QuantizationType, CoarsePayloadEncoding, Vec<u8>) {
    let harness = TestHarness::new().await;
    let namespace = harness.artifact_origin_namespace("two-bit-compaction");
    common::seed_active_namespace(&harness.store, &namespace, DIM, DistanceMetric::Euclidean).await;
    WalWriter::new(harness.store.clone())
        .append(&namespace, random_vectors(16, DIM), Vec::new())
        .await
        .unwrap();

    let compactor = Compactor::new(
        harness.store.clone(),
        WalReader::new(harness.store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        IndexingConfig {
            default_num_centroids: 2,
            quantization: mode,
            bitmap_index: false,
            ..Default::default()
        },
        Duration::from_secs(300),
    );
    let segment_id = compactor
        .compact(&namespace)
        .await
        .unwrap()
        .segment_id
        .unwrap();
    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    let segment = manifest
        .segments
        .iter()
        .find(|segment| segment.id == segment_id)
        .unwrap();
    let encoding = manifest.coarse_payload_encoding(&segment_id);
    let object = harness
        .store
        .get(&segment.cluster_objects[0].key)
        .await
        .unwrap();
    assert_eq!(&object[..4], b"ZBP\x04");
    let coarse_offset = u64::from_le_bytes(object[12..20].try_into().unwrap()) as usize;
    let coarse_len = u64::from_le_bytes(object[20..28].try_into().unwrap()) as usize;
    let coarse_prefix = object[coarse_offset..coarse_offset + coarse_len.min(4)].to_vec();
    let segment_mode = segment.quantization;

    harness.cleanup().await;
    (segment_mode, encoding, coarse_prefix)
}

#[tokio::test]
async fn compaction_round_trips_two_bit_and_scalar_modes() {
    let (two_bit_mode, two_bit_encoding, two_bit_coarse) =
        compact_with_mode(QuantizationType::TwoBit).await;
    assert_eq!(two_bit_mode, QuantizationType::TwoBit);
    assert_eq!(two_bit_encoding, CoarsePayloadEncoding::TwoBit);
    assert_eq!(two_bit_coarse, b"ZRQ1");

    let (scalar_mode, scalar_encoding, scalar_coarse) =
        compact_with_mode(QuantizationType::Scalar).await;
    assert_eq!(scalar_mode, QuantizationType::Scalar);
    assert_eq!(scalar_encoding, CoarsePayloadEncoding::Sq8);
    assert_ne!(scalar_coarse, b"ZRQ1");
}

#[tokio::test]
async fn default_quantization_stays_scalar() {
    let config = IndexingConfig {
        default_num_centroids: 2,
        bitmap_index: false,
        ..Default::default()
    };
    assert_eq!(config.quantization, QuantizationType::Scalar);

    let (segment_mode, encoding, coarse) = compact_with_mode(config.quantization).await;
    assert_eq!(segment_mode, QuantizationType::Scalar);
    assert_eq!(encoding, CoarsePayloadEncoding::Sq8);
    assert_ne!(coarse, b"ZRQ1");
}
