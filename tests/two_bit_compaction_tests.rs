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
) -> (QuantizationType, CoarsePayloadEncoding, u64) {
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
    // Quantized compactions write ZBP5: the grouped object carries no
    // per-section magic, so the manifest row layout is the authoritative
    // source of the coarse block's range and the encoding tag is the type
    // identity of the codes-only payload.
    assert_eq!(&object[..4], b"ZBP\x05");
    let object_ref = &segment.cluster_objects[0];
    assert!(object_ref.declares_row_layout());
    let layout = &object_ref.row_layouts[0];
    assert_eq!(
        layout.vectors_len,
        layout.row_count * DIM as u64 * 4,
        "published vector block must be exactly row_count rows of fixed-stride f32"
    );
    let coarse_len = layout.coarse_len;
    let segment_mode = segment.quantization;

    harness.cleanup().await;
    (segment_mode, encoding, coarse_len)
}

#[tokio::test]
async fn compaction_round_trips_two_bit_and_scalar_modes() {
    let (two_bit_mode, two_bit_encoding, two_bit_coarse_len) =
        compact_with_mode(QuantizationType::TwoBit).await;
    assert_eq!(two_bit_mode, QuantizationType::TwoBit);
    assert_eq!(two_bit_encoding, CoarsePayloadEncoding::TwoBit);

    let (scalar_mode, scalar_encoding, scalar_coarse_len) =
        compact_with_mode(QuantizationType::Scalar).await;
    assert_eq!(scalar_mode, QuantizationType::Scalar);
    assert_eq!(scalar_encoding, CoarsePayloadEncoding::Sq8);

    // Same corpus and partitioning in both runs: the two-bit codes-only block
    // must be narrower than the SQ8 one, proving the writer really emitted a
    // different encoding per mode rather than relabeling one payload.
    assert!(
        two_bit_coarse_len < scalar_coarse_len,
        "two-bit coarse block ({two_bit_coarse_len} B) must be narrower than SQ8 ({scalar_coarse_len} B)"
    );
}

#[tokio::test]
async fn default_quantization_stays_scalar() {
    let config = IndexingConfig {
        default_num_centroids: 2,
        bitmap_index: false,
        ..Default::default()
    };
    assert_eq!(config.quantization, QuantizationType::Scalar);

    let (segment_mode, encoding, coarse_len) = compact_with_mode(config.quantization).await;
    assert_eq!(segment_mode, QuantizationType::Scalar);
    assert_eq!(encoding, CoarsePayloadEncoding::Sq8);
    assert!(coarse_len > 0);
}
