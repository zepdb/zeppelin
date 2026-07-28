mod common;

use std::time::Duration;

use common::harness::TestHarness;
use common::vectors::random_vectors;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::index::quantization::QuantizationType;
use zeppelin::types::DistanceMetric;
use zeppelin::wal::manifest::{ClusterRowLayoutRef, CoarsePayloadEncoding, Manifest};
use zeppelin::wal::{WalReader, WalWriter};

const DIM: usize = 256;

async fn compact_with_mode(
    mode: QuantizationType,
) -> (QuantizationType, CoarsePayloadEncoding, ClusterRowLayoutRef) {
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
    let layout = layout.clone();
    let segment_mode = segment.quantization;

    harness.cleanup().await;
    (segment_mode, encoding, layout)
}

/// An SQ8 codes-only block is a `[row_count: u32][dim: u32]` header followed by
/// exactly one byte per component. That width is unreachable for any two-bit
/// payload, so it identifies the encoding without a per-section magic.
fn assert_sq8_coarse_width(layout: &ClusterRowLayoutRef) {
    assert_eq!(
        layout.coarse_len,
        8 + layout.row_count * DIM as u64,
        "SQ8 coarse block must be one byte per component over an 8-byte header"
    );
}

#[tokio::test]
async fn compaction_round_trips_two_bit_and_scalar_modes() {
    let (two_bit_mode, two_bit_encoding, two_bit_layout) =
        compact_with_mode(QuantizationType::TwoBit).await;
    assert_eq!(two_bit_mode, QuantizationType::TwoBit);
    assert_eq!(two_bit_encoding, CoarsePayloadEncoding::TwoBit);

    let (scalar_mode, scalar_encoding, scalar_layout) =
        compact_with_mode(QuantizationType::Scalar).await;
    assert_eq!(scalar_mode, QuantizationType::Scalar);
    assert_eq!(scalar_encoding, CoarsePayloadEncoding::Sq8);
    assert_sq8_coarse_width(&scalar_layout);

    // Same corpus and partitioning in both runs, so the two-bit block must be
    // narrower than the SQ8 one it replaces: the writer emitted a different
    // encoding per mode rather than relabeling one payload.
    assert!(
        two_bit_layout.coarse_len < scalar_layout.coarse_len,
        "two-bit coarse block ({} B) must be narrower than SQ8 ({} B)",
        two_bit_layout.coarse_len,
        scalar_layout.coarse_len
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

    let (segment_mode, encoding, layout) = compact_with_mode(config.quantization).await;
    assert_eq!(segment_mode, QuantizationType::Scalar);
    assert_eq!(encoding, CoarsePayloadEncoding::Sq8);
    assert!(layout.row_count > 0, "the fixture must persist rows");
    assert_sq8_coarse_width(&layout);
}
