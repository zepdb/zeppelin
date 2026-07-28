mod common;

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use common::counting::{counting_store, GetCounter};
use common::harness::TestHarness;
use common::vectors::random_vectors;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig, DEFAULT_RERANK_COALESCE_GAP_BYTES};
use zeppelin::index::distance::compute_distance;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, VectorEntry};
use zeppelin::wal::manifest::{CoarsePayloadEncoding, Manifest};
use zeppelin::wal::{WalReader, WalWriter};

const ROWS: usize = 20_000;
const DIM: usize = 768;
const FRAGMENTS: usize = 4;
const CLUSTERS: usize = 8;
const TOP_K: usize = 100;
const SELECTIVITY_DIVISORS: [usize; 3] = [2, 10, 100];
const GROUP_OBJECT_HEADER_LEN: usize = 8;
const GROUP_OBJECT_ENTRY_LEN: usize = 4 + 8 * 4;

struct ObjectRegions {
    key: String,
    coarse: Range<usize>,
    full: Range<usize>,
}

struct Measurement {
    selectivity: &'static str,
    mode: QuantizationType,
    recall: f64,
    coarse_bytes: u64,
    rerank_bytes: u64,
}

fn indexing_config(mode: QuantizationType) -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: CLUSTERS,
        target_rows_per_cluster: ROWS / CLUSTERS,
        max_num_centroids: CLUSTERS,
        kmeans_max_iterations: 4,
        quantization: mode,
        ..Default::default()
    }
}

fn test_compactor(store: &ZeppelinStore, mode: QuantizationType) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        indexing_config(mode),
        common::default_gc_upload_window(),
    )
}

fn bucket_field(divisor: usize) -> String {
    format!("bucket_{divisor}")
}

fn synthetic_corpus() -> (Vec<VectorEntry>, Vec<f32>, HashMap<usize, HashSet<String>>) {
    let mut generated = random_vectors(ROWS + 1, DIM);
    let query = generated
        .pop()
        .expect("synthetic corpus must include one held-out query")
        .values;

    for (row, vector) in generated.iter_mut().enumerate() {
        let fragment = row / (ROWS / FRAGMENTS);
        vector.id = format!("f{fragment}-row-{row:05}");
        let mut attributes = HashMap::new();
        for divisor in SELECTIVITY_DIVISORS {
            attributes.insert(
                bucket_field(divisor),
                AttributeValue::Integer((row % divisor) as i64),
            );
        }
        vector.attributes = Some(attributes);
    }

    let scored = generated
        .iter()
        .enumerate()
        .map(|(row, vector)| {
            (
                row,
                vector.id.clone(),
                compute_distance(&query, &vector.values, DistanceMetric::Euclidean),
            )
        })
        .collect::<Vec<_>>();
    let ground_truth = SELECTIVITY_DIVISORS
        .into_iter()
        .map(|divisor| {
            let mut filtered = scored
                .iter()
                .filter(|(row, _, _)| row % divisor == 0)
                .map(|(_, id, distance)| (id.clone(), *distance))
                .collect::<Vec<_>>();
            filtered.sort_by(|left, right| {
                left.1
                    .total_cmp(&right.1)
                    .then_with(|| left.0.cmp(&right.0))
            });
            assert!(filtered.len() >= TOP_K);
            (
                divisor,
                filtered.into_iter().take(TOP_K).map(|(id, _)| id).collect(),
            )
        })
        .collect();

    (generated, query, ground_truth)
}

fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

fn read_u64(data: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
}

fn parse_object_regions(key: String, data: &[u8]) -> ObjectRegions {
    assert_eq!(&data[..4], b"ZBP\x04");
    let entry_count = read_u32(data, 4) as usize;
    let coarse_start = GROUP_OBJECT_HEADER_LEN + entry_count * GROUP_OBJECT_ENTRY_LEN;
    let mut coarse_end = coarse_start;
    let mut full_start = data.len();
    let mut full_end = 0;

    for entry in 0..entry_count {
        let offset = GROUP_OBJECT_HEADER_LEN + entry * GROUP_OBJECT_ENTRY_LEN;
        let entry_coarse_start = read_u64(data, offset + 4) as usize;
        let entry_coarse_len = read_u64(data, offset + 12) as usize;
        let entry_full_start = read_u64(data, offset + 20) as usize;
        let entry_full_len = read_u64(data, offset + 28) as usize;
        assert!(entry_coarse_start >= coarse_start);
        coarse_end = coarse_end.max(entry_coarse_start + entry_coarse_len);
        full_start = full_start.min(entry_full_start);
        full_end = full_end.max(entry_full_start + entry_full_len);
    }

    assert_eq!(coarse_end, full_start);
    assert_eq!(full_end, data.len());
    ObjectRegions {
        key,
        coarse: coarse_start..coarse_end,
        full: full_start..full_end,
    }
}

async fn active_object_regions(
    store: &ZeppelinStore,
    namespace: &str,
    mode: QuantizationType,
) -> Vec<ObjectRegions> {
    let manifest = Manifest::read(store, namespace)
        .await
        .unwrap()
        .expect("measurement manifest must exist");
    assert!(manifest.fragments.is_empty());
    let segment_id = manifest
        .active_segment
        .as_deref()
        .expect("measurement manifest must have an active segment");
    let segment = manifest
        .segments
        .iter()
        .find(|segment| segment.id == segment_id)
        .expect("active measurement segment must exist");
    assert_eq!(segment.quantization, mode);
    let expected_encoding = match mode {
        QuantizationType::Scalar => CoarsePayloadEncoding::Sq8,
        QuantizationType::TwoBit => CoarsePayloadEncoding::TwoBit,
        other => panic!("unexpected measurement quantization mode: {other:?}"),
    };
    assert_eq!(
        manifest.coarse_payload_encoding(segment_id),
        expected_encoding
    );

    let mut regions = Vec::with_capacity(segment.cluster_objects.len());
    for object in &segment.cluster_objects {
        let data = store.get(&object.key).await.unwrap();
        regions.push(parse_object_regions(object.key.clone(), &data));
    }
    regions
}

fn measured_region_bytes(counter: &GetCounter, object_regions: &[ObjectRegions]) -> (u64, u64) {
    let mut coarse_bytes = 0_u64;
    let mut rerank_bytes = 0_u64;
    for object in object_regions {
        let ranges = counter.ranges_for(&object.key);
        assert!(
            !ranges.is_empty(),
            "production query did not read measured object {}",
            object.key
        );
        for range in ranges {
            let bytes = (range.end - range.start) as u64;
            if range.start >= object.coarse.start && range.end <= object.coarse.end {
                coarse_bytes += bytes;
            } else if range.start >= object.full.start && range.end <= object.full.end {
                rerank_bytes += bytes;
            } else if range.end <= object.coarse.start {
                // The grouped-object directory is supporting I/O, not either
                // measured data region.
            } else {
                panic!(
                    "query range {range:?} crossed measured regions for {}: coarse={:?}, full={:?}",
                    object.key, object.coarse, object.full
                );
            }
        }
    }
    (coarse_bytes, rerank_bytes)
}

fn equality_filter(divisor: usize) -> Filter {
    Filter::Eq {
        field: bucket_field(divisor),
        value: AttributeValue::Integer(0),
    }
}

fn selectivity_label(divisor: usize) -> &'static str {
    match divisor {
        2 => "50%",
        10 => "10%",
        100 => "1%",
        other => panic!("unexpected selectivity divisor: {other}"),
    }
}

fn mode_label(mode: QuantizationType) -> &'static str {
    match mode {
        QuantizationType::Scalar => "Scalar",
        QuantizationType::TwoBit => "TwoBit",
        other => panic!("unexpected measurement quantization mode: {other:?}"),
    }
}

async fn measure_mode(mode: QuantizationType) -> Vec<Measurement> {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace =
        harness.artifact_origin_namespace(&format!("two-bit-measurement-{}", mode_label(mode)));
    common::seed_active_namespace(&store, &namespace, DIM, DistanceMetric::Euclidean).await;

    let (vectors, query, ground_truth) = synthetic_corpus();
    let mut vectors = vectors.into_iter();
    let writer = WalWriter::new(store.clone());
    for _ in 0..FRAGMENTS {
        writer
            .append(
                &namespace,
                vectors.by_ref().take(ROWS / FRAGMENTS).collect(),
                Vec::new(),
            )
            .await
            .unwrap();
    }
    assert!(vectors.next().is_none());
    test_compactor(&store, mode)
        .compact(&namespace)
        .await
        .unwrap();
    let object_regions = active_object_regions(&store, &namespace, mode).await;
    let wal_reader = WalReader::new(store.clone());

    let mut measurements = Vec::new();
    for divisor in SELECTIVITY_DIVISORS {
        let filter = equality_filter(divisor);
        counter.reset();
        let response = execute_query(QueryParams {
            store: &store,
            wal_reader: &wal_reader,
            namespace: &namespace,
            query: &query,
            top_k: TOP_K,
            nprobe: CLUSTERS,
            filter: Some(&filter),
            consistency: ConsistencyLevel::Eventual,
            distance_metric: DistanceMetric::Euclidean,
            oversample_factor: IndexingConfig::default().oversample_factor,
            rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
            resident_row_bypass: false,
            cache: None,
            manifest_cache: None,
            include_attributes: false,
        })
        .await
        .unwrap();
        assert_eq!(response.scanned_fragments, 0);
        assert_eq!(response.scanned_segments, 1);

        let expected = ground_truth
            .get(&divisor)
            .expect("ground truth must cover every measurement selectivity");
        let hits = response
            .results
            .iter()
            .filter(|result| expected.contains(&result.id))
            .count();
        let recall = hits as f64 / TOP_K as f64;
        let (coarse_bytes, rerank_bytes) = measured_region_bytes(&counter, &object_regions);
        measurements.push(Measurement {
            selectivity: selectivity_label(divisor),
            mode,
            recall,
            coarse_bytes,
            rerank_bytes,
        });
    }

    harness.cleanup().await;
    measurements
}

#[tokio::test]
#[ignore = "phase 4 slice 5 one-shot filtered-path measurement"]
async fn measure_filtered_two_bit_storage() {
    let mut measurements = measure_mode(QuantizationType::Scalar).await;
    measurements.extend(measure_mode(QuantizationType::TwoBit).await);

    println!("| selectivity | mode | recall@100 | coarse B/q | rerank B/q | total B/q |");
    println!("| --- | --- | --- | --- | --- | --- |");
    for measurement in measurements {
        println!(
            "| {} | {} | {:.6} | {} | {} | {} |",
            measurement.selectivity,
            mode_label(measurement.mode),
            measurement.recall,
            measurement.coarse_bytes,
            measurement.rerank_bytes,
            measurement.coarse_bytes + measurement.rerank_bytes,
        );
    }
}
