mod common;

use std::collections::HashMap;
use std::ops::Range;
use std::time::{Duration, Instant};

use common::counting::{counting_store, GetCounter};
use common::harness::TestHarness;
use common::vectors::random_vectors;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
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
const NPROBE: usize = 4;
const TOP_K: usize = 5;
const FILTER_DIVISOR: usize = 2;
const FILTER_OVERSAMPLE: usize = 3;
const RERANK_FACTOR: usize = 4;
const RERANK_CANDIDATES: usize = TOP_K * FILTER_OVERSAMPLE * RERANK_FACTOR;
const GROUP_OBJECT_HEADER_LEN: usize = 8;
const GROUP_OBJECT_ENTRY_LEN: usize = 4 + 8 * 4;
const GAPS: [(&str, usize); 4] = [
    ("1 MiB", 1024 * 1024),
    ("128 KiB", 128 * 1024),
    ("32 KiB", 32 * 1024),
    ("8 KiB", 8 * 1024),
];

struct ObjectRegions {
    key: String,
    coarse: Range<usize>,
    full: Range<usize>,
}

struct Measurement {
    gap_label: &'static str,
    gap_bytes: usize,
    coarse_bytes: u64,
    rerank_bytes: u64,
    rerank_ranges: usize,
    logical_bytes: u64,
    wall_clock: Option<Duration>,
    results: Vec<(String, u32)>,
}

fn indexing_config() -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: CLUSTERS,
        target_rows_per_cluster: ROWS / CLUSTERS,
        max_num_centroids: CLUSTERS,
        kmeans_max_iterations: 4,
        quantization: QuantizationType::TwoBit,
        ..Default::default()
    }
}

fn test_compactor(store: &ZeppelinStore) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        indexing_config(),
        common::default_gc_upload_window(),
    )
}

fn bucket_field() -> String {
    format!("bucket_{FILTER_DIVISOR}")
}

fn synthetic_corpus() -> (Vec<VectorEntry>, Vec<f32>) {
    let mut generated = random_vectors(ROWS + 1, DIM);
    let query = generated
        .pop()
        .expect("synthetic corpus must include one held-out query")
        .values;

    for (row, vector) in generated.iter_mut().enumerate() {
        let fragment = row / (ROWS / FRAGMENTS);
        vector.id = format!("f{fragment}-row-{row:05}");
        vector.attributes = Some(HashMap::from([(
            bucket_field(),
            AttributeValue::Integer((row % FILTER_DIVISOR) as i64),
        )]));
    }

    let matching_rows = generated
        .iter()
        .filter(|vector| {
            vector
                .attributes
                .as_ref()
                .and_then(|attributes| attributes.get(&bucket_field()))
                == Some(&AttributeValue::Integer(0))
        })
        .count();
    assert_eq!(matching_rows, ROWS / FILTER_DIVISOR);
    assert!(matching_rows >= RERANK_CANDIDATES);

    (generated, query)
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

async fn active_object_regions(store: &ZeppelinStore, namespace: &str) -> Vec<ObjectRegions> {
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
    assert_eq!(segment.vector_count, ROWS);
    assert_eq!(segment.cluster_count, CLUSTERS);
    assert_eq!(segment.quantization, QuantizationType::TwoBit);
    assert_eq!(
        manifest.coarse_payload_encoding(segment_id),
        CoarsePayloadEncoding::TwoBit
    );

    let mut regions = Vec::with_capacity(segment.cluster_objects.len());
    for object in &segment.cluster_objects {
        let data = store.get(&object.key).await.unwrap();
        regions.push(parse_object_regions(object.key.clone(), &data));
    }
    regions
}

fn measured_region_metrics(
    counter: &GetCounter,
    object_regions: &[ObjectRegions],
) -> (u64, u64, usize) {
    let mut coarse_bytes = 0_u64;
    let mut rerank_bytes = 0_u64;
    let mut rerank_ranges = 0_usize;
    for object in object_regions {
        let ranges = counter.ranges_for(&object.key);
        if ranges.is_empty() {
            continue;
        }
        for range in ranges {
            let bytes = (range.end - range.start) as u64;
            if range.start >= object.coarse.start && range.end <= object.coarse.end {
                coarse_bytes += bytes;
            } else if range.start >= object.full.start && range.end <= object.full.end {
                rerank_bytes += bytes;
                rerank_ranges += 1;
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
    assert!(coarse_bytes > 0, "query read no measured coarse bytes");
    assert!(rerank_bytes > 0, "query read no measured rerank bytes");
    assert!(rerank_ranges > 0, "query issued no measured rerank ranges");
    (coarse_bytes, rerank_bytes, rerank_ranges)
}

fn equality_filter() -> Filter {
    Filter::Eq {
        field: bucket_field(),
        value: AttributeValue::Integer(0),
    }
}

async fn measure_gap_curve() -> (String, Vec<Measurement>) {
    let backend = std::env::var("TEST_BACKEND").unwrap_or_else(|_| "memory".to_string());
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.artifact_origin_namespace("coalescing-gap");
    common::seed_active_namespace(&store, &namespace, DIM, DistanceMetric::Euclidean).await;

    let (vectors, query) = synthetic_corpus();
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
    test_compactor(&store).compact(&namespace).await.unwrap();
    let object_regions = active_object_regions(&store, &namespace).await;
    let wal_reader = WalReader::new(store.clone());
    let filter = equality_filter();
    let oversample_factor = IndexingConfig::default().oversample_factor;
    assert_eq!(oversample_factor, FILTER_OVERSAMPLE);

    let mut measurements = Vec::with_capacity(GAPS.len());
    for (gap_label, gap_bytes) in GAPS {
        counter.reset();
        let started = Instant::now();
        let response = execute_query(QueryParams {
            store: &store,
            wal_reader: &wal_reader,
            namespace: &namespace,
            query: &query,
            top_k: TOP_K,
            nprobe: NPROBE,
            filter: Some(&filter),
            consistency: ConsistencyLevel::Eventual,
            distance_metric: DistanceMetric::Euclidean,
            oversample_factor,
            rerank_coalesce_gap_bytes: gap_bytes,
            cache: None,
            manifest_cache: None,
            include_attributes: false,
        })
        .await
        .unwrap();
        let elapsed = started.elapsed();

        assert_eq!(
            response.results.len(),
            TOP_K,
            "gap cell {gap_label} did not saturate the requested top-k"
        );
        assert_eq!(response.scanned_fragments, 0);
        assert_eq!(response.scanned_segments, 1);
        let (coarse_bytes, rerank_bytes, rerank_ranges) =
            measured_region_metrics(&counter, &object_regions);
        let logical_bytes = (RERANK_CANDIDATES * DIM * size_of::<f32>()) as u64;
        measurements.push(Measurement {
            gap_label,
            gap_bytes,
            coarse_bytes,
            rerank_bytes,
            rerank_ranges,
            logical_bytes,
            wall_clock: (backend == "minio").then_some(elapsed),
            results: response
                .results
                .into_iter()
                .map(|result| (result.id, result.score.to_bits()))
                .collect(),
        });
    }

    harness.cleanup().await;
    (backend, measurements)
}

fn assert_result_identity(measurements: &[Measurement]) {
    let control = measurements
        .first()
        .expect("gap curve must include the control cell");
    for measurement in &measurements[1..] {
        assert_eq!(
            measurement.results, control.results,
            "top-k IDs or distance bits changed between {} and {}",
            control.gap_label, measurement.gap_label
        );
    }
}

fn wall_clock_label(wall_clock: Option<Duration>) -> String {
    wall_clock.map_or_else(
        || "not measured".to_string(),
        |elapsed| format!("{:.3} ms", elapsed.as_secs_f64() * 1000.0),
    )
}

#[tokio::test]
#[ignore = "phase 4 slice 6.1 one-shot filtered gap-curve measurement"]
async fn measure_filtered_gap_curve() {
    let (backend, measurements) = measure_gap_curve().await;
    assert_result_identity(&measurements);

    println!("backend: {backend}");
    println!(
        "| gap | gap bytes | coarse B/q | rerank B/q | rerank ranges/q | logical B/q | A | ranges/candidate | wall clock |"
    );
    println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |");
    for measurement in measurements {
        println!(
            "| {} | {} | {} | {} | {} | {} | {:.6} | {:.6} | {} |",
            measurement.gap_label,
            measurement.gap_bytes,
            measurement.coarse_bytes,
            measurement.rerank_bytes,
            measurement.rerank_ranges,
            measurement.logical_bytes,
            measurement.rerank_bytes as f64 / measurement.logical_bytes as f64,
            measurement.rerank_ranges as f64 / RERANK_CANDIDATES as f64,
            wall_clock_label(measurement.wall_clock),
        );
    }
}

#[tokio::test]
async fn result_identity_across_gaps() {
    let (_, measurements) = measure_gap_curve().await;
    assert_result_identity(&measurements);
}
