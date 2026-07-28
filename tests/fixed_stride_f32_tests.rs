mod common;

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use common::counting::{counting_store, GetCounter};
use common::harness::TestHarness;
use common::vectors::random_vectors;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig, DEFAULT_RERANK_COALESCE_GAP_BYTES};
use zeppelin::index::distance::compute_distance;
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{
    AttributeValue, ConsistencyLevel, DistanceMetric, Filter, SearchResult, VectorEntry,
};
use zeppelin::wal::manifest::{
    ClusterDataObjectRef, ClusterRowLayoutRef, Manifest, CLUSTER_LAYOUT_VERSION_ZBP5,
};
use zeppelin::wal::{WalReader, WalWriter};

const ROWS: usize = 20_000;
const DIM: usize = 256;
const FRAGMENTS: usize = 4;
const CLUSTERS: usize = 8;
const NPROBE: usize = 8;
const TOP_K: usize = 100;
const FRONTIER: usize = TOP_K * 4;
const SEED: u64 = 42;

struct FixedStrideFixture {
    harness: TestHarness,
    store: ZeppelinStore,
    counter: GetCounter,
    namespace: String,
    query: Vec<f32>,
    vectors_by_id: HashMap<String, Vec<f32>>,
    ground_truth_ids: HashSet<String>,
    objects: Vec<ClusterDataObjectRef>,
}

#[derive(Debug, Default)]
struct ObjectIo {
    gets: usize,
    header_bytes: u64,
    coarse_bytes: u64,
    id_bytes: u64,
    rerank_bytes: u64,
}

impl ObjectIo {
    fn add(&mut self, other: &Self) {
        self.gets += other.gets;
        self.header_bytes += other.header_bytes;
        self.coarse_bytes += other.coarse_bytes;
        self.id_bytes += other.id_bytes;
        self.rerank_bytes += other.rerank_bytes;
    }

    fn physical_bytes(&self) -> u64 {
        self.header_bytes + self.coarse_bytes + self.id_bytes + self.rerank_bytes
    }
}

fn indexing_config() -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: CLUSTERS,
        target_rows_per_cluster: ROWS / CLUSTERS,
        max_num_centroids: CLUSTERS,
        kmeans_max_iterations: 4,
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

fn synthetic_corpus() -> (Vec<VectorEntry>, Vec<f32>) {
    let mut generated = random_vectors(ROWS + 1, DIM);
    let query = generated
        .pop()
        .expect("fixed-stride fixture needs one held-out query")
        .values;
    for (row, vector) in generated.iter_mut().enumerate() {
        let fragment = row / (ROWS / FRAGMENTS);
        vector.id = format!("f{fragment}-row-{row:05}");
        vector.attributes = Some(HashMap::from([(
            "all_rows".to_string(),
            AttributeValue::Integer(1),
        )]));
    }
    (generated, query)
}

async fn fixed_stride_fixture(name: &str) -> FixedStrideFixture {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.artifact_origin_namespace(name);
    common::seed_active_namespace(&store, &namespace, DIM, DistanceMetric::Euclidean).await;

    let (vectors, query) = synthetic_corpus();
    let vectors_by_id = vectors
        .iter()
        .map(|vector| (vector.id.clone(), vector.values.clone()))
        .collect::<HashMap<_, _>>();
    let mut exact = vectors
        .iter()
        .map(|vector| {
            (
                vector.id.clone(),
                compute_distance(&query, &vector.values, DistanceMetric::Euclidean),
            )
        })
        .collect::<Vec<_>>();
    exact.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    let ground_truth_ids = exact.into_iter().take(TOP_K).map(|(id, _)| id).collect();

    let writer = WalWriter::new(store.clone());
    let mut vectors = vectors.into_iter();
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

    let manifest = Manifest::read(&store, &namespace)
        .await
        .unwrap()
        .expect("fixed-stride fixture manifest must exist");
    let active_segment = manifest
        .active_segment
        .as_deref()
        .and_then(|active| {
            manifest
                .segments
                .iter()
                .find(|segment| segment.id == active)
        })
        .expect("fixed-stride fixture must publish one active segment");
    assert_eq!(active_segment.vector_count, ROWS);
    assert_eq!(active_segment.cluster_count, CLUSTERS);
    assert!(
        active_segment
            .cluster_objects
            .iter()
            .all(|object| object.cluster_layout_version == CLUSTER_LAYOUT_VERSION_ZBP5),
        "fixture must publish only ZBP5 objects"
    );

    FixedStrideFixture {
        harness,
        store,
        counter,
        namespace,
        query,
        vectors_by_id,
        ground_truth_ids,
        objects: active_segment.cluster_objects.clone(),
    }
}

fn all_rows_filter() -> Filter {
    Filter::Eq {
        field: "all_rows".to_string(),
        value: AttributeValue::Integer(1),
    }
}

async fn run_query(fixture: &FixedStrideFixture, filter: Option<&Filter>) -> Vec<SearchResult> {
    let wal_reader = WalReader::new(fixture.store.clone());
    let response = execute_query(QueryParams {
        store: &fixture.store,
        wal_reader: &wal_reader,
        namespace: &fixture.namespace,
        query: &fixture.query,
        top_k: TOP_K,
        nprobe: NPROBE,
        filter,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: false,
    })
    .await
    .unwrap();
    assert_eq!(response.scanned_fragments, 0);
    assert_eq!(response.scanned_segments, 1);
    assert_eq!(response.results.len(), TOP_K);
    response.results
}

fn recall_at_100(fixture: &FixedStrideFixture, results: &[SearchResult]) -> f64 {
    let hits = results
        .iter()
        .filter(|result| fixture.ground_truth_ids.contains(&result.id))
        .count();
    hits as f64 / TOP_K as f64
}

fn range_intersection_len(left: &Range<usize>, right: &Range<usize>) -> u64 {
    let start = left.start.max(right.start);
    let end = left.end.min(right.end);
    end.saturating_sub(start) as u64
}

fn layout_ranges(layout: &ClusterRowLayoutRef) -> [Range<usize>; 3] {
    [
        layout.coarse_offset as usize..(layout.coarse_offset + layout.coarse_len) as usize,
        layout.ids_offset as usize..(layout.ids_offset + layout.ids_len) as usize,
        layout.vectors_offset as usize..(layout.vectors_offset + layout.vectors_len) as usize,
    ]
}

fn measured_object_io(counter: &GetCounter, objects: &[ClusterDataObjectRef]) -> ObjectIo {
    let mut total = ObjectIo::default();
    for object in objects {
        let ranges = counter.ranges_for(&object.key);
        let mut object_io = ObjectIo {
            gets: ranges.len(),
            ..ObjectIo::default()
        };
        let first_data_offset = object
            .row_layouts
            .iter()
            .map(|layout| layout.coarse_offset as usize)
            .min()
            .expect("ZBP5 object must declare row layouts");
        let header = 0..first_data_offset;
        for range in ranges {
            object_io.header_bytes += range_intersection_len(&range, &header);
            let mut classified = range_intersection_len(&range, &header);
            for layout in &object.row_layouts {
                let [coarse, ids, vectors] = layout_ranges(layout);
                object_io.coarse_bytes += range_intersection_len(&range, &coarse);
                object_io.id_bytes += range_intersection_len(&range, &ids);
                object_io.rerank_bytes += range_intersection_len(&range, &vectors);
                classified += range_intersection_len(&range, &coarse)
                    + range_intersection_len(&range, &ids)
                    + range_intersection_len(&range, &vectors);
            }
            assert_eq!(
                classified,
                (range.end - range.start) as u64,
                "unclassified grouped-object bytes for {} range {range:?}",
                object.key
            );
        }
        total.add(&object_io);
    }
    total
}

fn id_blocks_read(counter: &GetCounter, objects: &[ClusterDataObjectRef]) -> usize {
    objects
        .iter()
        .map(|object| {
            let ranges = counter.ranges_for(&object.key);
            object
                .row_layouts
                .iter()
                .filter(|layout| {
                    let [_, ids, _] = layout_ranges(layout);
                    ranges.contains(&ids)
                })
                .count()
        })
        .sum()
}

/// The resident frontier remains approximate, but every returned score is exact f32.
#[tokio::test]
async fn resident_bypass_preserves_exact_distances_and_records_recall_at_100() {
    let fixture = fixed_stride_fixture("fixed-stride-quality").await;
    let results = run_query(&fixture, None).await;
    for result in &results {
        let vector = fixture
            .vectors_by_id
            .get(&result.id)
            .expect("every returned ID must belong to the corpus");
        let exact = compute_distance(&fixture.query, vector, DistanceMetric::Euclidean);
        assert_eq!(
            result.score.to_bits(),
            exact.to_bits(),
            "returned distance was not recomputed from exact f32 for {}",
            result.id
        );
    }
    let recall = recall_at_100(&fixture, &results);
    println!("fixed_stride_f32_quality recall@100={recall:.6}");
    assert!(
        recall >= 0.90,
        "small-corpus row-frontier recall collapsed: {recall:.6}"
    );
    fixture.harness.cleanup().await;
}

/// A cold bypass reads whole needed ID blocks and fixed-stride f32 ranges only.
#[tokio::test]
async fn resident_bypass_avoids_group_headers_and_coarse_regions() {
    let fixture = fixed_stride_fixture("fixed-stride-io").await;
    let filter = all_rows_filter();

    fixture.counter.reset();
    let coarse_results = run_query(&fixture, Some(&filter)).await;
    let coarse_io = measured_object_io(&fixture.counter, &fixture.objects);
    let coarse_recall = recall_at_100(&fixture, &coarse_results);
    assert!(
        coarse_io.coarse_bytes > 0,
        "filtered control did not exercise the current v5 coarse path"
    );

    fixture.counter.reset();
    let bypass_results = run_query(&fixture, None).await;
    let bypass_io = measured_object_io(&fixture.counter, &fixture.objects);
    let winner_clusters = id_blocks_read(&fixture.counter, &fixture.objects);
    let bypass_recall = recall_at_100(&fixture, &bypass_results);

    assert_eq!(
        bypass_io.header_bytes, 0,
        "resident bypass fetched a grouped-object header"
    );
    assert_eq!(
        bypass_io.coarse_bytes, 0,
        "resident bypass fetched a coarse-code region"
    );
    assert!(
        bypass_io.id_bytes > 0,
        "resident bypass fetched no ID blocks"
    );
    assert!(
        bypass_io.rerank_bytes > 0,
        "resident bypass fetched no fixed-stride vectors"
    );
    for object in &fixture.objects {
        let ranges = fixture.counter.ranges_for(&object.key);
        for range in ranges {
            let matching_id_block = object.row_layouts.iter().any(|layout| {
                let [_, ids, _] = layout_ranges(layout);
                range == ids
            });
            let vector_bytes = object
                .row_layouts
                .iter()
                .map(|layout| {
                    let [_, _, vectors] = layout_ranges(layout);
                    range_intersection_len(&range, &vectors)
                })
                .sum::<u64>();
            let lies_in_vector_regions = vector_bytes == (range.end - range.start) as u64;
            assert!(
                matching_id_block || lies_in_vector_regions,
                "bypass issued a non-ID/non-vector range for {}: {range:?}",
                object.key
            );
        }
    }

    println!("| path | GETs/q | header B/q | coarse B/q | ID B/q | rerank B/q | recall@100 |");
    println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: |");
    println!(
        "| current v5 coarse path | {} | {} | {} | {} | {} | {:.6} |",
        coarse_io.gets,
        coarse_io.header_bytes,
        coarse_io.coarse_bytes,
        coarse_io.id_bytes,
        coarse_io.rerank_bytes,
        coarse_recall
    );
    println!(
        "| resident-row bypass | {} | {} | {} | {} | {} | {:.6} |",
        bypass_io.gets,
        bypass_io.header_bytes,
        bypass_io.coarse_bytes,
        bypass_io.id_bytes,
        bypass_io.rerank_bytes,
        bypass_recall
    );
    println!(
        "fixed_stride_f32_bytes physical={} useful_f32={} amplification={:.6}",
        bypass_io.physical_bytes(),
        FRONTIER * DIM * 4,
        bypass_io.rerank_bytes as f64 / (FRONTIER * DIM * 4) as f64
    );
    let grouping_arity = fixture
        .objects
        .iter()
        .map(|object| object.clusters.len())
        .max()
        .unwrap_or(0);
    println!(
        "fixed_stride_f32_shape seed={SEED} grouping_arity={grouping_arity} winner_clusters={winner_clusters}"
    );

    fixture.harness.cleanup().await;
}
