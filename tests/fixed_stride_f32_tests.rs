//! Phase 4 slice 9.3 — resident-row bypass of coarse cluster reads.
//!
//! Two shapes are measured, because they answer different questions.
//!
//! `QUALITY` keeps the whole corpus in a handful of fat clusters and asks for
//! `top_k = 100`, which is what makes recall@100 meaningful. It cannot say
//! anything about request count: a 400-row frontier over 8 clusters touches all
//! 8 by construction, so "did the bypass touch fewer objects than the probe
//! set" has only one possible answer there.
//!
//! `PRODUCTION_RATIO` exists to answer exactly that question. It reproduces the
//! production *ratios* — about 19% of clusters probed, a small `top_k`, and a
//! frontier far smaller than the probe set — at a corpus size the slice's
//! measurement budget allows. Winner concentration is a property of those
//! ratios, not of absolute corpus size.
//!
//! Both cells run the same unfiltered query and differ only in
//! `resident_row_bypass`, so the comparison is not confounded by the filter
//! path the way an `all_rows` control would be.

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
use zeppelin::types::{ConsistencyLevel, DistanceMetric, SearchResult, VectorEntry};
use zeppelin::wal::manifest::{
    ClusterDataObjectRef, ClusterRowLayoutRef, Manifest, CLUSTER_LAYOUT_VERSION_ZBP5,
};
use zeppelin::wal::{WalReader, WalWriter};

const ROWS: usize = 20_000;
const DIM: usize = 256;
const FRAGMENTS: usize = 4;
const SEED: u64 = 42;

/// One measured index/query geometry.
#[derive(Clone, Copy)]
struct Shape {
    /// Number of IVF clusters the fixture trains.
    clusters: usize,
    /// Clusters probed per query.
    nprobe: usize,
    /// Requested results.
    top_k: usize,
}

impl Shape {
    /// Rows retained by the unfiltered exact-rerank frontier: the unchanged 4x.
    const fn frontier(self) -> usize {
        self.top_k * 4
    }
}

/// Few fat clusters, full probe, `top_k = 100`: the recall@100 cell.
const QUALITY: Shape = Shape {
    clusters: 8,
    nprobe: 8,
    top_k: 100,
};

/// Production ratios — 12 of 64 clusters probed (18.75%, against production's
/// 63 of 334) and a 40-row frontier against a 3,750-row probe set. This is the
/// only shape in which winner concentration can be observed at all.
const PRODUCTION_RATIO: Shape = Shape {
    clusters: 64,
    nprobe: 12,
    top_k: 10,
};

struct FixedStrideFixture {
    harness: TestHarness,
    store: ZeppelinStore,
    counter: GetCounter,
    namespace: String,
    shape: Shape,
    query: Vec<f32>,
    vectors_by_id: HashMap<String, Vec<f32>>,
    ground_truth_ids: HashSet<String>,
    objects: Vec<ClusterDataObjectRef>,
}

#[derive(Debug, Default)]
struct ObjectIo {
    gets: usize,
    objects_touched: usize,
    header_bytes: u64,
    coarse_bytes: u64,
    id_bytes: u64,
    rerank_bytes: u64,
}

impl ObjectIo {
    fn add(&mut self, other: &Self) {
        self.gets += other.gets;
        self.objects_touched += other.objects_touched;
        self.header_bytes += other.header_bytes;
        self.coarse_bytes += other.coarse_bytes;
        self.id_bytes += other.id_bytes;
        self.rerank_bytes += other.rerank_bytes;
    }

    fn physical_bytes(&self) -> u64 {
        self.header_bytes + self.coarse_bytes + self.id_bytes + self.rerank_bytes
    }
}

fn indexing_config(shape: Shape) -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: shape.clusters,
        target_rows_per_cluster: ROWS / shape.clusters,
        max_num_centroids: shape.clusters,
        kmeans_max_iterations: 4,
        ..Default::default()
    }
}

fn test_compactor(store: &ZeppelinStore, shape: Shape) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        indexing_config(shape),
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
    }
    (generated, query)
}

async fn fixed_stride_fixture(name: &str, shape: Shape) -> FixedStrideFixture {
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
    let ground_truth_ids = exact
        .into_iter()
        .take(shape.top_k)
        .map(|(id, _)| id)
        .collect();

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
    test_compactor(&store, shape)
        .compact(&namespace)
        .await
        .unwrap();

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
    assert_eq!(active_segment.cluster_count, shape.clusters);
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
        shape,
        query,
        vectors_by_id,
        ground_truth_ids,
        objects: active_segment.cluster_objects.clone(),
    }
}

/// Runs one unfiltered query. `resident_row_bypass` is the only variable.
async fn run_query(fixture: &FixedStrideFixture, resident_row_bypass: bool) -> Vec<SearchResult> {
    let wal_reader = WalReader::new(fixture.store.clone());
    let response = execute_query(QueryParams {
        store: &fixture.store,
        wal_reader: &wal_reader,
        namespace: &fixture.namespace,
        query: &fixture.query,
        top_k: fixture.shape.top_k,
        nprobe: fixture.shape.nprobe,
        filter: None,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 1,
        rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
        resident_row_bypass,
        cache: None,
        manifest_cache: None,
        include_attributes: false,
    })
    .await
    .unwrap();
    assert_eq!(response.scanned_fragments, 0);
    assert_eq!(response.scanned_segments, 1);
    assert_eq!(response.results.len(), fixture.shape.top_k);
    response.results
}

fn recall(fixture: &FixedStrideFixture, results: &[SearchResult]) -> f64 {
    let hits = results
        .iter()
        .filter(|result| fixture.ground_truth_ids.contains(&result.id))
        .count();
    hits as f64 / fixture.shape.top_k as f64
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

/// The object's contiguous ID region: first ID byte through last ID byte.
fn id_region(object: &ClusterDataObjectRef) -> Range<usize> {
    let start = object
        .row_layouts
        .iter()
        .map(|layout| layout.ids_offset as usize)
        .min()
        .expect("ZBP5 object must declare row layouts");
    let end = object
        .row_layouts
        .iter()
        .map(|layout| (layout.ids_offset + layout.ids_len) as usize)
        .max()
        .expect("ZBP5 object must declare row layouts");
    start..end
}

fn measured_object_io(counter: &GetCounter, objects: &[ClusterDataObjectRef]) -> ObjectIo {
    let mut total = ObjectIo::default();
    for object in objects {
        let ranges = counter.ranges_for(&object.key);
        let mut object_io = ObjectIo {
            gets: ranges.len(),
            objects_touched: usize::from(!ranges.is_empty()),
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

/// The resident frontier remains approximate, but every returned score is exact f32.
#[tokio::test]
async fn resident_bypass_preserves_exact_distances_and_records_recall_at_100() {
    let fixture = fixed_stride_fixture("fixed-stride-quality", QUALITY).await;
    let results = run_query(&fixture, true).await;
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
    let bypass_recall = recall(&fixture, &results);
    let coarse_recall = recall(&fixture, &run_query(&fixture, false).await);
    println!(
        "fixed_stride_f32_quality coarse_recall@100={coarse_recall:.6} \
         bypass_recall@100={bypass_recall:.6}"
    );
    assert!(
        bypass_recall >= 0.90,
        "small-corpus row-frontier recall collapsed: {bypass_recall:.6}"
    );
    fixture.harness.cleanup().await;
}

/// A cold bypass reads whole needed ID blocks and fixed-stride f32 ranges only,
/// one coalesced ID GET per object.
#[tokio::test]
async fn resident_bypass_avoids_group_headers_and_coarse_regions() {
    let fixture = fixed_stride_fixture("fixed-stride-io", PRODUCTION_RATIO).await;

    // Control and treatment are the same unfiltered query; only the knob moves.
    fixture.counter.reset();
    let coarse_results = run_query(&fixture, false).await;
    let coarse_io = measured_object_io(&fixture.counter, &fixture.objects);
    let coarse_recall = recall(&fixture, &coarse_results);
    assert!(
        coarse_io.coarse_bytes > 0,
        "control did not exercise the v5 coarse path"
    );

    fixture.counter.reset();
    let bypass_results = run_query(&fixture, true).await;
    let bypass_io = measured_object_io(&fixture.counter, &fixture.objects);
    let bypass_recall = recall(&fixture, &bypass_results);

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

    // A ZBP5 object stores every ID block contiguously, so one object's winning
    // clusters must cost exactly one ID GET however many of them there are.
    // Asserting per-block equality here would re-pin the per-cluster fetch this
    // slice exists to remove.
    for object in &fixture.objects {
        let ids = id_region(object);
        let mut id_gets = 0;
        for range in fixture.counter.ranges_for(&object.key) {
            let in_ids = range_intersection_len(&range, &ids);
            let vector_bytes = object
                .row_layouts
                .iter()
                .map(|layout| {
                    let [_, _, vectors] = layout_ranges(layout);
                    range_intersection_len(&range, &vectors)
                })
                .sum::<u64>();
            let span = (range.end - range.start) as u64;
            if in_ids > 0 {
                assert_eq!(
                    in_ids, span,
                    "an ID fetch for {} strayed outside the ID region: {range:?}",
                    object.key
                );
                id_gets += 1;
            } else {
                assert_eq!(
                    vector_bytes, span,
                    "bypass issued a non-ID/non-vector range for {}: {range:?}",
                    object.key
                );
            }
        }
        assert!(
            id_gets <= 1,
            "bypass issued {id_gets} ID GETs for {}; one coalesced span per object is the contract",
            object.key
        );
    }

    println!(
        "| path | GETs/q | objects | header B/q | coarse B/q | ID B/q | rerank B/q | recall |"
    );
    println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    for (label, io, measured_recall) in [
        ("v5 coarse path", &coarse_io, coarse_recall),
        ("resident-row bypass", &bypass_io, bypass_recall),
    ] {
        println!(
            "| {label} | {} | {} | {} | {} | {} | {} | {:.6} |",
            io.gets,
            io.objects_touched,
            io.header_bytes,
            io.coarse_bytes,
            io.id_bytes,
            io.rerank_bytes,
            measured_recall
        );
    }
    let frontier = fixture.shape.frontier();
    println!(
        "fixed_stride_f32_bytes coarse_physical={} bypass_physical={} useful_f32={} \
         bypass_amplification={:.6}",
        coarse_io.physical_bytes(),
        bypass_io.physical_bytes(),
        frontier * DIM * 4,
        bypass_io.rerank_bytes as f64 / (frontier * DIM * 4) as f64
    );
    let grouping_arity = fixture
        .objects
        .iter()
        .map(|object| object.clusters.len())
        .max()
        .unwrap_or(0);
    println!(
        "fixed_stride_f32_shape seed={SEED} clusters={} nprobe={} top_k={} frontier={frontier} \
         grouping_arity={grouping_arity} objects_total={} coarse_objects={} bypass_objects={}",
        fixture.shape.clusters,
        fixture.shape.nprobe,
        fixture.shape.top_k,
        fixture.objects.len(),
        coarse_io.objects_touched,
        bypass_io.objects_touched
    );

    fixture.harness.cleanup().await;
}
