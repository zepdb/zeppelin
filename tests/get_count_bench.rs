//! H1 — deterministic GET-count benchmark for core vector query paths.
//!
//! This suite intentionally measures object GET operations, not logical
//! roundtrip phases. The cold SQ8 vector path currently needs more than the
//! thesis-level "2 sequential S3 roundtrips" because it reads separate
//! manifest, centroid, SQ sidecar, full-precision cluster, and attrs objects.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::counting::{counting_store, ArtifactClass, GetCounter};
use common::harness::TestHarness;
use common::vectors::{random_vectors, simple_attributes, with_attributes};

use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::query::{execute_query, QueryParams, QueryResponse};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::manifest::Manifest;
use zeppelin::wal::{WalReader, WalWriter};

const DIM: usize = 4;
const VECTOR_COUNT: usize = 4;
const NUM_CENTROIDS: usize = 4;
const ALL_CLUSTERS: usize = 4;
const TOP_K_ALL: usize = 4;

#[derive(Debug, Clone, Copy)]
struct ExpectedGets {
    manifest: u64,
    centroids: u64,
    sq: u64,
    cluster: u64,
    attrs: u64,
    wal: u64,
}

impl ExpectedGets {
    fn total(self) -> u64 {
        self.manifest + self.centroids + self.sq + self.cluster + self.attrs + self.wal
    }
}

struct BenchFixture {
    harness: TestHarness,
    store: ZeppelinStore,
    counter: GetCounter,
    namespace: String,
    wal_reader: WalReader,
    manifest_cache: Arc<ManifestCache>,
    query: Vec<f32>,
}

fn indexing_config() -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: NUM_CENTROIDS,
        default_nprobe: ALL_CLUSTERS,
        kmeans_max_iterations: 10,
        // H1 measures the default SQ8 vector path, not bitmap prefiltering.
        bitmap_index: false,
        ..Default::default()
    }
}

fn test_compactor(store: &ZeppelinStore) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 3,
            ..Default::default()
        },
        indexing_config(),
    )
}

fn bench_vectors() -> Vec<VectorEntry> {
    with_attributes(random_vectors(VECTOR_COUNT, DIM), simple_attributes)
}

async fn compacted_fixture(label: &str) -> BenchFixture {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.key(label);
    let writer = WalWriter::new(store.clone());

    Manifest::new().write(&store, &namespace).await.unwrap();
    let vectors = bench_vectors();
    let query = vectors[0].values.clone();
    writer.append(&namespace, vectors, vec![]).await.unwrap();

    test_compactor(&store).compact(&namespace).await.unwrap();

    let manifest = Manifest::read(&store, &namespace).await.unwrap().unwrap();
    assert!(
        manifest.uncompacted_fragments().is_empty(),
        "fixture must start with no uncompacted WAL fragments"
    );
    let active_segment_id = manifest.active_segment.as_ref().unwrap();
    let active_segment = manifest
        .segments
        .iter()
        .find(|segment| segment.id == *active_segment_id)
        .unwrap();
    assert_eq!(active_segment.cluster_count, NUM_CENTROIDS);

    let manifest_cache = Arc::new(ManifestCache::new(Duration::from_secs(60)));
    warm_manifest_cache(&store, &namespace, &manifest_cache, 0).await;
    counter.reset();

    BenchFixture {
        harness,
        store: store.clone(),
        counter,
        namespace,
        wal_reader: WalReader::new(store),
        manifest_cache,
        query,
    }
}

async fn warm_manifest_cache(
    store: &ZeppelinStore,
    namespace: &str,
    manifest_cache: &Arc<ManifestCache>,
    expected_uncompacted_fragments: usize,
) {
    let manifest = manifest_cache.get(store, namespace).await.unwrap();
    assert_eq!(
        manifest.uncompacted_fragments().len(),
        expected_uncompacted_fragments,
        "manifest cache warmup must cache the intended manifest generation"
    );
}

async fn run_query(
    fixture: &BenchFixture,
    consistency: ConsistencyLevel,
    nprobe: usize,
    cache: Option<&Arc<DiskCache>>,
) -> QueryResponse {
    execute_query(QueryParams {
        store: &fixture.store,
        wal_reader: &fixture.wal_reader,
        namespace: &fixture.namespace,
        query: &fixture.query,
        top_k: TOP_K_ALL,
        nprobe,
        filter: None,
        consistency,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 3,
        cache,
        manifest_cache: Some(&fixture.manifest_cache),
    })
    .await
    .unwrap()
}

fn print_profile(label: &str, counter: &GetCounter) {
    println!("H1 profile: {label}");
    println!("{}", counter.report());
}

fn assert_get_profile(counter: &GetCounter, expected: ExpectedGets) {
    assert_eq!(counter.gets_for(ArtifactClass::Manifest), expected.manifest);
    assert_eq!(
        counter.gets_for(ArtifactClass::Centroids),
        expected.centroids
    );
    assert_eq!(counter.gets_for(ArtifactClass::Sq), expected.sq);
    assert_eq!(counter.gets_for(ArtifactClass::Cluster), expected.cluster);
    assert_eq!(counter.gets_for(ArtifactClass::Attrs), expected.attrs);
    assert_eq!(counter.gets_for(ArtifactClass::Wal), expected.wal);
    assert_eq!(counter.gets_for(ArtifactClass::Bitmap), 0);
    assert_eq!(counter.gets_for(ArtifactClass::Fts), 0);
    assert_eq!(
        counter.gets_for(ArtifactClass::Other),
        0,
        "Other GETs mean an unclassified or unexpected query roundtrip"
    );
    assert_eq!(counter.total_gets(), expected.total());
}

async fn append_uncompacted_vector(store: &ZeppelinStore, namespace: &str) {
    let writer = WalWriter::new(store.clone());
    writer
        .append(
            namespace,
            vec![VectorEntry {
                id: "fresh_uncompacted".to_string(),
                values: vec![10.0, 10.0, 10.0, 10.0],
                attributes: Some(simple_attributes(99)),
            }],
            vec![],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn cold_strong_vector_query_pins_sq8_get_profile() {
    let fixture = compacted_fixture("h1-cold-strong").await;

    let response = run_query(&fixture, ConsistencyLevel::Strong, ALL_CLUSTERS, None).await;
    assert_eq!(response.scanned_segments, 1);
    assert_eq!(response.scanned_fragments, 0);
    assert_eq!(response.results.len(), TOP_K_ALL);

    print_profile("cold strong vector query, SQ8, nprobe=4", &fixture.counter);

    // H1 cold strong profile, with only the manifest cache preloaded:
    // - manifest=1: Task 13 strong If-None-Match freshness GET.
    // - centroids=1: active segment IVF centroid blob.
    // - sq=5: SQ8 calibration plus one SQ cluster sidecar per probed cluster.
    // - cluster=4: full-precision cluster blobs for SQ8 rerank.
    // - attrs=4: lazy final-result enrichment still needs all four attrs
    //   blobs in this fixture because top_k=4 returns one vector from each
    //   one-vector cluster. attrs_laziness_tests pins the reduced top_k=1
    //   profile where only the winning cluster's attrs are fetched.
    // - total=15: honest object GET count, not the thesis-level "2".
    assert_get_profile(
        &fixture.counter,
        ExpectedGets {
            manifest: 1,
            centroids: 1,
            sq: 5,
            cluster: 4,
            attrs: 4,
            wal: 0,
        },
    );

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn eventual_query_is_two_gets_cheaper_than_strong_with_uncompacted_upsert() {
    let fixture = compacted_fixture("h1-eventual-strong-delta").await;
    append_uncompacted_vector(&fixture.store, &fixture.namespace).await;
    fixture.manifest_cache.invalidate(&fixture.namespace);
    warm_manifest_cache(
        &fixture.store,
        &fixture.namespace,
        &fixture.manifest_cache,
        1,
    )
    .await;

    fixture.counter.reset();
    let eventual = run_query(&fixture, ConsistencyLevel::Eventual, ALL_CLUSTERS, None).await;
    assert_eq!(eventual.scanned_segments, 1);
    assert_eq!(
        eventual.scanned_fragments, 0,
        "Eventual reads no delete-free WAL fragments"
    );
    print_profile(
        "eventual vector query with one uncompacted upsert, SQ8, nprobe=4",
        &fixture.counter,
    );
    assert_get_profile(
        &fixture.counter,
        ExpectedGets {
            manifest: 0,
            centroids: 1,
            sq: 5,
            cluster: 4,
            attrs: 4,
            wal: 0,
        },
    );
    let eventual_total = fixture.counter.total_gets();

    fixture.counter.reset();
    let strong = run_query(&fixture, ConsistencyLevel::Strong, ALL_CLUSTERS, None).await;
    assert_eq!(strong.scanned_segments, 1);
    assert_eq!(
        strong.scanned_fragments, 1,
        "Strong must scan the uncompacted upsert fragment"
    );
    print_profile(
        "strong vector query with one uncompacted upsert, SQ8, nprobe=4",
        &fixture.counter,
    );
    assert_get_profile(
        &fixture.counter,
        ExpectedGets {
            manifest: 1,
            centroids: 1,
            sq: 5,
            cluster: 4,
            attrs: 4,
            wal: 1,
        },
    );
    let strong_total = fixture.counter.total_gets();

    assert_eq!(eventual_total, 14);
    assert_eq!(strong_total, 16);
    assert_eq!(
        strong_total - eventual_total,
        2,
        "Strong pays one freshness manifest GET plus one WAL fragment GET"
    );

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn cluster_gets_scale_exactly_with_nprobe() {
    let fixture = compacted_fixture("h1-nprobe-scaling").await;

    let response = run_query(&fixture, ConsistencyLevel::Strong, 1, None).await;
    assert_eq!(response.scanned_segments, 1);
    assert_eq!(response.scanned_fragments, 0);
    print_profile("strong vector query, SQ8, nprobe=1", &fixture.counter);
    assert_get_profile(
        &fixture.counter,
        ExpectedGets {
            manifest: 1,
            centroids: 1,
            sq: 2,
            cluster: 1,
            attrs: 1,
            wal: 0,
        },
    );
    let cluster_gets_nprobe_1 = fixture.counter.gets_for(ArtifactClass::Cluster);

    fixture.counter.reset();
    let response = run_query(&fixture, ConsistencyLevel::Strong, ALL_CLUSTERS, None).await;
    assert_eq!(response.scanned_segments, 1);
    assert_eq!(response.scanned_fragments, 0);
    print_profile("strong vector query, SQ8, nprobe=4", &fixture.counter);
    assert_get_profile(
        &fixture.counter,
        ExpectedGets {
            manifest: 1,
            centroids: 1,
            sq: 5,
            cluster: 4,
            attrs: 4,
            wal: 0,
        },
    );
    let cluster_gets_nprobe_4 = fixture.counter.gets_for(ArtifactClass::Cluster);

    assert_eq!(cluster_gets_nprobe_1, 1);
    assert_eq!(cluster_gets_nprobe_4, cluster_gets_nprobe_1 * 4);

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn warm_query_serves_segment_artifacts_from_cache() {
    let fixture = compacted_fixture("h1-warm-query").await;
    let cache_dir = tempfile::TempDir::new().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().to_path_buf(), 100 * 1024 * 1024).unwrap(),
    );

    let cold = run_query(
        &fixture,
        ConsistencyLevel::Strong,
        ALL_CLUSTERS,
        Some(&cache),
    )
    .await;
    assert_eq!(cold.scanned_segments, 1);
    assert_eq!(cold.scanned_fragments, 0);
    print_profile(
        "cold strong vector query with empty disk cache, SQ8, nprobe=4",
        &fixture.counter,
    );
    assert_get_profile(
        &fixture.counter,
        ExpectedGets {
            manifest: 1,
            centroids: 1,
            sq: 5,
            cluster: 4,
            attrs: 4,
            wal: 0,
        },
    );
    let cold_total = fixture.counter.total_gets();

    fixture.counter.reset();
    let warm = run_query(
        &fixture,
        ConsistencyLevel::Strong,
        ALL_CLUSTERS,
        Some(&cache),
    )
    .await;
    assert_eq!(warm.scanned_segments, 1);
    assert_eq!(warm.scanned_fragments, 0);
    print_profile(
        "warm strong vector query with populated disk cache, SQ8, nprobe=4",
        &fixture.counter,
    );
    assert_get_profile(
        &fixture.counter,
        ExpectedGets {
            manifest: 1,
            centroids: 0,
            sq: 0,
            cluster: 0,
            attrs: 0,
            wal: 0,
        },
    );
    let warm_total = fixture.counter.total_gets();

    assert_eq!(cold_total, 15);
    assert_eq!(warm_total, 1);
    assert!(
        warm_total < cold_total,
        "warm query must reduce S3 GETs by serving segment artifacts from cache"
    );

    fixture.harness.cleanup().await;
}
