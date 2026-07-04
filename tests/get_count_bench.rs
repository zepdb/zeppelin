//! H1 — deterministic GET-count benchmark for core vector query paths.
//!
//! This suite intentionally measures object GET operations, not logical
//! roundtrip phases. New SQ8 segments co-locate SQ sidecars with full clusters
//! and embed calibration in centroids; legacy fixtures remain pinned so the
//! backwards-compatibility boundary stays measurable.

mod common;

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

use common::counting::{counting_store, ArtifactClass, GetCounter};
use common::harness::TestHarness;
use common::vectors::{random_vectors, simple_attributes, with_attributes};

use zeppelin::cache::manifest_cache::ManifestCache;
use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::index::quantization::sq::{serialize_sq_cluster, SqCalibration};
use zeppelin::index::quantization::QuantizationType;
use zeppelin::query::{execute_query, QueryParams, QueryResponse};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{ConsistencyLevel, DistanceMetric, VectorEntry};
use zeppelin::wal::manifest::{Manifest, SegmentRef};
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

#[derive(Debug, Clone, Copy)]
enum SqFixtureLayout {
    Legacy,
    Colocated,
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

async fn manual_sq_fixture(label: &str, layout: SqFixtureLayout) -> BenchFixture {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.key(label);
    let segment_id = "seg_manual_sq";
    let vectors = bench_vectors();
    let query = vectors[0].values.clone();
    let centroids: Vec<Vec<f32>> = vectors.iter().map(|vector| vector.values.clone()).collect();
    let refs: Vec<&[f32]> = vectors
        .iter()
        .map(|vector| vector.values.as_slice())
        .collect();
    let calibration = SqCalibration::calibrate(&refs, DIM);
    let calibration_bytes = calibration.to_bytes();

    let centroids_bytes = match layout {
        SqFixtureLayout::Legacy => legacy_centroids_bytes(&centroids, DIM),
        SqFixtureLayout::Colocated => v2_centroids_bytes(&centroids, DIM, &calibration_bytes),
    };
    store
        .put(
            &format!("{namespace}/segments/{segment_id}/centroids.bin"),
            centroids_bytes,
        )
        .await
        .unwrap();

    if matches!(layout, SqFixtureLayout::Legacy) {
        store
            .put(
                &format!("{namespace}/segments/{segment_id}/sq_calibration.bin"),
                calibration_bytes.clone(),
            )
            .await
            .unwrap();
    }

    for (cluster_idx, vector) in vectors.iter().enumerate() {
        let ids = vec![vector.id.clone()];
        let full_vectors = vec![vector.values.clone()];
        let attrs = vec![vector.attributes.clone()];
        let cluster_refs = vec![vector.values.as_slice()];
        let codes = calibration.encode_batch(&cluster_refs);
        let full_cluster = legacy_cluster_bytes(&ids, &full_vectors, DIM);
        let sq_cluster = serialize_sq_cluster(&ids, &codes, DIM).unwrap();
        let cluster_bytes = match layout {
            SqFixtureLayout::Legacy => full_cluster,
            SqFixtureLayout::Colocated => v2_cluster_bytes(&sq_cluster, &full_cluster),
        };

        store
            .put(
                &format!("{namespace}/segments/{segment_id}/cluster_{cluster_idx}.bin"),
                cluster_bytes,
            )
            .await
            .unwrap();
        store
            .put(
                &format!("{namespace}/segments/{segment_id}/attrs_{cluster_idx}.bin"),
                Bytes::from(serde_json::to_vec(&attrs).unwrap()),
            )
            .await
            .unwrap();
        if matches!(layout, SqFixtureLayout::Legacy) {
            store
                .put(
                    &format!("{namespace}/segments/{segment_id}/sq_cluster_{cluster_idx}.bin"),
                    sq_cluster,
                )
                .await
                .unwrap();
        }
    }

    let mut manifest = Manifest::new();
    manifest.add_segment(SegmentRef {
        id: segment_id.to_string(),
        vector_count: vectors.len(),
        cluster_count: vectors.len(),
        quantization: QuantizationType::Scalar,
        hierarchical: false,
        bitmap_fields: Vec::new(),
        fts_fields: Vec::new(),
        has_global_fts: false,
        cluster_owners: Vec::new(),
    });
    manifest.write(&store, &namespace).await.unwrap();

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

fn legacy_centroids_bytes(centroids: &[Vec<f32>], dim: usize) -> Bytes {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(centroids.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(dim as u32).to_le_bytes());
    for centroid in centroids {
        for value in centroid {
            buf.extend_from_slice(&value.to_le_bytes());
        }
    }
    Bytes::from(buf)
}

fn v2_centroids_bytes(centroids: &[Vec<f32>], dim: usize, calibration: &[u8]) -> Bytes {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"ZCT2");
    buf.extend_from_slice(&(centroids.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(dim as u32).to_le_bytes());
    for centroid in centroids {
        for value in centroid {
            buf.extend_from_slice(&value.to_le_bytes());
        }
    }
    buf.extend_from_slice(&(calibration.len() as u64).to_le_bytes());
    buf.extend_from_slice(calibration);
    Bytes::from(buf)
}

fn legacy_cluster_bytes(ids: &[String], vectors: &[Vec<f32>], dim: usize) -> Bytes {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(dim as u32).to_le_bytes());
    for (id, vector) in ids.iter().zip(vectors) {
        let id_bytes = id.as_bytes();
        buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(id_bytes);
        for value in vector {
            buf.extend_from_slice(&value.to_le_bytes());
        }
    }
    Bytes::from(buf)
}

fn v2_cluster_bytes(sq_cluster: &[u8], full_cluster: &[u8]) -> Bytes {
    let sq_offset = 36u64;
    let sq_len = sq_cluster.len() as u64;
    let full_offset = sq_offset + sq_len;
    let full_len = full_cluster.len() as u64;
    let mut buf = Vec::new();
    buf.extend_from_slice(b"ZCL2");
    buf.extend_from_slice(&sq_offset.to_le_bytes());
    buf.extend_from_slice(&sq_len.to_le_bytes());
    buf.extend_from_slice(&full_offset.to_le_bytes());
    buf.extend_from_slice(&full_len.to_le_bytes());
    buf.extend_from_slice(sq_cluster);
    buf.extend_from_slice(full_cluster);
    Bytes::from(buf)
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

fn assert_same_results(left: &QueryResponse, right: &QueryResponse) {
    assert_eq!(left.results.len(), right.results.len());
    for (left, right) in left.results.iter().zip(&right.results) {
        assert_eq!(left.id, right.id);
        assert_eq!(left.score.to_bits(), right.score.to_bits());
        assert_eq!(left.attributes, right.attributes);
    }
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

    // H1 cold strong profile for the v2 SQ8 storage layout, with only the
    // manifest cache preloaded:
    // - manifest=1: Task 13 strong If-None-Match freshness GET.
    // - centroids=1: active segment IVF centroid blob, including SQ calibration.
    // - sq=0: no SQ calibration or per-cluster SQ sidecars for v2 segments.
    // - cluster=4: one co-located cluster blob per probed cluster; those same
    //   bytes are reused for SQ8 rerank.
    // - attrs=4: lazy final-result enrichment still needs all four attrs
    //   blobs in this fixture because top_k=4 returns one vector from each
    //   one-vector cluster. attrs_laziness_tests pins the reduced top_k=1
    //   profile where only the winning cluster's attrs are fetched.
    // - total=10: honest object GET count, not the thesis-level "2".
    assert_get_profile(
        &fixture.counter,
        ExpectedGets {
            manifest: 1,
            centroids: 1,
            sq: 0,
            cluster: 4,
            attrs: 4,
            wal: 0,
        },
    );

    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn cold_strong_profiles_pin_legacy_and_colocated_sq8_layouts() {
    let legacy = manual_sq_fixture("h1-legacy-sq8", SqFixtureLayout::Legacy).await;
    let colocated = manual_sq_fixture("h1-colocated-sq8", SqFixtureLayout::Colocated).await;

    let legacy_response = run_query(&legacy, ConsistencyLevel::Strong, ALL_CLUSTERS, None).await;
    let colocated_response =
        run_query(&colocated, ConsistencyLevel::Strong, ALL_CLUSTERS, None).await;

    assert_same_results(&legacy_response, &colocated_response);

    print_profile(
        "legacy cold strong vector query, SQ8, nprobe=4",
        &legacy.counter,
    );
    assert_get_profile(
        &legacy.counter,
        ExpectedGets {
            manifest: 1,
            centroids: 1,
            sq: 5,
            cluster: 4,
            attrs: 4,
            wal: 0,
        },
    );

    print_profile(
        "co-located cold strong vector query, SQ8, nprobe=4",
        &colocated.counter,
    );
    assert_get_profile(
        &colocated.counter,
        ExpectedGets {
            manifest: 1,
            centroids: 1,
            sq: 0,
            cluster: 4,
            attrs: 4,
            wal: 0,
        },
    );
    assert!(
        colocated.counter.total_gets() < legacy.counter.total_gets(),
        "co-located v2 layout must perform fewer cold GETs than legacy layout"
    );

    legacy.harness.cleanup().await;
    colocated.harness.cleanup().await;
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
            sq: 0,
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
            sq: 0,
            cluster: 4,
            attrs: 4,
            wal: 1,
        },
    );
    let strong_total = fixture.counter.total_gets();

    assert_eq!(eventual_total, 9);
    assert_eq!(strong_total, 11);
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
            sq: 0,
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
            sq: 0,
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
            sq: 0,
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

    assert_eq!(cold_total, 10);
    assert_eq!(warm_total, 1);
    assert!(
        warm_total < cold_total,
        "warm query must reduce S3 GETs by serving segment artifacts from cache"
    );

    fixture.harness.cleanup().await;
}
