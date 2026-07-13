//! Isolated executors for query storage shapes not covered by frozen contracts.
//!
//! Current-layout fixtures are built through the production namespace, WAL,
//! and compaction paths. The frozen legacy SQ shape publishes production-
//! readable immutable formats directly so randomized k-means cannot leak into
//! deterministic artifacts. Every measured operation uses the production HTTP
//! query path. Setup, validation, and cleanup remain outside the interval.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use zeppelin::config::{Config, IndexingConfig};
use zeppelin::fts::FtsFieldConfig;
use zeppelin::index::hierarchical::tree_meta_key;
use zeppelin::index::quantization::sq::{serialize_sq_cluster, SqCalibration};
use zeppelin::index::quantization::QuantizationType;
use zeppelin::wal::manifest::{SegmentRef, SketchRef};
use zeppelin::wal::Manifest;

use crate::common::counting::{perf_counting_store, ArtifactClass, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::common::server::{api_ns, start_test_server_full, FullTestServer};
use crate::perf_contract::depth::{DepthTracker, OpSpan, SpanKind};
use crate::perf_contract::scenario::RepeatCounters;

use super::artifacts::IdealSample;
use super::catalog::{IdealCase, IdealOperation, QueryCase};

const TOP_K: usize = 4;
const PQ_M: usize = 4;

/// Report whether this executor owns the catalog row.
#[must_use]
pub(crate) fn supports(case: &IdealCase) -> bool {
    CaseSpec::from_case(case).is_some()
}

/// Execute exactly one cold production query for an owned catalog row.
pub(crate) async fn execute(case: &IdealCase) -> Option<IdealSample> {
    let spec = CaseSpec::from_case(case)?;
    let harness = TestHarness::new().await;
    let (depth_wrapped, tracker) = crate::perf_contract::depth::depth_store(&harness.store);
    let (instrumented_store, counter) = perf_counting_store(&depth_wrapped);
    let server = start_test_server_full(
        instrumented_store,
        Some(harness.prefix.clone()),
        query_config(spec),
        false,
        None,
    )
    .await;
    let client = Client::new();
    let namespace = api_ns(&harness, "ideal-variant-query");
    let vectors = fixture_vectors(spec);

    create_namespace(&client, &server, &namespace, spec).await;
    if spec.layout == PersistedLayout::LegacyStandaloneSketch {
        install_deterministic_legacy_sq_fixture(&server, &namespace, spec, &vectors).await;
    } else {
        upsert_vectors(&client, &server, &namespace, &vectors).await;
        let compacted = if spec.fts {
            server
                .compactor
                .compact_with_fts(
                    &namespace,
                    None,
                    &HashMap::from([("content".to_string(), FtsFieldConfig::default())]),
                )
                .await
        } else {
            server.compactor.compact(&namespace).await
        }
        .unwrap_or_else(|error| panic!("{} setup compaction failed: {error}", case.id.as_str()));
        assert_eq!(
            compacted.vectors_compacted,
            spec.vector_count,
            "{} setup compacted the wrong row count",
            case.id.as_str()
        );
        assert!(
            compacted.segment_id.is_some(),
            "{} setup compaction did not publish a segment",
            case.id.as_str()
        );
    }

    prepare_and_validate_shape(&server, &namespace, spec, &vectors).await;
    server.manifest_cache.invalidate(&namespace);
    await_tracker_idle(&tracker).await;
    counter.reset();
    tracker.reset();

    let response = run_query(&client, &server, &namespace, spec).await;
    assert_query_payload(case, spec, &response);

    await_tracker_idle(&tracker).await;
    let repeat = snapshot_repeat(&counter, &tracker);
    let sample = IdealSample::from_repeat(case.id.as_str(), &repeat);
    assert_measured_shape(case, spec, &sample);

    server.shutdown().await;
    harness
        .store
        .delete_prefix(&format!("{namespace}/"))
        .await
        .unwrap_or_else(|error| panic!("{} cleanup failed: {error}", case.id.as_str()));
    harness.cleanup().await;
    Some(sample)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryKind {
    Ann { filtered: bool },
    Bm25,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistedLayout {
    Current,
    LegacyStandaloneSketch,
    LegacyNoSketch,
    LegacyPerClusterFts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CaseSpec {
    query_case: QueryCase,
    dimensions: usize,
    vector_count: usize,
    nlist: usize,
    quantization: QuantizationType,
    hierarchical: bool,
    leaf_size: usize,
    bitmap: bool,
    fts: bool,
    query: QueryKind,
    layout: PersistedLayout,
}

impl CaseSpec {
    fn from_case(case: &IdealCase) -> Option<Self> {
        let IdealOperation::Query(query_case) = case.operation else {
            return None;
        };
        Some(match query_case {
            QueryCase::FlatNoneFilteredNoBitmap => Self::flat(
                query_case,
                QuantizationType::None,
                false,
                QueryKind::Ann { filtered: true },
                PersistedLayout::Current,
            ),
            QueryCase::FlatPqUnfilteredCurrent => Self::flat(
                query_case,
                QuantizationType::Product,
                false,
                QueryKind::Ann { filtered: false },
                PersistedLayout::Current,
            ),
            QueryCase::FlatPqFilteredBitmap => Self::flat(
                query_case,
                QuantizationType::Product,
                true,
                QueryKind::Ann { filtered: true },
                PersistedLayout::Current,
            ),
            QueryCase::HierarchicalNoneShallowUnfiltered => Self {
                query_case,
                dimensions: 16,
                vector_count: 8,
                nlist: 2,
                quantization: QuantizationType::None,
                hierarchical: true,
                leaf_size: 100,
                bitmap: false,
                fts: false,
                query: QueryKind::Ann { filtered: false },
                layout: PersistedLayout::Current,
            },
            QueryCase::HierarchicalSqDeepFilteredNoBitmap => Self::hierarchical(
                query_case,
                QuantizationType::Scalar,
                false,
                QueryKind::Ann { filtered: true },
            ),
            QueryCase::HierarchicalPqDeepFilteredBitmap => Self::hierarchical(
                query_case,
                QuantizationType::Product,
                true,
                QueryKind::Ann { filtered: true },
            ),
            QueryCase::FlatLegacySqStandaloneSketch => Self::flat(
                query_case,
                QuantizationType::Scalar,
                false,
                QueryKind::Ann { filtered: false },
                PersistedLayout::LegacyStandaloneSketch,
            ),
            QueryCase::FlatLegacyNoneNoSketch => Self::flat(
                query_case,
                QuantizationType::None,
                false,
                QueryKind::Ann { filtered: false },
                PersistedLayout::LegacyNoSketch,
            ),
            QueryCase::FtsGlobalCold => Self::fts(query_case, PersistedLayout::Current, 2),
            QueryCase::FtsPerClusterFallback => {
                Self::fts(query_case, PersistedLayout::LegacyPerClusterFts, 1)
            }
            _ => return None,
        })
    }

    fn flat(
        query_case: QueryCase,
        quantization: QuantizationType,
        bitmap: bool,
        query: QueryKind,
        layout: PersistedLayout,
    ) -> Self {
        Self {
            query_case,
            dimensions: 16,
            vector_count: 32,
            nlist: 4,
            quantization,
            hierarchical: false,
            leaf_size: 100,
            bitmap,
            fts: false,
            query,
            layout,
        }
    }

    fn hierarchical(
        query_case: QueryCase,
        quantization: QuantizationType,
        bitmap: bool,
        query: QueryKind,
    ) -> Self {
        Self {
            query_case,
            dimensions: 16,
            vector_count: 64,
            nlist: 4,
            quantization,
            hierarchical: true,
            leaf_size: 4,
            bitmap,
            fts: false,
            query,
            layout: PersistedLayout::Current,
        }
    }

    fn fts(query_case: QueryCase, layout: PersistedLayout, nlist: usize) -> Self {
        Self {
            query_case,
            dimensions: 16,
            vector_count: 16,
            nlist,
            quantization: QuantizationType::None,
            hierarchical: false,
            leaf_size: 100,
            bitmap: false,
            fts: true,
            query: QueryKind::Bm25,
            layout,
        }
    }
}

fn query_config(spec: CaseSpec) -> Config {
    let mut config = Config::load(None).expect("failed to load variant-query config");
    config.cache.namespace_registry_ttl_ms = 3_600_000;
    config.cache.manifest_cache_ttl_ms = 3_600_000;
    config.cache.hydration_enabled = false;
    config.compaction.max_wal_fragments_before_compact = usize::MAX;
    config.indexing = IndexingConfig {
        default_num_centroids: spec.nlist,
        default_nprobe: spec.nlist,
        max_nprobe: 64,
        kmeans_max_iterations: 10,
        quantization: spec.quantization,
        pq_m: PQ_M,
        hierarchical: spec.hierarchical,
        leaf_size: Some(spec.leaf_size),
        bitmap_index: spec.bitmap,
        fts_index: spec.fts,
        bm25_max_full_scan_clusters: 64,
        bm25_max_full_scan_vectors: 10_000,
        ..config.indexing.clone()
    };
    config
}

async fn create_namespace(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    spec: CaseSpec,
) {
    let full_text_search = if spec.fts {
        json!({ "content": {} })
    } else {
        json!({})
    };
    let response = client
        .post(format!("{}/v1/namespaces", server.base_url))
        .json(&json!({
            "name": namespace,
            "dimensions": spec.dimensions,
            "distance_metric": "euclidean",
            "full_text_search": full_text_search,
            "index_config": {
                "nlist": spec.nlist,
                "quantization": spec.quantization,
                "pq_m": PQ_M,
                "hierarchical": spec.hierarchical,
                "fts_index": spec.fts,
                "bitmap_index": spec.bitmap
            }
        }))
        .send()
        .await
        .expect("variant-query namespace create request failed");
    assert_response_status(
        response,
        StatusCode::CREATED,
        "variant-query namespace create",
    )
    .await;
}

async fn upsert_vectors(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    vectors: &[Value],
) {
    let response = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({ "vectors": vectors }))
        .send()
        .await
        .expect("variant-query upsert request failed");
    assert_response_status(response, StatusCode::OK, "variant-query upsert").await;
}

fn fixture_vectors(spec: CaseSpec) -> Vec<Value> {
    (0..spec.vector_count)
        .map(|index| {
            let magnitude = index as f32;
            let values = (0..spec.dimensions)
                .map(|dimension| magnitude * (1.0 + dimension as f32 / 100.0))
                .collect::<Vec<_>>();
            json!({
                "id": format!("variant-{index:03}"),
                "values": values,
                "attributes": {
                    "tenant": if index % 2 == 0 { "keep" } else { "drop" },
                    "content": if index == 0 { "needle needle" } else { "haystack" },
                    "ordinal": index
                }
            })
        })
        .collect()
}

/// Publish a deterministic, production-readable legacy SQ segment.
///
/// This fixture deliberately avoids the production k-means builder: centroid
/// initialization is allowed to choose different but valid partitions across
/// processes, which makes exact range lengths unsuitable for a deterministic
/// analyzer baseline. The persisted objects use Zeppelin's supported legacy
/// formats and are consumed by the normal HTTP query path.
async fn install_deterministic_legacy_sq_fixture(
    server: &FullTestServer,
    namespace: &str,
    spec: CaseSpec,
    vectors: &[Value],
) {
    const SEGMENT_ID: &str = "seg_ideal_legacy_sq";
    assert_eq!(spec.quantization, QuantizationType::Scalar);
    assert_eq!(spec.vector_count, vectors.len());
    assert_eq!(spec.vector_count % spec.nlist, 0);

    let values = vectors
        .iter()
        .map(|vector| vector_values(vector, spec.dimensions))
        .collect::<Vec<_>>();
    let value_refs = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let calibration = SqCalibration::calibrate(&value_refs, spec.dimensions);
    let calibration_bytes = calibration.to_bytes();
    let rows_per_cluster = spec.vector_count / spec.nlist;
    let cluster_counts = vec![rows_per_cluster; spec.nlist];
    let centroids = values
        .chunks(rows_per_cluster)
        .map(|cluster| mean_vector(cluster, spec.dimensions))
        .collect::<Vec<_>>();

    server
        .store
        .put(
            &format!("{namespace}/segments/{SEGMENT_ID}/centroids.bin"),
            legacy_centroids_bytes(&centroids, spec.dimensions),
        )
        .await
        .expect("deterministic legacy SQ centroids publication failed");
    server
        .store
        .put(
            &format!("{namespace}/segments/{SEGMENT_ID}/sq_calibration.bin"),
            calibration_bytes,
        )
        .await
        .expect("deterministic legacy SQ calibration publication failed");

    for cluster_idx in 0..spec.nlist {
        let start = cluster_idx * rows_per_cluster;
        let end = start + rows_per_cluster;
        let cluster_vectors = &vectors[start..end];
        let cluster_values = &values[start..end];
        let ids = cluster_vectors
            .iter()
            .map(|vector| {
                vector["id"]
                    .as_str()
                    .expect("deterministic legacy SQ vector omitted id")
                    .to_string()
            })
            .collect::<Vec<_>>();
        let refs = cluster_values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let codes = calibration.encode_batch(&refs);
        let attrs = cluster_vectors
            .iter()
            .map(|vector| vector["attributes"].clone())
            .collect::<Vec<_>>();

        server
            .store
            .put(
                &format!("{namespace}/segments/{SEGMENT_ID}/cluster_{cluster_idx}.bin"),
                legacy_cluster_bytes(cluster_vectors, spec.dimensions),
            )
            .await
            .expect("deterministic legacy SQ cluster publication failed");
        server
            .store
            .put(
                &format!("{namespace}/segments/{SEGMENT_ID}/attrs_{cluster_idx}.bin"),
                Bytes::from(
                    serde_json::to_vec(&attrs)
                        .expect("deterministic legacy SQ attrs serialization failed"),
                ),
            )
            .await
            .expect("deterministic legacy SQ attrs publication failed");
        server
            .store
            .put(
                &format!("{namespace}/segments/{SEGMENT_ID}/sq_cluster_{cluster_idx}.bin"),
                serialize_sq_cluster(&ids, &codes, spec.dimensions)
                    .expect("deterministic legacy SQ code serialization failed"),
            )
            .await
            .expect("deterministic legacy SQ sidecar publication failed");
    }

    let sketch_key = format!("{namespace}/segments/{SEGMENT_ID}/coarse_sketch.bin");
    let sketch_bytes = legacy_v3_sketch_bytes(spec.dimensions, &cluster_counts, &values);
    let sketch_ref = SketchRef {
        key: sketch_key.clone(),
        version: 3,
        code_dims: spec.dimensions,
        bytes_per_vector: spec.dimensions,
        size_bytes: sketch_bytes.len() as u64,
        rotation_seed: None,
    };
    server
        .store
        .put(&sketch_key, sketch_bytes)
        .await
        .expect("deterministic standalone sketch publication failed");

    let mut manifest = Manifest::read(&server.store, namespace)
        .await
        .expect("deterministic legacy SQ manifest read failed")
        .expect("deterministic legacy SQ initial manifest missing");
    assert!(manifest.segments.is_empty());
    assert!(manifest.uncompacted_fragments().is_empty());
    manifest.add_segment(SegmentRef {
        id: SEGMENT_ID.to_string(),
        vector_count: spec.vector_count,
        cluster_count: spec.nlist,
        quantization: QuantizationType::Scalar,
        hierarchical: false,
        bitmap_fields: Vec::new(),
        fts_fields: Vec::new(),
        has_global_fts: false,
        cluster_owners: Vec::new(),
        sketch: Some(sketch_ref),
        cluster_objects: Vec::new(),
        bootstrap: None,
        membership: None,
    });
    manifest
        .write(&server.store, namespace)
        .await
        .expect("deterministic legacy SQ manifest publication failed");
}

fn vector_values(vector: &Value, dimensions: usize) -> Vec<f32> {
    let values = vector["values"]
        .as_array()
        .expect("deterministic fixture vector omitted values");
    assert_eq!(values.len(), dimensions);
    values
        .iter()
        .map(|value| {
            value
                .as_f64()
                .expect("deterministic fixture vector value was not numeric") as f32
        })
        .collect()
}

fn mean_vector(cluster: &[Vec<f32>], dimensions: usize) -> Vec<f32> {
    assert!(!cluster.is_empty());
    let mut mean = vec![0.0_f32; dimensions];
    for vector in cluster {
        assert_eq!(vector.len(), dimensions);
        for (total, value) in mean.iter_mut().zip(vector) {
            *total += *value;
        }
    }
    let count = cluster.len() as f32;
    for value in &mut mean {
        *value /= count;
    }
    mean
}

fn legacy_centroids_bytes(centroids: &[Vec<f32>], dimensions: usize) -> Bytes {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &u32::try_from(centroids.len())
            .expect("legacy SQ centroid count exceeds u32")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(dimensions)
            .expect("legacy SQ dimensions exceed u32")
            .to_le_bytes(),
    );
    for centroid in centroids {
        assert_eq!(centroid.len(), dimensions);
        for value in centroid {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    Bytes::from(bytes)
}

/// Build a meaningful frozen ZSK1 v3 sketch with one scalar subquantizer per
/// vector dimension. Each fixture row owns one codeword, so the standalone
/// sketch performs real legacy ADC work without randomized training.
fn legacy_v3_sketch_bytes(
    dimensions: usize,
    cluster_counts: &[usize],
    values: &[Vec<f32>],
) -> Bytes {
    const VERSION: u32 = 3;
    const CODEBOOK_SIZE: usize = 256;
    assert!(!cluster_counts.is_empty());
    assert_eq!(cluster_counts.iter().sum::<usize>(), values.len());
    assert!(values.len() <= CODEBOOK_SIZE);

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"ZSK1");
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(
        &u32::try_from(dimensions)
            .expect("legacy sketch dimensions exceed u32")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(dimensions)
            .expect("legacy sketch subquantizer count exceeds u32")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(cluster_counts.len())
            .expect("legacy sketch cluster count exceeds u32")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(
        &u64::try_from(values.len())
            .expect("legacy sketch vector count exceeds u64")
            .to_le_bytes(),
    );

    for dimension in 0..dimensions {
        for code in 0..CODEBOOK_SIZE {
            let value = values.get(code).map_or(0.0, |vector| vector[dimension]);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }

    let mut attr_bits = vec![0_u8; cluster_counts.len().div_ceil(8)];
    for cluster_idx in 0..cluster_counts.len() {
        attr_bits[cluster_idx / 8] |= 1 << (cluster_idx % 8);
    }
    bytes.extend_from_slice(&attr_bits);
    for &count in cluster_counts {
        bytes.extend_from_slice(
            &u32::try_from(count)
                .expect("legacy sketch per-cluster count exceeds u32")
                .to_le_bytes(),
        );
    }
    for row in 0..values.len() {
        let code = u8::try_from(row).expect("legacy sketch row code exceeds u8");
        bytes.extend(std::iter::repeat_n(code, dimensions));
    }
    Bytes::from(bytes)
}

async fn prepare_and_validate_shape(
    server: &FullTestServer,
    namespace: &str,
    spec: CaseSpec,
    vectors: &[Value],
) {
    let mut manifest = Manifest::read(&server.store, namespace)
        .await
        .expect("variant-query manifest setup read failed")
        .expect("variant-query manifest missing after compaction");
    assert!(manifest.uncompacted_fragments().is_empty());
    assert_eq!(manifest.segments.len(), 1);
    let active_id = manifest
        .active_segment
        .clone()
        .expect("variant-query manifest has no active segment");
    let segment = manifest
        .segments
        .iter_mut()
        .find(|segment| segment.id == active_id)
        .expect("variant-query active segment descriptor missing");
    assert_eq!(segment.vector_count, spec.vector_count);
    assert_eq!(segment.quantization, spec.quantization);
    assert_eq!(segment.hierarchical, spec.hierarchical);
    assert_eq!(!segment.bitmap_fields.is_empty(), spec.bitmap);

    if spec.hierarchical {
        let bytes = server
            .store
            .get(&tree_meta_key(namespace, &active_id))
            .await
            .expect("variant-query tree metadata setup read failed");
        let tree: Value =
            serde_json::from_slice(&bytes).expect("variant-query tree metadata was not valid JSON");
        let levels = tree["num_levels"]
            .as_u64()
            .expect("variant-query tree metadata omitted num_levels");
        if spec.query_case == QueryCase::HierarchicalNoneShallowUnfiltered {
            assert_eq!(levels, 1, "shallow hierarchy fixture grew extra levels");
        } else {
            assert!(levels > 1, "deep hierarchy fixture remained shallow");
        }
    }

    match spec.layout {
        PersistedLayout::Current => {
            if !spec.hierarchical {
                assert!(
                    segment.bootstrap.is_some(),
                    "current flat fixture omitted bootstrap"
                );
                assert!(
                    !segment.cluster_objects.is_empty(),
                    "current flat fixture omitted grouped cluster objects"
                );
            }
            if spec.fts {
                assert!(
                    segment.has_global_fts,
                    "global FTS fixture was not published"
                );
                assert_eq!(segment.fts_fields, ["content"]);
            }
        }
        PersistedLayout::LegacyStandaloneSketch => {
            assert!(
                segment.sketch.is_some(),
                "standalone-sketch fixture has no sketch"
            );
            segment.bootstrap = None;
        }
        PersistedLayout::LegacyNoSketch => {
            segment.bootstrap = None;
            segment.sketch = None;
        }
        PersistedLayout::LegacyPerClusterFts => {
            assert_eq!(segment.cluster_count, 1);
            assert_eq!(segment.fts_fields, ["content"]);
            let cluster_key = format!("{namespace}/segments/{active_id}/cluster_0.bin");
            server
                .store
                .put(&cluster_key, legacy_cluster_bytes(vectors, spec.dimensions))
                .await
                .expect("legacy per-cluster FTS cluster publication failed");
            segment.has_global_fts = false;
            segment.cluster_objects.clear();
            segment.bootstrap = None;
            segment.sketch = None;
            segment.membership = None;
        }
    }

    if spec.layout != PersistedLayout::Current {
        manifest
            .write(&server.store, namespace)
            .await
            .expect("variant-query legacy manifest publication failed");
        let published = Manifest::read(&server.store, namespace)
            .await
            .expect("variant-query legacy manifest verification failed")
            .expect("variant-query legacy manifest disappeared");
        let published = published
            .segments
            .iter()
            .find(|segment| segment.id == active_id)
            .expect("variant-query legacy active segment disappeared");
        match spec.layout {
            PersistedLayout::LegacyStandaloneSketch => {
                assert!(published.bootstrap.is_none());
                assert!(published.sketch.is_some());
            }
            PersistedLayout::LegacyNoSketch => {
                assert!(published.bootstrap.is_none());
                assert!(published.sketch.is_none());
            }
            PersistedLayout::LegacyPerClusterFts => {
                assert!(!published.has_global_fts);
                assert!(published.cluster_objects.is_empty());
            }
            PersistedLayout::Current => unreachable!(),
        }
    }
}

fn legacy_cluster_bytes(vectors: &[Value], dimensions: usize) -> Bytes {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &u32::try_from(vectors.len())
            .expect("legacy FTS fixture row count exceeds u32")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(dimensions)
            .expect("legacy FTS fixture dimensions exceed u32")
            .to_le_bytes(),
    );
    for vector in vectors {
        let id = vector["id"]
            .as_str()
            .expect("legacy FTS fixture vector omitted id");
        bytes.extend_from_slice(
            &u32::try_from(id.len())
                .expect("legacy FTS fixture id length exceeds u32")
                .to_le_bytes(),
        );
        bytes.extend_from_slice(id.as_bytes());
        let values = vector["values"]
            .as_array()
            .expect("legacy FTS fixture vector omitted values");
        assert_eq!(values.len(), dimensions);
        for value in values {
            let value = value
                .as_f64()
                .expect("legacy FTS fixture value was not numeric") as f32;
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    Bytes::from(bytes)
}

async fn run_query(
    client: &Client,
    server: &FullTestServer,
    namespace: &str,
    spec: CaseSpec,
) -> Value {
    let body = match spec.query {
        QueryKind::Ann { filtered } => {
            let mut body = json!({
                "vector": vec![0.0_f32; spec.dimensions],
                "top_k": TOP_K,
                "nprobe": spec.nlist,
                "consistency": "eventual",
                "include_attributes": false
            });
            if filtered {
                body["filter"] = json!({
                    "op": "eq",
                    "field": "tenant",
                    "value": "keep"
                });
            }
            body
        }
        QueryKind::Bm25 => json!({
            "rank_by": ["content", "BM25", "needle"],
            "top_k": TOP_K,
            "consistency": "eventual",
            "include_attributes": false
        }),
    };
    let response = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&body)
        .send()
        .await
        .expect("variant-query measured request failed");
    assert_response_status(response, StatusCode::OK, "variant-query measured query").await
}

async fn assert_response_status(
    response: reqwest::Response,
    expected: StatusCode,
    label: &str,
) -> Value {
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("{label} response read failed: {error}"));
    assert_eq!(
        status,
        expected,
        "{label} returned an unexpected response: {}",
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("{label} returned invalid JSON: {error}"))
    }
}

fn assert_query_payload(case: &IdealCase, spec: CaseSpec, response: &Value) {
    let expected: &[&str] = match spec.query {
        QueryKind::Ann { filtered: false } => {
            &["variant-000", "variant-001", "variant-002", "variant-003"]
        }
        QueryKind::Ann { filtered: true } => {
            &["variant-000", "variant-002", "variant-004", "variant-006"]
        }
        QueryKind::Bm25 => &["variant-000"],
    };
    let results = response["results"]
        .as_array()
        .unwrap_or_else(|| panic!("{} results were not an array: {response}", case.id.as_str()));
    let ids = results
        .iter()
        .map(|result| {
            result["id"]
                .as_str()
                .unwrap_or_else(|| panic!("{} result omitted id: {result}", case.id.as_str()))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        expected,
        "{} returned the wrong deterministic payload: {response}",
        case.id.as_str()
    );
    assert_eq!(response["scanned_fragments"].as_u64(), Some(0));
    assert_eq!(response["scanned_segments"].as_u64(), Some(1));
    for result in results {
        assert!(
            result["attributes"].is_null(),
            "{} returned unrequested attributes: {result}",
            case.id.as_str()
        );
    }
}

fn assert_measured_shape(case: &IdealCase, spec: CaseSpec, sample: &IdealSample) {
    assert!(
        sample.total_get_ops > 0,
        "{} recorded no GETs",
        case.id.as_str()
    );
    assert!(
        sample
            .physical_operations
            .iter()
            .all(|operation| operation.verb != "put"
                && operation.verb != "delete"
                && operation.verb != "copy"
                && operation.verb != "list"),
        "{} leaked setup, cleanup, or maintenance into measurement: {:?}",
        case.id.as_str(),
        sample.physical_operations
    );

    match spec.query_case {
        QueryCase::FlatNoneFilteredNoBitmap => {
            assert_get(sample, ArtifactClass::Bootstrap);
            assert_get(sample, ArtifactClass::Attrs);
            assert_no_get(sample, ArtifactClass::Bitmap);
            assert_no_get(sample, ArtifactClass::Sq);
        }
        QueryCase::FlatPqUnfilteredCurrent => {
            assert_get(sample, ArtifactClass::Bootstrap);
            assert_get(sample, ArtifactClass::Sq);
            assert_get(sample, ArtifactClass::Cluster);
            assert_no_get(sample, ArtifactClass::Bitmap);
        }
        QueryCase::FlatPqFilteredBitmap => {
            assert_get(sample, ArtifactClass::Bootstrap);
            assert_get(sample, ArtifactClass::Sq);
            assert_get(sample, ArtifactClass::Bitmap);
            assert_get(sample, ArtifactClass::Cluster);
        }
        QueryCase::HierarchicalNoneShallowUnfiltered => {
            assert_hierarchy_gets(sample);
            assert_get(sample, ArtifactClass::Cluster);
            assert_no_get(sample, ArtifactClass::Sq);
        }
        QueryCase::HierarchicalSqDeepFilteredNoBitmap => {
            assert_hierarchy_gets(sample);
            // Current hierarchical SQ stores coarse codes beside exact rows in
            // cluster_<i>.bin; a separate `sq_*` GET would indicate the old
            // sidecar compatibility path rather than this current layout.
            assert_get(sample, ArtifactClass::Cluster);
            assert_no_get(sample, ArtifactClass::Sq);
            assert_no_get(sample, ArtifactClass::Bitmap);
        }
        QueryCase::HierarchicalPqDeepFilteredBitmap => {
            assert_hierarchy_gets(sample);
            assert_get(sample, ArtifactClass::Sq);
            assert_get(sample, ArtifactClass::Bitmap);
            assert_get(sample, ArtifactClass::Cluster);
        }
        QueryCase::FlatLegacySqStandaloneSketch => {
            assert_get(sample, ArtifactClass::Centroids);
            assert_get(sample, ArtifactClass::Sketch);
            assert_get(sample, ArtifactClass::Sq);
            assert_get(sample, ArtifactClass::Cluster);
            assert_no_get(sample, ArtifactClass::Bootstrap);
        }
        QueryCase::FlatLegacyNoneNoSketch => {
            assert_get(sample, ArtifactClass::Centroids);
            assert_get(sample, ArtifactClass::Cluster);
            assert_no_get(sample, ArtifactClass::Bootstrap);
            assert_no_get(sample, ArtifactClass::Sketch);
        }
        QueryCase::FtsGlobalCold => {
            assert_get_key(sample, "global_fts.bin");
            assert_get(sample, ArtifactClass::Cluster);
        }
        QueryCase::FtsPerClusterFallback => {
            assert_get_key(sample, "fts_index_<index>.bin");
            assert_get_key(sample, "cluster_<index>.bin");
            assert_get(sample, ArtifactClass::Centroids);
            assert!(
                !has_get_key(sample, "global_fts.bin"),
                "per-cluster FTS fallback read the global index"
            );
        }
        _ => unreachable!("unsupported query case reached variant executor"),
    }
}

fn get_count(sample: &IdealSample, class: ArtifactClass) -> usize {
    sample
        .physical_operations
        .iter()
        .filter(|operation| operation.verb == "get" && operation.class == class)
        .count()
}

fn assert_get(sample: &IdealSample, class: ArtifactClass) {
    assert!(
        get_count(sample, class) > 0,
        "{} did not execute expected {class} GET branch",
        sample.scenario_id
    );
}

fn assert_no_get(sample: &IdealSample, class: ArtifactClass) {
    assert_eq!(
        get_count(sample, class),
        0,
        "{} unexpectedly executed {class} GET branch",
        sample.scenario_id
    );
}

fn has_get_key(sample: &IdealSample, key: &str) -> bool {
    sample
        .physical_operations
        .iter()
        .any(|operation| operation.verb == "get" && operation.key == key)
}

fn assert_get_key(sample: &IdealSample, key: &str) {
    assert!(
        has_get_key(sample, key),
        "{} did not GET expected key pattern {key}: {:?}",
        sample.scenario_id,
        sample.physical_operations
    );
}

fn assert_hierarchy_gets(sample: &IdealSample) {
    assert_get_key(sample, "tree_meta.json");
    assert!(
        sample
            .physical_operations
            .iter()
            .any(|operation| operation.verb == "get" && operation.key.starts_with("node_")),
        "{} did not traverse a hierarchical node",
        sample.scenario_id
    );
}

async fn await_tracker_idle(tracker: &DepthTracker) {
    const MAX_YIELDS: usize = 4096;
    const REQUIRED_ZERO_STREAK: usize = 8;
    let mut zero_streak = 0;
    for _ in 0..MAX_YIELDS {
        tokio::task::yield_now().await;
        if tracker.active_operations() == 0 {
            zero_streak += 1;
            if zero_streak == REQUIRED_ZERO_STREAK {
                return;
            }
        } else {
            zero_streak = 0;
        }
    }
    panic!(
        "variant-query measurement did not quiesce: active_operations={}",
        tracker.active_operations()
    );
}

fn snapshot_repeat(counter: &GetCounter, tracker: &DepthTracker) -> RepeatCounters {
    let cutoff_us = tracker.elapsed_us();
    let classes = counter
        .class_breakdown()
        .into_iter()
        .map(|(class, stats)| (class.name().to_string(), stats))
        .collect::<BTreeMap<_, _>>();
    let totals = classes
        .values()
        .copied()
        .fold(ClassStats::default(), add_stats);
    let spans = tracker.take_spans();
    let raw_get_path = DepthTracker::critical_path(&spans, &[SpanKind::Get, SpanKind::Head], None);
    let raw_put_get_path = DepthTracker::critical_path(
        &spans,
        &[SpanKind::Get, SpanKind::Head, SpanKind::Put],
        None,
    );
    RepeatCounters {
        classes,
        totals,
        get_path: raw_get_path.clone(),
        put_get_path: raw_put_get_path.clone(),
        op_counts: operation_counts(&spans),
        spans,
        labeled: Vec::new(),
        wall_elapsed_us: 0,
        response_cutoff_us: cutoff_us,
        raw_get_path,
        raw_put_get_path,
    }
}

fn add_stats(mut total: ClassStats, class: ClassStats) -> ClassStats {
    total.get_ops = total
        .get_ops
        .checked_add(class.get_ops)
        .expect("GET ops overflow");
    total.get_bytes = total
        .get_bytes
        .checked_add(class.get_bytes)
        .expect("GET bytes overflow");
    total.put_ops = total
        .put_ops
        .checked_add(class.put_ops)
        .expect("PUT ops overflow");
    total.put_bytes = total
        .put_bytes
        .checked_add(class.put_bytes)
        .expect("PUT bytes overflow");
    total
}

fn operation_counts(spans: &[OpSpan]) -> BTreeMap<String, u64> {
    [
        ("head", SpanKind::Head),
        ("list", SpanKind::List),
        ("copy", SpanKind::Copy),
        ("delete", SpanKind::Delete),
    ]
    .into_iter()
    .map(|(name, kind)| {
        (
            name.to_string(),
            u64::try_from(spans.iter().filter(|span| span.kind == kind).count())
                .expect("operation count does not fit u64"),
        )
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::super::catalog;
    use super::*;

    #[test]
    fn executor_owns_exactly_the_ten_query_storage_variants() {
        let owned = catalog::all()
            .iter()
            .filter(|case| supports(case))
            .map(|case| case.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            owned,
            [
                "query.flat_none_filtered_no_bitmap",
                "query.flat_pq_unfiltered_current",
                "query.flat_pq_filtered_bitmap",
                "query.hierarchical_none_shallow_unfiltered",
                "query.hierarchical_sq_deep_filtered_no_bitmap",
                "query.hierarchical_pq_deep_filtered_bitmap",
                "query.flat_legacy_sq_standalone_sketch",
                "query.flat_legacy_none_no_sketch",
                "query.fts_global_cold",
                "query.fts_per_cluster_fallback",
            ]
        );
    }

    #[tokio::test]
    #[ignore = "real MinIO smoke for all ten isolated query storage variants"]
    async fn all_ten_query_storage_variants_complete_against_minio() {
        assert_eq!(
            std::env::var("TEST_BACKEND").as_deref(),
            Ok("minio"),
            "run this ignored storage smoke with TEST_BACKEND=minio"
        );
        let owned = catalog::all()
            .iter()
            .filter(|case| supports(case))
            .collect::<Vec<_>>();
        assert_eq!(owned.len(), 10);
        for case in owned {
            let sample = execute(case)
                .await
                .unwrap_or_else(|| panic!("variant executor rejected {}", case.id.as_str()));
            assert_eq!(sample.scenario_id, case.id.as_str());
            assert!(sample.total_get_ops > 0);
            assert!(!sample.serial_get_chain.links.is_empty());
        }
    }

    #[test]
    fn legacy_fts_cluster_encoding_preserves_ids_and_vectors() {
        let spec = CaseSpec::fts(
            QueryCase::FtsPerClusterFallback,
            PersistedLayout::LegacyPerClusterFts,
            1,
        );
        let vectors = fixture_vectors(spec);
        let bytes = legacy_cluster_bytes(&vectors, spec.dimensions);
        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 16);
        assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 16);
        assert!(bytes
            .windows("variant-000".len())
            .any(|window| window == b"variant-000"));
    }

    #[test]
    fn variant_specs_pin_shallow_and_deep_hierarchy_states() {
        let shallow = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "query.hierarchical_none_shallow_unfiltered")
            .and_then(CaseSpec::from_case)
            .unwrap();
        let deep = catalog::all()
            .iter()
            .find(|case| case.id.as_str() == "query.hierarchical_sq_deep_filtered_no_bitmap")
            .and_then(CaseSpec::from_case)
            .unwrap();
        assert!(shallow.vector_count <= shallow.leaf_size);
        assert!(deep.vector_count > deep.leaf_size);
        assert!(shallow.hierarchical && deep.hierarchical);
    }
}
