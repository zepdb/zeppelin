mod common;

use std::collections::HashMap;
use std::ops::Range;

use bytes::Bytes;
use common::counting::{counting_store, GetCounter};
use common::harness::TestHarness;
use common::vectors::random_vectors;
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig, DEFAULT_RERANK_COALESCE_GAP_BYTES};
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, VectorEntry};
use zeppelin::wal::manifest::{ClusterDataObjectRef, CoarsePayloadEncoding, Manifest, SegmentRef};
use zeppelin::wal::{WalReader, WalWriter};

const ROWS: usize = 20_000;
const DIM: usize = 256;
const GROUP_SIZE: usize = 10;
const CLUSTERS: usize = 4;
const QUERY_GROUP: usize = 1_240;
const GROUP_OBJECT_HEADER_LEN: usize = 8;
const GROUP_OBJECT_ENTRY_LEN: usize = 4 + 8 * 4;
const SKETCH_V4_HEADER_LEN: usize = 44;

struct ScanFixture {
    harness: TestHarness,
    store: ZeppelinStore,
    counter: GetCounter,
    namespace: String,
    query: Vec<f32>,
}

#[derive(Clone)]
struct RangedObjectEvidence {
    key: String,
    coarse_span: Range<usize>,
    first_full_offset: usize,
    object_size: usize,
}

struct SketchRows {
    code_dims: usize,
    row_bytes: usize,
    cluster_offsets: Vec<Range<usize>>,
    codes: Bytes,
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

fn synthetic_vectors() -> Vec<VectorEntry> {
    let anchors = random_vectors(ROWS / GROUP_SIZE, DIM);
    let mut vectors = Vec::with_capacity(ROWS);
    for (group, anchor) in anchors.into_iter().enumerate() {
        for member in 0..GROUP_SIZE {
            let mut values = anchor.values.clone();
            if member > 0 {
                values[member - 1] += member as f32 * 0.000_1;
            }
            let fragment = usize::from(group >= ROWS / GROUP_SIZE / 2);
            let mut attributes = HashMap::new();
            attributes.insert(
                "quarter".to_string(),
                AttributeValue::Integer((group % 4) as i64),
            );
            attributes.insert(
                "sparse".to_string(),
                AttributeValue::Integer((group % 31) as i64),
            );
            vectors.push(VectorEntry {
                id: format!("f{fragment}-g{group:04}-r{member:02}"),
                values,
                attributes: Some(attributes),
            });
        }
    }
    vectors
}

async fn scan_fixture(name: &str) -> ScanFixture {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let namespace = harness.artifact_origin_namespace(name);
    common::seed_active_namespace(&store, &namespace, DIM, DistanceMetric::Euclidean).await;

    let vectors = synthetic_vectors();
    let query = vectors[QUERY_GROUP * GROUP_SIZE].values.clone();
    let midpoint = vectors.len() / 2;
    let writer = WalWriter::new(store.clone());
    writer
        .append(&namespace, vectors[..midpoint].to_vec(), vec![])
        .await
        .unwrap();
    writer
        .append(&namespace, vectors[midpoint..].to_vec(), vec![])
        .await
        .unwrap();
    test_compactor(&store).compact(&namespace).await.unwrap();

    ScanFixture {
        harness,
        store,
        counter,
        namespace,
        query,
    }
}

fn active_segment(manifest: &Manifest) -> &SegmentRef {
    let active_id = manifest
        .active_segment
        .as_deref()
        .expect("fixture manifest must have an active segment");
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == active_id)
        .expect("active segment descriptor must exist")
}

fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
}

fn read_u64(data: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
}

fn parse_group_full_ranges(data: &[u8]) -> HashMap<usize, Range<usize>> {
    assert_eq!(&data[..4], b"ZBP\x04");
    let entry_count = read_u32(data, 4) as usize;
    let mut ranges = HashMap::with_capacity(entry_count);
    for entry in 0..entry_count {
        let offset = GROUP_OBJECT_HEADER_LEN + entry * GROUP_OBJECT_ENTRY_LEN;
        let cluster_idx = read_u32(data, offset) as usize;
        let full_offset = read_u64(data, offset + 20) as usize;
        let full_len = read_u64(data, offset + 28) as usize;
        let full_end = full_offset.checked_add(full_len).unwrap();
        assert!(full_end <= data.len());
        assert!(ranges.insert(cluster_idx, full_offset..full_end).is_none());
    }
    ranges
}

fn parse_full_ids(full: &[u8], expected_dim: usize) -> Vec<String> {
    let rows = read_u32(full, 0) as usize;
    assert_eq!(read_u32(full, 4) as usize, expected_dim);
    let mut ids = Vec::with_capacity(rows);
    let mut offset = 8;
    for _ in 0..rows {
        let id_len = read_u32(full, offset) as usize;
        offset += 4;
        let id_end = offset + id_len;
        ids.push(
            std::str::from_utf8(&full[offset..id_end])
                .unwrap()
                .to_string(),
        );
        offset = id_end + expected_dim * 4;
    }
    assert_eq!(offset, full.len());
    ids
}

fn parse_sketch_rows(data: Bytes) -> SketchRows {
    assert_eq!(&data[..4], b"ZSK1");
    assert_eq!(read_u32(&data, 4), 4);
    let code_dims = read_u32(&data, 12) as usize;
    let cluster_count = read_u32(&data, 16) as usize;
    let vector_count = read_u64(&data, 20) as usize;
    let attr_bytes = cluster_count.div_ceil(8);
    let counts_offset = SKETCH_V4_HEADER_LEN + attr_bytes;
    let codes_offset = counts_offset + cluster_count * 4;
    let row_bytes = code_dims / 4 + 8;
    assert_eq!(data.len(), codes_offset + vector_count * row_bytes);

    let mut row_offset = 0;
    let mut cluster_offsets = Vec::with_capacity(cluster_count);
    for cluster_idx in 0..cluster_count {
        let rows = read_u32(&data, counts_offset + cluster_idx * 4) as usize;
        cluster_offsets.push(row_offset..row_offset + rows);
        row_offset += rows;
    }
    assert_eq!(row_offset, vector_count);
    SketchRows {
        code_dims,
        row_bytes,
        cluster_offsets,
        codes: data.slice(codes_offset..),
    }
}

impl SketchRows {
    fn rq_payload(&self, cluster_idx: usize, ids: &[String]) -> Bytes {
        let rows = self
            .cluster_offsets
            .get(cluster_idx)
            .expect("sketch cluster must exist");
        assert_eq!(rows.len(), ids.len());
        let mut payload = Vec::new();
        payload.extend_from_slice(b"ZRQ1");
        payload.push(1);
        payload.extend_from_slice(&(self.code_dims as u64).to_le_bytes());
        payload.extend_from_slice(&(ids.len() as u64).to_le_bytes());
        for (local_row, id) in ids.iter().enumerate() {
            payload.extend_from_slice(&(id.len() as u64).to_le_bytes());
            payload.extend_from_slice(id.as_bytes());
            let global_row = rows.start + local_row;
            let start = global_row * self.row_bytes;
            payload.extend_from_slice(&self.codes[start..start + self.row_bytes]);
        }
        Bytes::from(payload)
    }
}

fn serialize_group(
    clusters: &[usize],
    coarse_payloads: &HashMap<usize, Bytes>,
    full_payloads: &HashMap<usize, Bytes>,
) -> (Bytes, RangedObjectEvidence) {
    let header_len = GROUP_OBJECT_HEADER_LEN + clusters.len() * GROUP_OBJECT_ENTRY_LEN;
    let mut coarse_offset = header_len;
    let mut full_offset = header_len
        + clusters
            .iter()
            .map(|cluster| coarse_payloads[cluster].len())
            .sum::<usize>();
    let first_full_offset = full_offset;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"ZBP\x04");
    bytes.extend_from_slice(&(clusters.len() as u32).to_le_bytes());
    for &cluster_idx in clusters {
        let coarse = &coarse_payloads[&cluster_idx];
        let full = &full_payloads[&cluster_idx];
        bytes.extend_from_slice(&(cluster_idx as u32).to_le_bytes());
        bytes.extend_from_slice(&(coarse_offset as u64).to_le_bytes());
        bytes.extend_from_slice(&(coarse.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(full_offset as u64).to_le_bytes());
        bytes.extend_from_slice(&(full.len() as u64).to_le_bytes());
        coarse_offset += coarse.len();
        full_offset += full.len();
    }
    for cluster_idx in clusters {
        bytes.extend_from_slice(&coarse_payloads[cluster_idx]);
    }
    for cluster_idx in clusters {
        bytes.extend_from_slice(&full_payloads[cluster_idx]);
    }
    let object_size = bytes.len();
    (
        Bytes::from(bytes),
        RangedObjectEvidence {
            key: String::new(),
            coarse_span: header_len..first_full_offset,
            first_full_offset,
            object_size,
        },
    )
}

async fn rewrite_active_segment_as_rq(fixture: &ScanFixture) -> Vec<RangedObjectEvidence> {
    let mut manifest = Manifest::read(&fixture.store, &fixture.namespace)
        .await
        .unwrap()
        .unwrap();
    let segment = active_segment(&manifest).clone();
    let sketch_ref = segment
        .sketch
        .as_ref()
        .expect("fixture segment must carry its resident sketch");
    let sketch = parse_sketch_rows(fixture.store.get(&sketch_ref.key).await.unwrap());
    assert_eq!(sketch.code_dims, sketch_ref.code_dims);

    let mut rewritten_objects = Vec::with_capacity(segment.cluster_objects.len());
    let mut evidence = Vec::with_capacity(segment.cluster_objects.len());
    for (object_index, object) in segment.cluster_objects.iter().enumerate() {
        let old_bytes = fixture.store.get(&object.key).await.unwrap();
        let full_ranges = parse_group_full_ranges(&old_bytes);
        let mut coarse_payloads = HashMap::new();
        let mut full_payloads = HashMap::new();
        for &cluster_idx in &object.clusters {
            let full_range = full_ranges
                .get(&cluster_idx)
                .expect("group directory must contain manifest cluster");
            let full = old_bytes.slice(full_range.clone());
            let ids = parse_full_ids(&full, DIM);
            coarse_payloads.insert(cluster_idx, sketch.rq_payload(cluster_idx, &ids));
            full_payloads.insert(cluster_idx, full);
        }
        let (bytes, mut object_evidence) =
            serialize_group(&object.clusters, &coarse_payloads, &full_payloads);
        let (prefix, _) = object.key.rsplit_once('/').unwrap();
        let key = format!("{prefix}/cluster_group_rq_{object_index}.bin");
        fixture.store.put(&key, bytes.clone()).await.unwrap();
        object_evidence.key = key.clone();
        evidence.push(object_evidence);
        rewritten_objects.push(ClusterDataObjectRef {
            key,
            clusters: object.clusters.clone(),
            live_offset: object.live_offset,
            live_len: object.live_len,
            size_bytes: bytes.len() as u64,
        });
    }

    let active_id = segment.id;
    let active = manifest
        .segments
        .iter_mut()
        .find(|candidate| candidate.id == active_id)
        .unwrap();
    active.cluster_objects = rewritten_objects;
    manifest.set_coarse_payload_encoding(active_id, CoarsePayloadEncoding::TwoBit);
    manifest
        .write(&fixture.store, &fixture.namespace)
        .await
        .unwrap();
    evidence
}

async fn query_ids(
    fixture: &ScanFixture,
    filter: Option<&Filter>,
    rerank_coalesce_gap_bytes: usize,
) -> Vec<String> {
    let wal_reader = WalReader::new(fixture.store.clone());
    execute_query(QueryParams {
        store: &fixture.store,
        wal_reader: &wal_reader,
        namespace: &fixture.namespace,
        query: &fixture.query,
        top_k: GROUP_SIZE,
        nprobe: CLUSTERS,
        filter,
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes,
        cache: None,
        manifest_cache: None,
        include_attributes: false,
    })
    .await
    .unwrap()
    .results
    .into_iter()
    .map(|result| result.id)
    .collect()
}

fn equality_filter(field: &str, value: i64) -> Filter {
    Filter::Eq {
        field: field.to_string(),
        value: AttributeValue::Integer(value),
    }
}

#[tokio::test]
async fn rq_scan_matches_sq8_unfiltered_and_at_two_filter_selectivities() {
    let fixture = scan_fixture("rq-scan-parity").await;
    let quarter = equality_filter("quarter", (QUERY_GROUP % 4) as i64);
    let sparse = equality_filter("sparse", (QUERY_GROUP % 31) as i64);
    let sq_unfiltered = query_ids(&fixture, None, DEFAULT_RERANK_COALESCE_GAP_BYTES).await;
    let sq_quarter = query_ids(&fixture, Some(&quarter), DEFAULT_RERANK_COALESCE_GAP_BYTES).await;
    let sq_sparse = query_ids(&fixture, Some(&sparse), DEFAULT_RERANK_COALESCE_GAP_BYTES).await;

    rewrite_active_segment_as_rq(&fixture).await;

    assert_eq!(
        query_ids(&fixture, None, DEFAULT_RERANK_COALESCE_GAP_BYTES).await,
        sq_unfiltered
    );
    assert_eq!(
        query_ids(&fixture, Some(&quarter), DEFAULT_RERANK_COALESCE_GAP_BYTES).await,
        sq_quarter
    );
    assert_eq!(
        query_ids(&fixture, Some(&sparse), DEFAULT_RERANK_COALESCE_GAP_BYTES).await,
        sq_sparse
    );
    fixture.harness.cleanup().await;
}

#[tokio::test]
#[ignore = "requires TEST_BACKEND=minio to validate physical range responses"]
async fn rq_coarse_fetch_reads_only_the_coarse_region() {
    let fixture = scan_fixture("rq-scan-ranges").await;
    let evidence = rewrite_active_segment_as_rq(&fixture).await;
    let sparse = equality_filter("sparse", (QUERY_GROUP % 31) as i64);

    fixture.counter.reset();
    let results = query_ids(&fixture, Some(&sparse), 0).await;
    assert_eq!(results.len(), GROUP_SIZE);

    for object in evidence {
        let ranges = fixture.counter.ranges_for(&object.key);
        assert!(
            ranges.contains(&object.coarse_span),
            "missing coarse-only fetch {:?} for {}: {ranges:?}",
            object.coarse_span,
            object.key
        );
        assert!(
            ranges
                .iter()
                .all(|range| !(range.start < object.first_full_offset
                    && range.end > object.first_full_offset)),
            "a range crossed from coarse bytes into full vectors for {}: {ranges:?}",
            object.key
        );
        assert!(
            !ranges.contains(&(0..object.object_size)),
            "RQ query fetched the complete grouped object {}",
            object.key
        );
    }
    fixture.harness.cleanup().await;
}
