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
use zeppelin::wal::manifest::{
    ClusterDataObjectRef, ClusterRowLayoutRef, CoarsePayloadEncoding, Manifest, SegmentRef,
    CLUSTER_LAYOUT_VERSION_ZBP5,
};
use zeppelin::wal::{WalReader, WalWriter};

const ROWS: usize = 20_000;
const DIM: usize = 256;
const GROUP_SIZE: usize = 10;
const CLUSTERS: usize = 4;
const QUERY_GROUP: usize = 1_240;
const GROUP_OBJECT_HEADER_LEN: usize = 8;
/// One `ZBP5` directory record: cluster index, row count, three ranges.
const GROUP_OBJECT_V5_ENTRY_LEN: usize = 4 + 4 + 8 * 6;
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
    /// The single span a `ZBP5` coarse read covers: codes plus ID blocks.
    coarse_span: Range<usize>,
    /// Absolute start of the first fixed-stride vector block.
    first_vectors_offset: usize,
    /// Per-cluster fixed-stride row spans, keyed by cluster index.
    vector_rows: HashMap<usize, Vec<Range<usize>>>,
    object_size: usize,
}

/// One `ZBP5` directory entry read back from a published object.
#[derive(Clone)]
struct GroupV5Entry {
    cluster_idx: usize,
    row_count: usize,
    coarse: Range<usize>,
    ids: Range<usize>,
    vectors: Range<usize>,
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

fn parse_group_v5(data: &[u8]) -> Vec<GroupV5Entry> {
    assert_eq!(&data[..4], b"ZBP\x05", "fixture must publish ZBP5 objects");
    let entry_count = read_u32(data, 4) as usize;
    let mut entries = Vec::with_capacity(entry_count);
    for entry in 0..entry_count {
        let base = GROUP_OBJECT_HEADER_LEN + entry * GROUP_OBJECT_V5_ENTRY_LEN;
        let cluster_idx = read_u32(data, base) as usize;
        let row_count = read_u32(data, base + 4) as usize;
        let coarse_offset = read_u64(data, base + 8) as usize;
        let coarse_len = read_u64(data, base + 16) as usize;
        let ids_offset = read_u64(data, base + 24) as usize;
        let ids_len = read_u64(data, base + 32) as usize;
        let vectors_offset = read_u64(data, base + 40) as usize;
        let vectors_len = read_u64(data, base + 48) as usize;
        assert!(vectors_offset + vectors_len <= data.len());
        assert_eq!(vectors_len, row_count * DIM * 4);
        assert!(coarse_offset + coarse_len <= ids_offset);
        entries.push(GroupV5Entry {
            cluster_idx,
            row_count,
            coarse: coarse_offset..coarse_offset + coarse_len,
            ids: ids_offset..ids_offset + ids_len,
            vectors: vectors_offset..vectors_offset + vectors_len,
        });
    }
    entries
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
    /// Builds the codes-only two-bit coarse block a `ZBP5` object stores.
    ///
    /// IDs live in the sibling ID block, so this payload is `[row_count][dim]`
    /// followed by each row's planes and factors, joined to IDs by position.
    fn rq_codes_only_payload(&self, cluster_idx: usize, row_count: usize) -> Bytes {
        let rows = self
            .cluster_offsets
            .get(cluster_idx)
            .expect("sketch cluster must exist");
        assert_eq!(rows.len(), row_count);
        let mut payload = Vec::new();
        payload.extend_from_slice(&(row_count as u64).to_le_bytes());
        payload.extend_from_slice(&(self.code_dims as u64).to_le_bytes());
        for local_row in 0..row_count {
            let start = (rows.start + local_row) * self.row_bytes;
            payload.extend_from_slice(&self.codes[start..start + self.row_bytes]);
        }
        Bytes::from(payload)
    }
}

/// Reserializes one cluster group as a `ZBP5` object with new coarse blocks.
///
/// The ID and fixed-stride vector blocks are copied verbatim from the source
/// object, so only the coarse encoding changes. The returned evidence records
/// exactly the spans a manifest-driven reader should request.
fn serialize_group_v5(
    entries: &[GroupV5Entry],
    source: &Bytes,
    coarse_payloads: &HashMap<usize, Bytes>,
) -> (Bytes, RangedObjectEvidence, Vec<ClusterRowLayoutRef>) {
    let header_len = GROUP_OBJECT_HEADER_LEN + entries.len() * GROUP_OBJECT_V5_ENTRY_LEN;
    let mut coarse_ranges = Vec::with_capacity(entries.len());
    let mut cursor = header_len;
    for entry in entries {
        let len = coarse_payloads[&entry.cluster_idx].len();
        coarse_ranges.push(cursor..cursor + len);
        cursor += len;
    }
    let mut id_ranges = Vec::with_capacity(entries.len());
    for entry in entries {
        let len = entry.ids.len();
        id_ranges.push(cursor..cursor + len);
        cursor += len;
    }
    let mut vector_ranges = Vec::with_capacity(entries.len());
    for entry in entries {
        let len = entry.vectors.len();
        vector_ranges.push(cursor..cursor + len);
        cursor += len;
    }

    let mut bytes = Vec::with_capacity(cursor);
    bytes.extend_from_slice(b"ZBP\x05");
    bytes.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (idx, entry) in entries.iter().enumerate() {
        bytes.extend_from_slice(&(entry.cluster_idx as u32).to_le_bytes());
        bytes.extend_from_slice(&(entry.row_count as u32).to_le_bytes());
        bytes.extend_from_slice(&(coarse_ranges[idx].start as u64).to_le_bytes());
        bytes.extend_from_slice(&(coarse_ranges[idx].len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(id_ranges[idx].start as u64).to_le_bytes());
        bytes.extend_from_slice(&(id_ranges[idx].len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(vector_ranges[idx].start as u64).to_le_bytes());
        bytes.extend_from_slice(&(vector_ranges[idx].len() as u64).to_le_bytes());
    }
    for entry in entries {
        bytes.extend_from_slice(&coarse_payloads[&entry.cluster_idx]);
    }
    for entry in entries {
        bytes.extend_from_slice(&source[entry.ids.clone()]);
    }
    for entry in entries {
        bytes.extend_from_slice(&source[entry.vectors.clone()]);
    }
    assert_eq!(bytes.len(), cursor);

    let row_layouts = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| ClusterRowLayoutRef {
            cluster_idx: entry.cluster_idx,
            row_count: entry.row_count as u64,
            coarse_offset: coarse_ranges[idx].start as u64,
            coarse_len: coarse_ranges[idx].len() as u64,
            ids_offset: id_ranges[idx].start as u64,
            ids_len: id_ranges[idx].len() as u64,
            vectors_offset: vector_ranges[idx].start as u64,
            vectors_len: vector_ranges[idx].len() as u64,
        })
        .collect();
    let vector_rows = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| {
            let base = vector_ranges[idx].start;
            let stride = DIM * 4;
            (
                entry.cluster_idx,
                (0..entry.row_count)
                    .map(|row| base + row * stride..base + (row + 1) * stride)
                    .collect(),
            )
        })
        .collect();
    let evidence = RangedObjectEvidence {
        key: String::new(),
        coarse_span: header_len..id_ranges[entries.len() - 1].end,
        first_vectors_offset: vector_ranges[0].start,
        vector_rows,
        object_size: cursor,
    };
    (Bytes::from(bytes), evidence, row_layouts)
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
        assert_eq!(
            object.cluster_layout_version, CLUSTER_LAYOUT_VERSION_ZBP5,
            "fixture SQ8 compaction must publish a row layout"
        );
        let old_bytes = fixture.store.get(&object.key).await.unwrap();
        let entries = parse_group_v5(&old_bytes);
        let mut coarse_payloads = HashMap::new();
        for entry in &entries {
            coarse_payloads.insert(
                entry.cluster_idx,
                sketch.rq_codes_only_payload(entry.cluster_idx, entry.row_count),
            );
        }
        let (bytes, mut object_evidence, row_layouts) =
            serialize_group_v5(&entries, &old_bytes, &coarse_payloads);
        let (prefix, _) = object.key.rsplit_once('/').unwrap();
        let key = format!("{prefix}/cluster_group_rq_{object_index}.bin");
        fixture.store.put(&key, bytes.clone()).await.unwrap();
        object_evidence.key = key.clone();
        evidence.push(object_evidence);
        rewritten_objects.push(ClusterDataObjectRef {
            key,
            clusters: entries.iter().map(|entry| entry.cluster_idx).collect(),
            live_offset: object.live_offset,
            live_len: object.live_len,
            size_bytes: bytes.len() as u64,
            cluster_layout_version: CLUSTER_LAYOUT_VERSION_ZBP5,
            row_layouts,
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

/// Rewrites the active segment's objects into the equivalent `ZBP4` layout.
///
/// Every row keeps its identity, order, coarse codes, and exact vector bytes;
/// only the physical arrangement changes — IDs move back inline and the exact
/// rows regain their per-row headers. This is the control for proving that the
/// `ZBP5` substitution does not move results.
async fn rewrite_active_segment_as_v4(fixture: &ScanFixture, encoding: CoarsePayloadEncoding) {
    let mut manifest = Manifest::read(&fixture.store, &fixture.namespace)
        .await
        .unwrap()
        .unwrap();
    let segment = active_segment(&manifest).clone();
    let mut rewritten_objects = Vec::with_capacity(segment.cluster_objects.len());
    for (object_index, object) in segment.cluster_objects.iter().enumerate() {
        assert_eq!(object.cluster_layout_version, CLUSTER_LAYOUT_VERSION_ZBP5);
        let source = fixture.store.get(&object.key).await.unwrap();
        let entries = parse_group_v5(&source);
        let mut coarse_payloads = HashMap::new();
        let mut full_payloads = HashMap::new();
        for entry in &entries {
            let ids = parse_id_block(&source[entry.ids.clone()]);
            assert_eq!(ids.len(), entry.row_count);
            let vectors = &source[entry.vectors.clone()];
            coarse_payloads.insert(
                entry.cluster_idx,
                v4_coarse_section(encoding, &ids, &source[entry.coarse.clone()]),
            );
            full_payloads.insert(entry.cluster_idx, legacy_full_section(&ids, vectors));
        }
        let clusters: Vec<usize> = entries.iter().map(|entry| entry.cluster_idx).collect();
        let bytes = serialize_group_v4(&clusters, &coarse_payloads, &full_payloads, encoding);
        let (prefix, _) = object.key.rsplit_once('/').unwrap();
        let key = format!("{prefix}/cluster_group_v4_{object_index}.bin");
        fixture.store.put(&key, bytes.clone()).await.unwrap();
        rewritten_objects.push(ClusterDataObjectRef {
            key,
            clusters,
            live_offset: 0,
            live_len: 0,
            size_bytes: bytes.len() as u64,
            cluster_layout_version: 0,
            row_layouts: Vec::new(),
        });
    }

    let active_id = segment.id;
    let active = manifest
        .segments
        .iter_mut()
        .find(|candidate| candidate.id == active_id)
        .unwrap();
    active.cluster_objects = rewritten_objects;
    manifest.set_coarse_payload_encoding(active_id, encoding);
    manifest
        .write(&fixture.store, &fixture.namespace)
        .await
        .unwrap();
}

fn parse_id_block(data: &[u8]) -> Vec<String> {
    let rows = read_u32(data, 0) as usize;
    let mut ids = Vec::with_capacity(rows);
    let mut offset = 4;
    for _ in 0..rows {
        let id_len = read_u32(data, offset) as usize;
        offset += 4;
        ids.push(
            std::str::from_utf8(&data[offset..offset + id_len])
                .unwrap()
                .to_string(),
        );
        offset += id_len;
    }
    assert_eq!(offset, data.len());
    ids
}

/// Rebuilds a coarse child payload that carries its own row IDs.
fn v4_coarse_section(encoding: CoarsePayloadEncoding, ids: &[String], codes_only: &[u8]) -> Bytes {
    let mut payload = Vec::new();
    match encoding {
        CoarsePayloadEncoding::Sq8 => {
            let row_count = read_u32(codes_only, 0) as usize;
            let dim = read_u32(codes_only, 4) as usize;
            assert_eq!(row_count, ids.len());
            payload.extend_from_slice(&(ids.len() as u32).to_le_bytes());
            payload.extend_from_slice(&(dim as u32).to_le_bytes());
            for (row, id) in ids.iter().enumerate() {
                payload.extend_from_slice(&(id.len() as u32).to_le_bytes());
                payload.extend_from_slice(id.as_bytes());
                let start = 8 + row * dim;
                payload.extend_from_slice(&codes_only[start..start + dim]);
            }
        }
        CoarsePayloadEncoding::TwoBit => {
            let row_count = read_u64(codes_only, 0) as usize;
            let dim = read_u64(codes_only, 8) as usize;
            assert_eq!(row_count, ids.len());
            let row_bytes = (codes_only.len() - 16) / row_count.max(1);
            payload.extend_from_slice(b"ZRQ1");
            payload.push(1);
            payload.extend_from_slice(&(dim as u64).to_le_bytes());
            payload.extend_from_slice(&(ids.len() as u64).to_le_bytes());
            for (row, id) in ids.iter().enumerate() {
                payload.extend_from_slice(&(id.len() as u64).to_le_bytes());
                payload.extend_from_slice(id.as_bytes());
                let start = 16 + row * row_bytes;
                payload.extend_from_slice(&codes_only[start..start + row_bytes]);
            }
        }
    }
    Bytes::from(payload)
}

/// Rebuilds the legacy `[n][dim][(id_len, id, f32[dim])...]` exact section.
fn legacy_full_section(ids: &[String], vectors: &[u8]) -> Bytes {
    let stride = DIM * 4;
    assert_eq!(vectors.len(), ids.len() * stride);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(DIM as u32).to_le_bytes());
    for (row, id) in ids.iter().enumerate() {
        payload.extend_from_slice(&(id.len() as u32).to_le_bytes());
        payload.extend_from_slice(id.as_bytes());
        payload.extend_from_slice(&vectors[row * stride..(row + 1) * stride]);
    }
    Bytes::from(payload)
}

fn serialize_group_v4(
    clusters: &[usize],
    coarse_payloads: &HashMap<usize, Bytes>,
    full_payloads: &HashMap<usize, Bytes>,
    encoding: CoarsePayloadEncoding,
) -> Bytes {
    let magic: &[u8; 4] = match encoding {
        CoarsePayloadEncoding::Sq8 => b"ZCL2",
        CoarsePayloadEncoding::TwoBit => b"ZCL3",
    };
    // Each v4 directory entry points at one complete co-located child section.
    let sections: Vec<(usize, Bytes)> = clusters
        .iter()
        .map(|&cluster_idx| {
            let coarse = &coarse_payloads[&cluster_idx];
            let full = &full_payloads[&cluster_idx];
            let header = 4 + 8 * 4;
            let mut section = Vec::with_capacity(header + coarse.len() + full.len());
            section.extend_from_slice(magic);
            section.extend_from_slice(&(header as u64).to_le_bytes());
            section.extend_from_slice(&(coarse.len() as u64).to_le_bytes());
            section.extend_from_slice(&((header + coarse.len()) as u64).to_le_bytes());
            section.extend_from_slice(&(full.len() as u64).to_le_bytes());
            section.extend_from_slice(coarse);
            section.extend_from_slice(full);
            (cluster_idx, Bytes::from(section))
        })
        .collect();

    let header_len = GROUP_OBJECT_HEADER_LEN + clusters.len() * (4 + 8 * 4);
    let mut coarse_offset = header_len;
    let mut full_offset = header_len
        + sections
            .iter()
            .map(|(_, section)| read_u64(section, 12) as usize)
            .sum::<usize>();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"ZBP\x04");
    bytes.extend_from_slice(&(clusters.len() as u32).to_le_bytes());
    for (cluster_idx, section) in &sections {
        let coarse_len = read_u64(section, 12) as usize;
        let full_len = read_u64(section, 28) as usize;
        bytes.extend_from_slice(&(*cluster_idx as u32).to_le_bytes());
        bytes.extend_from_slice(&(coarse_offset as u64).to_le_bytes());
        bytes.extend_from_slice(&(coarse_len as u64).to_le_bytes());
        bytes.extend_from_slice(&(full_offset as u64).to_le_bytes());
        bytes.extend_from_slice(&(full_len as u64).to_le_bytes());
        coarse_offset += coarse_len;
        full_offset += full_len;
    }
    for (_, section) in &sections {
        let coarse_start = read_u64(section, 4) as usize;
        let coarse_len = read_u64(section, 12) as usize;
        bytes.extend_from_slice(&section[coarse_start..coarse_start + coarse_len]);
    }
    for (_, section) in &sections {
        let full_start = read_u64(section, 20) as usize;
        let full_len = read_u64(section, 28) as usize;
        bytes.extend_from_slice(&section[full_start..full_start + full_len]);
    }
    Bytes::from(bytes)
}

async fn query_results(fixture: &ScanFixture, filter: Option<&Filter>) -> Vec<(String, u32)> {
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
        rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: None,
        manifest_cache: None,
        include_attributes: false,
    })
    .await
    .unwrap()
    .results
    .into_iter()
    .map(|result| (result.id, result.score.to_bits()))
    .collect()
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

/// Layout substitution must not move a single result.
///
/// The table covers both coarse encodings and both filter states. For each
/// cell, the same rows are queried through the published `ZBP5` layout and then
/// through an equivalent `ZBP4` object rebuilt from those exact bytes; top-k IDs
/// and raw distance bits must be identical. Distances are compared as bit
/// patterns because they are recomputed from exact f32 in both layouts, so any
/// difference would mean the rerank read the wrong row.
#[tokio::test]
async fn layout_substitution_preserves_top_k_ids_and_distance_bits() {
    let sparse = equality_filter("sparse", (QUERY_GROUP % 31) as i64);
    for encoding in [CoarsePayloadEncoding::Sq8, CoarsePayloadEncoding::TwoBit] {
        let name = match encoding {
            CoarsePayloadEncoding::Sq8 => "layout-identity-sq8",
            CoarsePayloadEncoding::TwoBit => "layout-identity-two-bit",
        };
        let fixture = scan_fixture(name).await;
        if encoding == CoarsePayloadEncoding::TwoBit {
            rewrite_active_segment_as_rq(&fixture).await;
        }

        let v5_unfiltered = query_results(&fixture, None).await;
        let v5_filtered = query_results(&fixture, Some(&sparse)).await;
        assert!(!v5_unfiltered.is_empty(), "{name}: v5 returned no results");
        assert!(
            !v5_filtered.is_empty(),
            "{name}: filtered v5 returned no results"
        );

        rewrite_active_segment_as_v4(&fixture, encoding).await;

        assert_eq!(
            query_results(&fixture, None).await,
            v5_unfiltered,
            "{name}: unfiltered results moved when the layout changed"
        );
        assert_eq!(
            query_results(&fixture, Some(&sparse)).await,
            v5_filtered,
            "{name}: filtered results moved when the layout changed"
        );
        fixture.harness.cleanup().await;
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
async fn zbp5_ranged_read_uses_published_regions_without_a_header_get() {
    let fixture = scan_fixture("rq-scan-ranges").await;
    let evidence = rewrite_active_segment_as_rq(&fixture).await;
    let sparse = equality_filter("sparse", (QUERY_GROUP % 31) as i64);

    fixture.counter.reset();
    // Gap zero disables coalescing so every rerank range is exactly one
    // published fixed-stride row.
    let results = query_ids(&fixture, Some(&sparse), 0).await;
    assert_eq!(results.len(), GROUP_SIZE);

    for object in evidence {
        let ranges = fixture.counter.ranges_for(&object.key);
        assert!(
            ranges.contains(&object.coarse_span),
            "missing published coarse+ID fetch {:?} for {}: {ranges:?}",
            object.coarse_span,
            object.key
        );
        // No grouped-object directory read: the manifest already published the
        // ranges, so nothing may request the header prefix.
        assert!(
            ranges
                .iter()
                .all(|range| range.start != 0 || range.end >= object.coarse_span.end),
            "a range read the grouped-object header for {}: {ranges:?}",
            object.key
        );
        assert!(
            ranges
                .iter()
                .all(|range| !(range.start < object.first_vectors_offset
                    && range.end > object.first_vectors_offset)),
            "a range crossed from coarse bytes into vector blocks for {}: {ranges:?}",
            object.key
        );
        assert!(
            !ranges.contains(&(0..object.object_size)),
            "query fetched the complete grouped object {}",
            object.key
        );
        // Every rerank range is exactly one published fixed-stride row: an
        // offset derived by walking ID lengths could not land on this grid.
        let published_rows: Vec<Range<usize>> = object
            .vector_rows
            .values()
            .flat_map(|rows| rows.iter().cloned())
            .collect();
        let rerank_ranges: Vec<&Range<usize>> = ranges
            .iter()
            .filter(|range| range.start >= object.first_vectors_offset)
            .collect();
        assert!(
            !rerank_ranges.is_empty(),
            "no rerank range was issued for {}",
            object.key
        );
        for range in rerank_ranges {
            assert_eq!(
                range.end - range.start,
                DIM * 4,
                "rerank range {range:?} for {} is not one fixed-stride row",
                object.key
            );
            assert!(
                published_rows.contains(range),
                "rerank range {range:?} for {} is not a published row span",
                object.key
            );
        }
    }
    fixture.harness.cleanup().await;
}
