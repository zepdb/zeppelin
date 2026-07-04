//! Build phase for IVF-Flat index.
//!
//! Pipeline: train centroids -> assign vectors to clusters -> serialize and
//! write artifacts (centroids, cluster vectors, cluster attributes) to S3.
//!
//! ## Phase C.0b storage-format design
//!
//! This change is intentionally per-object versioned; the manifest remains
//! unchanged so old immutable segments and new segments can coexist in one
//! namespace. The compatibility boundary is:
//!
//! - Old `centroids.bin`: `[num_centroids:u32][dim:u32][f32...]`.
//! - New `centroids.bin`: `[b"ZCT2"][num_centroids:u32][dim:u32][f32...]
//!   [sq_calibration_len:u64][sq_calibration bytes]`. The calibration bytes are
//!   exactly the existing SQ calibration payload. `sq_calibration_len == 0`
//!   means the segment has no embedded SQ calibration. Readers detect `b"ZCT2"`
//!   and otherwise parse the legacy bytes. SQ readers first ask the loaded
//!   centroids blob for embedded calibration; when absent they read the legacy
//!   `sq_calibration.bin` key.
//! - Old `cluster_i.bin`: `[num_vectors:u32][dim:u32] ... full-precision rows`.
//! - New `cluster_i.bin`: `[b"ZCL2"][sq_offset:u64][sq_len:u64]
//!   [full_offset:u64][full_len:u64][sq_cluster bytes][full cluster bytes]`.
//!   Section offsets are absolute byte offsets from the beginning of the
//!   object. The coarse SQ path fetches this whole object through the normal
//!   cache and parses only the SQ section; the rerank path later asks for the
//!   same `cluster_i.bin` key and gets a cache hit, then parses the full section.
//!   The offset table is present now so a later range-GET implementation can
//!   fetch only `[sq_offset, sq_offset + sq_len)` without changing the format.
//! - `attrs_i.bin` is unchanged and remains lazy-loaded.
//! - `sq_cluster_i.bin` and `sq_calibration.bin` are legacy read-only keys.
//!   New SQ segments do not write them. Old-format carried clusters continue
//!   routing through `cluster_owner()` to their original segment; if that
//!   owner's `cluster_i.bin` is legacy, SQ coarse reads fall back to
//!   `sq_cluster_i.bin` under the same owner and calibration falls back to the
//!   active segment's legacy `sq_calibration.bin`.
//! - Hierarchical indexes do not have an IVF `centroids.bin`; their
//!   read-once-up-front metadata is `tree_meta.json`. New hierarchical SQ
//!   segments store the same legacy SQ calibration bytes in an optional
//!   `sq_calibration` JSON field there. Old tree metadata lacks the field and
//!   therefore falls back to legacy `sq_calibration.bin`.
//! - Compaction migrates only rewritten clusters by writing the new co-located
//!   object under the new segment. Task 2B carried clusters keep their old owner
//!   string in `cluster_owners`, so their old physical keys stay authoritative.
//! - PQ is deliberately deferred. The current requirement and GET-count target
//!   are specific to SQ calibration and SQ cluster/full-vector co-location; PQ
//!   keeps `pq_codebook.bin` and `pq_cluster_i.bin` unchanged for this phase.

use bytes::Bytes;
use std::collections::HashMap;
use tracing::{debug, info};

use crate::config::IndexingConfig;
use crate::error::{Result, ZeppelinError};
use crate::index::quantization::QuantizationType;
use crate::storage::ZeppelinStore;
use crate::types::{AttributeValue, VectorEntry};

/// Pre-serialized cluster payload: (vec_key, vec_data, attr_key, attr_data, optional bitmap).
type ClusterPayload = (String, Bytes, String, Bytes, Option<(String, Bytes)>);

use super::kmeans::train_kmeans;
use super::IvfFlatIndex;
use crate::index::distance;

const CENTROIDS_V2_MAGIC: &[u8; 4] = b"ZCT2";
const CLUSTER_V2_MAGIC: &[u8; 4] = b"ZCL2";
const CLUSTER_V2_HEADER_LEN: usize = 4 + 8 * 4;

// ---------------------------------------------------------------------------
// Artifact paths
// ---------------------------------------------------------------------------

/// S3 key for the centroids blob.
pub fn centroids_key(namespace: &str, segment_id: &str) -> String {
    format!("{namespace}/segments/{segment_id}/centroids.bin")
}

/// S3 key for the vector data of cluster `i`.
pub(crate) fn cluster_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/cluster_{cluster_idx}.bin")
}

/// S3 key for the attribute data of cluster `i`.
pub(crate) fn attrs_key(namespace: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{namespace}/segments/{segment_id}/attrs_{cluster_idx}.bin")
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

/// Header written before the centroid float array.
///
/// Layout:
/// `[b"ZCT2"][num_centroids: u32][dimension: u32]`
/// `[f32 * num_centroids * dimension][sq_calibration_len:u64][sq_calibration bytes]`
pub(crate) fn serialize_centroids(centroids: &[Vec<f32>], dim: usize) -> Result<Bytes> {
    serialize_centroids_with_sq_calibration(centroids, dim, None)
}

/// Serialize centroids with an optional embedded SQ calibration payload.
pub(crate) fn serialize_centroids_with_sq_calibration(
    centroids: &[Vec<f32>],
    dim: usize,
    sq_calibration: Option<&[u8]>,
) -> Result<Bytes> {
    let num_centroids = centroids.len() as u32;
    let dimension = dim as u32;
    let sq_calibration_len = sq_calibration.map_or(0, |bytes| bytes.len());

    let float_bytes = centroids.len() * dim * std::mem::size_of::<f32>();
    let total = 4 + 8 + float_bytes + 8 + sq_calibration_len;
    let mut buf = Vec::with_capacity(total);

    buf.extend_from_slice(CENTROIDS_V2_MAGIC);
    buf.extend_from_slice(&num_centroids.to_le_bytes());
    buf.extend_from_slice(&dimension.to_le_bytes());

    for centroid in centroids {
        for &val in centroid {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }
    buf.extend_from_slice(&(sq_calibration_len as u64).to_le_bytes());
    if let Some(calibration) = sq_calibration {
        buf.extend_from_slice(calibration);
    }

    debug_assert_eq!(buf.len(), total);
    Ok(Bytes::from(buf))
}

/// Parsed centroid blob with optional embedded SQ calibration.
#[derive(Debug)]
pub(crate) struct CentroidsData {
    /// IVF centroids.
    pub centroids: Vec<Vec<f32>>,
    /// Vector dimensionality.
    pub dim: usize,
    /// Embedded legacy SQ calibration payload, present for new SQ segments.
    pub sq_calibration: Option<Bytes>,
}

/// Deserialize centroids from the binary format produced by `serialize_centroids`.
pub(crate) fn deserialize_centroids(data: &[u8]) -> Result<(Vec<Vec<f32>>, usize)> {
    let decoded = deserialize_centroids_data(data)?;
    Ok((decoded.centroids, decoded.dim))
}

/// Deserialize centroids, auto-detecting legacy and v2 object formats.
pub(crate) fn deserialize_centroids_data(data: &[u8]) -> Result<CentroidsData> {
    if data.starts_with(CENTROIDS_V2_MAGIC) {
        return deserialize_centroids_v2(data);
    }
    deserialize_centroids_legacy(data)
}

fn deserialize_centroids_legacy(data: &[u8]) -> Result<CentroidsData> {
    if data.len() < 8 {
        return Err(ZeppelinError::Index(
            "centroids blob too small for header".into(),
        ));
    }

    let num_centroids = u32::from_le_bytes(
        data[0..4]
            .try_into()
            .map_err(|_| ZeppelinError::Index("centroids header parse error".into()))?,
    ) as usize;
    let dim = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("centroids header parse error".into()))?,
    ) as usize;

    let expected = 8 + num_centroids * dim * 4;
    if data.len() < expected {
        return Err(ZeppelinError::Index(format!(
            "centroids blob size mismatch: expected {expected}, got {}",
            data.len()
        )));
    }

    let mut centroids = Vec::with_capacity(num_centroids);
    let mut offset = 8;
    for _ in 0..num_centroids {
        let mut c = Vec::with_capacity(dim);
        for _ in 0..dim {
            let val = f32::from_le_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .map_err(|_| ZeppelinError::Index("centroids float parse error".into()))?,
            );
            c.push(val);
            offset += 4;
        }
        centroids.push(c);
    }

    Ok(CentroidsData {
        centroids,
        dim,
        sq_calibration: None,
    })
}

fn deserialize_centroids_v2(data: &[u8]) -> Result<CentroidsData> {
    if data.len() < 12 {
        return Err(ZeppelinError::Index(
            "v2 centroids blob too small for header".into(),
        ));
    }

    let num_centroids = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("v2 centroids header parse error".into()))?,
    ) as usize;
    let dim = u32::from_le_bytes(
        data[8..12]
            .try_into()
            .map_err(|_| ZeppelinError::Index("v2 centroids header parse error".into()))?,
    ) as usize;

    let float_bytes = num_centroids
        .checked_mul(dim)
        .and_then(|v| v.checked_mul(4))
        .ok_or_else(|| {
            ZeppelinError::Index(format!(
                "v2 centroids size overflows: num_centroids={num_centroids}, dim={dim}"
            ))
        })?;
    let calibration_len_offset = 12usize.checked_add(float_bytes).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "v2 centroids offset overflows: num_centroids={num_centroids}, dim={dim}"
        ))
    })?;
    if data.len() < calibration_len_offset + 8 {
        return Err(ZeppelinError::Index(format!(
            "v2 centroids blob size mismatch: expected at least {}, got {}",
            calibration_len_offset + 8,
            data.len()
        )));
    }

    let mut centroids = Vec::with_capacity(num_centroids);
    let mut offset = 12;
    for _ in 0..num_centroids {
        let mut c = Vec::with_capacity(dim);
        for _ in 0..dim {
            let val = f32::from_le_bytes(
                data[offset..offset + 4]
                    .try_into()
                    .map_err(|_| ZeppelinError::Index("v2 centroids float parse error".into()))?,
            );
            c.push(val);
            offset += 4;
        }
        centroids.push(c);
    }

    let sq_len = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("v2 centroids SQ length parse error".into()))?,
    ) as usize;
    offset += 8;
    let expected = offset.checked_add(sq_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "v2 centroids SQ section size overflows: sq_len={sq_len}"
        ))
    })?;
    if data.len() != expected {
        return Err(ZeppelinError::Index(format!(
            "v2 centroids blob size mismatch: expected {expected}, got {}",
            data.len()
        )));
    }
    let sq_calibration = if sq_len == 0 {
        None
    } else {
        Some(Bytes::copy_from_slice(&data[offset..expected]))
    };

    Ok(CentroidsData {
        centroids,
        dim,
        sq_calibration,
    })
}

/// Cluster blob layout:
/// `[num_vectors: u32][dimension: u32]`
/// then for each vector: `[id_len: u32][id_bytes...][f32 * dim]`
pub(crate) fn serialize_cluster(ids: &[String], vectors: &[Vec<f32>], dim: usize) -> Result<Bytes> {
    let n = ids.len() as u32;
    let dimension = dim as u32;

    let mut buf = Vec::new();
    buf.extend_from_slice(&n.to_le_bytes());
    buf.extend_from_slice(&dimension.to_le_bytes());

    for (id, vec) in ids.iter().zip(vectors.iter()) {
        let id_bytes = id.as_bytes();
        let id_len = id_bytes.len() as u32;
        buf.extend_from_slice(&id_len.to_le_bytes());
        buf.extend_from_slice(id_bytes);
        for &val in vec {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }

    Ok(Bytes::from(buf))
}

/// Serialize a v2 per-cluster object containing SQ codes and full vectors.
pub(crate) fn serialize_colocated_sq_cluster(
    ids: &[String],
    vectors: &[Vec<f32>],
    sq_codes: &[Vec<u8>],
    dim: usize,
) -> Result<Bytes> {
    let sq_data = crate::index::quantization::sq::serialize_sq_cluster(ids, sq_codes, dim)?;
    let full_data = serialize_cluster(ids, vectors, dim)?;
    let sq_offset = CLUSTER_V2_HEADER_LEN as u64;
    let sq_len = sq_data.len() as u64;
    let full_offset = sq_offset + sq_len;
    let full_len = full_data.len() as u64;

    let total = CLUSTER_V2_HEADER_LEN + sq_data.len() + full_data.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(CLUSTER_V2_MAGIC);
    buf.extend_from_slice(&sq_offset.to_le_bytes());
    buf.extend_from_slice(&sq_len.to_le_bytes());
    buf.extend_from_slice(&full_offset.to_le_bytes());
    buf.extend_from_slice(&full_len.to_le_bytes());
    buf.extend_from_slice(&sq_data);
    buf.extend_from_slice(&full_data);
    debug_assert_eq!(buf.len(), total);

    Ok(Bytes::from(buf))
}

/// Cluster data for a single cluster.
#[derive(Debug)]
pub(crate) struct ClusterData {
    pub ids: Vec<String>,
    pub vectors: Vec<Vec<f32>>,
}

/// Deserialize a cluster blob.
pub(crate) fn deserialize_cluster(data: &[u8]) -> Result<ClusterData> {
    let data = full_cluster_section(data)?;
    deserialize_legacy_cluster(data)
}

fn deserialize_legacy_cluster(data: &[u8]) -> Result<ClusterData> {
    if data.len() < 8 {
        return Err(ZeppelinError::Index(
            "cluster blob too small for header".into(),
        ));
    }

    let n = u32::from_le_bytes(
        data[0..4]
            .try_into()
            .map_err(|_| ZeppelinError::Index("cluster header parse error".into()))?,
    ) as usize;
    let dim = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| ZeppelinError::Index("cluster header parse error".into()))?,
    ) as usize;

    let mut ids = Vec::with_capacity(n);
    let mut vectors = Vec::with_capacity(n);
    let mut offset = 8;

    for _ in 0..n {
        if offset + 4 > data.len() {
            return Err(ZeppelinError::Index(
                "cluster blob truncated at id_len".into(),
            ));
        }
        let id_len = u32::from_le_bytes(
            data[offset..offset + 4]
                .try_into()
                .map_err(|_| ZeppelinError::Index("cluster id_len parse error".into()))?,
        ) as usize;
        offset += 4;

        if offset + id_len > data.len() {
            return Err(ZeppelinError::Index("cluster blob truncated at id".into()));
        }
        let id = String::from_utf8_lossy(&data[offset..offset + id_len]).into_owned();
        offset += id_len;

        let float_bytes = dim * 4;
        if offset + float_bytes > data.len() {
            return Err(ZeppelinError::Index(
                "cluster blob truncated at vector data".into(),
            ));
        }
        // Parse f32 slice using chunks_exact — enables compiler auto-vectorization
        // and removes per-element try_into/map_err overhead.
        let vec: Vec<f32> = data[offset..offset + float_bytes]
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        offset += float_bytes;

        ids.push(id);
        vectors.push(vec);
    }

    Ok(ClusterData { ids, vectors })
}

/// Deserialize the SQ section of a v2 co-located cluster object.
pub(crate) fn deserialize_colocated_sq_cluster(
    data: &[u8],
) -> Result<Option<crate::index::quantization::sq::SqClusterData>> {
    if !data.starts_with(CLUSTER_V2_MAGIC) {
        return Ok(None);
    }

    let sections = colocated_cluster_sections(data)?;
    let sq_cluster = crate::index::quantization::sq::deserialize_sq_cluster(sections.sq)?;
    Ok(Some(sq_cluster))
}

struct ColocatedClusterSections<'a> {
    sq: &'a [u8],
    full: &'a [u8],
}

fn full_cluster_section(data: &[u8]) -> Result<&[u8]> {
    if !data.starts_with(CLUSTER_V2_MAGIC) {
        return Ok(data);
    }
    Ok(colocated_cluster_sections(data)?.full)
}

fn colocated_cluster_sections(data: &[u8]) -> Result<ColocatedClusterSections<'_>> {
    if data.len() < CLUSTER_V2_HEADER_LEN {
        return Err(ZeppelinError::Index(
            "v2 cluster blob too small for header".into(),
        ));
    }

    let sq_offset = read_u64_usize(data, 4, "v2 cluster SQ offset")?;
    let sq_len = read_u64_usize(data, 12, "v2 cluster SQ length")?;
    let full_offset = read_u64_usize(data, 20, "v2 cluster full offset")?;
    let full_len = read_u64_usize(data, 28, "v2 cluster full length")?;

    if sq_offset != CLUSTER_V2_HEADER_LEN {
        return Err(ZeppelinError::Index(format!(
            "v2 cluster SQ offset mismatch: expected {CLUSTER_V2_HEADER_LEN}, got {sq_offset}"
        )));
    }
    let expected_full_offset = sq_offset.checked_add(sq_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "v2 cluster SQ section overflows: offset={sq_offset}, len={sq_len}"
        ))
    })?;
    if full_offset != expected_full_offset {
        return Err(ZeppelinError::Index(format!(
            "v2 cluster full offset mismatch: expected {expected_full_offset}, got {full_offset}"
        )));
    }
    let expected_len = full_offset.checked_add(full_len).ok_or_else(|| {
        ZeppelinError::Index(format!(
            "v2 cluster full section overflows: offset={full_offset}, len={full_len}"
        ))
    })?;
    if data.len() != expected_len {
        return Err(ZeppelinError::Index(format!(
            "v2 cluster blob size mismatch: expected {expected_len}, got {}",
            data.len()
        )));
    }

    Ok(ColocatedClusterSections {
        sq: &data[sq_offset..full_offset],
        full: &data[full_offset..expected_len],
    })
}

fn read_u64_usize(data: &[u8], offset: usize, label: &str) -> Result<usize> {
    let value = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ZeppelinError::Index(format!("{label} parse error")))?,
    );
    usize::try_from(value)
        .map_err(|_| ZeppelinError::Index(format!("{label} does not fit in usize: {value}")))
}

/// Attributes blob: JSON-serialized `Vec<Option<HashMap<String, AttributeValue>>>`.
///
/// We use JSON rather than bincode because `AttributeValue` uses
/// `#[serde(untagged)]`, which requires `deserialize_any` -- a method
/// that bincode does not support.
pub(crate) fn serialize_attrs(attrs: &[Option<HashMap<String, AttributeValue>>]) -> Result<Bytes> {
    let encoded = serde_json::to_vec(attrs)?;
    Ok(Bytes::from(encoded))
}

/// Deserialize attributes blob.
pub(crate) fn deserialize_attrs(
    data: &[u8],
) -> Result<Vec<Option<HashMap<String, AttributeValue>>>> {
    Ok(serde_json::from_slice(data)?)
}

// ---------------------------------------------------------------------------
// Build pipeline
// ---------------------------------------------------------------------------

/// Build an IVF-Flat index from the given vectors.
///
/// 1. Train centroids via k-means++.
/// 2. Assign every vector to its nearest centroid.
/// 3. Serialize and write all artifacts to S3.
/// 4. Return an `IvfFlatIndex` handle with the metadata needed for search.
pub async fn build_ivf_flat(
    vectors: &[VectorEntry],
    config: &IndexingConfig,
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) -> Result<IvfFlatIndex> {
    if vectors.is_empty() {
        return Err(ZeppelinError::Index(
            "cannot build index from empty vector set".into(),
        ));
    }

    let dim = vectors[0].values.len();
    if dim == 0 {
        return Err(ZeppelinError::Index("vector dimension must be > 0".into()));
    }

    // Validate all dimensions match.
    for v in vectors.iter() {
        if v.values.len() != dim {
            return Err(ZeppelinError::DimensionMismatch {
                expected: dim,
                actual: v.values.len(),
            });
        }
    }

    let k = config.default_num_centroids.min(vectors.len());

    info!(
        n = vectors.len(),
        dim = dim,
        k = k,
        namespace = namespace,
        segment_id = segment_id,
        "building IVF-Flat index"
    );

    // --- Step 1: Train centroids ---
    let vec_refs: Vec<&[f32]> = vectors.iter().map(|v| v.values.as_slice()).collect();
    let centroids = train_kmeans(
        &vec_refs,
        dim,
        k,
        config.kmeans_max_iterations,
        config.kmeans_convergence_epsilon,
    )?;

    let num_clusters = centroids.len();
    info!(num_clusters = num_clusters, "k-means training complete");

    // --- Step 2: Assign vectors to clusters ---
    let mut cluster_ids: Vec<Vec<String>> = vec![Vec::new(); num_clusters];
    let mut cluster_vecs: Vec<Vec<Vec<f32>>> = vec![Vec::new(); num_clusters];
    let mut cluster_attrs: Vec<Vec<Option<HashMap<String, AttributeValue>>>> =
        vec![Vec::new(); num_clusters];

    for entry in vectors {
        let mut best_dist = f32::MAX;
        let mut best_cluster = 0usize;
        for (c, centroid) in centroids.iter().enumerate() {
            let d = distance::euclidean_distance(&entry.values, centroid);
            if d < best_dist {
                best_dist = d;
                best_cluster = c;
            }
        }
        cluster_ids[best_cluster].push(entry.id.clone());
        cluster_vecs[best_cluster].push(entry.values.clone());
        cluster_attrs[best_cluster].push(entry.attributes.clone());
    }

    for (i, ids) in cluster_ids.iter().enumerate() {
        debug!(cluster = i, count = ids.len(), "cluster assignment");
    }

    // --- Step 3: Write artifacts to S3 ---
    let quantization = config.quantization;
    let sq_calibration = if matches!(quantization, QuantizationType::Scalar) {
        Some(crate::index::quantization::sq::SqCalibration::calibrate(
            &vec_refs, dim,
        ))
    } else {
        None
    };
    let sq_calibration_bytes = sq_calibration.as_ref().map(|cal| cal.to_bytes());

    // Write centroids.
    let centroids_data = if let Some(bytes) = sq_calibration_bytes.as_ref() {
        serialize_centroids_with_sq_calibration(&centroids, dim, Some(bytes.as_ref()))?
    } else {
        serialize_centroids(&centroids, dim)?
    };
    let ckey = centroids_key(namespace, segment_id);
    store.put(&ckey, centroids_data).await?;
    debug!(key = %ckey, "wrote centroids");

    // CPU phase: pre-serialize all cluster payloads.
    let mut bitmap_fields_set = std::collections::HashSet::new();
    let mut cluster_payloads: Vec<ClusterPayload> = Vec::with_capacity(num_clusters);
    for i in 0..num_clusters {
        let cvec_data = if let Some(cal) = &sq_calibration {
            let cluster_refs: Vec<&[f32]> = cluster_vecs[i].iter().map(|v| v.as_slice()).collect();
            let codes = cal.encode_batch(&cluster_refs);
            serialize_colocated_sq_cluster(&cluster_ids[i], &cluster_vecs[i], &codes, dim)?
        } else {
            serialize_cluster(&cluster_ids[i], &cluster_vecs[i], dim)?
        };
        let cvec_key = cluster_key(namespace, segment_id, i);

        let cattr_data = serialize_attrs(&cluster_attrs[i])?;
        let cattr_key = attrs_key(namespace, segment_id, i);

        let bitmap = if config.bitmap_index {
            let attr_refs: Vec<Option<&HashMap<String, AttributeValue>>> =
                cluster_attrs[i].iter().map(|a| a.as_ref()).collect();
            let bitmap_index = crate::index::bitmap::build::build_cluster_bitmaps(&attr_refs);
            for field_name in bitmap_index.fields.keys() {
                bitmap_fields_set.insert(field_name.clone());
            }
            let bitmap_data = bitmap_index.to_bytes()?;
            let bkey = crate::index::bitmap::bitmap_key(namespace, segment_id, i);
            Some((bkey, bitmap_data))
        } else {
            None
        };

        cluster_payloads.push((cvec_key, cvec_data, cattr_key, cattr_data, bitmap));
    }
    let bitmap_fields: Vec<String> = bitmap_fields_set.into_iter().collect();

    // I/O phase: write all cluster data in parallel.
    let mut write_futs = Vec::new();
    for (cvec_key, cvec_data, cattr_key, cattr_data, bitmap) in &cluster_payloads {
        write_futs.push(store.put(cvec_key, cvec_data.clone()));
        write_futs.push(store.put(cattr_key, cattr_data.clone()));
        if let Some((bkey, bitmap_data)) = bitmap {
            write_futs.push(store.put(bkey, bitmap_data.clone()));
        }
    }
    let results = futures::future::join_all(write_futs).await;
    for result in results {
        result?;
    }
    debug!(num_clusters, "wrote all cluster data");

    // --- Step 4: Write quantized artifacts (if configured) ---
    match quantization {
        QuantizationType::Scalar => {
            info!("wrote SQ8 co-located clusters and embedded calibration");
        }
        QuantizationType::Product => {
            use crate::index::quantization::pq::{
                pq_cluster_key, pq_codebook_key, serialize_pq_cluster, PqCodebook,
            };

            let pq_m = config.pq_m;
            // Train PQ codebook on all vectors.
            let codebook = PqCodebook::train(&vec_refs, dim, pq_m, config.kmeans_max_iterations)?;
            let cb_bytes = codebook.to_bytes();
            store
                .put(&pq_codebook_key(namespace, segment_id), cb_bytes)
                .await?;
            debug!(m = pq_m, "wrote PQ codebook");

            // CPU phase: encode all clusters.
            let mut pq_payloads: Vec<(String, Bytes)> = Vec::with_capacity(num_clusters);
            for i in 0..num_clusters {
                let cluster_refs: Vec<&[f32]> =
                    cluster_vecs[i].iter().map(|v| v.as_slice()).collect();
                let codes = codebook.encode_batch(&cluster_refs);
                let pq_data = serialize_pq_cluster(&cluster_ids[i], &codes, pq_m)?;
                pq_payloads.push((pq_cluster_key(namespace, segment_id, i), pq_data));
            }

            // I/O phase: write all PQ clusters in parallel.
            let write_futs: Vec<_> = pq_payloads
                .iter()
                .map(|(key, data)| store.put(key, data.clone()))
                .collect();
            let results = futures::future::join_all(write_futs).await;
            for result in results {
                result?;
            }
            info!(m = pq_m, "wrote PQ-encoded clusters");
        }
        QuantizationType::None => {}
    }

    info!(
        namespace = namespace,
        segment_id = segment_id,
        num_vectors = vectors.len(),
        num_clusters = num_clusters,
        dim = dim,
        quantization = ?quantization,
        "IVF-Flat index build complete"
    );

    Ok(IvfFlatIndex {
        centroids,
        num_vectors: vectors.len(),
        dim,
        namespace: namespace.to_string(),
        segment_id: segment_id.to_string(),
        quantization,
        sq_calibration,
        bitmap_fields,
        // Freshly built segment: every cluster owned by this segment.
        cluster_owners: Vec::new(),
    })
}

/// Load an IVF-Flat index using pre-known metadata from the manifest.
///
/// Only fetches centroids — skips the cluster-count probe loop and
/// quantization-type detection that `load_ivf_flat` performs, saving
/// ~18 S3 GETs per query.
///
/// When `cache` is provided, the centroids blob is served through the
/// tiered cache (memory → disk → S3) and pinned for the namespace's
/// active segment: `pin_scoped` keeps it safe from LRU eviction and
/// automatically unpins the previous segment's centroids on rotation.
/// Cache errors are NOT swallowed — a failed fetch fails the load.
#[allow(clippy::too_many_arguments)]
pub async fn load_ivf_flat_from_manifest(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
    num_vectors: usize,
    quantization: QuantizationType,
    cluster_owners: Vec<String>,
    cache: Option<&std::sync::Arc<crate::cache::DiskCache>>,
) -> Result<IvfFlatIndex> {
    let ckey = centroids_key(namespace, segment_id);
    let data = match cache {
        Some(c) => {
            let data = c.get_or_fetch(&ckey, || store.get(&ckey)).await?;
            // This load path is only used for the manifest's active segment;
            // pin its centroids (unpinning the previous segment's).
            c.pin_scoped(namespace, &ckey).await;
            data
        }
        None => store.get(&ckey).await?,
    };
    let centroids_data = deserialize_centroids_data(&data)?;
    let sq_calibration = centroids_data
        .sq_calibration
        .as_ref()
        .map(|bytes| crate::index::quantization::sq::SqCalibration::from_bytes(bytes))
        .transpose()?;
    let centroids = centroids_data.centroids;
    let dim = centroids_data.dim;

    info!(
        namespace = namespace,
        segment_id = segment_id,
        num_vectors = num_vectors,
        num_clusters = centroids.len(),
        dim = dim,
        quantization = ?quantization,
        "loaded IVF-Flat index from manifest metadata"
    );

    Ok(IvfFlatIndex {
        centroids,
        num_vectors,
        dim,
        namespace: namespace.to_string(),
        segment_id: segment_id.to_string(),
        quantization,
        sq_calibration,
        bitmap_fields: Vec::new(), // Populated from SegmentRef at search time
        cluster_owners,
    })
}

/// Load an existing IVF-Flat index from S3 artifacts.
///
/// Only the centroids are loaded into memory; cluster data is fetched
/// on demand during search. Detects available quantization by probing
/// for calibration/codebook artifacts.
pub async fn load_ivf_flat(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
) -> Result<IvfFlatIndex> {
    let ckey = centroids_key(namespace, segment_id);
    let data = store.get(&ckey).await?;
    let centroids_data = deserialize_centroids_data(&data)?;
    let sq_calibration = centroids_data
        .sq_calibration
        .as_ref()
        .map(|bytes| crate::index::quantization::sq::SqCalibration::from_bytes(bytes))
        .transpose()?;
    let has_embedded_sq_calibration = sq_calibration.is_some();
    let centroids = centroids_data.centroids;
    let dim = centroids_data.dim;

    // Count total vectors by summing cluster sizes.
    let num_clusters = centroids.len();
    let mut num_vectors = 0usize;
    for i in 0..num_clusters {
        let cvec_key = cluster_key(namespace, segment_id, i);
        let cluster_data = store.get(&cvec_key).await?;
        let cluster = deserialize_cluster(&cluster_data)?;
        num_vectors += cluster.ids.len();
    }

    // Detect quantization: check for PQ codebook first, then embedded or
    // legacy SQ calibration.
    let quantization = {
        use crate::index::quantization::pq::pq_codebook_key;
        use crate::index::quantization::sq::sq_calibration_key;

        let pq_key = pq_codebook_key(namespace, segment_id);
        if store.get(&pq_key).await.is_ok() {
            QuantizationType::Product
        } else if has_embedded_sq_calibration {
            QuantizationType::Scalar
        } else {
            let sq_key = sq_calibration_key(namespace, segment_id);
            if store.get(&sq_key).await.is_ok() {
                QuantizationType::Scalar
            } else {
                QuantizationType::None
            }
        }
    };

    info!(
        namespace = namespace,
        segment_id = segment_id,
        num_vectors = num_vectors,
        num_clusters = num_clusters,
        dim = dim,
        quantization = ?quantization,
        "loaded IVF-Flat index"
    );

    Ok(IvfFlatIndex {
        centroids,
        num_vectors,
        dim,
        namespace: namespace.to_string(),
        segment_id: segment_id.to_string(),
        quantization,
        sq_calibration,
        bitmap_fields: Vec::new(), // Populated from SegmentRef at search time
        // Probing loader is used by compaction to read a segment it will fully
        // rewrite, and by tests — legacy single-segment layout.
        cluster_owners: Vec::new(),
    })
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_centroids() {
        let centroids = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        let data = serialize_centroids(&centroids, 3).unwrap();
        let (decoded, dim) = deserialize_centroids(&data).unwrap();
        assert_eq!(dim, 3);
        assert_eq!(decoded, centroids);
    }

    #[test]
    fn test_serialize_deserialize_cluster() {
        let ids = vec!["vec_1".to_string(), "vec_2".to_string()];
        let vecs = vec![vec![1.0, 2.0], vec![3.0, 4.0]];
        let data = serialize_cluster(&ids, &vecs, 2).unwrap();
        let cluster = deserialize_cluster(&data).unwrap();
        assert_eq!(cluster.ids, ids);
        assert_eq!(cluster.vectors, vecs);
    }

    #[test]
    fn test_serialize_deserialize_attrs() {
        let mut attrs_map = HashMap::new();
        attrs_map.insert(
            "color".to_string(),
            AttributeValue::String("red".to_string()),
        );
        let attrs = vec![Some(attrs_map), None];

        let data = serialize_attrs(&attrs).unwrap();
        let decoded = deserialize_attrs(&data).unwrap();
        assert_eq!(decoded.len(), 2);
        assert!(decoded[0].is_some());
        assert!(decoded[1].is_none());
    }

    #[test]
    fn test_centroids_header_too_small() {
        let data = vec![0u8; 4]; // less than 8 bytes
        assert!(deserialize_centroids(&data).is_err());
    }

    #[test]
    fn test_cluster_header_too_small() {
        let data = vec![0u8; 4];
        assert!(deserialize_cluster(&data).is_err());
    }

    #[test]
    fn test_deserialize_centroids_truncated_floats() {
        // Header says 2 centroids dim=3 but only provide 1 centroid of float data
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes()); // num_centroids = 2
        buf.extend_from_slice(&3u32.to_le_bytes()); // dim = 3
                                                    // Only provide 1 centroid worth of floats (3 floats = 12 bytes) instead of 2 (24 bytes)
        for _ in 0..3 {
            buf.extend_from_slice(&1.0f32.to_le_bytes());
        }
        let result = deserialize_centroids(&buf);
        assert!(result.is_err());
        match result.unwrap_err() {
            ZeppelinError::Index(msg) => assert!(msg.contains("mismatch"), "got: {msg}"),
            other => panic!("expected Index error, got: {other}"),
        }
    }

    #[test]
    fn test_deserialize_cluster_truncated_vector() {
        // Header says 1 vector dim=4 but truncate after 2 floats
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // n = 1
        buf.extend_from_slice(&4u32.to_le_bytes()); // dim = 4
                                                    // id
        let id = b"vec_0";
        buf.extend_from_slice(&(id.len() as u32).to_le_bytes());
        buf.extend_from_slice(id);
        // Only 2 floats instead of 4
        buf.extend_from_slice(&1.0f32.to_le_bytes());
        buf.extend_from_slice(&2.0f32.to_le_bytes());
        let result = deserialize_cluster(&buf);
        assert!(result.is_err());
        match result.unwrap_err() {
            ZeppelinError::Index(msg) => assert!(msg.contains("truncated"), "got: {msg}"),
            other => panic!("expected Index error, got: {other}"),
        }
    }
}
