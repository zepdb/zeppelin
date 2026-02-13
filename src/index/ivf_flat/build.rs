//! Build phase for IVF-Flat index.
//!
//! Pipeline: train centroids -> assign vectors to clusters -> serialize and
//! write artifacts (centroids, cluster vectors, cluster attributes) to S3.

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
/// Layout: `[num_centroids: u32][dimension: u32][f32 * num_centroids * dimension]`
pub(crate) fn serialize_centroids(centroids: &[Vec<f32>], dim: usize) -> Result<Bytes> {
    let num_centroids = centroids.len() as u32;
    let dimension = dim as u32;

    // 8 bytes header + floats
    let float_bytes = centroids.len() * dim * std::mem::size_of::<f32>();
    let total = 8 + float_bytes;
    let mut buf = Vec::with_capacity(total);

    buf.extend_from_slice(&num_centroids.to_le_bytes());
    buf.extend_from_slice(&dimension.to_le_bytes());

    for centroid in centroids {
        for &val in centroid {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }

    debug_assert_eq!(buf.len(), total);
    Ok(Bytes::from(buf))
}

/// Deserialize centroids from the binary format produced by `serialize_centroids`.
pub(crate) fn deserialize_centroids(data: &[u8]) -> Result<(Vec<Vec<f32>>, usize)> {
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

    Ok((centroids, dim))
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

/// Cluster data for a single cluster.
#[derive(Debug)]
pub(crate) struct ClusterData {
    pub ids: Vec<String>,
    pub vectors: Vec<Vec<f32>>,
}

/// Deserialize a cluster blob.
pub(crate) fn deserialize_cluster(data: &[u8]) -> Result<ClusterData> {
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

    // Write centroids.
    let centroids_data = serialize_centroids(&centroids, dim)?;
    let ckey = centroids_key(namespace, segment_id);
    store.put(&ckey, centroids_data).await?;
    debug!(key = %ckey, "wrote centroids");

    // CPU phase: pre-serialize all cluster payloads.
    let mut bitmap_fields_set = std::collections::HashSet::new();
    let mut cluster_payloads: Vec<ClusterPayload> = Vec::with_capacity(num_clusters);
    for i in 0..num_clusters {
        let cvec_data = serialize_cluster(&cluster_ids[i], &cluster_vecs[i], dim)?;
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
            use crate::index::quantization::sq::{
                serialize_sq_cluster, sq_calibration_key, sq_cluster_key, SqCalibration,
            };

            // Calibrate SQ8 on all vectors.
            let cal = SqCalibration::calibrate(&vec_refs, dim);
            let cal_bytes = cal.to_bytes();
            store
                .put(&sq_calibration_key(namespace, segment_id), cal_bytes)
                .await?;
            debug!("wrote SQ8 calibration");

            // CPU phase: encode all clusters.
            let mut sq_payloads: Vec<(String, Bytes)> = Vec::with_capacity(num_clusters);
            for i in 0..num_clusters {
                let cluster_refs: Vec<&[f32]> =
                    cluster_vecs[i].iter().map(|v| v.as_slice()).collect();
                let codes = cal.encode_batch(&cluster_refs);
                let sq_data = serialize_sq_cluster(&cluster_ids[i], &codes, dim)?;
                sq_payloads.push((sq_cluster_key(namespace, segment_id, i), sq_data));
            }

            // I/O phase: write all SQ8 clusters in parallel.
            let write_futs: Vec<_> = sq_payloads
                .iter()
                .map(|(key, data)| store.put(key, data.clone()))
                .collect();
            let results = futures::future::join_all(write_futs).await;
            for result in results {
                result?;
            }
            info!("wrote SQ8 quantized clusters");
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
        bitmap_fields,
    })
}

/// Load an IVF-Flat index using pre-known metadata from the manifest.
///
/// Only fetches centroids from S3 — skips the cluster-count probe loop
/// and quantization-type detection that `load_ivf_flat` performs, saving
/// ~18 S3 GETs per query.
pub async fn load_ivf_flat_from_manifest(
    store: &ZeppelinStore,
    namespace: &str,
    segment_id: &str,
    num_vectors: usize,
    quantization: QuantizationType,
) -> Result<IvfFlatIndex> {
    let ckey = centroids_key(namespace, segment_id);
    let data = store.get(&ckey).await?;
    let (centroids, dim) = deserialize_centroids(&data)?;

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
        bitmap_fields: Vec::new(), // Populated from SegmentRef at search time
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
    let (centroids, dim) = deserialize_centroids(&data)?;

    // Count total vectors by summing cluster sizes.
    let num_clusters = centroids.len();
    let mut num_vectors = 0usize;
    for i in 0..num_clusters {
        let cvec_key = cluster_key(namespace, segment_id, i);
        let cluster_data = store.get(&cvec_key).await?;
        if cluster_data.len() >= 8 {
            let n = u32::from_le_bytes(
                cluster_data[0..4]
                    .try_into()
                    .map_err(|_| ZeppelinError::Index("cluster count parse error".into()))?,
            ) as usize;
            num_vectors += n;
        }
    }

    // Detect quantization: check for PQ codebook first, then SQ calibration.
    let quantization = {
        use crate::index::quantization::pq::pq_codebook_key;
        use crate::index::quantization::sq::sq_calibration_key;

        let pq_key = pq_codebook_key(namespace, segment_id);
        if store.get(&pq_key).await.is_ok() {
            QuantizationType::Product
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
        bitmap_fields: Vec::new(), // Populated from SegmentRef at search time
    })
}

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
