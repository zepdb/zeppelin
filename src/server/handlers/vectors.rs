use std::collections::{HashMap, HashSet};
use std::fmt;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::Json;
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use tracing::{info, instrument};

use crate::error::ZeppelinError;
use crate::index::ivf_flat::build::{
    attrs_key, cluster_key, deserialize_attrs, deserialize_cluster_from_object,
};
use crate::index::ivf_flat::membership::deserialize_membership;
use crate::query;
use crate::server::AppState;
use crate::types::{AttributeValue, ConsistencyLevel, VectorEntry, VectorId};
use crate::wal::manifest::SegmentRef;
use crate::wal::Manifest;

use super::ApiError;

/// Request body for upserting vectors into a namespace.
#[derive(Debug, Deserialize)]
pub struct UpsertVectorsRequest {
    /// Vectors to upsert (insert or update by ID).
    pub vectors: Vec<VectorEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessagePackUpsertRequest {
    vectors: Option<Vec<VectorEntry>>,
    columnar: Option<ColumnarUpsertRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnarUpsertRequest {
    ids: Vec<VectorId>,
    dimensions: usize,
    #[serde(deserialize_with = "deserialize_f32_le_bytes")]
    values_f32_le: Vec<f32>,
    #[serde(default)]
    attributes: Option<Vec<Option<HashMap<String, AttributeValue>>>>,
}

/// Response body confirming the number of vectors upserted.
#[derive(Debug, Serialize)]
pub struct UpsertVectorsResponse {
    /// Number of vectors successfully upserted.
    pub upserted: usize,
}

/// Request body for deleting vectors by ID.
#[derive(Debug, Deserialize)]
pub struct DeleteVectorsRequest {
    /// IDs of vectors to delete.
    pub ids: Vec<VectorId>,
}

/// Request body for fetching vectors by ID.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetVectorsRequest {
    /// IDs to fetch, returned in request order when found.
    pub ids: Vec<VectorId>,
    /// Include vector values in each found record. Defaults to true.
    #[serde(default = "default_true")]
    pub include_vector: bool,
    /// Include attributes in each found record. Defaults to true.
    #[serde(default = "default_true")]
    pub include_attributes: bool,
    /// Optional attribute field allow-list, valid only when attributes are included.
    #[serde(default)]
    pub attribute_fields: Option<Vec<String>>,
    /// Read consistency level. Strong sees latest committed WAL writes.
    #[serde(default)]
    pub consistency: ConsistencyLevel,
}

/// Response body for fetching vectors by ID.
#[derive(Debug, Serialize)]
pub struct GetVectorsResponse {
    /// Found live records, in the same relative order as requested IDs.
    pub results: Vec<GetVectorRecord>,
    /// Requested IDs that were missing or tombstoned.
    pub missing: Vec<VectorId>,
}

/// One fetched vector record.
#[derive(Debug, Clone, Serialize)]
pub struct GetVectorRecord {
    /// Vector ID.
    pub id: VectorId,
    /// Vector values, omitted when `include_vector=false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<f32>>,
    /// Attributes, omitted when `include_attributes=false` or none are present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

#[derive(Debug, Clone, Copy)]
struct FetchProjection<'a> {
    include_vector: bool,
    include_attributes: bool,
    attribute_fields: Option<&'a [String]>,
}

fn default_true() -> bool {
    true
}

/// Upserts a batch of vectors into the specified namespace via the WAL.
///
/// Uses a raw-bytes + `serde_json` body (not axum's `Json` extractor) so
/// malformed JSON returns a 400 with the canonical error envelope, matching
/// the query handler — axum's `Json` rejection is a 422 plain-text body that
/// bypasses our envelope (Task 11 I5).
#[instrument(skip(state, body), fields(namespace = %ns))]
pub async fn upsert_vectors(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<(StatusCode, Json<UpsertVectorsResponse>), ApiError> {
    let req = parse_upsert_request(&headers, &body)?;
    if req.vectors.is_empty() {
        return Err(ApiError(ZeppelinError::Validation(
            "vectors array cannot be empty".into(),
        )));
    }
    if req.vectors.len() > state.config.server.max_batch_size {
        return Err(ApiError(ZeppelinError::PayloadTooLarge {
            resource: "upsert batch",
            actual: req.vectors.len(),
            limit: state.config.server.max_batch_size,
        }));
    }

    for vec in &req.vectors {
        if vec.id.is_empty() {
            return Err(ApiError(ZeppelinError::Validation(
                "vector id cannot be empty".into(),
            )));
        }
        if vec.id.len() > state.config.server.max_vector_id_length {
            return Err(ApiError(ZeppelinError::Validation(format!(
                "vector id length {} exceeds maximum of {}",
                vec.id.len(),
                state.config.server.max_vector_id_length
            ))));
        }
        if !is_valid_vector_id(&vec.id) {
            return Err(ApiError(ZeppelinError::Validation(format!(
                "vector id '{}' contains invalid characters; \
                 only alphanumeric, dash, underscore, and dot are allowed",
                vec.id
            ))));
        }
        // Reject NaN/inf before anything durable is written: one non-finite
        // value poisons distance orderings and k-means centroids permanently.
        if let Some((dim_idx, kind)) = super::find_non_finite(&vec.values) {
            return Err(ApiError(ZeppelinError::Validation(format!(
                "vector '{}' contains a non-finite value ({kind}) at dimension {dim_idx}",
                vec.id
            ))));
        }
    }

    info!(count = req.vectors.len(), "upserting vectors");

    // Validate namespace exists and check dimensions
    let meta = state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;

    for vec in &req.vectors {
        if vec.values.len() != meta.dimensions {
            // Name the offending vector: in a 50k-vector batch, "expected
            // 128, got 64" alone leaves the client hunting for the bad entry.
            return Err(ApiError(ZeppelinError::Validation(format!(
                "vector '{}' has dimension mismatch: expected {}, got {}",
                vec.id,
                meta.dimensions,
                vec.values.len()
            ))));
        }
    }

    let count = req.vectors.len();
    // WalWriter::append now does group commit internally (concurrent appends to
    // one namespace coalesce into a shared manifest CAS), so there is no
    // separate batch-writer path.
    let (_, manifest) = state
        .wal_writer
        .append(&ns, req.vectors, vec![])
        .await
        .map_err(ApiError::from)?;

    // Write-through: insert fresh manifest so next query skips S3 GET.
    state.manifest_cache.insert(&ns, manifest);

    info!(upserted = count, "vectors upserted");
    Ok((
        StatusCode::OK,
        Json(UpsertVectorsResponse { upserted: count }),
    ))
}

fn parse_upsert_request(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<UpsertVectorsRequest, ApiError> {
    if is_msgpack_content_type(headers) {
        parse_msgpack_upsert_request(body)
    } else {
        serde_json::from_slice(body).map_err(|e| {
            ApiError(ZeppelinError::Validation(format!(
                "invalid request body: {e}"
            )))
        })
    }
}

fn is_msgpack_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/msgpack"))
}

fn parse_msgpack_upsert_request(body: &[u8]) -> Result<UpsertVectorsRequest, ApiError> {
    let req: MessagePackUpsertRequest = rmp_serde::from_slice(body).map_err(|e| {
        ApiError(ZeppelinError::Validation(format!(
            "invalid request body: {e}"
        )))
    })?;

    match (req.vectors, req.columnar) {
        (Some(vectors), None) => Ok(UpsertVectorsRequest { vectors }),
        (None, Some(columnar)) => columnar.into_upsert_request(),
        (Some(_), Some(_)) | (None, None) => Err(ApiError(ZeppelinError::Validation(
            "msgpack upsert body must contain exactly one of vectors or columnar".into(),
        ))),
    }
}

impl ColumnarUpsertRequest {
    fn into_upsert_request(self) -> Result<UpsertVectorsRequest, ApiError> {
        if self.dimensions == 0 {
            return Err(ApiError(ZeppelinError::Validation(
                "columnar dimensions must be greater than zero".into(),
            )));
        }
        let expected_values = self.ids.len().checked_mul(self.dimensions).ok_or_else(|| {
            ApiError(ZeppelinError::Validation(
                "columnar values length overflows usize".into(),
            ))
        })?;
        if self.values_f32_le.len() != expected_values {
            return Err(ApiError(ZeppelinError::Validation(format!(
                "columnar values_f32_le contains {} floats, expected {} ({} ids * {} dimensions)",
                self.values_f32_le.len(),
                expected_values,
                self.ids.len(),
                self.dimensions
            ))));
        }
        if let Some(attributes) = self.attributes.as_ref() {
            if attributes.len() != self.ids.len() {
                return Err(ApiError(ZeppelinError::Validation(format!(
                    "columnar attributes contains {} entries, expected {}",
                    attributes.len(),
                    self.ids.len()
                ))));
            }
        }

        let attributes = self
            .attributes
            .unwrap_or_else(|| vec![None; self.ids.len()]);
        let vectors = self
            .ids
            .into_iter()
            .zip(self.values_f32_le.chunks_exact(self.dimensions))
            .zip(attributes)
            .map(|((id, values), attributes)| VectorEntry {
                id,
                values: values.to_vec(),
                attributes,
            })
            .collect();
        Ok(UpsertVectorsRequest { vectors })
    }
}

fn deserialize_f32_le_bytes<'de, D>(deserializer: D) -> Result<Vec<f32>, D::Error>
where
    D: Deserializer<'de>,
{
    struct F32LeVisitor;

    impl<'de> Visitor<'de> for F32LeVisitor {
        type Value = Vec<f32>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("little-endian f32 bytes")
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            f32_values_from_le_bytes(value)
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            f32_values_from_le_bytes(&value)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut bytes = Vec::new();
            while let Some(byte) = seq.next_element()? {
                bytes.push(byte);
            }
            f32_values_from_le_bytes(&bytes)
        }
    }

    deserializer.deserialize_bytes(F32LeVisitor)
}

fn f32_values_from_le_bytes<E>(bytes: &[u8]) -> Result<Vec<f32>, E>
where
    E: de::Error,
{
    if bytes.len() % std::mem::size_of::<f32>() != 0 {
        return Err(E::custom(format!(
            "values_f32_le byte length {} is not divisible by {}",
            bytes.len(),
            std::mem::size_of::<f32>()
        )));
    }
    Ok(bytes
        .chunks_exact(std::mem::size_of::<f32>())
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

/// Deletes vectors by ID from the specified namespace via the WAL.
#[instrument(skip(state, body), fields(namespace = %ns))]
pub async fn delete_vectors(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    body: bytes::Bytes,
) -> Result<StatusCode, ApiError> {
    let req: DeleteVectorsRequest = serde_json::from_slice(&body).map_err(|e| {
        ApiError(ZeppelinError::Validation(format!(
            "invalid request body: {e}"
        )))
    })?;
    if req.ids.is_empty() {
        return Err(ApiError(ZeppelinError::Validation(
            "ids array cannot be empty".into(),
        )));
    }

    info!(count = req.ids.len(), "deleting vectors");

    // Validate namespace exists
    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;

    let count = req.ids.len();
    // Group commit lives in WalWriter::append — no separate batch-writer path.
    let (_, manifest) = state
        .wal_writer
        .append(&ns, vec![], req.ids)
        .await
        .map_err(ApiError::from)?;

    // Write-through: insert fresh manifest so next query skips S3 GET.
    state.manifest_cache.insert(&ns, manifest);

    info!(deleted = count, "vectors deleted");
    Ok(StatusCode::NO_CONTENT)
}

/// Fetches vectors by ID with explicit projection controls.
#[instrument(skip(state, body), fields(namespace = %ns))]
pub async fn get_vectors(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    body: bytes::Bytes,
) -> Result<Json<GetVectorsResponse>, ApiError> {
    let req: GetVectorsRequest = serde_json::from_slice(&body).map_err(|e| {
        ApiError(ZeppelinError::Validation(format!(
            "invalid request body: {e}"
        )))
    })?;
    validate_get_vectors_request(&req, state.config.server.max_vector_id_length)
        .map_err(ApiError::from)?;

    state
        .namespace_manager
        .get(&ns)
        .await
        .map_err(ApiError::from)?;

    let manifest = query::read_manifest_for_query(
        &state.store,
        &ns,
        req.consistency,
        Some(&state.manifest_cache),
    )
    .await
    .map_err(ApiError::from)?;

    let projection = FetchProjection {
        include_vector: req.include_vector,
        include_attributes: req.include_attributes,
        attribute_fields: req.attribute_fields.as_deref(),
    };
    let response =
        fetch_vectors_by_id(&state, &ns, &req.ids, req.consistency, projection, manifest)
            .await
            .map_err(ApiError::from)?;

    info!(
        found = response.results.len(),
        missing = response.missing.len(),
        "vectors fetched"
    );
    Ok(Json(response))
}

fn validate_get_vectors_request(
    req: &GetVectorsRequest,
    max_vector_id_length: usize,
) -> Result<(), ZeppelinError> {
    if req.ids.is_empty() {
        return Err(ZeppelinError::Validation(
            "ids array cannot be empty".into(),
        ));
    }
    let mut seen = HashSet::with_capacity(req.ids.len());
    for id in &req.ids {
        validate_vector_id_for_request(id, max_vector_id_length)?;
        if !seen.insert(id) {
            return Err(ZeppelinError::Validation(format!(
                "duplicate vector id '{id}' in request"
            )));
        }
    }

    if !req.include_attributes && req.attribute_fields.is_some() {
        return Err(ZeppelinError::Validation(
            "attribute_fields requires include_attributes=true".into(),
        ));
    }
    if let Some(fields) = req.attribute_fields.as_ref() {
        if fields.is_empty() {
            return Err(ZeppelinError::Validation(
                "attribute_fields cannot be empty".into(),
            ));
        }
        if fields.iter().any(String::is_empty) {
            return Err(ZeppelinError::Validation(
                "attribute_fields cannot contain empty field names".into(),
            ));
        }
    }

    Ok(())
}

pub(crate) fn validate_vector_id_for_request(
    id: &str,
    max_vector_id_length: usize,
) -> Result<(), ZeppelinError> {
    if id.is_empty() {
        return Err(ZeppelinError::Validation(
            "vector id cannot be empty".into(),
        ));
    }
    if id.len() > max_vector_id_length {
        return Err(ZeppelinError::Validation(format!(
            "vector id length {} exceeds maximum of {}",
            id.len(),
            max_vector_id_length
        )));
    }
    if !is_valid_vector_id(id) {
        return Err(ZeppelinError::Validation(format!(
            "vector id '{}' contains invalid characters; \
             only alphanumeric, dash, underscore, and dot are allowed",
            id
        )));
    }
    Ok(())
}

pub(crate) async fn fetch_vector_values_by_id(
    state: &AppState,
    ns: &str,
    id: &str,
    consistency: ConsistencyLevel,
    manifest: Manifest,
) -> Result<Option<Vec<f32>>, ZeppelinError> {
    let projection = FetchProjection {
        include_vector: true,
        include_attributes: false,
        attribute_fields: None,
    };
    let response = fetch_vectors_by_id(
        state,
        ns,
        &[id.to_string()],
        consistency,
        projection,
        manifest,
    )
    .await?;
    response
        .results
        .into_iter()
        .next()
        .map(|record| {
            record.values.ok_or_else(|| {
                ZeppelinError::Index(format!("fetch by id returned no vector values for {id}"))
            })
        })
        .transpose()
}

async fn fetch_vectors_by_id(
    state: &AppState,
    ns: &str,
    ids: &[VectorId],
    consistency: ConsistencyLevel,
    projection: FetchProjection<'_>,
    manifest: Manifest,
) -> Result<GetVectorsResponse, ZeppelinError> {
    let requested: HashSet<&str> = ids.iter().map(String::as_str).collect();
    let mut found = HashMap::new();
    let mut deleted = HashSet::new();

    match consistency {
        ConsistencyLevel::Strong => {
            fetch_strong_wal_records(
                state,
                ns,
                &manifest,
                &requested,
                projection,
                &mut found,
                &mut deleted,
            )
            .await?;
        }
        ConsistencyLevel::Eventual => {
            deleted = state
                .wal_reader
                .read_delete_ids_from_refs_unchecked(
                    ns,
                    manifest.uncompacted_fragments(),
                    Some(&state.cache),
                )
                .await?;
        }
    }

    let segment_ids: Vec<VectorId> = ids
        .iter()
        .filter(|id| !found.contains_key(id.as_str()) && !deleted.contains(id.as_str()))
        .cloned()
        .collect();
    let segment_records =
        fetch_segment_records(state, ns, &manifest, &segment_ids, projection).await?;
    found.extend(segment_records);

    let mut results = Vec::new();
    let mut missing = Vec::new();
    for id in ids {
        match found.remove(id.as_str()) {
            Some(record) => results.push(record),
            None => missing.push(id.clone()),
        }
    }

    Ok(GetVectorsResponse { results, missing })
}

async fn fetch_strong_wal_records(
    state: &AppState,
    ns: &str,
    manifest: &Manifest,
    requested: &HashSet<&str>,
    projection: FetchProjection<'_>,
    found: &mut HashMap<VectorId, GetVectorRecord>,
    deleted: &mut HashSet<VectorId>,
) -> Result<(), ZeppelinError> {
    if manifest.uncompacted_fragments().is_empty() {
        return Ok(());
    }

    let fragments = state
        .wal_reader
        .read_fragments_from_refs_unchecked(
            ns,
            manifest.uncompacted_fragments(),
            Some(&state.cache),
        )
        .await?;
    for fragment in fragments {
        for id in fragment.deletes {
            if requested.contains(id.as_str()) {
                found.remove(id.as_str());
                deleted.insert(id);
            }
        }
        for vector in fragment.vectors {
            if requested.contains(vector.id.as_str()) {
                deleted.remove(vector.id.as_str());
                found.insert(vector.id.clone(), project_vector_entry(vector, projection));
            }
        }
    }
    Ok(())
}

async fn fetch_segment_records(
    state: &AppState,
    ns: &str,
    manifest: &Manifest,
    ids: &[VectorId],
    projection: FetchProjection<'_>,
) -> Result<HashMap<VectorId, GetVectorRecord>, ZeppelinError> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let Some(segment) = active_segment(manifest)? else {
        return Ok(HashMap::new());
    };
    let membership_ref = segment.membership.as_ref().ok_or_else(|| {
        ZeppelinError::Membership("fetch by id requires segment membership artifact".into())
    })?;
    let membership_bytes = state.store.get(&membership_ref.key).await?;
    let membership = deserialize_membership(&membership_bytes)?;
    if membership.cluster_count as usize != segment.cluster_count {
        return Err(ZeppelinError::Membership(format!(
            "membership cluster_count {} does not match segment cluster_count {}",
            membership.cluster_count, segment.cluster_count
        )));
    }

    let requested: HashSet<&str> = ids.iter().map(String::as_str).collect();
    let mut ids_by_cluster: HashMap<usize, Vec<VectorId>> = HashMap::new();
    for (id, cluster_idx) in membership.entries {
        if requested.contains(id.as_str()) {
            let cluster_idx = cluster_idx as usize;
            if cluster_idx >= segment.cluster_count {
                return Err(ZeppelinError::Membership(format!(
                    "membership id {id} references out-of-range cluster {cluster_idx}"
                )));
            }
            ids_by_cluster.entry(cluster_idx).or_default().push(id);
        }
    }

    if !projection.include_vector && !projection.include_attributes {
        return Ok(ids_by_cluster
            .into_values()
            .flatten()
            .map(|id| {
                (
                    id.clone(),
                    GetVectorRecord {
                        id,
                        values: None,
                        attributes: None,
                    },
                )
            })
            .collect());
    }

    let mut records = HashMap::new();
    for (cluster_idx, cluster_ids) in ids_by_cluster {
        let cluster = fetch_segment_cluster(state, ns, segment, cluster_idx).await?;
        let attrs = if projection.include_attributes {
            Some(fetch_segment_attrs(state, ns, segment, cluster_idx, cluster.ids.len()).await?)
        } else {
            None
        };

        for id in cluster_ids {
            let row_idx = cluster
                .ids
                .iter()
                .position(|candidate| candidate == &id)
                .ok_or_else(|| {
                    ZeppelinError::Index(format!(
                        "membership maps id {id} to cluster {cluster_idx}, but cluster lacks it"
                    ))
                })?;
            let raw_attrs = attrs
                .as_ref()
                .and_then(|cluster_attrs| cluster_attrs.get(row_idx).cloned().flatten());
            records.insert(
                id.clone(),
                GetVectorRecord {
                    id,
                    values: projection
                        .include_vector
                        .then(|| cluster.vectors[row_idx].clone()),
                    attributes: project_attributes(raw_attrs, projection),
                },
            );
        }
    }

    Ok(records)
}

fn active_segment(manifest: &Manifest) -> Result<Option<&SegmentRef>, ZeppelinError> {
    let Some(segment_id) = manifest.active_segment.as_ref() else {
        return Ok(None);
    };
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == *segment_id)
        .map(Some)
        .ok_or_else(|| {
            ZeppelinError::Index(format!(
                "active segment {segment_id} is missing from manifest segments"
            ))
        })
}

async fn fetch_segment_cluster(
    state: &AppState,
    ns: &str,
    segment: &SegmentRef,
    cluster_idx: usize,
) -> Result<crate::index::ivf_flat::build::ClusterData, ZeppelinError> {
    let key = segment
        .cluster_objects
        .iter()
        .find(|object| object.clusters.contains(&cluster_idx))
        .map(|object| object.key.clone())
        .unwrap_or_else(|| cluster_key(ns, segment.cluster_owner(cluster_idx), cluster_idx));
    let data = state.store.get(&key).await?;
    deserialize_cluster_from_object(&data, cluster_idx)
}

async fn fetch_segment_attrs(
    state: &AppState,
    ns: &str,
    segment: &SegmentRef,
    cluster_idx: usize,
    cluster_len: usize,
) -> Result<Vec<Option<HashMap<String, AttributeValue>>>, ZeppelinError> {
    let key = attrs_key(ns, segment.cluster_owner(cluster_idx), cluster_idx);
    let data = state.store.get(&key).await?;
    let attrs = deserialize_attrs(&data)?;
    if attrs.len() < cluster_len {
        return Err(ZeppelinError::Index(format!(
            "attrs length {} shorter than cluster {cluster_idx} vector count {cluster_len}",
            attrs.len()
        )));
    }
    Ok(attrs)
}

fn project_vector_entry(vector: VectorEntry, projection: FetchProjection<'_>) -> GetVectorRecord {
    GetVectorRecord {
        id: vector.id,
        values: projection.include_vector.then_some(vector.values),
        attributes: project_attributes(vector.attributes, projection),
    }
}

fn project_attributes(
    attrs: Option<HashMap<String, AttributeValue>>,
    projection: FetchProjection<'_>,
) -> Option<HashMap<String, AttributeValue>> {
    if !projection.include_attributes {
        return None;
    }
    let attrs = attrs?;
    let Some(fields) = projection.attribute_fields else {
        return Some(attrs);
    };
    let projected: HashMap<String, AttributeValue> = fields
        .iter()
        .filter_map(|field| {
            attrs
                .get(field)
                .cloned()
                .map(|value| (field.clone(), value))
        })
        .collect();
    (!projected.is_empty()).then_some(projected)
}

/// Validate a vector ID: only alphanumeric, dash, underscore, and dot allowed.
fn is_valid_vector_id(id: &str) -> bool {
    id.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
}
