use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::error::ZeppelinError;
use crate::server::AppState;
use crate::types::{VectorEntry, VectorId};

use super::ApiError;

/// Request body for upserting vectors into a namespace.
#[derive(Debug, Deserialize)]
pub struct UpsertVectorsRequest {
    /// Vectors to upsert (insert or update by ID).
    pub vectors: Vec<VectorEntry>,
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
    body: bytes::Bytes,
) -> Result<(StatusCode, Json<UpsertVectorsResponse>), ApiError> {
    let req: UpsertVectorsRequest = serde_json::from_slice(&body).map_err(|e| {
        ApiError(ZeppelinError::Validation(format!(
            "invalid request body: {e}"
        )))
    })?;
    if req.vectors.is_empty() {
        return Err(ApiError(ZeppelinError::Validation(
            "vectors array cannot be empty".into(),
        )));
    }
    if req.vectors.len() > state.config.server.max_batch_size {
        return Err(ApiError(ZeppelinError::Validation(format!(
            "batch size {} exceeds maximum of {}",
            req.vectors.len(),
            state.config.server.max_batch_size
        ))));
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

/// Validate a vector ID: only alphanumeric, dash, underscore, and dot allowed.
fn is_valid_vector_id(id: &str) -> bool {
    id.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
}
