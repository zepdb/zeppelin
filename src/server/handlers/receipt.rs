//! Structural receipt verification and signed manifest-root inspection.

use axum::extract::{Path, State};
use axum::Extension;
use axum::Json;
use serde::Serialize;

use crate::security::{
    MerkleTree, Principal, RequestContext, SecurityError, VerifyReceiptRequest,
    VerifyReceiptResponse,
};
use crate::server::AppState;
use crate::wal::manifest::ReceiptBindingVersion;
use crate::wal::Manifest;

use super::ApiError;

/// Signed identity of one current manifest generation.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestRootResponse {
    /// Namespace whose live manifest was read.
    pub namespace: String,
    /// Monotonic manifest generation.
    pub manifest_version: u64,
    /// Merkle root over all visible immutable artifacts.
    pub merkle_root: [u8; 32],
    /// Digest of the exact query-routing manifest projection.
    pub manifest_state_digest: [u8; 32],
    /// Stable projection version used to compute `manifest_state_digest`.
    pub manifest_binding_version: ReceiptBindingVersion,
    /// Node signer that published this generation.
    pub signer_node: String,
    /// Ed25519 signature over root, execution-state digest, generation, and fencing token.
    pub signature: Vec<u8>,
    /// Fencing generation included in the signed payload.
    pub fencing_token: u64,
}

/// Verify a receipt against caller-supplied results and optional authoritative bytes.
pub async fn verify(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<VerifyReceiptRequest>,
) -> Result<Json<VerifyReceiptResponse>, ApiError> {
    crate::security::verify_receipt(
        &state.store,
        &state.security,
        &principal,
        &context,
        &request,
    )
    .await
    .map(Json)
    .map_err(ApiError::from)
}

/// Read the current signed Merkle root for one namespace.
pub async fn manifest_root(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
) -> Result<Json<ManifestRootResponse>, ApiError> {
    let manifest = Manifest::read(&state.store, &namespace)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError(crate::error::ZeppelinError::ManifestNotFound {
                namespace: namespace.clone(),
            })
        })?;
    let artifacts = manifest
        .receipt_artifacts(&namespace)
        .map_err(|error| ApiError(error.into()))?;
    let recomputed_root = MerkleTree::build(artifacts)
        .map_err(|error| ApiError(error.into()))?
        .root();
    let merkle_root = manifest
        .merkle_root()
        .filter(|root| root == &recomputed_root)
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)
        .map_err(|error| ApiError(error.into()))?;
    let recomputed_state_digest = manifest
        .recompute_receipt_state_digest(&namespace)
        .map_err(ApiError::from)?;
    let manifest_state_digest = manifest
        .receipt_state_digest()
        .filter(|digest| digest == &recomputed_state_digest)
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)
        .map_err(|error| ApiError(error.into()))?;
    let manifest_binding_version = manifest
        .receipt_binding_version()
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)
        .map_err(|error| ApiError(error.into()))?;
    let signer_node = manifest
        .root_signer_node()
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)
        .map_err(|error| ApiError(error.into()))?
        .to_string();
    let signature = manifest
        .root_signature()
        .ok_or(SecurityError::ReceiptsUnavailableUnhashed)
        .map_err(|error| ApiError(error.into()))?
        .to_vec();
    Ok(Json(ManifestRootResponse {
        namespace,
        manifest_version: manifest.version(),
        merkle_root,
        manifest_state_digest,
        manifest_binding_version,
        signer_node,
        signature,
        fencing_token: manifest.fencing_token(),
    }))
}
