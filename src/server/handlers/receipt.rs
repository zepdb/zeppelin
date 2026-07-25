//! Structural receipt verification and signed manifest-root inspection.
//!
//! This file is the client-facing half of verifiable retrieval. A query issues
//! a signed [`RetrievalReceipt`](crate::security::RetrievalReceipt) describing exactly which
//! immutable artifacts answered it; these two routes let a holder check that
//! receipt against Zeppelin's own authoritative state.
//!
//! It is transport only. Merkle construction, canonical JSON hashing,
//! signature verification, and the divergence ordering all live in
//! `crate::security::receipt`; receipt *issuance* belongs to
//! [`handlers::query::query_namespace`](crate::server::handlers::query::query_namespace). Nothing here decides
//! authorization, publishes a manifest, or writes to object storage.
//!
//! ## Routes
//!
//! Both are registered in `build_router` on the non-query router.
//!
//! | Method and path | Action | Handler | Resource authorized |
//! | --- | --- | --- | --- |
//! | `POST /v1/verify` | `ReceiptVerify` | [`verify`](crate::server::handlers::receipt::verify) | `Resource::System` |
//! | `GET /v1/namespaces/:ns/manifest/root` | `NamespaceRead` | [`manifest_root`](crate::server::handlers::receipt::manifest_root) | that namespace |
//!
//! `/v1/verify` is deliberately system-scoped: the receipt itself names its
//! namespace, and the verifier's own namespace grants do not widen or narrow
//! what a structural check can prove.
//!
//! ## Licensing gate
//!
//! Both paths always exist. When `ReceiptCapability::compose` finds no
//! [`Feature::Receipts`](crate::security::Feature::Receipts) in the boot-verified entitlement set,
//! each is bound instead to a stub returning `feature_not_licensed` (403), so
//! an unlicensed deployment answers 403 rather than 404. The capability is
//! resolved once at composition, so a license change requires a restart.
//!
//! ## Response meaning
//!
//! [`verify`](crate::server::handlers::receipt::verify) is the unusual one: **a receipt that fails to verify is still a
//! 200.** The body is a [`VerifyReceiptResponse`](crate::security::VerifyReceiptResponse) with `valid: false` and the
//! `first_divergence` that stopped the check. HTTP errors are reserved for
//! transport, authorization, and storage failures — never for "the evidence
//! disagreed". A client that branches on status alone will read a forged
//! receipt as success.
//!
//! Checks run in a fixed first-divergence order, so `first_divergence` names
//! the earliest failing stage rather than an arbitrary one:
//!
//! ```text
//! POST /v1/verify { receipt, results, query, refetch }
//!        |
//!        v
//! receipt signature ------------------> Signature
//! results digest ---------------------> ResultDigest
//! query hash -------------------------> QueryHash
//! touched + derived Merkle paths -----> MerklePath
//! manifest root signature ------------> ManifestRootSignature
//! retained manifest generation -------> ManifestHistory   (skipped if aged out;
//!        |                                                 manifest_history_checked=false)
//!        v
//! policy-filter component ------------> PolicyFilterHash  (Unchecked unless the
//!        |                                                 verifier holds SecurityAdminRead)
//!        v
//! refetch=true: re-GET every touched
//! object and re-hash ----------------> ArtifactRefetch
//!        |
//!        v
//! 200 { valid: true, refetched_artifacts: n }
//! ```
//!
//! The policy-filter stage is a privilege-sensitive downgrade, not a denial: an
//! unprivileged verifier receives `PolicyFilterCheck::Unchecked` and the
//! predicate is never disclosed. `refetch: true` costs one object-store GET per
//! touched and derived artifact, so it is opt-in.
//!
//! [`manifest_root`](crate::server::handlers::receipt::manifest_root) returns the signed identity of the namespace's *current*
//! manifest generation ([`ManifestRootResponse`](crate::server::handlers::receipt::ManifestRootResponse)). It reads through
//! [`Manifest::read`](crate::wal::Manifest::read), which is authoritative S3/MinIO state rather than a
//! cached view, then recomputes both the Merkle root over visible artifacts and
//! the query-routing state digest and requires each to equal the value the
//! manifest carries. A stored value that is absent or disagrees is reported as
//! [`SecurityError::ReceiptsUnavailableUnhashed`](crate::security::SecurityError::ReceiptsUnavailableUnhashed) (409
//! `receipts_unavailable_unhashed`) — it is never recomputed into the response,
//! because a self-computed root would prove nothing.
//!
//! ## Error mapping
//!
//! [`ApiError`] renders every failure through the shared canonical envelope.
//! On this surface: 401 for an unauthenticated or unknown credential, 403 for a
//! missing grant or an unlicensed receipts feature, 404 when the namespace has
//! no manifest ([`ManifestNotFound`](crate::error::ZeppelinError::ManifestNotFound)), 409 when a
//! manifest predates root hashing or fails its recomputation, 415/422 for a
//! malformed body rejected by the extractor, and 429 from the rate limiter.
//!
//! ## Authority invariants
//!
//! - **Authorization is central.** `secure_route` runs the `authenticate` and
//!   `authorize` middleware ahead of both handlers, driven by the route map.
//!   [`manifest_root`](crate::server::handlers::receipt::manifest_root) extracts no decision at all and performs no check of its
//!   own; it must stay that way rather than growing a second, divergent rule.
//! - **The manifest is the authority for what a receipt may claim.** These
//!   handlers only compare; they never repair a manifest, backfill a missing
//!   root, or synthesize a signature to make verification succeed.
//! - **Absence of evidence is reported, not assumed.** A manifest generation
//!   that has aged out of history yields `manifest_history_checked: false`
//!   rather than a silent pass.
//!
//! ## Rust concepts used here
//!
//! Both handlers are thin async functions over axum extractors: [`State`]
//! clones an [`AppState`] whose store and kernel are shared behind `Arc`s,
//! [`Extension`](axum::Extension) pulls the [`Principal`](crate::security::Principal) and [`RequestContext`](crate::security::RequestContext) the
//! authentication middleware inserted, and [`Path`](axum::extract::Path) binds `:ns`. The chained
//! `Option` combinators in [`manifest_root`](crate::server::handlers::receipt::manifest_root) —
//! `.filter(|root| root == &recomputed).ok_or(..)?` — are the load-bearing
//! idiom: the compiler makes the "value present *and* it matches" case the only
//! way to reach the success branch, so there is no path where a missing digest
//! quietly becomes a default. A Java `Optional` chain is the closest analogue,
//! but here `?` converts the `None` case into a typed HTTP error at the same
//! statement.

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
