//! Thin HTTP administration over the S3-authoritative security kernel.
//!
//! Every `/v1/security/*` request lands here. This file is transport only: it
//! decodes a JSON body, converts raw strings into the security newtypes,
//! invokes exactly one [`SecurityKernel`](crate::security::SecurityKernel) operation, annotates
//! the request-scoped [`AuditRequest`](crate::server::AuditRequest), and chooses a status code. It owns the
//! wire shapes (the request/response structs below) and nothing else.
//!
//! It deliberately does **not** own:
//!
//! - **Authentication or authorization.** `secure_route` in [`server`](crate::server)
//!   wraps each route with the `authenticate` then `authorize` middleware.
//!   Authorization consults the central route map
//!   ([`classify_route`](crate::security::classify_route) over
//!   [`ROUTE_ACTIONS`](crate::security::ROUTE_ACTIONS)); a route absent from that map fails
//!   closed with [`SecurityError::UnmappedRoute`](crate::security::SecurityError::UnmappedRoute) rather than defaulting to
//!   public. Adding a handler here without a route-map entry and an
//!   `api/zeppelin-api.yaml` entry breaks `tests/contract_tests.rs`.
//! - **Policy state or its publication.** The authoritative policy document
//!   lives in `_security/` on S3/MinIO. `crate::security::policy_cache`
//!   re-loads the head, re-authorizes the mutation against the exact snapshot
//!   it is about to replace, and publishes with a bounded-retry ETag CAS. This
//!   layer never touches [`storage`](crate::storage) and holds no policy of its own.
//! - **Credential material.** Key and token secrets are generated, hashed, and
//!   signed inside `crate::security`; this file only relays the one-time
//!   plaintext to the caller.
//! - **Audit delivery.** Handlers annotate; the `authorize` middleware submits
//!   the record after the response settles.
//!
//! ## Routes
//!
//! All are registered by `security_routes`, merged into the non-query router in
//! [`build_router`](crate::server::build_router). `:key_id` and `:lock_id` use axum 0.7
//! parameter syntax.
//!
//! | Method and path | [`Action`](crate::security::Action) | Handler | Success |
//! | --- | --- | --- | --- |
//! | `GET /v1/security/principals` | `SecurityAdminRead` | [`list_principals`](crate::server::handlers::security::list_principals) | 200 |
//! | `POST /v1/security/principals` | `SecurityAdminWrite` | [`create_principal`](crate::server::handlers::security::create_principal) | 201 |
//! | `GET /v1/security/keys` | `SecurityAdminRead` | [`list_keys`](crate::server::handlers::security::list_keys) | 200 |
//! | `POST /v1/security/keys` | `SecurityAdminWrite` | [`create_key`](crate::server::handlers::security::create_key) | 201 |
//! | `DELETE /v1/security/keys/:key_id` | `SecurityAdminWrite` | [`revoke_key`](crate::server::handlers::security::revoke_key) | 200 |
//! | `POST /v1/security/keys/:key_id/rotate` | `SecurityAdminWrite` | [`rotate_key`](crate::server::handlers::security::rotate_key) | 201 |
//! | `GET /v1/security/grants` | `SecurityAdminRead` | [`list_grants`](crate::server::handlers::security::list_grants) | 200 |
//! | `POST /v1/security/grants` | `SecurityAdminWrite` | [`create_grant`](crate::server::handlers::security::create_grant) | 201 |
//! | `DELETE /v1/security/grants` | `SecurityAdminWrite` | [`delete_grant`](crate::server::handlers::security::delete_grant) | 200 |
//! | `GET /v1/security/policy` | `SecurityAdminRead` | [`get_policy`](crate::server::handlers::security::get_policy) | 200 |
//! | `POST /v1/security/tokens` | `CredentialDelegate` | [`mint_token`](crate::server::handlers::security::mint_token) | 201 |
//! | `GET /v1/security/preservation` | `PreservationAdmin` | [`list_preservation_locks`](crate::server::handlers::security::list_preservation_locks) | 200 |
//! | `POST /v1/security/preservation` | `PreservationAdmin` | [`create_preservation_lock`](crate::server::handlers::security::create_preservation_lock) | 201 |
//! | `POST /v1/security/preservation/:lock_id/release` | `PreservationRelease` | [`release_preservation_lock`](crate::server::handlers::security::release_preservation_lock) | 200 |
//!
//! `DELETE /v1/security/grants` carries a JSON body ([`GrantRemovalRequest`](crate::server::handlers::security::GrantRemovalRequest)),
//! because a grant is identified by its principal/scope/actions binding rather
//! than by a path-addressable ID.
//!
//! ## Feature availability
//!
//! The paths always exist and always bind these handlers; only the composed
//! services behind them change, so a deployment with a surface disabled by
//! configuration returns 403 rather than 404. RBAC administration requires
//! the policy authority (`security.rbac = true`), `/v1/security/tokens`
//! requires a composed delegation authority (a configured
//! `security.token_signing_key_path`), and the preservation paths require the
//! composed preservation service. When a surface is not composed the kernel
//! rejects with [`SecurityError::FeatureDisabled`](crate::security::SecurityError::FeatureDisabled) (403 `feature_disabled`).
//!
//! ## Validation here versus in the kernel
//!
//! This layer performs only syntactic validation:
//!
//! - `#[serde(deny_unknown_fields)]` on every request body. An unknown or
//!   ill-typed field is rejected by the axum extractor before the handler runs
//!   (415/422 per `tests/contract_tests.rs`), never silently ignored.
//! - Newtype parsing through `parse_principal_id`, `parse_api_key_id`, and
//!   [`NamespaceId::new`](crate::security::NamespaceId::new), each mapped to
//!   [`SecurityError::InvalidPolicyRequest`](crate::security::SecurityError::InvalidPolicyRequest) (400 `invalid_security_request`),
//!   and through [`PreservationLockId::new`](crate::security::PreservationLockId::new), which surfaces
//!   [`SecurityError::InvalidPreservationRequest`](crate::security::SecurityError::InvalidPreservationRequest) (400
//!   `invalid_preservation_request`).
//!
//! Everything semantic belongs to the kernel and is *not* duplicated here:
//! whether the actor may administer policy, whether the principal exists,
//! whether a delegated narrowing stays inside its parent's authority, whether
//! a key is already revoked, and whether the CAS publication won.
//!
//! ## Error mapping
//!
//! Handlers return [`ApiError`], which renders through the shared canonical
//! envelope; the status comes from `SecurityError::status_code`. The mapping
//! that matters on this surface:
//!
//! | Condition | Status | Code |
//! | --- | --- | --- |
//! | Unauthenticated or unknown/expired credential | 401 | authn code |
//! | Not granted, disabled feature, approval missing | 403 | `forbidden`, `feature_disabled`, `approval_required` |
//! | Stale policy cache (`SecurityStale`) | 403 | fail-closed, never served stale |
//! | Malformed ID, delegation exceeding parent, already-revoked key | 400 | `invalid_security_request`, `delegation_scope_exceeds_parent` |
//! | Unknown principal, key, grant, or lock | 404 | `security_entity_not_found`, `preservation_lock_not_found` |
//! | Duplicate entity, exhausted CAS retries, lock conflict | 409 | `security_entity_exists`, `security_conflict`, `preservation_conflict` |
//! | Preservation state could not be refreshed | 503 | `preservation_state_unavailable` |
//!
//! Retrying a successful mutation is **not** safe: there is no request-ID
//! dedup, each success publishes a new [`PolicyVersion`](crate::security::PolicyVersion), and the minting
//! routes return fresh secret material every time. Repeating a completed
//! mutation surfaces the domain's own answer instead — 409 for a duplicate
//! principal or grant, 404 for an already-removed grant, 400 for an
//! already-revoked key.
//!
//! ## Request path
//!
//! ```text
//! POST /v1/security/...
//!        |
//!        v
//! authenticate  -- no/!bad credential --> 401
//!        |
//!        v
//! authorize (route map -> kernel decision)
//!        |  |-- deny ---------------------> 403 + audited denial
//!        |  |-- Approval obligation -------> second approver header or 403
//!        |  inserts AllowDecision + AuditRequest extensions
//!        v
//! this handler: decode JSON, parse newtypes -- invalid --> 400
//!        |
//!        v
//! SecurityKernel op: re-authorize against the freshly loaded
//! policy head, then ETag-CAS publish to S3/MinIO
//!        |  |-- CAS lost after bounded retries --> 409 security_conflict
//!        v
//! write new snapshot through to this node's cache; annotate AuditRequest
//!        |
//!        v
//! authorize middleware submits the audit record with the response
//! ```
//!
//! ## Authority invariants
//!
//! - **Authorization is central, and this file must never be the only place a
//!   check exists.** Each handler binds `Extension<AllowDecision>` as
//!   `_decision` purely as proof that the `authorize` middleware ran: if the
//!   extension is absent the request fails before any domain code executes.
//!   The kernel then authorizes again against the snapshot it is about to read
//!   or replace, so a bypassed middleware cannot mutate policy.
//! - **Preservation release is always two-person.** The obligation is attached
//!   by the middleware outside persisted grants, so no administrator can mint a
//!   one-person release grant. [`release_preservation_lock`](crate::server::handlers::security::release_preservation_lock) must not acquire
//!   its own approval semantics.
//! - **Feature availability belongs in the kernel, not the router.**
//!   Delegation and preservation operations check their composed service
//!   inside the kernel, and RBAC administration rejects on the bootstrap
//!   authority arm with [`SecurityError::FeatureDisabled`](crate::security::SecurityError::FeatureDisabled) (403). Prefer a
//!   kernel-side check to relying on route selection — see
//!   `src/security/CLAUDE.md`.
//! - **Secrets are returned exactly once.** [`CreateKeyResponse`](crate::server::handlers::security::CreateKeyResponse),
//!   [`RotateKeyResponse`](crate::server::handlers::security::RotateKeyResponse), and [`MintTokenResponse`](crate::server::handlers::security::MintTokenResponse) carry plaintext material
//!   that is never retrievable again; the listing views ([`PolicyKeyView`](crate::server::handlers::security::PolicyKeyView)) are
//!   redacted and expose no digest.
//! - **Failure still audits.** `security_operation_api_error` unpacks the
//!   [`Decision`](crate::security::Decision) the kernel attached to a [`SecurityOperationError`](crate::security::SecurityOperationError) and
//!   records it, so a denial or post-allow failure is audited with its real
//!   decision instead of a synthesized one.
//!
//! ## Rust concepts used here
//!
//! Handler parameters are axum *extractors*: [`State`] hands over a cloned
//! [`AppState`] (whose [`SecurityKernel`](crate::security::SecurityKernel) is shared through an
//! `Arc`, so the clone bumps a refcount rather than copying the kernel),
//! [`Extension`](axum::Extension) pulls typed values the middleware inserted into the request,
//! [`Path`](axum::extract::Path) binds URL segments, and [`Json`] owns the decoded body. Java's
//! nearest analogue is annotation-driven argument binding, but here each
//! extractor is a trait implementation checked at compile time: forgetting the
//! authorization middleware for a route that extracts `Extension<AllowDecision>`
//! is a runtime rejection, not a silently missing check.
//!
//! [`AuditRequest`](crate::server::AuditRequest) is `Arc<Mutex<..>>` inside, so the handler and the
//! surrounding middleware annotate one shared record; the mutex is never held
//! across an `.await`. Errors travel as [`ApiError`], a newtype whose
//! `IntoResponse` implementation is the single place a domain error becomes an
//! HTTP status — the C analogue would be a global errno-to-status table, except
//! the compiler forces every fallible handler through it.

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::ZeppelinError;
use crate::security::{
    Action, AllowDecision, ApiKeyId, AuditParams, Decision, DelegationNarrowing, FieldMask,
    GrantActions, GrantDefinition, GrantScope, KeyState, NamespaceId, PolicyGrant, PolicyPrincipal,
    PolicyVersion, PreservationLockId, PreservationLockRecord, Principal, PrincipalId,
    PrincipalKind, ResourceRef, SecurityError, SecurityOperationError, WriteConstraints,
};
use crate::server::{AppState, AuditRequest};
use crate::types::Filter;

use super::ApiError;

fn security_operation_api_error(
    audit: &AuditRequest,
    action: Action,
    error: SecurityOperationError,
) -> ApiError {
    let (decision, error) = error.into_parts();
    match decision {
        Some(Decision::Allow(allow)) => {
            audit.set_allow(action, ResourceRef::SecurityPolicy, *allow)
        }
        Some(Decision::Deny(deny)) => audit.set_deny(action, ResourceRef::SecurityPolicy, deny),
        None => {}
    }
    ApiError(error)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Body accepted when creating one stable principal.
pub struct CreatePrincipalRequest {
    principal_id: String,
    kind: PrincipalKind,
    display_name: String,
}

#[derive(Serialize)]
/// Newly published principal and its authoritative policy version.
pub struct CreatePrincipalResponse {
    policy_version: PolicyVersion,
    principal: PolicyPrincipal,
}

#[derive(Serialize)]
/// Redacted principal inventory from one policy snapshot.
pub struct ListPrincipalsResponse {
    policy_version: PolicyVersion,
    principals: Vec<PolicyPrincipal>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Body accepted when issuing a new API key for an existing principal.
pub struct CreateKeyRequest {
    principal_id: String,
    name: String,
    #[serde(default)]
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
/// One-time plaintext API-key response paired with its persisted key ID.
pub struct CreateKeyResponse {
    policy_version: PolicyVersion,
    key_id: ApiKeyId,
    api_key: String,
}

#[derive(Serialize)]
/// Successful key revocation and the snapshot version that enforces it.
pub struct RevokeKeyResponse {
    policy_version: PolicyVersion,
    key_id: ApiKeyId,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Body accepted when rotating a credential with an optional overlap window.
pub struct RotateKeyRequest {
    #[serde(default)]
    overlap_secs: u64,
}

#[derive(Serialize)]
/// One-time replacement credential and its immutable lineage.
pub struct RotateKeyResponse {
    policy_version: PolicyVersion,
    key_id: ApiKeyId,
    api_key: String,
    rotated_from: ApiKeyId,
}

#[derive(Serialize)]
/// Redacted credential inventory from one policy snapshot.
pub struct ListKeysResponse {
    policy_version: PolicyVersion,
    keys: Vec<PolicyKeyView>,
}

#[derive(Serialize)]
/// Credential metadata safe for repeated administrative reads.
pub struct PolicyKeyView {
    key_id: String,
    name: String,
    principal_id: String,
    state: KeyState,
    expires_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    rotated_from: Option<String>,
    revokes_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
/// Fresh active preservation-lock inventory.
pub struct ListPreservationLocksResponse {
    locks: Vec<PreservationLockRecord>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Exact principal, scope, and actions for one grant mutation.
pub struct GrantMutationRequest {
    principal_id: String,
    scope: GrantScope,
    actions: GrantActions,
    #[serde(default)]
    mandatory_filter: Option<Filter>,
    #[serde(default)]
    field_mask: Option<FieldMask>,
    #[serde(default)]
    write_constraints: WriteConstraints,
    #[serde(default)]
    require_approval: Vec<Action>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Binding-only request for removing a grant regardless of its constraints.
pub struct GrantRemovalRequest {
    principal_id: String,
    scope: GrantScope,
    actions: GrantActions,
}

#[derive(Serialize)]
/// Newly published or removed grant and its policy version.
pub struct GrantMutationResponse {
    policy_version: PolicyVersion,
    grant: PolicyGrant,
}

#[derive(Serialize)]
/// Complete redaction-safe grant inventory from one policy snapshot.
pub struct ListGrantsResponse {
    policy_version: PolicyVersion,
    grants: Vec<PolicyGrant>,
}

#[derive(Serialize)]
/// Active policy head and bounded redaction-safe snapshot metadata.
pub struct PolicyMetadataResponse {
    policy_version: PolicyVersion,
    object_key: String,
    checksum: String,
    created_at: DateTime<Utc>,
    created_by: String,
    principals: usize,
    keys: usize,
    grants: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Strict narrowing and lifetime requested for one delegated credential.
pub struct MintTokenRequest {
    actions: Vec<Action>,
    namespaces: Vec<String>,
    #[serde(default)]
    mandatory_filter: Option<Filter>,
    purpose: String,
    expires_in_secs: u64,
}

#[derive(Serialize)]
/// One-time delegated bearer response.
pub struct MintTokenResponse {
    policy_version: PolicyVersion,
    token_id: String,
    token: String,
    expires_at: DateTime<Utc>,
}

/// Mint one parent-attributed credential after the kernel proves narrowing.
pub async fn mint_token(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
    Json(request): Json<MintTokenRequest>,
) -> Result<(StatusCode, Json<MintTokenResponse>), ApiError> {
    let purpose = request.purpose.clone();
    audit.set_params(AuditParams::DelegationMint {
        token_id: None,
        parent_principal: principal.id.clone(),
        purpose: purpose.clone(),
    });
    let namespaces = request
        .namespaces
        .into_iter()
        .map(NamespaceId::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            ApiError(ZeppelinError::from(SecurityError::InvalidPolicyRequest(
                "invalid delegated namespace".to_string(),
            )))
        })?;
    let narrowed = DelegationNarrowing::new(
        request.actions,
        namespaces,
        request.mandatory_filter,
        request.purpose,
    )
    .map_err(|error| ApiError(error.into()))?;
    let (issued, authorization) = state
        .security
        .mint_delegated_token(
            &principal,
            narrowed,
            request.expires_in_secs,
            state.clock.now(),
        )
        .map_err(|error| security_operation_api_error(&audit, Action::CredentialDelegate, error))?;
    let policy_version = authorization.policy_version;
    audit.set_allow(
        Action::CredentialDelegate,
        ResourceRef::SecurityPolicy,
        authorization,
    );
    audit.set_params(AuditParams::DelegationMint {
        token_id: Some(issued.token_id().to_string()),
        parent_principal: principal.id.clone(),
        purpose,
    });
    Ok((
        StatusCode::CREATED,
        Json(MintTokenResponse {
            policy_version,
            token_id: issued.token_id().to_string(),
            token: issued.token().to_string(),
            expires_at: issued.expires_at(),
        }),
    ))
}

/// CAS-publish a new principal and durably audit the version transition.
pub async fn create_principal(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
    Json(request): Json<CreatePrincipalRequest>,
) -> Result<(StatusCode, Json<CreatePrincipalResponse>), ApiError> {
    let principal_id = PrincipalId::new(request.principal_id).map_err(|_| {
        ApiError(ZeppelinError::from(SecurityError::InvalidPolicyRequest(
            "invalid principal_id".to_string(),
        )))
    })?;
    let (authorization, new_version, created) = state
        .security
        .create_principal(&principal, principal_id, request.kind, request.display_name)
        .await
        .map_err(|error| security_operation_api_error(&audit, Action::SecurityAdminWrite, error))?;
    let old_version = authorization.policy_version;
    audit.set_allow(
        Action::SecurityAdminWrite,
        ResourceRef::SecurityPolicy,
        authorization,
    );
    audit.set_params(AuditParams::SecurityPolicyChange {
        old_version,
        new_version,
    });
    Ok((
        StatusCode::CREATED,
        Json(CreatePrincipalResponse {
            policy_version: new_version,
            principal: created,
        }),
    ))
}

/// Return every principal without exposing any credential material.
pub async fn list_principals(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
) -> Result<Json<ListPrincipalsResponse>, ApiError> {
    let (authorization, snapshot) = state
        .security
        .policy_snapshot(&principal, state.clock.now())
        .map_err(|error| security_operation_api_error(&audit, Action::SecurityAdminRead, error))?;
    audit.set_allow(
        Action::SecurityAdminRead,
        ResourceRef::SecurityPolicy,
        authorization,
    );
    audit.set_params(AuditParams::SecurityPolicyRead {
        version: snapshot.version(),
    });
    Ok(Json(ListPrincipalsResponse {
        policy_version: snapshot.version(),
        principals: snapshot.principals().to_vec(),
    }))
}

/// Generate an API-key secret, persist only its digest, and return it once.
pub async fn create_key(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
    Json(request): Json<CreateKeyRequest>,
) -> Result<(StatusCode, Json<CreateKeyResponse>), ApiError> {
    let principal_id = PrincipalId::new(request.principal_id).map_err(|_| {
        ApiError(ZeppelinError::from(SecurityError::InvalidPolicyRequest(
            "invalid principal_id".to_string(),
        )))
    })?;
    let issued = state
        .security
        .create_key(&principal, principal_id, request.name, request.expires_at)
        .await
        .map_err(|error| security_operation_api_error(&audit, Action::SecurityAdminWrite, error))?;
    let old_version = issued.authorization().policy_version;
    audit.set_allow(
        Action::SecurityAdminWrite,
        ResourceRef::SecurityPolicy,
        issued.authorization().clone(),
    );
    audit.set_params(AuditParams::SecurityPolicyChange {
        old_version,
        new_version: issued.policy_version(),
    });
    Ok((
        StatusCode::CREATED,
        Json(CreateKeyResponse {
            policy_version: issued.policy_version(),
            key_id: issued.key_id().clone(),
            api_key: issued.api_key().to_string(),
        }),
    ))
}

/// Return redacted key lifecycle metadata without digests or secrets.
pub async fn list_keys(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
) -> Result<Json<ListKeysResponse>, ApiError> {
    let (authorization, snapshot) = state
        .security
        .policy_snapshot(&principal, state.clock.now())
        .map_err(|error| security_operation_api_error(&audit, Action::SecurityAdminRead, error))?;
    audit.set_allow(
        Action::SecurityAdminRead,
        ResourceRef::SecurityPolicy,
        authorization,
    );
    audit.set_params(AuditParams::SecurityPolicyRead {
        version: snapshot.version(),
    });
    let keys = snapshot
        .keys()
        .iter()
        .map(|key| PolicyKeyView {
            key_id: key.key_id().as_str().to_string(),
            name: key.name().to_string(),
            principal_id: key.principal_id().as_str().to_string(),
            state: key.state(),
            expires_at: key.expires_at(),
            created_at: key.created_at(),
            rotated_from: key.rotated_from().map(|key_id| key_id.as_str().to_string()),
            revokes_at: key.revokes_at(),
        })
        .collect();
    Ok(Json(ListKeysResponse {
        policy_version: snapshot.version(),
        keys,
    }))
}

/// Revoke one API key and write the new policy through to this node immediately.
pub async fn revoke_key(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
    Path(key_id): Path<String>,
) -> Result<Json<RevokeKeyResponse>, ApiError> {
    let key_id = parse_api_key_id(key_id)?;
    let (authorization, new_version) = state
        .security
        .revoke_key(&principal, &key_id)
        .await
        .map_err(|error| security_operation_api_error(&audit, Action::SecurityAdminWrite, error))?;
    let old_version = authorization.policy_version;
    audit.set_allow(
        Action::SecurityAdminWrite,
        ResourceRef::SecurityPolicy,
        authorization,
    );
    audit.set_params(AuditParams::SecurityPolicyChange {
        old_version,
        new_version,
    });
    Ok(Json(RevokeKeyResponse {
        policy_version: new_version,
        key_id,
    }))
}

/// Atomically issue a replacement key and schedule predecessor revocation.
pub async fn rotate_key(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
    Path(key_id): Path<String>,
    Json(request): Json<RotateKeyRequest>,
) -> Result<(StatusCode, Json<RotateKeyResponse>), ApiError> {
    let rotated_from = parse_api_key_id(key_id)?;
    let issued = state
        .security
        .rotate_key(&principal, &rotated_from, request.overlap_secs)
        .await
        .map_err(|error| security_operation_api_error(&audit, Action::SecurityAdminWrite, error))?;
    let old_version = issued.authorization().policy_version;
    audit.set_allow(
        Action::SecurityAdminWrite,
        ResourceRef::SecurityPolicy,
        issued.authorization().clone(),
    );
    audit.set_params(AuditParams::SecurityPolicyChange {
        old_version,
        new_version: issued.policy_version(),
    });
    Ok((
        StatusCode::CREATED,
        Json(RotateKeyResponse {
            policy_version: issued.policy_version(),
            key_id: issued.key_id().clone(),
            api_key: issued.api_key().to_string(),
            rotated_from,
        }),
    ))
}

fn parse_api_key_id(value: String) -> Result<ApiKeyId, ApiError> {
    ApiKeyId::new(value).map_err(|_| {
        ApiError(ZeppelinError::from(SecurityError::InvalidPolicyRequest(
            "invalid key_id".to_string(),
        )))
    })
}

/// Return every grant from one atomically authorized policy snapshot.
pub async fn list_grants(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
) -> Result<Json<ListGrantsResponse>, ApiError> {
    let (authorization, snapshot) = state
        .security
        .policy_snapshot(&principal, state.clock.now())
        .map_err(|error| security_operation_api_error(&audit, Action::SecurityAdminRead, error))?;
    audit.set_allow(
        Action::SecurityAdminRead,
        ResourceRef::SecurityPolicy,
        authorization,
    );
    audit.set_params(AuditParams::SecurityPolicyRead {
        version: snapshot.version(),
    });
    Ok(Json(ListGrantsResponse {
        policy_version: snapshot.version(),
        grants: snapshot.grants().to_vec(),
    }))
}

/// CAS-publish one exact namespace or global grant.
pub async fn create_grant(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
    Json(request): Json<GrantMutationRequest>,
) -> Result<(StatusCode, Json<GrantMutationResponse>), ApiError> {
    let principal_id = parse_principal_id(request.principal_id)?;
    let definition = GrantDefinition::new(
        principal_id,
        request.scope,
        request.actions,
        request.mandatory_filter,
        request.field_mask,
        request.write_constraints,
        request.require_approval,
    );
    let (authorization, new_version, grant) = state
        .security
        .add_grant(&principal, definition)
        .await
        .map_err(|error| security_operation_api_error(&audit, Action::SecurityAdminWrite, error))?;
    let old_version = authorization.policy_version;
    audit.set_allow(
        Action::SecurityAdminWrite,
        ResourceRef::SecurityPolicy,
        authorization,
    );
    audit.set_params(AuditParams::SecurityPolicyChange {
        old_version,
        new_version,
    });
    Ok((
        StatusCode::CREATED,
        Json(GrantMutationResponse {
            policy_version: new_version,
            grant,
        }),
    ))
}

/// CAS-remove one exact namespace or global grant.
pub async fn delete_grant(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
    Json(request): Json<GrantRemovalRequest>,
) -> Result<Json<GrantMutationResponse>, ApiError> {
    let principal_id = parse_principal_id(request.principal_id)?;
    let (authorization, new_version, grant) = state
        .security
        .remove_grant(&principal, principal_id, request.scope, request.actions)
        .await
        .map_err(|error| security_operation_api_error(&audit, Action::SecurityAdminWrite, error))?;
    let old_version = authorization.policy_version;
    audit.set_allow(
        Action::SecurityAdminWrite,
        ResourceRef::SecurityPolicy,
        authorization,
    );
    audit.set_params(AuditParams::SecurityPolicyChange {
        old_version,
        new_version,
    });
    Ok(Json(GrantMutationResponse {
        policy_version: new_version,
        grant,
    }))
}

fn parse_principal_id(value: String) -> Result<PrincipalId, ApiError> {
    PrincipalId::new(value).map_err(|_| {
        ApiError(ZeppelinError::from(SecurityError::InvalidPolicyRequest(
            "invalid principal_id".to_string(),
        )))
    })
}

/// Return the atomically authorized policy head and active snapshot metadata.
pub async fn get_policy(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
) -> Result<Json<PolicyMetadataResponse>, ApiError> {
    let (authorization, head, snapshot) = state
        .security
        .policy_view(&principal, state.clock.now())
        .map_err(|error| security_operation_api_error(&audit, Action::SecurityAdminRead, error))?;
    audit.set_allow(
        Action::SecurityAdminRead,
        ResourceRef::SecurityPolicy,
        authorization,
    );
    audit.set_params(AuditParams::SecurityPolicyRead {
        version: snapshot.version(),
    });
    Ok(Json(PolicyMetadataResponse {
        policy_version: head.version(),
        object_key: head.object_key().to_string(),
        checksum: head.checksum().to_string(),
        created_at: snapshot.created_at(),
        created_by: snapshot.created_by().as_str().to_string(),
        principals: snapshot.principals().len(),
        keys: snapshot.keys().len(),
        grants: snapshot.grants().len(),
    }))
}

/// Return the fresh S3-authoritative active preservation-lock inventory.
pub async fn list_preservation_locks(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
) -> Result<Json<ListPreservationLocksResponse>, ApiError> {
    let locks = state
        .security
        .active_preservation_locks()
        .map_err(|error| ApiError(error.into()))?;
    Ok(Json(ListPreservationLocksResponse { locks }))
}

/// Create one immutable preservation lock and CAS-publish it as active.
pub async fn create_preservation_lock(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
    Json(request): Json<crate::security::CreatePreservationLock>,
) -> Result<(StatusCode, Json<PreservationLockRecord>), ApiError> {
    let record = state
        .security
        .create_preservation_lock(principal.id, request)
        .await
        .map_err(ApiError::from)?;
    audit.set_params(AuditParams::PreservationCreate {
        lock_id: record.lock_id.as_str().to_string(),
    });
    Ok((StatusCode::CREATED, Json(record)))
}

/// Release one active lock after middleware proves a distinct approver.
pub async fn release_preservation_lock(
    State(state): State<AppState>,
    Extension(_decision): Extension<AllowDecision>,
    Extension(principal): Extension<Principal>,
    Extension(audit): Extension<AuditRequest>,
    Path(lock_id): Path<String>,
) -> Result<Json<PreservationLockRecord>, ApiError> {
    let lock_id = PreservationLockId::new(lock_id).map_err(|error| ApiError(error.into()))?;
    let record = state
        .security
        .release_preservation_lock(&lock_id, principal.id)
        .await
        .map_err(ApiError::from)?;
    audit.set_params(AuditParams::PreservationRelease {
        lock_id: lock_id.as_str().to_string(),
    });
    Ok(Json(record))
}
