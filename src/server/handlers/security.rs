//! Thin HTTP administration over the S3-authoritative security kernel.

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::ZeppelinError;
use crate::security::{
    Action, AllowDecision, ApiKeyId, AuditParams, Decision, FieldMask, GrantActions,
    GrantDefinition, GrantScope, KeyState, PolicyGrant, PolicyPrincipal, PolicyVersion, Principal,
    PrincipalId, PrincipalKind, ResourceRef, SecurityError, SecurityOperationError,
    WriteConstraints,
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
