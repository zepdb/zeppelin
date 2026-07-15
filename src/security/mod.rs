//! Central authentication, authorization, and decision vocabulary.
//!
//! Transport adapters resolve credentials to typed principals. The security
//! kernel then evaluates one exhaustive [`Action`] against a typed resource and
//! returns an explicit decision before domain work can begin.

mod action;
mod audit;
mod audit_sink;
mod authn;
mod constraints;
mod context;
mod decision;
mod entitlements;
mod kernel;
mod license;
mod policy;
mod policy_cache;
mod policy_store;
mod principal;
mod resource;
mod route_map;

pub use action::Action;
pub use audit::{
    AuditOutcome, AuditParams, AuditRecord, AuditedVectorIds, IndexConfigValues, ResourceRef,
    RuntimeConfigValues, MAX_AUDITED_VECTOR_IDS,
};
pub use audit_sink::{AuditClient, AuditRuntime, AuditSinkError};
pub use authn::{ApiKeyAdapter, AuthenticationOutcome, AuthnFailure, CredentialAdapter};
pub(crate) use constraints::filter_matches_write_scope;
pub use constraints::{apply_field_mask, filter_references_denied_field};
pub use context::RequestContext;
pub(crate) use decision::CursorBindingKey;
pub use decision::{
    AllowDecision, Decision, DecisionId, DenyDecision, DenyReason, FieldMask, Obligation,
    PolicyVersion, WriteConstraints,
};
pub use entitlements::{CustomerId, EntitlementLimits, EntitlementSource, Entitlements, Feature};
pub use kernel::SecurityKernel;
#[cfg(feature = "managed")]
pub use license::ControlPlaneResolver;
pub use license::{
    canonical_payload_bytes, read_key_file, validate_license_payload, verify_signed_license_bytes,
    EntitlementResolver, FileLicenseResolver, LicenseError, LicenseLimits, LicensePayload,
    SignedLicense, LICENSE_PUBKEY,
};
pub use policy::{
    ApiKeyId, GrantActions, GrantDefinition, GrantScope, IssuedApiKey, KeyState, PolicyGrant,
    PolicyHead, PolicyKey, PolicyPrincipal, PolicySnapshot,
};
pub use policy_store::{LoadedPolicy, PolicyStore};
pub use principal::{AuthStrength, Principal, PrincipalId, PrincipalKind};
pub use resource::{NamespaceId, Resource, SnapshotName};
pub use route_map::{classify_route, RouteAction, RouteClass, ROUTE_ACTIONS};

use thiserror::Error;

/// Failures produced while constructing or invoking the security subsystem.
#[derive(Debug, Error)]
pub enum SecurityError {
    /// A configured or decoded action name is not in the exhaustive inventory.
    #[error("unknown security action: {0}")]
    UnknownAction(String),
    /// A principal identifier violates the safe bounded identifier grammar.
    #[error("invalid principal identifier")]
    InvalidPrincipalId,
    /// A namespace identifier violates Zeppelin's storage and URL grammar.
    #[error("invalid namespace identifier")]
    InvalidNamespaceId,
    /// A snapshot name violates the manifest key grammar.
    #[error("invalid snapshot name")]
    InvalidSnapshotName,
    /// More than one boot grant resolved to the same principal.
    #[error("duplicate security principal")]
    DuplicatePrincipal,
    /// A configured API-key digest is not exactly 32 hexadecimal bytes.
    #[error("invalid API-key SHA-256 digest")]
    InvalidApiKeyDigest,
    /// Enforced mode lacks the server-only key required for authenticated cursors.
    #[error("missing required security.cursor_hmac_key_hex in enforced mode")]
    MissingCursorHmacKey,
    /// Cursor authentication material is not exactly 32 hexadecimal bytes.
    #[error("invalid security.cursor_hmac_key_hex")]
    InvalidCursorHmacKey,
    /// Credential authentication failed without exposing credential material.
    #[error("authentication failed: {0}")]
    Authentication(AuthnFailure),
    /// Central policy denied an authenticated operation.
    #[error("authorization denied: {}", .0.code())]
    Authorization(DenyReason),
    /// An allowed operation attempted to violate server-owned data constraints.
    #[error("security constraint violation")]
    ConstraintViolation,
    /// A continuation token was issued under a different policy version.
    #[error("cursor policy version is stale")]
    CursorPolicyStale,
    /// A registered route reached middleware without a central mapping.
    #[error("protected route has no security mapping")]
    UnmappedRoute,
    /// Authorization ran without the principal inserted by authentication.
    #[error("authenticated principal missing from request extensions")]
    MissingPrincipal,
    /// Authorization ran without the server-derived request context.
    #[error("security request context missing from request extensions")]
    MissingRequestContext,
    /// Trusted-proxy client identity was not established by outer middleware.
    #[error("trusted source address missing from request extensions")]
    MissingSourceIp,
    /// A required mutation completed without durable audit acknowledgement.
    #[error("durable security audit evidence is unavailable")]
    AuditUnavailable,
    /// The requested surface is not present in the resolved entitlement set.
    #[error("feature is not licensed: {0}")]
    FeatureNotLicensed(Feature),
    /// Security management is frozen after the license-expiry grace period.
    #[error("security management is frozen because the license expired")]
    LicenseExpired,
    /// An authoritative policy requires a missing enforcement capability.
    #[error("authoritative security policy requires licensed feature: {0}")]
    FeatureRequired(Feature),
    /// An authoritative policy exceeds a signed license capacity limit.
    #[error("authoritative security policy exceeds licensed limit: {0}")]
    LicenseLimitExceeded(&'static str),
    /// An authoritative policy object violates its strict schema or invariants.
    #[error("invalid security policy: {0}")]
    InvalidPolicy(String),
    /// A policy checksum does not match its canonical content.
    #[error("security policy checksum mismatch")]
    PolicyChecksumMismatch,
    /// A persisted policy head lacks the ETag required for later CAS updates.
    #[error("security policy head did not provide an ETag")]
    PolicyHeadMissingEtag,
    /// No authoritative policy exists and boot config cannot create one.
    #[error("no authoritative security policy and no usable bootstrap credentials")]
    MissingBootstrapCredentials,
    /// An immutable policy artifact collided with an existing object key.
    #[error("immutable security policy object already exists")]
    PolicyObjectCollision,
    /// The monotonic persisted policy version cannot advance further.
    #[error("security policy version overflow")]
    PolicyVersionOverflow,
    /// A security-policy mutation request is structurally invalid.
    #[error("invalid security policy request: {0}")]
    InvalidPolicyRequest(String),
    /// A bounded policy-head CAS retry loop could not publish its mutation.
    #[error("security policy changed concurrently; retry")]
    PolicyConflict,
    /// A requested principal, key, or grant already exists.
    #[error("security policy entity already exists")]
    PolicyEntityAlreadyExists,
    /// A requested principal, key, or grant does not exist.
    #[error("security policy entity not found")]
    PolicyEntityNotFound,
}

/// Security administration failure paired with the exact decision already evaluated.
#[derive(Debug, Error)]
#[error("{error}")]
pub struct SecurityOperationError {
    decision: Option<Box<Decision>>,
    #[source]
    error: Box<crate::error::ZeppelinError>,
}

/// Result of a security administration operation that preserves audit context.
pub type SecurityOperationResult<T> = std::result::Result<T, SecurityOperationError>;

impl SecurityOperationError {
    pub(crate) fn denied(decision: DenyDecision) -> Self {
        let reason = decision.reason;
        Self {
            decision: Some(Box::new(Decision::Deny(decision))),
            error: Box::new(SecurityError::Authorization(reason).into()),
        }
    }

    pub(crate) fn after_allow(error: crate::error::ZeppelinError, decision: AllowDecision) -> Self {
        Self {
            decision: Some(Box::new(Decision::Allow(Box::new(decision)))),
            error: Box::new(error),
        }
    }

    pub(crate) fn with_fallback_allow(self, decision: Option<AllowDecision>) -> Self {
        if self.decision.is_some() {
            self
        } else if let Some(decision) = decision {
            Self::after_allow(*self.error, decision)
        } else {
            self
        }
    }

    /// Borrow the exact decision evaluated before the operation failed, if any.
    #[must_use]
    pub fn decision(&self) -> Option<&Decision> {
        self.decision.as_deref()
    }

    /// Split the audit decision from the canonical Zeppelin failure.
    #[must_use]
    pub fn into_parts(self) -> (Option<Decision>, crate::error::ZeppelinError) {
        (self.decision.map(|decision| *decision), *self.error)
    }

    /// Discard decision context at a caller that has no audit responsibility.
    #[must_use]
    pub fn into_error(self) -> crate::error::ZeppelinError {
        *self.error
    }
}

impl From<crate::error::ZeppelinError> for SecurityOperationError {
    fn from(error: crate::error::ZeppelinError) -> Self {
        Self {
            decision: None,
            error: Box::new(error),
        }
    }
}

impl From<SecurityError> for SecurityOperationError {
    fn from(error: SecurityError) -> Self {
        crate::error::ZeppelinError::from(error).into()
    }
}

impl SecurityError {
    /// HTTP status used by the canonical API envelope.
    #[must_use]
    pub const fn status_code(&self) -> u16 {
        match self {
            Self::Authentication(_)
            | Self::Authorization(
                DenyReason::Unauthenticated
                | DenyReason::CredentialExpired
                | DenyReason::CredentialUnknown,
            ) => 401,
            Self::Authorization(_)
            | Self::FeatureNotLicensed(_)
            | Self::LicenseExpired
            | Self::LicenseLimitExceeded(_) => 403,
            Self::ConstraintViolation => 403,
            Self::CursorPolicyStale => 400,
            Self::InvalidNamespaceId | Self::InvalidSnapshotName => 400,
            Self::InvalidPolicyRequest(_) => 400,
            Self::PolicyConflict | Self::PolicyEntityAlreadyExists => 409,
            Self::PolicyEntityNotFound => 404,
            Self::UnknownAction(_)
            | Self::InvalidPrincipalId
            | Self::DuplicatePrincipal
            | Self::InvalidApiKeyDigest
            | Self::MissingCursorHmacKey
            | Self::InvalidCursorHmacKey
            | Self::UnmappedRoute
            | Self::MissingPrincipal
            | Self::MissingRequestContext
            | Self::MissingSourceIp
            | Self::AuditUnavailable
            | Self::InvalidPolicy(_)
            | Self::PolicyChecksumMismatch
            | Self::PolicyHeadMissingEtag
            | Self::MissingBootstrapCredentials
            | Self::PolicyObjectCollision
            | Self::PolicyVersionOverflow
            | Self::FeatureRequired(_) => 500,
        }
    }

    /// Stable lowercase machine-readable security code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Authentication(failure) => failure.code(),
            Self::Authorization(DenyReason::ActionNotGranted) => "forbidden",
            Self::Authorization(reason) => reason.code(),
            Self::ConstraintViolation => "constraint_violation",
            Self::CursorPolicyStale => "cursor_policy_stale",
            Self::InvalidNamespaceId => "invalid_namespace",
            Self::InvalidSnapshotName => "invalid_snapshot",
            Self::InvalidPolicyRequest(_) => "invalid_security_request",
            Self::PolicyConflict => "security_conflict",
            Self::PolicyEntityAlreadyExists => "security_entity_exists",
            Self::PolicyEntityNotFound => "security_entity_not_found",
            Self::AuditUnavailable => "audit_unavailable",
            Self::FeatureNotLicensed(_) => "feature_not_licensed",
            Self::LicenseExpired => "license_expired",
            Self::FeatureRequired(_) => "security_internal",
            Self::LicenseLimitExceeded(_) => "license_limit_exceeded",
            Self::UnmappedRoute => "unmapped_route",
            Self::UnknownAction(_)
            | Self::InvalidPrincipalId
            | Self::DuplicatePrincipal
            | Self::InvalidApiKeyDigest
            | Self::MissingCursorHmacKey
            | Self::InvalidCursorHmacKey
            | Self::MissingPrincipal
            | Self::MissingRequestContext
            | Self::MissingSourceIp
            | Self::InvalidPolicy(_)
            | Self::PolicyChecksumMismatch
            | Self::PolicyHeadMissingEtag
            | Self::MissingBootstrapCredentials
            | Self::PolicyObjectCollision
            | Self::PolicyVersionOverflow => "security_internal",
        }
    }

    /// Redacted prose safe for an external response.
    #[must_use]
    pub fn client_message(&self) -> String {
        match self {
            Self::Authentication(_)
            | Self::Authorization(
                DenyReason::Unauthenticated
                | DenyReason::CredentialExpired
                | DenyReason::CredentialUnknown,
            ) => "authentication required".to_string(),
            Self::Authorization(_) => "access forbidden".to_string(),
            Self::ConstraintViolation => "operation violates security constraints".to_string(),
            Self::CursorPolicyStale => {
                "cursor was issued under a different security policy; re-query required".to_string()
            }
            Self::InvalidNamespaceId => "invalid namespace name".to_string(),
            Self::InvalidSnapshotName => "invalid snapshot name".to_string(),
            Self::InvalidPolicyRequest(_) => "invalid security request".to_string(),
            Self::PolicyConflict => "security policy changed concurrently; retry".to_string(),
            Self::PolicyEntityAlreadyExists => "security policy entity already exists".to_string(),
            Self::PolicyEntityNotFound => "security policy entity not found".to_string(),
            Self::AuditUnavailable => {
                "operation may have completed, but durable audit evidence is unavailable"
                    .to_string()
            }
            Self::FeatureNotLicensed(feature) => {
                format!("feature is not licensed: {}", feature.as_str())
            }
            Self::LicenseExpired => {
                "security management is frozen because the license expired".to_string()
            }
            Self::LicenseLimitExceeded(limit) => {
                format!("licensed security limit exceeded: {limit}")
            }
            Self::UnknownAction(_)
            | Self::InvalidPrincipalId
            | Self::DuplicatePrincipal
            | Self::InvalidApiKeyDigest
            | Self::MissingCursorHmacKey
            | Self::InvalidCursorHmacKey
            | Self::UnmappedRoute
            | Self::MissingPrincipal
            | Self::MissingRequestContext
            | Self::MissingSourceIp
            | Self::InvalidPolicy(_)
            | Self::PolicyChecksumMismatch
            | Self::PolicyHeadMissingEtag
            | Self::MissingBootstrapCredentials
            | Self::PolicyObjectCollision
            | Self::PolicyVersionOverflow
            | Self::FeatureRequired(_) => "an internal security error occurred".to_string(),
        }
    }
}
