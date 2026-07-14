//! Central authentication, authorization, and decision vocabulary.
//!
//! Transport adapters resolve credentials to typed principals. The security
//! kernel then evaluates one exhaustive [`Action`] against a typed resource and
//! returns an explicit decision before domain work can begin.

mod action;
mod authn;
mod context;
mod decision;
mod kernel;
mod principal;
mod resource;
mod route_map;

pub use action::Action;
pub use authn::{ApiKeyAdapter, AuthnFailure, CredentialAdapter};
pub use context::RequestContext;
pub use decision::{
    AllowDecision, Decision, DecisionId, DenyDecision, DenyReason, FieldMask, Obligation,
    PolicyVersion, WriteConstraints,
};
pub use kernel::SecurityKernel;
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
    /// Credential authentication failed without exposing credential material.
    #[error("authentication failed: {0}")]
    Authentication(AuthnFailure),
    /// Central policy denied an authenticated operation.
    #[error("authorization denied: {}", .0.code())]
    Authorization(DenyReason),
    /// A registered route reached middleware without a central mapping.
    #[error("protected route has no security mapping")]
    UnmappedRoute,
    /// Authorization ran without the principal inserted by authentication.
    #[error("authenticated principal missing from request extensions")]
    MissingPrincipal,
    /// Authorization ran without the server-derived request context.
    #[error("security request context missing from request extensions")]
    MissingRequestContext,
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
            Self::Authorization(_) => 403,
            Self::InvalidNamespaceId | Self::InvalidSnapshotName => 400,
            Self::UnknownAction(_)
            | Self::InvalidPrincipalId
            | Self::DuplicatePrincipal
            | Self::InvalidApiKeyDigest
            | Self::UnmappedRoute
            | Self::MissingPrincipal
            | Self::MissingRequestContext => 500,
        }
    }

    /// Stable lowercase machine-readable security code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Authentication(failure) => failure.code(),
            Self::Authorization(DenyReason::ActionNotGranted) => "forbidden",
            Self::Authorization(reason) => reason.code(),
            Self::InvalidNamespaceId => "invalid_namespace",
            Self::InvalidSnapshotName => "invalid_snapshot",
            Self::UnmappedRoute => "unmapped_route",
            Self::UnknownAction(_)
            | Self::InvalidPrincipalId
            | Self::DuplicatePrincipal
            | Self::InvalidApiKeyDigest
            | Self::MissingPrincipal
            | Self::MissingRequestContext => "security_internal",
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
            Self::InvalidNamespaceId => "invalid namespace name".to_string(),
            Self::InvalidSnapshotName => "invalid snapshot name".to_string(),
            Self::UnknownAction(_)
            | Self::InvalidPrincipalId
            | Self::DuplicatePrincipal
            | Self::InvalidApiKeyDigest
            | Self::UnmappedRoute
            | Self::MissingPrincipal
            | Self::MissingRequestContext => "an internal security error occurred".to_string(),
        }
    }
}
