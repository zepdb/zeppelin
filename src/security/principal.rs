//! Typed authenticated identity used by every protected operation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{ApiKeyId, SecurityError};

/// Stable, validated identifier for a security principal.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrincipalId(String);

impl PrincipalId {
    /// Validate and construct a principal identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, SecurityError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 128
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.')
            });
        if valid {
            Ok(Self(value))
        } else {
            Err(SecurityError::InvalidPrincipalId)
        }
    }

    /// Borrow the stable identifier text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Origin and operational semantics of one principal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// A human identity supplied by a future identity-provider adapter.
    Human,
    /// A named workload or API-key identity.
    Service,
    /// A delegated agent or job identity.
    Agent,
    /// A Zeppelin-owned background identity.
    Internal,
    /// An identity used only on explicitly public routes or unsafe-open mode.
    Anonymous,
}

/// Strength and adapter family that established an identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthStrength {
    /// No credential was presented on an explicitly anonymous path.
    Anonymous,
    /// A named high-entropy API key was verified.
    ApiKey,
}

/// Authenticated identity passed to central authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    /// Stable identity used in policy and audit.
    pub id: PrincipalId,
    /// Human, service, agent, internal, or anonymous semantics.
    pub kind: PrincipalKind,
    /// Redaction-safe display identity for audit events.
    pub display_name: String,
    /// Credential strength used for this request.
    pub auth_strength: AuthStrength,
    /// Credential expiry, if the adapter supplied one.
    pub expires_at: Option<DateTime<Utc>>,
    /// Exact API-key identifier whose proof established this request identity.
    pub api_key_id: Option<ApiKeyId>,
    /// Parent identity for delegated credentials.
    pub delegation_parent: Option<PrincipalId>,
}

impl Principal {
    /// Construct a service principal authenticated by a named API key.
    #[must_use]
    pub fn api_key(
        id: PrincipalId,
        api_key_id: ApiKeyId,
        display_name: String,
        expires_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self::authenticated_api_key(
            id,
            api_key_id,
            PrincipalKind::Service,
            display_name,
            expires_at,
        )
    }

    pub(crate) fn authenticated_api_key(
        id: PrincipalId,
        api_key_id: ApiKeyId,
        kind: PrincipalKind,
        display_name: String,
        expires_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id,
            kind,
            display_name,
            auth_strength: AuthStrength::ApiKey,
            expires_at,
            api_key_id: Some(api_key_id),
            delegation_parent: None,
        }
    }

    /// Construct the identity used by public and explicit unsafe-open requests.
    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            id: PrincipalId("anonymous".to_string()),
            kind: PrincipalKind::Anonymous,
            display_name: "anonymous".to_string(),
            auth_strength: AuthStrength::Anonymous,
            expires_at: None,
            api_key_id: None,
            delegation_parent: None,
        }
    }

    /// Return whether this identity was established without a credential.
    #[must_use]
    pub const fn is_anonymous(&self) -> bool {
        matches!(self.kind, PrincipalKind::Anonymous)
    }
}
