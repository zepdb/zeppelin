//! Strong resource identifiers presented to central authorization.

use serde::{Deserialize, Serialize};

use super::SecurityError;
use crate::namespace::NamespaceId;

impl NamespaceId {
    /// Validate a namespace using the same grammar as namespace creation.
    pub fn new(value: impl Into<String>) -> Result<Self, SecurityError> {
        Self::parse(value).map_err(|_| SecurityError::InvalidNamespaceId)
    }
}

/// Validated snapshot name used by security policy.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SnapshotName(String);

impl SnapshotName {
    /// Validate a snapshot name using the manifest grammar.
    pub fn new(value: impl Into<String>) -> Result<Self, SecurityError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 255
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        if valid {
            Ok(Self(value))
        } else {
            Err(SecurityError::InvalidSnapshotName)
        }
    }

    /// Borrow the snapshot name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Typed target of an authorization decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Resource {
    /// Process-wide health, readiness, metrics, or profiling state.
    System,
    /// Live runtime query configuration.
    RuntimeConfig,
    /// S3-authoritative security principals, credentials, grants, and policy.
    SecurityPolicy,
    /// Namespace data or lifecycle state.
    Namespace(NamespaceId),
    /// One named snapshot within a namespace.
    Snapshot(NamespaceId, SnapshotName),
}

impl Resource {
    /// Return the namespace scope when the resource belongs to one.
    #[must_use]
    pub fn namespace(&self) -> Option<&NamespaceId> {
        match self {
            Self::Namespace(namespace) | Self::Snapshot(namespace, _) => Some(namespace),
            Self::System | Self::RuntimeConfig | Self::SecurityPolicy => None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::Resource;

    #[test]
    fn security_policy_resource_is_global_and_round_trips() {
        let encoded = r#""SecurityPolicy""#;
        let resource: Resource = serde_json::from_str(encoded)
            .expect("SecurityPolicy must be a recognized authorization resource");

        assert_eq!(resource.namespace(), None);
        assert_eq!(serde_json::to_string(&resource).unwrap(), encoded);
    }
}
