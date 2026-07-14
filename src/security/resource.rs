//! Strong resource identifiers presented to central authorization.

use serde::{Deserialize, Serialize};

use super::SecurityError;

/// Validated namespace identifier used by security policy.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NamespaceId(String);

impl NamespaceId {
    /// Validate a namespace using the same grammar as namespace creation.
    pub fn new(value: impl Into<String>) -> Result<Self, SecurityError> {
        let value = value.into();
        if crate::namespace::manager::is_valid_namespace_name(&value) {
            Ok(Self(value))
        } else {
            Err(SecurityError::InvalidNamespaceId)
        }
    }

    /// Borrow the namespace text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
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
            Self::System | Self::RuntimeConfig => None,
        }
    }
}
