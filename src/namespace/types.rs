//! Strong namespace identities shared across domain boundaries.
//!
//! Namespace names identify a logical storage and authorization boundary.
//! Incarnation IDs distinguish successive lifetimes of the same name. Keeping
//! both types here lets manifests and namespace-graph code name physical owners
//! without importing them through the security subsystem or metadata manager.

use std::fmt;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Result, ZeppelinError};

/// Validated namespace identifier shared by storage and security policy.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NamespaceId(String);

/// A namespace string did not satisfy the shared storage and URL grammar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InvalidNamespaceId;

impl NamespaceId {
    /// Borrow the namespace text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parse an identity using the namespace layer's shared grammar.
    pub(crate) fn parse(value: impl Into<String>) -> std::result::Result<Self, InvalidNamespaceId> {
        let value = value.into();
        if is_valid_namespace_name(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidNamespaceId)
        }
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Collision-resistant identity for one lifetime of a namespace name.
///
/// The stable text and Serde representation is UUID's lowercase hyphenated
/// form, matching the value stored in S3 user metadata.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamespaceIncarnationId(uuid::Uuid);

impl NamespaceIncarnationId {
    /// Mint a fresh namespace-lifetime identity.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// Parse the stable UUID text stored in namespace object metadata.
    pub(crate) fn parse(value: &str) -> Result<Self> {
        uuid::Uuid::parse_str(value).map(Self).map_err(|error| {
            ZeppelinError::Serialization(format!(
                "invalid namespace incarnation metadata {value:?}: {error}"
            ))
        })
    }

    /// Wrap an already validated manifest UUID without changing its bytes.
    #[must_use]
    pub(crate) const fn from_uuid(value: uuid::Uuid) -> Self {
        Self(value)
    }

    /// Render the stable UUID text stored in S3 user metadata.
    #[must_use]
    pub(crate) fn as_string(&self) -> String {
        self.0.to_string()
    }

    /// Return the underlying UUID used by manifest incarnation binding.
    #[must_use]
    pub(crate) const fn as_uuid(&self) -> uuid::Uuid {
        self.0
    }

    /// Return whether this is the forbidden all-zero incarnation identity.
    #[must_use]
    pub(crate) const fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl fmt::Display for NamespaceIncarnationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl Serialize for NamespaceIncarnationId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for NamespaceIncarnationId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        uuid::Uuid::parse_str(&value)
            .map(Self)
            .map_err(de::Error::custom)
    }
}

/// Validates a namespace name as both an S3 top-level key prefix and one URL
/// path segment.
///
/// This deliberately matches the safe names produced by the test helpers:
/// raw S3 keys may contain `/`, while namespace names used in HTTP paths may
/// not. Names are 1–255 ASCII characters, start with an alphanumeric character,
/// and otherwise contain only alphanumerics, `-`, `_`, or `.`.
///
/// # Examples
///
/// `tenant-a`, `tenant_a`, and `tenant.a` are valid. `tenant/a`, `../tenant`,
/// and `-tenant` are rejected before they can become ambiguous keys or paths.
#[must_use]
pub fn is_valid_namespace_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 255 {
        return false;
    }
    let bytes = name.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() {
        return false;
    }
    bytes
        .iter()
        .all(|&byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}
