//! Strong resource identifiers presented to central authorization.
//!
//! This module owns the vocabulary for *what* an authorization decision is
//! about. It deliberately owns no rules: it cannot say whether an operation is
//! permitted, only name the target precisely enough that [`super::kernel`] and
//! [`super::policy`] can reason about it without parsing strings.
//!
//! [`Resource`] is the typed target every `authorize_*` call carries alongside
//! an [`super::Action`] and a [`super::Principal`]. Grants in the persisted
//! policy document are matched against these variants, so the shape here is
//! the shape policy authors write against.
//!
//! ## Scope hierarchy
//!
//! ```text
//! global, no namespace scope        namespace-scoped
//! ------------------------------    -----------------------------
//! System          health/metrics    Namespace(ns)   data+lifecycle
//! RuntimeConfig   live query knobs  Snapshot(ns,n)  one named snapshot
//! SecurityPolicy  principals/grants
//! ```
//!
//! [`Resource::namespace`] is the single place that distinction is encoded.
//! It returns `None` for the three global variants, and a grant scoped to a
//! namespace can therefore never silently widen to cover policy administration
//! or process-wide state.
//!
//! ## Invariants
//!
//! - Identifiers are validated at construction, not at use. [`SnapshotName`]
//!   and [`NamespaceId`] are newtypes whose constructors
//!   reject invalid input with a typed [`super::SecurityError`], so a
//!   `Resource` value in hand is already well-formed.
//! - Namespace validation reuses the same grammar as namespace creation, so a
//!   name that policy can express is a name the rest of Zeppelin can address.
//! - Snapshot names permit only ASCII alphanumerics, `-`, `_`, and `.`, capped
//!   at 255 bytes. That grammar keeps a name safe in both an S3 key and a URL
//!   path segment — see the root `CLAUDE.md` note that the two have different
//!   rules.
//!
//! ## Rust concepts used here
//!
//! The newtypes wrap a private `String` and expose only `as_str`, so no caller
//! can mutate a validated identifier after the check. `#[serde(transparent)]`
//! makes each newtype serialize as the bare string it wraps, keeping the
//! persisted policy document readable while preserving the compile-time
//! distinction between a `SnapshotName` and any other string. In Java this
//! resembles a final value class, but here the compiler also prevents passing
//! a raw `String` where a validated name is required.

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
