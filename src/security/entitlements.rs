//! Typed boot-time feature authority resolved from a verified license source.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use super::LicenseError;

const MANAGEMENT_GRACE_DAYS: i64 = 14;

/// One independently licensed Zeppelin capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u16)]
pub enum Feature {
    /// S3 policy administration, rotation, and role-based grants.
    Rbac,
    /// Mandatory filters, response masks, and write constraints.
    Constraints,
    /// Durable object-storage audit delivery and must-audit barriers.
    AuditS3,
    /// Short-lived delegated credentials introduced in Phase 7.
    Delegation,
    /// Preservation and governance controls introduced in Phase 8.
    Preservation,
    /// Signed retrieval receipts introduced in Phase 10.
    Receipts,
    /// External audit-stream delivery reserved for a later phase.
    AuditStreaming,
    /// Customer-managed encryption keys reserved for a later phase.
    Cmek,
    /// Namespace branching and fork lifecycle operations.
    Branching,
}

impl Feature {
    /// Every licensable feature in stable bit-assignment order.
    pub const ALL: [Self; 9] = [
        Self::Rbac,
        Self::Constraints,
        Self::AuditS3,
        Self::Delegation,
        Self::Preservation,
        Self::Receipts,
        Self::AuditStreaming,
        Self::Cmek,
        Self::Branching,
    ];

    /// Return the stable license and error-envelope spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rbac => "rbac",
            Self::Constraints => "constraints",
            Self::AuditS3 => "audit_s3",
            Self::Delegation => "delegation",
            Self::Preservation => "preservation",
            Self::Receipts => "receipts",
            Self::Branching => "branching",
            Self::AuditStreaming => "audit_streaming",
            Self::Cmek => "cmek",
        }
    }

    const fn bit(self) -> u16 {
        1_u16 << (self as u16)
    }
}

impl std::fmt::Display for Feature {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable origin of one resolved entitlement set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntitlementSource {
    /// No license path was configured.
    Community,
    /// An offline Ed25519-signed file was verified.
    FileLicense,
    /// A managed control plane supplied the entitlement set.
    ControlPlane,
}

/// Validated customer identity carried by a license.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CustomerId(String);

impl CustomerId {
    pub(crate) fn new(value: String) -> Result<Self, LicenseError> {
        let valid = !value.is_empty()
            && value.len() <= 128
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.')
            });
        if valid {
            Ok(Self(value))
        } else {
            Err(LicenseError::InvalidField("customer_id"))
        }
    }

    /// Borrow the stable customer identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Optional licensed capacity limits enforced by composition roots.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntitlementLimits {
    /// Maximum principals permitted in an authoritative policy snapshot.
    pub max_principals: Option<u32>,
}

/// Immutable feature authority resolved exactly once during boot.
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct Entitlements {
    source: EntitlementSource,
    customer: Option<CustomerId>,
    customer_name: Option<String>,
    issued_at: Option<DateTime<Utc>>,
    expires_at: Option<DateTime<Utc>>,
    management_freeze_at: Option<DateTime<Utc>>,
    feature_bits: u16,
    limits: EntitlementLimits,
}

impl Entitlements {
    /// Construct the secure community floor used when no license is configured.
    #[must_use]
    pub const fn community() -> Self {
        Self {
            source: EntitlementSource::Community,
            customer: None,
            customer_name: None,
            issued_at: None,
            expires_at: None,
            management_freeze_at: None,
            feature_bits: 0,
            limits: EntitlementLimits {
                max_principals: None,
            },
        }
    }

    pub(crate) fn from_verified_license(
        customer: CustomerId,
        customer_name: String,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        features: impl IntoIterator<Item = Feature>,
        limits: EntitlementLimits,
    ) -> Result<Self, LicenseError> {
        Self::licensed(
            EntitlementSource::FileLicense,
            Some(customer),
            Some(customer_name),
            Some(issued_at),
            Some(expires_at),
            features,
            limits,
        )
    }

    /// Construct an explicit entitlement set for integration composition tests.
    ///
    /// Production startup never calls this seam; it resolves through
    /// [`super::FileLicenseResolver`]. The constructor exists only in this
    /// crate's unit-test build. Integration-test binaries cannot name it and
    /// add no safe construction path to the release library.
    #[doc(hidden)]
    #[must_use]
    #[cfg(test)]
    pub fn licensed_for_testing(
        features: impl IntoIterator<Item = Feature>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Self {
        match Self::licensed(
            EntitlementSource::FileLicense,
            None,
            None,
            expires_at,
            expires_at,
            features,
            EntitlementLimits::default(),
        ) {
            Ok(entitlements) => entitlements,
            Err(error) => panic!("invalid test entitlements: {error}"),
        }
    }

    fn licensed(
        source: EntitlementSource,
        customer: Option<CustomerId>,
        customer_name: Option<String>,
        issued_at: Option<DateTime<Utc>>,
        expires_at: Option<DateTime<Utc>>,
        features: impl IntoIterator<Item = Feature>,
        limits: EntitlementLimits,
    ) -> Result<Self, LicenseError> {
        let management_freeze_at = match expires_at {
            Some(expiry) => Some(
                expiry
                    .checked_add_signed(Duration::days(MANAGEMENT_GRACE_DAYS))
                    .ok_or(LicenseError::InvalidField("expires_at"))?,
            ),
            None => None,
        };
        let mut feature_bits = 0_u16;
        for feature in features {
            feature_bits |= feature.bit();
        }
        Ok(Self {
            source,
            customer,
            customer_name,
            issued_at,
            expires_at,
            management_freeze_at,
            feature_bits,
            limits,
        })
    }

    /// Return whether the verified license grants one capability.
    ///
    /// Hard expiry deliberately does not clear enforcement capabilities. It
    /// only freezes management after the grace deadline.
    #[must_use]
    pub const fn has(&self, feature: Feature) -> bool {
        self.feature_bits & feature.bit() != 0
    }

    /// Return whether security mutations must be rejected after expiry grace.
    #[must_use]
    pub fn management_frozen(&self, now: DateTime<Utc>) -> bool {
        self.management_freeze_at
            .is_some_and(|freeze_at| now > freeze_at)
    }

    /// Return seconds until expiry, negative after expiry.
    #[must_use]
    pub fn expiry_seconds(&self, now: DateTime<Utc>) -> Option<i64> {
        self.expires_at
            .map(|expires_at| expires_at.signed_duration_since(now).num_seconds())
    }

    /// Return the verified entitlement source.
    #[must_use]
    pub const fn source(&self) -> EntitlementSource {
        self.source
    }

    /// Borrow the licensed customer identifier, if present.
    #[must_use]
    pub const fn customer(&self) -> Option<&CustomerId> {
        self.customer.as_ref()
    }

    /// Borrow the licensed customer display name, if present.
    #[must_use]
    pub fn customer_name(&self) -> Option<&str> {
        self.customer_name.as_deref()
    }

    /// Return the signed issue time, if present.
    #[must_use]
    pub const fn issued_at(&self) -> Option<DateTime<Utc>> {
        self.issued_at
    }

    /// Return the hard expiry time, if present.
    #[must_use]
    pub const fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expires_at
    }

    /// Return the verified capacity limits.
    #[must_use]
    pub const fn limits(&self) -> EntitlementLimits {
        self.limits
    }
}
