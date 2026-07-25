//! Typed boot-time feature authority resolved from a verified license source.
//!
//! This file owns the *answer* to "is this capability licensed?" and nothing
//! else. It defines the closed inventory of licensable capabilities
//! ([`Feature`]), the immutable per-process authority resolved from a verified
//! license ([`Entitlements`]), and the validated customer identity and capacity
//! limits that travel with it ([`CustomerId`], [`EntitlementLimits`]).
//!
//! It deliberately does **not** own:
//!
//! - parsing, canonicalization, or Ed25519 verification of a license document —
//!   that is `src/security/license.rs`, the only module allowed to build a
//!   licensed [`Entitlements`] value (see [`super::verify_signed_license_bytes`]);
//! - the decision to *deny* an unlicensed request — that belongs to
//!   [`super::SecurityKernel`] (and, for policy documents, to
//!   [`super::PolicyStore`]), which turn a `false` from [`Entitlements::has`]
//!   into a typed `FeatureNotLicensed` / `FeatureRequired` error;
//! - anything time-driven beyond reporting expiry. Nothing here schedules,
//!   refreshes, or re-resolves.
//!
//! ## Where this sits
//!
//! [`build_app`](crate::startup::build_app) resolves a license exactly once, on a blocking
//! thread, and wraps the result in an `Arc<Entitlements>` that is shared —
//! never replaced — for the process lifetime. Every downstream consumer only
//! reads it.
//!
//! ```text
//! boot: FileLicenseResolver (src/security/license.rs)
//!         |  Ed25519 verify + field validation
//!         v
//!   Entitlements  (this file; immutable, Arc-shared, process-local)
//!         |
//!         +--> SecurityKernel   composition gates + per-request authorization
//!         +--> PolicyStore      rejects policy documents that use unlicensed
//!         |                     constraints/delegation/preservation features
//!         +--> startup metrics  LICENSE_EXPIRY_SECONDS, expired-boot audit
//! ```
//!
//! With no license path configured, [`Entitlements::community`] is the secure
//! floor: an all-zero feature mask. That is the *absence* of paid capability,
//! not a degraded version of it — an unlicensed feature is denied outright, and
//! callers must never substitute a weaker behavior for a missing entitlement.
//!
//! ## The bit-assignment rule
//!
//! [`Feature`] is `#[repr(u16)]`; each variant's discriminant is its
//! declaration index, and its mask bit is `1 << discriminant`.
//! [`Entitlements`] stores the union of granted bits in a private `u16`.
//! Consequences a maintainer must respect:
//!
//! - **Append only.** Adding a variant at the end is safe. Reordering or
//!   removing one silently reassigns every later bit index.
//! - **Never rename a variant.** A signed license carries feature *names*, not
//!   bits, and the serde `rename_all = "snake_case"` spelling is the exact text
//!   the Ed25519 signature covers — renaming invalidates every issued license
//!   that names that feature. [`Feature::as_str`] is the matching spelling used
//!   in error envelopes, audit fields, and the `zeppelin_license` CLI; the two
//!   are defined independently, so keep them identical or an operator will see
//!   one name in a license file and a different one in a denial.
//! - **The mask is process-local.** It is never serialized, signed, or written
//!   to object storage; it is recomputed from names on every boot. So a
//!   reorder does not break *signature verification* — it breaks meaning, which
//!   is worse, because the failure is silent.
//! - **`Feature::ALL` is the declared order.** Its `[Self; 9]` type forces the
//!   literal to be updated when the count changes, but the compiler cannot
//!   check that every variant is present. Add to both places in one edit.
//! - **The `u16` mask caps the inventory at 16 features.** A 17th variant would
//!   shift past the width of `feature_bits`.
//! - **`Entitlements` is `#[repr(C)]` on purpose.** `tests/common/server.rs`
//!   mirrors this exact field order and transmutes into it to compose licensed
//!   integration servers without a release-visible constructor. Changing field
//!   order or types here breaks that mirror.
//!
//! ## Expiry authority
//!
//! Expiry does **not** revoke enforcement. [`Entitlements::has`] ignores time
//! entirely: a customer whose license lapsed keeps RBAC, audit, and every other
//! protective capability, because silently dropping enforcement would be a
//! security regression dressed up as a billing control. What expiry does is
//! start a 14-day grace window, after which
//! [`Entitlements::management_frozen`] reports that *security mutations* must
//! be rejected. [`Entitlements::expiry_seconds`] feeds the boot metric and the
//! daily observer; it goes negative after expiry rather than saturating.
//!
//! ## Where to start reading
//!
//! [`Feature`] for the inventory, then [`Entitlements::has`] for the read path,
//! then the private `licensed` constructor for how a verified license becomes
//! an authority. Construction is crate-private by design: outside this crate
//! the only way to obtain a licensed value is
//! [`super::verify_signed_license_bytes`] or [`super::FileLicenseResolver`].
//!
//! ## Rust concepts used here
//!
//! The privacy boundary *is* the security boundary. Every field of
//! [`Entitlements`] is private and every constructor that grants a feature is
//! `pub(crate)` or `#[cfg(test)]`, so no downstream crate can fabricate an
//! authority. Java would need a sealed class plus discipline; here the compiler
//! refuses the code outright.
//!
//! [`Feature`] derives `Copy` because it is a one-word tag passed by value into
//! `has(...)` on hot authorization paths — no allocation, no borrow, no
//! lifetime to thread through. [`Entitlements`] deliberately does *not* derive
//! `Serialize`: it is a decision, not a document, and there is no legitimate
//! reason to round-trip one.
//!
//! A C engineer will recognize the mask arithmetic; the difference is that the
//! bit index is derived from the enum discriminant rather than a hand-written
//! `#define`, so the inventory and the bit layout cannot drift apart within a
//! build. They can still drift *across* builds, which is exactly what the
//! append-only rule above protects.

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
