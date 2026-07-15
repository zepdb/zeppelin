//! Offline Ed25519 license parsing, canonicalization, and resolution.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::{CustomerId, EntitlementLimits, Entitlements, Feature};

/// Embedded production license-verification key.
///
/// The corresponding private key is never present in this repository. Release
/// custody replaces this public key through an intentional source change and
/// review before issuing production licenses.
#[cfg(not(test))]
pub const LICENSE_PUBKEY: [u8; 32] = [
    0x54, 0x84, 0x57, 0xc2, 0xb1, 0xac, 0xd3, 0xdd, 0x8a, 0x3b, 0xa7, 0xda, 0x83, 0xe3, 0x55, 0x92,
    0x0b, 0x05, 0xba, 0x9d, 0x9e, 0x5f, 0x30, 0x06, 0xb6, 0x56, 0x4a, 0xea, 0xd6, 0x6d, 0x5c, 0x2c,
];

/// Test-only public key selected exclusively for this crate's unit-test build.
#[cfg(test)]
pub const LICENSE_PUBKEY: [u8; 32] = [
    0xea, 0x4a, 0x6c, 0x63, 0xe2, 0x9c, 0x52, 0x0a, 0xbe, 0xf5, 0x50, 0x7b, 0x13, 0x2e, 0xc5, 0xf9,
    0x95, 0x47, 0x76, 0xae, 0xbe, 0xbe, 0x7b, 0x92, 0x42, 0x1e, 0xea, 0x69, 0x14, 0x46, 0xd2, 0x2c,
];

/// Strict offline license parsing and signature failures.
#[derive(Debug, Error)]
pub enum LicenseError {
    /// The configured license file could not be read.
    #[error("license file read failed: {0}")]
    Read(String),
    /// The license JSON was invalid or contained unknown fields.
    #[error("license JSON is invalid: {0}")]
    Json(String),
    /// A signed field violates the license schema.
    #[error("invalid license field: {0}")]
    InvalidField(&'static str),
    /// The signature encoding or Ed25519 proof is invalid.
    #[error("license signature verification failed")]
    InvalidSignature,
    /// Canonical payload serialization failed.
    #[error("license canonicalization failed: {0}")]
    Canonicalization(String),
    /// The managed control-plane resolver is only an architectural seam here.
    #[error("managed entitlement resolution is not implemented")]
    ControlPlaneUnimplemented,
}

/// Optional limits embedded in a signed license payload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LicenseLimits {
    /// Maximum principals permitted in an authoritative policy snapshot.
    pub max_principals: Option<u32>,
}

/// Exact canonical payload covered by the Ed25519 signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LicensePayload {
    /// Stable customer identifier.
    pub customer_id: String,
    /// Human-readable customer name.
    pub customer_name: String,
    /// License issue time.
    pub issued_at: DateTime<Utc>,
    /// Hard license expiry time.
    pub expires_at: DateTime<Utc>,
    /// Explicit licensed capabilities.
    pub features: Vec<Feature>,
    /// Optional capacity limits.
    #[serde(default)]
    pub limits: LicenseLimits,
}

/// Signed JSON document accepted by the file resolver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedLicense {
    /// Stable customer identifier.
    pub customer_id: String,
    /// Human-readable customer name.
    pub customer_name: String,
    /// License issue time.
    pub issued_at: DateTime<Utc>,
    /// Hard license expiry time.
    pub expires_at: DateTime<Utc>,
    /// Explicit licensed capabilities.
    pub features: Vec<Feature>,
    /// Optional capacity limits.
    #[serde(default)]
    pub limits: LicenseLimits,
    /// Base64url-without-padding Ed25519 signature over canonical payload JSON.
    pub signature: String,
}

impl SignedLicense {
    /// Combine a payload with a caller-produced signature.
    #[must_use]
    pub fn new(payload: LicensePayload, signature: String) -> Self {
        Self {
            customer_id: payload.customer_id,
            customer_name: payload.customer_name,
            issued_at: payload.issued_at,
            expires_at: payload.expires_at,
            features: payload.features,
            limits: payload.limits,
            signature,
        }
    }

    /// Project the exact payload covered by the signature.
    #[must_use]
    pub fn payload(&self) -> LicensePayload {
        LicensePayload {
            customer_id: self.customer_id.clone(),
            customer_name: self.customer_name.clone(),
            issued_at: self.issued_at,
            expires_at: self.expires_at,
            features: self.features.clone(),
            limits: self.limits,
        }
    }
}

/// Synchronous boot-time entitlement source.
pub trait EntitlementResolver: Send + Sync {
    /// Resolve one immutable entitlement set or fail boot loudly.
    fn resolve(&self) -> Result<Entitlements, LicenseError>;
}

/// Offline resolver for an optional signed JSON license file.
#[derive(Debug, Clone)]
pub struct FileLicenseResolver {
    path: PathBuf,
}

impl FileLicenseResolver {
    /// Bind the resolver to one configured path; an empty path means community.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn resolve_with_key(&self, public_key: &[u8; 32]) -> Result<Entitlements, LicenseError> {
        if self.path.as_os_str().is_empty() {
            return Ok(Entitlements::community());
        }
        let body =
            std::fs::read(&self.path).map_err(|error| LicenseError::Read(error.to_string()))?;
        verify_license_bytes_with_key(&body, public_key)
    }
}

impl EntitlementResolver for FileLicenseResolver {
    fn resolve(&self) -> Result<Entitlements, LicenseError> {
        self.resolve_with_key(&LICENSE_PUBKEY)
    }
}

/// Managed-service entitlement seam filled by the control-plane track.
#[cfg(feature = "managed")]
#[derive(Debug, Clone, Copy, Default)]
pub struct ControlPlaneResolver;

#[cfg(feature = "managed")]
impl EntitlementResolver for ControlPlaneResolver {
    fn resolve(&self) -> Result<Entitlements, LicenseError> {
        Err(LicenseError::ControlPlaneUnimplemented)
    }
}

/// Return canonical JSON bytes used by both issuance and verification tools.
pub fn canonical_payload_bytes(payload: &LicensePayload) -> Result<Vec<u8>, LicenseError> {
    let value = serde_json::to_value(payload)
        .map_err(|error| LicenseError::Canonicalization(error.to_string()))?;
    serde_json::to_vec(&canonicalize(value))
        .map_err(|error| LicenseError::Canonicalization(error.to_string()))
}

/// Verify one signed document against the embedded production public key.
pub fn verify_signed_license_bytes(body: &[u8]) -> Result<Entitlements, LicenseError> {
    verify_license_bytes_with_key(body, &LICENSE_PUBKEY)
}

fn verify_license_bytes_with_key(
    body: &[u8],
    public_key: &[u8; 32],
) -> Result<Entitlements, LicenseError> {
    let document: SignedLicense =
        serde_json::from_slice(body).map_err(|error| LicenseError::Json(error.to_string()))?;
    validate_license_payload(&document.payload())?;
    let signature_bytes = URL_SAFE_NO_PAD
        .decode(&document.signature)
        .map_err(|_| LicenseError::InvalidSignature)?;
    let signature =
        Signature::from_slice(&signature_bytes).map_err(|_| LicenseError::InvalidSignature)?;
    let verifying_key =
        VerifyingKey::from_bytes(public_key).map_err(|_| LicenseError::InvalidSignature)?;
    let payload_bytes = canonical_payload_bytes(&document.payload())?;
    verifying_key
        .verify(&payload_bytes, &signature)
        .map_err(|_| LicenseError::InvalidSignature)?;

    Entitlements::from_verified_license(
        CustomerId::new(document.customer_id)?,
        document.customer_name,
        document.issued_at,
        document.expires_at,
        document.features,
        EntitlementLimits {
            max_principals: document.limits.max_principals,
        },
    )
}

/// Validate every signed payload field before issuance.
pub fn validate_license_payload(payload: &LicensePayload) -> Result<(), LicenseError> {
    let _customer = CustomerId::new(payload.customer_id.clone())?;
    if payload.customer_name.trim().is_empty() || payload.customer_name.len() > 256 {
        return Err(LicenseError::InvalidField("customer_name"));
    }
    if payload.issued_at >= payload.expires_at {
        return Err(LicenseError::InvalidField("expires_at"));
    }
    if payload
        .expires_at
        .checked_add_signed(chrono::Duration::days(14))
        .is_none()
    {
        return Err(LicenseError::InvalidField("expires_at"));
    }
    let unique = payload.features.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != payload.features.len() {
        return Err(LicenseError::InvalidField("features"));
    }
    if payload.limits.max_principals == Some(0) {
        return Err(LicenseError::InvalidField("limits.max_principals"));
    }
    Ok(())
}

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        Value::Object(values) => {
            let sorted = values
                .into_iter()
                .map(|(key, value)| (key, canonicalize(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect())
        }
        scalar => scalar,
    }
}

/// Read exactly 32 raw key bytes from a hex or base64url text file.
pub fn read_key_file(path: &Path) -> Result<[u8; 32], LicenseError> {
    let encoded =
        std::fs::read_to_string(path).map_err(|error| LicenseError::Read(error.to_string()))?;
    let encoded = encoded.trim();
    let bytes = if encoded.len() == 64 && encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        (0..32)
            .map(|index| u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| LicenseError::InvalidField("key"))?
    } else {
        URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| LicenseError::InvalidField("key"))?
    };
    bytes
        .try_into()
        .map_err(|_| LicenseError::InvalidField("key"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use chrono::{TimeZone, Utc};
    use ed25519_dalek::{Signer, SigningKey};
    use proptest::prelude::*;

    use super::{
        canonical_payload_bytes, verify_license_bytes_with_key, Feature, LicenseLimits,
        LicensePayload, SignedLicense,
    };

    fn payload() -> LicensePayload {
        LicensePayload {
            customer_id: "customer:test".to_string(),
            customer_name: "Test Customer".to_string(),
            issued_at: Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap(),
            expires_at: Utc.with_ymd_and_hms(2031, 1, 1, 0, 0, 0).unwrap(),
            features: vec![Feature::Rbac, Feature::Constraints, Feature::AuditS3],
            limits: LicenseLimits {
                max_principals: Some(100),
            },
        }
    }

    fn signed_document(signing_key: &SigningKey) -> SignedLicense {
        let payload = payload();
        let signature = signing_key.sign(&canonical_payload_bytes(&payload).unwrap());
        SignedLicense::new(payload, URL_SAFE_NO_PAD.encode(signature.to_bytes()))
    }

    #[test]
    fn signed_license_verifies_and_preserves_features_after_expiry() {
        let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
        let body = serde_json::to_vec(&signed_document(&signing_key)).unwrap();

        let entitlements =
            verify_license_bytes_with_key(&body, &signing_key.verifying_key().to_bytes()).unwrap();
        assert!(entitlements.has(Feature::Rbac));
        assert!(entitlements.has(Feature::Constraints));
        assert!(entitlements.has(Feature::AuditS3));
        assert_eq!(entitlements.limits().max_principals, Some(100));
    }

    #[test]
    fn tampered_license_and_wrong_key_fail_verification() {
        let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
        let mut document = signed_document(&signing_key);
        document.customer_name.push_str(" tampered");
        let tampered = serde_json::to_vec(&document).unwrap();
        assert!(
            verify_license_bytes_with_key(&tampered, &signing_key.verifying_key().to_bytes())
                .is_err()
        );

        let original = serde_json::to_vec(&signed_document(&signing_key)).unwrap();
        let wrong_key = SigningKey::from_bytes(&[8_u8; 32]);
        assert!(
            verify_license_bytes_with_key(&original, &wrong_key.verifying_key().to_bytes())
                .is_err()
        );
    }

    #[test]
    fn duplicate_features_and_invalid_expiry_fail_before_acceptance() {
        let signing_key = SigningKey::from_bytes(&[9_u8; 32]);
        let mut duplicate = payload();
        duplicate.features.push(Feature::Rbac);
        let signature = signing_key.sign(&canonical_payload_bytes(&duplicate).unwrap());
        let document = SignedLicense::new(duplicate, URL_SAFE_NO_PAD.encode(signature.to_bytes()));
        assert!(verify_license_bytes_with_key(
            &serde_json::to_vec(&document).unwrap(),
            &signing_key.verifying_key().to_bytes(),
        )
        .is_err());

        let mut reversed = payload();
        reversed.expires_at = reversed.issued_at;
        let signature = signing_key.sign(&canonical_payload_bytes(&reversed).unwrap());
        let document = SignedLicense::new(reversed, URL_SAFE_NO_PAD.encode(signature.to_bytes()));
        assert!(verify_license_bytes_with_key(
            &serde_json::to_vec(&document).unwrap(),
            &signing_key.verifying_key().to_bytes(),
        )
        .is_err());
    }

    proptest! {
        #[test]
        fn arbitrary_license_bytes_never_panic(input in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let signing_key = SigningKey::from_bytes(&[11_u8; 32]);
            prop_assert!(
                verify_license_bytes_with_key(&input, &signing_key.verifying_key().to_bytes())
                    .is_err(),
                "arbitrary unsigned or forged input must never verify"
            );
        }
    }
}
