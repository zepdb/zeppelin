//! Credential adapters that resolve transport headers to typed principals.

use std::collections::HashMap;

use axum::http::{header::AUTHORIZATION, HeaderMap};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::config::SecurityConfig;

use super::{Principal, PrincipalId, SecurityError};

const SECRET_LEN: usize = 43;
const SECRET_BYTES: usize = 32;
const DUMMY_KEY_ID: &str = "zpk1_timing_dummy";
const DUMMY_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const DUMMY_DIGEST: [u8; 32] = [0x5a; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CredentialCandidate<'a> {
    key_id: &'a str,
    secret: &'a str,
    syntactically_valid: bool,
}

/// Stable failure family returned by a credential adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AuthnFailure {
    /// A protected request supplied no authorization header.
    #[error("authentication required")]
    Unauthenticated,
    /// A malformed, unknown, or incorrectly signed credential was supplied.
    #[error("credential unknown")]
    CredentialUnknown,
    /// The credential is known but past its configured expiry.
    #[error("credential expired")]
    CredentialExpired,
}

impl AuthnFailure {
    /// Stable lowercase reason code for envelopes and audit.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::CredentialUnknown => "credential_unknown",
            Self::CredentialExpired => "credential_expired",
        }
    }
}

/// Adapter boundary shared by API keys and future identity mechanisms.
pub trait CredentialAdapter: Send + Sync {
    /// Authenticate transport headers at a trusted server-derived instant.
    fn authenticate(
        &self,
        headers: &HeaderMap,
        now: DateTime<Utc>,
    ) -> Result<Principal, AuthnFailure>;
}

#[derive(Debug)]
struct StoredApiKey {
    principal: Principal,
    digest: [u8; 32],
}

/// Named API-key adapter backed only by hashed boot configuration.
#[derive(Debug)]
pub struct ApiKeyAdapter {
    keys: HashMap<String, StoredApiKey>,
}

impl ApiKeyAdapter {
    /// Compile validated API-key configuration without retaining any secret.
    pub fn from_config(config: &SecurityConfig) -> Result<Self, SecurityError> {
        let mut keys = HashMap::new();
        for key in &config.api_keys {
            let principal = Principal::api_key(
                PrincipalId::new(key.key_id.clone())?,
                key.name.clone(),
                key.expires_at,
            );
            let digest = decode_digest(&key.sha256_hex)?;
            if keys
                .insert(key.key_id.clone(), StoredApiKey { principal, digest })
                .is_some()
            {
                return Err(SecurityError::DuplicatePrincipal);
            }
        }
        Ok(Self { keys })
    }

    /// Authenticate one already-decoded Authorization header value.
    ///
    /// HTTP middleware uses this after enforcing the single-header contract;
    /// keeping the canonical parser here gives fuzzing and future adapters one
    /// production seam instead of a second token grammar.
    pub fn authenticate_bearer(
        &self,
        authorization: &str,
        now: DateTime<Utc>,
    ) -> Result<Principal, AuthnFailure> {
        let candidate = credential_candidate(authorization);
        let digest: [u8; 32] = Sha256::digest(candidate.secret.as_bytes()).into();
        let stored = self.keys.get(candidate.key_id);
        let expected = stored.map_or(&DUMMY_DIGEST, |key| &key.digest);
        let decoded = URL_SAFE_NO_PAD.decode(candidate.secret);
        let digest_matches: bool = digest.ct_eq(expected).into();
        let canonical_secret = decoded.is_ok_and(|decoded| decoded.len() == SECRET_BYTES);
        if !candidate.syntactically_valid
            || stored.is_none()
            || !canonical_secret
            || !digest_matches
        {
            return Err(AuthnFailure::CredentialUnknown);
        }

        let Some(stored) = stored else {
            return Err(AuthnFailure::CredentialUnknown);
        };
        if stored
            .principal
            .expires_at
            .is_some_and(|expires_at| expires_at <= now)
        {
            return Err(AuthnFailure::CredentialExpired);
        }
        Ok(stored.principal.clone())
    }
}

impl CredentialAdapter for ApiKeyAdapter {
    fn authenticate(
        &self,
        headers: &HeaderMap,
        now: DateTime<Utc>,
    ) -> Result<Principal, AuthnFailure> {
        let values = headers.get_all(AUTHORIZATION);
        let mut values = values.iter();
        let Some(value) = values.next() else {
            return Err(AuthnFailure::Unauthenticated);
        };
        if values.next().is_some() {
            consume_unknown_profile(self, now);
            return Err(AuthnFailure::CredentialUnknown);
        }
        let Ok(value) = value.to_str() else {
            consume_unknown_profile(self, now);
            return Err(AuthnFailure::CredentialUnknown);
        };
        self.authenticate_bearer(value, now)
    }
}

fn credential_candidate(value: &str) -> CredentialCandidate<'_> {
    let Some((key_id, secret)) = parse_bearer(value) else {
        return CredentialCandidate {
            key_id: DUMMY_KEY_ID,
            secret: DUMMY_SECRET,
            syntactically_valid: false,
        };
    };
    if secret.len() != SECRET_LEN
        || secret
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')))
    {
        return CredentialCandidate {
            key_id: DUMMY_KEY_ID,
            secret: DUMMY_SECRET,
            syntactically_valid: false,
        };
    }
    CredentialCandidate {
        key_id,
        secret,
        syntactically_valid: true,
    }
}

fn parse_bearer(value: &str) -> Option<(&str, &str)> {
    let token = value.strip_prefix("Bearer ")?;
    let (key_id, secret) = token.split_once('.')?;
    if !key_id.starts_with("zpk1_")
        || key_id.len() <= "zpk1_".len()
        || secret.contains('.')
        || key_id
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')))
    {
        return None;
    }
    Some((key_id, secret))
}

fn consume_unknown_profile(adapter: &ApiKeyAdapter, now: DateTime<Utc>) {
    let _ = std::hint::black_box(adapter.authenticate_bearer("not a bearer", now));
}

fn decode_digest(value: &str) -> Result<[u8; 32], SecurityError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SecurityError::InvalidApiKeyDigest);
    }
    let mut decoded = [0_u8; 32];
    for (index, output) in decoded.iter_mut().enumerate() {
        let start = index * 2;
        *output = u8::from_str_radix(&value[start..start + 2], 16)
            .map_err(|_| SecurityError::InvalidApiKeyDigest)?;
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::{credential_candidate, parse_bearer, DUMMY_KEY_ID, DUMMY_SECRET, SECRET_LEN};

    #[test]
    fn bearer_parser_requires_canonical_shape() {
        assert_eq!(
            parse_bearer("Bearer zpk1_key.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            Some(("zpk1_key", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
        );
        assert!(parse_bearer("bearer zpk1_key.secret").is_none());
        assert!(parse_bearer("Bearer zpk1_key.secret.extra").is_none());
        assert!(parse_bearer("Bearer key.secret").is_none());
    }

    #[test]
    fn malformed_bearers_normalize_to_fixed_unknown_work() {
        let malformed = credential_candidate("not a bearer");
        assert_eq!(malformed.key_id, DUMMY_KEY_ID);
        assert_eq!(malformed.secret, DUMMY_SECRET);
        assert_eq!(malformed.secret.len(), SECRET_LEN);
        assert!(!malformed.syntactically_valid);

        let unknown_header = format!("Bearer zpk1_missing.{DUMMY_SECRET}");
        let unknown = credential_candidate(&unknown_header);
        assert_eq!(unknown.secret.len(), malformed.secret.len());
        assert!(unknown.syntactically_valid);
    }
}
