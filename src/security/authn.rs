//! Credential adapters that resolve transport headers to typed principals.
//!
//! This file owns *authentication only*: turning an untrusted
//! `Authorization` header into either a typed
//! [`Principal`] or a redacted [`AuthnFailure`]. It answers
//! "who is calling?" and never "may they do this?".
//!
//! It deliberately does **not** own:
//!
//! - authorization. Once a principal exists, the decision belongs to
//!   [`SecurityKernel`][super::SecurityKernel];
//! - the credential registry. Digests and key states live in the
//!   S3-authoritative policy snapshot (`src/security/policy.rs`), reached
//!   through the read cache in `src/security/policy_cache.rs`;
//! - delegated-token cryptography. `Bearer zpt1_…` is forwarded verbatim to
//!   the `DelegationVerifier` in `src/security/delegation.rs`;
//! - HTTP concerns. Extracting the header, mapping failures to status codes,
//!   rate limiting, and audit emission all live in `src/server/mod.rs`.
//!
//! ## Where this sits
//!
//! `src/server/mod.rs` runs one authentication middleware in front of every
//! protected route. It calls [`CredentialAdapter::authenticate_with_policy`]
//! with a server-derived instant, never a client-supplied one, then inserts the
//! resolved principal into request extensions for the authorization layer.
//! [`build_app`](crate::startup::build_app) chooses the concrete adapter:
//! [`ApiKeyAdapter`] built from boot configuration when `security.rbac` is
//! off, or one bound to the live policy cache when it is on.
//!
//! ```text
//! Authorization header (untrusted)
//!         |
//!         | exactly one header value, valid UTF-8, else unknown-profile work
//!         v
//!   "Bearer zpt1_…" ---------> DelegationVerifier (delegation.rs)
//!         |                          |
//!         | "Bearer zpk1_<id>.<secret>"
//!         v                          |
//!   parse_bearer -> CredentialCandidate                 |
//!         |   malformed => fixed dummy id + dummy secret|
//!         v                                             |
//!   SHA-256(secret), constant-time compare vs stored digest
//!         |                                             |
//!         v                                             v
//!   AuthenticationOutcome { result, policy_version, policy_fresh }
//!         |
//!         +-- policy_fresh == false => middleware denies with SecurityStale
//!         +-- Ok(Principal) ........ authorization proceeds
//!         +-- Err(AuthnFailure) .... audited, request rejected
//! ```
//!
//! ## Invariants this file protects
//!
//! - **No secret is ever stored.** Both credential sources hold a SHA-256
//!   digest. [`ApiKeyAdapter::from_config`] hashes nothing itself — it decodes
//!   an operator-supplied hex digest and rejects anything that is not exactly
//!   32 hex-encoded bytes.
//! - **Comparison is constant-time and shape-normalizing.** Digest equality
//!   uses `subtle`'s `ct_eq`. Every rejected shape — no header, several
//!   headers, a non-UTF-8 value, a bad prefix, a wrong-length secret — is
//!   routed through the *same* amount of work against a fixed dummy key id and
//!   a dummy secret of exactly the canonical length, so an attacker cannot
//!   distinguish "unknown key" from "malformed header" by timing. The
//!   `black_box` call in `consume_unknown_profile` exists so the optimizer
//!   cannot delete that work.
//! - **Failure families are redacted on purpose.** [`AuthnFailure`] has three
//!   variants and [`AuthnFailure::code`] gives each a stable string for
//!   envelopes and audit. A revoked key deliberately reports
//!   `credential_unknown`, not `credential_expired`; the distinction would
//!   confirm the key had once existed.
//! - **Expiry is evaluated against the caller-supplied `now`,** which the HTTP
//!   layer derives from the server clock. An expired credential is
//!   `CredentialExpired`; a revoked one whose revocation time has arrived is
//!   `CredentialUnknown`.
//! - **The evaluated policy version travels with the result.**
//!   [`AuthenticationOutcome`] reports the exact
//!   [`PolicyVersion`] used for the lookup *and* whether
//!   that snapshot is still inside its freshness bound. This is the
//!   fail-closed hinge: a stale cache denies the request rather than
//!   authenticating against possibly-revoked credentials. The bootstrap source
//!   reports `PolicyVersion::BOOT` and is always fresh, because configuration
//!   cannot go stale relative to itself.
//! - **One token grammar.** [`ApiKeyAdapter::authenticate_bearer`] is the
//!   single production parser, shared by middleware and fuzz targets, so a
//!   second grammar cannot drift into existence.
//!
//! ## Where to start reading
//!
//! [`ApiKeyAdapter::authenticate_bearer_with_policy`] is the whole decision.
//! Read it, then the private `credential_candidate` and `parse_bearer` helpers
//! for the accepted shape, then [`CredentialAdapter`] for the boundary the HTTP
//! layer depends on.
//!
//! ## Rust concepts used here
//!
//! The private `ApiKeySource` enum makes an invalid combination
//! unrepresentable: an adapter is *either* a fixed bootstrap map *or* a live
//! policy cache, never both and never neither. An exhaustive `match` on it
//! means adding a third credential source will not compile until every branch
//! is handled — the compiler enforces what a Java `instanceof` chain would only
//! suggest.
//!
//! `CredentialCandidate<'a>` borrows slices *into* the caller's header string
//! rather than allocating owned `String`s per request. The lifetime `'a` ties
//! the candidate to the header buffer, so the compiler guarantees it cannot
//! outlive the bytes it points at. In C this would be two `const char *` plus a
//! length and an unwritten rule about the buffer's lifetime; here the rule is
//! checked.
//!
//! [`CredentialAdapter`] is `Send + Sync` because a single adapter instance is
//! shared behind an `Arc` by every concurrent request handler. Note that the
//! trait's methods take `&self` and are *not* `async`: the policy cache read is
//! a short lock-and-clone of an `Arc<CachedPolicy>`, so no guard is held across
//! an `.await`.
//!
//! `Result<Principal, AuthnFailure>` stored *inside*
//! [`AuthenticationOutcome`] is deliberate. The policy version and freshness
//! flag are needed on the failure path too — for audit and for the stale-policy
//! denial — so the error cannot be the outer type.

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{header::AUTHORIZATION, HeaderMap};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::config::SecurityConfig;

use super::{
    delegation::DelegationVerifier, policy_cache::PolicyCache, ApiKeyId, KeyState, PolicyVersion,
    Principal, PrincipalId, SecurityError,
};

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

/// Authentication result paired with the exact policy snapshot evaluated.
#[derive(Debug, Clone)]
pub struct AuthenticationOutcome {
    /// Resolved principal or stable redacted failure family.
    pub result: Result<Principal, AuthnFailure>,
    /// Authoritative policy version used by this authentication attempt.
    pub policy_version: PolicyVersion,
    /// Whether the evaluated policy snapshot remains inside the fail-closed bound.
    pub policy_fresh: bool,
}

/// Adapter boundary shared by API keys and future identity mechanisms.
pub trait CredentialAdapter: Send + Sync {
    /// Authenticate transport headers and retain the exact evaluated policy version.
    fn authenticate_with_policy(
        &self,
        headers: &HeaderMap,
        now: DateTime<Utc>,
    ) -> AuthenticationOutcome;

    /// Authenticate transport headers at a trusted server-derived instant.
    fn authenticate(
        &self,
        headers: &HeaderMap,
        now: DateTime<Utc>,
    ) -> Result<Principal, AuthnFailure> {
        self.authenticate_with_policy(headers, now).result
    }

    /// Return the exact policy version used for current credential lookup.
    fn policy_version(&self) -> PolicyVersion {
        self.policy_freshness().0
    }

    /// Return one atomic policy-version/freshness observation for pre-auth fail-closed checks.
    fn policy_freshness(&self) -> (PolicyVersion, bool);
}

#[derive(Debug)]
struct StoredApiKey {
    principal: Principal,
    digest: [u8; 32],
}

enum ApiKeySource {
    Bootstrap(HashMap<String, StoredApiKey>),
    Policy(Arc<PolicyCache>),
}

/// Named API-key adapter backed only by hashed credential material.
pub struct ApiKeyAdapter {
    source: ApiKeySource,
    delegation: Option<Arc<DelegationVerifier>>,
}

impl ApiKeyAdapter {
    /// Compile validated API-key configuration without retaining any secret.
    pub fn from_config(config: &SecurityConfig) -> Result<Self, SecurityError> {
        let mut keys = HashMap::new();
        for key in &config.api_keys {
            let principal = Principal::api_key(
                PrincipalId::new(key.key_id.clone())?,
                ApiKeyId::new(key.key_id.clone())?,
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
        Ok(Self {
            source: ApiKeySource::Bootstrap(keys),
            delegation: None,
        })
    }

    pub(crate) fn from_policy_cache(cache: Arc<PolicyCache>) -> Self {
        Self {
            source: ApiKeySource::Policy(cache),
            delegation: None,
        }
    }

    pub(crate) fn from_policy_cache_with_delegation(
        cache: Arc<PolicyCache>,
        delegation: Arc<DelegationVerifier>,
    ) -> Self {
        Self {
            source: ApiKeySource::Policy(cache),
            delegation: Some(delegation),
        }
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
        self.authenticate_bearer_with_policy(authorization, now)
            .result
    }

    /// Authenticate a decoded bearer and retain the exact evaluated policy version.
    #[must_use]
    pub fn authenticate_bearer_with_policy(
        &self,
        authorization: &str,
        now: DateTime<Utc>,
    ) -> AuthenticationOutcome {
        if authorization.starts_with("Bearer zpt1_") {
            let (result, policy_version, policy_fresh) = self.delegation.as_ref().map_or_else(
                || {
                    let (policy_version, policy_fresh) = self.current_policy_freshness();
                    (
                        Err(AuthnFailure::CredentialUnknown),
                        policy_version,
                        policy_fresh,
                    )
                },
                |delegation| delegation.verify_authorization(authorization),
            );
            return AuthenticationOutcome {
                result,
                policy_version,
                policy_fresh,
            };
        }
        let candidate = credential_candidate(authorization);
        let digest: [u8; 32] = Sha256::digest(candidate.secret.as_bytes()).into();
        let decoded = URL_SAFE_NO_PAD.decode(candidate.secret);
        let canonical_secret = decoded.is_ok_and(|decoded| decoded.len() == SECRET_BYTES);

        match &self.source {
            ApiKeySource::Bootstrap(keys) => {
                let stored = keys.get(candidate.key_id);
                let expected = stored.map_or(&DUMMY_DIGEST, |key| &key.digest);
                let digest_matches: bool = digest.ct_eq(expected).into();
                let result = if !candidate.syntactically_valid
                    || stored.is_none()
                    || !canonical_secret
                    || !digest_matches
                {
                    Err(AuthnFailure::CredentialUnknown)
                } else if let Some(stored) = stored {
                    if stored
                        .principal
                        .expires_at
                        .is_some_and(|expires_at| expires_at <= now)
                    {
                        Err(AuthnFailure::CredentialExpired)
                    } else {
                        Ok(stored.principal.clone())
                    }
                } else {
                    Err(AuthnFailure::CredentialUnknown)
                };
                AuthenticationOutcome {
                    result,
                    policy_version: PolicyVersion::BOOT,
                    policy_fresh: true,
                }
            }
            ApiKeySource::Policy(cache) => {
                let cached = cache.current();
                let policy_version = cached.policy.version();
                let stored = cached.policy.key(candidate.key_id);
                let expected = stored.map_or(&DUMMY_DIGEST, |key| &key.digest);
                let digest_matches: bool = digest.ct_eq(expected).into();
                let result = if !candidate.syntactically_valid
                    || stored.is_none()
                    || !canonical_secret
                    || !digest_matches
                {
                    Err(AuthnFailure::CredentialUnknown)
                } else if let Some(stored) = stored {
                    if stored.state == KeyState::Revoked
                        && stored.revokes_at.is_none_or(|revokes_at| revokes_at <= now)
                    {
                        Err(AuthnFailure::CredentialUnknown)
                    } else if stored.state == KeyState::Expired
                        || stored
                            .expires_at
                            .is_some_and(|expires_at| expires_at <= now)
                    {
                        Err(AuthnFailure::CredentialExpired)
                    } else {
                        Ok(stored.principal.clone())
                    }
                } else {
                    Err(AuthnFailure::CredentialUnknown)
                };
                AuthenticationOutcome {
                    result,
                    policy_version,
                    policy_fresh: cache.is_fresh(&cached),
                }
            }
        }
    }

    fn current_policy_freshness(&self) -> (PolicyVersion, bool) {
        match &self.source {
            ApiKeySource::Bootstrap(_) => (PolicyVersion::BOOT, true),
            ApiKeySource::Policy(cache) => cache.freshness(),
        }
    }
}

impl CredentialAdapter for ApiKeyAdapter {
    fn authenticate_with_policy(
        &self,
        headers: &HeaderMap,
        now: DateTime<Utc>,
    ) -> AuthenticationOutcome {
        let values = headers.get_all(AUTHORIZATION);
        let mut values = values.iter();
        let Some(value) = values.next() else {
            let mut outcome = self.authenticate_bearer_with_policy("not a bearer", now);
            outcome.result = Err(AuthnFailure::Unauthenticated);
            return outcome;
        };
        if values.next().is_some() {
            return consume_unknown_profile(self, now);
        }
        let Ok(value) = value.to_str() else {
            return consume_unknown_profile(self, now);
        };
        self.authenticate_bearer_with_policy(value, now)
    }

    fn policy_freshness(&self) -> (PolicyVersion, bool) {
        self.current_policy_freshness()
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

fn consume_unknown_profile(adapter: &ApiKeyAdapter, now: DateTime<Utc>) -> AuthenticationOutcome {
    std::hint::black_box(adapter.authenticate_bearer_with_policy("not a bearer", now))
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
