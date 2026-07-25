//! Typed authenticated identity used by every protected operation.
//!
//! This file owns the vocabulary of *who is acting*: the validated identifier
//! newtype [`PrincipalId`], the origin taxonomy [`PrincipalKind`], the
//! credential-strength taxonomy [`AuthStrength`], and the [`Principal`] value
//! that carries all three plus the credential facts an authorization decision
//! and an audit record need.
//!
//! It is a leaf type module. It deliberately does **not** own:
//!
//! - how an identity is established. Credential parsing and digest comparison
//!   live in `src/security/authn.rs`;
//! - what an identity may do. Grants and decisions live in
//!   `src/security/policy.rs` and [`SecurityKernel`][super::SecurityKernel];
//! - delegated-token verification. This file only stores the already-verified
//!   [`DelegationContext`] produced by
//!   `src/security/delegation.rs`.
//!
//! ## Where this sits
//!
//! ```text
//!   credential (untrusted bytes)
//!         |
//!         | authn.rs / delegation.rs — the ONLY producers
//!         v
//!     Principal  (this file)
//!         |
//!         +--> request extensions in src/server/mod.rs
//!         +--> SecurityKernel authorization
//!         +--> AuditRecord (copies id / kind / delegation_parent by value)
//! ```
//!
//! No object-storage state is read or written here; a [`Principal`] exists for
//! the lifetime of one request and is never persisted. The durable policy
//! snapshot stores a `PolicyKey` carrying a [`PrincipalId`], not a whole
//! [`Principal`] — identity on disk is a *reference*, and the live identity is
//! reconstructed by authentication on every request.
//!
//! ## Invariants this file protects
//!
//! - **An identity is produced, never parsed.** See the Rust note below; this
//!   is the load-bearing security property of the file.
//! - **Identifiers are validated at construction.** [`PrincipalId::new`]
//!   enforces a bounded ASCII grammar (non-empty, at most 128 bytes,
//!   alphanumeric plus `-`, `_`, `:`, `.`) and returns
//!   [`SecurityError::InvalidPrincipalId`][super::SecurityError] otherwise.
//!   The inner `String` is private, so an unvalidated identifier cannot be
//!   fabricated from outside this module. That grammar is the same one used
//!   for namespace-safe identifiers: a principal id appears in audit keys and
//!   log fields, so it must never carry a path separator or whitespace.
//! - **Constructors encode strength honestly.** Only
//!   [`Principal::api_key`] and the crate-private `authenticated_api_key` set
//!   [`AuthStrength::ApiKey`]; only the crate-private `delegated` constructor
//!   sets [`AuthStrength::DelegatedToken`] and it always records the parent
//!   principal and the token expiry. There is no constructor that lets a caller
//!   choose an arbitrary strength.
//! - **Anonymous is explicit, never a fallback.** [`Principal::anonymous`]
//!   exists for routes classified public and for `open_unsafe` mode, and
//!   [`Principal::is_anonymous`] lets callers test for it. It is never
//!   substituted for a failed authentication — a failed credential produces an
//!   `AuthnFailure` and the request is rejected.
//! - **A delegated principal is not its parent.** `id` becomes the token id and
//!   `kind` becomes [`PrincipalKind::Agent`], so token-scoped rate limiting and
//!   audit cannot be confused with the parent key's own activity, while
//!   `delegation_parent` preserves the link the kernel needs to intersect the
//!   parent's current grants.
//!
//! ## Where to start reading
//!
//! [`Principal`] and its four fields' meanings, then the three public
//! constructors. Everything else is a small validated newtype.
//!
//! ## Rust concepts used here
//!
//! **[`Principal`] derives `Serialize` but deliberately not `Deserialize`.**
//! This is not an oversight and must not be "fixed". An identity is an output
//! of authentication, and there is no legitimate input from which one could be
//! parsed. Because serde's derives are structural, the omission propagates: no
//! type that contains a `Principal` can derive `Deserialize` either, so the
//! compiler — not a code review — is what prevents a request body, a cached
//! blob, or a replayed audit line from ever becoming a live identity. Rejecting
//! a forged identity at runtime is a check that can be forgotten; making the
//! type unparseable is a check that cannot be. `Serialize` remains because
//! projecting an identity outward, into diagnostics and structured logs, is
//! safe and useful.
//!
//! The `delegation` field carries `#[serde(skip_serializing_if)]`, so anything
//! that serializes a [`Principal`] must use a self-describing format (JSON,
//! MessagePack); the repository forbids `bincode` for such trees. The
//! accompanying `#[serde(default)]` is inert here precisely because there is no
//! `Deserialize` impl — it documents intent for a future reader rather than
//! affecting behavior.
//!
//! [`PrincipalId`] is a newtype over `String` with a private field: a "parse,
//! don't validate" wrapper. Java would express this as a final class with a
//! validating constructor, but nothing stops a Java caller from passing a raw
//! `String` to a method expecting an id; here the types simply do not match. C
//! has no equivalent at all — a `char *` is a `char *`. `#[serde(transparent)]`
//! keeps the wire form a plain string, so the extra type costs nothing on disk.
//!
//! [`PrincipalKind`] and [`AuthStrength`] are `Copy` field-less enums: one word
//! each, passed by value, and matched exhaustively so a new variant forces
//! every decision site to be revisited. [`Principal`] itself is `Clone` but not
//! `Copy` — it owns a `String` display name and optional heap fields, so each
//! `clone()` really does allocate. The authentication path clones one per
//! successful request, which is the intended cost of handing an owned identity
//! to a handler rather than lending a borrow across an `.await`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{ApiKeyId, DelegationContext, SecurityError};

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
    /// A short-lived Ed25519 token narrowed from a current parent principal.
    DelegatedToken,
}

/// Authenticated identity passed to central authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    /// Signed narrowing and token identity for delegated request authorization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation: Option<DelegationContext>,
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
            delegation: None,
        }
    }

    pub(crate) fn delegated(context: DelegationContext) -> Self {
        let token_id = context.token_id().to_string();
        let parent = context.parent_principal().clone();
        Self {
            id: PrincipalId(token_id.clone()),
            kind: PrincipalKind::Agent,
            display_name: token_id,
            auth_strength: AuthStrength::DelegatedToken,
            expires_at: Some(context.expires_at()),
            api_key_id: None,
            delegation_parent: Some(parent),
            delegation: Some(context),
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
            delegation: None,
        }
    }

    /// Return whether this identity was established without a credential.
    #[must_use]
    pub const fn is_anonymous(&self) -> bool {
        matches!(self.kind, PrincipalKind::Anonymous)
    }
}
