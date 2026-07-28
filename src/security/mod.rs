//! Central authentication, authorization, and decision vocabulary.
//!
//! Transport adapters resolve credentials to typed principals. The security
//! kernel then evaluates one exhaustive [`Action`](crate::security::Action) against a typed resource and
//! returns an explicit decision before domain work can begin.
//!
//! Everything in this subsystem is **fail-closed**. When state is missing,
//! stale, unverifiable, or unlicensed, the answer is a typed denial. There are
//! no permissive defaults and no degraded modes: a policy cache that cannot
//! revalidate stops authorizing, and an audit writer that cannot durably record
//! a mutation withholds `/readyz` rather than letting the mutation go
//! unrecorded.
//!
//! ## What this subsystem owns
//!
//! - the identity vocabulary ([`Principal`](crate::security::Principal), [`PrincipalId`](crate::security::PrincipalId), [`AuthStrength`](crate::security::AuthStrength))
//!   and the credential adapters that produce it;
//! - the closed inventory of operations ([`Action`](crate::security::Action)) and the resources they act
//!   on ([`Resource`](crate::security::Resource)), plus the route table ([`classify_route`](crate::security::classify_route)) that maps an
//!   HTTP route onto them;
//! - the decision values ([`Decision`](crate::security::Decision), [`AllowDecision`](crate::security::AllowDecision), [`DenyDecision`](crate::security::DenyDecision),
//!   [`Obligation`](crate::security::Obligation)) that carry constraints downstream;
//! - the authoritative policy document in the reserved `_security/` keyspace,
//!   and its cache;
//! - licensed feature authority ([`Entitlements`](crate::security::Entitlements), [`Feature`](crate::security::Feature));
//! - durable, hash-chained audit evidence;
//! - preservation locks that veto destruction.
//!
//! It does **not** own HTTP plumbing (that is `src/server/`), domain work such
//! as namespace or WAL mutation, or object-store access, which always goes
//! through `src/storage/`. Authorization decides; it does not perform.
//!
//! ## Request admission
//!
//! ```text
//!            HTTP request
//!                 |
//!                 v
//!  server middleware: CredentialAdapter (ApiKeyAdapter)
//!                 |  bearer digest -> Principal, never a stored secret
//!                 v
//!  classify_route(method, matched path) -> RouteClass
//!                 |  Protected(Action) | Public | (unmapped => denied)
//!                 v
//!  SecurityKernel::authorize_* / guard_*        <-- the admission point
//!         |                        |
//!         | Bootstrap authority    | Policy authority (licensed RBAC)
//!         | grants compiled from   | compiled from the S3-authoritative
//!         | boot config            | policy head, revalidated on an interval
//!         v                        v
//!            Decision::Allow(..)  |  Decision::Deny(..)
//!                 |                        |
//!                 | obligations:           +--> typed error, no domain work
//!                 |   mandatory filter,
//!                 |   field mask,
//!                 |   write constraints,
//!                 |   two-person approval
//!                 v
//!         domain work runs, then durable audit records the outcome
//! ```
//!
//! The two authorities are selected at boot by resolved entitlements. Without
//! [`Feature::Rbac`](crate::security::Feature::Rbac), the kernel compiles immutable grants from validated boot
//! configuration and never constructs the object-store policy registry. With
//! it, the authoritative policy in `_security/` takes over and boot credentials
//! become advisory only.
//!
//! ## Persisted artifacts
//!
//! All authoritative security state lives under the reserved `_security/`
//! prefix: `heads/policy.json` and immutable `policies/<ulid>.json` (policy),
//! `leases/policy-publication.json` ([`POLICY_PUBLICATION_LEASE_KEY`](crate::security::POLICY_PUBLICATION_LEASE_KEY)),
//! `preservation/`, `signers/` and `signer-slots/` (delegation),
//! `audit-writers/`, and `migrations/`. S3 or MinIO is authoritative for every
//! one of them; process memory only caches what it has verified.
//!
//! ## Reading map
//!
//! Approach the subsystem in this order; each layer assumes the previous one.
//!
//! 1. **Vocabulary** — `action.rs` ([`Action`](crate::security::Action)), `resource.rs` ([`Resource`](crate::security::Resource)),
//!    `principal.rs` ([`Principal`](crate::security::Principal)), `context.rs` ([`RequestContext`](crate::security::RequestContext)),
//!    `decision.rs` ([`Decision`](crate::security::Decision)). Nothing else makes sense first.
//! 2. **Route mapping** — `route_map.rs` ([`classify_route`](crate::security::classify_route),
//!    [`ROUTE_ACTIONS`](crate::security::ROUTE_ACTIONS)): how an Axum route becomes an [`Action`](crate::security::Action), and why an
//!    unmapped protected route is an error rather than a pass.
//! 3. **Authentication** — `authn.rs` ([`CredentialAdapter`](crate::security::CredentialAdapter),
//!    [`ApiKeyAdapter`](crate::security::ApiKeyAdapter), [`AuthenticationOutcome`](crate::security::AuthenticationOutcome)): credentials to principals,
//!    digest-only, never retaining secret material.
//! 4. **The kernel** — `kernel.rs` ([`SecurityKernel`](crate::security::SecurityKernel)): the single admission
//!    point every protected route passes through, and the place entitlements
//!    are meant to be enforced.
//! 5. **Authoritative policy** — `policy.rs` ([`PolicyHead`](crate::security::PolicyHead),
//!    [`PolicySnapshot`](crate::security::PolicySnapshot), [`canonical_policy_checksum`](crate::security::canonical_policy_checksum)) for the persisted
//!    vocabulary, then `policy_store.rs` ([`PolicyStore`](crate::security::PolicyStore), [`LoadedPolicy`](crate::security::LoadedPolicy))
//!    for load/bootstrap/CAS publication, then `policy_cache.rs` for the
//!    disposable read cache and its staleness rule, then
//!    `policy_publication.rs` ([`PolicyPublicationLease`](crate::security::PolicyPublicationLease)) for the global
//!    fenced publication lease and branch-activation guards.
//! 6. **Licensing** — `entitlements.rs` ([`Entitlements`](crate::security::Entitlements), [`Feature`](crate::security::Feature)) and
//!    `license.rs` ([`SignedLicense`](crate::security::SignedLicense), [`EntitlementResolver`](crate::security::EntitlementResolver),
//!    [`FileLicenseResolver`](crate::security::FileLicenseResolver)): offline Ed25519 verification of which surfaces
//!    exist at all.
//! 7. **Audit** — `audit.rs` ([`AuditRecord`](crate::security::AuditRecord), [`AuditParams`](crate::security::AuditParams)),
//!    `audit_sink.rs` ([`AuditClient`](crate::security::AuditClient), [`AuditRuntime`](crate::security::AuditRuntime)), and
//!    `audit_chain.rs` ([`verify_audit_day`](crate::security::verify_audit_day)).
//! 8. **Constraint enforcement** — `constraints.rs` ([`apply_field_mask`](crate::security::apply_field_mask),
//!    [`filter_references_denied_field`](crate::security::filter_references_denied_field)): the server-owned obligations an
//!    allow decision carries into the query and write paths.
//! 9. **Licensed surfaces** — `delegation.rs` ([`DelegationContext`](crate::security::DelegationContext),
//!    [`IssuedDelegatedToken`](crate::security::IssuedDelegatedToken)) for short-lived credentials that can only
//!    narrow parent authority; `preservation.rs`
//!    ([`PreservationService`](crate::security::PreservationService), [`PreservationGuard`](crate::security::PreservationGuard)) for legal-hold vetoes
//!    over destruction.
//!
//! ## Invariants
//!
//! - **Deny by default.** An unmapped protected route, a missing principal, a
//!   missing request context, a stale policy cache, and an unresolvable
//!   preservation state all deny. See [`SecurityError`](crate::security::SecurityError) for the exhaustive
//!   failure vocabulary and its stable machine-readable codes.
//! - **Decisions precede work.** The kernel is consulted before domain logic
//!   runs, and an allow decision carries obligations the caller must apply.
//! - **S3 is authoritative for policy.** Once a policy head exists, boot
//!   configuration is ignored; drift is warned about, never reconciled by
//!   overwriting.
//! - **Entitlements belong in the kernel.** A handler-only feature check is
//!   bypassable from a new call site. [`Feature`](crate::security::Feature) variants have a stable
//!   bit-assignment order (`Feature::ALL`); append, never reorder, or existing
//!   signed licenses become invalid.
//! - **Audit is not best effort where policy says it must be durable.** A
//!   failed audit writer withholds readiness deliberately.
//! - **Errors carry their decision.** [`SecurityOperationError`](crate::security::SecurityOperationError) pairs a
//!   failure with the exact [`Decision`](crate::security::Decision) already evaluated, so a mutation that
//!   fails after authorization still produces a truthful audit record.
//! - **Redaction at the boundary.** [`SecurityError::client_message`](crate::security::SecurityError::client_message) and
//!   [`SecurityError::code`](crate::security::SecurityError::code) are the only shapes that reach a client; internal
//!   detail stays in structured logs.
//!
//! ## Known limitation
//!
//! Policy publication requires ETag compare-and-swap. `object_store`'s
//! `LocalFileSystem` does not implement conditional update, so on
//! `StorageBackend::Local` the publication-lease renew and release fail with
//! `Storage(NotImplemented)` and **first boot against a `Local`-backed store
//! currently fails**. `Local` is development/testing only and S3/MinIO are
//! unaffected; see `src/security/CLAUDE.md` for the three unit tests that are
//! red for exactly this reason.
//!
//! ## Rust concepts used here
//!
//! The submodules are all private and this file re-exports their public surface
//! with `pub use`. That is deliberate: callers depend on
//! `crate::security::Action`, not on a file layout, so internals can move
//! without breaking anything outside. A handful of items are exported
//! `pub(crate)` instead — the cursor binding key and delegated-fork admission
//! types — because they are seams between Zeppelin's own modules rather than
//! API for external users. Java's package-private is the
//! closest analogy, but `pub(crate)` is enforced across the whole crate rather
//! than one namespace.
//!
//! [`Action`](crate::security::Action) and [`Resource`](crate::security::Resource) are enums, not strings. An action that is not in
//! the inventory cannot be constructed, so "unknown action" is a parse-time
//! error ([`SecurityError::UnknownAction`](crate::security::SecurityError::UnknownAction)) rather than a silent mismatch, and
//! adding an operation forces every exhaustive `match` in the subsystem to be
//! updated. This is the type system carrying an invariant that a string-keyed
//! permission table in Java or C would leave to review discipline.
//!
//! [`SecurityOperationError`](crate::security::SecurityOperationError) boxes both its [`Decision`](crate::security::Decision) and its underlying
//! error. Boxing keeps the `Result` return values small on hot paths where
//! nearly every call succeeds, at the cost of one allocation on the rare
//! failure — the opposite tradeoff from an exception-based design, where the
//! failure path is the expensive one and the success path is free.

mod action;
mod audit;
mod audit_chain;
mod audit_sink;
mod authn;
mod constraints;
mod context;
mod decision;
mod delegation;
mod entitlements;
mod kernel;
mod license;
mod policy;
mod policy_cache;
mod policy_publication;
mod policy_store;
mod preservation;
mod principal;
mod resource;
mod route_map;

pub use crate::namespace::NamespaceId;
pub use action::Action;
pub use audit::{
    AuditChainPosition, AuditOutcome, AuditParams, AuditRecord, AuditedVectorIds,
    IndexConfigValues, PreservationBlockedSurface, ResourceRef, RootReleaseAuditProgress,
    RootReleaseFailureClass, RuntimeConfigValues, MAX_AUDITED_VECTOR_IDS,
};
pub use audit_chain::{
    verify_audit_day, AuditChainDivergence, AuditChainVerification, AuditDayAnchor,
};
pub use audit_sink::{AuditClient, AuditRuntime, AuditSinkError};
pub use authn::{ApiKeyAdapter, AuthenticationOutcome, AuthnFailure, CredentialAdapter};
pub(crate) use constraints::filter_matches_write_scope;
pub use constraints::{apply_field_mask, filter_references_denied_field};
pub use context::RequestContext;
pub(crate) use decision::CursorBindingKey;
pub use decision::{
    AllowDecision, Decision, DecisionId, DenyDecision, DenyReason, FieldMask, Obligation,
    PolicyVersion, WriteConstraints,
};
pub use delegation::{DelegationContext, DelegationNarrowing, IssuedDelegatedToken};
pub use entitlements::{CustomerId, EntitlementLimits, EntitlementSource, Entitlements, Feature};
pub use kernel::SecurityKernel;
pub(crate) use kernel::{NamespaceDeleteAdmission, NamespaceForkAdmission};
#[cfg(feature = "managed")]
pub use license::ControlPlaneResolver;
pub use license::{
    canonical_payload_bytes, read_key_file, validate_license_payload, verify_signed_license_bytes,
    EntitlementResolver, FileLicenseResolver, LicenseError, LicenseLimits, LicensePayload,
    SignedLicense, LICENSE_PUBKEY,
};
pub use policy::{
    canonical_policy_checksum, ApiKeyId, GrantActions, GrantDefinition, GrantScope, IssuedApiKey,
    KeyState, PolicyGrant, PolicyHead, PolicyKey, PolicyPrincipal, PolicySnapshot,
};
pub use policy_publication::{
    PendingBranchActivation, PolicyActivationGuardPermit, PolicyControlRevision, PolicyHeadDigest,
    PolicyLeaseFencingToken, PolicyPublicationLease, PolicyPublicationLeaseClaim,
    PolicySnapshotMemo, MAX_PENDING_BRANCH_ACTIVATIONS,
    MAX_PENDING_BRANCH_ACTIVATION_LIFETIME_SECS, POLICY_PUBLICATION_LEASE_KEY,
};
pub use policy_store::{LoadedPolicy, PolicyStore};
pub use preservation::{
    CreatePreservationLock, PreservationGuard, PreservationHeadProof, PreservationLockId,
    PreservationLockRecord, PreservationReasonKind, PreservationScope, PreservationService,
    PreservationState,
};
pub use principal::{AuthStrength, Principal, PrincipalId, PrincipalKind};
pub use resource::{Resource, SnapshotName};
pub use route_map::{classify_route, RouteAction, RouteClass, ROUTE_ACTIONS};

use thiserror::Error;

/// Failures produced while constructing or invoking the security subsystem.
#[derive(Debug, Error)]
pub enum SecurityError {
    /// A configured or decoded action name is not in the exhaustive inventory.
    #[error("unknown security action: {0}")]
    UnknownAction(String),
    /// A principal identifier violates the safe bounded identifier grammar.
    #[error("invalid principal identifier")]
    InvalidPrincipalId,
    /// A namespace identifier violates Zeppelin's storage and URL grammar.
    #[error("invalid namespace identifier")]
    InvalidNamespaceId,
    /// A snapshot name violates the manifest key grammar.
    #[error("invalid snapshot name")]
    InvalidSnapshotName,
    /// More than one boot grant resolved to the same principal.
    #[error("duplicate security principal")]
    DuplicatePrincipal,
    /// A configured API-key digest is not exactly 32 hexadecimal bytes.
    #[error("invalid API-key SHA-256 digest")]
    InvalidApiKeyDigest,
    /// Enforced mode lacks the server-only key required for authenticated cursors.
    #[error("missing required security.cursor_hmac_key_hex in enforced mode")]
    MissingCursorHmacKey,
    /// Cursor authentication material is not exactly 32 hexadecimal bytes.
    #[error("invalid security.cursor_hmac_key_hex")]
    InvalidCursorHmacKey,
    /// Credential authentication failed without exposing credential material.
    #[error("authentication failed: {0}")]
    Authentication(AuthnFailure),
    /// Central policy denied an authenticated operation.
    #[error("authorization denied: {}", .0.code())]
    Authorization(DenyReason),
    /// An allowed operation attempted to violate server-owned data constraints.
    #[error("security constraint violation")]
    ConstraintViolation,
    /// A continuation token was issued under a different policy version.
    #[error("cursor policy version is stale")]
    CursorPolicyStale,
    /// A registered route reached middleware without a central mapping.
    #[error("protected route has no security mapping")]
    UnmappedRoute,
    /// Authorization ran without the principal inserted by authentication.
    #[error("authenticated principal missing from request extensions")]
    MissingPrincipal,
    /// Authorization ran without the server-derived request context.
    #[error("security request context missing from request extensions")]
    MissingRequestContext,
    /// Trusted-proxy client identity was not established by outer middleware.
    #[error("trusted source address missing from request extensions")]
    MissingSourceIp,
    /// A required mutation completed without durable audit acknowledgement.
    #[error("durable security audit evidence is unavailable")]
    AuditUnavailable,
    /// The requested surface is not present in the resolved entitlement set.
    #[error("feature is not licensed: {0}")]
    FeatureNotLicensed(Feature),
    /// Security management is frozen after the license-expiry grace period.
    #[error("security management is frozen because the license expired")]
    LicenseExpired,
    /// An authoritative policy requires a missing enforcement capability.
    #[error("authoritative security policy requires licensed feature: {0}")]
    FeatureRequired(Feature),
    /// An authoritative policy exceeds a signed license capacity limit.
    #[error("authoritative security policy exceeds licensed limit: {0}")]
    LicenseLimitExceeded(&'static str),
    /// An authoritative policy object violates its strict schema or invariants.
    #[error("invalid security policy: {0}")]
    InvalidPolicy(String),
    /// A policy checksum does not match its canonical content.
    #[error("security policy checksum mismatch")]
    PolicyChecksumMismatch,
    /// A persisted policy head lacks the ETag required for later CAS updates.
    #[error("security policy head did not provide an ETag")]
    PolicyHeadMissingEtag,
    /// No authoritative policy exists and boot config cannot create one.
    #[error("no authoritative security policy and no usable bootstrap credentials")]
    MissingBootstrapCredentials,
    /// An immutable policy artifact collided with an existing object key.
    #[error("immutable security policy object already exists")]
    PolicyObjectCollision,
    /// The monotonic persisted policy version cannot advance further.
    #[error("security policy version overflow")]
    PolicyVersionOverflow,
    /// A security-policy mutation request is structurally invalid.
    #[error("invalid security policy request: {0}")]
    InvalidPolicyRequest(String),
    /// Delegation was licensed but no private signing-key path was configured.
    #[error("missing required security.token_signing_key_path for delegation")]
    MissingDelegationSigningKey,
    /// The delegated-token signing seed is not exactly 32 hexadecimal bytes.
    #[error("invalid delegation signing key")]
    InvalidDelegationSigningKey,
    /// The private signing-key file is not restricted to mode 0600.
    #[error("delegation signing key must have 0600 permissions")]
    DelegationSigningKeyPermissions,
    /// An immutable signer document collided with different key material.
    #[error("delegation signer object collision")]
    DelegationSignerCollision,
    /// A published signer document is malformed or invalid.
    #[error("invalid delegation signer document")]
    InvalidDelegationSigner,
    /// A requested delegated scope is not a subset of current parent authority.
    #[error("delegated scope exceeds current parent authority")]
    DelegationScopeExceeded,
    /// Delegated credentials cannot mint further delegated credentials.
    #[error("delegation chains are forbidden")]
    DelegationChainingForbidden,
    /// Only human and service principals may act as delegation parents.
    #[error("principal kind cannot mint delegated credentials")]
    DelegationPrincipalKindForbidden,
    /// A two-person obligation lacked one distinct authorized API-key approver.
    #[error("independent approval is required")]
    ApprovalRequired,
    /// An active preservation lock blocks the requested destructive operation.
    #[error("preservation lock blocks destructive operation")]
    PreservationLocked,
    /// Authoritative preservation state is stale or unavailable.
    #[error("authoritative preservation state is unavailable")]
    PreservationStateUnavailable,
    /// A preservation request violates the strict public schema or bounds.
    #[error("invalid preservation request: {0}")]
    InvalidPreservationRequest(String),
    /// The requested active preservation lock does not exist.
    #[error("active preservation lock not found")]
    PreservationLockNotFound,
    /// Concurrent lock-head publication exhausted its bounded retries.
    #[error("preservation state changed concurrently; retry")]
    PreservationConflict,
    /// Persisted preservation head or record data violates its invariants.
    #[error("invalid authoritative preservation state")]
    InvalidPreservationState,
    /// A bounded policy-head CAS retry loop could not publish its mutation.
    #[error("security policy changed concurrently; retry")]
    PolicyConflict,
    /// A requested principal, key, or grant already exists.
    #[error("security policy entity already exists")]
    PolicyEntityAlreadyExists,
    /// A requested principal, key, or grant does not exist.
    #[error("security policy entity not found")]
    PolicyEntityNotFound,
}

/// Security administration failure paired with the exact decision already evaluated.
#[derive(Debug, Error)]
#[error("{error}")]
pub struct SecurityOperationError {
    decision: Option<Box<Decision>>,
    #[source]
    error: Box<crate::error::ZeppelinError>,
}

/// Result of a security administration operation that preserves audit context.
pub type SecurityOperationResult<T> = std::result::Result<T, SecurityOperationError>;

impl SecurityOperationError {
    pub(crate) fn denied(decision: DenyDecision) -> Self {
        let reason = decision.reason;
        Self {
            decision: Some(Box::new(Decision::Deny(decision))),
            error: Box::new(SecurityError::Authorization(reason).into()),
        }
    }

    pub(crate) fn denied_with_error(
        decision: DenyDecision,
        error: crate::error::ZeppelinError,
    ) -> Self {
        Self {
            decision: Some(Box::new(Decision::Deny(decision))),
            error: Box::new(error),
        }
    }

    pub(crate) fn after_allow(error: crate::error::ZeppelinError, decision: AllowDecision) -> Self {
        Self {
            decision: Some(Box::new(Decision::Allow(Box::new(decision)))),
            error: Box::new(error),
        }
    }

    pub(crate) fn with_fallback_allow(self, decision: Option<AllowDecision>) -> Self {
        if self.decision.is_some() {
            self
        } else if let Some(decision) = decision {
            Self::after_allow(*self.error, decision)
        } else {
            self
        }
    }

    /// Borrow the exact decision evaluated before the operation failed, if any.
    #[must_use]
    pub fn decision(&self) -> Option<&Decision> {
        self.decision.as_deref()
    }

    /// Split the audit decision from the canonical Zeppelin failure.
    #[must_use]
    pub fn into_parts(self) -> (Option<Decision>, crate::error::ZeppelinError) {
        (self.decision.map(|decision| *decision), *self.error)
    }

    /// Discard decision context at a caller that has no audit responsibility.
    #[must_use]
    pub fn into_error(self) -> crate::error::ZeppelinError {
        *self.error
    }
}

impl From<crate::error::ZeppelinError> for SecurityOperationError {
    fn from(error: crate::error::ZeppelinError) -> Self {
        Self {
            decision: None,
            error: Box::new(error),
        }
    }
}

impl From<SecurityError> for SecurityOperationError {
    fn from(error: SecurityError) -> Self {
        crate::error::ZeppelinError::from(error).into()
    }
}

impl SecurityError {
    /// HTTP status used by the canonical API envelope.
    #[must_use]
    pub const fn status_code(&self) -> u16 {
        match self {
            Self::Authentication(_)
            | Self::Authorization(
                DenyReason::Unauthenticated
                | DenyReason::CredentialExpired
                | DenyReason::CredentialUnknown,
            ) => 401,
            Self::Authorization(_)
            | Self::FeatureNotLicensed(_)
            | Self::LicenseExpired
            | Self::LicenseLimitExceeded(_) => 403,
            Self::ConstraintViolation => 403,
            Self::DelegationChainingForbidden | Self::DelegationPrincipalKindForbidden => 403,
            Self::ApprovalRequired => 403,
            Self::PreservationLocked => 409,
            Self::PreservationStateUnavailable => 503,
            Self::CursorPolicyStale => 400,
            Self::InvalidNamespaceId | Self::InvalidSnapshotName => 400,
            Self::InvalidPolicyRequest(_)
            | Self::DelegationScopeExceeded
            | Self::InvalidPreservationRequest(_) => 400,
            Self::PolicyConflict | Self::PolicyEntityAlreadyExists | Self::PreservationConflict => {
                409
            }
            Self::PolicyEntityNotFound | Self::PreservationLockNotFound => 404,
            Self::UnknownAction(_)
            | Self::InvalidPrincipalId
            | Self::DuplicatePrincipal
            | Self::InvalidApiKeyDigest
            | Self::MissingCursorHmacKey
            | Self::InvalidCursorHmacKey
            | Self::UnmappedRoute
            | Self::MissingPrincipal
            | Self::MissingRequestContext
            | Self::MissingSourceIp
            | Self::AuditUnavailable
            | Self::InvalidPolicy(_)
            | Self::PolicyChecksumMismatch
            | Self::PolicyHeadMissingEtag
            | Self::MissingBootstrapCredentials
            | Self::PolicyObjectCollision
            | Self::PolicyVersionOverflow
            | Self::FeatureRequired(_)
            | Self::MissingDelegationSigningKey
            | Self::InvalidDelegationSigningKey
            | Self::DelegationSigningKeyPermissions
            | Self::DelegationSignerCollision
            | Self::InvalidDelegationSigner
            | Self::InvalidPreservationState => 500,
        }
    }

    /// Stable lowercase machine-readable security code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Authentication(failure) => failure.code(),
            Self::Authorization(DenyReason::ActionNotGranted) => "forbidden",
            Self::Authorization(reason) => reason.code(),
            Self::ConstraintViolation => "constraint_violation",
            Self::CursorPolicyStale => "cursor_policy_stale",
            Self::InvalidNamespaceId => "invalid_namespace",
            Self::InvalidSnapshotName => "invalid_snapshot",
            Self::InvalidPolicyRequest(_) => "invalid_security_request",
            Self::DelegationScopeExceeded => "delegation_scope_exceeds_parent",
            Self::DelegationChainingForbidden => "delegation_chaining_forbidden",
            Self::DelegationPrincipalKindForbidden => "delegation_parent_kind_forbidden",
            Self::ApprovalRequired => "approval_required",
            Self::PreservationLocked => "preservation_locked",
            Self::PreservationStateUnavailable => "preservation_state_unavailable",
            Self::InvalidPreservationRequest(_) => "invalid_preservation_request",
            Self::PreservationLockNotFound => "preservation_lock_not_found",
            Self::PreservationConflict => "preservation_conflict",
            Self::PolicyConflict => "security_conflict",
            Self::PolicyEntityAlreadyExists => "security_entity_exists",
            Self::PolicyEntityNotFound => "security_entity_not_found",
            Self::AuditUnavailable => "audit_unavailable",
            Self::FeatureNotLicensed(_) => "feature_not_licensed",
            Self::LicenseExpired => "license_expired",
            Self::FeatureRequired(_) => "security_internal",
            Self::LicenseLimitExceeded(_) => "license_limit_exceeded",
            Self::UnmappedRoute => "unmapped_route",
            Self::UnknownAction(_)
            | Self::InvalidPrincipalId
            | Self::DuplicatePrincipal
            | Self::InvalidApiKeyDigest
            | Self::MissingCursorHmacKey
            | Self::InvalidCursorHmacKey
            | Self::MissingPrincipal
            | Self::MissingRequestContext
            | Self::MissingSourceIp
            | Self::InvalidPolicy(_)
            | Self::PolicyChecksumMismatch
            | Self::PolicyHeadMissingEtag
            | Self::MissingBootstrapCredentials
            | Self::PolicyObjectCollision
            | Self::PolicyVersionOverflow
            | Self::MissingDelegationSigningKey
            | Self::InvalidDelegationSigningKey
            | Self::DelegationSigningKeyPermissions
            | Self::DelegationSignerCollision
            | Self::InvalidDelegationSigner
            | Self::InvalidPreservationState => "security_internal",
        }
    }

    /// Redacted prose safe for an external response.
    #[must_use]
    pub fn client_message(&self) -> String {
        match self {
            Self::Authentication(_)
            | Self::Authorization(
                DenyReason::Unauthenticated
                | DenyReason::CredentialExpired
                | DenyReason::CredentialUnknown,
            ) => "authentication required".to_string(),
            Self::Authorization(_) => "access forbidden".to_string(),
            Self::ConstraintViolation => "operation violates security constraints".to_string(),
            Self::CursorPolicyStale => {
                "cursor was issued under a different security policy; re-query required".to_string()
            }
            Self::InvalidNamespaceId => "invalid namespace name".to_string(),
            Self::InvalidSnapshotName => "invalid snapshot name".to_string(),
            Self::InvalidPolicyRequest(_) => "invalid security request".to_string(),
            Self::DelegationScopeExceeded => {
                "delegated scope exceeds current parent authority".to_string()
            }
            Self::DelegationChainingForbidden => {
                "delegated credentials cannot mint further tokens".to_string()
            }
            Self::DelegationPrincipalKindForbidden => {
                "principal kind cannot mint delegated credentials".to_string()
            }
            Self::ApprovalRequired => "independent approval is required".to_string(),
            Self::PreservationLocked => {
                "operation is blocked by an active preservation lock".to_string()
            }
            Self::PreservationStateUnavailable => {
                "preservation state is unavailable; destructive operation denied".to_string()
            }
            Self::InvalidPreservationRequest(_) => "invalid preservation request".to_string(),
            Self::PreservationLockNotFound => "active preservation lock not found".to_string(),
            Self::PreservationConflict => {
                "preservation state changed concurrently; retry".to_string()
            }
            Self::PolicyConflict => "security policy changed concurrently; retry".to_string(),
            Self::PolicyEntityAlreadyExists => "security policy entity already exists".to_string(),
            Self::PolicyEntityNotFound => "security policy entity not found".to_string(),
            Self::AuditUnavailable => {
                "operation may have completed, but durable audit evidence is unavailable"
                    .to_string()
            }
            Self::FeatureNotLicensed(feature) => {
                format!("feature is not licensed: {}", feature.as_str())
            }
            Self::LicenseExpired => {
                "security management is frozen because the license expired".to_string()
            }
            Self::LicenseLimitExceeded(limit) => {
                format!("licensed security limit exceeded: {limit}")
            }
            Self::UnknownAction(_)
            | Self::InvalidPrincipalId
            | Self::DuplicatePrincipal
            | Self::InvalidApiKeyDigest
            | Self::MissingCursorHmacKey
            | Self::InvalidCursorHmacKey
            | Self::UnmappedRoute
            | Self::MissingPrincipal
            | Self::MissingRequestContext
            | Self::MissingSourceIp
            | Self::InvalidPolicy(_)
            | Self::PolicyChecksumMismatch
            | Self::PolicyHeadMissingEtag
            | Self::MissingBootstrapCredentials
            | Self::PolicyObjectCollision
            | Self::PolicyVersionOverflow
            | Self::FeatureRequired(_)
            | Self::MissingDelegationSigningKey
            | Self::InvalidDelegationSigningKey
            | Self::DelegationSigningKeyPermissions
            | Self::DelegationSignerCollision
            | Self::InvalidDelegationSigner
            | Self::InvalidPreservationState => "an internal security error occurred".to_string(),
        }
    }
}
