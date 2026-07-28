//! The authorization admission point every protected route must pass through.
//!
//! This file owns one question: *may this principal perform this action on this
//! resource, right now?* [`SecurityKernel`] answers it with a typed [`Decision`]
//! **before** any namespace, WAL, index, or storage work begins. The kernel is
//! fail-closed in every direction — an unknown credential, an unmapped grant, a
//! stale policy cache, an unlicensed feature, and a missing approver all produce
//! a [`Decision::Deny`] or a typed [`SecurityError`], never a permissive default.
//!
//! It deliberately does **not** own:
//!
//! - **Authentication.** Turning a bearer credential into a [`Principal`] belongs
//!   to `authn.rs` ([`ApiKeyAdapter`]). The kernel only consumes an already-typed
//!   principal.
//! - **Route classification.** Deciding which [`Action`] and [`Resource`] a URL
//!   maps to belongs to `route_map.rs`; an unmapped route fails closed there.
//! - **The persisted policy vocabulary.** `policy.rs` defines `PolicySnapshot`,
//!   `PolicyGrant`, and the compiled evaluator; `policy_store.rs` reads and
//!   publishes it on S3; `policy_cache.rs` holds the revalidated copy. The kernel
//!   selects among those authorities and translates their verdicts.
//! - **Constraint enforcement.** The kernel *returns* a mandatory filter, field
//!   mask, and write constraints inside [`AllowDecision`]; query, upsert, and
//!   response code enforce them. A handler that ignores those fields silently
//!   widens access, which is why the middleware rejects an allow carrying data
//!   constraints on an action that cannot consume them.
//! - **Audit durability.** `audit_sink.rs` owns delivery; the kernel only submits
//!   records and refuses to proceed when a required record has not settled.
//!
//! ## Where this sits
//!
//! ```text
//!  HTTP request
//!      |
//!      v
//!  authn: ApiKeyAdapter -> Principal            (authn.rs)
//!      |
//!      v
//!  route_map::classify_route -> Action, Resource  (route_map.rs)
//!      |                              unmapped route -> DENY, fail closed
//!      v
//!  server::authorize middleware                 (src/server/mod.rs)
//!      |
//! ====== this file ==========================================================
//!      v
//!  SecurityKernel::authorize        (or ::authorize_action when the namespace
//!      |                             is only known after body parsing)
//!      +--> Bootstrap authority: grants compiled from boot config, pure CPU
//!      +--> Policy authority: PolicyCache -> CompiledPolicy
//!      |         S3 head is authoritative; a cache older than its freshness
//!      |         bound denies with DenyReason::SecurityStale
//!      v
//!  Decision
//!      |                                    \
//!      | Allow(AllowDecision)                 Deny -> denial audit, 403
//!      |   mandatory_filter, field_mask,
//!      |   write_constraints, obligations
//!      v
//!  obligations discharged by the middleware:
//!      Approval    -> second authorized principal, distinct from the actor
//!      DurableAudit-> audit record must settle
//! ===========================================================================
//!      |
//!      v
//!  domain work: namespace / wal / index / query
//!
//!
//!  Fork and delete take a second pass through this file, because an HTTP-time
//!  decision is not sufficient authority to mutate the branch graph:
//!
//!  authorize_namespace_fork(admission)   -- entitlement + structural checks
//!      |
//!      v
//!  AuthorizedForkNamespace -> graph reserves a target
//!      |
//!      v
//!  NamespaceForkGovernance::begin  -- re-authorize against the freshly loaded
//!      |                              policy head, park an activation guard
//!      v                              in the head under the publication lease
//!  settle_audit  ->  revalidate  ->  finalize | abort
//!   (durable)       (guard renew,     (guard removed from the head)
//!                    re-authorize)
//! ```
//!
//! ## Reading map
//!
//! 1. [`SecurityKernel`] and the private `SecurityAuthority` enum: the whole file
//!    branches on whether authority is boot configuration or the S3-backed
//!    [`PolicyCache`]. Every method is written twice, once per arm.
//! 2. [`SecurityKernel::from_config`], [`SecurityKernel::from_store`], and
//!    [`SecurityKernel::from_resolved_entitlements`]: composition. The last one
//!    holds the licensed-feature dependency rules.
//! 3. [`SecurityKernel::authorize`] and [`SecurityKernel::authorize_action`]: the
//!    hot admission path. `authorize_action` exists for routes whose namespace is
//!    only known after the body is parsed (namespace create).
//! 4. `authorize_delegated` and `authorize_delegated_action`: the same path for a
//!    short-lived delegated token, which can never exceed its parent's grants.
//! 5. [`SecurityKernel::mint_delegated_token`]: issuance, and the structural
//!    narrowing proof that must hold before a token exists.
//! 6. Preservation seams — [`SecurityKernel::guard_namespace_destruction`],
//!    [`SecurityKernel::guard_namespace_destruction_strong`], and
//!    [`SecurityKernel::guard_vector_destruction`] — which block destruction
//!    while a legal hold is active.
//! 7. The branch/fork half of the file, which is the most intricate:
//!    [`SecurityKernel::authorize_namespace_fork`] mints an
//!    [`AuthorizedForkNamespace`], and `NamespaceForkGovernance` performs the
//!    *second*, authoritative authorization later under the policy-publication
//!    interlock. Follow `begin` -> `settle_audit` -> `revalidate` ->
//!    `finalize`/`abort` across [`BootstrapBranchActivationPermit`] and
//!    [`PolicyBranchActivationPermit`].
//! 8. [`SecurityKernel::authorize_namespace_delete`] and
//!    `namespace_delete_governance`, which wire preservation, per-target
//!    disclosure, and deletion-lifecycle audit into one governance adapter.
//!
//! ## State this file touches
//!
//! The kernel holds no durable state of its own. It reads and mutates state that
//! other modules own:
//!
//! - **Read.** The compiled policy and its snapshot checksum, borrowed from
//!   [`PolicyCache`]. S3 remains authoritative; the cache is a disposable copy
//!   with an explicit staleness bound.
//! - **Published.** Principal, key, grant, revoke, and rotate mutations are
//!   forwarded to the cache's compare-and-swap publication path. The kernel never
//!   writes a policy object directly.
//! - **Published (branch activation).** A pending activation guard is inserted
//!   into the policy head under the publication lease, then removed on finalize
//!   or abort. That guard is what makes a crashed fork recoverable.
//! - **Published (audit).** Fork activation submits a durable [`AuditRecord`]
//!   before the branch may be revalidated and finalized.
//! - **Published (preservation).** A blocked delete records a durable deferral
//!   through [`PreservationService`].
//!
//! ## Invariants
//!
//! - **Deny by default.** An anonymous principal is denied unless the process runs
//!   in `SecurityMode::OpenUnsafe`, which is the only mode that returns a boot
//!   allow for anonymous callers.
//! - **Stale authority denies.** When [`PolicyCache`] has not confirmed its head
//!   within its freshness bound, every policy-backed decision returns
//!   [`DenyReason::SecurityStale`]. An old allow is never replayed.
//! - **Bootstrap authority cannot administer policy.** Every administrative
//!   method returns `InvalidPolicyRequest` on the `Bootstrap` arm; security
//!   administration requires the S3 policy authority.
//! - **Entitlements are checked here, not only at the route.** Branching is
//!   verified inside [`SecurityKernel::authorize_namespace_fork`],
//!   [`SecurityKernel::authorize_branch_list`], and again in both fresh
//!   re-authorization paths, so a new call site cannot reach fork or branch-list
//!   authority by skipping the handler's config flag. Preservation and delegation
//!   APIs return `FeatureNotLicensed` when their feature is absent.
//! - **Licensed features have ordering dependencies.** Composition rejects
//!   delegation or preservation without RBAC, receipts without delegation, and
//!   delegation or preservation in `OpenUnsafe` mode.
//! - **Delegation cannot widen.** A delegated token may not be re-delegated, may
//!   only descend from a human or service principal, and is authorized by
//!   re-checking *both* the narrowing and the live parent grants for every
//!   narrowed action/namespace pair. Every destructive action performed through a
//!   delegated token carries the approval obligation.
//! - **Fork admission is structurally exact.** The three HTTP decisions (fork,
//!   source read, target create) must share one policy version, must carry the
//!   durable-audit obligation, and must carry no data constraints — a filtered or
//!   masked principal cannot fork a raw copy. Under policy authority, a delegated
//!   principal is refused outright during re-authorization.
//! - **Approval is two-person.** An approver is required exactly when the allow
//!   carries [`Obligation::Approval`], must differ from the actor and from the
//!   actor's delegation parent, and must itself be authorized at the same policy
//!   version without its own approval obligation.
//! - **A copy may not widen access.** `validate_namespace_copy_no_widening`
//!   fails closed unless every principal's authority over the target is no
//!   broader than over the source, under the same verified policy version.
//! - **Admission is not activation.** The decision made at HTTP time only
//!   *reserves* the attempt. The governance adapter re-authorizes against the
//!   freshly loaded policy head, and `revalidate` refuses to proceed until the
//!   activation audit record has settled ([`SecurityError::AuditUnavailable`]).
//! - **Signing capability is fail-closed.** `install_object_signer` errors when
//!   receipts or S3 audit are licensed but no signer was composed.
//!
//! ## Concurrency
//!
//! One [`SecurityKernel`] is shared as an `Arc` across every request task; all
//! authorization is read-only against an `Arc`-shared cache snapshot, so
//! decisions never block each other. The long-lived authorities it composes
//! ([`PolicyCache`], [`DelegationAuthority`], [`PreservationService`]) each own a
//! background revalidation task. [`SecurityKernel::shutdown_refresh_tasks`] must
//! be called only after HTTP has stopped accepting work, so a retired node cannot
//! keep revalidating authority a replacement now owns.
//!
//! ## Rust concepts used here
//!
//! **Exhaustive `match` as a safety net.** [`Action`] is a fieldless enum, and
//! every authority arm matches on it or on `SecurityAuthority`. Adding an action
//! or a third authority kind turns "we forgot to handle this" into a compile
//! error rather than a silent allow. Java's `switch` over an enum has no such
//! guarantee unless exhaustiveness is opted into; C's `switch` over an `int` has
//! none at all.
//!
//! **A returned decision that cannot be ignored.** [`SecurityKernel::authorize`]
//! is `#[must_use]` and returns [`Decision`], not `bool`. A caller cannot
//! accidentally drop the result, and cannot read a filter or field mask without
//! first destructuring the `Allow` arm — the deny path has no such fields to read.
//! [`AllowDecision`] is boxed inside the enum so the common deny value stays
//! small.
//!
//! **`Arc<Self>` receivers.** [`SecurityKernel::authorize_namespace_fork`] and
//! [`SecurityKernel::authorize_namespace_delete`] take `self: &Arc<Self>` because
//! the governance adapters they mint outlive the calling stack frame and must
//! keep the kernel alive. In Java every object reference is implicitly shared and
//! GC-managed; here the sharing is explicit and refcounted, and cloning the `Arc`
//! is a counter increment, not a copy of the kernel.
//!
//! **`Box<dyn Trait>` with consuming methods.** [`BranchActivationPermit`]'s
//! `finalize` and `abort` take `self: Box<Self>`. Ownership moves into the call,
//! so the permit cannot be finalized twice or used afterwards — the type system
//! encodes a state machine that C would express with a manual "already released"
//! flag. `#[async_trait]` supplies the boxed futures that make such an async
//! trait object possible on stable Rust.
//!
//! **Closures captured into shared callbacks.** `namespace_disclosure_callback`
//! moves an `Arc<SecurityKernel>` and the request's principal into a closure
//! stored behind an `Arc`, letting the deletion graph ask "may this caller see
//! this branch?" without knowing anything about the security subsystem.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::config::{SecurityConfig, SecurityMode};
use crate::error::Result as ZeppelinResult;
use crate::namespace::branching::activation::{
    AuthorizedForkNamespace, BranchActivationAttempt, BranchActivationGovernance,
    BranchActivationGuard, BranchActivationPermit, BranchActivationRecovery,
    BranchActivationTarget,
};
use crate::namespace::branching::deletion::{
    deletion_decision_evidence_key, persist_deletion_lifecycle_audit, AuthorizedBranchList,
    AuthorizedNamespaceDelete, CallbackDeletionGovernance, DeletionDecision, DeletionGovernance,
    DisclosureCallback,
};
use crate::namespace::branching::{
    ActivationNonce, BranchActivationEvidence, PolicyHeadIdentity, PreparedBranch,
};
use crate::storage::store::ObjectSigner;
use crate::storage::ZeppelinStore;
use crate::time::Clock;
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};

use super::{
    delegation::{DelegationAuthority, PublishedObjectSigner},
    policy_cache::PolicyCache,
    Action, AllowDecision, ApiKeyAdapter, AuditClient, AuditOutcome, AuditParams, AuditRecord,
    Decision, DelegationNarrowing, DenyDecision, DenyReason, Entitlements, GrantActions,
    GrantDefinition, GrantScope, IssuedApiKey, IssuedDelegatedToken, LoadedPolicy, NamespaceId,
    Obligation, PolicyActivationGuardPermit, PolicyGrant, PolicyPrincipal, PolicySnapshot,
    PolicyStore, PreservationLockId, PreservationLockRecord, PreservationService, Principal,
    PrincipalId, PrincipalKind, RequestContext, Resource, ResourceRef, SecurityError,
    SecurityOperationError, SecurityOperationResult, MAX_PENDING_BRANCH_ACTIVATION_LIFETIME_SECS,
};

const BRANCH_ACTIVATION_GUARD_TTL_SECS: i64 = MAX_PENDING_BRANCH_ACTIVATION_LIFETIME_SECS / 2;

#[derive(Debug)]
struct BootstrapGrant {
    actions: HashSet<Action>,
    all_namespaces: bool,
    namespaces: HashSet<NamespaceId>,
}

impl BootstrapGrant {
    fn allows_namespace(&self, action: Action, namespace: &NamespaceId) -> bool {
        self.actions.contains(&action)
            && (self.all_namespaces || self.namespaces.contains(namespace))
    }
}

enum SecurityAuthority {
    Bootstrap(HashMap<PrincipalId, BootstrapGrant>),
    Policy(Arc<PolicyCache>),
}

/// Central authorization seam used by every protected route.
pub struct SecurityKernel {
    mode: SecurityMode,
    authority: SecurityAuthority,
    cursor_binding_key: super::CursorBindingKey,
    entitlements: Arc<Entitlements>,
    delegation: Option<Arc<DelegationAuthority>>,
    audit_signer: Option<Arc<PublishedObjectSigner>>,
    preservation: Option<Arc<PreservationService>>,
}

#[allow(dead_code)]
pub(crate) struct ReceiptPolicyLookup<'a> {
    pub verifier: &'a Principal,
    pub context: &'a RequestContext,
    pub receipt_principal: &'a super::PrincipalId,
    pub delegation_parent: Option<&'a super::PrincipalId>,
    pub namespace: &'a super::NamespaceId,
    pub version: super::PolicyVersion,
    pub checksum: Option<&'a str>,
}

/// Request-owned facts consumed when minting graph deletion authority.
pub(crate) struct NamespaceDeleteAdmission {
    pub namespace: NamespaceId,
    pub principal: Principal,
    pub context: RequestContext,
    pub allow: AllowDecision,
    pub approver: Option<PrincipalId>,
    pub store: ZeppelinStore,
    pub clock: Clock,
}

/// Request-owned facts consumed when minting one graph fork authority.
///
/// These are already-redacted principals and decisions. Bearer credentials are
/// deliberately absent; fresh activation reuses only the typed identities.
#[derive(Clone)]
pub(crate) struct NamespaceForkAdmission {
    pub source: NamespaceId,
    pub target: NamespaceId,
    pub principal: Principal,
    pub approver: Option<Principal>,
    pub context: RequestContext,
    pub fork_decision: AllowDecision,
    pub source_read_decision: AllowDecision,
    pub target_create_decision: AllowDecision,
    pub audit: AuditClient,
    pub source_ip: IpAddr,
    pub clock: Clock,
}

struct NamespaceForkGovernance {
    kernel: Arc<SecurityKernel>,
    admission: NamespaceForkAdmission,
}

struct FreshForkAuthorization {
    fork: AllowDecision,
    approver: Option<PrincipalId>,
    authorized_at: DateTime<Utc>,
}

struct BootstrapBranchActivationPermit {
    kernel: Arc<SecurityKernel>,
    admission: NamespaceForkAdmission,
    prepared: PreparedBranch,
    evidence: BranchActivationEvidence,
    audit_settled: bool,
}

struct PolicyBranchActivationPermit {
    kernel: Arc<SecurityKernel>,
    cache: Arc<PolicyCache>,
    admission: NamespaceForkAdmission,
    prepared: PreparedBranch,
    guard: PolicyActivationGuardPermit,
    evidence: BranchActivationEvidence,
    audit_settled: bool,
}

struct PolicyRetainedBranchActivationGuard {
    cache: Arc<PolicyCache>,
    guard: PolicyActivationGuardPermit,
    attempt: BranchActivationAttempt,
}

struct NamespaceBranchActivationRecovery {
    kernel: Arc<SecurityKernel>,
}

/// Outcome of the privileged historical-policy lookup used by receipt verification.
#[allow(dead_code)]
pub(crate) enum ReceiptPolicyResolution {
    /// The verifier lacks `SecurityAdminRead`, so predicate consistency is intentionally skipped.
    Unchecked,
    /// The exact immutable policy generation authorized the receipt principal.
    Resolved {
        filter: Option<crate::types::Filter>,
        delegated: bool,
    },
    /// A privileged verifier could not resolve or authorize the receipt's claimed policy state.
    Diverged { delegated: bool },
}

impl SecurityKernel {
    /// Compile validated boot configuration into typed, immutable grants.
    pub fn from_config(config: &SecurityConfig) -> Result<Self, SecurityError> {
        Self::from_config_with_entitlements(config, Arc::new(Entitlements::community()))
    }

    fn from_config_with_entitlements(
        config: &SecurityConfig,
        entitlements: Arc<Entitlements>,
    ) -> Result<Self, SecurityError> {
        let cursor_binding_key = if config.mode == SecurityMode::OpenUnsafe {
            super::CursorBindingKey::default()
        } else {
            super::CursorBindingKey::from_config_hex(config.cursor_hmac_key_hex())?
        };
        let mut grants = HashMap::new();
        for key in &config.api_keys {
            if key.actions.iter().any(|action| action == "*") && key.actions.len() != 1 {
                return Err(SecurityError::InvalidPolicyRequest(
                    "bootstrap actions must not mix '*' with named actions".to_string(),
                ));
            }
            let principal_id = PrincipalId::new(key.key_id.clone())?;
            let mut actions = HashSet::new();
            if key.actions.iter().any(|action| action == "*") {
                actions.extend(Action::BOOTSTRAP_ADMIN_V1);
                if entitlements.has(super::Feature::Preservation) {
                    actions.extend([Action::PreservationAdmin, Action::PreservationRelease]);
                }
            } else {
                for action in &key.actions {
                    actions.insert(Action::from_str(action)?);
                }
            }

            let all_namespaces = key.namespaces.iter().any(|namespace| namespace == "*");
            let namespaces = if all_namespaces {
                HashSet::new()
            } else {
                key.namespaces
                    .iter()
                    .map(|namespace| NamespaceId::new(namespace.clone()))
                    .collect::<Result<HashSet<_>, _>>()?
            };
            if grants
                .insert(
                    principal_id,
                    BootstrapGrant {
                        actions,
                        all_namespaces,
                        namespaces,
                    },
                )
                .is_some()
            {
                return Err(SecurityError::DuplicatePrincipal);
            }
        }
        Ok(Self {
            mode: config.mode,
            authority: SecurityAuthority::Bootstrap(grants),
            cursor_binding_key,
            entitlements,
            delegation: None,
            audit_signer: None,
            preservation: None,
        })
    }

    /// Compose the configured authority selected by verified entitlements.
    ///
    /// Community mode keeps boot-config authentication and namespace grants
    /// without constructing the licensed S3 policy registry. Licensed RBAC
    /// selects the S3-authoritative policy store and refresh path.
    pub async fn from_resolved_entitlements(
        store: ZeppelinStore,
        config: &SecurityConfig,
        clock: Clock,
        entitlements: Arc<Entitlements>,
    ) -> ZeppelinResult<(Arc<Self>, Arc<ApiKeyAdapter>)> {
        if entitlements.has(super::Feature::Delegation) && !entitlements.has(super::Feature::Rbac) {
            return Err(crate::error::ZeppelinError::Config(
                "delegation entitlement requires the rbac entitlement".to_string(),
            ));
        }
        if entitlements.has(super::Feature::Preservation) && !entitlements.has(super::Feature::Rbac)
        {
            return Err(crate::error::ZeppelinError::Config(
                "preservation entitlement requires the rbac entitlement".to_string(),
            ));
        }
        if entitlements.has(super::Feature::Receipts)
            && !entitlements.has(super::Feature::Delegation)
        {
            return Err(crate::error::ZeppelinError::Config(
                "receipts entitlement requires the delegation signer entitlement".to_string(),
            ));
        }
        if config.mode == SecurityMode::OpenUnsafe && entitlements.has(super::Feature::Delegation) {
            return Err(crate::error::ZeppelinError::Config(
                "delegation requires security.mode = enforced so every token has authoritative parent grants"
                    .to_string(),
            ));
        }
        if config.mode == SecurityMode::OpenUnsafe && entitlements.has(super::Feature::Preservation)
        {
            return Err(crate::error::ZeppelinError::Config(
                "preservation requires security.mode = enforced so destruction is centrally authorized"
                    .to_string(),
            ));
        }
        let (kernel, adapter) = if !entitlements.has(super::Feature::Rbac) {
            let now = clock.now();
            if config.mode == SecurityMode::Enforced
                && !config
                    .api_keys
                    .iter()
                    .any(|key| key.expires_at.is_none_or(|expires_at| expires_at > now))
            {
                return Err(SecurityError::MissingBootstrapCredentials.into());
            }
            let audit_signer = if entitlements.has(super::Feature::AuditS3) {
                Some(
                    PublishedObjectSigner::compose(
                        store.signer_detached_clone(),
                        PathBuf::from(&config.token_signing_key_path),
                    )
                    .await?,
                )
            } else {
                None
            };
            let mut kernel = Self::from_config_with_entitlements(config, entitlements)?;
            kernel.audit_signer = audit_signer;
            (
                Arc::new(kernel),
                Arc::new(ApiKeyAdapter::from_config(config)?),
            )
        } else {
            Self::from_store(store.clone(), config, clock, entitlements).await?
        };

        // Composition historically leaves the caller's application store ready
        // to sign manifests and audit evidence. Keep that public contract at
        // this root while each long-lived authority keeps only its detached
        // store view. The application slot is weak, so disposable store clones
        // cannot retain security caches after their live components end.
        kernel.install_object_signer(&store)?;
        Ok((kernel, adapter))
    }

    /// Build authentication and authorization over one shared policy cache.
    pub(crate) async fn from_store(
        store: ZeppelinStore,
        config: &SecurityConfig,
        clock: Clock,
        entitlements: Arc<Entitlements>,
    ) -> ZeppelinResult<(Arc<Self>, Arc<ApiKeyAdapter>)> {
        // Authority-side caches retain their store view for background refresh.
        // Keep that view detached from the application signing capability; the
        // application's weak signer slot independently prevents disposable
        // store clones from retaining those caches.
        let authority_store = store.signer_detached_clone();
        if config.mode == SecurityMode::OpenUnsafe {
            let audit_signer = if entitlements.has(super::Feature::AuditS3) {
                Some(
                    PublishedObjectSigner::compose(
                        authority_store,
                        PathBuf::from(&config.token_signing_key_path),
                    )
                    .await?,
                )
            } else {
                None
            };
            let mut kernel =
                Self::from_config_with_entitlements(config, Arc::clone(&entitlements))?;
            kernel.audit_signer = audit_signer;
            return Ok((
                Arc::new(kernel),
                Arc::new(ApiKeyAdapter::from_config(config)?),
            ));
        }

        let policy_store = PolicyStore::new(authority_store.clone(), Arc::clone(&entitlements));
        let cursor_binding_key =
            super::CursorBindingKey::from_config_hex(config.cursor_hmac_key_hex())?;
        let loaded = policy_store.load_or_bootstrap(config, clock.now()).await?;
        let cache = PolicyCache::start(
            policy_store,
            loaded,
            Duration::from_secs(config.policy_refresh_secs),
            clock.clone(),
        )?;
        let delegation = if entitlements.has(super::Feature::Delegation) {
            Some(
                DelegationAuthority::compose(
                    authority_store.clone(),
                    Arc::clone(&cache),
                    clock.clone(),
                    PathBuf::from(&config.token_signing_key_path),
                    config.delegated_token_max_ttl_secs,
                    Duration::from_secs(config.policy_refresh_secs),
                )
                .await?,
            )
        } else {
            None
        };
        let audit_signer = if delegation.is_none() && entitlements.has(super::Feature::AuditS3) {
            Some(
                PublishedObjectSigner::compose(
                    authority_store.clone(),
                    PathBuf::from(&config.token_signing_key_path),
                )
                .await?,
            )
        } else {
            None
        };
        let preservation = if entitlements.has(super::Feature::Preservation) {
            Some(
                PreservationService::start(
                    authority_store,
                    clock,
                    Duration::from_secs(config.policy_refresh_secs),
                )
                .await?,
            )
        } else {
            None
        };
        let adapter = match &delegation {
            Some(authority) => Arc::new(ApiKeyAdapter::from_policy_cache_with_delegation(
                Arc::clone(&cache),
                authority.verifier(),
            )),
            None => Arc::new(ApiKeyAdapter::from_policy_cache(Arc::clone(&cache))),
        };
        Ok((
            Arc::new(Self {
                mode: config.mode,
                authority: SecurityAuthority::Policy(cache),
                cursor_binding_key,
                entitlements,
                delegation,
                audit_signer,
                preservation,
            }),
            adapter,
        ))
    }

    /// Borrow the boot-resolved feature authority used by composition roots.
    #[must_use]
    pub fn entitlements(&self) -> &Entitlements {
        &self.entitlements
    }

    /// Install this kernel's published node signer on another wrapper of the same backend.
    pub fn install_object_signer(&self, store: &ZeppelinStore) -> ZeppelinResult<()> {
        if let Some(authority) = &self.delegation {
            let signer: Arc<dyn ObjectSigner> = authority.clone();
            return store.install_object_signer(signer);
        }
        if let Some(audit_signer) = &self.audit_signer {
            let signer: Arc<dyn ObjectSigner> = audit_signer.clone();
            return store.install_object_signer(signer);
        }
        if self.entitlements.has(super::Feature::Receipts)
            || self.entitlements.has(super::Feature::AuditS3)
        {
            return Err(super::SecurityError::FeatureRequired(super::Feature::Delegation).into());
        }
        Ok(())
    }

    /// Borrow the composed preservation module for maintenance integration.
    #[must_use]
    pub fn preservation_service(&self) -> Option<&Arc<PreservationService>> {
        self.preservation.as_ref()
    }

    /// Abort and join every disposable authority-refresh task owned by this kernel.
    ///
    /// The external application owner must call this only after HTTP has
    /// stopped accepting work. It prevents a retired node from continuing S3
    /// revalidation while a replacement owns the same authority.
    pub async fn shutdown_refresh_tasks(&self) {
        if let Some(delegation) = &self.delegation {
            delegation.shutdown_refresh_task().await;
        }
        if let SecurityAuthority::Policy(cache) = &self.authority {
            cache.shutdown_refresh_task().await;
        }
        if let Some(preservation) = &self.preservation {
            preservation.shutdown_refresh_task().await;
        }
    }

    /// Fail closed when an active lock protects one namespace destruction seam.
    pub fn guard_namespace_destruction(
        &self,
        namespace: &NamespaceId,
    ) -> std::result::Result<super::PreservationGuard, SecurityError> {
        let Some(preservation) = &self.preservation else {
            return Ok(super::PreservationGuard::unlocked());
        };
        preservation.guard_namespace(namespace)
    }

    /// Strongly read preservation authority for one destructive boundary.
    pub async fn guard_namespace_destruction_strong(
        &self,
        namespace: &NamespaceId,
    ) -> ZeppelinResult<(super::PreservationGuard, super::PreservationHeadProof)> {
        let Some(preservation) = &self.preservation else {
            return Ok((
                super::PreservationGuard::unlocked(),
                super::PreservationHeadProof {
                    head_sha256: [0; 32],
                    e_tag: None,
                },
            ));
        };
        let (guard, proof) = preservation.guard_namespace_strong(namespace).await?;
        if guard.is_locked() {
            preservation
                .record_namespace_delete_deferral(namespace, &guard)
                .await?;
        }
        Ok((guard, proof))
    }

    /// Durably record a delete deferral already proved by cached authority.
    ///
    /// The request adapter uses this only when the locally committed
    /// preservation head already contains the blocking lock. A cache miss still
    /// goes through [`Self::guard_namespace_destruction_strong`], which records
    /// the same evidence after its authoritative S3 read.
    pub async fn record_namespace_delete_deferral(
        &self,
        namespace: &NamespaceId,
        guard: &super::PreservationGuard,
    ) -> ZeppelinResult<()> {
        let Some(preservation) = &self.preservation else {
            return Err(crate::error::ZeppelinError::Validation(format!(
                "namespace {namespace} has a preservation lock without a preservation service"
            )));
        };
        preservation
            .record_namespace_delete_deferral(namespace, guard)
            .await
    }

    /// Mint one request-scoped graph deletion envelope from the exact middleware decision.
    #[must_use]
    pub(crate) fn authorize_namespace_delete(
        self: &Arc<Self>,
        admission: NamespaceDeleteAdmission,
    ) -> AuthorizedNamespaceDelete {
        let decision_evidence_ref = deletion_decision_evidence_key(admission.allow.decision_id);
        let decision = DeletionDecision {
            actor: admission.principal.id.clone(),
            approver: admission.approver,
            decision_id: admission.allow.decision_id,
            policy_version: admission.allow.policy_version,
            decision_evidence_ref,
        };
        let governance = self.namespace_delete_governance(
            admission.store,
            admission.clock,
            Some((admission.principal, admission.context)),
        );
        AuthorizedNamespaceDelete::new(
            admission.namespace,
            decision,
            governance,
            self.branch_activation_recovery(),
        )
    }

    /// Mint one request-scoped graph fork envelope from all three exact HTTP
    /// admission decisions.
    ///
    /// Structural drift is rejected before the graph can reserve a target.
    /// The returned governance adapter performs the second, authoritative
    /// authorization under the policy-publication interlock.
    pub(crate) fn authorize_namespace_fork(
        self: &Arc<Self>,
        admission: NamespaceForkAdmission,
    ) -> ZeppelinResult<AuthorizedForkNamespace> {
        if !self.entitlements.has(super::Feature::Branching) {
            return Err(SecurityError::FeatureNotLicensed(super::Feature::Branching).into());
        }
        validate_namespace_fork_admission(&admission)?;
        let source = admission.source.clone();
        let target = admission.target.clone();
        Ok(AuthorizedForkNamespace::new(
            source,
            target,
            Box::new(NamespaceForkGovernance {
                kernel: Arc::clone(self),
                admission,
            }),
        ))
    }

    /// Compose restart-safe deletion governance without a disclosure principal.
    pub(crate) fn namespace_delete_maintenance_governance(
        self: &Arc<Self>,
        store: ZeppelinStore,
        clock: Clock,
    ) -> Arc<dyn DeletionGovernance> {
        self.namespace_delete_governance(store, clock, None)
    }

    /// Compose mechanical activation-guard recovery for deletion and
    /// background maintenance. This adapter never authorizes or activates a
    /// target; it only retains an exact persisted guard for graph resolution.
    pub(crate) fn branch_activation_recovery(
        self: &Arc<Self>,
    ) -> Arc<dyn BranchActivationRecovery> {
        Arc::new(NamespaceBranchActivationRecovery {
            kernel: Arc::clone(self),
        })
    }

    /// Mint a source-authorized branch-list request with per-target disclosure.
    ///
    /// Branch listing is part of the same licensed surface as forking, so the
    /// entitlement is checked here rather than only at the route. A kernel-side
    /// check cannot be bypassed by a future caller that reaches this seam
    /// without repeating the handler's configuration guard.
    pub(crate) fn authorize_branch_list(
        self: &Arc<Self>,
        source: NamespaceId,
        principal: Principal,
        context: RequestContext,
    ) -> ZeppelinResult<AuthorizedBranchList> {
        if !self.entitlements.has(super::Feature::Branching) {
            return Err(SecurityError::FeatureNotLicensed(super::Feature::Branching).into());
        }
        Ok(AuthorizedBranchList::new(
            source,
            self.namespace_disclosure_callback(Some((principal, context))),
        ))
    }

    fn namespace_delete_governance(
        self: &Arc<Self>,
        store: ZeppelinStore,
        clock: Clock,
        disclosure: Option<(Principal, RequestContext)>,
    ) -> Arc<dyn DeletionGovernance> {
        let preservation_kernel = Arc::clone(self);
        let audit_store = store;
        let audit_clock = clock;
        Arc::new(CallbackDeletionGovernance::new(
            Arc::new(move |namespace, _boundary| {
                let kernel = Arc::clone(&preservation_kernel);
                Box::pin(async move { kernel.guard_namespace_destruction_strong(&namespace).await })
            }),
            self.namespace_disclosure_callback(disclosure),
            Arc::new(move |event| {
                let store = audit_store.clone();
                let clock = audit_clock.clone();
                Box::pin(
                    async move { persist_deletion_lifecycle_audit(&store, &clock, event).await },
                )
            }),
        ))
    }

    fn namespace_disclosure_callback(
        self: &Arc<Self>,
        disclosure: Option<(Principal, RequestContext)>,
    ) -> Arc<DisclosureCallback> {
        let kernel = Arc::clone(self);
        Arc::new(move |target| {
            let Some((principal, context)) = disclosure.as_ref() else {
                return Err(SecurityError::MissingPrincipal.into());
            };
            match kernel.authorize(
                principal,
                Action::NamespaceRead,
                &Resource::Namespace(target.clone()),
                context,
            ) {
                Decision::Allow(_) => Ok(true),
                Decision::Deny(deny) if deny.reason == DenyReason::SecurityStale => {
                    Err(SecurityError::Authorization(deny.reason).into())
                }
                Decision::Deny(_) => Ok(false),
            }
        })
    }

    /// Fail closed when an active lock conservatively overlaps vector deletion.
    pub fn guard_vector_destruction(
        &self,
        namespace: &NamespaceId,
        filter: Option<&crate::types::Filter>,
    ) -> std::result::Result<super::PreservationGuard, SecurityError> {
        let Some(preservation) = &self.preservation else {
            return Ok(super::PreservationGuard::unlocked());
        };
        preservation.guard_vector_delete(namespace, filter)
    }

    /// Create one S3-authoritative active preservation lock.
    pub async fn create_preservation_lock(
        &self,
        actor: PrincipalId,
        request: super::CreatePreservationLock,
    ) -> ZeppelinResult<PreservationLockRecord> {
        self.preservation
            .as_ref()
            .ok_or_else(|| {
                crate::error::ZeppelinError::from(SecurityError::FeatureNotLicensed(
                    super::Feature::Preservation,
                ))
            })?
            .create_lock(actor, request)
            .await
    }

    /// Release one active lock through immutable evidence plus CAS head removal.
    pub async fn release_preservation_lock(
        &self,
        lock_id: &PreservationLockId,
        actor: PrincipalId,
    ) -> ZeppelinResult<PreservationLockRecord> {
        self.preservation
            .as_ref()
            .ok_or_else(|| {
                crate::error::ZeppelinError::from(SecurityError::FeatureNotLicensed(
                    super::Feature::Preservation,
                ))
            })?
            .release_lock(lock_id, actor)
            .await
    }

    /// Return the current fresh active lock inventory.
    pub fn active_preservation_locks(
        &self,
    ) -> std::result::Result<Vec<PreservationLockRecord>, SecurityError> {
        self.preservation
            .as_ref()
            .ok_or(SecurityError::FeatureNotLicensed(
                super::Feature::Preservation,
            ))?
            .list_active()
    }

    /// Decide whether one principal may perform an action on a resource.
    #[must_use]
    pub fn authorize(
        &self,
        principal: &Principal,
        action: Action,
        resource: &Resource,
        context: &RequestContext,
    ) -> Decision {
        if self.mode == SecurityMode::OpenUnsafe && principal.is_anonymous() {
            return Decision::Allow(Box::new(AllowDecision::boot(action)));
        }
        if principal.is_anonymous() {
            return Decision::Deny(DenyDecision::boot(DenyReason::Unauthenticated));
        }
        if principal.delegation.is_some() {
            return self.authorize_delegated(principal, action, resource, context.now);
        }
        match &self.authority {
            SecurityAuthority::Bootstrap(grants) => {
                if principal
                    .expires_at
                    .is_some_and(|expires_at| expires_at <= context.now)
                {
                    return Decision::Deny(DenyDecision::boot(DenyReason::CredentialExpired));
                }
                let Some(grant) = grants.get(&principal.id) else {
                    return Decision::Deny(DenyDecision::boot(DenyReason::CredentialUnknown));
                };
                if !grant.actions.contains(&action) {
                    return Decision::Deny(DenyDecision::boot(DenyReason::ActionNotGranted));
                }
                if let Some(namespace) = resource.namespace() {
                    if !grant.all_namespaces && !grant.namespaces.contains(namespace) {
                        return Decision::Deny(DenyDecision::boot(DenyReason::NamespaceNotGranted));
                    }
                }

                let mut allow = AllowDecision::boot(action);
                allow.cursor_binding_key = self.cursor_binding_key;
                if action == Action::VectorUpsert && grant.actions.contains(&Action::AttributeAdmin)
                {
                    allow.mark_attribute_admin_write();
                }
                Decision::Allow(Box::new(allow))
            }
            SecurityAuthority::Policy(cache) => {
                let cached = cache.current();
                let version = cached.policy.version();
                if !cache.is_fresh(&cached) {
                    return Decision::Deny(DenyDecision::for_policy(
                        DenyReason::SecurityStale,
                        version,
                    ));
                }
                match cached
                    .policy
                    .authorize(principal, context.now, action, resource)
                {
                    Ok(constraints) => {
                        let mut allow = AllowDecision::for_policy(action, version);
                        allow.policy_checksum = Some(cached.snapshot.checksum().to_string());
                        allow.cursor_binding_key = self.cursor_binding_key;
                        let attribute_admin = constraints.attribute_admin;
                        allow.policy_filter = constraints.mandatory_filter.clone();
                        allow.mandatory_filter = constraints.mandatory_filter;
                        allow.field_mask = constraints.field_mask;
                        allow.write_constraints = constraints.write_constraints;
                        if action == Action::VectorUpsert && attribute_admin {
                            allow.mark_attribute_admin_write();
                        }
                        if constraints.require_approval {
                            allow.require_approval();
                        }
                        Decision::Allow(Box::new(allow))
                    }
                    Err(reason) => Decision::Deny(DenyDecision::for_policy(reason, version)),
                }
            }
        }
    }

    /// Check one action before a handler resolves its body-derived namespace target.
    #[must_use]
    pub fn authorize_action(
        &self,
        principal: &Principal,
        action: Action,
        context: &RequestContext,
    ) -> Decision {
        if self.mode == SecurityMode::OpenUnsafe && principal.is_anonymous() {
            return Decision::Allow(Box::new(AllowDecision::boot(action)));
        }
        if principal.is_anonymous() {
            return Decision::Deny(DenyDecision::boot(DenyReason::Unauthenticated));
        }
        if principal.delegation.is_some() {
            return self.authorize_delegated_action(principal, action, context.now);
        }
        match &self.authority {
            SecurityAuthority::Bootstrap(grants) => {
                if principal
                    .expires_at
                    .is_some_and(|expires_at| expires_at <= context.now)
                {
                    return Decision::Deny(DenyDecision::boot(DenyReason::CredentialExpired));
                }
                let Some(grant) = grants.get(&principal.id) else {
                    return Decision::Deny(DenyDecision::boot(DenyReason::CredentialUnknown));
                };
                if !grant.actions.contains(&action) {
                    return Decision::Deny(DenyDecision::boot(DenyReason::ActionNotGranted));
                }
                Decision::Allow(Box::new(AllowDecision::boot(action)))
            }
            SecurityAuthority::Policy(cache) => {
                let cached = cache.current();
                let version = cached.policy.version();
                if !cache.is_fresh(&cached) {
                    return Decision::Deny(DenyDecision::for_policy(
                        DenyReason::SecurityStale,
                        version,
                    ));
                }
                if let Err(reason) = cached
                    .policy
                    .authorize_action(principal, context.now, action)
                {
                    return Decision::Deny(DenyDecision::for_policy(reason, version));
                }
                Decision::Allow(Box::new(AllowDecision::for_policy(action, version)))
            }
        }
    }

    fn authorize_delegated(
        &self,
        principal: &Principal,
        action: Action,
        resource: &Resource,
        now: DateTime<Utc>,
    ) -> Decision {
        let Some(delegation) = principal.delegation.as_ref() else {
            return Decision::Deny(DenyDecision::boot(DenyReason::CredentialUnknown));
        };
        let SecurityAuthority::Policy(cache) = &self.authority else {
            return Decision::Deny(DenyDecision::boot(DenyReason::CredentialUnknown));
        };
        let cached = cache.current();
        let version = cached.policy.version();
        if !cache.is_fresh(&cached) {
            return Decision::Deny(DenyDecision::for_policy(DenyReason::SecurityStale, version));
        }
        if !cached.policy.delegated_parent_credential_is_active(
            delegation.parent_principal(),
            delegation.parent_api_key_id(),
            now,
        ) {
            return Decision::Deny(DenyDecision::for_policy(
                DenyReason::CredentialUnknown,
                version,
            ));
        }
        if let Some(namespace) = resource.namespace() {
            if !delegation.narrowed().allows(action, namespace) {
                let reason = if delegation.narrowed().actions().contains(&action) {
                    DenyReason::NamespaceNotGranted
                } else {
                    DenyReason::ActionNotGranted
                };
                return Decision::Deny(DenyDecision::for_policy(reason, version));
            }
        } else if !delegation.narrowed().actions().contains(&action) {
            return Decision::Deny(DenyDecision::for_policy(
                DenyReason::ActionNotGranted,
                version,
            ));
        }
        for delegated_action in delegation.narrowed().actions() {
            for namespace in delegation.narrowed().namespaces() {
                if let Err(reason) = cached.policy.authorize_delegated_parent(
                    delegation.parent_principal(),
                    *delegated_action,
                    &Resource::Namespace(namespace.clone()),
                ) {
                    return Decision::Deny(DenyDecision::for_policy(reason, version));
                }
            }
        }
        match cached.policy.authorize_delegated_parent(
            delegation.parent_principal(),
            action,
            resource,
        ) {
            Ok(constraints) => {
                let mut allow = AllowDecision::for_policy(action, version);
                allow.policy_checksum = Some(cached.snapshot.checksum().to_string());
                allow.cursor_binding_key = self.cursor_binding_key;
                allow.policy_filter = constraints.mandatory_filter.clone();
                allow.mandatory_filter = crate::index::filter::combine_filters(
                    constraints.mandatory_filter,
                    delegation.narrowed().mandatory_filter().cloned(),
                );
                allow.field_mask = constraints.field_mask;
                allow.write_constraints = constraints.write_constraints;
                if action == Action::VectorUpsert
                    && constraints.attribute_admin
                    && delegation
                        .narrowed()
                        .actions()
                        .contains(&Action::AttributeAdmin)
                {
                    allow.mark_attribute_admin_write();
                }
                if constraints.require_approval || action.is_destructive() {
                    allow.require_approval();
                }
                Decision::Allow(Box::new(allow))
            }
            Err(reason) => Decision::Deny(DenyDecision::for_policy(reason, version)),
        }
    }

    fn authorize_delegated_action(
        &self,
        principal: &Principal,
        action: Action,
        now: DateTime<Utc>,
    ) -> Decision {
        let Some(delegation) = principal.delegation.as_ref() else {
            return Decision::Deny(DenyDecision::boot(DenyReason::CredentialUnknown));
        };
        let SecurityAuthority::Policy(cache) = &self.authority else {
            return Decision::Deny(DenyDecision::boot(DenyReason::CredentialUnknown));
        };
        let cached = cache.current();
        let version = cached.policy.version();
        if !cache.is_fresh(&cached) {
            return Decision::Deny(DenyDecision::for_policy(DenyReason::SecurityStale, version));
        }
        if !cached.policy.delegated_parent_credential_is_active(
            delegation.parent_principal(),
            delegation.parent_api_key_id(),
            now,
        ) {
            return Decision::Deny(DenyDecision::for_policy(
                DenyReason::CredentialUnknown,
                version,
            ));
        }
        if !delegation.narrowed().actions().contains(&action) {
            return Decision::Deny(DenyDecision::for_policy(
                DenyReason::ActionNotGranted,
                version,
            ));
        }
        match cached
            .policy
            .authorize_delegated_parent_action(delegation.parent_principal(), action)
        {
            Ok(()) => Decision::Allow(Box::new(AllowDecision::for_policy(action, version))),
            Err(reason) => Decision::Deny(DenyDecision::for_policy(reason, version)),
        }
    }

    /// Mint one short-lived credential only after proving structural narrowing.
    pub fn mint_delegated_token(
        &self,
        parent: &Principal,
        narrowed: DelegationNarrowing,
        requested_ttl_secs: u64,
        now: DateTime<Utc>,
    ) -> SecurityOperationResult<(IssuedDelegatedToken, AllowDecision)> {
        if parent.delegation.is_some() {
            return Err(SecurityError::DelegationChainingForbidden.into());
        }
        if !matches!(
            parent.kind,
            super::PrincipalKind::Human | super::PrincipalKind::Service
        ) {
            return Err(SecurityError::DelegationPrincipalKindForbidden.into());
        }
        let Some(authority) = &self.delegation else {
            return Err(SecurityError::FeatureNotLicensed(super::Feature::Delegation).into());
        };
        let SecurityAuthority::Policy(cache) = &self.authority else {
            return Err(SecurityError::FeatureRequired(super::Feature::Rbac).into());
        };
        let cached = cache.current();
        let policy_version = cached.policy.version();
        if !cache.is_fresh(&cached) {
            return Err(super::SecurityOperationError::denied(
                DenyDecision::for_policy(DenyReason::SecurityStale, policy_version),
            ));
        }
        if !cached.policy.credential_is_active(parent, now) {
            return Err(super::SecurityOperationError::denied(
                DenyDecision::for_policy(DenyReason::CredentialUnknown, policy_version),
            ));
        }
        let constraints = cached
            .policy
            .authorize(
                parent,
                now,
                Action::CredentialDelegate,
                &Resource::SecurityPolicy,
            )
            .map_err(|reason| {
                super::SecurityOperationError::denied(DenyDecision::for_policy(
                    reason,
                    policy_version,
                ))
            })?;
        let mut allow = AllowDecision::for_policy(Action::CredentialDelegate, policy_version);
        allow.policy_filter = constraints.mandatory_filter.clone();
        allow.mandatory_filter = constraints.mandatory_filter;
        allow.field_mask = constraints.field_mask;
        allow.write_constraints = constraints.write_constraints;
        if allow.mandatory_filter.is_some()
            || allow.field_mask.is_some()
            || !allow.write_constraints.is_empty()
        {
            return Err(super::SecurityOperationError::after_allow(
                SecurityError::ConstraintViolation.into(),
                allow,
            ));
        }
        for action in narrowed.actions() {
            for namespace in narrowed.namespaces() {
                let parent_allows = cached
                    .policy
                    .authorize(
                        parent,
                        now,
                        *action,
                        &Resource::Namespace(namespace.clone()),
                    )
                    .is_ok();
                if !narrowed.effective_allows(*action, namespace, parent_allows) {
                    return Err(super::SecurityOperationError::denied_with_error(
                        DenyDecision::for_policy(DenyReason::ActionNotGranted, policy_version),
                        SecurityError::DelegationScopeExceeded.into(),
                    ));
                }
            }
        }
        let parent_api_key_id = parent.api_key_id.clone().ok_or_else(|| {
            super::SecurityOperationError::denied(DenyDecision::for_policy(
                DenyReason::CredentialUnknown,
                policy_version,
            ))
        })?;
        authority
            .mint(
                parent.id.clone(),
                parent_api_key_id,
                narrowed,
                requested_ttl_secs,
                now,
            )
            .map(|issued| (issued, allow.clone()))
            .map_err(|error| super::SecurityOperationError::after_allow(error.into(), allow))
    }

    /// Fail closed unless one verified policy version proves that a raw namespace
    /// copy preserves every principal's namespace-scoped authority.
    pub(crate) fn validate_namespace_copy_no_widening(
        &self,
        expected_policy_version: super::PolicyVersion,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<(), SecurityError> {
        match &self.authority {
            SecurityAuthority::Bootstrap(grants) => {
                if expected_policy_version != super::PolicyVersion::BOOT {
                    return Err(SecurityError::ConstraintViolation);
                }
                for grant in grants.values() {
                    for action in super::policy::DERIVED_NAMESPACE_ACTIONS {
                        if grant.allows_namespace(action, target)
                            && !grant.allows_namespace(action, source)
                        {
                            return Err(SecurityError::ConstraintViolation);
                        }
                    }
                }
                Ok(())
            }
            SecurityAuthority::Policy(cache) => {
                let cached = cache.current();
                if cached.policy.version() != expected_policy_version || !cache.is_fresh(&cached) {
                    return Err(SecurityError::ConstraintViolation);
                }
                cached
                    .policy
                    .validate_namespace_copy_no_widening(source, target)
            }
        }
    }

    /// Add one stable principal through the authoritative CAS publication path.
    pub async fn create_principal(
        &self,
        actor: &Principal,
        principal_id: PrincipalId,
        kind: PrincipalKind,
        display_name: String,
    ) -> SecurityOperationResult<(AllowDecision, super::PolicyVersion, PolicyPrincipal)> {
        match &self.authority {
            SecurityAuthority::Policy(cache) => {
                cache
                    .create_principal(actor, principal_id, kind, display_name)
                    .await
            }
            SecurityAuthority::Bootstrap(_) => Err(SecurityError::InvalidPolicyRequest(
                "security administration requires S3 policy authority".to_string(),
            )
            .into()),
        }
    }

    /// Generate and persist one hashed API key, returning plaintext once.
    pub async fn create_key(
        &self,
        actor: &Principal,
        principal_id: PrincipalId,
        name: String,
        expires_at: Option<DateTime<Utc>>,
    ) -> SecurityOperationResult<IssuedApiKey> {
        match &self.authority {
            SecurityAuthority::Policy(cache) => {
                cache
                    .create_key(actor, principal_id, name, expires_at)
                    .await
            }
            SecurityAuthority::Bootstrap(_) => Err(SecurityError::InvalidPolicyRequest(
                "security administration requires S3 policy authority".to_string(),
            )
            .into()),
        }
    }

    /// Revoke one API key through the authoritative CAS publication path.
    pub async fn revoke_key(
        &self,
        actor: &Principal,
        key_id: &super::ApiKeyId,
    ) -> SecurityOperationResult<(AllowDecision, super::PolicyVersion)> {
        match &self.authority {
            SecurityAuthority::Policy(cache) => cache.revoke_key(actor, key_id).await,
            SecurityAuthority::Bootstrap(_) => Err(SecurityError::InvalidPolicyRequest(
                "security administration requires S3 policy authority".to_string(),
            )
            .into()),
        }
    }

    /// Rotate one API key atomically, retaining the predecessor for the overlap.
    pub async fn rotate_key(
        &self,
        actor: &Principal,
        key_id: &super::ApiKeyId,
        overlap_secs: u64,
    ) -> SecurityOperationResult<IssuedApiKey> {
        match &self.authority {
            SecurityAuthority::Policy(cache) => cache.rotate_key(actor, key_id, overlap_secs).await,
            SecurityAuthority::Bootstrap(_) => Err(SecurityError::InvalidPolicyRequest(
                "security administration requires S3 policy authority".to_string(),
            )
            .into()),
        }
    }

    /// Add one exact principal/scope/action grant through CAS publication.
    pub async fn add_grant(
        &self,
        actor: &Principal,
        definition: GrantDefinition,
    ) -> SecurityOperationResult<(AllowDecision, super::PolicyVersion, PolicyGrant)> {
        match &self.authority {
            SecurityAuthority::Policy(cache) => cache.add_grant(actor, definition).await,
            SecurityAuthority::Bootstrap(_) => Err(SecurityError::InvalidPolicyRequest(
                "security administration requires S3 policy authority".to_string(),
            )
            .into()),
        }
    }

    /// Remove one exact principal/scope/action grant through CAS publication.
    pub async fn remove_grant(
        &self,
        actor: &Principal,
        principal_id: PrincipalId,
        scope: GrantScope,
        actions: GrantActions,
    ) -> SecurityOperationResult<(AllowDecision, super::PolicyVersion, PolicyGrant)> {
        match &self.authority {
            SecurityAuthority::Policy(cache) => {
                cache
                    .remove_grant(actor, principal_id, scope, actions)
                    .await
            }
            SecurityAuthority::Bootstrap(_) => Err(SecurityError::InvalidPolicyRequest(
                "security administration requires S3 policy authority".to_string(),
            )
            .into()),
        }
    }

    /// Return the current verified snapshot for redacted administration views.
    pub fn policy_snapshot(
        &self,
        actor: &Principal,
        now: DateTime<Utc>,
    ) -> SecurityOperationResult<(AllowDecision, Arc<PolicySnapshot>)> {
        match &self.authority {
            SecurityAuthority::Policy(cache) => cache.authorized_snapshot(actor, now),
            SecurityAuthority::Bootstrap(_) => Err(SecurityError::InvalidPolicyRequest(
                "security administration requires S3 policy authority".to_string(),
            )
            .into()),
        }
    }

    /// Return one atomically authorized head plus active policy snapshot.
    pub fn policy_view(
        &self,
        actor: &Principal,
        now: DateTime<Utc>,
    ) -> SecurityOperationResult<(AllowDecision, super::PolicyHead, Arc<PolicySnapshot>)> {
        match &self.authority {
            SecurityAuthority::Policy(cache) => cache.authorized_policy_view(actor, now),
            SecurityAuthority::Bootstrap(_) => Err(SecurityError::InvalidPolicyRequest(
                "security administration requires S3 policy authority".to_string(),
            )
            .into()),
        }
    }

    /// Resolve the historical policy-filter component for a receipt verifier.
    ///
    /// Only a caller lacking `SecurityAdminRead` receives `Unchecked`. Once a
    /// caller is privileged, missing or inconsistent historical evidence is a
    /// divergence rather than a silent downgrade. The route never reveals the
    /// predicate itself.
    #[allow(dead_code)]
    pub(crate) async fn receipt_policy_filter(
        &self,
        lookup: ReceiptPolicyLookup<'_>,
    ) -> ZeppelinResult<ReceiptPolicyResolution> {
        if !matches!(
            self.authorize(
                lookup.verifier,
                Action::SecurityAdminRead,
                &Resource::SecurityPolicy,
                lookup.context,
            ),
            Decision::Allow(_)
        ) {
            return Ok(ReceiptPolicyResolution::Unchecked);
        }
        let delegated = lookup.delegation_parent.is_some();
        let SecurityAuthority::Policy(cache) = &self.authority else {
            return Ok(ReceiptPolicyResolution::Diverged { delegated });
        };
        let Some(checksum) = lookup.checksum else {
            return Ok(ReceiptPolicyResolution::Diverged { delegated });
        };
        let policy_principal = lookup.delegation_parent.unwrap_or(lookup.receipt_principal);
        Ok(
            match cache
                .historical_query_filter(
                    lookup.version,
                    checksum,
                    policy_principal,
                    lookup.namespace,
                )
                .await?
            {
                Some(filter) => ReceiptPolicyResolution::Resolved { filter, delegated },
                None => ReceiptPolicyResolution::Diverged { delegated },
            },
        )
    }

    /// Force one authoritative policy-head refresh for integration harnesses.
    ///
    /// Production refresh scheduling remains internal and monotonic. This
    /// explicit seam exists so the adversarial runner can close its bounded
    /// staleness window before final security oracles execute.
    #[doc(hidden)]
    pub async fn refresh_authoritative_policy_for_test(&self) -> ZeppelinResult<()> {
        match &self.authority {
            SecurityAuthority::Policy(cache) => {
                cache.refresh_once().await?;
                if let Some(delegation) = &self.delegation {
                    delegation.refresh_signers_once().await?;
                }
                Ok(())
            }
            SecurityAuthority::Bootstrap(_) => Err(SecurityError::InvalidPolicyRequest(
                "authoritative policy refresh requires S3 policy authority".to_string(),
            )
            .into()),
        }
    }
}

fn validate_namespace_fork_admission(admission: &NamespaceForkAdmission) -> ZeppelinResult<()> {
    validate_fork_decision_set(
        &admission.fork_decision,
        &admission.source_read_decision,
        &admission.target_create_decision,
    )?;
    let approval_required = admission
        .fork_decision
        .obligations
        .contains(&Obligation::Approval);
    if approval_required != admission.approver.is_some() {
        return Err(SecurityError::ApprovalRequired.into());
    }
    if admission.approver.as_ref().is_some_and(|approver| {
        approver.id == admission.principal.id
            || admission
                .principal
                .delegation_parent
                .as_ref()
                .is_some_and(|parent| parent == &approver.id)
    }) {
        return Err(SecurityError::ApprovalRequired.into());
    }
    Ok(())
}

fn validate_fork_decision_set(
    fork: &AllowDecision,
    source_read: &AllowDecision,
    target_create: &AllowDecision,
) -> ZeppelinResult<()> {
    if fork.policy_version != source_read.policy_version
        || fork.policy_version != target_create.policy_version
        || !fork.obligations.contains(&Obligation::DurableAudit)
        || [fork, source_read, target_create]
            .into_iter()
            .any(allow_has_copy_constraints)
    {
        return Err(SecurityError::ConstraintViolation.into());
    }
    Ok(())
}

fn allow_has_copy_constraints(allow: &AllowDecision) -> bool {
    allow.mandatory_filter.is_some()
        || allow.field_mask.is_some()
        || !allow.write_constraints.is_empty()
}

fn require_allow(decision: Decision) -> ZeppelinResult<AllowDecision> {
    match decision {
        Decision::Allow(allow) => Ok(*allow),
        Decision::Deny(deny) => Err(SecurityError::Authorization(deny.reason).into()),
    }
}

fn validate_fresh_approver(
    kernel: &SecurityKernel,
    admission: &NamespaceForkAdmission,
    context: &RequestContext,
    fork: &AllowDecision,
) -> ZeppelinResult<Option<PrincipalId>> {
    let approval_required = fork.obligations.contains(&Obligation::Approval);
    let Some(approver) = admission.approver.as_ref() else {
        return if approval_required {
            Err(SecurityError::ApprovalRequired.into())
        } else {
            Ok(None)
        };
    };
    if !approval_required
        || approver.id == admission.principal.id
        || admission
            .principal
            .delegation_parent
            .as_ref()
            .is_some_and(|parent| parent == &approver.id)
    {
        return Err(SecurityError::ApprovalRequired.into());
    }
    let allow = require_allow(kernel.authorize(
        approver,
        Action::NamespaceFork,
        &Resource::Namespace(admission.source.clone()),
        context,
    ))?;
    if allow.policy_version != fork.policy_version
        || allow.obligations.contains(&Obligation::Approval)
        || allow_has_copy_constraints(&allow)
    {
        return Err(SecurityError::ApprovalRequired.into());
    }
    Ok(Some(approver.id.clone()))
}

impl SecurityKernel {
    fn fresh_current_fork_authorization(
        &self,
        admission: &NamespaceForkAdmission,
    ) -> ZeppelinResult<FreshForkAuthorization> {
        if !self.entitlements.has(super::Feature::Branching) {
            return Err(SecurityError::FeatureNotLicensed(super::Feature::Branching).into());
        }
        let authorized_at = admission.clock.now();
        let context = RequestContext::at(admission.context.request_id.clone(), authorized_at);
        let fork = require_allow(self.authorize(
            &admission.principal,
            Action::NamespaceFork,
            &Resource::Namespace(admission.source.clone()),
            &context,
        ))?;
        let source_read = require_allow(self.authorize(
            &admission.principal,
            Action::NamespaceRead,
            &Resource::Namespace(admission.source.clone()),
            &context,
        ))?;
        let target_create = require_allow(self.authorize(
            &admission.principal,
            Action::NamespaceCreate,
            &Resource::Namespace(admission.target.clone()),
            &context,
        ))?;
        validate_fork_decision_set(&fork, &source_read, &target_create)?;
        self.validate_namespace_copy_no_widening(
            fork.policy_version,
            &admission.source,
            &admission.target,
        )?;
        let approver = validate_fresh_approver(self, admission, &context, &fork)?;
        Ok(FreshForkAuthorization {
            fork,
            approver,
            authorized_at,
        })
    }

    fn fresh_loaded_policy_fork_authorization(
        &self,
        admission: &NamespaceForkAdmission,
        loaded: &LoadedPolicy,
    ) -> SecurityOperationResult<FreshForkAuthorization> {
        let version = loaded.snapshot().version();
        if admission.principal.delegation.is_some() {
            return Err(SecurityOperationError::denied(DenyDecision::for_policy(
                DenyReason::ActionNotGranted,
                version,
            )));
        }
        if !self.entitlements.has(super::Feature::Branching) {
            return Err(SecurityError::FeatureNotLicensed(super::Feature::Branching).into());
        }
        let authorized_at = admission.clock.now();
        let compiled = loaded.snapshot().compile()?;
        let authorize = |principal: &Principal,
                         action: Action,
                         resource: Resource|
         -> SecurityOperationResult<AllowDecision> {
            let constraints = compiled
                .authorize(principal, authorized_at, action, &resource)
                .map_err(|reason| {
                    SecurityOperationError::denied(DenyDecision::for_policy(reason, version))
                })?;
            let mut allow = AllowDecision::for_policy(action, version);
            allow.policy_checksum = Some(loaded.snapshot().checksum().to_string());
            allow.cursor_binding_key = self.cursor_binding_key;
            allow.policy_filter = constraints.mandatory_filter.clone();
            allow.mandatory_filter = constraints.mandatory_filter;
            allow.field_mask = constraints.field_mask;
            allow.write_constraints = constraints.write_constraints;
            if constraints.require_approval {
                allow.require_approval();
            }
            Ok(allow)
        };
        let fork = authorize(
            &admission.principal,
            Action::NamespaceFork,
            Resource::Namespace(admission.source.clone()),
        )?;
        let source_read = authorize(
            &admission.principal,
            Action::NamespaceRead,
            Resource::Namespace(admission.source.clone()),
        )?;
        let target_create = authorize(
            &admission.principal,
            Action::NamespaceCreate,
            Resource::Namespace(admission.target.clone()),
        )?;
        validate_fork_decision_set(&fork, &source_read, &target_create)
            .map_err(|error| SecurityOperationError::after_allow(error, fork.clone()))?;
        compiled
            .validate_namespace_copy_no_widening(&admission.source, &admission.target)
            .map_err(|error| SecurityOperationError::after_allow(error.into(), fork.clone()))?;

        let approval_required = fork.obligations.contains(&Obligation::Approval);
        let approver = match admission.approver.as_ref() {
            None if approval_required => {
                return Err(SecurityOperationError::after_allow(
                    SecurityError::ApprovalRequired.into(),
                    fork,
                ))
            }
            None => None,
            Some(approver)
                if !approval_required
                    || approver.id == admission.principal.id
                    || admission
                        .principal
                        .delegation_parent
                        .as_ref()
                        .is_some_and(|parent| parent == &approver.id) =>
            {
                return Err(SecurityOperationError::after_allow(
                    SecurityError::ApprovalRequired.into(),
                    fork,
                ))
            }
            Some(approver) => {
                let approval = authorize(
                    approver,
                    Action::NamespaceFork,
                    Resource::Namespace(admission.source.clone()),
                )?;
                if approval.obligations.contains(&Obligation::Approval)
                    || allow_has_copy_constraints(&approval)
                {
                    return Err(SecurityOperationError::after_allow(
                        SecurityError::ApprovalRequired.into(),
                        fork,
                    ));
                }
                Some(approver.id.clone())
            }
        };
        Ok(FreshForkAuthorization {
            fork,
            approver,
            authorized_at,
        })
    }
}

fn validate_prepared_fork(
    admission: &NamespaceForkAdmission,
    prepared: &PreparedBranch,
) -> ZeppelinResult<()> {
    if prepared.identity.source_namespace != admission.source
        || prepared.identity.target_namespace != admission.target
        || prepared.root.branch_id != prepared.identity.branch_id
        || prepared.root.target_namespace != admission.target
        || prepared.root.target_incarnation != prepared.identity.target_incarnation
    {
        return Err(crate::namespace::branching::BranchError::IntentMismatch {
            target: admission.target.clone(),
        }
        .into());
    }
    Ok(())
}

fn branch_activation_audit_ref(
    admission: &NamespaceForkAdmission,
    prepared: &PreparedBranch,
    decision_id: super::DecisionId,
    nonce: ActivationNonce,
) -> String {
    format!(
        "namespace-fork-activation:{}:{}:{}:{}",
        admission.context.request_id,
        prepared.identity.branch_id,
        decision_id.get(),
        nonce
    )
}

fn branch_activation_evidence(
    admission: &NamespaceForkAdmission,
    prepared: &PreparedBranch,
    fresh: &FreshForkAuthorization,
    policy_head: PolicyHeadIdentity,
) -> BranchActivationEvidence {
    let nonce = policy_head.activation_nonce();
    BranchActivationEvidence {
        branch_id: prepared.identity.branch_id,
        target_namespace: prepared.identity.target_namespace.clone(),
        target_incarnation: prepared.identity.target_incarnation.clone(),
        policy_head,
        decision_id: fresh.fork.decision_id,
        approver: fresh.approver.clone(),
        audit_evidence_ref: branch_activation_audit_ref(
            admission,
            prepared,
            fresh.fork.decision_id,
            nonce,
        ),
        activated_at: fresh.authorized_at,
    }
}

fn activation_audit_record(
    admission: &NamespaceForkAdmission,
    prepared: &PreparedBranch,
    evidence: &BranchActivationEvidence,
) -> AuditRecord {
    let mut record = AuditRecord::decision_outcome(
        evidence.activated_at,
        admission.context.request_id.clone(),
        evidence.decision_id,
        &admission.principal,
        Action::NamespaceFork,
        ResourceRef::Namespace {
            namespace: admission.source.clone(),
        },
        match &evidence.policy_head {
            PolicyHeadIdentity::Boot { .. } => super::PolicyVersion::BOOT,
            PolicyHeadIdentity::Persisted { policy_version, .. } => *policy_version,
        },
        admission.source_ip,
        AuditOutcome::Success,
        AuditParams::NamespaceForkActivation {
            source: admission.source.clone(),
            target: admission.target.clone(),
            branch_id: prepared.identity.branch_id,
            target_incarnation: prepared.identity.target_incarnation.clone(),
            source_generation: prepared.identity.source_generation,
            depth: prepared.identity.depth,
            activation_nonce: evidence.activation_nonce(),
            admission_decision_id: admission.fork_decision.decision_id,
        },
        admission.audit.node_id(),
    );
    record.approval_principal_id = evidence.approver.clone();
    record
}

fn validate_reauthorization(
    fresh: &FreshForkAuthorization,
    evidence: &BranchActivationEvidence,
) -> ZeppelinResult<()> {
    let expected_version = match &evidence.policy_head {
        PolicyHeadIdentity::Boot { .. } => super::PolicyVersion::BOOT,
        PolicyHeadIdentity::Persisted { policy_version, .. } => *policy_version,
    };
    if fresh.fork.policy_version != expected_version || fresh.approver != evidence.approver {
        return Err(SecurityError::ConstraintViolation.into());
    }
    Ok(())
}

#[async_trait]
impl BranchActivationGovernance for NamespaceForkGovernance {
    async fn begin(
        &self,
        prepared: &PreparedBranch,
        nonce: ActivationNonce,
    ) -> ZeppelinResult<Box<dyn BranchActivationPermit>> {
        validate_prepared_fork(&self.admission, prepared)?;
        match &self.kernel.authority {
            SecurityAuthority::Bootstrap(_) => {
                let fresh = self
                    .kernel
                    .fresh_current_fork_authorization(&self.admission)?;
                if fresh.fork.policy_version != super::PolicyVersion::BOOT {
                    return Err(SecurityError::ConstraintViolation.into());
                }
                let evidence = branch_activation_evidence(
                    &self.admission,
                    prepared,
                    &fresh,
                    PolicyHeadIdentity::Boot {
                        activation_nonce: nonce,
                    },
                );
                Ok(Box::new(BootstrapBranchActivationPermit {
                    kernel: Arc::clone(&self.kernel),
                    admission: self.admission.clone(),
                    prepared: prepared.clone(),
                    evidence,
                    audit_settled: false,
                }))
            }
            SecurityAuthority::Policy(cache) => {
                let expires_at = self
                    .admission
                    .clock
                    .now()
                    .checked_add_signed(ChronoDuration::seconds(BRANCH_ACTIVATION_GUARD_TTL_SECS))
                    .ok_or_else(|| {
                        SecurityError::InvalidPolicy(
                            "branch activation guard expiry overflow".to_string(),
                        )
                    })?;
                let kernel = Arc::clone(&self.kernel);
                let validation_admission = self.admission.clone();
                let (_, fresh, guard) = cache
                    .begin_branch_activation(
                        prepared.identity.branch_id,
                        prepared.identity.target_namespace.clone(),
                        prepared.identity.target_incarnation.clone(),
                        nonce,
                        expires_at,
                        move |loaded| async move {
                            let fresh = kernel.fresh_loaded_policy_fork_authorization(
                                &validation_admission,
                                &loaded,
                            )?;
                            Ok((fresh.fork.clone(), fresh))
                        },
                    )
                    .await
                    .map_err(SecurityOperationError::into_error)?;
                let evidence = branch_activation_evidence(
                    &self.admission,
                    prepared,
                    &fresh,
                    PolicyHeadIdentity::Persisted {
                        policy_version: guard.policy_version(),
                        policy_head_digest: guard.policy_head_digest(),
                        control_revision: guard.control_revision(),
                        lease_fencing_token: guard.lease_fencing_token(),
                        activation_nonce: nonce,
                    },
                );
                Ok(Box::new(PolicyBranchActivationPermit {
                    kernel: Arc::clone(&self.kernel),
                    cache: Arc::clone(cache),
                    admission: self.admission.clone(),
                    prepared: prepared.clone(),
                    guard,
                    evidence,
                    audit_settled: false,
                }))
            }
        }
    }

    async fn retain_guard(
        &self,
        prepared: &PreparedBranch,
        expected_nonce: Option<ActivationNonce>,
    ) -> ZeppelinResult<Option<Box<dyn BranchActivationGuard>>> {
        validate_prepared_fork(&self.admission, prepared)?;
        let target = BranchActivationTarget::from_identity(&prepared.identity);
        match &self.kernel.authority {
            SecurityAuthority::Bootstrap(_) => Ok(None),
            SecurityAuthority::Policy(cache) => cache
                .retain_pending_branch_activation(&target, expected_nonce)
                .await
                .map(|guard| {
                    guard.map(|guard| {
                        let attempt = guard.attempt();
                        Box::new(PolicyRetainedBranchActivationGuard {
                            cache: Arc::clone(cache),
                            guard,
                            attempt,
                        }) as Box<dyn BranchActivationGuard>
                    })
                }),
        }
    }
}

#[async_trait]
impl BranchActivationPermit for BootstrapBranchActivationPermit {
    fn evidence(&self) -> &BranchActivationEvidence {
        &self.evidence
    }

    async fn settle_audit(&mut self) -> ZeppelinResult<()> {
        if self.audit_settled {
            return Ok(());
        }
        self.admission
            .audit
            .submit_durable(activation_audit_record(
                &self.admission,
                &self.prepared,
                &self.evidence,
            ))
            .await?;
        self.audit_settled = true;
        Ok(())
    }

    async fn revalidate(&mut self) -> ZeppelinResult<()> {
        if !self.audit_settled {
            return Err(SecurityError::AuditUnavailable.into());
        }
        let fresh = self
            .kernel
            .fresh_current_fork_authorization(&self.admission)?;
        validate_reauthorization(&fresh, &self.evidence)
    }

    async fn finalize(self: Box<Self>) -> ZeppelinResult<()> {
        Ok(())
    }

    async fn abort(self: Box<Self>) -> ZeppelinResult<()> {
        Ok(())
    }
}

#[async_trait]
impl BranchActivationPermit for PolicyBranchActivationPermit {
    fn evidence(&self) -> &BranchActivationEvidence {
        &self.evidence
    }

    async fn settle_audit(&mut self) -> ZeppelinResult<()> {
        if self.audit_settled {
            return Ok(());
        }
        self.admission
            .audit
            .submit_durable(activation_audit_record(
                &self.admission,
                &self.prepared,
                &self.evidence,
            ))
            .await?;
        self.audit_settled = true;
        Ok(())
    }

    async fn revalidate(&mut self) -> ZeppelinResult<()> {
        if !self.audit_settled {
            return Err(SecurityError::AuditUnavailable.into());
        }
        self.cache
            .renew_branch_activation_guard(&mut self.guard)
            .await?;
        let fresh = self
            .kernel
            .fresh_current_fork_authorization(&self.admission)?;
        validate_reauthorization(&fresh, &self.evidence)
    }

    async fn finalize(self: Box<Self>) -> ZeppelinResult<()> {
        let permit = *self;
        permit.cache.finalize_branch_activation(permit.guard).await
    }

    async fn abort(self: Box<Self>) -> ZeppelinResult<()> {
        let permit = *self;
        permit.cache.abort_branch_activation(permit.guard).await
    }
}

#[async_trait]
impl BranchActivationGuard for PolicyRetainedBranchActivationGuard {
    fn attempt(&self) -> &BranchActivationAttempt {
        &self.attempt
    }

    async fn finalize(self: Box<Self>) -> ZeppelinResult<()> {
        let guard = *self;
        guard.cache.finalize_branch_activation(guard.guard).await
    }

    async fn abort(self: Box<Self>) -> ZeppelinResult<()> {
        let guard = *self;
        guard.cache.abort_branch_activation(guard.guard).await
    }
}

#[async_trait]
impl BranchActivationRecovery for NamespaceBranchActivationRecovery {
    async fn retain_branch(
        &self,
        target: &BranchActivationTarget,
    ) -> ZeppelinResult<Option<Box<dyn BranchActivationGuard>>> {
        match &self.kernel.authority {
            SecurityAuthority::Bootstrap(_) => Ok(None),
            SecurityAuthority::Policy(cache) => cache
                .retain_pending_branch_activation(target, None)
                .await
                .map(|guard| {
                    guard.map(|guard| {
                        let attempt = guard.attempt();
                        Box::new(PolicyRetainedBranchActivationGuard {
                            cache: Arc::clone(cache),
                            guard,
                            attempt,
                        }) as Box<dyn BranchActivationGuard>
                    })
                }),
        }
    }

    async fn retain_next_expired(&self) -> ZeppelinResult<Option<Box<dyn BranchActivationGuard>>> {
        match &self.kernel.authority {
            SecurityAuthority::Bootstrap(_) => Ok(None),
            SecurityAuthority::Policy(cache) => cache
                .retain_next_expired_branch_activation()
                .await
                .map(|guard| {
                    guard.map(|guard| {
                        let attempt = guard.attempt();
                        Box::new(PolicyRetainedBranchActivationGuard {
                            cache: Arc::clone(cache),
                            guard,
                            attempt,
                        }) as Box<dyn BranchActivationGuard>
                    })
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{
        config::Config,
        security::{NamespaceId, PolicyVersion, SecurityError},
    };

    use super::SecurityKernel;

    fn fixture<T, E: std::fmt::Debug>(result: Result<T, E>, context: &str) -> T {
        result.unwrap_or_else(|error| panic!("{context}: {error:?}"))
    }

    fn kernel(namespaces: &[&str]) -> SecurityKernel {
        let namespaces = namespaces
            .iter()
            .map(|namespace| format!(r#""{namespace}""#))
            .collect::<Vec<_>>()
            .join(", ");
        let config = fixture(
            Config::from_str(&format!(
                r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_reader"
name = "reader"
sha256_hex = "0000000000000000000000000000000000000000000000000000000000000000"
actions = ["NamespaceRead", "Query", "VectorFetch"]
namespaces = [{namespaces}]
"#
            )),
            "bootstrap clone config must parse",
        );
        fixture(
            SecurityKernel::from_config(&config.security),
            "bootstrap clone kernel must compile",
        )
    }

    fn namespace(name: &str) -> NamespaceId {
        fixture(
            NamespaceId::new(name.to_string()),
            "bootstrap clone namespace must validate",
        )
    }

    #[test]
    fn bootstrap_copy_allows_target_denied_or_equal_access() {
        assert!(kernel(&["source"])
            .validate_namespace_copy_no_widening(
                PolicyVersion::BOOT,
                &namespace("source"),
                &namespace("target"),
            )
            .is_ok());
        assert!(kernel(&["*"])
            .validate_namespace_copy_no_widening(
                PolicyVersion::BOOT,
                &namespace("source"),
                &namespace("target"),
            )
            .is_ok());
    }

    #[test]
    fn bootstrap_copy_rejects_target_only_access_and_version_mismatch() {
        let widened = kernel(&["target"]).validate_namespace_copy_no_widening(
            PolicyVersion::BOOT,
            &namespace("source"),
            &namespace("target"),
        );
        assert!(matches!(widened, Err(SecurityError::ConstraintViolation)));

        let mismatch = kernel(&["*"]).validate_namespace_copy_no_widening(
            fixture(
                PolicyVersion::persisted(1),
                "mismatched policy version must validate",
            ),
            &namespace("source"),
            &namespace("target"),
        );
        assert!(matches!(mismatch, Err(SecurityError::ConstraintViolation)));
    }
}
