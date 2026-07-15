//! Pure-CPU phase-1 authorization over validated boot grants.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::config::{SecurityConfig, SecurityMode};
use crate::error::Result as ZeppelinResult;
use crate::storage::ZeppelinStore;
use crate::time::Clock;
use chrono::{DateTime, Utc};

use super::{
    delegation::DelegationAuthority, policy_cache::PolicyCache, Action, AllowDecision,
    ApiKeyAdapter, Decision, DelegationNarrowing, DenyDecision, DenyReason, Entitlements,
    GrantActions, GrantDefinition, GrantScope, IssuedApiKey, IssuedDelegatedToken, NamespaceId,
    PolicyGrant, PolicyPrincipal, PolicySnapshot, PolicyStore, PreservationLockId,
    PreservationLockRecord, PreservationService, Principal, PrincipalId, PrincipalKind,
    RequestContext, Resource, SecurityError, SecurityOperationResult,
};

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
    preservation: Option<Arc<PreservationService>>,
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
        if !entitlements.has(super::Feature::Rbac) {
            let now = clock.now();
            if config.mode == SecurityMode::Enforced
                && !config
                    .api_keys
                    .iter()
                    .any(|key| key.expires_at.is_none_or(|expires_at| expires_at > now))
            {
                return Err(SecurityError::MissingBootstrapCredentials.into());
            }
            return Ok((
                Arc::new(Self::from_config_with_entitlements(config, entitlements)?),
                Arc::new(ApiKeyAdapter::from_config(config)?),
            ));
        }
        Self::from_store(store, config, clock, entitlements).await
    }

    /// Build authentication and authorization over one shared policy cache.
    pub(crate) async fn from_store(
        store: ZeppelinStore,
        config: &SecurityConfig,
        clock: Clock,
        entitlements: Arc<Entitlements>,
    ) -> ZeppelinResult<(Arc<Self>, Arc<ApiKeyAdapter>)> {
        if config.mode == SecurityMode::OpenUnsafe {
            return Ok((
                Arc::new(Self::from_config_with_entitlements(
                    config,
                    Arc::clone(&entitlements),
                )?),
                Arc::new(ApiKeyAdapter::from_config(config)?),
            ));
        }

        let policy_store = PolicyStore::new(store.clone(), Arc::clone(&entitlements));
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
                    store.clone(),
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
        let preservation = if entitlements.has(super::Feature::Preservation) {
            Some(
                PreservationService::start(
                    store,
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

    /// Borrow the composed preservation module for maintenance integration.
    #[must_use]
    pub fn preservation_service(&self) -> Option<&Arc<PreservationService>> {
        self.preservation.as_ref()
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
                        allow.cursor_binding_key = self.cursor_binding_key;
                        let attribute_admin = constraints.attribute_admin;
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
                allow.cursor_binding_key = self.cursor_binding_key;
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
