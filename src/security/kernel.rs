//! Pure-CPU phase-1 authorization over validated boot grants.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::config::{SecurityConfig, SecurityMode};
use crate::error::Result as ZeppelinResult;
use crate::storage::ZeppelinStore;
use crate::time::Clock;
use chrono::{DateTime, Utc};

use super::{
    policy_cache::PolicyCache, Action, AllowDecision, ApiKeyAdapter, Decision, DenyDecision,
    DenyReason, GrantActions, GrantScope, IssuedApiKey, NamespaceId, PolicyGrant, PolicyPrincipal,
    PolicySnapshot, PolicyStore, Principal, PrincipalId, PrincipalKind, RequestContext, Resource,
    SecurityError, SecurityOperationResult,
};

#[derive(Debug)]
struct BootstrapGrant {
    actions: HashSet<Action>,
    all_namespaces: bool,
    namespaces: HashSet<NamespaceId>,
}

enum SecurityAuthority {
    Bootstrap(HashMap<PrincipalId, BootstrapGrant>),
    Policy(Arc<PolicyCache>),
}

/// Central authorization seam used by every protected route.
pub struct SecurityKernel {
    mode: SecurityMode,
    authority: SecurityAuthority,
}

impl SecurityKernel {
    /// Compile validated boot configuration into typed, immutable grants.
    pub fn from_config(config: &SecurityConfig) -> Result<Self, SecurityError> {
        let mut grants = HashMap::new();
        for key in &config.api_keys {
            let principal_id = PrincipalId::new(key.key_id.clone())?;
            let mut actions = HashSet::new();
            if key.actions.iter().any(|action| action == "*") {
                actions.extend(Action::ALL);
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
        })
    }

    /// Build authentication and authorization over one shared policy cache.
    pub async fn from_store(
        store: ZeppelinStore,
        config: &SecurityConfig,
        clock: Clock,
    ) -> ZeppelinResult<(Arc<Self>, Arc<ApiKeyAdapter>)> {
        if config.mode == SecurityMode::OpenUnsafe {
            return Ok((
                Arc::new(Self::from_config(config)?),
                Arc::new(ApiKeyAdapter::from_config(config)?),
            ));
        }

        let policy_store = PolicyStore::new(store);
        let loaded = policy_store.load_or_bootstrap(config, clock.now()).await?;
        let cache = PolicyCache::start(
            policy_store,
            loaded,
            Duration::from_secs(config.policy_refresh_secs),
            clock,
        )?;
        Ok((
            Arc::new(Self {
                mode: config.mode,
                authority: SecurityAuthority::Policy(Arc::clone(&cache)),
            }),
            Arc::new(ApiKeyAdapter::from_policy_cache(cache)),
        ))
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
            return Decision::Allow(AllowDecision::boot(action));
        }
        if principal.is_anonymous() {
            return Decision::Deny(DenyDecision::boot(DenyReason::Unauthenticated));
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

                Decision::Allow(AllowDecision::boot(action))
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
                if let Err(reason) =
                    cached
                        .policy
                        .authorize(principal, context.now, action, resource)
                {
                    return Decision::Deny(DenyDecision::for_policy(reason, version));
                }

                Decision::Allow(AllowDecision::for_policy(action, version))
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
            return Decision::Allow(AllowDecision::boot(action));
        }
        if principal.is_anonymous() {
            return Decision::Deny(DenyDecision::boot(DenyReason::Unauthenticated));
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
                Decision::Allow(AllowDecision::boot(action))
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
                Decision::Allow(AllowDecision::for_policy(action, version))
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
        principal_id: PrincipalId,
        scope: GrantScope,
        actions: GrantActions,
    ) -> SecurityOperationResult<(AllowDecision, super::PolicyVersion, PolicyGrant)> {
        match &self.authority {
            SecurityAuthority::Policy(cache) => {
                cache.add_grant(actor, principal_id, scope, actions).await
            }
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
}
