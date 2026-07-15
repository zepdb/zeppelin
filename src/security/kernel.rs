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
    DenyReason, GrantActions, GrantDefinition, GrantScope, IssuedApiKey, NamespaceId, PolicyGrant,
    PolicyPrincipal, PolicySnapshot, PolicyStore, Principal, PrincipalId, PrincipalKind,
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
}

impl SecurityKernel {
    /// Compile validated boot configuration into typed, immutable grants.
    pub fn from_config(config: &SecurityConfig) -> Result<Self, SecurityError> {
        let cursor_binding_key = if config.mode == SecurityMode::OpenUnsafe {
            super::CursorBindingKey::default()
        } else {
            super::CursorBindingKey::from_config_hex(config.cursor_hmac_key_hex())?
        };
        let mut grants = HashMap::new();
        for key in &config.api_keys {
            let principal_id = PrincipalId::new(key.key_id.clone())?;
            let mut actions = HashSet::new();
            if key.actions.iter().any(|action| action == "*") {
                actions.extend(Action::POLICY_ALL_V1);
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
        let cursor_binding_key =
            super::CursorBindingKey::from_config_hex(config.cursor_hmac_key_hex())?;
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
                cursor_binding_key,
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
            return Decision::Allow(Box::new(AllowDecision::boot(action)));
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
            SecurityAuthority::Policy(cache) => cache.refresh_once().await,
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
