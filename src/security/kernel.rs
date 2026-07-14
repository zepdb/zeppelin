//! Pure-CPU phase-1 authorization over validated boot grants.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use crate::config::{SecurityConfig, SecurityMode};

use super::{
    Action, AllowDecision, Decision, DenyDecision, DenyReason, NamespaceId, Principal, PrincipalId,
    RequestContext, Resource, SecurityError,
};

#[derive(Debug)]
struct BootstrapGrant {
    actions: HashSet<Action>,
    all_namespaces: bool,
    namespaces: HashSet<NamespaceId>,
}

/// Central authorization seam used by every protected route.
#[derive(Debug)]
pub struct SecurityKernel {
    mode: SecurityMode,
    grants: HashMap<PrincipalId, BootstrapGrant>,
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
            grants,
        })
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
        if principal
            .expires_at
            .is_some_and(|expires_at| expires_at <= context.now)
        {
            return Decision::Deny(DenyDecision::boot(DenyReason::CredentialExpired));
        }

        let Some(grant) = self.grants.get(&principal.id) else {
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
}
