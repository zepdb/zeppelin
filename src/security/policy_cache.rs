//! Disposable compiled-policy cache with bounded S3 revalidation.

use std::sync::{Arc, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};
use tokio::task::AbortHandle;
use ulid::Ulid;

use super::policy::CompiledPolicy;
use super::policy_store::{PolicyPublication, PolicyRefresh};
use super::{
    Action, ApiKeyId, GrantActions, GrantDefinition, GrantScope, IssuedApiKey, LoadedPolicy,
    PolicyGrant, PolicyHead, PolicyKey, PolicyPrincipal, PolicySnapshot, PolicyStore,
    PolicyVersion, Principal, PrincipalId, PrincipalKind, Resource, SecurityError,
    SecurityOperationError, SecurityOperationResult,
};
use crate::error::Result;
use crate::time::Clock;

const POLICY_CAS_ATTEMPTS: usize = 5;

#[derive(Debug, Clone)]
struct PolicyTransition {
    authorization: super::AllowDecision,
    current: PolicyVersion,
}

#[derive(Debug)]
pub(crate) struct CachedPolicy {
    pub(crate) policy: Arc<CompiledPolicy>,
    pub(crate) snapshot: Arc<PolicySnapshot>,
    head: PolicyHead,
    head_etag: String,
    last_confirmed: Instant,
}

pub(crate) struct PolicyCache {
    store: PolicyStore,
    clock: Clock,
    current: RwLock<Arc<CachedPolicy>>,
    refresh_interval: Duration,
    refresh_abort: OnceLock<AbortHandle>,
}

impl PolicyCache {
    pub(crate) fn start(
        store: PolicyStore,
        loaded: LoadedPolicy,
        refresh_interval: Duration,
        clock: Clock,
    ) -> Result<Arc<Self>> {
        let cached = Arc::new(CachedPolicy {
            policy: Arc::new(loaded.snapshot().compile()?),
            snapshot: Arc::new(loaded.snapshot().clone()),
            head: loaded.head().clone(),
            head_etag: loaded.head_etag().to_string(),
            last_confirmed: loaded.observed_at(),
        });
        let cache = Arc::new(Self {
            store,
            clock,
            current: RwLock::new(cached),
            refresh_interval,
            refresh_abort: OnceLock::new(),
        });
        let weak = Arc::downgrade(&cache);
        let task = tokio::spawn(refresh_loop(weak, refresh_interval));
        cache.refresh_abort.set(task.abort_handle()).map_err(|_| {
            SecurityError::InvalidPolicy(
                "policy refresh runtime initialized more than once".to_string(),
            )
        })?;
        Ok(cache)
    }

    #[must_use]
    pub(crate) fn current(&self) -> Arc<CachedPolicy> {
        self.current
            .read()
            .unwrap_or_else(|_| panic!("security policy cache lock poisoned"))
            .clone()
    }

    #[must_use]
    pub(crate) fn is_fresh(&self, cached: &CachedPolicy) -> bool {
        cached.last_confirmed.elapsed() <= self.refresh_interval.saturating_mul(2)
    }

    pub(crate) async fn refresh_once(&self) -> Result<()> {
        let observed = self.current();
        let refresh = self
            .store
            .refresh(&observed.head_etag, self.clock.now())
            .await?;
        match refresh {
            PolicyRefresh::Unchanged { observed_at } => {
                let mut current = self
                    .current
                    .write()
                    .unwrap_or_else(|_| panic!("security policy cache lock poisoned"));
                if current.head_etag == observed.head_etag {
                    *current = Arc::new(CachedPolicy {
                        policy: Arc::clone(&current.policy),
                        snapshot: Arc::clone(&current.snapshot),
                        head: current.head.clone(),
                        head_etag: current.head_etag.clone(),
                        last_confirmed: observed_at,
                    });
                }
            }
            PolicyRefresh::Changed { loaded } => {
                Self::install_refreshed_into(&self.current, *loaded)?;
            }
        }
        Ok(())
    }

    #[must_use]
    pub(crate) fn version(&self) -> PolicyVersion {
        self.current().policy.version()
    }

    #[must_use]
    pub(crate) fn freshness(&self) -> (PolicyVersion, bool) {
        let current = self.current();
        (current.policy.version(), self.is_fresh(&current))
    }

    pub(crate) async fn create_principal(
        &self,
        actor: &Principal,
        principal_id: PrincipalId,
        kind: PrincipalKind,
        display_name: String,
    ) -> SecurityOperationResult<(super::AllowDecision, PolicyVersion, PolicyPrincipal)> {
        let principal = PolicyPrincipal::new(principal_id, kind, display_name)?;
        let transition = self
            .publish_mutation(actor, |snapshot, now| {
                Ok(snapshot.add_principal(&actor.id, now, principal.clone())?)
            })
            .await?;
        Ok((transition.authorization, transition.current, principal))
    }

    pub(crate) async fn create_key(
        &self,
        actor: &Principal,
        principal_id: PrincipalId,
        name: String,
        expires_at: Option<DateTime<Utc>>,
    ) -> SecurityOperationResult<IssuedApiKey> {
        let (key_id, api_key, digest) = generate_api_key()?;
        let transition = self
            .publish_mutation(actor, |snapshot, now| {
                let key = PolicyKey::new_active(
                    key_id.clone(),
                    name.clone(),
                    digest.clone(),
                    principal_id.clone(),
                    expires_at,
                    now,
                    None,
                )?;
                Ok(snapshot.add_key(&actor.id, now, key.clone())?)
            })
            .await?;
        Ok(IssuedApiKey::new(
            key_id,
            api_key,
            transition.authorization,
            transition.current,
        ))
    }

    pub(crate) async fn revoke_key(
        &self,
        actor: &Principal,
        key_id: &ApiKeyId,
    ) -> SecurityOperationResult<(super::AllowDecision, PolicyVersion)> {
        let transition = self
            .publish_mutation(actor, |snapshot, now| {
                Ok(snapshot.revoke_key(&actor.id, now, key_id)?)
            })
            .await?;
        Ok((transition.authorization, transition.current))
    }

    pub(crate) async fn rotate_key(
        &self,
        actor: &Principal,
        old_key_id: &ApiKeyId,
        overlap_secs: u64,
    ) -> SecurityOperationResult<IssuedApiKey> {
        let (key_id, api_key, digest) = generate_api_key()?;
        let transition = self
            .publish_mutation(actor, |snapshot, now| {
                Ok(snapshot.rotate_key(
                    &actor.id,
                    now,
                    old_key_id,
                    key_id.clone(),
                    digest.clone(),
                    overlap_secs,
                )?)
            })
            .await?;
        Ok(IssuedApiKey::new(
            key_id,
            api_key,
            transition.authorization,
            transition.current,
        ))
    }

    pub(crate) async fn add_grant(
        &self,
        actor: &Principal,
        definition: GrantDefinition,
    ) -> SecurityOperationResult<(super::AllowDecision, PolicyVersion, PolicyGrant)> {
        let grant = definition.into_policy_grant()?;
        let transition = self
            .publish_mutation(actor, |snapshot, now| {
                Ok(snapshot.add_grant(&actor.id, now, grant.clone())?)
            })
            .await?;
        Ok((transition.authorization, transition.current, grant))
    }

    pub(crate) async fn remove_grant(
        &self,
        actor: &Principal,
        principal_id: PrincipalId,
        scope: GrantScope,
        actions: GrantActions,
    ) -> SecurityOperationResult<(super::AllowDecision, PolicyVersion, PolicyGrant)> {
        let grant = PolicyGrant::new(principal_id, scope, actions)?;
        let transition = self
            .publish_mutation(actor, |snapshot, now| {
                Ok(snapshot.remove_grant(&actor.id, now, &grant)?)
            })
            .await?;
        Ok((transition.authorization, transition.current, grant))
    }

    pub(crate) fn authorized_snapshot(
        &self,
        actor: &Principal,
        now: DateTime<Utc>,
    ) -> SecurityOperationResult<(super::AllowDecision, Arc<PolicySnapshot>)> {
        let current = self.current();
        let version = current.snapshot.version();
        if !self.is_fresh(&current) {
            return Err(SecurityOperationError::denied(
                super::DenyDecision::for_policy(super::DenyReason::SecurityStale, version),
            ));
        }
        if let Err(reason) = current.policy.authorize(
            actor,
            now,
            Action::SecurityAdminRead,
            &Resource::SecurityPolicy,
        ) {
            return Err(SecurityOperationError::denied(
                super::DenyDecision::for_policy(reason, version),
            ));
        }
        Ok((
            super::AllowDecision::for_policy(Action::SecurityAdminRead, version),
            Arc::clone(&current.snapshot),
        ))
    }

    pub(crate) fn authorized_policy_view(
        &self,
        actor: &Principal,
        now: DateTime<Utc>,
    ) -> SecurityOperationResult<(super::AllowDecision, PolicyHead, Arc<PolicySnapshot>)> {
        let current = self.current();
        let version = current.snapshot.version();
        if !self.is_fresh(&current) {
            return Err(SecurityOperationError::denied(
                super::DenyDecision::for_policy(super::DenyReason::SecurityStale, version),
            ));
        }
        if let Err(reason) = current.policy.authorize(
            actor,
            now,
            Action::SecurityAdminRead,
            &Resource::SecurityPolicy,
        ) {
            return Err(SecurityOperationError::denied(
                super::DenyDecision::for_policy(reason, version),
            ));
        }
        Ok((
            super::AllowDecision::for_policy(Action::SecurityAdminRead, version),
            current.head.clone(),
            Arc::clone(&current.snapshot),
        ))
    }

    async fn publish_mutation<F>(
        &self,
        actor: &Principal,
        build: F,
    ) -> SecurityOperationResult<PolicyTransition>
    where
        F: Fn(&PolicySnapshot, DateTime<Utc>) -> Result<PolicySnapshot>,
    {
        let mut latest_authorization = None;
        for _attempt in 0..POLICY_CAS_ATTEMPTS {
            let now = self.clock.now();
            let base = self.store.load_current(now).await.map_err(|error| {
                SecurityOperationError::from(error)
                    .with_fallback_allow(latest_authorization.clone())
            })?;
            let authorization = authorize_policy_write(base.snapshot(), actor, now)
                .map_err(|error| error.with_fallback_allow(latest_authorization.clone()))?;
            latest_authorization = Some(authorization.clone());
            let candidate = build(base.snapshot(), now).map_err(|error| {
                SecurityOperationError::after_allow(error, authorization.clone())
            })?;
            match self
                .store
                .publish(candidate, base.head_etag())
                .await
                .map_err(|error| {
                    SecurityOperationError::after_allow(error, authorization.clone())
                })? {
                PolicyPublication::Conflict => continue,
                PolicyPublication::Published(loaded) => {
                    let current = loaded.snapshot().version();
                    self.install(*loaded).map_err(|error| {
                        SecurityOperationError::after_allow(error, authorization.clone())
                    })?;
                    return Ok(PolicyTransition {
                        authorization,
                        current,
                    });
                }
            }
        }
        let conflict = crate::error::ZeppelinError::from(SecurityError::PolicyConflict);
        match latest_authorization {
            Some(authorization) => {
                Err(SecurityOperationError::after_allow(conflict, authorization))
            }
            None => Err(SecurityOperationError::from(conflict)),
        }
    }

    fn install(&self, loaded: LoadedPolicy) -> Result<()> {
        Self::install_into(&self.current, loaded)
    }

    fn install_refreshed_into(
        current_slot: &RwLock<Arc<CachedPolicy>>,
        loaded: LoadedPolicy,
    ) -> Result<()> {
        let next_version = loaded.snapshot().version();
        let next_checksum = loaded.snapshot().checksum().to_string();
        let next_policy = Arc::new(loaded.snapshot().compile()?);
        let mut current = current_slot
            .write()
            .unwrap_or_else(|_| panic!("security policy cache lock poisoned"));
        match next_version.cmp(&current.policy.version()) {
            std::cmp::Ordering::Less => {
                return Err(SecurityError::InvalidPolicy(format!(
                    "authoritative policy head regressed from version {} to {}",
                    current.policy.version().get(),
                    next_version.get()
                ))
                .into());
            }
            std::cmp::Ordering::Equal if next_checksum != current.head.checksum() => {
                return Err(SecurityError::InvalidPolicy(
                    "policy version was reused with different content".to_string(),
                )
                .into());
            }
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {
                *current = Arc::new(CachedPolicy {
                    policy: next_policy,
                    snapshot: Arc::new(loaded.snapshot().clone()),
                    head: loaded.head().clone(),
                    head_etag: loaded.head_etag().to_string(),
                    last_confirmed: loaded.observed_at(),
                });
            }
        }
        Ok(())
    }

    fn install_into(current_slot: &RwLock<Arc<CachedPolicy>>, loaded: LoadedPolicy) -> Result<()> {
        let next_policy = Arc::new(loaded.snapshot().compile()?);
        let mut current = current_slot
            .write()
            .unwrap_or_else(|_| panic!("security policy cache lock poisoned"));
        match next_policy.version().cmp(&current.policy.version()) {
            std::cmp::Ordering::Less => return Ok(()),
            std::cmp::Ordering::Equal => {
                if loaded.head().checksum() != current.head.checksum() {
                    return Err(SecurityError::InvalidPolicy(
                        "policy version was reused with different content".to_string(),
                    )
                    .into());
                }
                return Ok(());
            }
            std::cmp::Ordering::Greater => {}
        }
        *current = Arc::new(CachedPolicy {
            policy: next_policy,
            snapshot: Arc::new(loaded.snapshot().clone()),
            head: loaded.head().clone(),
            head_etag: loaded.head_etag().to_string(),
            last_confirmed: loaded.observed_at(),
        });
        Ok(())
    }
}

fn authorize_policy_write(
    snapshot: &PolicySnapshot,
    actor: &Principal,
    now: DateTime<Utc>,
) -> SecurityOperationResult<super::AllowDecision> {
    let version = snapshot.version();
    let policy = snapshot.compile()?;
    if let Err(reason) = policy.authorize(
        actor,
        now,
        Action::SecurityAdminWrite,
        &Resource::SecurityPolicy,
    ) {
        return Err(SecurityOperationError::denied(
            super::DenyDecision::for_policy(reason, version),
        ));
    }
    Ok(super::AllowDecision::for_policy(
        Action::SecurityAdminWrite,
        version,
    ))
}

fn generate_api_key() -> Result<(ApiKeyId, String, String)> {
    let key_id = ApiKeyId::new(format!(
        "zpk1_{}",
        Ulid::new().to_string().to_ascii_lowercase()
    ))?;
    let mut secret_bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut secret_bytes);
    let secret = URL_SAFE_NO_PAD.encode(secret_bytes);
    let api_key = format!("{}.{}", key_id.as_str(), secret);
    let digest = Sha256::digest(secret.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok((key_id, api_key, digest))
}

impl Drop for PolicyCache {
    fn drop(&mut self) {
        if let Some(abort) = self.refresh_abort.get() {
            abort.abort();
        }
    }
}

async fn refresh_loop(cache: Weak<PolicyCache>, refresh_interval: Duration) {
    loop {
        tokio::time::sleep(refresh_interval).await;
        let Some(cache) = cache.upgrade() else {
            return;
        };
        if let Err(error) = cache.refresh_once().await {
            tracing::error!(
                error = %error,
                policy_version = cache.version().get(),
                "security policy refresh failed"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::config::{ApiKeyConfig, SecurityConfig};

    use super::*;

    fn bootstrap_config() -> SecurityConfig {
        let mut config = SecurityConfig::default();
        config.set_cursor_hmac_key_hex("11".repeat(32));
        config.api_keys = vec![ApiKeyConfig {
            key_id: "zpk1_install_origin".to_string(),
            name: "install origin administrator".to_string(),
            sha256_hex: "00".repeat(32),
            actions: vec!["*".to_string()],
            namespaces: vec!["*".to_string()],
            expires_at: None,
        }];
        config
    }

    #[test]
    fn delayed_write_through_install_uses_cas_completion_origin() {
        let now = Utc::now();
        let initial = PolicySnapshot::from_bootstrap(&bootstrap_config(), now, false)
            .expect("unit policy must compile");
        let initial_head =
            PolicyHead::new(&initial, format!("_security/policies/{}.json", Ulid::new()))
                .expect("initial policy head must be valid");
        let current = RwLock::new(Arc::new(CachedPolicy {
            policy: Arc::new(initial.compile().expect("initial policy must compile")),
            snapshot: Arc::new(initial.clone()),
            head: initial_head,
            head_etag: "initial-etag".to_string(),
            last_confirmed: Instant::now(),
        }));
        let candidate = initial
            .add_principal(
                &PrincipalId::new("zpk1_install_origin").expect("actor id must be valid"),
                now,
                PolicyPrincipal::new(
                    PrincipalId::new("service:post-cas-delay")
                        .expect("new principal id must be valid"),
                    PrincipalKind::Service,
                    "post CAS delay".to_string(),
                )
                .expect("new principal must be valid"),
            )
            .expect("version 2 candidate must be valid");
        let next_head = PolicyHead::new(
            &candidate,
            format!("_security/policies/{}.json", Ulid::new()),
        )
        .expect("published policy head must be valid");
        let cas_completed_at = Instant::now() - Duration::from_millis(125);
        let loaded = LoadedPolicy::from_publication(
            next_head,
            candidate,
            "published-etag".to_string(),
            cas_completed_at,
        );

        PolicyCache::install_into(&current, loaded)
            .expect("delayed write-through policy must install atomically");

        let installed = current
            .read()
            .unwrap_or_else(|_| panic!("test policy cache lock poisoned"));
        assert_eq!(installed.policy.version().get(), 2);
        assert_eq!(installed.last_confirmed, cas_completed_at);
        assert!(
            installed.last_confirmed.elapsed() > Duration::from_millis(100),
            "post-CAS installation must not restart the freshness budget"
        );
    }

    #[test]
    fn regressing_authoritative_refresh_is_rejected() {
        let now = Utc::now();
        let initial = PolicySnapshot::from_bootstrap(&bootstrap_config(), now, false)
            .expect("unit policy must compile");
        let initial_head =
            PolicyHead::new(&initial, format!("_security/policies/{}.json", Ulid::new()))
                .expect("initial policy head must be valid");
        let current = RwLock::new(Arc::new(CachedPolicy {
            policy: Arc::new(initial.compile().expect("initial policy must compile")),
            snapshot: Arc::new(initial.clone()),
            head: initial_head.clone(),
            head_etag: "initial-etag".to_string(),
            last_confirmed: Instant::now(),
        }));
        let candidate = initial
            .add_principal(
                &PrincipalId::new("zpk1_install_origin").expect("actor id must be valid"),
                now,
                PolicyPrincipal::new(
                    PrincipalId::new("service:refresh-regression")
                        .expect("new principal id must be valid"),
                    PrincipalKind::Service,
                    "refresh regression".to_string(),
                )
                .expect("new principal must be valid"),
            )
            .expect("version 2 candidate must be valid");
        let next_head = PolicyHead::new(
            &candidate,
            format!("_security/policies/{}.json", Ulid::new()),
        )
        .expect("version 2 policy head must be valid");
        PolicyCache::install_into(
            &current,
            LoadedPolicy::from_publication(
                next_head,
                candidate,
                "version-2-etag".to_string(),
                Instant::now(),
            ),
        )
        .expect("version 2 must install");

        let error = PolicyCache::install_refreshed_into(
            &current,
            LoadedPolicy::from_publication(
                initial_head,
                initial,
                "regressing-etag".to_string(),
                Instant::now(),
            ),
        )
        .expect_err("a regressing authoritative head must fail the refresh");

        assert_eq!(
            error.to_string(),
            "security error: invalid security policy: authoritative policy head regressed from version 2 to 1"
        );
        assert_eq!(
            current
                .read()
                .unwrap_or_else(|_| panic!("test policy cache lock poisoned"))
                .policy
                .version()
                .get(),
            2,
            "failed refresh must not replace the last verified cache"
        );
    }
}
