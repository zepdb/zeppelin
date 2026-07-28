//! Disposable compiled-policy cache with bounded S3 revalidation.
//!
//! Authorization runs on every protected request, but the authoritative policy
//! lives in object storage. This file is the seam between those two facts: it
//! holds one compiled generation of the policy in memory, revalidates it
//! against S3 on a bounded interval, and refuses to answer once that copy is
//! older than its budget. The cached copy is an accelerator with an expiry
//! date, never an authority — it can only ever be a faithful echo of a head
//! that [`PolicyStore`] already verified.
//!
//! What this file deliberately does **not** own:
//!
//! - authoritative state, key layout, or CAS discipline — that is
//!   `policy_store.rs`, reached only through [`PolicyStore`];
//! - the authorization rules themselves. [`CompiledPolicy`] (in `policy.rs`)
//!   evaluates a principal against an action and resource; this file only
//!   decides *which* compiled generation is allowed to answer;
//! - the admission decision, its obligations, and its audit record — that is
//!   `kernel.rs`;
//! - lease and fencing mechanics — that is `policy_publication.rs`;
//! - HTTP concerns. No handler touches this file directly.
//!
//! ## Where this sits
//!
//! `SecurityKernel::from_store` builds the cache once at boot with
//! [`PolicyCache::start`], handing it the [`LoadedPolicy`] that bootstrap
//! already verified. From then on there are three classes of caller:
//!
//! - **read path** — `SecurityKernel` authorizes an action through
//!   [`PolicyCache::current`] plus [`PolicyCache::is_fresh`], and
//!   [`super::ApiKeyAdapter`] authenticates a bearer digest through
//!   [`PolicyCache::current`] and reports staleness through
//!   [`PolicyCache::freshness`]. Neither performs any object-store I/O.
//! - **administration path** — `create_principal`, `create_key`, `revoke_key`,
//!   `rotate_key`, `add_grant`, and `remove_grant` authorize the mutation
//!   against the freshly reloaded authoritative snapshot, publish a new version
//!   through [`PolicyStore`], and write the result back through the cache.
//! - **branch activation path** — `begin_branch_activation` and its
//!   finalize/abort/recovery counterparts hold the global publication lease
//!   across caller-supplied validation so namespace branch activation
//!   linearizes against policy.
//!
//! ## Authority versus cache
//!
//! ```text
//!  authoritative (S3 / MinIO)              disposable (this process)
//!  --------------------------              -------------------------
//!  _security/heads/policy.json  ---------> CachedPolicy {
//!    version, checksum, ETag,                  policy:   CompiledPolicy
//!    control revision                          snapshot: PolicySnapshot
//!         | selects exactly one                head, head_version
//!         v                                    last_confirmed: Instant
//!  _security/policies/<ulid>.json  ------> }
//!                                              ^
//!            conditional GET (If-None-Match)   |
//!            every refresh_interval  ----------+
//!
//!  last_confirmed older than 2x refresh_interval
//!            |
//!            v
//!  is_fresh() == false  ->  DenyReason::SecurityStale
//!  (the kernel denies; it never serves the stale copy)
//! ```
//!
//! A refresh that fails leaves the previously verified generation in place and
//! logs an error. That is not silent degradation: the freshness budget keeps
//! running, so a cache that cannot revalidate stops authorizing on its own.
//!
//! ## Reading map
//!
//! 1. [`CachedPolicy`] — one installed generation: compiled rules, the snapshot
//!    behind them, the head and its ETag, and the monotonic instant at which
//!    that head was last confirmed authoritative.
//! 2. [`PolicyCache::start`] and `refresh_loop` — construction and the owned
//!    background revalidation task.
//! 3. [`PolicyCache::current`] / [`PolicyCache::is_fresh`] — the short read
//!    path every request takes.
//! 4. `PolicyCache::install_into` and `PolicyCache::install_refreshed_into` —
//!    the two monotonicity gates. Read these before changing anything else.
//! 5. `PolicyCache::publish_mutation` — the bounded read-authorize-build-CAS
//!    retry loop shared by all administrative mutations.
//! 6. The `*_branch_activation` family — lease-held activation guards.
//! 7. [`PolicyCache::shutdown_refresh_task`] and the [`Drop`] impl — task
//!    lifetime.
//!
//! ## Invariants
//!
//! - **Cache never outranks S3.** Every installed generation originates from a
//!   [`LoadedPolicy`] that [`PolicyStore`] verified against the authoritative
//!   head. Nothing is synthesized locally.
//! - **Staleness denies; it does not degrade.** Beyond twice the refresh
//!   interval the kernel returns `DenyReason::SecurityStale` and administration
//!   reads refuse outright.
//! - **Installation is monotonic.** A head whose control revision regressed is
//!   rejected on both install paths. A version reused with a different checksum
//!   is rejected on both paths too — that would mean the authoritative history
//!   forked. The two paths differ only on an older version: a background
//!   refresh that observes one treats it as an authority regression and errors,
//!   while a write-through install of a version already superseded locally is a
//!   harmless no-op.
//! - **A write-through install inherits the CAS completion instant**, not the
//!   moment of installation, so publishing cannot silently extend the freshness
//!   budget past the point where the head was actually confirmed.
//! - **Administrative mutations re-authorize per attempt.** Each CAS attempt
//!   reloads the authoritative snapshot and re-checks `SecurityAdminWrite`
//!   against it, so a revocation that lands mid-retry takes effect immediately.
//! - **Failures keep their decision.** [`SecurityOperationError`] carries the
//!   exact allow or deny that was evaluated, so an operation that fails after
//!   authorization still produces a truthful audit record.
//! - **Secrets are never cached.** `generate_api_key` returns the plaintext key
//!   to its one caller; only the SHA-256 digest is persisted or stored here.
//! - **A poisoned lock panics.** Recovering a possibly-torn security cache is
//!   not a supported degraded mode.
//!
//! ## Cost
//!
//! The read path is one `RwLock` read plus one `Arc` clone — no allocation of
//! policy data and no I/O. Revalidation is one conditional GET per interval
//! when nothing changed, and one additional GET plus a recompile when it did.
//! An administrative mutation costs a full [`PolicyStore`] publication
//! (lease, snapshot PUT, head CAS) and recompiles the policy once.
//!
//! ## Rust concepts used here
//!
//! The cache is `RwLock<Arc<CachedPolicy>>`, not `RwLock<CachedPolicy>`.
//! Readers take the lock only long enough to bump a reference count, then
//! evaluate against their own `Arc` after the guard is dropped. A generation
//! installed mid-request therefore cannot tear a decision in progress, and no
//! lock is ever held across an `.await`. Java's nearest analogy is publishing
//! an immutable object through a volatile field; C's is a refcounted
//! read-copy-update pointer swap — except the Rust compiler, not convention,
//! enforces that nobody mutates the shared generation in place.
//!
//! `refresh_loop` receives a [`Weak`] handle to the cache rather than an
//! [`Arc`]. A
//! spawned Tokio task outlives its spawner unless something stops it, and an
//! owning handle would make the cache immortal: it would keep its
//! [`PolicyStore`] and object-store client alive forever. With a `Weak`, the
//! upgrade fails once the last real owner drops, and the task returns on its
//! own. The [`Drop`] impl aborts the task as a backstop, and
//! [`PolicyCache::shutdown_refresh_task`] is the deterministic path used when a
//! kernel retires while the process keeps running.
//!
//! `publish_mutation` takes `F: Fn(&PolicySnapshot, DateTime<Utc>) -> Result<PolicySnapshot>`.
//! `Fn` rather than `FnOnce` is the contract that matters: after a CAS
//! conflict, the loop must rebuild the candidate against a newly reloaded base,
//! so the closure has to be callable more than once. `begin_branch_activation`
//! is the opposite case — its validator runs exactly once and is therefore
//! `FnOnce` returning a `Future`, which lets a caller `.await` object-store work
//! while this cache still holds the publication lease.

use std::future::Future;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;
use ulid::Ulid;

use super::policy::CompiledPolicy;
use super::policy_store::{PolicyPublication, PolicyRefresh};
use super::{
    Action, ApiKeyId, GrantActions, GrantDefinition, GrantScope, IssuedApiKey, LoadedPolicy,
    PendingBranchActivation, PolicyActivationGuardPermit, PolicyGrant, PolicyHead, PolicyKey,
    PolicyPrincipal, PolicySnapshot, PolicyStore, PolicyVersion, Principal, PrincipalId,
    PrincipalKind, Resource, SecurityError, SecurityOperationError, SecurityOperationResult,
};
use crate::error::Result;
use crate::namespace::branching::activation::BranchActivationTarget;
use crate::namespace::branching::ActivationNonce;
use crate::namespace::{BranchId, NamespaceIncarnationId};
use crate::storage::StorageVersion;
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
    head_version: StorageVersion,
    last_confirmed: Instant,
}

pub(crate) struct PolicyCache {
    store: PolicyStore,
    clock: Clock,
    current: RwLock<Arc<CachedPolicy>>,
    refresh_interval: Duration,
    refresh_task: Mutex<Option<JoinHandle<()>>>,
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
            head_version: loaded.head_version().clone(),
            last_confirmed: loaded.observed_at(),
        });
        let cache = Arc::new(Self {
            store,
            clock,
            current: RwLock::new(cached),
            refresh_interval,
            refresh_task: Mutex::new(None),
        });
        let weak = Arc::downgrade(&cache);
        let task = tokio::spawn(refresh_loop(weak, refresh_interval));
        *cache
            .refresh_task
            .lock()
            .unwrap_or_else(|_| panic!("security policy refresh task lock poisoned")) = Some(task);
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
            .refresh(&observed.head_version, self.clock.now())
            .await?;
        self.install_refresh(observed, refresh)
    }

    fn install_refresh(&self, observed: Arc<CachedPolicy>, refresh: PolicyRefresh) -> Result<()> {
        match refresh {
            PolicyRefresh::Unchanged { observed_at } => {
                let mut current = self
                    .current
                    .write()
                    .unwrap_or_else(|_| panic!("security policy cache lock poisoned"));
                if current.head_version == observed.head_version {
                    *current = Arc::new(CachedPolicy {
                        policy: Arc::clone(&current.policy),
                        snapshot: Arc::clone(&current.snapshot),
                        head: current.head.clone(),
                        head_version: current.head_version.clone(),
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

    /// Abort and join this cache's owned refresh task before its authority retires.
    pub(crate) async fn shutdown_refresh_task(&self) {
        let task = self
            .refresh_task
            .lock()
            .unwrap_or_else(|_| panic!("security policy refresh task lock poisoned"))
            .take();
        let Some(task) = task else {
            return;
        };
        task.abort();
        match task.await {
            Err(error) if error.is_cancelled() => {}
            Ok(()) => {
                panic!("security policy refresh task exited normally while authority remained live")
            }
            Err(error) => panic!("security policy refresh task failed during shutdown: {error}"),
        }
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

    /// Resolve the policy-owned query filter from one immutable generation.
    #[allow(dead_code)]
    pub(crate) async fn historical_query_filter(
        &self,
        version: PolicyVersion,
        checksum: &str,
        principal_id: &PrincipalId,
        namespace: &super::NamespaceId,
    ) -> Result<Option<Option<crate::types::Filter>>> {
        let Some(snapshot) = self.store.load_version(version, checksum).await? else {
            return Ok(None);
        };
        let compiled = snapshot.compile()?;
        let authorization = compiled.authorize_delegated_parent(
            principal_id,
            Action::Query,
            &Resource::Namespace(namespace.clone()),
        );
        Ok(match authorization {
            Ok(constraints) => Some(constraints.mandatory_filter),
            Err(_) => None,
        })
    }

    /// Acquire and claim the global publication lease, run fresh caller
    /// validation against that exact authoritative head/snapshot, then install
    /// a pending activation guard while retaining the same lease claim.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn begin_branch_activation<T, F, Fut>(
        &self,
        branch_id: BranchId,
        target_namespace: super::NamespaceId,
        target_incarnation: NamespaceIncarnationId,
        activation_nonce: ActivationNonce,
        expires_at: DateTime<Utc>,
        validate: F,
    ) -> SecurityOperationResult<(super::AllowDecision, T, PolicyActivationGuardPermit)>
    where
        F: FnOnce(LoadedPolicy) -> Fut,
        Fut: Future<Output = SecurityOperationResult<(super::AllowDecision, T)>>,
    {
        let session = self.store.acquire_claimed_publication().await?;
        let observation = session.loaded().clone();
        let (authorization, validated) = match validate(observation.clone()).await {
            Ok(validated) => validated,
            Err(error) => {
                self.store.release_claimed_publication(session).await;
                return Err(error);
            }
        };
        let created_at = self.clock.now();
        let guard_result = (|| {
            let fencing_token =
                observation
                    .head()
                    .publication_fencing_token()
                    .ok_or_else(|| {
                        SecurityError::InvalidPolicy(
                            "claimed policy head has no publication fencing token".to_string(),
                        )
                    })?;
            PendingBranchActivation::new(
                branch_id,
                target_namespace,
                target_incarnation,
                activation_nonce,
                observation.head().version(),
                observation.head().semantic_digest()?,
                fencing_token,
                created_at,
                expires_at,
            )
        })();
        let guard = match guard_result {
            Ok(guard) => guard,
            Err(error) => {
                self.store.release_claimed_publication(session).await;
                return Err(SecurityOperationError::after_allow(
                    error.into(),
                    authorization,
                ));
            }
        };
        let (loaded, permit) = self
            .store
            .insert_pending_branch_activation(session, guard)
            .await
            .map_err(|error| SecurityOperationError::after_allow(error, authorization.clone()))?;
        if let Err(error) = self.install(loaded) {
            self.store.release_branch_activation_permit(permit).await;
            return Err(SecurityOperationError::after_allow(error, authorization));
        }
        Ok((authorization, validated, permit))
    }

    /// Renew and strongly re-read the exact guard immediately before target
    /// activation. Expired guards fail closed and require nonce recovery.
    pub(crate) async fn renew_branch_activation_guard(
        &self,
        permit: &mut PolicyActivationGuardPermit,
    ) -> Result<()> {
        if permit.guard().expires_at() <= self.clock.now() {
            return Err(SecurityError::PolicyConflict.into());
        }
        let loaded = self.store.renew_branch_activation_guard(permit).await?;
        self.install(loaded)
    }

    /// Take over and retain one exact persisted guard for target nonce
    /// inspection during crash recovery or prepared cancellation.
    pub(crate) async fn retain_pending_branch_activation(
        &self,
        target: &BranchActivationTarget,
        expected_nonce: Option<ActivationNonce>,
    ) -> Result<Option<PolicyActivationGuardPermit>> {
        let session = self.store.acquire_claimed_publication().await?;
        let retained = self
            .store
            .retain_pending_branch_activation(session, target, expected_nonce)
            .await?;
        let Some((loaded, permit)) = retained else {
            return Ok(None);
        };
        if let Err(error) = self.install(loaded) {
            self.store.release_branch_activation_permit(permit).await;
            return Err(error);
        }
        Ok(Some(permit))
    }

    /// Retain the deterministic next expired guard under one newly claimed
    /// publication lease. This path is mechanical and performs no authz.
    pub(crate) async fn retain_next_expired_branch_activation(
        &self,
    ) -> Result<Option<PolicyActivationGuardPermit>> {
        let session = self.store.acquire_claimed_publication().await?;
        let retained = self
            .store
            .retain_next_expired_branch_activation(session, self.clock.now())
            .await?;
        let Some((loaded, permit)) = retained else {
            return Ok(None);
        };
        if let Err(error) = self.install(loaded) {
            self.store.release_branch_activation_permit(permit).await;
            return Err(error);
        }
        Ok(Some(permit))
    }

    /// Finalize a committed target activation and remove only its exact guard.
    pub(crate) async fn finalize_branch_activation(
        &self,
        permit: PolicyActivationGuardPermit,
    ) -> Result<()> {
        let loaded = self.store.finalize_branch_activation(permit).await?;
        self.install(loaded)
    }

    /// Abort after the exact target nonce was durably revoked, then remove only
    /// the matching policy guard.
    pub(crate) async fn abort_branch_activation(
        &self,
        permit: PolicyActivationGuardPermit,
    ) -> Result<()> {
        let loaded = self.store.abort_branch_activation(permit).await?;
        self.install(loaded)
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
                .publish(candidate, base.head_version())
                .await
                .map_err(|error| {
                    SecurityOperationError::after_allow(error, authorization.clone())
                })? {
                PolicyPublication::Conflict => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                PolicyPublication::Guarded => {
                    return Err(SecurityOperationError::after_allow(
                        SecurityError::PolicyConflict.into(),
                        authorization,
                    ));
                }
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
        if loaded.head().control_revision() < current.head.control_revision() {
            return Err(SecurityError::InvalidPolicy(
                "authoritative policy control revision regressed".to_string(),
            )
            .into());
        }
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
                    head_version: loaded.head_version().clone(),
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
        if loaded.head().control_revision() < current.head.control_revision() {
            return Err(SecurityError::InvalidPolicy(
                "authoritative policy control revision regressed".to_string(),
            )
            .into());
        }
        match next_policy.version().cmp(&current.policy.version()) {
            std::cmp::Ordering::Less => return Ok(()),
            std::cmp::Ordering::Equal => {
                if loaded.head().checksum() != current.head.checksum() {
                    return Err(SecurityError::InvalidPolicy(
                        "policy version was reused with different content".to_string(),
                    )
                    .into());
                }
            }
            std::cmp::Ordering::Greater => {}
        }
        *current = Arc::new(CachedPolicy {
            policy: next_policy,
            snapshot: Arc::new(loaded.snapshot().clone()),
            head: loaded.head().clone(),
            head_version: loaded.head_version().clone(),
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
        let task = self
            .refresh_task
            .get_mut()
            .unwrap_or_else(|_| panic!("security policy refresh task lock poisoned"))
            .take();
        if let Some(task) = task {
            task.abort();
        }
    }
}

async fn refresh_loop(cache: Weak<PolicyCache>, refresh_interval: Duration) {
    loop {
        tokio::time::sleep(refresh_interval).await;
        let (store, clock, observed) = {
            let Some(cache) = cache.upgrade() else {
                return;
            };
            (cache.store.clone(), cache.clock.clone(), cache.current())
        };
        let refresh = store.refresh(&observed.head_version, clock.now()).await;
        let Some(cache) = cache.upgrade() else {
            return;
        };
        let result = match refresh {
            Ok(refresh) => cache.install_refresh(observed, refresh),
            Err(error) => Err(error),
        };
        if let Err(error) = result {
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

    fn test_version(etag: &str) -> StorageVersion {
        StorageVersion::from_parts(Some(etag.to_string()), None)
            .expect("a non-empty etag always yields a token")
    }

    fn initial_test_version() -> StorageVersion {
        test_version("initial-etag")
    }

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
    fn same_version_control_head_update_replaces_cached_etag_without_semantic_change() {
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
            head_version: initial_test_version(),
            last_confirmed: Instant::now(),
        }));
        let claimed = initial_head
            .claim_publication(
                crate::security::PolicyLeaseFencingToken::new(1)
                    .expect("fencing token must validate"),
            )
            .expect("control claim must validate");

        PolicyCache::install_into(
            &current,
            LoadedPolicy::from_publication(
                claimed,
                initial,
                test_version("claimed-etag"),
                Instant::now(),
            ),
        )
        .expect("same-version control update must install");

        let installed = current
            .read()
            .unwrap_or_else(|_| panic!("test policy cache lock poisoned"));
        assert_eq!(installed.policy.version().get(), 1);
        assert_eq!(installed.head.control_revision().get(), 1);
        assert_eq!(installed.head_version, test_version("claimed-etag"));
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
            head_version: initial_test_version(),
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
            test_version("published-etag"),
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
            head_version: initial_test_version(),
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
                test_version("version-2-etag"),
                Instant::now(),
            ),
        )
        .expect("version 2 must install");

        let error = PolicyCache::install_refreshed_into(
            &current,
            LoadedPolicy::from_publication(
                initial_head,
                initial,
                test_version("regressing-etag"),
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
