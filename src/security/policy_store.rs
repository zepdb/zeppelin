//! S3-authoritative loading, bootstrap, and CAS publication of security policy.
//!
//! Zeppelin's authorization rules live in object storage, not in process memory
//! and not in the boot configuration file. This file is the only place that
//! reads or writes the reserved `_security/` policy keyspace, and the only
//! place that turns raw objects into a [`LoadedPolicy`]: one verified head
//! observation, the exact immutable snapshot that head selects, and the head
//! ETag that authorizes the next publication.
//!
//! What this file deliberately does **not** own:
//!
//! - the persisted policy vocabulary, its strict validation, its canonical
//!   checksum, and its compiled evaluation form — that is `policy.rs`, which
//!   defines [`PolicyHead`] and [`PolicySnapshot`];
//! - lease mechanics, fencing-token arithmetic, and guard records — that is
//!   `policy_publication.rs`, which defines [`PolicyPublicationLease`],
//!   [`PolicyPublicationLeaseClaim`], and [`PendingBranchActivation`];
//! - request-path caching, refresh scheduling, and the staleness rule — that is
//!   `policy_cache.rs`;
//! - the authorization decision itself and its audit record — that is
//!   `kernel.rs`;
//! - `object_store` access. Every request goes through [`ZeppelinStore`], the
//!   single object-store boundary.
//!
//! ## Where this sits
//!
//! `SecurityKernel::from_store` constructs one [`PolicyStore`] during boot and
//! calls [`PolicyStore::load_or_bootstrap`] before the server accepts traffic.
//! The resulting [`LoadedPolicy`] seeds the process-local policy cache, which
//! afterwards owns every subsequent call into this file: periodic
//! revalidation, administrative mutations, and the branch-activation guard
//! lifecycle. No HTTP handler reaches this file directly.
//!
//! ## Persisted artifacts
//!
//! Everything below is authoritative object-store state. Nothing here is a
//! cache, and nothing local may override it.
//!
//! | Key | Mutability | Written by |
//! | --- | --- | --- |
//! | `_security/heads/policy.json` | mutable; ETag CAS only | bootstrap (create-only), then every publication |
//! | `_security/policies/<ulid>.json` | immutable; create-only | every publication, including bootstrap |
//! | `_security/migrations/phase7-safe-all-v2/<old>-<new>.json` | immutable; create-only | the legacy-wildcard migration |
//! | `_security/leases/policy-publication.json` | mutable; create-only then CAS | `policy_publication.rs`, on behalf of this file |
//!
//! The head is small and names one snapshot by object key, version, and
//! checksum. Snapshots are the full policy documents and are never modified
//! after creation. Both are plain JSON (`serde_json`), so an operator can read
//! authoritative security state without a decoder.
//!
//! ## Publication path
//!
//! Writing a snapshot object does not change any decision. Only a successful
//! conditional PUT of the head does. Two independent defenses guard that PUT:
//! the single global publication lease, and the ETag of the exact head this
//! caller observed.
//!
//! ```text
//! acquire global publication lease         (create-only, or CAS takeover)
//!              |
//!              v
//! GET  _security/heads/policy.json         (ETag becomes the CAS token)
//! GET  _security/policies/<ulid>.json      (snapshot the head selects)
//!              |
//!              | verify head/snapshot version + checksum agree
//!              v
//! CAS the lease fencing token INTO the head
//!              |
//!              v
//! PUT  _security/policies/<new ulid>.json  (create-only; object exists,
//!              |                            but authorizes nothing yet)
//!              v
//! renew lease, then CAS the head on the observed ETag
//!              |
//!       +------+---------------------------+
//!       | Updated                          | Conflict
//!       v                                  v
//! PolicyPublication::Published       PolicyPublication::Conflict
//! (new rules are now authoritative)  (reload and retry; never overwrite;
//!       |                             the new snapshot stays an orphan)
//!       v
//! release lease (best effort, warn on failure)
//! ```
//!
//! A pending branch-activation guard in the head blocks semantic publication
//! entirely: [`PolicyStore::publish`] returns `PolicyPublication::Guarded`
//! rather than racing a half-finished branch activation.
//!
//! ## Reading map
//!
//! 1. [`LoadedPolicy`] — the unit of authority every other entry point returns.
//! 2. [`PolicyStore::load_or_bootstrap`] — the boot entry point, including the
//!    first-boot race and the ignored-bootstrap-config drift warning.
//! 3. `PolicyStore::load_head_bytes` — the verification pipeline that every
//!    head observation must pass before it can authorize anything.
//! 4. [`PolicyStore::publish`] — lease claim, immutable snapshot write, and
//!    head CAS.
//! 5. `PolicyStore::bootstrap` and its helpers — how version 1 is created
//!    exactly once across concurrent cold starts.
//! 6. [`PolicyStore::acquire_claimed_publication`] and the
//!    `*_branch_activation` family — the crash-recoverable guard lifecycle that
//!    linearizes namespace branch activation against policy.
//! 7. [`PolicyStore::refresh`] and [`PolicyStore::load_current`] — the
//!    revalidation paths the cache drives.
//! 8. `bootstrap_config_drifted` and its normalizers — comparison only; they
//!    never write.
//!
//! ## Invariants
//!
//! - **S3 is authoritative.** Once `_security/heads/policy.json` exists, boot
//!   configuration is advisory only. Semantic drift emits a redacted
//!   structured warning and is ignored; it never rewrites the head.
//! - **A stale ETag never overwrites a newer head.** Every mutation is a
//!   conditional PUT on the exact observed ETag. A `Conflict` reloads.
//! - **A CAS that reports success without an ETag is not trusted.** Those paths
//!   re-read the head and compare identity before returning success.
//! - **Snapshots are immutable.** They are written create-only. A colliding key
//!   with different bytes is [`SecurityError::PolicyObjectCollision`], never an
//!   overwrite.
//! - **Head and snapshot must agree.** A head whose version or checksum
//!   disagrees with the snapshot it names is rejected as
//!   [`SecurityError::InvalidPolicy`]; no repair is attempted.
//! - **Two-layer concurrency defense.** The lease serializes head writers and
//!   its fencing token is claimed into the head; ETag CAS then catches any
//!   writer whose claim went stale. Neither layer alone is sufficient.
//! - **Lease release is best effort.** Failure is logged and swallowed so a
//!   process cannot deadlock on a lease it already lost; the lease expiry and
//!   fencing token remain the real authority.
//! - **Fail closed on entitlements.** A policy that uses constraints,
//!   delegation, or preservation without the matching [`Feature`] is rejected
//!   on both load and publish, so an unlicensed node cannot silently enforce
//!   less than the authoritative document requires.
//! - **Legacy wildcard grants must migrate before use.** A snapshot with
//!   `GrantActions::All` cannot compile; every load path drives a bounded
//!   migration that publishes a new version, and refuses to claim publication
//!   authority over an unmigrated head.
//! - **Bounded retries, then a typed error.** Migration, bootstrap-lease
//!   acquisition, and head claiming all give up with
//!   [`SecurityError::PolicyConflict`] rather than looping.
//!
//! ## Backend requirement
//!
//! Publication requires ETag compare-and-swap, which
//! `object_store`'s `LocalFileSystem` does not implement. Acquiring the
//! publication lease is create-only and succeeds on any backend, but renewing
//! and releasing it are conditional PUTs. On `StorageBackend::Local` the first
//! such PUT fails with `Storage(NotImplemented)`, so **first boot against a
//! `Local`-backed store currently fails outright** — `bootstrap` cannot renew
//! the lease it just acquired. This is a known and accepted limitation:
//! `Local` is documented as development/testing only, and S3/MinIO are
//! unaffected. See `src/security/CLAUDE.md` and `src/storage/CLAUDE.md`; three
//! `cargo test --lib` tests are red for exactly this reason.
//!
//! ## Cost
//!
//! Nothing here is on the query path. A head load is two sequential GETs (head,
//! then the snapshot it names). [`PolicyStore::refresh`] is one conditional GET
//! when nothing changed. A publication is a bounded sequence of lease, GET, and
//! conditional PUT roundtrips. [`PolicyStore::load_version`] is the expensive
//! outlier: it LISTs `_security/policies/` and GETs every object, so it belongs
//! only on privileged receipt-verification paths.
//!
//! ## Rust concepts used here
//!
//! [`PolicyStore`] derives `Clone`, but cloning it copies no policy data: the
//! entitlement set and the publication lease are `Arc`s, so a clone is two
//! reference-count bumps over the same shared authority. Java's nearest analogy
//! is sharing one object reference; the difference is that `Arc` makes the
//! sharing and its thread-safety explicit in the type.
//!
//! `claim_current_head` returns
//! `Result<Option<ClaimedPolicyPublication>, (ZeppelinError, PolicyPublicationLeaseClaim)>`.
//! The unusual error type exists because a [`PolicyPublicationLeaseClaim`] is an
//! owned obligation: whoever holds it must attempt release. Rust's move
//! semantics mean an early `?` would silently drop that obligation, so the
//! failing path hands the claim back to the caller instead. In Java this would
//! be a `try`/`finally`; in C, a `goto cleanup`. Here the compiler makes the
//! handoff visible in the signature.
//!
//! [`CreateOnlyOutcome`] and [`ConditionalPutOutcome`] are enums rather than
//! booleans or exceptions, so an exhaustive `match` forces every caller to
//! decide what a lost race means. "Someone else won" is a normal outcome of
//! this file's protocol, not an error.
//!
//! [`LoadedPolicy`] stores `observed_at: Instant`, not a wall-clock timestamp.
//! `Instant` is monotonic and immune to clock adjustment, which is what a
//! freshness budget needs; the `DateTime<Utc>` values threaded through this
//! file are domain timestamps recorded inside policy documents, and the two are
//! deliberately not interchangeable.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::Serialize;
use ulid::Ulid;

use crate::config::SecurityConfig;
use crate::error::{Result, ZeppelinError};
use crate::namespace::branching::activation::BranchActivationTarget;
use crate::namespace::branching::ActivationNonce;
use crate::namespace::BranchId;
use crate::storage::{ConditionalPutOutcome, CreateOnlyOutcome, StorageVersion, ZeppelinStore};

use super::{Entitlements, Feature, PolicyHead, PolicySnapshot, SecurityError};
use super::{
    PendingBranchActivation, PolicyActivationGuardPermit, PolicyPublicationLease,
    PolicyPublicationLeaseClaim,
};

const POLICY_ROOT: &str = "_security";
const POLICY_HEAD_KEY: &str = "_security/heads/policy.json";
const PHASE_SEVEN_MIGRATION_ATTEMPTS: usize = 5;
const BOOTSTRAP_LEASE_ATTEMPTS: usize = 100;
const BOOTSTRAP_LEASE_RETRY_DELAY: Duration = Duration::from_millis(10);

enum BootstrapPublicationAuthority {
    Existing(LoadedPolicy),
    Claimed(PolicyPublicationLeaseClaim),
}

#[derive(Serialize)]
struct PhaseSevenMigrationRecord<'a> {
    migration: &'static str,
    state: &'static str,
    old_version: u64,
    old_checksum: &'a str,
    candidate_version: u64,
    candidate_checksum: &'a str,
    created_at: DateTime<Utc>,
}

/// One verified authoritative policy observation and its CAS capability.
#[derive(Debug, Clone)]
pub struct LoadedPolicy {
    head: PolicyHead,
    snapshot: PolicySnapshot,
    head_version: StorageVersion,
    observed_at: Instant,
}

impl LoadedPolicy {
    fn from_head_observation(
        head: PolicyHead,
        snapshot: PolicySnapshot,
        head_version: StorageVersion,
        observed_at: Instant,
    ) -> Self {
        Self {
            head,
            snapshot,
            head_version,
            observed_at,
        }
    }

    pub(crate) fn from_publication(
        head: PolicyHead,
        snapshot: PolicySnapshot,
        head_version: StorageVersion,
        cas_completed_at: Instant,
    ) -> Self {
        Self {
            head,
            snapshot,
            head_version,
            observed_at: cas_completed_at,
        }
    }

    /// Borrow the selected head.
    #[must_use]
    pub fn head(&self) -> &PolicyHead {
        &self.head
    }

    /// Borrow the fully verified snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &PolicySnapshot {
        &self.snapshot
    }

    /// Borrow the exact head identity required for a later CAS publication.
    #[must_use]
    pub fn head_version(&self) -> &StorageVersion {
        &self.head_version
    }

    /// Return when the authoritative head read or CAS publication completed.
    #[must_use]
    pub(crate) fn observed_at(&self) -> Instant {
        self.observed_at
    }
}

/// Deep storage boundary for immutable snapshots and the CAS-published head.
#[derive(Clone)]
pub struct PolicyStore {
    store: ZeppelinStore,
    entitlements: Arc<Entitlements>,
    publication_lease: Arc<PolicyPublicationLease>,
}

/// One exact policy head claimed by a still-retained publication lease.
pub(crate) struct ClaimedPolicyPublication {
    loaded: LoadedPolicy,
    lease_claim: PolicyPublicationLeaseClaim,
}

impl ClaimedPolicyPublication {
    /// Borrow the exact strongly loaded head/snapshot observation.
    #[must_use]
    pub(crate) fn loaded(&self) -> &LoadedPolicy {
        &self.loaded
    }
}

pub(crate) enum PolicyRefresh {
    Unchanged { observed_at: Instant },
    Changed { loaded: Box<LoadedPolicy> },
}

pub(crate) enum PolicyPublication {
    Published(Box<LoadedPolicy>),
    Conflict,
    Guarded,
}

impl PolicyStore {
    /// Bind policy authority to the exact reserved `_security/` keyspace.
    #[must_use]
    pub fn new(store: ZeppelinStore, entitlements: Arc<Entitlements>) -> Self {
        let publication_lease = Arc::new(PolicyPublicationLease::new(store.clone()));
        Self {
            store,
            entitlements,
            publication_lease,
        }
    }

    /// Load the active policy, or atomically bootstrap version 1 from config.
    ///
    /// Concurrent first boots each write an immutable candidate snapshot, then
    /// race on one create-only head. The loser treats its unreferenced snapshot
    /// as a safe orphan and reads the winner's authoritative head. Once a head
    /// exists, S3 remains authoritative: bootstrap config is ignored, and a
    /// semantic mismatch emits a redacted structured warning.
    pub async fn load_or_bootstrap(
        &self,
        config: &SecurityConfig,
        now: DateTime<Utc>,
    ) -> Result<LoadedPolicy> {
        let mut policy = match self.load_current_unmigrated().await {
            Ok(policy) => policy,
            Err(ZeppelinError::NotFound { key })
                if key == POLICY_HEAD_KEY || key.ends_with(&format!("/{POLICY_HEAD_KEY}")) =>
            {
                self.bootstrap(config, now).await?
            }
            Err(error) => return Err(error),
        };
        policy = self.ensure_phase_seven_migrated(policy, now).await?;
        if bootstrap_config_drifted(
            config,
            policy.snapshot(),
            now,
            self.entitlements.has(Feature::Preservation),
        )? {
            tracing::warn!(
                policy_version = policy.snapshot().version().get(),
                configured_bootstrap_key_count = config.api_keys.len(),
                authoritative_policy_key_count = policy.snapshot().keys().len(),
                "configured bootstrap credentials drift from S3-authoritative security policy and are ignored"
            );
        }
        Ok(policy)
    }

    // Transitional raw load used only inside the bounded Phase 7 migration path.
    async fn load_current_unmigrated(&self) -> Result<LoadedPolicy> {
        let (head_bytes, head_version) = self.store.get_with_meta(POLICY_HEAD_KEY).await?;
        let observed_at = Instant::now();
        self.load_head_bytes(head_bytes, head_version, observed_at)
            .await
    }

    /// Read the authoritative head and return only a fully migrated, compilable snapshot.
    pub async fn load_current(&self, now: DateTime<Utc>) -> Result<LoadedPolicy> {
        let loaded = self.load_current_unmigrated().await?;
        self.ensure_phase_seven_migrated(loaded, now).await
    }

    /// Resolve one immutable historical snapshot by its monotonic version.
    ///
    /// The policy head only names the current snapshot, so privileged receipt
    /// verification enumerates the immutable policy inventory and validates
    /// every candidate it inspects. Missing or duplicate versions never fall
    /// back to the current policy.
    #[allow(dead_code)]
    pub(crate) async fn load_version(
        &self,
        version: super::PolicyVersion,
        checksum: &str,
    ) -> Result<Option<PolicySnapshot>> {
        if version == super::PolicyVersion::BOOT {
            return Ok(None);
        }
        let mut matched = None;
        for key in self.store.list_prefix("_security/policies/").await? {
            let bytes = self.store.get(&key).await?;
            let snapshot: PolicySnapshot = serde_json::from_slice(&bytes).map_err(|error| {
                SecurityError::InvalidPolicy(format!(
                    "historical policy snapshot JSON is invalid: {error}"
                ))
            })?;
            snapshot.validate_for_use()?;
            if snapshot.version() == version && snapshot.checksum() == checksum {
                matched = Some(snapshot);
            }
        }
        Ok(matched)
    }

    pub(crate) async fn refresh(
        &self,
        head_version: &StorageVersion,
        now: DateTime<Utc>,
    ) -> Result<PolicyRefresh> {
        let head = self
            .store
            .get_if_none_match(POLICY_HEAD_KEY, head_version)
            .await?;
        // This instant belongs to the authoritative head observation. Snapshot
        // loading may be delayed while a newer revoke head becomes CAS-visible.
        let observed_at = Instant::now();
        match head {
            None => Ok(PolicyRefresh::Unchanged { observed_at }),
            Some((head_bytes, next_etag)) => {
                let loaded = self
                    .load_head_bytes(head_bytes, next_etag, observed_at)
                    .await?;
                let loaded = self.ensure_phase_seven_migrated(loaded, now).await?;
                Ok(PolicyRefresh::Changed {
                    loaded: Box::new(loaded),
                })
            }
        }
    }

    async fn ensure_phase_seven_migrated(
        &self,
        mut loaded: LoadedPolicy,
        now: DateTime<Utc>,
    ) -> Result<LoadedPolicy> {
        for _ in 0..PHASE_SEVEN_MIGRATION_ATTEMPTS {
            if !loaded.snapshot().requires_phase_seven_all_migration() {
                return Ok(loaded);
            }
            let candidate = loaded.snapshot().migrate_phase_seven_all(now)?;
            self.record_phase_seven_migration_candidate(loaded.snapshot(), &candidate)
                .await?;
            loaded = match self.publish(candidate, loaded.head_version()).await? {
                PolicyPublication::Published(published) => *published,
                PolicyPublication::Conflict => {
                    tokio::time::sleep(BOOTSTRAP_LEASE_RETRY_DELAY).await;
                    self.load_current_unmigrated().await?
                }
                PolicyPublication::Guarded => {
                    return Err(SecurityError::PolicyConflict.into());
                }
            };
        }
        Err(SecurityError::PolicyConflict.into())
    }

    async fn record_phase_seven_migration_candidate(
        &self,
        previous: &PolicySnapshot,
        candidate: &PolicySnapshot,
    ) -> Result<()> {
        let record = PhaseSevenMigrationRecord {
            migration: "phase7-safe-all-v2",
            state: "candidate",
            old_version: previous.version().get(),
            old_checksum: previous.checksum(),
            candidate_version: candidate.version().get(),
            candidate_checksum: candidate.checksum(),
            created_at: candidate.created_at(),
        };
        let body = serde_json::to_vec(&record).map_err(|error| {
            SecurityError::InvalidPolicy(format!(
                "Phase 7 migration record encoding failed: {error}"
            ))
        })?;
        let key = format!(
            "_security/migrations/phase7-safe-all-v2/{}-{}.json",
            previous.checksum(),
            candidate.checksum()
        );
        match self
            .store
            .put_create_outcome(&key, Bytes::from(body.clone()))
            .await?
        {
            CreateOnlyOutcome::Created { .. } => Ok(()),
            CreateOnlyOutcome::AlreadyExists => {
                let existing = self.store.get(&key).await?;
                if existing.as_ref() == body.as_slice() {
                    Ok(())
                } else {
                    Err(SecurityError::PolicyObjectCollision.into())
                }
            }
        }
    }

    /// Acquire the global lease, claim its token into the exact policy head,
    /// and strongly load the snapshot selected by that claimed head.
    pub(crate) async fn acquire_claimed_publication(&self) -> Result<ClaimedPolicyPublication> {
        self.acquire_claimed_publication_from(None)
            .await?
            .ok_or_else(|| SecurityError::PolicyConflict.into())
    }

    async fn acquire_claimed_publication_from(
        &self,
        expected_head_version: Option<&StorageVersion>,
    ) -> Result<Option<ClaimedPolicyPublication>> {
        let lease_claim = self.publication_lease.acquire().await?;
        match self
            .claim_current_head(lease_claim, expected_head_version)
            .await
        {
            Ok(session) => Ok(session),
            Err((error, lease_claim)) => {
                self.release_publication_best_effort(&lease_claim).await;
                Err(error)
            }
        }
    }

    async fn claim_current_head(
        &self,
        mut lease_claim: PolicyPublicationLeaseClaim,
        expected_head_version: Option<&StorageVersion>,
    ) -> std::result::Result<
        Option<ClaimedPolicyPublication>,
        (ZeppelinError, PolicyPublicationLeaseClaim),
    > {
        for _attempt in 0..PHASE_SEVEN_MIGRATION_ATTEMPTS {
            let current = match self.load_current_unmigrated().await {
                Ok(current) => current,
                Err(error) => return Err((error, lease_claim)),
            };
            if expected_head_version.is_some_and(|expected| expected != current.head_version()) {
                self.release_publication_best_effort(&lease_claim).await;
                return Ok(None);
            }
            if current.snapshot().requires_phase_seven_all_migration() {
                return Err((
                    SecurityError::InvalidPolicy(
                        "legacy wildcard policy must migrate before publication claim".to_string(),
                    )
                    .into(),
                    lease_claim,
                ));
            }
            lease_claim = match self.publication_lease.renew(&lease_claim).await {
                Ok(renewed) => renewed,
                Err(error) => return Err((error, lease_claim)),
            };
            let claimed_head = match current
                .head()
                .claim_publication(lease_claim.fencing_token())
            {
                Ok(claimed) => claimed,
                Err(error) => return Err((error.into(), lease_claim)),
            };
            let head_bytes = match encode_policy_head(&claimed_head) {
                Ok(bytes) => bytes,
                Err(error) => return Err((error, lease_claim)),
            };
            let publication = match self
                .store
                .put_if_match_outcome(POLICY_HEAD_KEY, head_bytes, current.head_version())
                .await
            {
                Ok(publication) => publication,
                Err(error) => return Err((error, lease_claim)),
            };
            let observed_at = Instant::now();
            match publication {
                ConditionalPutOutcome::Conflict => continue,
                ConditionalPutOutcome::Updated {
                    version: Some(head_version),
                } => {
                    return Ok(Some(ClaimedPolicyPublication {
                        loaded: LoadedPolicy::from_publication(
                            claimed_head,
                            current.snapshot().clone(),
                            head_version,
                            observed_at,
                        ),
                        lease_claim,
                    }));
                }
                ConditionalPutOutcome::Updated { version: None } => {
                    let loaded = match self.load_current_unmigrated().await {
                        Ok(loaded) => loaded,
                        Err(error) => return Err((error, lease_claim)),
                    };
                    if loaded.head() != &claimed_head {
                        return Err((SecurityError::PolicyConflict.into(), lease_claim));
                    }
                    return Ok(Some(ClaimedPolicyPublication {
                        loaded,
                        lease_claim,
                    }));
                }
            }
        }
        Err((SecurityError::PolicyConflict.into(), lease_claim))
    }

    /// Release a claimed session whose caller validation did not install a guard.
    pub(crate) async fn release_claimed_publication(&self, session: ClaimedPolicyPublication) {
        self.release_publication_best_effort(&session.lease_claim)
            .await;
    }

    /// Relinquish a retained lease while intentionally leaving its durable
    /// guard for exact nonce recovery after a local post-CAS failure.
    pub(crate) async fn release_branch_activation_permit(
        &self,
        permit: PolicyActivationGuardPermit,
    ) {
        self.release_publication_best_effort(&permit.lease_claim)
            .await;
    }

    /// Take over the publication lease and retain an exact crash-recovery guard.
    /// Callers inspect/revoke the target nonce before choosing finalize or abort.
    pub(crate) async fn retain_pending_branch_activation(
        &self,
        session: ClaimedPolicyPublication,
        target: &BranchActivationTarget,
        expected_nonce: Option<ActivationNonce>,
    ) -> Result<Option<(LoadedPolicy, PolicyActivationGuardPermit)>> {
        let pending = session.loaded.head().pending_branch_activations();
        let guard = match select_pending_branch_activation(pending, target, expected_nonce) {
            Ok(guard) => guard,
            Err(error) => {
                self.release_publication_best_effort(&session.lease_claim)
                    .await;
                return Err(error.into());
            }
        };
        let Some(guard) = guard else {
            self.release_publication_best_effort(&session.lease_claim)
                .await;
            // Claiming the head fenced every older holder. The branch-key
            // absence is authoritative even when unrelated guards remain.
            return Ok(None);
        };
        let loaded = session.loaded;
        let permit = PolicyActivationGuardPermit {
            guard,
            lease_claim: session.lease_claim,
            head_version: loaded.head_version().clone(),
            control_revision: loaded.head().control_revision(),
        };
        Ok(Some((loaded, permit)))
    }

    /// Take over publication authority and retain the deterministic next
    /// expired guard from that same claimed head observation.
    pub(crate) async fn retain_next_expired_branch_activation(
        &self,
        session: ClaimedPolicyPublication,
        now: DateTime<Utc>,
    ) -> Result<Option<(LoadedPolicy, PolicyActivationGuardPermit)>> {
        let guard = select_next_expired_branch_activation(
            session.loaded.head().pending_branch_activations(),
            now,
        );
        let Some(guard) = guard else {
            self.release_publication_best_effort(&session.lease_claim)
                .await;
            return Ok(None);
        };
        let loaded = session.loaded;
        let permit = PolicyActivationGuardPermit {
            guard,
            lease_claim: session.lease_claim,
            head_version: loaded.head_version().clone(),
            control_revision: loaded.head().control_revision(),
        };
        Ok(Some((loaded, permit)))
    }

    /// CAS-insert one exact guard while retaining the same lease claim in the
    /// returned permit. The permit is the only path to later target activation
    /// verification and guard removal.
    pub(crate) async fn insert_pending_branch_activation(
        &self,
        mut session: ClaimedPolicyPublication,
        guard: PendingBranchActivation,
    ) -> Result<(LoadedPolicy, PolicyActivationGuardPermit)> {
        let result = self
            .insert_pending_branch_activation_head(&mut session, &guard)
            .await;
        match result {
            Ok(loaded) => {
                let permit = PolicyActivationGuardPermit {
                    guard,
                    lease_claim: session.lease_claim,
                    head_version: loaded.head_version().clone(),
                    control_revision: loaded.head().control_revision(),
                };
                Ok((loaded, permit))
            }
            Err(error) => {
                self.release_publication_best_effort(&session.lease_claim)
                    .await;
                Err(error)
            }
        }
    }

    async fn insert_pending_branch_activation_head(
        &self,
        session: &mut ClaimedPolicyPublication,
        guard: &PendingBranchActivation,
    ) -> Result<LoadedPolicy> {
        session.lease_claim = self.publication_lease.renew(&session.lease_claim).await?;
        let guarded_head = session
            .loaded
            .head()
            .insert_pending_branch_activation(guard.clone(), session.lease_claim.fencing_token())?;
        let publication = self
            .store
            .put_if_match_outcome(
                POLICY_HEAD_KEY,
                encode_policy_head(&guarded_head)?,
                session.loaded.head_version(),
            )
            .await?;
        let observed_at = Instant::now();
        match publication {
            ConditionalPutOutcome::Conflict => Err(SecurityError::PolicyConflict.into()),
            ConditionalPutOutcome::Updated {
                version: Some(head_version),
            } => Ok(LoadedPolicy::from_publication(
                guarded_head,
                session.loaded.snapshot().clone(),
                head_version,
                observed_at,
            )),
            ConditionalPutOutcome::Updated { version: None } => {
                let loaded = self.load_current_unmigrated().await?;
                if loaded.head() != &guarded_head {
                    return Err(SecurityError::PolicyConflict.into());
                }
                Ok(loaded)
            }
        }
    }

    /// Renew and strongly verify that the exact guard/lease/head permit remains
    /// authoritative immediately before the target visibility CAS.
    pub(crate) async fn renew_branch_activation_guard(
        &self,
        permit: &mut PolicyActivationGuardPermit,
    ) -> Result<LoadedPolicy> {
        permit.lease_claim = self.publication_lease.renew(&permit.lease_claim).await?;
        let loaded = self.load_current_unmigrated().await?;
        if *loaded.head_version() != permit.head_version
            || loaded.head().publication_fencing_token() != Some(permit.lease_claim.fencing_token())
            || loaded
                .head()
                .pending_branch_activations()
                .get(&permit.guard.branch_id())
                != Some(&permit.guard)
        {
            return Err(SecurityError::PolicyConflict.into());
        }
        Ok(loaded)
    }

    /// Remove a guard after the matching target Active CAS committed.
    pub(crate) async fn finalize_branch_activation(
        &self,
        permit: PolicyActivationGuardPermit,
    ) -> Result<LoadedPolicy> {
        self.resolve_branch_activation(permit, "finalized").await
    }

    /// Remove a guard only after the matching target nonce was durably revoked.
    pub(crate) async fn abort_branch_activation(
        &self,
        permit: PolicyActivationGuardPermit,
    ) -> Result<LoadedPolicy> {
        self.resolve_branch_activation(permit, "aborted").await
    }

    async fn resolve_branch_activation(
        &self,
        mut permit: PolicyActivationGuardPermit,
        outcome: &'static str,
    ) -> Result<LoadedPolicy> {
        let result = async {
            let current = self.renew_branch_activation_guard(&mut permit).await?;
            let resolved_head = current.head().remove_pending_branch_activation(
                &permit.guard,
                permit.lease_claim.fencing_token(),
            )?;
            let publication = self
                .store
                .put_if_match_outcome(
                    POLICY_HEAD_KEY,
                    encode_policy_head(&resolved_head)?,
                    current.head_version(),
                )
                .await?;
            let observed_at = Instant::now();
            let loaded = match publication {
                ConditionalPutOutcome::Conflict => {
                    return Err(SecurityError::PolicyConflict.into());
                }
                ConditionalPutOutcome::Updated {
                    version: Some(head_version),
                } => LoadedPolicy::from_publication(
                    resolved_head,
                    current.snapshot().clone(),
                    head_version,
                    observed_at,
                ),
                ConditionalPutOutcome::Updated { version: None } => {
                    let loaded = self.load_current_unmigrated().await?;
                    if loaded.head() != &resolved_head {
                        return Err(SecurityError::PolicyConflict.into());
                    }
                    loaded
                }
            };
            tracing::info!(
                branch_id = %permit.guard.branch_id(),
                activation_nonce = %permit.guard.activation_nonce(),
                outcome,
                "resolved pending branch activation policy guard"
            );
            Ok(loaded)
        }
        .await;
        self.release_publication_best_effort(&permit.lease_claim)
            .await;
        result
    }

    async fn release_publication_best_effort(&self, claim: &PolicyPublicationLeaseClaim) {
        if let Err(error) = self.publication_lease.release(claim).await {
            tracing::warn!(
                error = %error,
                fencing_token = claim.fencing_token().get(),
                "policy-publication lease release failed (best-effort)"
            );
        }
    }

    pub(crate) async fn publish(
        &self,
        candidate: PolicySnapshot,
        expected_head_version: &StorageVersion,
    ) -> Result<PolicyPublication> {
        candidate.validate_for_use()?;
        self.validate_entitlements(&candidate)?;
        let mut session = match self
            .acquire_claimed_publication_from(Some(expected_head_version))
            .await
        {
            Ok(Some(session)) => session,
            Ok(None) | Err(ZeppelinError::Security(SecurityError::PolicyConflict)) => {
                return Ok(PolicyPublication::Conflict);
            }
            Err(error) => return Err(error),
        };
        if !session
            .loaded
            .head()
            .pending_branch_activations()
            .is_empty()
        {
            self.release_publication_best_effort(&session.lease_claim)
                .await;
            return Ok(PolicyPublication::Guarded);
        }

        let result = self.publish_claimed(&mut session, candidate).await;
        self.release_publication_best_effort(&session.lease_claim)
            .await;
        result
    }

    async fn publish_claimed(
        &self,
        session: &mut ClaimedPolicyPublication,
        candidate: PolicySnapshot,
    ) -> Result<PolicyPublication> {
        let object_key = format!("{POLICY_ROOT}/policies/{}.json", Ulid::new());
        let snapshot_bytes = serde_json::to_vec(&candidate).map_err(|error| {
            SecurityError::InvalidPolicy(format!("policy snapshot encoding failed: {error}"))
        })?;
        match self
            .store
            .put_create_outcome(&object_key, Bytes::from(snapshot_bytes))
            .await?
        {
            CreateOnlyOutcome::Created { .. } => {}
            CreateOnlyOutcome::AlreadyExists => {
                return Err(SecurityError::PolicyObjectCollision.into());
            }
        }

        session.lease_claim = self.publication_lease.renew(&session.lease_claim).await?;
        let head = session.loaded.head().advance_semantic(
            &candidate,
            object_key,
            session.lease_claim.fencing_token(),
        )?;
        let publication = self
            .store
            .put_if_match_outcome(
                POLICY_HEAD_KEY,
                encode_policy_head(&head)?,
                session.loaded.head_version(),
            )
            .await?;
        let observed_at = Instant::now();
        match publication {
            ConditionalPutOutcome::Conflict => Ok(PolicyPublication::Conflict),
            ConditionalPutOutcome::Updated {
                version: Some(head_version),
            } => Ok(PolicyPublication::Published(Box::new(
                LoadedPolicy::from_publication(head, candidate, head_version, observed_at),
            ))),
            ConditionalPutOutcome::Updated { version: None } => {
                let loaded = self.load_current_unmigrated().await?;
                if loaded.snapshot().version() != candidate.version()
                    || loaded.snapshot().checksum() != candidate.checksum()
                {
                    return Err(SecurityError::InvalidPolicy(
                        "policy CAS succeeded without ETag but reread selected different content"
                            .to_string(),
                    )
                    .into());
                }
                Ok(PolicyPublication::Published(Box::new(loaded)))
            }
        }
    }

    async fn load_head_bytes(
        &self,
        head_bytes: Bytes,
        head_version: Option<StorageVersion>,
        observed_at: Instant,
    ) -> Result<LoadedPolicy> {
        let head: PolicyHead = serde_json::from_slice(&head_bytes).map_err(|error| {
            SecurityError::InvalidPolicy(format!("policy head JSON is invalid: {error}"))
        })?;
        head.validate(POLICY_ROOT)?;
        let head_version = head_version.ok_or(SecurityError::PolicyHeadMissingEtag)?;

        let snapshot_bytes = self.store.get(head.object_key()).await?;
        let snapshot: PolicySnapshot =
            serde_json::from_slice(&snapshot_bytes).map_err(|error| {
                SecurityError::InvalidPolicy(format!("policy snapshot JSON is invalid: {error}"))
            })?;
        if snapshot.requires_phase_seven_all_migration() {
            snapshot.verify_checksum()?;
        } else {
            snapshot.validate_for_use()?;
        }
        self.validate_entitlements(&snapshot)?;
        if snapshot.version() != head.version() || snapshot.checksum() != head.checksum() {
            return Err(SecurityError::InvalidPolicy(
                "policy head and snapshot identity disagree".to_string(),
            )
            .into());
        }

        Ok(LoadedPolicy::from_head_observation(
            head,
            snapshot,
            head_version,
            observed_at,
        ))
    }

    async fn bootstrap(&self, config: &SecurityConfig, now: DateTime<Utc>) -> Result<LoadedPolicy> {
        if config
            .api_keys
            .iter()
            .all(|key| key.expires_at.is_some_and(|expires_at| expires_at <= now))
        {
            return match self.load_current_unmigrated().await {
                Ok(policy) => Ok(policy),
                Err(ZeppelinError::NotFound { key })
                    if key == POLICY_HEAD_KEY || key.ends_with(&format!("/{POLICY_HEAD_KEY}")) =>
                {
                    Err(SecurityError::MissingBootstrapCredentials.into())
                }
                Err(error) => Err(error),
            };
        }
        let snapshot = PolicySnapshot::from_bootstrap(
            config,
            now,
            self.entitlements.has(super::Feature::Preservation),
        )?;
        snapshot.validate_for_use()?;
        self.validate_entitlements(&snapshot)?;
        let mut lease_claim = match self.acquire_bootstrap_publication().await? {
            BootstrapPublicationAuthority::Existing(existing) => return Ok(existing),
            BootstrapPublicationAuthority::Claimed(claim) => claim,
        };
        let result = self
            .publish_bootstrap_claimed(snapshot, &mut lease_claim)
            .await;
        self.release_publication_best_effort(&lease_claim).await;
        result
    }

    async fn acquire_bootstrap_publication(&self) -> Result<BootstrapPublicationAuthority> {
        for _attempt in 0..BOOTSTRAP_LEASE_ATTEMPTS {
            match self.publication_lease.acquire().await {
                Ok(claim) => match self.load_current_unmigrated().await {
                    Ok(existing) => {
                        self.release_publication_best_effort(&claim).await;
                        return Ok(BootstrapPublicationAuthority::Existing(existing));
                    }
                    Err(ZeppelinError::NotFound { .. }) => {
                        return Ok(BootstrapPublicationAuthority::Claimed(claim));
                    }
                    Err(error) => {
                        self.release_publication_best_effort(&claim).await;
                        return Err(error);
                    }
                },
                Err(ZeppelinError::Security(SecurityError::PolicyConflict)) => {
                    match self.load_current_unmigrated().await {
                        Ok(existing) => {
                            return Ok(BootstrapPublicationAuthority::Existing(existing));
                        }
                        Err(ZeppelinError::NotFound { .. }) => {
                            tokio::time::sleep(BOOTSTRAP_LEASE_RETRY_DELAY).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(SecurityError::PolicyConflict.into())
    }

    async fn publish_bootstrap_claimed(
        &self,
        snapshot: PolicySnapshot,
        lease_claim: &mut PolicyPublicationLeaseClaim,
    ) -> Result<LoadedPolicy> {
        let object_key = format!("{POLICY_ROOT}/policies/{}.json", Ulid::new());
        let snapshot_bytes = serde_json::to_vec(&snapshot).map_err(|error| {
            SecurityError::InvalidPolicy(format!("policy snapshot encoding failed: {error}"))
        })?;
        match self
            .store
            .put_create_outcome(&object_key, Bytes::from(snapshot_bytes))
            .await?
        {
            CreateOnlyOutcome::Created { .. } => {}
            CreateOnlyOutcome::AlreadyExists => {
                return Err(SecurityError::PolicyObjectCollision.into());
            }
        }

        *lease_claim = self.publication_lease.renew(lease_claim).await?;
        let head = PolicyHead::new(&snapshot, object_key)?
            .claim_publication(lease_claim.fencing_token())?;
        let publication = self
            .store
            .put_create_outcome(POLICY_HEAD_KEY, encode_policy_head(&head)?)
            .await?;
        let observed_at = Instant::now();
        match publication {
            CreateOnlyOutcome::Created {
                version: Some(head_version),
            } => Ok(LoadedPolicy::from_publication(
                head,
                snapshot,
                head_version,
                observed_at,
            )),
            CreateOnlyOutcome::Created { version: None } | CreateOnlyOutcome::AlreadyExists => {
                self.load_current_unmigrated().await
            }
        }
    }

    fn validate_entitlements(&self, snapshot: &PolicySnapshot) -> Result<()> {
        if snapshot.has_constraints() && !self.entitlements.has(Feature::Constraints) {
            return Err(SecurityError::FeatureRequired(Feature::Constraints).into());
        }
        if snapshot.has_delegation_features() && !self.entitlements.has(Feature::Delegation) {
            return Err(SecurityError::FeatureRequired(Feature::Delegation).into());
        }
        if snapshot.has_preservation_features() && !self.entitlements.has(Feature::Preservation) {
            return Err(SecurityError::FeatureRequired(Feature::Preservation).into());
        }
        if self
            .entitlements
            .limits()
            .max_principals
            .is_some_and(|limit| snapshot.principal_count() > limit as usize)
        {
            return Err(SecurityError::LicenseLimitExceeded("max_principals").into());
        }
        Ok(())
    }
}

fn select_pending_branch_activation(
    pending: &BTreeMap<BranchId, PendingBranchActivation>,
    target: &BranchActivationTarget,
    expected_nonce: Option<ActivationNonce>,
) -> std::result::Result<Option<PendingBranchActivation>, SecurityError> {
    let Some(guard) = pending.get(&target.branch_id()) else {
        return Ok(None);
    };
    if guard.target_namespace() != target.target_namespace()
        || guard.target_incarnation() != target.target_incarnation()
        || expected_nonce.is_some_and(|nonce| guard.activation_nonce() != nonce)
    {
        return Err(SecurityError::PolicyConflict);
    }
    Ok(Some(guard.clone()))
}

fn select_next_expired_branch_activation(
    pending: &BTreeMap<BranchId, PendingBranchActivation>,
    now: DateTime<Utc>,
) -> Option<PendingBranchActivation> {
    pending
        .values()
        .filter(|guard| guard.expires_at() <= now)
        .min_by(|left, right| {
            left.expires_at()
                .cmp(&right.expires_at())
                .then_with(|| left.branch_id().cmp(&right.branch_id()))
        })
        .cloned()
}

fn encode_policy_head(head: &PolicyHead) -> Result<Bytes> {
    serde_json::to_vec(head).map(Bytes::from).map_err(|error| {
        SecurityError::InvalidPolicy(format!("policy head encoding failed: {error}")).into()
    })
}

fn bootstrap_config_drifted(
    config: &SecurityConfig,
    authoritative: &PolicySnapshot,
    now: DateTime<Utc>,
    include_preservation: bool,
) -> Result<bool> {
    if config.api_keys.is_empty() {
        return Ok(!authoritative.keys().is_empty()
            || !authoritative.principals().is_empty()
            || !authoritative.grants().is_empty());
    }

    let comparison_time = authoritative
        .keys()
        .iter()
        .map(super::PolicyKey::created_at)
        .min()
        .unwrap_or(now);
    let configured = PolicySnapshot::from_bootstrap(config, comparison_time, include_preservation)?;
    Ok(normalized_bootstrap_semantics(&configured)?
        != normalized_bootstrap_semantics(authoritative)?)
}

fn normalized_bootstrap_semantics(snapshot: &PolicySnapshot) -> Result<serde_json::Value> {
    let mut value = serde_json::to_value(snapshot).map_err(|error| {
        SecurityError::InvalidPolicy(format!(
            "policy drift comparison serialization failed: {error}"
        ))
    })?;
    let object = value.as_object_mut().ok_or_else(|| {
        SecurityError::InvalidPolicy("policy drift comparison expected an object".to_string())
    })?;
    object.remove("version");
    object.remove("created_at");
    object.remove("created_by");
    object.remove("checksum");

    for field in ["principals", "keys", "grants"] {
        let entries = object
            .get_mut(field)
            .and_then(serde_json::Value::as_array_mut)
            .ok_or_else(|| {
                SecurityError::InvalidPolicy(format!(
                    "policy drift comparison expected {field} array"
                ))
            })?;
        if field == "keys" {
            for entry in entries.iter_mut() {
                let key = entry.as_object_mut().ok_or_else(|| {
                    SecurityError::InvalidPolicy(
                        "policy drift comparison expected key object".to_string(),
                    )
                })?;
                key.remove("created_at");
            }
        }
        if field == "grants" {
            normalize_grant_action_partitions(entries)?;
        }
        entries.sort_by_key(serde_json::Value::to_string);
    }
    Ok(value)
}

fn normalize_grant_action_partitions(entries: &mut Vec<serde_json::Value>) -> Result<()> {
    use std::collections::{BTreeMap, BTreeSet};

    let mut grouped: BTreeMap<
        String,
        (serde_json::Map<String, serde_json::Value>, BTreeSet<String>),
    > = BTreeMap::new();
    for entry in entries.drain(..) {
        let mut grant = entry.as_object().cloned().ok_or_else(|| {
            SecurityError::InvalidPolicy(
                "policy drift comparison expected grant object".to_string(),
            )
        })?;
        let actions = grant.remove("actions").ok_or_else(|| {
            SecurityError::InvalidPolicy(
                "policy drift comparison expected grant actions".to_string(),
            )
        })?;
        let selected = actions
            .as_object()
            .and_then(|actions| actions.get("actions"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                SecurityError::InvalidPolicy(
                    "policy drift comparison requires migrated selected actions".to_string(),
                )
            })?;
        let binding = serde_json::Value::Object(grant.clone()).to_string();
        let (_, merged) = grouped
            .entry(binding)
            .or_insert_with(|| (grant, BTreeSet::new()));
        for action in selected {
            merged.insert(
                action
                    .as_str()
                    .ok_or_else(|| {
                        SecurityError::InvalidPolicy(
                            "policy drift comparison expected action string".to_string(),
                        )
                    })?
                    .to_string(),
            );
        }
    }
    *entries = grouped
        .into_values()
        .map(|(mut grant, actions)| {
            grant.insert(
                "actions".to_string(),
                serde_json::json!({
                    "kind": "selected",
                    "actions": actions.into_iter().collect::<Vec<_>>()
                }),
            );
            serde_json::Value::Object(grant)
        })
        .collect();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use ulid::Ulid;
    use uuid::Uuid;

    use crate::namespace::branching::activation::BranchActivationTarget;
    use crate::namespace::branching::ActivationNonce;
    use crate::namespace::{BranchId, NamespaceId, NamespaceIncarnationId};

    use crate::security::{PolicyHeadDigest, PolicyLeaseFencingToken, PolicyVersion};

    use super::{
        select_next_expired_branch_activation, select_pending_branch_activation,
        PendingBranchActivation,
    };

    fn pending_guard(
        branch: u128,
        target: &str,
        incarnation: u128,
        nonce: u128,
    ) -> PendingBranchActivation {
        pending_guard_expiring_after(branch, target, incarnation, nonce, 300)
    }

    fn pending_guard_expiring_after(
        branch: u128,
        target: &str,
        incarnation: u128,
        nonce: u128,
        lifetime_secs: i64,
    ) -> PendingBranchActivation {
        let created_at = Utc
            .with_ymd_and_hms(2026, 7, 18, 12, 0, 0)
            .single()
            .expect("guard time");
        PendingBranchActivation::new(
            BranchId::from_ulid(Ulid::from(branch)),
            NamespaceId::new(target).expect("target namespace"),
            NamespaceIncarnationId::from_uuid(Uuid::from_u128(incarnation)),
            ActivationNonce::from_ulid(Ulid::from(nonce)),
            PolicyVersion::persisted(7).expect("policy version"),
            PolicyHeadDigest::new([0x5a; 32]),
            PolicyLeaseFencingToken::new(11).expect("lease token"),
            created_at,
            created_at + chrono::Duration::seconds(lifetime_secs),
        )
        .expect("pending guard")
    }

    #[test]
    fn claimed_lookup_treats_an_absent_branch_as_authoritative_despite_unrelated_guards() {
        let unrelated = pending_guard(1, "other-target", 101, 201);
        let pending = BTreeMap::from([(unrelated.branch_id(), unrelated)]);
        let requested = BranchActivationTarget::new(
            BranchId::from_ulid(Ulid::from(2_u128)),
            NamespaceId::new("requested-target").expect("requested namespace"),
            NamespaceIncarnationId::from_uuid(Uuid::from_u128(102)),
        );

        let selected = select_pending_branch_activation(&pending, &requested, None)
            .expect("an absent branch is authoritative after the lease claim");

        assert!(selected.is_none());
    }

    #[test]
    fn claimed_lookup_rejects_a_branch_key_bound_to_another_target_lifetime() {
        let stored = pending_guard(7, "target", 107, 207);
        let pending = BTreeMap::from([(stored.branch_id(), stored.clone())]);
        let mismatched = BranchActivationTarget::new(
            stored.branch_id(),
            NamespaceId::new("target").expect("target namespace"),
            NamespaceIncarnationId::from_uuid(Uuid::from_u128(108)),
        );

        let error = select_pending_branch_activation(&pending, &mismatched, None)
            .expect_err("a reused branch key must not cross target lifetimes");

        assert!(matches!(
            error,
            crate::security::SecurityError::PolicyConflict
        ));
    }

    #[test]
    fn claimed_expired_lookup_selects_oldest_expiry_then_branch_id() {
        let unexpired = pending_guard_expiring_after(1, "unexpired", 101, 201, 360);
        let tied_low = pending_guard_expiring_after(2, "tied-low", 102, 202, 120);
        let tied_high = pending_guard_expiring_after(3, "tied-high", 103, 203, 120);
        let later = pending_guard_expiring_after(4, "later", 104, 204, 240);
        let pending = BTreeMap::from([
            (unexpired.branch_id(), unexpired),
            (later.branch_id(), later),
            (tied_high.branch_id(), tied_high),
            (tied_low.branch_id(), tied_low),
        ]);
        let now = Utc
            .with_ymd_and_hms(2026, 7, 18, 12, 5, 0)
            .single()
            .expect("recovery time");

        let selected = select_next_expired_branch_activation(&pending, now)
            .expect("at least one guard is expired");

        assert_eq!(
            selected.branch_id(),
            BranchId::from_ulid(Ulid::from(2_u128))
        );
    }
}
