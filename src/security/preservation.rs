//! S3-authoritative preservation locks and fail-closed destruction guards.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use tokio::task::AbortHandle;
use ulid::Ulid;

use crate::error::{Result, ZeppelinError};
use crate::storage::{ConditionalPutOutcome, CreateOnlyOutcome, ZeppelinStore};
use crate::time::Clock;
use crate::types::Filter;

use super::{NamespaceId, PrincipalId, SecurityError};

const PRESERVATION_HEAD_KEY: &str = "_security/preservation/heads/locks.json";
const PRESERVATION_CAS_ATTEMPTS: usize = 5;
const MAX_REASON_TEXT_BYTES: usize = 4096;
const MAX_ACTIVE_LOCKS: usize = 10_000;

/// Stable identifier for one immutable preservation history.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PreservationLockId(String);

impl PreservationLockId {
    fn generate() -> Self {
        Self(format!("plk_{}", Ulid::new()))
    }

    /// Parse one bounded lock identifier safe for an object-key component.
    pub fn new(value: String) -> std::result::Result<Self, SecurityError> {
        let Some(suffix) = value.strip_prefix("plk_") else {
            return Err(SecurityError::InvalidPreservationRequest(
                "invalid preservation lock id".to_string(),
            ));
        };
        if suffix.len() != 26 || suffix.parse::<Ulid>().is_err() {
            return Err(SecurityError::InvalidPreservationRequest(
                "invalid preservation lock id".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Borrow the stable wire and object-key spelling.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for PreservationLockId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PreservationLockId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

/// Generic protected scope. Legal interpretation remains deployment policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PreservationScope {
    /// Every destructive operation in every namespace.
    Global,
    /// Every destructive operation in one namespace.
    Namespace {
        /// Validated namespace identifier.
        namespace: NamespaceId,
    },
    /// Rows conservatively overlapping one namespace filter.
    NamespaceFilter {
        /// Validated namespace identifier.
        namespace: NamespaceId,
        /// Existing exact filter AST.
        filter: Filter,
    },
}

impl PreservationScope {
    fn validate(&self) -> std::result::Result<(), SecurityError> {
        if let Self::NamespaceFilter { filter, .. } = self {
            super::policy::validate_policy_filter(filter)
                .map_err(SecurityError::InvalidPreservationRequest)?;
        }
        Ok(())
    }

    #[must_use]
    fn protects_namespace(&self, namespace: &NamespaceId) -> bool {
        match self {
            Self::Global => true,
            Self::Namespace { namespace: locked }
            | Self::NamespaceFilter {
                namespace: locked, ..
            } => locked == namespace,
        }
    }

    #[must_use]
    fn protects_vector_delete(&self, namespace: &NamespaceId, _filter: Option<&Filter>) -> bool {
        // A namespace filter is allowed to over-retain. Until disjointness is
        // structurally provable for the complete Filter AST, uncertain overlap
        // is treated as overlap and blocks the deletion.
        self.protects_namespace(namespace)
    }
}

/// Operator-selected reason category without legal behavior in core code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreservationReasonKind {
    /// Litigation-related deployment policy.
    Litigation,
    /// Regulatory deployment policy.
    Regulatory,
    /// Investigation-related deployment policy.
    Investigation,
    /// Customer-directed freeze.
    CustomerFreeze,
    /// Any validated deployment-specific reason.
    Other,
}

/// Lifecycle state encoded in immutable preservation records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreservationState {
    /// The lock is named by the authoritative active head.
    Active,
    /// A release record exists and the active head no longer names the lock.
    Released,
}

/// Strict request used to create one active lock.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatePreservationLock {
    /// Protected scope.
    pub scope: PreservationScope,
    /// Generic operator-selected category.
    pub reason_kind: PreservationReasonKind,
    /// Required human-readable reason retained as evidence.
    pub reason_text: String,
}

impl CreatePreservationLock {
    fn validate(&self) -> std::result::Result<(), SecurityError> {
        self.scope.validate()?;
        let trimmed = self.reason_text.trim();
        if trimmed.is_empty() || self.reason_text.len() > MAX_REASON_TEXT_BYTES {
            return Err(SecurityError::InvalidPreservationRequest(format!(
                "reason_text must contain 1..={MAX_REASON_TEXT_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

/// Immutable create or release evidence for one preservation lock.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreservationLockRecord {
    /// Stable lock identity.
    pub lock_id: PreservationLockId,
    /// Protected scope.
    pub scope: PreservationScope,
    /// Generic reason category.
    pub reason_kind: PreservationReasonKind,
    /// Required operator-supplied explanation.
    pub reason_text: String,
    /// Principal that created the lock.
    pub created_by: PrincipalId,
    /// Trusted creation time.
    pub created_at: DateTime<Utc>,
    /// Principal that released the lock, only on a release record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub released_by: Option<PrincipalId>,
    /// Trusted release time, only on a release record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub released_at: Option<DateTime<Utc>>,
    /// State represented by this immutable record.
    pub state: PreservationState,
}

/// Mutation kind named by one immutable preservation-head transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PreservationTransitionKind {
    Create,
    Release,
}

/// Immutable link proving which lock record a successful head CAS committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreservationTransitionRecord {
    version: u64,
    previous_transition_record: Option<String>,
    kind: PreservationTransitionKind,
    lock_id: PreservationLockId,
    lock_record_key: String,
}

impl PreservationLockRecord {
    fn active(
        lock_id: PreservationLockId,
        request: CreatePreservationLock,
        actor: PrincipalId,
        now: DateTime<Utc>,
    ) -> std::result::Result<Self, SecurityError> {
        request.validate()?;
        Ok(Self {
            lock_id,
            scope: request.scope,
            reason_kind: request.reason_kind,
            reason_text: request.reason_text,
            created_by: actor,
            created_at: now,
            released_by: None,
            released_at: None,
            state: PreservationState::Active,
        })
    }

    fn released(&self, actor: PrincipalId, now: DateTime<Utc>) -> Self {
        let mut released = self.clone();
        released.released_by = Some(actor);
        released.released_at = Some(now);
        released.state = PreservationState::Released;
        released
    }

    fn validate(&self) -> std::result::Result<(), SecurityError> {
        self.scope.validate()?;
        if self.reason_text.trim().is_empty() || self.reason_text.len() > MAX_REASON_TEXT_BYTES {
            return Err(SecurityError::InvalidPreservationState);
        }
        let release_fields_match = match self.state {
            PreservationState::Active => self.released_by.is_none() && self.released_at.is_none(),
            PreservationState::Released => self.released_by.is_some() && self.released_at.is_some(),
        };
        if !release_fields_match {
            return Err(SecurityError::InvalidPreservationState);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreservationHead {
    version: u64,
    active_lock_ids: Vec<PreservationLockId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_transition_record: Option<String>,
}

impl PreservationHead {
    fn empty() -> Self {
        Self {
            version: 1,
            active_lock_ids: Vec::new(),
            last_transition_record: None,
        }
    }

    fn validate(&self) -> std::result::Result<(), SecurityError> {
        if self.version == 0 || self.active_lock_ids.len() > MAX_ACTIVE_LOCKS {
            return Err(SecurityError::InvalidPreservationState);
        }
        let unique = self
            .active_lock_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if unique.len() != self.active_lock_ids.len() {
            return Err(SecurityError::InvalidPreservationState);
        }
        if self.active_lock_ids.windows(2).any(|ids| ids[0] >= ids[1]) {
            return Err(SecurityError::InvalidPreservationState);
        }
        if (self.version == 1) != self.last_transition_record.is_none() {
            return Err(SecurityError::InvalidPreservationState);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct CachedPreservation {
    head: PreservationHead,
    head_etag: Option<String>,
    active: BTreeMap<PreservationLockId, PreservationLockRecord>,
    last_confirmed: Instant,
}

/// One fresh destruction guard with redaction-safe matching lock identifiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreservationGuard {
    lock_ids: Vec<PreservationLockId>,
}

impl PreservationGuard {
    pub(crate) fn unlocked() -> Self {
        Self {
            lock_ids: Vec::new(),
        }
    }

    /// Return whether the operation is blocked by one or more active locks.
    #[must_use]
    pub fn is_locked(&self) -> bool {
        !self.lock_ids.is_empty()
    }

    /// Borrow identifiers intended for audit evidence, never response bodies.
    #[must_use]
    pub fn lock_ids(&self) -> &[PreservationLockId] {
        &self.lock_ids
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum MaintenanceDeferralSurface {
    Gc,
    Compaction,
    NamespaceDelete,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceDeferralRecord<'a> {
    event: &'static str,
    surface: MaintenanceDeferralSurface,
    namespace: &'a NamespaceId,
    lock_ids: &'a [PreservationLockId],
    ts: DateTime<Utc>,
}

/// Deep preservation module: immutable records, CAS head, cache, and guards.
pub struct PreservationService {
    store: ZeppelinStore,
    clock: Clock,
    current: RwLock<Arc<CachedPreservation>>,
    refresh_interval: Duration,
    refresh_abort: OnceLock<AbortHandle>,
}

impl PreservationService {
    /// Load or atomically initialize the authoritative lock head and start refresh.
    pub async fn start(
        store: ZeppelinStore,
        clock: Clock,
        refresh_interval: Duration,
    ) -> Result<Arc<Self>> {
        let loaded = load_or_initialize(&store).await?;
        let service = Arc::new(Self {
            store,
            clock,
            current: RwLock::new(Arc::new(loaded)),
            refresh_interval,
            refresh_abort: OnceLock::new(),
        });
        let weak = Arc::downgrade(&service);
        let task = tokio::spawn(refresh_loop(weak, refresh_interval));
        service
            .refresh_abort
            .set(task.abort_handle())
            .map_err(|_| {
                SecurityError::InvalidPreservationRequest(
                    "preservation refresh runtime initialized twice".to_string(),
                )
            })?;
        Ok(service)
    }

    fn current(&self) -> Arc<CachedPreservation> {
        self.current
            .read()
            .unwrap_or_else(|_| panic!("preservation cache lock poisoned"))
            .clone()
    }

    fn require_fresh(&self) -> std::result::Result<Arc<CachedPreservation>, SecurityError> {
        let current = self.current();
        if current.last_confirmed.elapsed() > self.refresh_interval.saturating_mul(2) {
            Err(SecurityError::PreservationStateUnavailable)
        } else {
            Ok(current)
        }
    }

    /// Return a fail-closed guard for namespace/snapshot/GC/compaction destruction.
    pub fn guard_namespace(
        &self,
        namespace: &NamespaceId,
    ) -> std::result::Result<PreservationGuard, SecurityError> {
        let current = self.require_fresh()?;
        let lock_ids = current
            .active
            .values()
            .filter(|record| record.scope.protects_namespace(namespace))
            .map(|record| record.lock_id.clone())
            .collect();
        Ok(PreservationGuard { lock_ids })
    }

    /// Return a conservative fail-closed guard for vector deletion.
    pub fn guard_vector_delete(
        &self,
        namespace: &NamespaceId,
        filter: Option<&Filter>,
    ) -> std::result::Result<PreservationGuard, SecurityError> {
        let current = self.require_fresh()?;
        let lock_ids = current
            .active
            .values()
            .filter(|record| record.scope.protects_vector_delete(namespace, filter))
            .map(|record| record.lock_id.clone())
            .collect();
        Ok(PreservationGuard { lock_ids })
    }

    /// List the fresh active set in stable identifier order.
    pub fn list_active(&self) -> std::result::Result<Vec<PreservationLockRecord>, SecurityError> {
        Ok(self.require_fresh()?.active.values().cloned().collect())
    }

    /// Publish one immutable active record and CAS-add it to the head.
    pub async fn create_lock(
        &self,
        actor: PrincipalId,
        request: CreatePreservationLock,
    ) -> Result<PreservationLockRecord> {
        let record = PreservationLockRecord::active(
            PreservationLockId::generate(),
            request,
            actor,
            self.clock.now(),
        )?;
        let key = create_record_key(&record.lock_id);
        self.store.put_create(&key, encode_json(&record)?).await?;

        for _attempt in 0..PRESERVATION_CAS_ATTEMPTS {
            let current = load_existing(&self.store).await?;
            if current.head.active_lock_ids.len() >= MAX_ACTIVE_LOCKS {
                return Err(SecurityError::InvalidPreservationRequest(
                    "active preservation lock limit reached".to_string(),
                )
                .into());
            }
            let mut next_head = current.head.clone();
            next_head.version = next_head
                .version
                .checked_add(1)
                .ok_or(SecurityError::InvalidPreservationState)?;
            next_head.active_lock_ids.push(record.lock_id.clone());
            next_head.active_lock_ids.sort();
            let transition_key = transition_record_key(Ulid::new());
            let transition = PreservationTransitionRecord {
                version: next_head.version,
                previous_transition_record: current.head.last_transition_record.clone(),
                kind: PreservationTransitionKind::Create,
                lock_id: record.lock_id.clone(),
                lock_record_key: key.clone(),
            };
            self.store
                .put_create(&transition_key, encode_json(&transition)?)
                .await?;
            next_head.last_transition_record = Some(transition_key);
            match self
                .store
                .put_if_match_outcome(
                    PRESERVATION_HEAD_KEY,
                    encode_json(&next_head)?,
                    current
                        .head_etag
                        .as_deref()
                        .ok_or(SecurityError::InvalidPreservationState)?,
                )
                .await?
            {
                ConditionalPutOutcome::Updated { e_tag } => {
                    let observed_at = Instant::now();
                    let mut active = current.active.clone();
                    active.insert(record.lock_id.clone(), record.clone());
                    self.install_committed(CachedPreservation {
                        head: next_head,
                        head_etag: e_tag,
                        active,
                        last_confirmed: observed_at,
                    });
                    return Ok(record);
                }
                ConditionalPutOutcome::Conflict => continue,
            }
        }
        Err(SecurityError::PreservationConflict.into())
    }

    /// Append one immutable release record, then CAS-remove the active identity.
    pub async fn release_lock(
        &self,
        lock_id: &PreservationLockId,
        actor: PrincipalId,
    ) -> Result<PreservationLockRecord> {
        let initial = load_existing(&self.store).await?;
        let active = initial
            .active
            .get(lock_id)
            .ok_or(SecurityError::PreservationLockNotFound)?;
        let released = active.released(actor, self.clock.now());
        let release_key = release_record_key(lock_id, Ulid::new());
        self.store
            .put_create(&release_key, encode_json(&released)?)
            .await?;

        for _attempt in 0..PRESERVATION_CAS_ATTEMPTS {
            let current = load_existing(&self.store).await?;
            if !current.active.contains_key(lock_id) {
                return Err(SecurityError::PreservationConflict.into());
            }
            let mut next_head = current.head.clone();
            next_head.version = next_head
                .version
                .checked_add(1)
                .ok_or(SecurityError::InvalidPreservationState)?;
            next_head.active_lock_ids.retain(|active| active != lock_id);
            let transition_key = transition_record_key(Ulid::new());
            let transition = PreservationTransitionRecord {
                version: next_head.version,
                previous_transition_record: current.head.last_transition_record.clone(),
                kind: PreservationTransitionKind::Release,
                lock_id: lock_id.clone(),
                lock_record_key: release_key.clone(),
            };
            self.store
                .put_create(&transition_key, encode_json(&transition)?)
                .await?;
            next_head.last_transition_record = Some(transition_key);
            match self
                .store
                .put_if_match_outcome(
                    PRESERVATION_HEAD_KEY,
                    encode_json(&next_head)?,
                    current
                        .head_etag
                        .as_deref()
                        .ok_or(SecurityError::InvalidPreservationState)?,
                )
                .await?
            {
                ConditionalPutOutcome::Updated { e_tag } => {
                    let observed_at = Instant::now();
                    let mut active = current.active.clone();
                    active.remove(lock_id);
                    self.install_committed(CachedPreservation {
                        head: next_head,
                        head_etag: e_tag,
                        active,
                        last_confirmed: observed_at,
                    });
                    return Ok(released);
                }
                ConditionalPutOutcome::Conflict => continue,
            }
        }
        Err(SecurityError::PreservationConflict.into())
    }

    /// Revalidate the authoritative head and replace disposable cache state.
    pub async fn refresh_once(&self) -> Result<()> {
        let loaded = load_existing(&self.store).await?;
        self.install_refreshed(loaded)
    }

    fn install_refreshed(&self, loaded: CachedPreservation) -> Result<()> {
        let mut current = self
            .current
            .write()
            .unwrap_or_else(|_| panic!("preservation cache lock poisoned"));
        Self::install_refreshed_state(&mut current, loaded)
    }

    fn install_refreshed_state(
        current: &mut Arc<CachedPreservation>,
        loaded: CachedPreservation,
    ) -> Result<()> {
        match loaded.head.version.cmp(&current.head.version) {
            std::cmp::Ordering::Less => {
                return Err(SecurityError::InvalidPreservationState.into());
            }
            std::cmp::Ordering::Equal if loaded.head != current.head => {
                return Err(SecurityError::InvalidPreservationState.into());
            }
            std::cmp::Ordering::Equal => {
                let last_confirmed = loaded.last_confirmed.max(current.last_confirmed);
                *current = Arc::new(CachedPreservation {
                    last_confirmed,
                    ..loaded
                });
            }
            std::cmp::Ordering::Greater => {
                *current = Arc::new(loaded);
            }
        }
        Ok(())
    }

    /// Install state already made authoritative by a successful CAS without a
    /// fallible reread that could downgrade the committed HTTP/audit outcome.
    fn install_committed(&self, committed: CachedPreservation) {
        let mut current = self
            .current
            .write()
            .unwrap_or_else(|_| panic!("preservation cache lock poisoned"));
        match committed.head.version.cmp(&current.head.version) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => {
                assert_eq!(
                    committed.head, current.head,
                    "preservation head version was reused with different content"
                );
                if committed.last_confirmed > current.last_confirmed {
                    *current = Arc::new(committed);
                }
            }
            std::cmp::Ordering::Greater => *current = Arc::new(committed),
        }
    }

    /// Durably record that locked maintenance exited before destructive work.
    pub async fn record_maintenance_deferral(
        &self,
        compaction: bool,
        namespace: &NamespaceId,
        guard: &PreservationGuard,
    ) -> Result<()> {
        let surface = if compaction {
            MaintenanceDeferralSurface::Compaction
        } else {
            MaintenanceDeferralSurface::Gc
        };
        let record = MaintenanceDeferralRecord {
            event: if compaction {
                "compaction_deferred_preservation"
            } else {
                "gc_deferred_preservation"
            },
            surface,
            namespace,
            lock_ids: guard.lock_ids(),
            ts: self.clock.now(),
        };
        self.write_maintenance_deferral(&record).await
    }

    /// Record a resumed namespace purge blocked by a newly active lock.
    pub async fn record_namespace_delete_deferral(
        &self,
        namespace: &NamespaceId,
        guard: &PreservationGuard,
    ) -> Result<()> {
        let record = MaintenanceDeferralRecord {
            event: "namespace_delete_deferred_preservation",
            surface: MaintenanceDeferralSurface::NamespaceDelete,
            namespace,
            lock_ids: guard.lock_ids(),
            ts: self.clock.now(),
        };
        self.write_maintenance_deferral(&record).await
    }

    async fn write_maintenance_deferral(
        &self,
        record: &MaintenanceDeferralRecord<'_>,
    ) -> Result<()> {
        self.store
            .put_create(
                &format!("_audit/preservation/{}.json", Ulid::new()),
                encode_json(record)?,
            )
            .await
    }
}

impl Drop for PreservationService {
    fn drop(&mut self) {
        if let Some(abort) = self.refresh_abort.get() {
            abort.abort();
        }
    }
}

async fn refresh_loop(cache: Weak<PreservationService>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let Some(cache) = cache.upgrade() else {
            return;
        };
        if let Err(error) = cache.refresh_once().await {
            tracing::error!(
                error = %error,
                "preservation head refresh failed; destruction will fail closed after freshness bound"
            );
        }
    }
}

async fn load_or_initialize(store: &ZeppelinStore) -> Result<CachedPreservation> {
    match load_existing(store).await {
        Ok(loaded) => Ok(loaded),
        Err(ZeppelinError::NotFound { key })
            if key == PRESERVATION_HEAD_KEY
                || key.ends_with(&format!("/{PRESERVATION_HEAD_KEY}")) =>
        {
            match store
                .put_create_outcome(
                    PRESERVATION_HEAD_KEY,
                    encode_json(&PreservationHead::empty())?,
                )
                .await?
            {
                CreateOnlyOutcome::Created { .. } | CreateOnlyOutcome::AlreadyExists => {
                    load_existing(store).await
                }
            }
        }
        Err(error) => Err(error),
    }
}

async fn load_existing(store: &ZeppelinStore) -> Result<CachedPreservation> {
    let (bytes, etag) = store.get_with_meta(PRESERVATION_HEAD_KEY).await?;
    // Freshness begins at the authoritative head observation, not after the
    // sequential lock/transition document loads below.
    let observed_at = Instant::now();
    let head: PreservationHead =
        serde_json::from_slice(&bytes).map_err(|_| SecurityError::InvalidPreservationState)?;
    head.validate()?;
    let head_etag = Some(etag.ok_or(SecurityError::InvalidPreservationState)?);
    let mut active = BTreeMap::new();
    for lock_id in &head.active_lock_ids {
        let bytes = store.get(&create_record_key(lock_id)).await?;
        let record: PreservationLockRecord =
            serde_json::from_slice(&bytes).map_err(|_| SecurityError::InvalidPreservationState)?;
        record.validate()?;
        if record.state != PreservationState::Active || &record.lock_id != lock_id {
            return Err(SecurityError::InvalidPreservationState.into());
        }
        active.insert(lock_id.clone(), record);
    }
    validate_committed_transition(store, &head, &active).await?;
    Ok(CachedPreservation {
        head,
        head_etag,
        active,
        last_confirmed: observed_at,
    })
}

async fn validate_committed_transition(
    store: &ZeppelinStore,
    head: &PreservationHead,
    active: &BTreeMap<PreservationLockId, PreservationLockRecord>,
) -> Result<()> {
    let Some(transition_key) = &head.last_transition_record else {
        return Ok(());
    };
    if !transition_key.starts_with("_security/preservation/transitions/")
        || !transition_key.ends_with(".json")
        || transition_key.contains("..")
    {
        return Err(SecurityError::InvalidPreservationState.into());
    }
    let bytes = store.get(transition_key).await?;
    let transition: PreservationTransitionRecord =
        serde_json::from_slice(&bytes).map_err(|_| SecurityError::InvalidPreservationState)?;
    if transition.version != head.version
        || transition.previous_transition_record.as_deref() == Some(transition_key.as_str())
    {
        return Err(SecurityError::InvalidPreservationState.into());
    }
    let lock_bytes = store.get(&transition.lock_record_key).await?;
    let record: PreservationLockRecord =
        serde_json::from_slice(&lock_bytes).map_err(|_| SecurityError::InvalidPreservationState)?;
    record.validate()?;
    if record.lock_id != transition.lock_id {
        return Err(SecurityError::InvalidPreservationState.into());
    }
    match transition.kind {
        PreservationTransitionKind::Create => {
            if transition.lock_record_key != create_record_key(&transition.lock_id)
                || record.state != PreservationState::Active
                || !active.contains_key(&transition.lock_id)
            {
                return Err(SecurityError::InvalidPreservationState.into());
            }
        }
        PreservationTransitionKind::Release => {
            let release_prefix = format!(
                "_security/preservation/releases/{}/",
                transition.lock_id.as_str()
            );
            if !transition.lock_record_key.starts_with(&release_prefix)
                || !transition.lock_record_key.ends_with(".json")
                || record.state != PreservationState::Released
                || active.contains_key(&transition.lock_id)
            {
                return Err(SecurityError::InvalidPreservationState.into());
            }
        }
    }
    Ok(())
}

fn create_record_key(lock_id: &PreservationLockId) -> String {
    format!("_security/preservation/{}.json", lock_id.as_str())
}

fn release_record_key(lock_id: &PreservationLockId, record_id: Ulid) -> String {
    format!(
        "_security/preservation/releases/{}/{record_id}.json",
        lock_id.as_str()
    )
}

fn transition_record_key(record_id: Ulid) -> String {
    format!("_security/preservation/transitions/{record_id}.json")
}

fn encode_json(value: &impl Serialize) -> Result<Bytes> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|_| SecurityError::InvalidPreservationState.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AttributeValue;

    #[test]
    fn preservation_lock_id_is_strict() {
        let id = PreservationLockId::generate();
        let parsed = PreservationLockId::new(id.as_str().to_string())
            .unwrap_or_else(|error| panic!("generated lock id must parse: {error}"));
        assert_eq!(parsed, id);
        assert!(PreservationLockId::new("../escape".to_string()).is_err());
        assert!(serde_json::from_str::<PreservationLockId>("\"plk_short\"").is_err());
    }

    #[test]
    fn namespace_filter_conservatively_blocks_uncertain_overlap() {
        let namespace = NamespaceId::new("preserved".to_string())
            .unwrap_or_else(|error| panic!("preserved namespace must parse: {error}"));
        let other = NamespaceId::new("other".to_string())
            .unwrap_or_else(|error| panic!("other namespace must parse: {error}"));
        let scope = PreservationScope::NamespaceFilter {
            namespace: namespace.clone(),
            filter: Filter::Eq {
                field: "tenant".to_string(),
                value: AttributeValue::String("a".to_string()),
            },
        };
        assert!(scope.protects_vector_delete(&namespace, None));
        assert!(!scope.protects_vector_delete(&other, None));
    }

    #[test]
    fn regressing_refresh_cannot_replace_or_refresh_newer_lock_state() {
        let current_confirmed = Instant::now();
        let mut current = Arc::new(CachedPreservation {
            head: PreservationHead {
                version: 2,
                active_lock_ids: Vec::new(),
                last_transition_record: Some(
                    "_security/preservation/transitions/current.json".to_string(),
                ),
            },
            head_etag: Some("current".to_string()),
            active: BTreeMap::new(),
            last_confirmed: current_confirmed,
        });
        let regressing = CachedPreservation {
            head: PreservationHead::empty(),
            head_etag: Some("stale".to_string()),
            active: BTreeMap::new(),
            last_confirmed: Instant::now(),
        };

        assert!(
            PreservationService::install_refreshed_state(&mut current, regressing).is_err(),
            "an out-of-order refresh must fail closed"
        );
        assert_eq!(current.head.version, 2);
        assert_eq!(current.last_confirmed, current_confirmed);
    }
}
