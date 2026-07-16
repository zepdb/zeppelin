//! S3-authoritative loading and atomic bootstrap of security policy.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::Serialize;
use ulid::Ulid;

use crate::config::SecurityConfig;
use crate::error::{Result, ZeppelinError};
use crate::storage::{ConditionalPutOutcome, CreateOnlyOutcome, ZeppelinStore};

use super::{Entitlements, Feature, PolicyHead, PolicySnapshot, SecurityError};

const POLICY_ROOT: &str = "_security";
const POLICY_HEAD_KEY: &str = "_security/heads/policy.json";
const PHASE_SEVEN_MIGRATION_ATTEMPTS: usize = 5;

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
    head_etag: String,
    observed_at: Instant,
}

impl LoadedPolicy {
    fn from_head_observation(
        head: PolicyHead,
        snapshot: PolicySnapshot,
        head_etag: String,
        observed_at: Instant,
    ) -> Self {
        Self {
            head,
            snapshot,
            head_etag,
            observed_at,
        }
    }

    pub(crate) fn from_publication(
        head: PolicyHead,
        snapshot: PolicySnapshot,
        head_etag: String,
        cas_completed_at: Instant,
    ) -> Self {
        Self {
            head,
            snapshot,
            head_etag,
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

    /// Borrow the exact head ETag required for a later CAS publication.
    #[must_use]
    pub fn head_etag(&self) -> &str {
        &self.head_etag
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
}

pub(crate) enum PolicyRefresh {
    Unchanged { observed_at: Instant },
    Changed { loaded: Box<LoadedPolicy> },
}

pub(crate) enum PolicyPublication {
    Published(Box<LoadedPolicy>),
    Conflict,
}

impl PolicyStore {
    /// Bind policy authority to the exact reserved `_security/` keyspace.
    #[must_use]
    pub fn new(store: ZeppelinStore, entitlements: Arc<Entitlements>) -> Self {
        Self {
            store,
            entitlements,
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
        let (head_bytes, head_etag) = self.store.get_with_meta(POLICY_HEAD_KEY).await?;
        let observed_at = Instant::now();
        self.load_head_bytes(head_bytes, head_etag, observed_at)
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
        head_etag: &str,
        now: DateTime<Utc>,
    ) -> Result<PolicyRefresh> {
        let head = self
            .store
            .get_if_none_match(POLICY_HEAD_KEY, head_etag)
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
            loaded = match self.publish(candidate, loaded.head_etag()).await? {
                PolicyPublication::Published(published) => *published,
                PolicyPublication::Conflict => self.load_current_unmigrated().await?,
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

    pub(crate) async fn publish(
        &self,
        candidate: PolicySnapshot,
        expected_head_etag: &str,
    ) -> Result<PolicyPublication> {
        candidate.validate_for_use()?;
        self.validate_entitlements(&candidate)?;
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

        let head = PolicyHead::new(&candidate, object_key)?;
        let head_bytes = serde_json::to_vec(&head).map_err(|error| {
            SecurityError::InvalidPolicy(format!("policy head encoding failed: {error}"))
        })?;
        let publication = self
            .store
            .put_if_match_outcome(POLICY_HEAD_KEY, Bytes::from(head_bytes), expected_head_etag)
            .await?;
        let observed_at = Instant::now();
        match publication {
            ConditionalPutOutcome::Conflict => Ok(PolicyPublication::Conflict),
            ConditionalPutOutcome::Updated {
                e_tag: Some(head_etag),
            } => Ok(PolicyPublication::Published(Box::new(
                LoadedPolicy::from_publication(head, candidate, head_etag, observed_at),
            ))),
            ConditionalPutOutcome::Updated { e_tag: None } => {
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
        head_etag: Option<String>,
        observed_at: Instant,
    ) -> Result<LoadedPolicy> {
        let head: PolicyHead = serde_json::from_slice(&head_bytes).map_err(|error| {
            SecurityError::InvalidPolicy(format!("policy head JSON is invalid: {error}"))
        })?;
        head.validate(POLICY_ROOT)?;
        let head_etag = head_etag.ok_or(SecurityError::PolicyHeadMissingEtag)?;

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
            head_etag,
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

        let head = PolicyHead::new(&snapshot, object_key)?;
        let head_bytes = serde_json::to_vec(&head).map_err(|error| {
            SecurityError::InvalidPolicy(format!("policy head encoding failed: {error}"))
        })?;
        let publication = self
            .store
            .put_create_outcome(POLICY_HEAD_KEY, Bytes::from(head_bytes))
            .await?;
        let observed_at = Instant::now();
        match publication {
            CreateOnlyOutcome::Created {
                e_tag: Some(head_etag),
            } => Ok(LoadedPolicy::from_publication(
                head,
                snapshot,
                head_etag,
                observed_at,
            )),
            CreateOnlyOutcome::Created { e_tag: None } | CreateOnlyOutcome::AlreadyExists => {
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
