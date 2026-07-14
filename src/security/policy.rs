//! Strict persisted vocabulary for S3-authoritative security policy.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::{ApiKeyConfig, SecurityConfig};

use super::{
    Action, AllowDecision, DenyReason, NamespaceId, PolicyVersion, Principal, PrincipalId,
    PrincipalKind, Resource, SecurityError,
};

const SHA256_HEX_LEN: usize = 64;
// A persisted `All` grant is intentionally frozen to the Phase 3 action
// universe. Adding an Action makes this assignment fail to compile until a
// policy-schema migration is designed; old immutable snapshots never widen
// merely because a newer binary adds an action.
const POLICY_ALL_V1: [Action; 21] = Action::ALL;

#[derive(Debug, Clone)]
pub(crate) struct CompiledKey {
    pub(crate) principal: Principal,
    pub(crate) digest: [u8; 32],
    pub(crate) state: KeyState,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) revokes_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct CompiledGrant {
    scope: GrantScope,
    actions: HashSet<Action>,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledPolicy {
    version: PolicyVersion,
    principals: HashSet<PrincipalId>,
    keys: HashMap<String, CompiledKey>,
    grants: HashMap<PrincipalId, Vec<CompiledGrant>>,
}

impl CompiledPolicy {
    #[must_use]
    pub(crate) const fn version(&self) -> PolicyVersion {
        self.version
    }

    #[must_use]
    pub(crate) fn key(&self, key_id: &str) -> Option<&CompiledKey> {
        self.keys.get(key_id)
    }

    pub(crate) fn authorize(
        &self,
        principal: &Principal,
        now: DateTime<Utc>,
        action: Action,
        resource: &Resource,
    ) -> Result<(), DenyReason> {
        let grants = self.active_grants(principal, now)?;
        let matching_action = grants
            .iter()
            .filter(|grant| grant.actions.contains(&action))
            .collect::<Vec<_>>();
        if matching_action.is_empty() {
            return Err(DenyReason::ActionNotGranted);
        }
        if matching_action
            .iter()
            .any(|grant| match (&grant.scope, resource.namespace()) {
                (GrantScope::Global, _) => true,
                (GrantScope::Namespace { namespace }, Some(resource_namespace)) => {
                    namespace == resource_namespace
                }
                (GrantScope::Namespace { .. }, None) => false,
            })
        {
            Ok(())
        } else if resource.namespace().is_some() {
            Err(DenyReason::NamespaceNotGranted)
        } else {
            Err(DenyReason::ActionNotGranted)
        }
    }

    pub(crate) fn authorize_action(
        &self,
        principal: &Principal,
        now: DateTime<Utc>,
        action: Action,
    ) -> Result<(), DenyReason> {
        let grants = self.active_grants(principal, now)?;
        if grants.iter().any(|grant| grant.actions.contains(&action)) {
            Ok(())
        } else {
            Err(DenyReason::ActionNotGranted)
        }
    }

    fn active_grants<'a>(
        &'a self,
        principal: &Principal,
        now: DateTime<Utc>,
    ) -> Result<&'a [CompiledGrant], DenyReason> {
        if principal.is_anonymous() {
            return Err(DenyReason::Unauthenticated);
        }
        let key_id = principal
            .api_key_id
            .as_ref()
            .ok_or(DenyReason::CredentialUnknown)?;
        let key = self
            .keys
            .get(key_id.as_str())
            .ok_or(DenyReason::CredentialUnknown)?;
        if key.principal.id != principal.id || !self.principals.contains(&principal.id) {
            return Err(DenyReason::CredentialUnknown);
        }
        if key.state == KeyState::Revoked
            && key.revokes_at.is_none_or(|revokes_at| revokes_at <= now)
        {
            return Err(DenyReason::CredentialUnknown);
        }
        if key.state == KeyState::Expired
            || key.expires_at.is_some_and(|expires_at| expires_at <= now)
        {
            return Err(DenyReason::CredentialExpired);
        }

        self.grants
            .get(&principal.id)
            .map(Vec::as_slice)
            .ok_or(DenyReason::ActionNotGranted)
    }
}

/// Validated public identifier carried before an API-key secret.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ApiKeyId(String);

impl ApiKeyId {
    /// Construct one canonical `zpk1_` identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, SecurityError> {
        let value = value.into();
        let suffix = value.strip_prefix("zpk1_");
        let valid = value.len() <= 128
            && suffix.is_some_and(|suffix| {
                !suffix.is_empty()
                    && suffix
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            });
        if valid {
            Ok(Self(value))
        } else {
            Err(SecurityError::InvalidPolicy(
                "invalid API-key identifier".to_string(),
            ))
        }
    }

    /// Borrow the canonical identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ApiKeyId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Lifecycle state persisted with one hashed API credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyState {
    /// Credential may authenticate until another time/state restriction applies.
    Active,
    /// Credential was explicitly revoked.
    Revoked,
    /// Credential was already expired when this snapshot was published.
    Expired,
}

/// Stable identity metadata independent of any one credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyPrincipal {
    principal_id: PrincipalId,
    kind: PrincipalKind,
    display_name: String,
}

impl PolicyPrincipal {
    pub(crate) fn new(
        principal_id: PrincipalId,
        kind: PrincipalKind,
        display_name: String,
    ) -> Result<Self, SecurityError> {
        if display_name.trim().is_empty() {
            return Err(SecurityError::InvalidPolicyRequest(
                "principal display_name must not be empty".to_string(),
            ));
        }
        Ok(Self {
            principal_id,
            kind,
            display_name,
        })
    }

    /// Borrow the stable principal identifier.
    #[must_use]
    pub fn principal_id(&self) -> &PrincipalId {
        &self.principal_id
    }

    /// Return the typed principal kind.
    #[must_use]
    pub const fn kind(&self) -> PrincipalKind {
        self.kind
    }

    /// Borrow the redaction-safe display identity.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

/// Hashed named credential embedded in an immutable policy snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyKey {
    key_id: ApiKeyId,
    name: String,
    sha256_hex: String,
    principal_id: PrincipalId,
    state: KeyState,
    expires_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    rotated_from: Option<ApiKeyId>,
    revokes_at: Option<DateTime<Utc>>,
}

impl PolicyKey {
    pub(crate) fn new_active(
        key_id: ApiKeyId,
        name: String,
        sha256_hex: String,
        principal_id: PrincipalId,
        expires_at: Option<DateTime<Utc>>,
        created_at: DateTime<Utc>,
        rotated_from: Option<ApiKeyId>,
    ) -> Result<Self, SecurityError> {
        if name.trim().is_empty() {
            return Err(SecurityError::InvalidPolicyRequest(
                "key name must not be empty".to_string(),
            ));
        }
        if !valid_checksum(&sha256_hex) {
            return Err(SecurityError::InvalidPolicyRequest(
                "key digest must be SHA-256 hex".to_string(),
            ));
        }
        Ok(Self {
            key_id,
            name,
            sha256_hex: sha256_hex.to_ascii_lowercase(),
            principal_id,
            state: KeyState::Active,
            expires_at,
            created_at,
            rotated_from,
            revokes_at: None,
        })
    }

    /// Borrow the public key identifier.
    #[must_use]
    pub fn key_id(&self) -> &ApiKeyId {
        &self.key_id
    }

    /// Borrow the lowercase SHA-256 digest. No plaintext secret is retained.
    #[must_use]
    pub fn sha256_hex(&self) -> &str {
        &self.sha256_hex
    }

    /// Borrow the owning stable principal.
    #[must_use]
    pub fn principal_id(&self) -> &PrincipalId {
        &self.principal_id
    }

    /// Borrow the human-readable credential name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the persisted lifecycle state.
    #[must_use]
    pub const fn state(&self) -> KeyState {
        self.state
    }

    /// Return the optional credential expiry.
    #[must_use]
    pub const fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expires_at
    }

    /// Return the creation instant.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Borrow the predecessor key when this key came from rotation.
    #[must_use]
    pub fn rotated_from(&self) -> Option<&ApiKeyId> {
        self.rotated_from.as_ref()
    }

    /// Return the scheduled rotation-overlap deadline; `None` means an
    /// explicitly revoked credential is invalid independent of wall-clock skew.
    #[must_use]
    pub const fn revokes_at(&self) -> Option<DateTime<Utc>> {
        self.revokes_at
    }
}

/// Scope to which one action grant applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GrantScope {
    /// Process-wide resources and every namespace.
    Global,
    /// One exact namespace.
    Namespace {
        /// Validated namespace receiving the grant.
        namespace: NamespaceId,
    },
}

/// Explicit action set granted at one scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GrantActions {
    /// Every action in the exhaustive inventory, including destructive actions.
    All,
    /// A nonempty explicit set of action variants.
    Selected {
        /// Sorted, unique actions granted at this scope.
        actions: Vec<Action>,
    },
}

/// One independently evaluated principal/scope/action binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyGrant {
    principal_id: PrincipalId,
    scope: GrantScope,
    actions: GrantActions,
}

/// Transient result of one credential issuance. The plaintext appears only
/// here and is never embedded in a policy snapshot or cache entry.
pub struct IssuedApiKey {
    key_id: ApiKeyId,
    api_key: String,
    authorization: AllowDecision,
    policy_version: PolicyVersion,
}

impl IssuedApiKey {
    pub(crate) fn new(
        key_id: ApiKeyId,
        api_key: String,
        authorization: AllowDecision,
        policy_version: PolicyVersion,
    ) -> Self {
        Self {
            key_id,
            api_key,
            authorization,
            policy_version,
        }
    }

    /// Borrow the public key identifier.
    #[must_use]
    pub fn key_id(&self) -> &ApiKeyId {
        &self.key_id
    }

    /// Borrow the complete one-time bearer credential.
    #[must_use]
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Return the authoritative base version changed by this issuance.
    #[must_use]
    pub fn authorization(&self) -> &AllowDecision {
        &self.authorization
    }

    /// Return the policy version that first contains the digest.
    #[must_use]
    pub const fn policy_version(&self) -> PolicyVersion {
        self.policy_version
    }
}

impl PolicyGrant {
    pub(crate) fn new(
        principal_id: PrincipalId,
        scope: GrantScope,
        actions: GrantActions,
    ) -> Result<Self, SecurityError> {
        let actions = match actions {
            GrantActions::All => GrantActions::All,
            GrantActions::Selected { mut actions } => {
                if actions.is_empty() {
                    return Err(SecurityError::InvalidPolicyRequest(
                        "grant actions must not be empty".to_string(),
                    ));
                }
                let original_len = actions.len();
                actions.sort_unstable();
                actions.dedup();
                if actions.len() != original_len {
                    return Err(SecurityError::InvalidPolicyRequest(
                        "grant actions must be unique".to_string(),
                    ));
                }
                GrantActions::Selected { actions }
            }
        };
        Ok(Self {
            principal_id,
            scope,
            actions,
        })
    }

    /// Borrow the principal receiving this grant.
    #[must_use]
    pub fn principal_id(&self) -> &PrincipalId {
        &self.principal_id
    }

    /// Borrow the exact global or namespace scope.
    #[must_use]
    pub fn scope(&self) -> &GrantScope {
        &self.scope
    }

    /// Borrow the explicit or frozen-all action grant.
    #[must_use]
    pub fn actions(&self) -> &GrantActions {
        &self.actions
    }
}

/// Small CAS-published pointer to one immutable policy snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyHead {
    version: PolicyVersion,
    object_key: String,
    checksum: String,
}

impl PolicyHead {
    pub(crate) fn new(
        snapshot: &PolicySnapshot,
        object_key: String,
    ) -> Result<Self, SecurityError> {
        let head = Self {
            version: snapshot.version,
            object_key,
            checksum: snapshot.checksum.clone(),
        };
        head.validate("_security")?;
        Ok(head)
    }

    pub(crate) fn validate(&self, root: &str) -> Result<(), SecurityError> {
        if self.version == PolicyVersion::BOOT {
            return Err(SecurityError::InvalidPolicy(
                "policy head version must be nonzero".to_string(),
            ));
        }
        let prefix = format!("{root}/policies/");
        let object_id = self
            .object_key
            .strip_prefix(&prefix)
            .and_then(|suffix| suffix.strip_suffix(".json"));
        if object_id.is_none_or(|object_id| {
            object_id.contains('/') || object_id.parse::<ulid::Ulid>().is_err()
        }) || !valid_checksum(&self.checksum)
        {
            return Err(SecurityError::InvalidPolicy(
                "invalid policy head fields".to_string(),
            ));
        }
        Ok(())
    }

    /// Return the selected monotonic version.
    #[must_use]
    pub const fn version(&self) -> PolicyVersion {
        self.version
    }

    /// Borrow the exact immutable snapshot key.
    #[must_use]
    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    /// Borrow the selected snapshot checksum.
    #[must_use]
    pub fn checksum(&self) -> &str {
        &self.checksum
    }
}

/// Complete immutable policy authority selected by [`PolicyHead`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicySnapshot {
    version: PolicyVersion,
    created_at: DateTime<Utc>,
    created_by: PrincipalId,
    checksum: String,
    principals: Vec<PolicyPrincipal>,
    keys: Vec<PolicyKey>,
    grants: Vec<PolicyGrant>,
}

#[derive(Serialize)]
struct ChecksumContent<'a> {
    version: PolicyVersion,
    created_at: DateTime<Utc>,
    created_by: &'a PrincipalId,
    principals: &'a [PolicyPrincipal],
    keys: &'a [PolicyKey],
    grants: &'a [PolicyGrant],
}

impl PolicySnapshot {
    /// Compile version 1 from validated bootstrap credentials.
    pub(crate) fn from_bootstrap(
        config: &SecurityConfig,
        now: DateTime<Utc>,
    ) -> Result<Self, SecurityError> {
        if config.api_keys.is_empty() {
            return Err(SecurityError::MissingBootstrapCredentials);
        }

        let mut principals = Vec::with_capacity(config.api_keys.len());
        let mut keys = Vec::with_capacity(config.api_keys.len());
        let mut grants = Vec::new();
        for configured in &config.api_keys {
            let (principal, key, mut key_grants) = bootstrap_key(configured, now)?;
            principals.push(principal);
            keys.push(key);
            grants.append(&mut key_grants);
        }
        principals
            .sort_by(|left, right| left.principal_id.as_str().cmp(right.principal_id.as_str()));
        keys.sort_by(|left, right| left.key_id.cmp(&right.key_id));
        grants.sort_by(grant_order);

        let mut snapshot = Self {
            version: PolicyVersion::persisted(1)?,
            created_at: now,
            created_by: PrincipalId::new("system:bootstrap")?,
            checksum: String::new(),
            principals,
            keys,
            grants,
        };
        snapshot.validate_structure()?;
        snapshot.checksum = snapshot.compute_checksum()?;
        Ok(snapshot)
    }

    /// Verify strict invariants and the canonical SHA-256 checksum.
    pub fn verify_checksum(&self) -> Result<(), SecurityError> {
        self.validate_structure()?;
        if !valid_checksum(&self.checksum) || self.compute_checksum()? != self.checksum {
            return Err(SecurityError::PolicyChecksumMismatch);
        }
        Ok(())
    }

    /// Verify integrity and compile every principal, key, and grant invariant.
    ///
    /// Storage/cache code retains the compiled value internally; external
    /// validation and fuzz targets use this seam when only success or failure
    /// matters.
    pub fn validate_for_use(&self) -> Result<(), SecurityError> {
        self.compile().map(|_| ())
    }

    pub(crate) fn compile(&self) -> Result<CompiledPolicy, SecurityError> {
        self.verify_checksum()?;
        let principals = self
            .principals
            .iter()
            .map(|principal| (principal.principal_id.clone(), principal))
            .collect::<HashMap<_, _>>();
        let mut keys = HashMap::with_capacity(self.keys.len());
        for key in &self.keys {
            let principal = principals.get(&key.principal_id).ok_or_else(|| {
                SecurityError::InvalidPolicy(
                    "policy key references an unknown principal".to_string(),
                )
            })?;
            let compiled = CompiledKey {
                principal: Principal::authenticated_api_key(
                    principal.principal_id.clone(),
                    key.key_id.clone(),
                    principal.kind,
                    principal.display_name.clone(),
                    key.expires_at,
                ),
                digest: decode_sha256(&key.sha256_hex)?,
                state: key.state,
                expires_at: key.expires_at,
                revokes_at: key.revokes_at,
            };
            if keys.insert(key.key_id.0.clone(), compiled).is_some() {
                return Err(SecurityError::InvalidPolicy(
                    "duplicate compiled policy key".to_string(),
                ));
            }
        }

        let mut grants: HashMap<PrincipalId, Vec<CompiledGrant>> = HashMap::new();
        for grant in &self.grants {
            let actions = match &grant.actions {
                GrantActions::All => POLICY_ALL_V1.into_iter().collect(),
                GrantActions::Selected { actions } => actions.iter().copied().collect(),
            };
            grants
                .entry(grant.principal_id.clone())
                .or_default()
                .push(CompiledGrant {
                    scope: grant.scope.clone(),
                    actions,
                });
        }
        Ok(CompiledPolicy {
            version: self.version,
            principals: principals.keys().cloned().collect(),
            keys,
            grants,
        })
    }

    pub(crate) fn add_principal(
        &self,
        actor: &PrincipalId,
        now: DateTime<Utc>,
        principal: PolicyPrincipal,
    ) -> Result<Self, SecurityError> {
        if self
            .principals
            .iter()
            .any(|existing| existing.principal_id == principal.principal_id)
        {
            return Err(SecurityError::PolicyEntityAlreadyExists);
        }
        let mut next = self.clone();
        next.principals.push(principal);
        next.principals
            .sort_by(|left, right| left.principal_id.as_str().cmp(right.principal_id.as_str()));
        next.finalize_next(actor, now)?;
        Ok(next)
    }

    pub(crate) fn add_key(
        &self,
        actor: &PrincipalId,
        now: DateTime<Utc>,
        key: PolicyKey,
    ) -> Result<Self, SecurityError> {
        if !self
            .principals
            .iter()
            .any(|principal| principal.principal_id == key.principal_id)
        {
            return Err(SecurityError::PolicyEntityNotFound);
        }
        if self
            .keys
            .iter()
            .any(|existing| existing.key_id == key.key_id)
        {
            return Err(SecurityError::PolicyEntityAlreadyExists);
        }

        let mut next = self.clone();
        next.keys.push(key);
        next.keys
            .sort_by(|left, right| left.key_id.cmp(&right.key_id));
        next.finalize_next(actor, now)?;
        Ok(next)
    }

    pub(crate) fn revoke_key(
        &self,
        actor: &PrincipalId,
        now: DateTime<Utc>,
        key_id: &ApiKeyId,
    ) -> Result<Self, SecurityError> {
        let mut next = self.clone();
        let key = next
            .keys
            .iter_mut()
            .find(|key| &key.key_id == key_id)
            .ok_or(SecurityError::PolicyEntityNotFound)?;
        if key.state == KeyState::Revoked {
            if key.revokes_at.is_some() {
                key.revokes_at = None;
                next.finalize_next(actor, now)?;
                return Ok(next);
            }
            return Err(SecurityError::InvalidPolicyRequest(
                "key is already revoked".to_string(),
            ));
        }
        key.state = KeyState::Revoked;
        key.revokes_at = None;
        next.finalize_next(actor, now)?;
        Ok(next)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn rotate_key(
        &self,
        actor: &PrincipalId,
        now: DateTime<Utc>,
        old_key_id: &ApiKeyId,
        new_key_id: ApiKeyId,
        digest: String,
        overlap_secs: u64,
    ) -> Result<Self, SecurityError> {
        if old_key_id == &new_key_id || self.keys.iter().any(|key| key.key_id == new_key_id) {
            return Err(SecurityError::PolicyEntityAlreadyExists);
        }
        if !valid_checksum(&digest) {
            return Err(SecurityError::InvalidPolicyRequest(
                "key digest must be SHA-256 hex".to_string(),
            ));
        }
        let overlap_secs = i64::try_from(overlap_secs).map_err(|_| {
            SecurityError::InvalidPolicyRequest("rotation overlap is too large".to_string())
        })?;
        let overlap = chrono::Duration::try_seconds(overlap_secs).ok_or_else(|| {
            SecurityError::InvalidPolicyRequest("rotation overlap is too large".to_string())
        })?;
        let revokes_at = now.checked_add_signed(overlap).ok_or_else(|| {
            SecurityError::InvalidPolicyRequest("rotation overlap is too large".to_string())
        })?;
        let old = self
            .keys
            .iter()
            .find(|key| &key.key_id == old_key_id)
            .ok_or(SecurityError::PolicyEntityNotFound)?
            .clone();
        if old.state != KeyState::Active
            || old.expires_at.is_some_and(|expires_at| expires_at <= now)
        {
            return Err(SecurityError::InvalidPolicyRequest(
                "only an active unexpired key may be rotated".to_string(),
            ));
        }

        let mut next = self.clone();
        let predecessor = next
            .keys
            .iter_mut()
            .find(|key| &key.key_id == old_key_id)
            .ok_or(SecurityError::PolicyEntityNotFound)?;
        predecessor.state = KeyState::Revoked;
        predecessor.revokes_at = (overlap_secs > 0).then_some(revokes_at);
        next.keys.push(PolicyKey {
            key_id: new_key_id,
            name: old.name,
            sha256_hex: digest.to_ascii_lowercase(),
            principal_id: old.principal_id,
            state: KeyState::Active,
            expires_at: old.expires_at,
            created_at: now,
            rotated_from: Some(old.key_id),
            revokes_at: None,
        });
        next.keys
            .sort_by(|left, right| left.key_id.cmp(&right.key_id));
        next.finalize_next(actor, now)?;
        Ok(next)
    }

    pub(crate) fn add_grant(
        &self,
        actor: &PrincipalId,
        now: DateTime<Utc>,
        grant: PolicyGrant,
    ) -> Result<Self, SecurityError> {
        if !self
            .principals
            .iter()
            .any(|principal| principal.principal_id == grant.principal_id)
        {
            return Err(SecurityError::PolicyEntityNotFound);
        }
        if self.grants.iter().any(|existing| existing == &grant) {
            return Err(SecurityError::PolicyEntityAlreadyExists);
        }
        let mut next = self.clone();
        next.grants.push(grant);
        next.grants.sort_by(grant_order);
        next.finalize_next(actor, now)?;
        Ok(next)
    }

    pub(crate) fn remove_grant(
        &self,
        actor: &PrincipalId,
        now: DateTime<Utc>,
        grant: &PolicyGrant,
    ) -> Result<Self, SecurityError> {
        let mut next = self.clone();
        let original_len = next.grants.len();
        next.grants.retain(|existing| existing != grant);
        if next.grants.len() == original_len {
            return Err(SecurityError::PolicyEntityNotFound);
        }
        next.finalize_next(actor, now)?;
        Ok(next)
    }

    fn finalize_next(
        &mut self,
        actor: &PrincipalId,
        now: DateTime<Utc>,
    ) -> Result<(), SecurityError> {
        self.version = self.version.checked_next()?;
        self.created_at = now;
        self.created_by = actor.clone();
        self.checksum.clear();
        self.validate_structure()?;
        self.checksum = self.compute_checksum()?;
        Ok(())
    }

    fn validate_structure(&self) -> Result<(), SecurityError> {
        if self.version == PolicyVersion::BOOT {
            return Err(SecurityError::InvalidPolicy(
                "persisted policy version must be nonzero".to_string(),
            ));
        }
        PrincipalId::new(self.created_by.as_str())?;

        let mut principal_ids = BTreeSet::new();
        for principal in &self.principals {
            PrincipalId::new(principal.principal_id.as_str())?;
            if principal.display_name.trim().is_empty()
                || !principal_ids.insert(principal.principal_id.as_str())
            {
                return Err(SecurityError::InvalidPolicy(
                    "invalid or duplicate policy principal".to_string(),
                ));
            }
        }

        let mut key_ids = BTreeSet::new();
        for key in &self.keys {
            ApiKeyId::new(key.key_id.as_str())?;
            PrincipalId::new(key.principal_id.as_str())?;
            if key.name.trim().is_empty()
                || !valid_checksum(&key.sha256_hex)
                || !key_ids.insert(key.key_id.as_str())
                || !principal_ids.contains(key.principal_id.as_str())
            {
                return Err(SecurityError::InvalidPolicy(
                    "invalid or duplicate policy key".to_string(),
                ));
            }
            if key.state != KeyState::Revoked && key.revokes_at.is_some() {
                return Err(SecurityError::InvalidPolicy(
                    "only revoked keys may carry revokes_at".to_string(),
                ));
            }
        }

        let keys_by_id = self
            .keys
            .iter()
            .map(|key| (key.key_id.as_str(), key))
            .collect::<BTreeMap<_, _>>();
        for key in &self.keys {
            let Some(rotated_from) = &key.rotated_from else {
                continue;
            };
            let predecessor = keys_by_id.get(rotated_from.as_str()).ok_or_else(|| {
                SecurityError::InvalidPolicy(
                    "rotated key references an unknown predecessor".to_string(),
                )
            })?;
            if rotated_from == &key.key_id
                || predecessor.principal_id != key.principal_id
                || predecessor.created_at > key.created_at
            {
                return Err(SecurityError::InvalidPolicy(
                    "invalid rotated-key lineage".to_string(),
                ));
            }
            let mut visited = BTreeSet::new();
            let mut cursor = Some(key);
            while let Some(current) = cursor {
                if !visited.insert(current.key_id.as_str()) {
                    return Err(SecurityError::InvalidPolicy(
                        "rotated-key lineage contains a cycle".to_string(),
                    ));
                }
                cursor = current
                    .rotated_from
                    .as_ref()
                    .and_then(|parent| keys_by_id.get(parent.as_str()).copied());
            }
        }

        let mut grant_ids = BTreeSet::new();
        for grant in &self.grants {
            PrincipalId::new(grant.principal_id.as_str())?;
            if !principal_ids.contains(grant.principal_id.as_str()) {
                return Err(SecurityError::InvalidPolicy(
                    "grant references an unknown principal".to_string(),
                ));
            }
            if let GrantScope::Namespace { namespace } = &grant.scope {
                NamespaceId::new(namespace.as_str().to_string())?;
            }
            if let GrantActions::Selected { actions } = &grant.actions {
                let unique = actions.iter().copied().collect::<BTreeSet<_>>();
                if actions.is_empty() || unique.len() != actions.len() {
                    return Err(SecurityError::InvalidPolicy(
                        "explicit grant actions must be nonempty and unique".to_string(),
                    ));
                }
            }
            let identity = serde_json::to_string(grant).map_err(|error| {
                SecurityError::InvalidPolicy(format!("grant serialization failed: {error}"))
            })?;
            if !grant_ids.insert(identity) {
                return Err(SecurityError::InvalidPolicy(
                    "duplicate policy grant".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn compute_checksum(&self) -> Result<String, SecurityError> {
        let content = ChecksumContent {
            version: self.version,
            created_at: self.created_at,
            created_by: &self.created_by,
            principals: &self.principals,
            keys: &self.keys,
            grants: &self.grants,
        };
        let value = serde_json::to_value(content).map_err(|error| {
            SecurityError::InvalidPolicy(format!("policy canonicalization failed: {error}"))
        })?;
        let mut canonical = String::new();
        write_canonical_json(&value, &mut canonical)?;
        let digest = Sha256::digest(canonical.as_bytes());
        let mut encoded = String::with_capacity(SHA256_HEX_LEN);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in digest {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        Ok(encoded)
    }

    /// Return the monotonic snapshot version.
    #[must_use]
    pub const fn version(&self) -> PolicyVersion {
        self.version
    }

    /// Return the wall-clock instant recorded when this version was published.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Borrow the principal that published this version.
    #[must_use]
    pub fn created_by(&self) -> &PrincipalId {
        &self.created_by
    }

    /// Borrow the canonical checksum.
    #[must_use]
    pub fn checksum(&self) -> &str {
        &self.checksum
    }

    /// Borrow every stable principal record.
    #[must_use]
    pub fn principals(&self) -> &[PolicyPrincipal] {
        &self.principals
    }

    /// Borrow every hashed credential record.
    #[must_use]
    pub fn keys(&self) -> &[PolicyKey] {
        &self.keys
    }

    /// Borrow every typed grant record.
    #[must_use]
    pub fn grants(&self) -> &[PolicyGrant] {
        &self.grants
    }
}

fn bootstrap_key(
    configured: &ApiKeyConfig,
    now: DateTime<Utc>,
) -> Result<(PolicyPrincipal, PolicyKey, Vec<PolicyGrant>), SecurityError> {
    let key_id = ApiKeyId::new(configured.key_id.clone())?;
    let principal_id = PrincipalId::new(configured.key_id.clone())?;
    let principal = PolicyPrincipal {
        principal_id: principal_id.clone(),
        kind: PrincipalKind::Service,
        display_name: configured.name.clone(),
    };
    let key = PolicyKey {
        key_id,
        name: configured.name.clone(),
        sha256_hex: configured.sha256_hex.to_ascii_lowercase(),
        principal_id: principal_id.clone(),
        state: if configured
            .expires_at
            .is_some_and(|expires_at| expires_at <= now)
        {
            KeyState::Expired
        } else {
            KeyState::Active
        },
        expires_at: configured.expires_at,
        created_at: now,
        rotated_from: None,
        revokes_at: None,
    };

    let actions = if configured.actions.iter().any(|action| action == "*") {
        GrantActions::All
    } else {
        let mut actions = configured
            .actions
            .iter()
            .map(|action| action.parse())
            .collect::<Result<Vec<Action>, _>>()?;
        actions.sort_unstable();
        actions.dedup();
        GrantActions::Selected { actions }
    };
    let scopes = if configured
        .namespaces
        .iter()
        .any(|namespace| namespace == "*")
    {
        vec![GrantScope::Global]
    } else {
        configured
            .namespaces
            .iter()
            .map(|namespace| {
                NamespaceId::new(namespace.clone())
                    .map(|namespace| GrantScope::Namespace { namespace })
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let grants = scopes
        .into_iter()
        .map(|scope| PolicyGrant {
            principal_id: principal_id.clone(),
            scope,
            actions: actions.clone(),
        })
        .collect();
    Ok((principal, key, grants))
}

fn grant_order(left: &PolicyGrant, right: &PolicyGrant) -> std::cmp::Ordering {
    left.principal_id
        .as_str()
        .cmp(right.principal_id.as_str())
        .then_with(|| match (&left.scope, &right.scope) {
            (GrantScope::Global, GrantScope::Global) => std::cmp::Ordering::Equal,
            (GrantScope::Global, GrantScope::Namespace { .. }) => std::cmp::Ordering::Less,
            (GrantScope::Namespace { .. }, GrantScope::Global) => std::cmp::Ordering::Greater,
            (
                GrantScope::Namespace {
                    namespace: left_namespace,
                },
                GrantScope::Namespace {
                    namespace: right_namespace,
                },
            ) => left_namespace.as_str().cmp(right_namespace.as_str()),
        })
        .then_with(|| match (&left.actions, &right.actions) {
            (GrantActions::All, GrantActions::All) => std::cmp::Ordering::Equal,
            (GrantActions::All, GrantActions::Selected { .. }) => std::cmp::Ordering::Less,
            (GrantActions::Selected { .. }, GrantActions::All) => std::cmp::Ordering::Greater,
            (
                GrantActions::Selected {
                    actions: left_actions,
                },
                GrantActions::Selected {
                    actions: right_actions,
                },
            ) => left_actions.cmp(right_actions),
        })
}

fn valid_checksum(value: &str) -> bool {
    value.len() == SHA256_HEX_LEN && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn decode_sha256(value: &str) -> Result<[u8; 32], SecurityError> {
    if !valid_checksum(value) {
        return Err(SecurityError::InvalidPolicy(
            "invalid SHA-256 digest".to_string(),
        ));
    }
    let mut decoded = [0_u8; 32];
    for (index, byte) in decoded.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16).map_err(|error| {
            SecurityError::InvalidPolicy(format!("invalid SHA-256 digest: {error}"))
        })?;
    }
    Ok(decoded)
}

fn write_canonical_json(value: &Value, output: &mut String) -> Result<(), SecurityError> {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => output.push_str(&value.to_string()),
        Value::String(value) => {
            output.push_str(&serde_json::to_string(value).map_err(|error| {
                SecurityError::InvalidPolicy(format!("policy string encoding failed: {error}"))
            })?)
        }
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_canonical_json(value, output)?;
            }
            output.push(']');
        }
        Value::Object(values) => {
            output.push('{');
            let ordered = values.iter().collect::<BTreeMap<_, _>>();
            for (index, (key, value)) in ordered.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key).map_err(|error| {
                    SecurityError::InvalidPolicy(format!(
                        "policy object-key encoding failed: {error}"
                    ))
                })?);
                output.push(':');
                write_canonical_json(value, output)?;
            }
            output.push('}');
        }
    }
    Ok(())
}
