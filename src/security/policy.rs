//! Strict persisted vocabulary for S3-authoritative security policy.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::{ApiKeyConfig, SecurityConfig};
use crate::namespace::BranchId;

use super::{
    Action, AllowDecision, DenyReason, FieldMask, NamespaceId, PendingBranchActivation,
    PolicyControlRevision, PolicyHeadDigest, PolicyLeaseFencingToken, PolicyVersion, Principal,
    PrincipalId, PrincipalKind, Resource, SecurityError, WriteConstraints,
};

const SHA256_HEX_LEN: usize = 64;
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
    mandatory_filter: Option<crate::types::Filter>,
    field_mask: Option<FieldMask>,
    write_constraints: WriteConstraints,
    require_approval: HashSet<Action>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CompiledAuthorization {
    pub(crate) mandatory_filter: Option<crate::types::Filter>,
    pub(crate) field_mask: Option<FieldMask>,
    pub(crate) write_constraints: WriteConstraints,
    pub(crate) attribute_admin: bool,
    pub(crate) require_approval: bool,
}

#[derive(Debug, Default)]
struct CompiledNamespaceScope {
    filter_conjuncts: BTreeSet<String>,
    denied_fields: BTreeSet<String>,
    write_constraints: WriteConstraints,
}

/// Namespace-scoped capabilities whose target availability can expose, mutate,
/// or destroy a raw clone. `NamespaceCreate` and `NamespaceClone` are checked as
/// the clone operation's explicit control permissions; process-wide actions do
/// not depend on the source or target namespace.
pub(crate) const DERIVED_NAMESPACE_ACTIONS: [Action; 14] = [
    Action::NamespaceRead,
    Action::NamespaceDelete,
    Action::SnapshotRead,
    Action::SnapshotWrite,
    Action::SnapshotDelete,
    Action::IndexConfigWrite,
    Action::CompactionTrigger,
    Action::CompactionStatusRead,
    Action::HydrationTrigger,
    Action::VectorFetch,
    Action::VectorUpsert,
    Action::VectorDelete,
    Action::Query,
    Action::AttributeAdmin,
];

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

    /// Prove that publishing a raw namespace copy cannot broaden any principal's
    /// current namespace-scoped observation or mutation authority.
    ///
    /// The proof is deliberately conservative. A target predicate is accepted
    /// only when every syntactic source conjunct is retained; predicates that
    /// are semantically equivalent but not structurally provable fail closed.
    pub(crate) fn validate_namespace_copy_no_widening(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<(), SecurityError> {
        for grants in self.grants.values() {
            for action in DERIVED_NAMESPACE_ACTIONS {
                let source_scope = compiled_namespace_scope(grants, action, source)?;
                let target_scope = compiled_namespace_scope(grants, action, target)?;
                let Some(target_scope) = target_scope else {
                    continue;
                };
                let Some(source_scope) = source_scope else {
                    return Err(SecurityError::ConstraintViolation);
                };
                if !source_scope
                    .filter_conjuncts
                    .is_subset(&target_scope.filter_conjuncts)
                    || !source_scope
                        .denied_fields
                        .is_subset(&target_scope.denied_fields)
                    || !source_scope
                        .write_constraints
                        .stamp()
                        .iter()
                        .all(|(field, value)| {
                            target_scope.write_constraints.stamp().get(field) == Some(value)
                        })
                    || !source_scope
                        .write_constraints
                        .forbidden_fields()
                        .is_subset(target_scope.write_constraints.forbidden_fields())
                {
                    return Err(SecurityError::ConstraintViolation);
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub(crate) fn key(&self, key_id: &str) -> Option<&CompiledKey> {
        self.keys.get(key_id)
    }

    #[must_use]
    pub(crate) fn credential_is_active(&self, principal: &Principal, now: DateTime<Utc>) -> bool {
        let Some(key_id) = principal.api_key_id.as_ref() else {
            return false;
        };
        self.delegated_parent_credential_is_active(&principal.id, key_id, now)
    }

    #[must_use]
    pub(crate) fn delegated_parent_credential_is_active(
        &self,
        parent_principal: &PrincipalId,
        key_id: &ApiKeyId,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(key) = self.keys.get(key_id.as_str()) else {
            return false;
        };
        key.principal.id == *parent_principal
            && self.principals.contains(parent_principal)
            && !(key.state == KeyState::Revoked
                && key.revokes_at.is_none_or(|revokes_at| revokes_at <= now))
            && key.state != KeyState::Expired
            && key.expires_at.is_none_or(|expires_at| expires_at > now)
    }

    pub(crate) fn authorize(
        &self,
        principal: &Principal,
        now: DateTime<Utc>,
        action: Action,
        resource: &Resource,
    ) -> Result<CompiledAuthorization, DenyReason> {
        let grants = self.active_grants(principal, now)?;
        Self::authorize_grants(grants, action, resource)
    }

    pub(crate) fn authorize_delegated_parent(
        &self,
        parent_principal: &PrincipalId,
        action: Action,
        resource: &Resource,
    ) -> Result<CompiledAuthorization, DenyReason> {
        if !self.principals.contains(parent_principal) {
            return Err(DenyReason::CredentialUnknown);
        }
        let grants = self
            .grants
            .get(parent_principal)
            .map(Vec::as_slice)
            .ok_or(DenyReason::ActionNotGranted)?;
        Self::authorize_grants(grants, action, resource)
    }

    pub(crate) fn authorize_delegated_parent_action(
        &self,
        parent_principal: &PrincipalId,
        action: Action,
    ) -> Result<(), DenyReason> {
        if !self.principals.contains(parent_principal) {
            return Err(DenyReason::CredentialUnknown);
        }
        let grants = self
            .grants
            .get(parent_principal)
            .map(Vec::as_slice)
            .ok_or(DenyReason::ActionNotGranted)?;
        if grants.iter().any(|grant| grant.actions.contains(&action)) {
            Ok(())
        } else {
            Err(DenyReason::ActionNotGranted)
        }
    }

    fn authorize_grants(
        grants: &[CompiledGrant],
        action: Action,
        resource: &Resource,
    ) -> Result<CompiledAuthorization, DenyReason> {
        let matching_action = grants
            .iter()
            .filter(|grant| grant.actions.contains(&action))
            .collect::<Vec<_>>();
        if matching_action.is_empty() {
            return Err(DenyReason::ActionNotGranted);
        }
        let applicable = matching_action
            .iter()
            .copied()
            .filter(|grant| grant_scope_matches(&grant.scope, resource))
            .collect::<Vec<_>>();
        if applicable.is_empty() {
            return if resource.namespace().is_some() {
                Err(DenyReason::NamespaceNotGranted)
            } else {
                Err(DenyReason::ActionNotGranted)
            };
        }

        let mut filters = Vec::new();
        let mut field_mask = FieldMask::default();
        let mut has_field_mask = false;
        let mut write_constraints = WriteConstraints::none();
        let require_approval = applicable
            .iter()
            .any(|grant| grant.require_approval.contains(&action));
        for grant in applicable {
            if let Some(filter) = &grant.mandatory_filter {
                filters.push(filter.clone());
            }
            if let Some(mask) = &grant.field_mask {
                field_mask.union_from(mask);
                has_field_mask = true;
            }
            write_constraints
                .merge_from(&grant.write_constraints)
                .unwrap_or_else(|error| {
                    panic!("validated compiled policy carried conflicting stamps: {error}")
                });
        }
        let mandatory_filter = match filters.len() {
            0 => None,
            1 => filters.pop(),
            _ => Some(crate::types::Filter::And { filters }),
        };
        let field_mask = has_field_mask.then_some(field_mask);
        let has_attribute_admin = grants.iter().any(|grant| {
            grant.actions.contains(&Action::AttributeAdmin)
                && grant_scope_matches(&grant.scope, resource)
        });
        if has_attribute_admin {
            write_constraints = write_constraints.with_forbid_set_bypassed();
        }

        Ok(CompiledAuthorization {
            mandatory_filter,
            field_mask,
            write_constraints,
            attribute_admin: has_attribute_admin,
            require_approval,
        })
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

fn compiled_namespace_scope(
    grants: &[CompiledGrant],
    action: Action,
    namespace: &NamespaceId,
) -> Result<Option<CompiledNamespaceScope>, SecurityError> {
    let resource = Resource::Namespace(namespace.clone());
    let applicable = grants.iter().filter(|grant| {
        grant.actions.contains(&action) && grant_scope_matches(&grant.scope, &resource)
    });
    let mut matched = false;
    let mut scope = CompiledNamespaceScope::default();
    for grant in applicable {
        matched = true;
        if let Some(filter) = &grant.mandatory_filter {
            collect_filter_conjuncts(filter, &mut scope.filter_conjuncts)?;
        }
        if let Some(mask) = &grant.field_mask {
            scope
                .denied_fields
                .extend(mask.denied_fields().iter().cloned());
        }
        scope
            .write_constraints
            .merge_from(&grant.write_constraints)
            .unwrap_or_else(|error| {
                panic!("validated compiled policy carried conflicting stamps: {error}")
            });
    }
    if grants.iter().any(|grant| {
        grant.actions.contains(&Action::AttributeAdmin)
            && grant_scope_matches(&grant.scope, &resource)
    }) {
        scope.write_constraints = scope.write_constraints.with_forbid_set_bypassed();
    }
    Ok(matched.then_some(scope))
}

fn collect_filter_conjuncts(
    filter: &crate::types::Filter,
    output: &mut BTreeSet<String>,
) -> Result<(), SecurityError> {
    if let crate::types::Filter::And { filters } = filter {
        for filter in filters {
            collect_filter_conjuncts(filter, output)?;
        }
        return Ok(());
    }
    let fingerprint = serde_json::to_string(filter).map_err(|error| {
        SecurityError::InvalidPolicy(format!(
            "mandatory filter fingerprint serialization failed: {error}"
        ))
    })?;
    output.insert(fingerprint);
    Ok(())
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
    /// Every action in the frozen Phase 3 inventory, including destructive actions.
    ///
    /// Later capabilities such as `AttributeAdmin` require an explicit selected
    /// grant so an immutable wildcard cannot silently widen after publication.
    All,
    /// A nonempty explicit set of action variants.
    Selected {
        /// Sorted, unique actions granted at this scope.
        actions: Vec<Action>,
    },
}

/// Complete caller-supplied definition for one grant publication.
///
/// The definition keeps transport parsing separate from policy validation while
/// carrying all grant fields through the kernel and cache as one typed value.
/// Validation still occurs inside the S3-authoritative publication path, after
/// the kernel has selected policy authority.
#[derive(Debug, Clone)]
pub struct GrantDefinition {
    principal_id: PrincipalId,
    scope: GrantScope,
    actions: GrantActions,
    mandatory_filter: Option<crate::types::Filter>,
    field_mask: Option<FieldMask>,
    write_constraints: WriteConstraints,
    require_approval: Vec<Action>,
}

impl GrantDefinition {
    /// Collect one parsed grant request without publishing or validating it.
    #[must_use]
    pub fn new(
        principal_id: PrincipalId,
        scope: GrantScope,
        actions: GrantActions,
        mandatory_filter: Option<crate::types::Filter>,
        field_mask: Option<FieldMask>,
        write_constraints: WriteConstraints,
        require_approval: Vec<Action>,
    ) -> Self {
        Self {
            principal_id,
            scope,
            actions,
            mandatory_filter,
            field_mask,
            write_constraints,
            require_approval,
        }
    }

    pub(crate) fn into_policy_grant(self) -> Result<PolicyGrant, SecurityError> {
        let write_constraints =
            (!self.write_constraints.is_empty()).then_some(self.write_constraints);
        PolicyGrant::new_constrained(
            self.principal_id,
            self.scope,
            self.actions,
            self.mandatory_filter,
            self.field_mask,
            write_constraints,
            self.require_approval,
        )
    }
}

/// One independently evaluated principal/scope/action binding.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyGrant {
    principal_id: PrincipalId,
    scope: GrantScope,
    actions: GrantActions,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mandatory_filter: Option<crate::types::Filter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    field_mask: Option<FieldMask>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    write_constraints: Option<WriteConstraints>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    require_approval: Vec<Action>,
}

impl PartialEq for PolicyGrant {
    fn eq(&self, other: &Self) -> bool {
        self.same_binding(other)
            && filters_equal(
                self.mandatory_filter.as_ref(),
                other.mandatory_filter.as_ref(),
            )
            && self.field_mask == other.field_mask
            && self.write_constraints == other.write_constraints
            && self.require_approval == other.require_approval
    }
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
        Self::new_constrained(principal_id, scope, actions, None, None, None, Vec::new())
    }

    pub(crate) fn new_constrained(
        principal_id: PrincipalId,
        scope: GrantScope,
        actions: GrantActions,
        mandatory_filter: Option<crate::types::Filter>,
        field_mask: Option<FieldMask>,
        write_constraints: Option<WriteConstraints>,
        mut require_approval: Vec<Action>,
    ) -> Result<Self, SecurityError> {
        let actions = match actions {
            GrantActions::All => GrantActions::Selected {
                actions: Action::POLICY_SAFE_ALL_V2.to_vec(),
            },
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
        require_approval.sort_unstable();
        let original_approval_len = require_approval.len();
        require_approval.dedup();
        if require_approval.len() != original_approval_len {
            return Err(SecurityError::InvalidPolicyRequest(
                "require_approval actions must be unique".to_string(),
            ));
        }
        let grant = Self {
            principal_id,
            scope,
            actions,
            mandatory_filter,
            field_mask,
            write_constraints,
            require_approval,
        };
        grant
            .validate_constraints()
            .map_err(SecurityError::InvalidPolicyRequest)?;
        Ok(grant)
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

    /// Borrow destructive actions that require a second authorized principal.
    #[must_use]
    pub fn require_approval(&self) -> &[Action] {
        &self.require_approval
    }

    /// Borrow the server-owned predicate contributed by this grant.
    #[must_use]
    pub fn mandatory_filter(&self) -> Option<&crate::types::Filter> {
        self.mandatory_filter.as_ref()
    }

    /// Borrow the response-field restrictions contributed by this grant.
    #[must_use]
    pub fn field_mask(&self) -> Option<&FieldMask> {
        self.field_mask.as_ref()
    }

    /// Borrow the write restrictions contributed by this grant.
    #[must_use]
    pub fn write_constraints(&self) -> Option<&WriteConstraints> {
        self.write_constraints.as_ref()
    }

    fn same_binding(&self, other: &Self) -> bool {
        self.principal_id == other.principal_id
            && self.scope == other.scope
            && self.actions == other.actions
    }

    fn validate_constraints(&self) -> Result<(), String> {
        if !self
            .require_approval
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err("require_approval actions must be sorted and unique".to_string());
        }
        let granted_actions = match &self.actions {
            GrantActions::All => Action::POLICY_ALL_V1.into_iter().collect::<HashSet<_>>(),
            GrantActions::Selected { actions } => actions.iter().copied().collect(),
        };
        if self
            .require_approval
            .iter()
            .any(|action| !action.is_destructive() || !granted_actions.contains(action))
        {
            return Err(
                "require_approval must name destructive actions in the same grant".to_string(),
            );
        }
        if let Some(filter) = &self.mandatory_filter {
            validate_policy_filter(filter)?;
        }
        if self.field_mask.as_ref().is_some_and(FieldMask::is_empty) {
            return Err("field_mask.deny must not be empty".to_string());
        }
        if self
            .write_constraints
            .as_ref()
            .is_some_and(WriteConstraints::is_empty)
        {
            return Err("write_constraints must not be empty".to_string());
        }
        if let GrantActions::Selected { actions } = &self.actions {
            let constrained_attribute_admin = actions.contains(&Action::AttributeAdmin)
                && !actions.contains(&Action::VectorUpsert)
                && (self.mandatory_filter.is_some()
                    || self.field_mask.is_some()
                    || self
                        .write_constraints
                        .as_ref()
                        .is_some_and(|constraints| !constraints.is_empty()));
            if constrained_attribute_admin {
                return Err(
                    "AttributeAdmin grants carrying constraints must also include VectorUpsert"
                        .to_string(),
                );
            }
        }
        Ok(())
    }
}

/// Small CAS-published pointer to one immutable policy snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyHead {
    version: PolicyVersion,
    object_key: String,
    checksum: String,
    #[serde(default)]
    control_revision: PolicyControlRevision,
    #[serde(default)]
    publication_fencing_token: Option<PolicyLeaseFencingToken>,
    #[serde(default)]
    pending_branch_activations: BTreeMap<BranchId, PendingBranchActivation>,
}

#[derive(Serialize)]
struct SemanticPolicyHead<'a> {
    version: PolicyVersion,
    object_key: &'a str,
    checksum: &'a str,
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
            control_revision: PolicyControlRevision::INITIAL,
            publication_fencing_token: None,
            pending_branch_activations: BTreeMap::new(),
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
        if self.pending_branch_activations.len() > super::MAX_PENDING_BRANCH_ACTIVATIONS {
            return Err(SecurityError::InvalidPolicy(
                "policy head has too many pending branch activations".to_string(),
            ));
        }
        if (self.control_revision == PolicyControlRevision::INITIAL)
            != self.publication_fencing_token.is_none()
        {
            return Err(SecurityError::InvalidPolicy(
                "policy head control revision and publication fence disagree".to_string(),
            ));
        }
        let semantic_digest = self.semantic_digest()?;
        let mut target_identities = BTreeSet::new();
        let mut activation_nonces = BTreeSet::new();
        for (branch_id, guard) in &self.pending_branch_activations {
            guard.validate()?;
            if *branch_id != guard.branch_id()
                || guard.policy_version() != self.version
                || guard.policy_head_digest() != semantic_digest
                || !target_identities.insert((
                    guard.target_namespace().clone(),
                    guard.target_incarnation().clone(),
                ))
                || !activation_nonces.insert(guard.activation_nonce())
            {
                return Err(SecurityError::InvalidPolicy(
                    "pending branch activation disagrees with policy head".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Canonical digest of semantic head identity, excluding mutable controls.
    pub fn semantic_digest(&self) -> Result<PolicyHeadDigest, SecurityError> {
        let canonical = serde_json::to_vec(&SemanticPolicyHead {
            version: self.version,
            object_key: &self.object_key,
            checksum: &self.checksum,
        })
        .map_err(|error| {
            SecurityError::InvalidPolicy(format!("semantic policy head encoding failed: {error}"))
        })?;
        Ok(PolicyHeadDigest::new(Sha256::digest(canonical).into()))
    }

    pub(crate) fn claim_publication(
        &self,
        fencing_token: PolicyLeaseFencingToken,
    ) -> Result<Self, SecurityError> {
        let mut claimed = self.clone();
        claimed.control_revision = claimed.control_revision.next()?;
        claimed.publication_fencing_token = Some(fencing_token);
        claimed.validate("_security")?;
        Ok(claimed)
    }

    pub(crate) fn advance_semantic(
        &self,
        snapshot: &PolicySnapshot,
        object_key: String,
        fencing_token: PolicyLeaseFencingToken,
    ) -> Result<Self, SecurityError> {
        let expected_version = self.version.get().checked_add(1);
        if self.publication_fencing_token != Some(fencing_token)
            || !self.pending_branch_activations.is_empty()
            || expected_version != Some(snapshot.version.get())
        {
            return Err(SecurityError::PolicyConflict);
        }
        let head = Self {
            version: snapshot.version,
            object_key,
            checksum: snapshot.checksum.clone(),
            control_revision: self.control_revision.next()?,
            publication_fencing_token: Some(fencing_token),
            pending_branch_activations: BTreeMap::new(),
        };
        head.validate("_security")?;
        Ok(head)
    }

    pub(crate) fn insert_pending_branch_activation(
        &self,
        guard: PendingBranchActivation,
        fencing_token: PolicyLeaseFencingToken,
    ) -> Result<Self, SecurityError> {
        if self.publication_fencing_token != Some(fencing_token)
            || !self.pending_branch_activations.is_empty()
            || guard.policy_version() != self.version
            || guard.policy_head_digest() != self.semantic_digest()?
            || guard.lease_fencing_token() != fencing_token
        {
            return Err(SecurityError::PolicyConflict);
        }
        let mut guarded = self.clone();
        guarded.control_revision = guarded.control_revision.next()?;
        if guarded
            .pending_branch_activations
            .insert(guard.branch_id(), guard)
            .is_some()
        {
            return Err(SecurityError::PolicyConflict);
        }
        guarded.validate("_security")?;
        Ok(guarded)
    }

    pub(crate) fn remove_pending_branch_activation(
        &self,
        expected: &PendingBranchActivation,
        fencing_token: PolicyLeaseFencingToken,
    ) -> Result<Self, SecurityError> {
        if self.publication_fencing_token != Some(fencing_token)
            || self.pending_branch_activations.get(&expected.branch_id()) != Some(expected)
        {
            return Err(SecurityError::PolicyConflict);
        }
        let mut resolved = self.clone();
        resolved.control_revision = resolved.control_revision.next()?;
        resolved
            .pending_branch_activations
            .remove(&expected.branch_id());
        resolved.validate("_security")?;
        Ok(resolved)
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

    /// Return the non-semantic head control revision.
    #[must_use]
    pub const fn control_revision(&self) -> PolicyControlRevision {
        self.control_revision
    }

    /// Return the lease generation most recently claimed into this head.
    #[must_use]
    pub const fn publication_fencing_token(&self) -> Option<PolicyLeaseFencingToken> {
        self.publication_fencing_token
    }

    /// Borrow the bounded crash-recovery activation guards.
    #[must_use]
    pub fn pending_branch_activations(&self) -> &BTreeMap<BranchId, PendingBranchActivation> {
        &self.pending_branch_activations
    }
}

/// Complete immutable policy authority selected by [`PolicyHead`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        include_preservation: bool,
    ) -> Result<Self, SecurityError> {
        if config.api_keys.is_empty() {
            return Err(SecurityError::MissingBootstrapCredentials);
        }

        let mut principals = Vec::with_capacity(config.api_keys.len());
        let mut keys = Vec::with_capacity(config.api_keys.len());
        let mut grants = Vec::new();
        for configured in &config.api_keys {
            let (principal, key, mut key_grants) =
                bootstrap_key(configured, now, include_preservation)?;
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

    #[must_use]
    pub(crate) fn requires_phase_seven_all_migration(&self) -> bool {
        self.grants
            .iter()
            .any(|grant| matches!(grant.actions, GrantActions::All))
    }

    pub(crate) fn migrate_phase_seven_all(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Self, SecurityError> {
        self.verify_checksum()?;
        if !self.requires_phase_seven_all_migration() {
            return Ok(self.clone());
        }

        let mut grants = Vec::with_capacity(self.grants.len().saturating_mul(2));
        for grant in &self.grants {
            if !matches!(grant.actions, GrantActions::All) {
                grants.push(grant.clone());
                continue;
            }

            let mut safe = grant.clone();
            safe.actions = GrantActions::Selected {
                actions: Action::POLICY_SAFE_ALL_V2.to_vec(),
            };
            safe.require_approval
                .retain(|action| Action::POLICY_SAFE_ALL_V2.contains(action));
            grants.push(safe);

            if matches!(grant.scope, GrantScope::Global) {
                let mut security_admin = grant.clone();
                security_admin.actions = GrantActions::Selected {
                    actions: vec![Action::SecurityAdminWrite],
                };
                security_admin
                    .require_approval
                    .retain(|action| *action == Action::SecurityAdminWrite);
                grants.push(security_admin);
            }
        }
        grants.sort_by(grant_order);
        let mut deduplicated: Vec<PolicyGrant> = Vec::with_capacity(grants.len());
        for grant in grants {
            if let Some(existing) = deduplicated
                .iter_mut()
                .find(|existing| existing.same_binding(&grant))
            {
                if existing != &grant {
                    merge_migrated_grant_constraints(existing, grant)?;
                }
                continue;
            }
            deduplicated.push(grant);
        }

        let mut migrated = Self {
            version: self.version.checked_next()?,
            created_at: now,
            created_by: PrincipalId::new("system:phase7-all-migration")?,
            checksum: String::new(),
            principals: self.principals.clone(),
            keys: self.keys.clone(),
            grants: deduplicated,
        };
        migrated.validate_structure()?;
        migrated.checksum = migrated.compute_checksum()?;
        Ok(migrated)
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
                GrantActions::All => {
                    return Err(SecurityError::InvalidPolicy(
                        "unmigrated legacy All grant cannot be compiled".to_string(),
                    ));
                }
                GrantActions::Selected { actions } => actions.iter().copied().collect(),
            };
            grants
                .entry(grant.principal_id.clone())
                .or_default()
                .push(CompiledGrant {
                    scope: grant.scope.clone(),
                    actions,
                    mandatory_filter: grant.mandatory_filter.clone(),
                    field_mask: grant.field_mask.clone(),
                    write_constraints: grant
                        .write_constraints
                        .clone()
                        .unwrap_or_else(WriteConstraints::none),
                    require_approval: grant.require_approval.iter().copied().collect(),
                });
        }
        validate_compiled_grant_conflicts(&grants)?;
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
        let principal = self
            .principals
            .iter()
            .find(|principal| principal.principal_id == grant.principal_id)
            .ok_or(SecurityError::PolicyEntityNotFound)?;
        if matches!(
            &grant.actions,
            GrantActions::Selected { actions } if actions.contains(&Action::CredentialDelegate)
        ) && !matches!(
            principal.kind,
            PrincipalKind::Human | PrincipalKind::Service
        ) {
            return Err(SecurityError::InvalidPolicyRequest(
                "CredentialDelegate grants require a human or service principal".to_string(),
            ));
        }
        if self
            .grants
            .iter()
            .any(|existing| existing.same_binding(&grant))
        {
            return Err(SecurityError::PolicyEntityAlreadyExists);
        }
        let mut next = self.clone();
        next.grants.push(grant);
        next.grants.sort_by(grant_order);
        next.finalize_next(actor, now)?;
        if let Err(error) = next.validate_for_use() {
            return match error {
                SecurityError::InvalidPolicy(message)
                    if message.starts_with("conflicting server stamps for attribute ") =>
                {
                    Err(SecurityError::InvalidPolicyRequest(message))
                }
                other => Err(other),
            };
        }
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
        next.grants.retain(|existing| !existing.same_binding(grant));
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
        let principal_kinds = self
            .principals
            .iter()
            .map(|principal| (principal.principal_id.as_str(), principal.kind))
            .collect::<BTreeMap<_, _>>();

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
                if actions.contains(&Action::CredentialDelegate)
                    && !matches!(
                        principal_kinds.get(grant.principal_id.as_str()),
                        Some(PrincipalKind::Human | PrincipalKind::Service)
                    )
                {
                    return Err(SecurityError::InvalidPolicy(
                        "CredentialDelegate grants require a human or service principal"
                            .to_string(),
                    ));
                }
            }
            grant
                .validate_constraints()
                .map_err(SecurityError::InvalidPolicy)?;
            let identity =
                serde_json::to_string(&(&grant.principal_id, &grant.scope, &grant.actions))
                    .map_err(|error| {
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
        canonical_policy_checksum(&value)
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

    /// Return whether any policy binding carries server-owned data constraints.
    #[must_use]
    pub fn has_constraints(&self) -> bool {
        self.grants.iter().any(|grant| {
            grant.mandatory_filter().is_some()
                || grant.field_mask().is_some()
                || grant
                    .write_constraints()
                    .is_some_and(|constraints| !constraints.is_empty())
        })
    }

    /// Return whether this snapshot activates Phase 7 delegation or approval policy.
    #[must_use]
    pub fn has_delegation_features(&self) -> bool {
        self.grants.iter().any(|grant| {
            !grant.require_approval.is_empty()
                || matches!(
                    &grant.actions,
                    GrantActions::Selected { actions }
                        if actions.contains(&Action::CredentialDelegate)
                )
        })
    }

    /// Return whether any grant carries Phase 8 preservation authority.
    #[must_use]
    pub fn has_preservation_features(&self) -> bool {
        self.grants.iter().any(|grant| {
            matches!(
                &grant.actions,
                GrantActions::Selected { actions }
                    if actions.contains(&Action::PreservationAdmin)
                        || actions.contains(&Action::PreservationRelease)
            )
        })
    }

    /// Return the number of stable principals in this snapshot.
    #[must_use]
    pub fn principal_count(&self) -> usize {
        self.principals.len()
    }
}

/// Hash one policy checksum payload with Zeppelin's canonical JSON writer.
pub fn canonical_policy_checksum(value: &Value) -> Result<String, SecurityError> {
    let mut canonical = String::new();
    write_canonical_json(value, &mut canonical)?;
    let digest = Sha256::digest(canonical.as_bytes());
    let mut encoded = String::with_capacity(SHA256_HEX_LEN);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}

fn grant_scope_matches(scope: &GrantScope, resource: &Resource) -> bool {
    match (scope, resource.namespace()) {
        (GrantScope::Global, _) => true,
        (GrantScope::Namespace { namespace }, Some(resource_namespace)) => {
            namespace == resource_namespace
        }
        (GrantScope::Namespace { .. }, None) => false,
    }
}

fn validate_compiled_grant_conflicts(
    grants: &HashMap<PrincipalId, Vec<CompiledGrant>>,
) -> Result<(), SecurityError> {
    for principal_grants in grants.values() {
        for (index, left) in principal_grants.iter().enumerate() {
            for right in &principal_grants[index + 1..] {
                if !grant_scopes_overlap(&left.scope, &right.scope)
                    || left.actions.is_disjoint(&right.actions)
                {
                    continue;
                }
                for (field, left_value) in left.write_constraints.stamp() {
                    if right
                        .write_constraints
                        .stamp()
                        .get(field)
                        .is_some_and(|right_value| right_value != left_value)
                    {
                        return Err(SecurityError::InvalidPolicy(format!(
                            "conflicting server stamps for attribute {field}"
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

fn grant_scopes_overlap(left: &GrantScope, right: &GrantScope) -> bool {
    match (left, right) {
        (GrantScope::Global, _) | (_, GrantScope::Global) => true,
        (
            GrantScope::Namespace {
                namespace: left_namespace,
            },
            GrantScope::Namespace {
                namespace: right_namespace,
            },
        ) => left_namespace == right_namespace,
    }
}

fn bootstrap_key(
    configured: &ApiKeyConfig,
    now: DateTime<Utc>,
    include_preservation: bool,
) -> Result<(PolicyPrincipal, PolicyKey, Vec<PolicyGrant>), SecurityError> {
    if configured.actions.iter().any(|action| action == "*") && configured.actions.len() != 1 {
        return Err(SecurityError::InvalidPolicyRequest(
            "bootstrap actions must not mix '*' with named actions".to_string(),
        ));
    }
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
        let mut actions = Action::BOOTSTRAP_ADMIN_V1.to_vec();
        if include_preservation {
            actions.extend([Action::PreservationAdmin, Action::PreservationRelease]);
        }
        GrantActions::Selected { actions }
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
            mandatory_filter: None,
            field_mask: None,
            write_constraints: None,
            require_approval: Vec::new(),
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

fn merge_migrated_grant_constraints(
    existing: &mut PolicyGrant,
    incoming: PolicyGrant,
) -> Result<(), SecurityError> {
    existing.mandatory_filter = crate::index::filter::combine_filters(
        existing.mandatory_filter.take(),
        incoming.mandatory_filter,
    );
    match (&mut existing.field_mask, incoming.field_mask) {
        (Some(existing), Some(incoming)) => existing.union_from(&incoming),
        (slot @ None, Some(incoming)) => *slot = Some(incoming),
        (Some(_) | None, None) => {}
    }
    match (&mut existing.write_constraints, incoming.write_constraints) {
        (Some(existing), Some(incoming)) => existing.merge_from(&incoming)?,
        (slot @ None, Some(incoming)) => *slot = Some(incoming),
        (Some(_) | None, None) => {}
    }
    existing.require_approval.extend(incoming.require_approval);
    existing.require_approval.sort_unstable();
    existing.require_approval.dedup();
    existing
        .validate_constraints()
        .map_err(SecurityError::InvalidPolicy)
}

fn filters_equal(
    left: Option<&crate::types::Filter>,
    right: Option<&crate::types::Filter>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            match (serde_json::to_value(left), serde_json::to_value(right)) {
                (Ok(left), Ok(right)) => left == right,
                _ => false,
            }
        }
        (None, Some(_)) | (Some(_), None) => false,
    }
}

pub(crate) fn validate_policy_filter(filter: &crate::types::Filter) -> Result<(), String> {
    use crate::types::Filter;

    match filter {
        Filter::Eq { field, value }
        | Filter::NotEq { field, value }
        | Filter::Contains { field, value } => {
            validate_filter_field(field)?;
            validate_filter_value(value)
        }
        Filter::Range {
            field,
            gte,
            lte,
            gt,
            lt,
        } => {
            validate_filter_field(field)?;
            let bounds = [*gte, *lte, *gt, *lt];
            if bounds.iter().all(Option::is_none) {
                return Err("mandatory range filter must contain a bound".to_string());
            }
            if bounds.into_iter().flatten().any(|value| !value.is_finite()) {
                return Err("mandatory range filter bounds must be finite".to_string());
            }
            Ok(())
        }
        Filter::In { field, values } | Filter::NotIn { field, values } => {
            validate_filter_field(field)?;
            if values.is_empty() {
                return Err("mandatory membership filter values must not be empty".to_string());
            }
            for value in values {
                validate_filter_value(value)?;
            }
            Ok(())
        }
        Filter::And { filters } | Filter::Or { filters } => {
            if filters.is_empty() {
                return Err("mandatory logical filter must not be empty".to_string());
            }
            for filter in filters {
                validate_policy_filter(filter)?;
            }
            Ok(())
        }
        Filter::Not { filter } => validate_policy_filter(filter),
        Filter::ContainsAllTokens { field, tokens }
        | Filter::ContainsTokenSequence { field, tokens } => {
            validate_filter_field(field)?;
            let analyzer = crate::fts::FtsFieldConfig::default();
            if tokens.is_empty()
                || tokens.iter().any(|token| {
                    crate::fts::tokenizer::tokenize_text(token, &analyzer, false).is_empty()
                })
            {
                return Err(
                    "mandatory token filter entries must produce at least one token under the default analyzer"
                        .to_string(),
                );
            }
            Ok(())
        }
    }
}

fn validate_filter_field(field: &str) -> Result<(), String> {
    if field.trim().is_empty() {
        Err("mandatory filter field must not be empty".to_string())
    } else {
        Ok(())
    }
}

fn validate_filter_value(value: &crate::types::AttributeValue) -> Result<(), String> {
    match value {
        crate::types::AttributeValue::Float(value) if !value.is_finite() => {
            Err("mandatory filter values must contain only finite numbers".to_string())
        }
        crate::types::AttributeValue::FloatList(values)
            if values.iter().any(|value| !value.is_finite()) =>
        {
            Err("mandatory filter values must contain only finite numbers".to_string())
        }
        crate::types::AttributeValue::String(_)
        | crate::types::AttributeValue::Integer(_)
        | crate::types::AttributeValue::Float(_)
        | crate::types::AttributeValue::Bool(_)
        | crate::types::AttributeValue::StringList(_)
        | crate::types::AttributeValue::IntegerList(_)
        | crate::types::AttributeValue::FloatList(_) => Ok(()),
    }
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::str::FromStr;

    use chrono::{TimeZone, Utc};
    use serde_json::json;

    use super::{grant_order, CompiledPolicy, PolicyGrant, PolicyHead, PolicySnapshot};
    use crate::{
        config::Config,
        namespace::branching::ActivationNonce,
        namespace::{BranchId, NamespaceIncarnationId},
        security::{
            Action, DenyReason, FieldMask, GrantActions, GrantScope, NamespaceId,
            PendingBranchActivation, PolicyControlRevision, PolicyLeaseFencingToken, PrincipalId,
            PrincipalKind, Resource, SecurityError, WriteConstraints,
        },
        types::{AttributeValue, Filter},
    };

    fn fixture<T, E: std::fmt::Debug>(result: Result<T, E>, context: &str) -> T {
        result.unwrap_or_else(|error| panic!("{context}: {error:?}"))
    }

    fn fixture_option<T>(value: Option<T>, context: &str) -> T {
        value.unwrap_or_else(|| panic!("{context}: value was absent"))
    }

    fn fixture_error<T, E>(result: Result<T, E>, context: &str) -> E {
        match result {
            Ok(_) => panic!("{context}: operation unexpectedly succeeded"),
            Err(error) => error,
        }
    }

    fn snapshot_with_grants(grants: Vec<PolicyGrant>) -> PolicySnapshot {
        let config = fixture(
            Config::from_str(
                r#"
[security]
mode = "enforced"
cursor_hmac_key_hex = "1111111111111111111111111111111111111111111111111111111111111111"

[[security.api_keys]]
key_id = "zpk1_reader"
name = "reader"
sha256_hex = "0000000000000000000000000000000000000000000000000000000000000000"
actions = ["SystemRead"]
namespaces = ["*"]
"#,
            ),
            "policy unit config must parse",
        );
        let now = fixture_option(
            Utc.with_ymd_and_hms(2026, 7, 14, 12, 0, 0).single(),
            "fixed policy time must be valid",
        );
        let mut snapshot = fixture(
            PolicySnapshot::from_bootstrap(&config.security, now, false),
            "bootstrap snapshot must compile",
        );
        snapshot.grants = grants;
        snapshot.grants.sort_by(grant_order);
        snapshot.checksum.clear();
        fixture(
            snapshot.validate_structure(),
            "fixture grants must be structurally valid",
        );
        snapshot.checksum = fixture(snapshot.compute_checksum(), "fixture checksum must compute");
        snapshot
    }

    fn compiled_policy_with_grants(grants: Vec<PolicyGrant>) -> (CompiledPolicy, super::Principal) {
        let snapshot = snapshot_with_grants(grants);
        let compiled = fixture(snapshot.compile(), "fixture policy must compile");
        let principal = fixture_option(compiled.key("zpk1_reader"), "fixture key must compile")
            .principal
            .clone();
        (compiled, principal)
    }

    fn grant(value: serde_json::Value) -> PolicyGrant {
        fixture(serde_json::from_value(value), "grant fixture must decode")
    }

    #[test]
    fn legacy_policy_head_defaults_control_state_without_changing_semantic_digest() {
        let snapshot = snapshot_with_grants(Vec::new());
        let object_key = format!("_security/policies/{}.json", ulid::Ulid::new());
        let head = fixture(
            PolicyHead::new(&snapshot, object_key),
            "policy head must validate",
        );
        let expected_digest = fixture(head.semantic_digest(), "semantic digest must compute");
        let mut legacy = fixture(
            serde_json::to_value(&head),
            "policy head must encode for legacy fixture",
        );
        let object = fixture_option(legacy.as_object_mut(), "policy head must encode as object");
        object.remove("control_revision");
        object.remove("publication_fencing_token");
        object.remove("pending_branch_activations");

        let decoded: PolicyHead = fixture(
            serde_json::from_value(legacy),
            "legacy policy head must remain readable",
        );
        fixture(decoded.validate("_security"), "legacy head must validate");
        assert_eq!(decoded.control_revision(), PolicyControlRevision::INITIAL);
        assert_eq!(decoded.publication_fencing_token(), None);
        assert!(decoded.pending_branch_activations().is_empty());
        assert_eq!(
            fixture(decoded.semantic_digest(), "legacy digest must compute"),
            expected_digest
        );
    }

    #[test]
    fn activation_guard_changes_only_control_identity_and_is_digest_covered() {
        let snapshot = snapshot_with_grants(Vec::new());
        let head = fixture(
            PolicyHead::new(
                &snapshot,
                format!("_security/policies/{}.json", ulid::Ulid::new()),
            ),
            "policy head must validate",
        );
        let token = fixture(
            PolicyLeaseFencingToken::new(7),
            "fencing token must validate",
        );
        let claimed = fixture(head.claim_publication(token), "head claim must validate");
        let semantic_digest = fixture(
            claimed.semantic_digest(),
            "claimed semantic digest must compute",
        );
        let created_at = fixture_option(
            Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).single(),
            "guard time must exist",
        );
        let branch_id = BranchId::new();
        let guard = fixture(
            PendingBranchActivation::new(
                branch_id,
                fixture(NamespaceId::new("target"), "target namespace must validate"),
                NamespaceIncarnationId::from_uuid(uuid::Uuid::new_v4()),
                ActivationNonce::new(),
                claimed.version(),
                semantic_digest,
                token,
                created_at,
                created_at + chrono::Duration::minutes(5),
            ),
            "pending activation must validate",
        );
        let guarded = fixture(
            claimed.insert_pending_branch_activation(guard.clone(), token),
            "guard insertion must validate",
        );

        assert_eq!(guarded.version(), claimed.version());
        assert_eq!(guarded.checksum(), claimed.checksum());
        assert_eq!(
            fixture(guarded.semantic_digest(), "guarded digest must compute"),
            semantic_digest
        );
        assert_eq!(
            guarded.control_revision().get(),
            claimed.control_revision().get() + 1
        );
        assert_eq!(
            guarded.pending_branch_activations().get(&branch_id),
            Some(&guard)
        );
        fixture(guarded.validate("_security"), "guarded head must validate");
    }

    #[test]
    fn constrained_grant_round_trips_strict_optional_schema() {
        let value = json!({
            "principal_id": "service:search",
            "scope": {"kind": "namespace", "namespace": "tenant-acme"},
            "actions": {"kind": "selected", "actions": ["Query", "VectorUpsert"]},
            "mandatory_filter": {"op": "eq", "field": "tenant_id", "value": "acme"},
            "field_mask": {"deny": ["salary", "ssn"]},
            "write_constraints": {
                "stamp": {"tenant_id": "acme"},
                "forbid_set": ["is_public", "tenant_id"]
            }
        });

        let grant: PolicyGrant = fixture(
            serde_json::from_value(value.clone()),
            "phase-4 grant schema must decode",
        );
        assert!(matches!(grant.mandatory_filter(), Some(Filter::Eq { .. })));
        assert_eq!(
            grant.field_mask(),
            Some(&fixture(
                FieldMask::new(BTreeSet::from(["salary".to_string(), "ssn".to_string()])),
                "fixture mask must validate",
            ))
        );
        assert_eq!(
            grant.write_constraints(),
            Some(&fixture(
                WriteConstraints::new(
                    BTreeMap::from([(
                        "tenant_id".to_string(),
                        AttributeValue::String("acme".to_string()),
                    )]),
                    BTreeSet::from(["is_public".to_string(), "tenant_id".to_string()]),
                ),
                "fixture write constraints must validate",
            ))
        );
        assert_eq!(
            fixture(serde_json::to_value(&grant), "grant JSON must encode"),
            value
        );
    }

    #[test]
    fn all_applicable_grants_compile_into_one_non_widening_decision() {
        let grants = vec![
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "global"},
                "actions": {"kind": "selected", "actions": ["Query"]},
                "mandatory_filter": {"op": "eq", "field": "tenant_id", "value": "acme"},
                "field_mask": {"deny": ["ssn"]},
                "write_constraints": {
                    "stamp": {"tenant_id": "acme"},
                    "forbid_set": ["is_public"]
                }
            })),
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "namespace", "namespace": "tenant-a"},
                "actions": {"kind": "selected", "actions": ["Query"]},
                "mandatory_filter": {"op": "eq", "field": "region", "value": "west"},
                "field_mask": {"deny": ["salary"]},
                "write_constraints": {"forbid_set": ["classification"]}
            })),
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "namespace", "namespace": "tenant-b"},
                "actions": {"kind": "selected", "actions": ["Query"]},
                "mandatory_filter": {"op": "eq", "field": "region", "value": "east"}
            })),
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "global"},
                "actions": {"kind": "selected", "actions": ["VectorFetch"]},
                "mandatory_filter": {"op": "eq", "field": "wrong_action", "value": true}
            })),
        ];
        let (policy, principal) = compiled_policy_with_grants(grants);

        let authorization = fixture(
            policy.authorize(
                &principal,
                fixture_option(
                    Utc.with_ymd_and_hms(2026, 7, 14, 12, 0, 1).single(),
                    "fixed authorization time must exist",
                ),
                Action::Query,
                &Resource::Namespace(fixture(
                    NamespaceId::new("tenant-a"),
                    "tenant-a namespace fixture must validate",
                )),
            ),
            "tenant-a query must be granted",
        );

        assert_eq!(
            fixture(
                serde_json::to_value(&authorization.mandatory_filter),
                "compiled filter must serialize",
            ),
            json!({"op": "and", "filters": [
                {"op": "eq", "field": "tenant_id", "value": "acme"},
                {"op": "eq", "field": "region", "value": "west"}
            ]})
        );
        assert_eq!(
            fixture_option(
                authorization.field_mask.as_ref(),
                "matching masks must aggregate",
            )
            .denied_fields(),
            &BTreeSet::from(["salary".to_string(), "ssn".to_string()])
        );
        assert_eq!(
            authorization.write_constraints.stamp(),
            &BTreeMap::from([(
                "tenant_id".to_string(),
                AttributeValue::String("acme".to_string())
            )])
        );
        assert_eq!(
            authorization.write_constraints.forbidden_fields(),
            &BTreeSet::from(["classification".to_string(), "is_public".to_string()])
        );
    }

    #[test]
    fn unconstrained_grant_retains_the_phase_three_wire_shape() {
        let grant = fixture(
            PolicyGrant::new(
                fixture(
                    PrincipalId::new("service:search"),
                    "service search principal fixture must validate",
                ),
                GrantScope::Namespace {
                    namespace: fixture(
                        NamespaceId::new("tenant-acme"),
                        "tenant-acme namespace fixture must validate",
                    ),
                },
                GrantActions::Selected {
                    actions: vec![Action::Query],
                },
            ),
            "unconstrained grant must validate",
        );

        assert_eq!(
            fixture(serde_json::to_value(grant), "grant JSON must encode"),
            json!({
                "principal_id": "service:search",
                "scope": {"kind": "namespace", "namespace": "tenant-acme"},
                "actions": {"kind": "selected", "actions": ["Query"]}
            })
        );
    }

    #[test]
    fn grant_binding_identity_ignores_constraints_for_add_and_remove() {
        let constrained = grant(json!({
            "principal_id": "zpk1_reader",
            "scope": {"kind": "namespace", "namespace": "tenant-a"},
            "actions": {"kind": "selected", "actions": ["Query"]},
            "mandatory_filter": {"op": "eq", "field": "tenant_id", "value": "acme"}
        }));
        let binding = fixture(
            PolicyGrant::new(
                fixture(
                    PrincipalId::new("zpk1_reader"),
                    "reader principal fixture must validate",
                ),
                GrantScope::Namespace {
                    namespace: fixture(
                        NamespaceId::new("tenant-a"),
                        "tenant-a namespace fixture must validate",
                    ),
                },
                GrantActions::Selected {
                    actions: vec![Action::Query],
                },
            ),
            "binding must validate",
        );
        let base = snapshot_with_grants(Vec::new());
        let actor = fixture(
            PrincipalId::new("system:test"),
            "system test actor fixture must validate",
        );
        let now = fixture_option(
            Utc.with_ymd_and_hms(2026, 7, 14, 12, 1, 0).single(),
            "fixed mutation time must exist",
        );
        let added = fixture(
            base.add_grant(&actor, now, constrained),
            "first binding must publish",
        );

        assert!(matches!(
            added.add_grant(&actor, now, binding.clone()),
            Err(SecurityError::PolicyEntityAlreadyExists)
        ));
        let removed = fixture(
            added.remove_grant(&actor, now, &binding),
            "unconstrained binding must remove constrained grant",
        );
        assert!(removed.grants().is_empty());
    }

    #[test]
    fn overlapping_applicable_grants_reject_conflicting_server_stamps() {
        let snapshot = snapshot_with_grants(vec![
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "global"},
                "actions": {"kind": "selected", "actions": ["VectorUpsert"]},
                "write_constraints": {"stamp": {"tenant_id": "acme"}}
            })),
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "namespace", "namespace": "tenant-a"},
                "actions": {"kind": "selected", "actions": ["VectorUpsert"]},
                "write_constraints": {"stamp": {"tenant_id": "other"}}
            })),
        ]);

        let error = fixture_error(
            snapshot.compile(),
            "co-applicable conflicting stamps must reject the policy",
        );
        assert!(matches!(
            error,
            SecurityError::InvalidPolicy(message)
                if message == "conflicting server stamps for attribute tenant_id"
        ));
    }

    #[test]
    fn attribute_admin_requires_selected_grant_and_bypasses_only_forbid_set() {
        let write_grant = grant(json!({
            "principal_id": "zpk1_reader",
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["VectorUpsert"]},
            "write_constraints": {
                "stamp": {"tenant_id": "acme"},
                "forbid_set": ["is_public", "tenant_id"]
            }
        }));
        let admin_grant = grant(json!({
            "principal_id": "zpk1_reader",
            "scope": {"kind": "namespace", "namespace": "tenant-a"},
            "actions": {"kind": "selected", "actions": ["AttributeAdmin"]}
        }));
        let (policy, principal) = compiled_policy_with_grants(vec![write_grant.clone()]);
        let now = fixture_option(
            Utc.with_ymd_and_hms(2026, 7, 14, 12, 2, 0).single(),
            "fixed authorization time must exist",
        );
        let resource = Resource::Namespace(fixture(
            NamespaceId::new("tenant-a"),
            "tenant-a namespace fixture must validate",
        ));

        assert!(matches!(
            policy.authorize(&principal, now, Action::AttributeAdmin, &resource),
            Err(DenyReason::ActionNotGranted)
        ));
        let without_admin = fixture(
            policy.authorize(&principal, now, Action::VectorUpsert, &resource),
            "selected write grant must authorize VectorUpsert",
        );
        assert_eq!(
            without_admin.write_constraints.forbidden_fields(),
            &BTreeSet::from(["is_public".to_string(), "tenant_id".to_string()])
        );

        let (policy, principal) = compiled_policy_with_grants(vec![write_grant, admin_grant]);
        let with_admin = fixture(
            policy.authorize(&principal, now, Action::VectorUpsert, &resource),
            "explicit AttributeAdmin must retain write authorization",
        );
        assert!(with_admin.write_constraints.forbidden_fields().is_empty());
        assert_eq!(
            with_admin.write_constraints.stamp(),
            &BTreeMap::from([(
                "tenant_id".to_string(),
                AttributeValue::String("acme".to_string())
            )])
        );
    }

    #[test]
    fn constrained_grant_constructor_rejects_widening_empty_filter_forms() {
        let error = fixture_error(
            PolicyGrant::new_constrained(
                fixture(
                    PrincipalId::new("zpk1_reader"),
                    "reader principal fixture must validate",
                ),
                GrantScope::Global,
                GrantActions::Selected {
                    actions: vec![Action::Query],
                },
                Some(Filter::And {
                    filters: Vec::new(),
                }),
                None,
                None,
                Vec::new(),
            ),
            "empty mandatory conjunction must not mean unrestricted access",
        );
        assert!(matches!(
            error,
            SecurityError::InvalidPolicyRequest(message)
                if message == "mandatory logical filter must not be empty"
        ));
    }

    #[test]
    fn constrained_grant_constructor_rejects_tokens_empty_after_default_analysis() {
        for token in [
            "   ".to_string(),
            "!!!".to_string(),
            "the".to_string(),
            "x".repeat(41),
        ] {
            for mandatory_filter in [
                Filter::ContainsAllTokens {
                    field: "content".to_string(),
                    tokens: vec![token.clone()],
                },
                Filter::ContainsTokenSequence {
                    field: "content".to_string(),
                    tokens: vec![token.clone()],
                },
            ] {
                let error = fixture_error(
                    PolicyGrant::new_constrained(
                        fixture(
                            PrincipalId::new("zpk1_reader"),
                            "reader principal fixture must validate",
                        ),
                        GrantScope::Global,
                        GrantActions::Selected {
                            actions: vec![Action::Query],
                        },
                        Some(mandatory_filter),
                        None,
                        None,
                        Vec::new(),
                    ),
                    "every mandatory token entry must survive default analysis",
                );
                assert!(matches!(
                    error,
                    SecurityError::InvalidPolicyRequest(message)
                        if message
                            == "mandatory token filter entries must produce at least one token under the default analyzer"
                ));
            }
        }
    }

    fn namespace(name: &str) -> NamespaceId {
        fixture(
            NamespaceId::new(name.to_string()),
            "clone namespace fixture must validate",
        )
    }

    fn validate_clone(grants: Vec<PolicyGrant>) -> Result<(), SecurityError> {
        let snapshot = snapshot_with_grants(grants);
        let policy = fixture(snapshot.compile(), "clone policy fixture must compile");
        policy.validate_namespace_copy_no_widening(&namespace("source"), &namespace("target"))
    }

    #[test]
    fn derived_copy_allows_denied_or_equally_constrained_target_reads() {
        let target_denied = validate_clone(vec![grant(json!({
            "principal_id": "zpk1_reader",
            "scope": {"kind": "namespace", "namespace": "source"},
            "actions": {"kind": "selected", "actions": ["Query", "VectorFetch"]},
            "mandatory_filter": {"op": "eq", "field": "tenant_id", "value": "acme"}
        }))]);
        assert!(target_denied.is_ok());

        let equal = validate_clone(vec![grant(json!({
            "principal_id": "zpk1_reader",
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["Query", "VectorFetch"]},
            "mandatory_filter": {"op": "eq", "field": "tenant_id", "value": "acme"},
            "field_mask": {"deny": ["ssn"]}
        }))]);
        assert!(equal.is_ok());
    }

    #[test]
    fn derived_copy_rejects_new_target_read_or_dropped_source_conjunct() {
        let target_only = validate_clone(vec![grant(json!({
            "principal_id": "zpk1_reader",
            "scope": {"kind": "namespace", "namespace": "target"},
            "actions": {"kind": "selected", "actions": ["VectorFetch"]}
        }))]);
        assert!(matches!(
            target_only,
            Err(SecurityError::ConstraintViolation)
        ));

        let dropped = validate_clone(vec![
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "global"},
                "actions": {"kind": "selected", "actions": ["Query"]}
            })),
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "namespace", "namespace": "source"},
                "actions": {"kind": "selected", "actions": ["Query"]},
                "mandatory_filter": {"op": "eq", "field": "tenant_id", "value": "acme"}
            })),
        ]);
        assert!(matches!(dropped, Err(SecurityError::ConstraintViolation)));
    }

    #[test]
    fn derived_copy_accepts_extra_target_conjunct_and_field_mask() {
        let result = validate_clone(vec![
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "global"},
                "actions": {"kind": "selected", "actions": ["Query"]},
                "mandatory_filter": {"op": "eq", "field": "tenant_id", "value": "acme"},
                "field_mask": {"deny": ["ssn"]}
            })),
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "namespace", "namespace": "target"},
                "actions": {"kind": "selected", "actions": ["Query"]},
                "mandatory_filter": {"op": "eq", "field": "region", "value": "west"},
                "field_mask": {"deny": ["salary"]}
            })),
        ]);
        assert!(result.is_ok());
    }

    #[test]
    fn derived_copy_rejects_target_field_mask_that_drops_source_denial() {
        let result = validate_clone(vec![
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "global"},
                "actions": {"kind": "selected", "actions": ["Query"]}
            })),
            grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "namespace", "namespace": "source"},
                "actions": {"kind": "selected", "actions": ["Query"]},
                "field_mask": {"deny": ["ssn"]}
            })),
        ]);
        assert!(matches!(result, Err(SecurityError::ConstraintViolation)));
    }

    #[test]
    fn derived_copy_rejects_target_only_control_observation_and_write_oracles() {
        for action in ["CompactionStatusRead", "CompactionTrigger", "VectorUpsert"] {
            let result = validate_clone(vec![grant(json!({
                "principal_id": "zpk1_reader",
                "scope": {"kind": "namespace", "namespace": "target"},
                "actions": {"kind": "selected", "actions": [action]},
                "mandatory_filter": {"op": "eq", "field": "tenant_id", "value": "acme"}
            }))]);
            assert!(
                matches!(result, Err(SecurityError::ConstraintViolation)),
                "target-only {action} must not observe or mutate a raw clone"
            );
        }
    }

    #[test]
    fn checksum_valid_persisted_approval_invariants_are_revalidated() {
        let valid = grant(json!({
            "principal_id": "zpk1_reader",
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["NamespaceDelete", "VectorDelete"]},
            "require_approval": ["NamespaceDelete"]
        }));
        for invalid in [
            vec![Action::NamespaceDelete, Action::NamespaceDelete],
            vec![Action::VectorDelete, Action::NamespaceDelete],
            vec![Action::Query],
            vec![Action::SnapshotDelete],
        ] {
            let mut snapshot = snapshot_with_grants(vec![valid.clone()]);
            snapshot.grants[0].require_approval = invalid;
            snapshot.checksum.clear();
            snapshot.checksum = fixture(
                snapshot.compute_checksum(),
                "malformed approval fixture checksum must compute",
            );
            assert!(matches!(
                snapshot.validate_for_use(),
                Err(SecurityError::InvalidPolicy(_))
            ));
        }
    }

    #[test]
    fn phase_seven_migration_rewrites_legacy_all_before_compile() {
        let legacy = grant(json!({
            "principal_id": "zpk1_reader",
            "scope": {"kind": "global"},
            "actions": {"kind": "all"}
        }));
        let snapshot = snapshot_with_grants(vec![legacy]);
        assert!(snapshot.requires_phase_seven_all_migration());
        assert!(snapshot.compile().is_err());

        let migrated = fixture(
            snapshot.migrate_phase_seven_all(fixture_option(
                Utc.with_ymd_and_hms(2026, 7, 14, 13, 0, 0).single(),
                "migration timestamp must exist",
            )),
            "legacy wildcard migration must succeed",
        );
        assert!(!migrated.requires_phase_seven_all_migration());
        assert_eq!(migrated.version().get(), snapshot.version().get() + 1);
        assert!(migrated.grants().iter().any(|grant| matches!(
            grant.actions(),
            GrantActions::Selected { actions }
                if actions == Action::POLICY_SAFE_ALL_V2.as_slice()
        )));
        assert!(migrated.grants().iter().any(|grant| matches!(
            grant.actions(),
            GrantActions::Selected { actions }
                if actions == &[Action::SecurityAdminWrite]
        )));
        let compiled = fixture(migrated.compile(), "migrated policy must compile");
        let principal = fixture_option(compiled.key("zpk1_reader"), "reader key must compile")
            .principal
            .clone();
        assert!(compiled
            .authorize(
                &principal,
                fixture_option(
                    Utc.with_ymd_and_hms(2026, 7, 14, 13, 0, 1).single(),
                    "authorization timestamp must exist",
                ),
                Action::SecurityAdminWrite,
                &Resource::SecurityPolicy,
            )
            .is_ok());
        assert!(compiled
            .authorize(
                &principal,
                fixture_option(
                    Utc.with_ymd_and_hms(2026, 7, 14, 13, 0, 1).single(),
                    "authorization timestamp must exist",
                ),
                Action::CredentialDelegate,
                &Resource::SecurityPolicy,
            )
            .is_err());
    }

    #[test]
    fn phase_seven_migration_merges_overlapping_safe_grant_constraints() {
        let legacy = grant(json!({
            "principal_id": "zpk1_reader",
            "scope": {"kind": "global"},
            "actions": {"kind": "all"},
            "mandatory_filter": {"op": "eq", "field": "tenant", "value": "a"},
            "field_mask": {"deny": ["ssn"]},
            "write_constraints": {
                "stamp": {"tenant": "a"},
                "forbid_set": ["classification"]
            },
            "require_approval": ["NamespaceDelete"]
        }));
        let safe_actions = fixture(
            serde_json::to_value(Action::POLICY_SAFE_ALL_V2),
            "safe action list must encode",
        );
        let overlapping = grant(json!({
            "principal_id": "zpk1_reader",
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": safe_actions},
            "mandatory_filter": {"op": "eq", "field": "region", "value": "west"},
            "field_mask": {"deny": ["salary"]},
            "write_constraints": {
                "stamp": {"region": "west"},
                "forbid_set": ["owner"]
            },
            "require_approval": ["VectorDelete"]
        }));
        let snapshot = snapshot_with_grants(vec![legacy, overlapping]);
        let migrated = fixture(
            snapshot.migrate_phase_seven_all(fixture_option(
                Utc.with_ymd_and_hms(2026, 7, 14, 13, 2, 0).single(),
                "migration timestamp must exist",
            )),
            "overlapping wildcard migration must succeed",
        );
        let safe = fixture_option(
            migrated.grants().iter().find(|grant| {
                matches!(
                    grant.actions(),
                    GrantActions::Selected { actions }
                        if actions == Action::POLICY_SAFE_ALL_V2.as_slice()
                )
            }),
            "merged safe grant must exist",
        );
        let filter = fixture(
            serde_json::to_value(safe.mandatory_filter()),
            "merged filter must encode",
        );
        assert_eq!(filter["op"], "and");
        assert_eq!(filter["filters"].as_array().map(Vec::len), Some(2));
        let mask = fixture_option(safe.field_mask(), "merged field mask must exist");
        assert_eq!(
            mask.denied_fields(),
            &BTreeSet::from(["salary".to_string(), "ssn".to_string()])
        );
        let writes = fixture_option(
            safe.write_constraints(),
            "merged write constraints must exist",
        );
        assert_eq!(writes.stamp().len(), 2);
        assert_eq!(
            writes.forbidden_fields(),
            &BTreeSet::from(["classification".to_string(), "owner".to_string()])
        );
        assert_eq!(
            safe.require_approval(),
            &[Action::NamespaceDelete, Action::VectorDelete]
        );
        fixture(migrated.compile(), "merged migration must compile");
    }

    #[test]
    fn new_all_binding_round_trips_as_safe_selected_for_add_and_remove() {
        let principal_id = fixture(
            PrincipalId::new("zpk1_reader"),
            "reader principal fixture must validate",
        );
        let created = fixture(
            PolicyGrant::new(principal_id.clone(), GrantScope::Global, GrantActions::All),
            "new wildcard must normalize",
        );
        let removed = fixture(
            PolicyGrant::new(principal_id, GrantScope::Global, GrantActions::All),
            "removal wildcard must normalize identically",
        );
        assert_eq!(created, removed);
        assert!(matches!(
            created.actions(),
            GrantActions::Selected { actions }
                if actions == Action::POLICY_SAFE_ALL_V2.as_slice()
                    && !actions.contains(&Action::SecurityAdminWrite)
                    && !actions.contains(&Action::CredentialDelegate)
        ));
    }

    #[test]
    fn credential_delegate_grants_reject_agent_and_internal_principals() {
        let grant = grant(json!({
            "principal_id": "zpk1_reader",
            "scope": {"kind": "global"},
            "actions": {"kind": "selected", "actions": ["CredentialDelegate"]}
        }));
        for kind in [PrincipalKind::Agent, PrincipalKind::Internal] {
            let mut snapshot = snapshot_with_grants(Vec::new());
            snapshot.principals[0].kind = kind;
            let error = fixture_error(
                snapshot.add_grant(
                    &fixture(PrincipalId::new("system:test"), "test actor must validate"),
                    fixture_option(
                        Utc.with_ymd_and_hms(2026, 7, 14, 13, 5, 0).single(),
                        "grant timestamp must exist",
                    ),
                    grant.clone(),
                ),
                "non-parent principal kind must reject delegation grant",
            );
            assert!(matches!(error, SecurityError::InvalidPolicyRequest(_)));
        }
    }
}
