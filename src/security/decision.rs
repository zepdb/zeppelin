//! Full-shaped allow and deny values returned before domain work.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize};
use ulid::Ulid;

use crate::types::{AttributeValue, Filter};

use super::Action;

/// Collision-resistant identity shared by authorization and audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DecisionId(Ulid);

impl DecisionId {
    /// Generate a fresh decision identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// Return the underlying ULID.
    #[must_use]
    pub const fn get(self) -> Ulid {
        self.0
    }
}

impl Default for DecisionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Monotonic security-policy version attached to every decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PolicyVersion(u64);

impl PolicyVersion {
    /// Boot-config policy version used until S3 policy lands in phase 3.
    pub const BOOT: Self = Self(0);

    /// Construct a persisted nonzero policy version.
    pub fn persisted(value: u64) -> Result<Self, super::SecurityError> {
        if value == 0 {
            Err(super::SecurityError::InvalidPolicy(
                "persisted policy version must be greater than zero".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }

    /// Return the next persisted version, failing loudly on overflow.
    pub fn checked_next(self) -> Result<Self, super::SecurityError> {
        self.0
            .checked_add(1)
            .ok_or(super::SecurityError::PolicyVersionOverflow)
            .and_then(Self::persisted)
    }

    /// Return the numeric policy version.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Server-only key authenticating policy-bound continuation tokens.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CursorBindingKey([u8; 32]);

impl CursorBindingKey {
    #[must_use]
    pub(crate) const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub(crate) fn from_config_hex(value: &str) -> Result<Self, super::SecurityError> {
        if value.is_empty() {
            return Err(super::SecurityError::MissingCursorHmacKey);
        }
        if value.len() != 64 {
            return Err(super::SecurityError::InvalidCursorHmacKey);
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = cursor_hex_nibble(pair[0])?;
            let low = cursor_hex_nibble(pair[1])?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }
}

fn cursor_hex_nibble(byte: u8) -> Result<u8, super::SecurityError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(super::SecurityError::InvalidCursorHmacKey),
    }
}

impl std::fmt::Debug for CursorBindingKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CursorBindingKey([REDACTED])")
    }
}

/// Stable reason why an authenticated request was denied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DenyReason {
    /// No authenticated identity was available.
    Unauthenticated,
    /// The presented credential is past its configured expiry.
    CredentialExpired,
    /// The credential or principal is not known to current policy.
    CredentialUnknown,
    /// The principal lacks the requested action.
    ActionNotGranted,
    /// The action is granted, but not for this namespace.
    NamespaceNotGranted,
    /// Authoritative policy freshness cannot be proven.
    SecurityStale,
    /// A required audit, approval, or other obligation was not satisfied.
    ObligationUnsatisfied,
}

impl DenyReason {
    /// Stable lowercase wire and audit reason code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::CredentialExpired => "credential_expired",
            Self::CredentialUnknown => "credential_unknown",
            Self::ActionNotGranted => "action_not_granted",
            Self::NamespaceNotGranted => "namespace_not_granted",
            Self::SecurityStale => "security_stale",
            Self::ObligationUnsatisfied => "obligation_unsatisfied",
        }
    }
}

/// Attribute names denied from response projection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct FieldMask {
    /// Attribute keys removed from every observation surface.
    deny: BTreeSet<String>,
}

impl FieldMask {
    /// Construct a nonempty validated response-field deny set.
    pub fn new(deny: BTreeSet<String>) -> Result<Self, super::SecurityError> {
        if deny.is_empty() {
            return Err(super::SecurityError::InvalidPolicyRequest(
                "field_mask.deny must not be empty".to_string(),
            ));
        }
        validate_attribute_names(deny.iter(), "field_mask.deny")?;
        Ok(Self { deny })
    }

    /// Borrow the exact attribute keys removed from response surfaces.
    #[must_use]
    pub fn denied_fields(&self) -> &BTreeSet<String> {
        &self.deny
    }

    /// Return whether this mask removes no fields.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.deny.is_empty()
    }

    pub(crate) fn union_from(&mut self, other: &Self) {
        self.deny.extend(other.deny.iter().cloned());
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldMaskWire {
    deny: Vec<String>,
}

impl<'de> Deserialize<'de> for FieldMask {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FieldMaskWire::deserialize(deserializer)?;
        let item_count = wire.deny.len();
        let deny = wire.deny.into_iter().collect::<BTreeSet<_>>();
        if deny.len() != item_count {
            return Err(serde::de::Error::custom(
                "field_mask.deny must not contain duplicate attribute names",
            ));
        }
        Self::new(deny).map_err(serde::de::Error::custom)
    }
}

/// Server-owned attribute stamps and caller-forbidden fields.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct WriteConstraints {
    /// Attributes stamped by the server, overriding caller values.
    stamp: BTreeMap<String, AttributeValue>,
    /// Attributes ordinary callers may not set or change.
    forbid_set: BTreeSet<String>,
}

impl WriteConstraints {
    /// Construct one nonempty validated write-constraint block.
    pub fn new(
        stamp: BTreeMap<String, AttributeValue>,
        forbid_set: BTreeSet<String>,
    ) -> Result<Self, super::SecurityError> {
        if stamp.is_empty() && forbid_set.is_empty() {
            return Err(super::SecurityError::InvalidPolicyRequest(
                "write_constraints must not be empty".to_string(),
            ));
        }
        validate_attribute_names(stamp.keys(), "write_constraints.stamp")?;
        validate_attribute_names(forbid_set.iter(), "write_constraints.forbid_set")?;
        if stamp.values().any(attribute_value_has_nonfinite_float) {
            return Err(super::SecurityError::InvalidPolicyRequest(
                "write_constraints.stamp values must contain only finite numbers".to_string(),
            ));
        }
        Ok(Self { stamp, forbid_set })
    }

    /// Construct the phase-1 empty constraint set.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Borrow attributes the server overwrites on every accepted upsert.
    #[must_use]
    pub fn stamp(&self) -> &BTreeMap<String, AttributeValue> {
        &self.stamp
    }

    /// Borrow fields ordinary callers are forbidden to provide.
    #[must_use]
    pub fn forbidden_fields(&self) -> &BTreeSet<String> {
        &self.forbid_set
    }

    /// Return whether this value constrains no attributes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.stamp.is_empty() && self.forbid_set.is_empty()
    }

    /// Preserve server stamps while applying the explicit AttributeAdmin exception.
    #[must_use]
    pub fn with_forbid_set_bypassed(&self) -> Self {
        Self {
            stamp: self.stamp.clone(),
            forbid_set: BTreeSet::new(),
        }
    }

    pub(crate) fn merge_from(&mut self, other: &Self) -> Result<(), super::SecurityError> {
        for (field, value) in &other.stamp {
            if let Some(existing) = self.stamp.get(field) {
                if existing != value {
                    return Err(super::SecurityError::InvalidPolicy(format!(
                        "conflicting server stamps for attribute {field}"
                    )));
                }
            } else {
                self.stamp.insert(field.clone(), value.clone());
            }
        }
        self.forbid_set.extend(other.forbid_set.iter().cloned());
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteConstraintsWire {
    #[serde(default, deserialize_with = "deserialize_unique_stamp")]
    stamp: BTreeMap<String, AttributeValue>,
    #[serde(default)]
    forbid_set: Vec<String>,
}

fn deserialize_unique_stamp<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, AttributeValue>, D::Error>
where
    D: Deserializer<'de>,
{
    struct UniqueStampVisitor;

    impl<'de> serde::de::Visitor<'de> for UniqueStampVisitor {
        type Value = BTreeMap<String, AttributeValue>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a map with unique write-constraint stamp fields")
        }

        fn visit_map<A>(self, mut entries: A) -> Result<BTreeMap<String, AttributeValue>, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut stamp = BTreeMap::new();
            while let Some((field, value)) = entries.next_entry::<String, AttributeValue>()? {
                if stamp.insert(field, value).is_some() {
                    return Err(serde::de::Error::custom(
                        "write_constraints.stamp must not contain duplicate attribute names",
                    ));
                }
            }
            Ok(stamp)
        }
    }

    deserializer.deserialize_map(UniqueStampVisitor)
}

impl<'de> Deserialize<'de> for WriteConstraints {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = WriteConstraintsWire::deserialize(deserializer)?;
        let item_count = wire.forbid_set.len();
        let forbid_set = wire.forbid_set.into_iter().collect::<BTreeSet<_>>();
        if forbid_set.len() != item_count {
            return Err(serde::de::Error::custom(
                "write_constraints.forbid_set must not contain duplicate attribute names",
            ));
        }
        Self::new(wire.stamp, forbid_set).map_err(serde::de::Error::custom)
    }
}

fn validate_attribute_names<'a>(
    names: impl IntoIterator<Item = &'a String>,
    location: &str,
) -> Result<(), super::SecurityError> {
    if names.into_iter().any(|name| name.trim().is_empty()) {
        Err(super::SecurityError::InvalidPolicyRequest(format!(
            "{location} contains an empty attribute name"
        )))
    } else {
        Ok(())
    }
}

fn attribute_value_has_nonfinite_float(value: &AttributeValue) -> bool {
    match value {
        AttributeValue::Float(value) => !value.is_finite(),
        AttributeValue::FloatList(values) => values.iter().any(|value| !value.is_finite()),
        AttributeValue::String(_)
        | AttributeValue::Integer(_)
        | AttributeValue::Bool(_)
        | AttributeValue::StringList(_)
        | AttributeValue::IntegerList(_) => false,
    }
}

/// Additional work that must complete before an allowed operation succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Obligation {
    /// Persist an audit record before returning success.
    DurableAudit,
    /// Obtain a distinct authorized approver.
    Approval,
}

/// Authorization result that callers must handle explicitly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Decision {
    /// The operation may proceed with all attached constraints and obligations.
    Allow(Box<AllowDecision>),
    /// The operation must not reach domain logic.
    Deny(DenyDecision),
}

/// Full set of server-owned constraints for one allowed operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowDecision {
    /// Identity shared with audit evidence.
    pub decision_id: DecisionId,
    /// Authoritative policy version used for the evaluation.
    pub policy_version: PolicyVersion,
    /// Checksum of the exact immutable policy snapshot used for this decision.
    #[serde(skip, default)]
    pub(crate) policy_checksum: Option<String>,
    /// Server-only key authenticating cursor version, shape, and marker fields.
    #[serde(skip, default)]
    pub(crate) cursor_binding_key: CursorBindingKey,
    /// Server-owned filter ANDed with any caller filter.
    pub mandatory_filter: Option<Filter>,
    /// Policy-owned component before any delegated-token narrowing is applied.
    ///
    /// This remains server-only so retrieval receipts can bind and later
    /// verify the historical policy predicate without exposing it.
    #[serde(skip, default)]
    pub(crate) policy_filter: Option<Filter>,
    /// Server-owned response projection restrictions.
    pub field_mask: Option<FieldMask>,
    /// Server-owned write attribute constraints.
    pub write_constraints: WriteConstraints,
    /// Whether this write decision exercised the explicit AttributeAdmin privilege.
    #[serde(skip, default)]
    attribute_admin_write: bool,
    /// Required work that must settle before success.
    pub obligations: Vec<Obligation>,
}

impl AllowDecision {
    /// Construct the full-shaped boot-policy allow value for one action.
    ///
    /// Phase 2 attaches the baseline durable-audit obligation here so the
    /// kernel's decision remains the single source of pre-success work.
    #[must_use]
    pub fn boot(action: Action) -> Self {
        Self::for_policy(action, PolicyVersion::BOOT)
    }

    /// Construct a full-shaped allow from one authoritative policy version.
    #[must_use]
    pub fn for_policy(action: Action, policy_version: PolicyVersion) -> Self {
        let obligations = if matches!(
            action,
            Action::RuntimeConfigWrite
                | Action::NamespaceDelete
                | Action::NamespaceFork
                | Action::SnapshotDelete
                | Action::IndexConfigWrite
                | Action::VectorDelete
                | Action::SecurityAdminRead
                | Action::SecurityAdminWrite
                | Action::CredentialDelegate
                | Action::PreservationAdmin
                | Action::PreservationRelease
        ) {
            vec![Obligation::DurableAudit]
        } else {
            Vec::new()
        };
        Self {
            decision_id: DecisionId::new(),
            policy_version,
            policy_checksum: None,
            cursor_binding_key: CursorBindingKey::default(),
            mandatory_filter: None,
            policy_filter: None,
            field_mask: None,
            write_constraints: WriteConstraints::none(),
            attribute_admin_write: false,
            obligations,
        }
    }

    /// Mark a vector write as privileged and require its audit evidence before success.
    pub(crate) fn mark_attribute_admin_write(&mut self) {
        self.attribute_admin_write = true;
        if !self.obligations.contains(&Obligation::DurableAudit) {
            self.obligations.push(Obligation::DurableAudit);
        }
    }

    pub(crate) fn require_approval(&mut self) {
        if !self.obligations.contains(&Obligation::Approval) {
            self.obligations.push(Obligation::Approval);
        }
    }

    /// Return whether this decision exercised the AttributeAdmin exception.
    #[must_use]
    pub(crate) const fn is_attribute_admin_write(&self) -> bool {
        self.attribute_admin_write
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{AllowDecision, FieldMask, Obligation, WriteConstraints};
    use crate::{security::Action, types::AttributeValue};

    #[test]
    fn durable_audit_obligation_inventory_is_exact_through_phase_ten() {
        let durable = Action::ALL
            .into_iter()
            .filter(|action| {
                AllowDecision::boot(*action)
                    .obligations
                    .contains(&Obligation::DurableAudit)
            })
            .collect::<Vec<_>>();

        assert_eq!(
            durable,
            vec![
                Action::RuntimeConfigWrite,
                Action::NamespaceDelete,
                Action::SnapshotDelete,
                Action::NamespaceFork,
                Action::IndexConfigWrite,
                Action::VectorDelete,
                Action::SecurityAdminRead,
                Action::SecurityAdminWrite,
                Action::CredentialDelegate,
                Action::PreservationAdmin,
                Action::PreservationRelease,
            ]
        );
    }

    #[test]
    fn field_mask_rejects_empty_or_blank_attribute_names() {
        assert!(FieldMask::new(BTreeSet::new()).is_err());
        assert!(FieldMask::new(BTreeSet::from([" ".to_string()])).is_err());
        assert!(serde_json::from_str::<FieldMask>(r#"{"deny":[]}"#).is_err());
        assert!(serde_json::from_str::<FieldMask>(r#"{"deny":[""]}"#).is_err());
        assert!(serde_json::from_str::<FieldMask>(r#"{"deny":["ssn","ssn"]}"#).is_err());

        let mask = FieldMask::new(BTreeSet::from(["salary".to_string(), "ssn".to_string()]))
            .unwrap_or_else(|error| panic!("nonempty exact field names must form a mask: {error}"));
        assert_eq!(
            mask.denied_fields(),
            &BTreeSet::from(["salary".to_string(), "ssn".to_string()])
        );
    }

    #[test]
    fn write_constraints_validate_names_but_allow_stamp_forbid_overlap() {
        assert!(WriteConstraints::new(BTreeMap::new(), BTreeSet::new()).is_err());
        assert!(WriteConstraints::new(
            BTreeMap::from([("".to_string(), AttributeValue::Bool(true))]),
            BTreeSet::new(),
        )
        .is_err());
        assert!(
            serde_json::from_str::<WriteConstraints>(r#"{"stamp":{},"forbid_set":[]}"#).is_err()
        );
        assert!(serde_json::from_str::<WriteConstraints>(
            r#"{"forbid_set":["tenant_id","tenant_id"]}"#
        )
        .is_err());
        assert!(serde_json::from_str::<WriteConstraints>(
            r#"{"stamp":{"tenant_id":"acme","tenant_id":"bravo"}}"#
        )
        .is_err());

        let constraints = WriteConstraints::new(
            BTreeMap::from([(
                "tenant_id".to_string(),
                AttributeValue::String("acme".to_string()),
            )]),
            BTreeSet::from(["is_public".to_string(), "tenant_id".to_string()]),
        )
        .unwrap_or_else(|error| {
            panic!("server stamp may overlap caller-forbidden fields: {error}")
        });
        assert_eq!(constraints.stamp().len(), 1);
        assert_eq!(
            constraints.forbidden_fields(),
            &BTreeSet::from(["is_public".to_string(), "tenant_id".to_string()])
        );
    }

    #[test]
    fn attribute_admin_bypass_removes_only_forbid_set() {
        let constraints = WriteConstraints::new(
            BTreeMap::from([(
                "tenant_id".to_string(),
                AttributeValue::String("acme".to_string()),
            )]),
            BTreeSet::from(["tenant_id".to_string()]),
        )
        .unwrap_or_else(|error| panic!("fixture constraints must validate: {error}"));

        let bypassed = constraints.with_forbid_set_bypassed();
        assert_eq!(bypassed.stamp(), constraints.stamp());
        assert!(bypassed.forbidden_fields().is_empty());
    }

    #[test]
    fn attribute_admin_write_requires_durable_audit() {
        let mut decision = AllowDecision::boot(Action::VectorUpsert);
        assert!(!decision.is_attribute_admin_write());
        assert!(!decision.obligations.contains(&Obligation::DurableAudit));

        decision.mark_attribute_admin_write();

        assert!(decision.is_attribute_admin_write());
        assert!(decision.obligations.contains(&Obligation::DurableAudit));
    }
}

/// Explicit failed authorization decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DenyDecision {
    /// Identity shared with audit evidence.
    pub decision_id: DecisionId,
    /// Authoritative policy version used for the evaluation.
    pub policy_version: PolicyVersion,
    /// Stable reason the operation was rejected.
    pub reason: DenyReason,
}

impl DenyDecision {
    /// Construct a boot-policy denial with a fresh decision identity.
    #[must_use]
    pub fn boot(reason: DenyReason) -> Self {
        Self::for_policy(reason, PolicyVersion::BOOT)
    }

    /// Construct a denial tied to one authoritative policy version.
    #[must_use]
    pub fn for_policy(reason: DenyReason, policy_version: PolicyVersion) -> Self {
        Self {
            decision_id: DecisionId::new(),
            policy_version,
            reason,
        }
    }
}
