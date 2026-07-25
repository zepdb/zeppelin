//! Full-shaped allow and deny values returned before domain work.
//!
//! This file owns the *vocabulary of an authorization outcome*. Every protected
//! request in Zeppelin passes through the security kernel, which produces
//! exactly one [`Decision`] before any namespace, WAL, index, or storage code
//! runs. That decision is not a boolean: an allow carries the complete set of
//! server-owned restrictions and pre-success obligations the rest of the request
//! must honor, and a deny carries a stable reason code that audit and the HTTP
//! error mapping both consume.
//!
//! It deliberately does **not** own:
//!
//! - *evaluation* — grants, scopes, and freshness live in `policy.rs` and
//!   `kernel.rs`; this module only shapes what they return;
//! - *enforcement* — the constraints on an allow are applied downstream:
//!   [`apply_field_mask`](crate::security::apply_field_mask) strips masked attributes from
//!   responses, `src/server/handlers/vectors.rs` rejects forbidden fields and
//!   applies server stamps on upsert, the query planner ANDs
//!   `mandatory_filter` into the caller's filter, and `src/server/handlers/query.rs`
//!   uses the cursor binding key when minting and validating continuation tokens;
//! - *audit emission* — [`Obligation::DurableAudit`] states that a durable
//!   record must settle before success; `audit.rs` and `audit_sink.rs` do it.
//!
//! ## Where this sits
//!
//! ```text
//!   HTTP request (axum)
//!         |
//!         v
//!   crate::server::authorize  --- classify_route -> Action
//!         |
//!         v
//!   SecurityKernel::authorize*      (policy.rs evaluates grants)
//!         |
//!         +-- Decision::Deny(DenyDecision) --> audit the denial, return 4xx
//!         |                                   (domain code never runs)
//!         v
//!   Decision::Allow(Box<AllowDecision>)
//!         |
//!         |   wire-visible          server-only (#[serde(skip)])
//!         |   decision_id           policy_checksum
//!         |   policy_version        cursor_binding_key
//!         |   mandatory_filter      policy_filter
//!         |   field_mask            attribute_admin_write
//!         |   write_constraints
//!         |   obligations
//!         v
//!   handler runs, then settles obligations before reporting success
//! ```
//!
//! ## Reading map
//!
//! 1. [`Decision`] — the two-variant outcome every caller must `match`.
//! 2. [`AllowDecision`] and [`AllowDecision::for_policy`] — the full allow shape
//!    and the action-driven baseline obligations attached to it.
//! 3. [`DenyDecision`] and [`DenyReason`] — the stable denial vocabulary.
//! 4. [`FieldMask`] and [`WriteConstraints`] — the validated, non-empty-by-
//!    construction constraint blocks a grant can attach.
//! 5. [`PolicyVersion`] — how a boot-config decision is kept distinguishable
//!    from one backed by the authoritative policy document.
//!
//! ## State and persisted artifacts
//!
//! Nothing here touches object storage. [`FieldMask`], [`WriteConstraints`], and
//! [`PolicyVersion`] are, however, embedded in the authoritative policy document
//! under `_security/` and in audit records, so their wire shapes are a
//! compatibility surface. Both constraint types use a private wire struct with
//! `#[serde(deny_unknown_fields)]`, so an unrecognized policy field is rejected
//! rather than ignored.
//!
//! The security-sensitive fields of [`AllowDecision`] are `#[serde(skip)]`: the
//! policy checksum, the cursor binding key, the pre-narrowing `policy_filter`,
//! and the attribute-admin marker never leave the process. `mandatory_filter` is
//! the *effective* predicate (policy filter combined with any delegated-token
//! narrowing) and may be observed; `policy_filter` is the policy-owned component
//! alone, kept private so a retrieval receipt can bind and later re-verify the
//! historical predicate without disclosing it.
//!
//! Because those fields are skipped, a deserialized [`AllowDecision`] comes back
//! with an all-zero cursor binding key, no policy checksum, no policy filter, and
//! `attribute_admin_write` cleared. A round-tripped allow is therefore evidence,
//! not authority: only a decision produced by the kernel in this process may be
//! used to authorize work.
//!
//! ## Invariants
//!
//! - **Fail closed by shape.** [`Decision`] has exactly two variants and no
//!   "unknown" state, so a caller cannot forget to handle denial. The kernel
//!   returns a decision *before* domain work starts; there is no path that runs
//!   the operation first and checks afterwards.
//! - **Boot and persisted policy versions are distinguishable.**
//!   [`PolicyVersion::BOOT`] is `0` and [`PolicyVersion::persisted`] rejects `0`,
//!   so a decision made from boot configuration can never be mistaken for one
//!   backed by an authoritative S3 policy document.
//!   [`PolicyVersion::checked_next`] fails with `PolicyVersionOverflow` instead
//!   of wrapping.
//! - **Constraint blocks cannot be vacuous.** [`FieldMask::new`] rejects an empty
//!   deny set and blank names; [`WriteConstraints::new`] rejects a wholly empty
//!   block, blank names, and non-finite floats in server stamps (a `NaN` stamp
//!   would not survive canonical serialization). Deserialization additionally
//!   rejects duplicate names, which the collection types would otherwise absorb
//!   silently. [`WriteConstraints::none`] is the one explicit empty value.
//! - **A server stamp always wins.** `stamp` and `forbid_set` may overlap: the
//!   server sets a field that ordinary callers may not.
//!   [`WriteConstraints::with_forbid_set_bypassed`] — the `AttributeAdmin`
//!   exception — clears only `forbid_set` and preserves every stamp, so a
//!   privileged caller can set caller-forbidden attributes but still cannot
//!   overwrite a server-owned one.
//! - **Privileged writes are never unaudited.** Marking a decision as an
//!   attribute-admin write also appends [`Obligation::DurableAudit`] if it is not
//!   already present.
//! - **Obligations are additive and idempotent.** Both mutators check for the
//!   obligation before pushing, so repeated application by the kernel and by the
//!   middleware cannot duplicate an entry. Obligations are only ever added after
//!   construction, never removed.
//! - **The baseline audit inventory is action-driven and pinned.**
//!   [`AllowDecision::for_policy`] attaches [`Obligation::DurableAudit`] to a
//!   fixed set of eleven actions, asserted exactly by
//!   `durable_audit_obligation_inventory_is_exact_through_phase_ten`. Note that
//!   `SecurityAdminRead` is in that set: reading principals, keys, grants, and
//!   policy metadata is itself sensitive. A failed audit writer marks `/readyz`
//!   unavailable rather than letting the obligation be skipped.
//! - **Approval is two-person and can be imposed outside the grant.** A grant may
//!   request approval, delegated-parent authorization adds it for destructive
//!   actions, and `crate::server::authorize` unconditionally adds it for
//!   `Action::PreservationRelease` so no administrator can mint a one-person
//!   release grant.
//! - **Cursor binding material stays opaque.** `CursorBindingKey` requires
//!   exactly 64 hexadecimal characters, distinguishes "missing" from "malformed"
//!   with separate typed errors, and has a hand-written `Debug` that prints
//!   `[REDACTED]` so it cannot reach a log through a derived formatter.
//!
//! ## Rust concepts used here
//!
//! **An enum instead of a boolean.** [`Decision`] makes "allowed but I forgot
//! the field mask" unrepresentable: the constraints are inside the `Allow`
//! variant, so obtaining them requires having matched on it. A Java engineer
//! would reach for an interface plus `instanceof`, or a nullable result; Rust's
//! exhaustive `match` means adding a third outcome later would be a compile error
//! at every call site rather than a silently unhandled branch.
//!
//! **`Box<AllowDecision>` inside the enum.** [`AllowDecision`] is large — several
//! collections, an optional [`Filter`], and a 32-byte key. Rust sizes an enum to
//! its largest variant, so an unboxed allow would make every [`DenyDecision`]
//! carry that footprint too. Boxing keeps `Decision` small on the hot path, at
//! the cost of one heap allocation on the (already expensive) allow path.
//!
//! **Newtypes with `#[serde(transparent)]`.** [`DecisionId`] wraps a ULID and
//! [`PolicyVersion`] wraps a `u64`. On the wire they are a bare string and a bare
//! number, so nothing about the stored format changes; in code the compiler
//! refuses to let a policy version be passed where a fencing token or a count is
//! expected. This is the repository's "make invalid states unrepresentable" rule
//! applied to identifiers.
//!
//! **Parse, don't validate.** [`FieldMask`] and [`WriteConstraints`] keep their
//! fields private and expose only checked constructors and borrowing accessors.
//! Once you hold one, it is known-valid — there is no "did someone validate this
//! yet?" question anywhere downstream. This is why both types implement
//! `Deserialize` by hand over a private wire struct: a derived implementation
//! would build the collections directly and bypass the constructor, and
//! `BTreeSet`/`BTreeMap` would silently collapse duplicate policy entries instead
//! of failing loudly.
//!
//! **`Copy` plus a redacting `Debug`.** `CursorBindingKey` is a `[u8; 32]`, so it
//! is copied by value into handlers with no allocation and no shared mutable
//! state — but its `Debug` is written by hand rather than derived, which is the
//! only reason a `tracing` field or a `{:?}` in an error path cannot leak it.
//!
//! **Borrowing accessors and `#[must_use]`.** [`FieldMask::denied_fields`] and
//! [`WriteConstraints::stamp`] return shared references into the decision rather
//! than clones, so enforcement code reads the authoritative constraint in place;
//! `#[must_use]` on the predicates ensures a check like `is_empty()` cannot be
//! called and discarded.

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
