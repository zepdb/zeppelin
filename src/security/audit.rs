//! Structured, redaction-safe security audit evidence.
//!
//! Audit records deliberately expose a closed vocabulary. Callers can attach
//! namespace names, snapshot names, bounded vector identifiers, and selected
//! numeric configuration values. Production builders never copy bearer
//! headers, vector bodies, document bodies, or query text into that schema.
//! The durable sink serializes these values as one JSON object per line.

use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroU64;

use chrono::{DateTime, Utc};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};

use crate::index::quantization::QuantizationType;
use crate::namespace::manager::NamespaceIndexConfig;
use crate::runtime_config::QueryKnobs;

use super::{
    Action, AuthnFailure, DecisionId, DenyDecision, NamespaceId, PolicyVersion, Principal,
    PrincipalId, PrincipalKind, Resource, SnapshotName,
};

/// Maximum number of vector identifiers included in one audit record.
pub const MAX_AUDITED_VECTOR_IDS: usize = 10;

/// Serialization-safe form of an authorization resource.
///
/// The enum representation makes impossible combinations unrepresentable: a
/// snapshot always has both a namespace and snapshot name, while system and
/// runtime-configuration resources cannot accidentally carry either.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceRef {
    /// Process-wide health, readiness, metrics, or profiling state.
    System,
    /// Live runtime query configuration.
    RuntimeConfig,
    /// S3-authoritative security principals, credentials, grants, and policy.
    SecurityPolicy,
    /// Namespace-scoped data or lifecycle state.
    Namespace {
        /// Validated namespace identifier.
        namespace: NamespaceId,
    },
    /// One named snapshot within a namespace.
    Snapshot {
        /// Namespace that owns the snapshot.
        namespace: NamespaceId,
        /// Validated snapshot name.
        snapshot: SnapshotName,
    },
    /// Normalized Axum matched-route template used before path-resource parsing.
    ///
    /// Authentication middleware supplies `MatchedPath`, never a raw URI or
    /// query string, so attacker-controlled path text cannot enter evidence.
    Route {
        /// Registered route template such as `/v1/namespaces/:ns`.
        matched_path: String,
    },
}

impl From<&Resource> for ResourceRef {
    fn from(resource: &Resource) -> Self {
        match resource {
            Resource::System => Self::System,
            Resource::RuntimeConfig => Self::RuntimeConfig,
            Resource::SecurityPolicy => Self::SecurityPolicy,
            Resource::Namespace(namespace) => Self::Namespace {
                namespace: namespace.clone(),
            },
            Resource::Snapshot(namespace, snapshot) => Self::Snapshot {
                namespace: namespace.clone(),
                snapshot: snapshot.clone(),
            },
        }
    }
}

impl From<Resource> for ResourceRef {
    fn from(resource: Resource) -> Self {
        match resource {
            Resource::System => Self::System,
            Resource::RuntimeConfig => Self::RuntimeConfig,
            Resource::SecurityPolicy => Self::SecurityPolicy,
            Resource::Namespace(namespace) => Self::Namespace { namespace },
            Resource::Snapshot(namespace, snapshot) => Self::Snapshot {
                namespace,
                snapshot,
            },
        }
    }
}

/// Stable result vocabulary for one audited operation.
///
/// External tagging intentionally produces compact evidence such as
/// `"success"` and `{"denied":{"reason":"action_not_granted"}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AuditOutcome {
    /// The operation completed successfully.
    Success,
    /// Central authorization rejected the operation.
    Denied {
        /// Stable redacted denial reason code.
        reason: String,
    },
    /// Credential authentication failed before authorization.
    AuthnFailed {
        /// Stable redacted authentication reason code.
        reason: String,
    },
    /// Domain or infrastructure work returned an error.
    Error {
        /// Stable canonical API error code, never an internal error string.
        code: String,
    },
}

/// Destructive surface blocked by one or more preservation locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreservationBlockedSurface {
    /// Namespace lifecycle deletion.
    NamespaceDelete,
    /// Named snapshot deletion.
    SnapshotDelete,
    /// Vector ID or filter tombstoning.
    VectorDelete,
}

/// Redacted progress vocabulary for one branch parent-root release attempt.
///
/// Failure values carry only Zeppelin's closed, low-cardinality failure class.
/// Internal object keys, ETags, lease identities, and error strings never
/// enter the lifecycle audit record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RootReleaseAuditProgress {
    /// Target visibility is gone while the durable reader-safety grace runs.
    GracePending {
        /// Persisted earliest release time derived from the marker's S3 time.
        not_before: DateTime<Utc>,
    },
    /// This attempt removed the exact matching parent root.
    Released,
    /// This attempt proved the exact root was already safely absent.
    Converged,
    /// This attempt failed before a durable release acknowledgement.
    Failed {
        /// Closed redacted failure class, never an internal diagnostic string.
        class: RootReleaseFailureClass,
    },
}

/// Closed, low-cardinality failure classes for branch root-release audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootReleaseFailureClass {
    /// A current preservation lock ordered before root removal.
    PreservationBlocked,
    /// The parent writer lease could not be acquired, renewed, or validated.
    ParentLeaseUnavailable,
    /// Durable branch/root identities failed exact validation.
    IntegrityRejected,
    /// Authoritative object storage could not complete the attempt.
    StorageUnavailable,
    /// Required lifecycle audit durability was unavailable.
    AuditUnavailable,
    /// Another typed mutation precondition rejected the attempt.
    MutationRejected,
}

impl AuditOutcome {
    /// Return the bounded label used by audit metrics.
    #[must_use]
    pub const fn outcome_class(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Denied { .. } => "denied",
            Self::AuthnFailed { .. } => "authn_failed",
            Self::Error { .. } => "error",
        }
    }
}

/// Redaction-safe copy of process-local runtime query configuration.
///
/// This owned projection exists because [`QueryKnobs`] is an in-process
/// snapshot rather than a persisted format. Keeping the audit representation
/// explicit also prevents later runtime fields from silently entering audit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfigValues {
    /// Maximum byte gap merged during rerank range coalescing.
    pub rerank_coalesce_gap_bytes: usize,
    /// Minimum probe count used when a query omits `nprobe`.
    pub default_nprobe: usize,
    /// Result count used when a query omits `top_k`.
    pub default_top_k: usize,
    /// BM25 full-scan cluster breaker; zero disables it.
    pub bm25_max_full_scan_clusters: usize,
    /// BM25 full-scan vector breaker; zero disables it.
    pub bm25_max_full_scan_vectors: usize,
}

impl From<&QueryKnobs> for RuntimeConfigValues {
    fn from(knobs: &QueryKnobs) -> Self {
        Self {
            rerank_coalesce_gap_bytes: knobs.rerank_coalesce_gap_bytes,
            default_nprobe: knobs.default_nprobe,
            default_top_k: knobs.default_top_k,
            bm25_max_full_scan_clusters: knobs.bm25_max_full_scan_clusters,
            bm25_max_full_scan_vectors: knobs.bm25_max_full_scan_vectors,
        }
    }
}

/// Redaction-safe copy of namespace index-build configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexConfigValues {
    /// Number of IVF centroids used by future compactions.
    pub nlist: usize,
    /// Vector compression mode used by future segments.
    pub quantization: QuantizationType,
    /// Product-quantization subspace count.
    pub pq_m: usize,
    /// Whether future compactions use hierarchical IVF.
    pub hierarchical: bool,
    /// Whether future segments build full-text indexes.
    pub fts_index: bool,
    /// Whether future segments build metadata bitmap indexes.
    pub bitmap_index: bool,
}

impl From<&NamespaceIndexConfig> for IndexConfigValues {
    fn from(config: &NamespaceIndexConfig) -> Self {
        Self {
            nlist: config.nlist,
            quantization: config.quantization,
            pq_m: config.pq_m,
            hierarchical: config.hierarchical,
            fts_index: config.fts_index,
            bitmap_index: config.bitmap_index,
        }
    }
}

/// A validated, audit-eligible collection of at most ten vector identifiers.
///
/// The inner vector is private so production call sites cannot construct an
/// over-limit value. Deserialization enforces the same rule during read-back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct AuditedVectorIds(Vec<String>);

impl AuditedVectorIds {
    /// Construct an audited identifier list when its length is within the cap.
    #[must_use]
    pub fn new(ids: Vec<String>) -> Option<Self> {
        (ids.len() <= MAX_AUDITED_VECTOR_IDS).then_some(Self(ids))
    }

    /// Borrow the audited identifiers in request order.
    #[must_use]
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }
}

impl<'de> Deserialize<'de> for AuditedVectorIds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ids = Vec::<String>::deserialize(deserializer)?;
        Self::new(ids).ok_or_else(|| {
            D::Error::custom(format_args!(
                "audit vector id list exceeds {MAX_AUDITED_VECTOR_IDS} entries"
            ))
        })
    }
}

/// Typed, deliberately narrow parameters for every Phase 2 event family.
///
/// There is intentionally no generic map or arbitrary JSON variant. Adding a
/// new audited value therefore requires a schema review at compile time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AuditParams {
    /// No operation-specific values are needed.
    None,
    /// Authentication failed before a principal was resolved.
    AuthnFailure,
    /// Authorization denied an authenticated principal.
    AuthzDenial,
    /// One or more entries in a query batch violated server-owned constraints.
    BatchQueryConstraintDenial {
        /// Number of positional entries rejected by constraint enforcement.
        denied_entries: usize,
        /// Total positional entries in the accepted outer batch.
        total_entries: usize,
    },
    /// The current process-local query configuration was read.
    RuntimeConfigRead {
        /// Configuration returned to the authorized caller.
        current: RuntimeConfigValues,
    },
    /// The process-local query configuration was updated.
    RuntimeConfigUpdate {
        /// Snapshot in force immediately before the update.
        old: RuntimeConfigValues,
        /// Snapshot published by the update.
        new: RuntimeConfigValues,
    },
    /// A namespace was created or idempotently confirmed.
    NamespaceCreate {
        /// Created namespace identifier.
        namespace: NamespaceId,
    },
    /// A namespace entered its durable deletion protocol.
    NamespaceDelete {
        /// Deleted namespace identifier.
        namespace: NamespaceId,
    },
    /// A named snapshot was created or idempotently confirmed.
    SnapshotPut {
        /// Namespace that owns the snapshot.
        namespace: NamespaceId,
        /// Snapshot name.
        snapshot: SnapshotName,
    },
    /// A named snapshot was deleted.
    SnapshotDelete {
        /// Namespace that owned the snapshot.
        namespace: NamespaceId,
        /// Snapshot name.
        snapshot: SnapshotName,
    },
    /// Retained source state was cloned into a fresh namespace.
    NamespaceClone {
        /// Existing source namespace.
        source: NamespaceId,
        /// Newly materialized target namespace.
        target: NamespaceId,
    },
    /// A live-head copy-on-write branch was prepared or activated.
    NamespaceFork {
        /// Existing source namespace.
        source: NamespaceId,
        /// Newly prepared branch target namespace.
        target: NamespaceId,
    },
    /// Desired namespace index settings were replaced.
    IndexConfigPatch {
        /// Namespace whose settings changed.
        namespace: NamespaceId,
        /// Complete settings before the patch.
        old: IndexConfigValues,
        /// Complete settings after the patch.
        new: IndexConfigValues,
    },
    /// Manual compaction was requested for a namespace.
    CompactionTrigger {
        /// Namespace offered to the compactor.
        namespace: NamespaceId,
    },
    /// Cache hydration was requested for a namespace.
    HydrationTrigger {
        /// Namespace offered to the hydrator.
        namespace: NamespaceId,
    },
    /// Vector tombstones were appended.
    VectorDelete {
        /// Namespace that owns the vector IDs.
        namespace: NamespaceId,
        /// Total number of requested tombstones.
        count: usize,
        /// IDs only when `count` is at most [`MAX_AUDITED_VECTOR_IDS`].
        ids: Option<AuditedVectorIds>,
    },
    /// Vectors were written with explicit AttributeAdmin authority.
    VectorUpsert {
        /// Namespace that owns the vectors.
        namespace: NamespaceId,
        /// Number of vectors in the accepted batch.
        count: usize,
        /// Whether the decision carried privileged AttributeAdmin authority.
        attribute_admin: bool,
    },
    /// Security policy metadata was read through an administrative route.
    SecurityPolicyRead {
        /// Authoritative version returned to the caller.
        version: PolicyVersion,
    },
    /// A CAS-published policy mutation advanced the authoritative version.
    SecurityPolicyChange {
        /// Version on which the mutation was based.
        old_version: PolicyVersion,
        /// Newly published authoritative version.
        new_version: PolicyVersion,
    },
    /// A parent minted one short-lived, purpose-bound delegated credential.
    DelegationMint {
        /// Stable token identifier on success; never the bearer or signature.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token_id: Option<String>,
        /// Parent principal whose current grants remain authoritative.
        parent_principal: PrincipalId,
        /// Mandatory operator-supplied purpose.
        purpose: String,
    },
    /// One preservation lock was created.
    PreservationCreate {
        /// Stable lock identity; reason text is deliberately excluded.
        lock_id: String,
    },
    /// One preservation lock was released through two-person authorization.
    PreservationRelease {
        /// Stable lock identity.
        lock_id: String,
    },
    /// A destructive HTTP request was blocked before domain mutation.
    PreservationBlocked {
        /// Exact destructive surface.
        surface: PreservationBlockedSurface,
        /// Stable active lock identities; never returned in the HTTP error.
        lock_ids: Vec<String>,
    },
    /// One opted-in query issued a signed structural retrieval receipt.
    ReceiptIssued {
        /// Stable receipt identity; query text and policy predicate are excluded.
        receipt_id: String,
    },
    /// The process booted with explicit unsafe-open security mode.
    OpenUnsafeBoot,
    /// The process booted with a verified but expired license.
    LicenseExpiredBoot,
}

impl AuditParams {
    /// Construct lock-bearing evidence for a blocked destructive request.
    #[must_use]
    pub fn preservation_blocked(
        surface: PreservationBlockedSurface,
        guard: &super::PreservationGuard,
    ) -> Self {
        Self::PreservationBlocked {
            surface,
            lock_ids: guard
                .lock_ids()
                .iter()
                .map(|lock_id| lock_id.as_str().to_string())
                .collect(),
        }
    }

    /// Construct vector-delete parameters while enforcing identifier redaction.
    #[must_use]
    pub fn vector_delete(namespace: NamespaceId, ids: &[String]) -> Self {
        let audited_ids = AuditedVectorIds::new(ids.to_vec());
        Self::VectorDelete {
            namespace,
            count: ids.len(),
            ids: audited_ids,
        }
    }

    /// Construct a redacted vector-upsert projection for security audit.
    #[must_use]
    pub fn vector_upsert(namespace: NamespaceId, count: usize, attribute_admin: bool) -> Self {
        Self::VectorUpsert {
            namespace,
            count,
            attribute_admin,
        }
    }
}

/// One-based position in a durable `(node, UTC day)` audit chain.
///
/// Request-path records carry no position until the single audit writer stages
/// them. Persisted records always carry `Some`; recovery and verification reject
/// a missing value explicitly instead of guessing a count from an unbounded
/// historical scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AuditChainPosition(NonZeroU64);

impl AuditChainPosition {
    /// Construct a non-zero chain position.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Return the one-based numeric position.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Complete structured evidence for one security-relevant event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditRecord {
    /// Trusted event time supplied by Zeppelin's shared clock.
    pub ts: DateTime<Utc>,
    /// Canonical request correlation identifier.
    pub request_id: String,
    /// Authorization decision identity, absent for authentication failures.
    pub decision_id: Option<DecisionId>,
    /// Stable principal identifier; authentication failures use `anonymous`.
    pub principal_id: PrincipalId,
    /// Principal origin and operational semantics.
    pub principal_kind: PrincipalKind,
    /// Parent principal for delegated credentials.
    pub delegation_parent: Option<PrincipalId>,
    /// Independent principal that satisfied a two-person approval obligation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_principal_id: Option<PrincipalId>,
    /// Exhaustive operation evaluated by central authorization.
    pub action: Action,
    /// Typed target of the operation.
    pub resource: ResourceRef,
    /// Authoritative policy version used for the decision.
    pub policy_version: PolicyVersion,
    /// Trusted-proxy-resolved source address.
    pub source_ip: IpAddr,
    /// Redacted stable operation result.
    pub outcome: AuditOutcome,
    /// Explicitly selected operation parameters.
    pub params: AuditParams,
    /// Process node identifier used to partition durable audit objects.
    pub node_id: String,
    /// Per-node/day hash-chain link assigned by the durable writer.
    pub prev_hash: Option<String>,
    /// One-based per-node/day position assigned atomically with `prev_hash`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_position: Option<AuditChainPosition>,
}

impl AuditRecord {
    /// Build a record for a resolved principal and explicitly selected outcome.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ts: DateTime<Utc>,
        request_id: impl Into<String>,
        decision_id: Option<DecisionId>,
        principal: &Principal,
        action: Action,
        resource: ResourceRef,
        policy_version: PolicyVersion,
        source_ip: IpAddr,
        outcome: AuditOutcome,
        params: AuditParams,
        node_id: impl Into<String>,
    ) -> Self {
        Self {
            ts,
            request_id: request_id.into(),
            decision_id,
            principal_id: principal.id.clone(),
            principal_kind: principal.kind,
            delegation_parent: principal.delegation_parent.clone(),
            approval_principal_id: None,
            action,
            resource,
            policy_version,
            source_ip,
            outcome,
            params,
            node_id: node_id.into(),
            prev_hash: None,
            chain_position: None,
        }
    }

    /// Build a record for a credential failure without retaining credential data.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn authn_failure(
        ts: DateTime<Utc>,
        request_id: impl Into<String>,
        action: Action,
        resource: ResourceRef,
        policy_version: PolicyVersion,
        source_ip: IpAddr,
        failure: AuthnFailure,
        node_id: impl Into<String>,
    ) -> Self {
        let principal = Principal::anonymous();
        Self::new(
            ts,
            request_id,
            None,
            &principal,
            action,
            resource,
            policy_version,
            source_ip,
            AuditOutcome::AuthnFailed {
                reason: failure.code().to_string(),
            },
            AuditParams::AuthnFailure,
            node_id,
        )
    }

    /// Build a buffered record for a security-admin admission rejection.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn rate_limit_rejection(
        ts: DateTime<Utc>,
        request_id: impl Into<String>,
        principal: &Principal,
        action: Action,
        resource: ResourceRef,
        policy_version: PolicyVersion,
        source_ip: IpAddr,
        node_id: impl Into<String>,
    ) -> Self {
        Self::new(
            ts,
            request_id,
            None,
            principal,
            action,
            resource,
            policy_version,
            source_ip,
            AuditOutcome::Error {
                code: "RATE_LIMITED".to_string(),
            },
            AuditParams::None,
            node_id,
        )
    }

    /// Build a record for an explicit central-authorization denial.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn authorization_denial(
        ts: DateTime<Utc>,
        request_id: impl Into<String>,
        principal: &Principal,
        action: Action,
        resource: ResourceRef,
        source_ip: IpAddr,
        decision: &DenyDecision,
        node_id: impl Into<String>,
    ) -> Self {
        Self::new(
            ts,
            request_id,
            Some(decision.decision_id),
            principal,
            action,
            resource,
            decision.policy_version,
            source_ip,
            AuditOutcome::Denied {
                reason: decision.reason.code().to_string(),
            },
            AuditParams::AuthzDenial,
            node_id,
        )
    }

    /// Build a record for an allowed decision after domain work settles.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn decision_outcome(
        ts: DateTime<Utc>,
        request_id: impl Into<String>,
        decision_id: DecisionId,
        principal: &Principal,
        action: Action,
        resource: ResourceRef,
        policy_version: PolicyVersion,
        source_ip: IpAddr,
        outcome: AuditOutcome,
        params: AuditParams,
        node_id: impl Into<String>,
    ) -> Self {
        Self::new(
            ts,
            request_id,
            Some(decision_id),
            principal,
            action,
            resource,
            policy_version,
            source_ip,
            outcome,
            params,
            node_id,
        )
    }

    /// Build the startup event for explicit unsafe-open mode.
    #[must_use]
    pub fn open_unsafe_boot(ts: DateTime<Utc>, node_id: impl Into<String>) -> Self {
        let principal = Principal::anonymous();
        Self::new(
            ts,
            "startup",
            None,
            &principal,
            Action::SystemRead,
            ResourceRef::System,
            PolicyVersion::BOOT,
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            AuditOutcome::Success,
            AuditParams::OpenUnsafeBoot,
            node_id,
        )
    }

    /// Build the startup event for an expired verified license.
    #[must_use]
    pub fn license_expired_boot(ts: DateTime<Utc>, node_id: impl Into<String>) -> Self {
        let principal = Principal::anonymous();
        Self::new(
            ts,
            "startup",
            None,
            &principal,
            Action::SystemRead,
            ResourceRef::System,
            PolicyVersion::BOOT,
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            AuditOutcome::Success,
            AuditParams::LicenseExpiredBoot,
            node_id,
        )
    }

    /// Serialize this record as exactly one newline-terminated JSON object.
    pub fn to_json_line(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut line = serde_json::to_vec(self)?;
        line.push(b'\n');
        Ok(line)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use chrono::{TimeZone, Utc};
    use serde_json::{json, Value};

    use super::{
        AuditOutcome, AuditParams, AuditRecord, AuditedVectorIds, ResourceRef, RuntimeConfigValues,
        MAX_AUDITED_VECTOR_IDS,
    };
    use crate::security::{
        Action, ApiKeyId, AuthnFailure, DecisionId, NamespaceId, PolicyVersion, Principal,
        PrincipalId, Resource,
    };

    fn namespace(value: &str) -> NamespaceId {
        match NamespaceId::new(value) {
            Ok(namespace) => namespace,
            Err(error) => panic!("test namespace must be valid: {error}"),
        }
    }

    fn principal() -> Principal {
        let id = match PrincipalId::new("audit-test") {
            Ok(id) => id,
            Err(error) => panic!("test principal must be valid: {error}"),
        };
        Principal::api_key(
            id,
            ApiKeyId::new("zpk1_audit_test").unwrap(),
            "Audit Test".to_string(),
            None,
        )
    }

    fn runtime_values(default_top_k: usize) -> RuntimeConfigValues {
        RuntimeConfigValues {
            rerank_coalesce_gap_bytes: 4096,
            default_nprobe: 32,
            default_top_k,
            bm25_max_full_scan_clusters: 128,
            bm25_max_full_scan_vectors: 100_000,
        }
    }

    #[test]
    fn security_policy_resource_uses_typed_redacted_audit_vocabulary() {
        let resource = ResourceRef::from(&Resource::SecurityPolicy);

        assert_eq!(resource, ResourceRef::SecurityPolicy);
        assert_eq!(
            serde_json::to_value(resource).unwrap(),
            json!("security_policy")
        );
    }

    fn record() -> AuditRecord {
        let ts = match Utc.with_ymd_and_hms(2026, 7, 13, 12, 30, 0).single() {
            Some(ts) => ts,
            None => panic!("test timestamp must be valid"),
        };
        AuditRecord::decision_outcome(
            ts,
            "audit-request",
            DecisionId::new(),
            &principal(),
            Action::RuntimeConfigWrite,
            ResourceRef::RuntimeConfig,
            PolicyVersion::BOOT,
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
            AuditOutcome::Success,
            AuditParams::RuntimeConfigUpdate {
                old: runtime_values(10),
                new: runtime_values(20),
            },
            "node-test",
        )
    }

    #[test]
    fn record_round_trips_and_rejects_unknown_fields() {
        let record = record();
        let encoded = match serde_json::to_vec(&record) {
            Ok(encoded) => encoded,
            Err(error) => panic!("audit record must serialize: {error}"),
        };
        let decoded: AuditRecord = match serde_json::from_slice(&encoded) {
            Ok(decoded) => decoded,
            Err(error) => panic!("audit record must deserialize: {error}"),
        };
        assert_eq!(decoded, record);
        assert_eq!(decoded.prev_hash, None);

        let mut value = match serde_json::to_value(&record) {
            Ok(value) => value,
            Err(error) => panic!("audit record must become JSON: {error}"),
        };
        let Some(object) = value.as_object_mut() else {
            panic!("audit record must encode as an object");
        };
        object.insert("bearer_token".to_string(), json!("must-not-be-accepted"));
        assert!(serde_json::from_value::<AuditRecord>(value).is_err());
    }

    #[test]
    fn params_are_typed_and_reject_unknown_nested_fields() {
        let params = AuditParams::RuntimeConfigUpdate {
            old: runtime_values(10),
            new: runtime_values(20),
        };
        let value = match serde_json::to_value(&params) {
            Ok(value) => value,
            Err(error) => panic!("audit params must serialize: {error}"),
        };
        assert_eq!(
            value,
            json!({
                "runtime_config_update": {
                    "old": {
                        "rerank_coalesce_gap_bytes": 4096,
                        "default_nprobe": 32,
                        "default_top_k": 10,
                        "bm25_max_full_scan_clusters": 128,
                        "bm25_max_full_scan_vectors": 100_000
                    },
                    "new": {
                        "rerank_coalesce_gap_bytes": 4096,
                        "default_nprobe": 32,
                        "default_top_k": 20,
                        "bm25_max_full_scan_clusters": 128,
                        "bm25_max_full_scan_vectors": 100_000
                    }
                }
            })
        );

        let mut with_unknown = value;
        let Some(update) = with_unknown
            .get_mut("runtime_config_update")
            .and_then(Value::as_object_mut)
        else {
            panic!("runtime update must encode as an object");
        };
        let Some(new) = update.get_mut("new").and_then(Value::as_object_mut) else {
            panic!("new runtime values must encode as an object");
        };
        new.insert("query_text".to_string(), json!("private"));
        assert!(serde_json::from_value::<AuditParams>(with_unknown).is_err());
    }

    #[test]
    fn vector_delete_includes_at_most_ten_ids() {
        let ten: Vec<String> = (0..MAX_AUDITED_VECTOR_IDS)
            .map(|index| format!("vector-{index}"))
            .collect();
        let included = AuditParams::vector_delete(namespace("catalog"), &ten);
        let included_json = match serde_json::to_value(&included) {
            Ok(value) => value,
            Err(error) => panic!("bounded vector params must serialize: {error}"),
        };
        assert_eq!(
            included_json["vector_delete"]["ids"]
                .as_array()
                .map(Vec::len),
            Some(MAX_AUDITED_VECTOR_IDS)
        );
        assert_eq!(
            included_json["vector_delete"]["count"],
            MAX_AUDITED_VECTOR_IDS
        );

        let eleven: Vec<String> = (0..=MAX_AUDITED_VECTOR_IDS)
            .map(|index| format!("vector-{index}"))
            .collect();
        let omitted = AuditParams::vector_delete(namespace("catalog"), &eleven);
        let omitted_json = match serde_json::to_value(&omitted) {
            Ok(value) => value,
            Err(error) => panic!("redacted vector params must serialize: {error}"),
        };
        assert!(omitted_json["vector_delete"]["ids"].is_null());
        assert_eq!(
            omitted_json["vector_delete"]["count"],
            MAX_AUDITED_VECTOR_IDS + 1
        );

        let over_limit = json!((0..=MAX_AUDITED_VECTOR_IDS)
            .map(|index| format!("vector-{index}"))
            .collect::<Vec<_>>());
        assert!(serde_json::from_value::<AuditedVectorIds>(over_limit).is_err());
    }

    #[test]
    fn json_line_is_one_record_and_newline_terminated() {
        let line = match record().to_json_line() {
            Ok(line) => line,
            Err(error) => panic!("audit record must serialize: {error}"),
        };
        assert_eq!(line.last(), Some(&b'\n'));
        assert_eq!(line.iter().filter(|byte| **byte == b'\n').count(), 1);
    }

    #[test]
    fn authn_failure_uses_anonymous_sentinel_and_matched_route_only() {
        let ts = match Utc.with_ymd_and_hms(2026, 7, 13, 12, 31, 0).single() {
            Some(ts) => ts,
            None => panic!("test timestamp must be valid"),
        };
        let record = AuditRecord::authn_failure(
            ts,
            "authn-request",
            Action::NamespaceRead,
            ResourceRef::Route {
                matched_path: "/v1/namespaces/:ns".to_string(),
            },
            PolicyVersion::BOOT,
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 11)),
            AuthnFailure::CredentialUnknown,
            "node-test",
        );

        assert_eq!(record.principal_id.as_str(), "anonymous");
        assert_eq!(record.decision_id, None);
        assert_eq!(
            record.outcome,
            AuditOutcome::AuthnFailed {
                reason: "credential_unknown".to_string()
            }
        );
        assert_eq!(
            record.resource,
            ResourceRef::Route {
                matched_path: "/v1/namespaces/:ns".to_string()
            }
        );
    }
}
