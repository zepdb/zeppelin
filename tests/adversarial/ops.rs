use std::collections::HashMap;
use std::fmt;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::json;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter};
use zeppelin::wal::LateCandidateKind;

use super::oracle::ViolationId;

#[cfg(test)]
mod branching_operation_tests {
    use super::BranchingOp;

    #[test]
    fn branching_operations_have_stable_kinds() {
        let encoded = serde_json::to_value(BranchingOp::ForkNamespace {
            actor: super::ActorSel::ADMIN,
            source: "source".to_string(),
            target: "target".to_string(),
        })
        .unwrap();
        assert_eq!(encoded["kind"], "fork_namespace");
    }

    #[test]
    fn branching_operations_expose_replay_metadata() {
        let operations = [
            BranchingOp::ForkNamespace {
                actor: super::ActorSel::ADMIN,
                source: "source".to_string(),
                target: "target".to_string(),
            },
            BranchingOp::ListBranches {
                actor: super::ActorSel(2),
                source: "source".to_string(),
            },
            BranchingOp::CompactBranch {
                actor: super::ActorSel(3),
                namespace: "target".to_string(),
            },
            BranchingOp::DeleteBranch {
                actor: super::ActorSel(4),
                namespace: "target".to_string(),
            },
            BranchingOp::DeleteSourceWithBranches {
                actor: super::ActorSel(5),
                source: "source".to_string(),
            },
        ];
        let expected = [
            ("fork_namespace", "source", 0),
            ("list_branches", "source", 2),
            ("compact_branch", "target", 3),
            ("delete_branch", "target", 4),
            ("delete_source_with_branches", "source", 5),
        ];
        for (operation, (kind, namespace, actor)) in operations.iter().zip(expected) {
            assert_eq!(operation.kind(), kind);
            assert_eq!(operation.namespace(), namespace);
            assert_eq!(operation.actor().0, actor);
            assert!(!operation.kind().contains("merge"));
        }
    }

    #[test]
    fn wrapped_branching_operation_rewrites_every_namespace_for_replay() {
        let wrapped = super::Op::Branching(BranchingOp::ForkNamespace {
            actor: super::ActorSel::ADMIN,
            source: "old-source".to_string(),
            target: "old-target".to_string(),
        });
        let rewritten = wrapped.rewrite_namespace_prefix("old-", "new-");
        assert!(matches!(
            rewritten,
            super::Op::Branching(BranchingOp::ForkNamespace { source, target, .. })
                if source == "new-source" && target == "new-target"
        ));
    }
}

pub type Consistency = ConsistencyLevel;

/// Branching operations emitted by the Phase 10 adapter layer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BranchingOp {
    /// Prepare or retry a live-head copy-on-write fork.
    ForkNamespace {
        actor: ActorSel,
        source: String,
        target: String,
    },
    /// List direct branch children.
    ListBranches { actor: ActorSel, source: String },
    /// Compact a writable branch.
    CompactBranch { actor: ActorSel, namespace: String },
    /// Delete a branch target.
    DeleteBranch { actor: ActorSel, namespace: String },
    /// Attempt source deletion while children remain.
    DeleteSourceWithBranches { actor: ActorSel, source: String },
}

impl BranchingOp {
    /// Stable operation vocabulary used by replay artifacts and coverage
    /// reports.  Keep this separate from the legacy `Op` kind table until the
    /// branching HTTP adapter is enabled in the workload scheduler.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ForkNamespace { .. } => "fork_namespace",
            Self::ListBranches { .. } => "list_branches",
            Self::CompactBranch { .. } => "compact_branch",
            Self::DeleteBranch { .. } => "delete_branch",
            Self::DeleteSourceWithBranches { .. } => "delete_source_with_branches",
        }
    }

    #[must_use]
    pub fn namespace(&self) -> &str {
        match self {
            Self::ForkNamespace { source, .. }
            | Self::ListBranches { source, .. }
            | Self::DeleteSourceWithBranches { source, .. } => source,
            Self::CompactBranch { namespace, .. } | Self::DeleteBranch { namespace, .. } => {
                namespace
            }
        }
    }

    #[must_use]
    pub const fn actor(&self) -> ActorSel {
        match self {
            Self::ForkNamespace { actor, .. }
            | Self::ListBranches { actor, .. }
            | Self::CompactBranch { actor, .. }
            | Self::DeleteBranch { actor, .. }
            | Self::DeleteSourceWithBranches { actor, .. } => *actor,
        }
    }

    #[must_use]
    pub fn rewrite_namespace_prefix(&self, old_prefix: &str, new_prefix: &str) -> Self {
        let rewrite = |value: &str| {
            value.strip_prefix(old_prefix).map_or_else(
                || value.to_string(),
                |suffix| format!("{new_prefix}{suffix}"),
            )
        };
        match self {
            Self::ForkNamespace {
                actor,
                source,
                target,
            } => Self::ForkNamespace {
                actor: *actor,
                source: rewrite(source),
                target: rewrite(target),
            },
            Self::ListBranches { actor, source } => Self::ListBranches {
                actor: *actor,
                source: rewrite(source),
            },
            Self::CompactBranch { actor, namespace } => Self::CompactBranch {
                actor: *actor,
                namespace: rewrite(namespace),
            },
            Self::DeleteBranch { actor, namespace } => Self::DeleteBranch {
                actor: *actor,
                namespace: rewrite(namespace),
            },
            Self::DeleteSourceWithBranches { actor, source } => Self::DeleteSourceWithBranches {
                actor: *actor,
                source: rewrite(source),
            },
        }
    }
}

/// Deterministic index into one seed's redaction-safe principal vocabulary.
/// Index zero is the implicit administrator used by legacy artifacts.
#[derive(
    Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(transparent)]
pub struct ActorSel(pub u8);

impl ActorSel {
    pub const ADMIN: Self = Self(0);

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self.0 {
            0 => "implicit-admin",
            1 => "read-only",
            2 => "tenant-a",
            3 => "tenant-b",
            4 => "revocation-target",
            5 => "security-admin",
            _ => "generated-actor",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActorRole {
    ReadOnly,
    TenantA,
    TenantB,
    RevocationTarget,
    SecurityAdmin,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecurityGrantSpec {
    pub namespace: Option<String>,
    pub actions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mandatory_filter: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_constraints: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GrantChange {
    Add,
    Remove,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TenantProbeSurface {
    Query,
    Batch,
    Fetch,
    Paginate,
    Facet,
    Group,
    AsOf,
    Explain,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ForbiddenWriteKind {
    StampForgery,
    ForbidSetAttribute,
    CrossScopeDelete,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeySel {
    pub actor: ActorSel,
    /// Zero selects the current key; one selects the immediately retired key.
    #[serde(default)]
    pub retired: u8,
}

/// Deterministic selector for one delegated credential derived from a parent
/// actor. Artifacts retain only this redaction-safe identity; bearer material
/// remains process-local in the runner.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TokenSel {
    pub parent: ActorSel,
    pub slot: u8,
}

/// Deterministic selector for one preservation lock within a seed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct LockSel(pub u8);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PreservationScopeSpec {
    Global,
    Namespace {
        namespace: String,
    },
    NamespaceFilter {
        namespace: String,
        filter: serde_json::Value,
    },
}

impl PreservationScopeSpec {
    #[must_use]
    pub fn namespace(&self) -> Option<&str> {
        match self {
            Self::Global => None,
            Self::Namespace { namespace } | Self::NamespaceFilter { namespace, .. } => {
                Some(namespace)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeleteUnderLockSurface {
    Namespace,
    Snapshot,
    VectorIds,
    VectorFilter,
}

impl TokenSel {
    #[must_use]
    pub fn artifact_key(self) -> String {
        format!("actor-{}-token-{}", self.parent.0, self.slot)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DelegatedTokenSpec {
    pub actions: Vec<String>,
    pub namespaces: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mandatory_filter: Option<serde_json::Value>,
    pub purpose: String,
    pub expires_after_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)] // `AuditBarrierOp` is the frozen artifact vocabulary.
pub enum Op {
    CreateNamespace {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        spec: NamespaceSpec,
    },
    GetNamespace {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
    },
    Upsert {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        vectors: Vec<GenVector>,
    },
    LateUpsert {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        records: Vec<LateGenRecord>,
    },
    DeleteVectors {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        ids: Vec<String>,
    },
    FetchVectors {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        ids: Vec<String>,
        consistency: Consistency,
    },
    Query {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        q: GeneratedQuery,
        as_of: Option<AsOfTarget>,
    },
    LateQuery {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        query: Vec<Vec<f32>>,
        top_k: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<Filter>,
        consistency: Consistency,
    },
    BatchQuery {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        qs: Vec<GeneratedQuery>,
    },
    PaginateAll {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        q: GeneratedQuery,
        page_size: usize,
    },
    InvalidProbe {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        probe: InvalidProbe,
    },
    CompactEndpoint {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
    },
    GcCycle {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        keep_count: u64,
    },
    CreateSnapshot {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        name: String,
    },
    GetSnapshot {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        name: String,
    },
    ListSnapshots {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
    },
    DeleteSnapshot {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        name: String,
    },
    CloneNamespace {
        #[serde(default)]
        actor: ActorSel,
        source: String,
        target: String,
        as_of: AsOfTarget,
    },
    PatchIndexConfig {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        patch: serde_json::Value,
    },
    Hydrate {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
    },
    DeleteNamespace {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
    },
    /// Feature-gated branching vocabulary carried by ordinary runner records.
    Branching(BranchingOp),
    ProbeSandwich {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
        maintenance: MaintenanceKind,
    },
    CompactInline {
        #[serde(default)]
        actor: ActorSel,
        ns: String,
    },
    CreateKey {
        actor: ActorSel,
        subject: ActorSel,
        principal_kind: ActorRole,
        grants: Vec<SecurityGrantSpec>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_after_secs: Option<u64>,
    },
    RotateKey {
        actor: ActorSel,
        key: KeySel,
    },
    RevokeKey {
        actor: ActorSel,
        key: KeySel,
    },
    PublishGrantChange {
        actor: ActorSel,
        principal: ActorSel,
        grants: Vec<SecurityGrantSpec>,
        change: GrantChange,
    },
    MintToken {
        actor: ActorSel,
        token: TokenSel,
        narrowed: DelegatedTokenSpec,
    },
    UseToken {
        token: TokenSel,
        target_ns: String,
    },
    TokenExceedScopeProbe {
        token: TokenSel,
        target_ns: String,
    },
    UseExpiredToken {
        token: TokenSel,
        target_ns: String,
    },
    RevokeParentThenUseToken {
        token: TokenSel,
        target_ns: String,
    },
    TenantBoundaryProbe {
        actor: ActorSel,
        target_ns: String,
        surface: TenantProbeSurface,
    },
    UseRevokedCredential {
        key: KeySel,
    },
    ForbiddenWriteProbe {
        actor: ActorSel,
        target_ns: String,
        kind: ForbiddenWriteKind,
    },
    ExportProbe {
        actor: ActorSel,
        target_ns: String,
    },
    SecurityAdminProbe {
        actor: ActorSel,
    },
    AuditBarrierOp {
        actor: ActorSel,
    },
    AuditChainCheck {
        actor: ActorSel,
    },
    CreateLock {
        actor: ActorSel,
        lock: LockSel,
        scope: PreservationScopeSpec,
    },
    ReleaseLock {
        actor: ActorSel,
        lock: LockSel,
    },
    DeleteUnderLock {
        actor: ActorSel,
        lock: LockSel,
        ns: String,
        surface: DeleteUnderLockSurface,
    },
    GcUnderLock {
        actor: ActorSel,
        lock: LockSel,
        ns: String,
        keep_count: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceSpec {
    pub dims: usize,
    pub metric: DistanceMetric,
    pub quantization: QuantizationType,
    pub num_centroids: usize,
    pub fts_fields: Vec<String>,
    pub bitmap: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub late_interaction: Option<LateInteractionSpec>,
}

/// Per-seed late-interaction settings consumed by the adversarial test server.
///
/// Only the immutable admission fields are emitted by [`NamespaceSpec::create_body`].
/// The remaining values configure the seed's server and keep the exact small-corpus
/// operating point in replay artifacts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LateInteractionSpec {
    pub candidate_kind: LateCandidateKind,
    pub nlist: usize,
    pub probe_budget: usize,
    pub candidate_k: usize,
    pub kmeans_max_iterations: usize,
    pub max_matrix_object_bytes: usize,
    pub max_cluster_object_bytes: usize,
    pub max_resident_bootstrap_bytes: usize,
    pub read_gap_budget_bytes: usize,
    pub read_max_request_bytes: usize,
    pub read_max_concurrency: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenVector {
    pub id: String,
    pub values: Vec<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

/// One replayable multi-vector retrieval unit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LateGenRecord {
    pub id: String,
    /// Token-major matrix: one inner vector per document token.
    pub values: Vec<Vec<f32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratedQuery {
    pub body: serde_json::Value,
    pub class: QueryOracleClass,
    pub pattern_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryOracleClass {
    ExactAnn {
        top_k: usize,
        consistency: Consistency,
        filter: Option<Filter>,
    },
    Membership {
        consistency: Consistency,
    },
    ExpectError {
        status: u16,
        code: String,
    },
    Unauthorized,
    Forbidden,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AsOfTarget {
    Generation(u64),
    Timestamp(String),
    Snapshot(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum InvalidProbe {
    NanVector,
    WrongDims,
    BadIdCharset,
    EmptyBatch,
    OversizedBatch,
    UnknownField,
    BadCursorToken,
    GroupingPlusCursor,
    WeightsLenMismatch,
    AsOfGenZero,
    AsOfGenFuture,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum MaintenanceKind {
    CompactInline,
    CompactEndpoint,
    GcCycle,
    Hydrate,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPhase {
    #[default]
    Legacy,
    Workload,
    DeferredDrain,
    Quiescence,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HoldReleaseCause {
    #[default]
    Legacy,
    LogicalOp,
    Quiesce,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeldExecutionMetadata {
    pub event_id: String,
    pub window_op: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_release_op: Option<u64>,
    #[serde(alias = "release_op")]
    pub actual_join_op: u64,
    #[serde(default)]
    pub release_cause: HoldReleaseCause,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionMetadata {
    #[serde(default)]
    pub phase: ExecutionPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold: Option<HeldExecutionMetadata>,
}

impl ExecutionMetadata {
    #[must_use]
    pub fn workload() -> Self {
        Self {
            phase: ExecutionPhase::Workload,
            hold: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpRecord {
    pub index: u64,
    pub wall_ms: u64,
    pub op: Op,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub response: serde_json::Value,
    #[serde(default)]
    pub outcome: String,
    #[serde(default, deserialize_with = "deserialize_target_node")]
    pub target_node: u8,
    #[serde(default)]
    pub execution: ExecutionMetadata,
    pub gen_after: Option<u64>,
    pub duration_ms: u64,
    pub violations: Vec<ViolationId>,
}

fn deserialize_target_node<'de, D>(deserializer: D) -> Result<u8, D::Error>
where
    D: Deserializer<'de>,
{
    let target_node = u8::deserialize(deserializer)?;
    if target_node <= 1 {
        Ok(target_node)
    } else {
        Err(D::Error::custom(format!(
            "target_node must be 0 or 1, got {target_node}"
        )))
    }
}

impl Op {
    /// Actor selected for this operation. Deserialization defaults preserve
    /// implicit-administrator semantics for legacy artifacts.
    #[must_use]
    pub const fn actor(&self) -> ActorSel {
        match self {
            Self::CreateNamespace { actor, .. }
            | Self::GetNamespace { actor, .. }
            | Self::Upsert { actor, .. }
            | Self::LateUpsert { actor, .. }
            | Self::DeleteVectors { actor, .. }
            | Self::FetchVectors { actor, .. }
            | Self::Query { actor, .. }
            | Self::LateQuery { actor, .. }
            | Self::BatchQuery { actor, .. }
            | Self::PaginateAll { actor, .. }
            | Self::InvalidProbe { actor, .. }
            | Self::CompactEndpoint { actor, .. }
            | Self::GcCycle { actor, .. }
            | Self::CreateSnapshot { actor, .. }
            | Self::GetSnapshot { actor, .. }
            | Self::ListSnapshots { actor, .. }
            | Self::DeleteSnapshot { actor, .. }
            | Self::CloneNamespace { actor, .. }
            | Self::PatchIndexConfig { actor, .. }
            | Self::Hydrate { actor, .. }
            | Self::DeleteNamespace { actor, .. }
            | Self::ProbeSandwich { actor, .. }
            | Self::CompactInline { actor, .. }
            | Self::CreateKey { actor, .. }
            | Self::RotateKey { actor, .. }
            | Self::RevokeKey { actor, .. }
            | Self::PublishGrantChange { actor, .. }
            | Self::MintToken { actor, .. }
            | Self::TenantBoundaryProbe { actor, .. }
            | Self::ForbiddenWriteProbe { actor, .. }
            | Self::ExportProbe { actor, .. }
            | Self::SecurityAdminProbe { actor }
            | Self::AuditBarrierOp { actor }
            | Self::AuditChainCheck { actor }
            | Self::CreateLock { actor, .. }
            | Self::ReleaseLock { actor, .. }
            | Self::DeleteUnderLock { actor, .. }
            | Self::GcUnderLock { actor, .. } => *actor,
            Self::Branching(operation) => operation.actor(),
            Self::UseRevokedCredential { key } => key.actor,
            Self::UseToken { token, .. }
            | Self::TokenExceedScopeProbe { token, .. }
            | Self::UseExpiredToken { token, .. }
            | Self::RevokeParentThenUseToken { token, .. } => token.parent,
        }
    }

    /// Returns whether this operation is safe to route to a designated
    /// read-only secondary in the supported one-writer topology.
    #[must_use]
    pub fn is_read_only_request(&self) -> bool {
        matches!(
            self,
            Op::GetNamespace { .. }
                | Op::FetchVectors { .. }
                | Op::Query { .. }
                | Op::LateQuery { .. }
                | Op::BatchQuery { .. }
                | Op::PaginateAll { .. }
                | Op::GetSnapshot { .. }
                | Op::ListSnapshots { .. }
                | Op::TenantBoundaryProbe { .. }
                | Op::UseRevokedCredential { .. }
                | Op::UseToken { .. }
                | Op::TokenExceedScopeProbe { .. }
                | Op::UseExpiredToken { .. }
                | Op::RevokeParentThenUseToken { .. }
                | Op::ExportProbe { .. }
                | Op::SecurityAdminProbe { .. }
                | Op::AuditBarrierOp { .. }
                | Op::AuditChainCheck { .. }
                | Op::CreateLock { .. }
                | Op::ReleaseLock { .. }
                | Op::DeleteUnderLock { .. }
                | Op::GcUnderLock { .. }
                | Op::Branching(BranchingOp::ListBranches { .. })
        )
    }

    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Op::CreateNamespace { .. } => "create_namespace",
            Op::GetNamespace { .. } => "get_namespace",
            Op::Upsert { .. } => "upsert",
            Op::LateUpsert { .. } => "late_upsert",
            Op::DeleteVectors { .. } => "delete_vectors",
            Op::FetchVectors { .. } => "fetch_vectors",
            Op::Query { .. } => "query",
            Op::LateQuery { .. } => "late_query",
            Op::BatchQuery { .. } => "batch_query",
            Op::PaginateAll { .. } => "paginate_all",
            Op::InvalidProbe { .. } => "invalid_probe",
            Op::CompactEndpoint { .. } => "compact_endpoint",
            Op::GcCycle { .. } => "gc_cycle",
            Op::CreateSnapshot { .. } => "create_snapshot",
            Op::GetSnapshot { .. } => "get_snapshot",
            Op::ListSnapshots { .. } => "list_snapshots",
            Op::DeleteSnapshot { .. } => "delete_snapshot",
            Op::CloneNamespace { .. } => "clone_namespace",
            Op::PatchIndexConfig { .. } => "patch_index_config",
            Op::Hydrate { .. } => "hydrate",
            Op::DeleteNamespace { .. } => "delete_namespace",
            Op::Branching(operation) => operation.kind(),
            Op::ProbeSandwich { .. } => "probe_sandwich",
            Op::CompactInline { .. } => "compact_inline",
            Op::CreateKey { .. } => "create_key",
            Op::RotateKey { .. } => "rotate_key",
            Op::RevokeKey { .. } => "revoke_key",
            Op::PublishGrantChange { .. } => "publish_grant_change",
            Op::MintToken { .. } => "mint_token",
            Op::UseToken { .. } => "use_token",
            Op::TokenExceedScopeProbe { .. } => "token_exceed_scope_probe",
            Op::UseExpiredToken { .. } => "use_expired_token",
            Op::RevokeParentThenUseToken { .. } => "revoke_parent_then_use_token",
            Op::TenantBoundaryProbe { .. } => "tenant_boundary_probe",
            Op::UseRevokedCredential { .. } => "use_revoked_credential",
            Op::ForbiddenWriteProbe { .. } => "forbidden_write_probe",
            Op::ExportProbe { .. } => "export_probe",
            Op::SecurityAdminProbe { .. } => "security_admin_probe",
            Op::AuditBarrierOp { .. } => "audit_barrier",
            Op::AuditChainCheck { .. } => "audit_chain_check",
            Op::CreateLock { .. } => "create_lock",
            Op::ReleaseLock { .. } => "release_lock",
            Op::DeleteUnderLock { .. } => "delete_under_lock",
            Op::GcUnderLock { .. } => "gc_under_lock",
        }
    }

    #[must_use]
    pub fn is_security_op(&self) -> bool {
        matches!(
            self,
            Op::CreateKey { .. }
                | Op::RotateKey { .. }
                | Op::RevokeKey { .. }
                | Op::PublishGrantChange { .. }
                | Op::MintToken { .. }
                | Op::UseToken { .. }
                | Op::TokenExceedScopeProbe { .. }
                | Op::UseExpiredToken { .. }
                | Op::RevokeParentThenUseToken { .. }
                | Op::TenantBoundaryProbe { .. }
                | Op::UseRevokedCredential { .. }
                | Op::ForbiddenWriteProbe { .. }
                | Op::ExportProbe { .. }
                | Op::SecurityAdminProbe { .. }
                | Op::AuditBarrierOp { .. }
                | Op::AuditChainCheck { .. }
                | Op::CreateLock { .. }
                | Op::ReleaseLock { .. }
                | Op::DeleteUnderLock { .. }
                | Op::GcUnderLock { .. }
        )
    }

    #[must_use]
    pub fn namespace(&self) -> &str {
        match self {
            Op::CreateNamespace { ns, .. }
            | Op::GetNamespace { ns, .. }
            | Op::Upsert { ns, .. }
            | Op::LateUpsert { ns, .. }
            | Op::DeleteVectors { ns, .. }
            | Op::FetchVectors { ns, .. }
            | Op::Query { ns, .. }
            | Op::LateQuery { ns, .. }
            | Op::BatchQuery { ns, .. }
            | Op::PaginateAll { ns, .. }
            | Op::InvalidProbe { ns, .. }
            | Op::CompactEndpoint { ns, .. }
            | Op::GcCycle { ns, .. }
            | Op::CreateSnapshot { ns, .. }
            | Op::GetSnapshot { ns, .. }
            | Op::ListSnapshots { ns, .. }
            | Op::DeleteSnapshot { ns, .. }
            | Op::PatchIndexConfig { ns, .. }
            | Op::Hydrate { ns, .. }
            | Op::DeleteNamespace { ns, .. }
            | Op::ProbeSandwich { ns, .. }
            | Op::CompactInline { ns, .. } => ns,
            Op::Branching(operation) => operation.namespace(),
            Op::DeleteUnderLock { ns, .. } | Op::GcUnderLock { ns, .. } => ns,
            Op::CreateLock { scope, .. } => scope.namespace().unwrap_or("_security"),
            Op::CloneNamespace { source, .. } => source,
            Op::TenantBoundaryProbe { target_ns, .. }
            | Op::ForbiddenWriteProbe { target_ns, .. }
            | Op::ExportProbe { target_ns, .. }
            | Op::UseToken { target_ns, .. }
            | Op::TokenExceedScopeProbe { target_ns, .. }
            | Op::UseExpiredToken { target_ns, .. }
            | Op::RevokeParentThenUseToken { target_ns, .. } => target_ns,
            Op::CreateKey { .. }
            | Op::RotateKey { .. }
            | Op::RevokeKey { .. }
            | Op::PublishGrantChange { .. }
            | Op::MintToken { .. }
            | Op::UseRevokedCredential { .. }
            | Op::SecurityAdminProbe { .. }
            | Op::AuditBarrierOp { .. }
            | Op::AuditChainCheck { .. } => "_security",
            Op::ReleaseLock { .. } => "_security",
        }
    }

    #[must_use]
    pub fn is_mutating(&self) -> bool {
        matches!(
            self,
            Op::CreateNamespace { .. }
                | Op::Upsert { .. }
                | Op::LateUpsert { .. }
                | Op::DeleteVectors { .. }
                | Op::CompactEndpoint { .. }
                | Op::CreateSnapshot { .. }
                | Op::DeleteSnapshot { .. }
                | Op::CloneNamespace { .. }
                | Op::PatchIndexConfig { .. }
                | Op::DeleteNamespace { .. }
                | Op::Branching(
                    BranchingOp::ForkNamespace { .. }
                        | BranchingOp::CompactBranch { .. }
                        | BranchingOp::DeleteBranch { .. }
                        | BranchingOp::DeleteSourceWithBranches { .. }
                )
                | Op::ProbeSandwich { .. }
                | Op::CompactInline { .. }
                | Op::CreateKey { .. }
                | Op::RotateKey { .. }
                | Op::RevokeKey { .. }
                | Op::PublishGrantChange { .. }
                | Op::MintToken { .. }
                | Op::ForbiddenWriteProbe { .. }
                | Op::CreateLock { .. }
                | Op::ReleaseLock { .. }
                | Op::DeleteUnderLock { .. }
                | Op::GcUnderLock { .. }
        )
    }

    #[must_use]
    pub fn tags(&self) -> Vec<&str> {
        match self {
            Op::LateUpsert { .. } | Op::LateQuery { .. } => vec!["late-interaction"],
            Op::Query { q, .. } => q.pattern_tags.iter().map(String::as_str).collect(),
            Op::BatchQuery { qs, .. } => qs
                .iter()
                .flat_map(|q| q.pattern_tags.iter().map(String::as_str))
                .collect(),
            Op::PaginateAll { q, .. } => q.pattern_tags.iter().map(String::as_str).collect(),
            Op::InvalidProbe { probe, .. } => vec!["invalid-probe", probe.tag()],
            Op::CompactEndpoint { .. } => vec!["compact-endpoint"],
            Op::GcCycle { .. } => vec!["gc-cycle"],
            Op::CreateSnapshot { .. } | Op::GetSnapshot { .. } | Op::ListSnapshots { .. } => {
                vec!["snapshot"]
            }
            Op::DeleteSnapshot { .. } => vec!["snapshot", "delete-snapshot"],
            Op::CloneNamespace { .. } => vec!["clone"],
            Op::PatchIndexConfig { .. } => vec!["config-patch"],
            Op::Hydrate { .. } => vec!["hydrate"],
            Op::DeleteNamespace { .. } => vec!["delete-recreate"],
            Op::Branching(
                BranchingOp::DeleteBranch { .. } | BranchingOp::DeleteSourceWithBranches { .. },
            ) => vec!["branching", "branch-delete"],
            Op::Branching(_) => vec!["branching"],
            Op::ProbeSandwich { maintenance, .. } => vec!["sandwich", maintenance.tag()],
            Op::CreateKey { .. }
            | Op::RotateKey { .. }
            | Op::RevokeKey { .. }
            | Op::PublishGrantChange { .. }
            | Op::MintToken { .. }
            | Op::UseToken { .. }
            | Op::TokenExceedScopeProbe { .. }
            | Op::UseExpiredToken { .. }
            | Op::RevokeParentThenUseToken { .. }
            | Op::TenantBoundaryProbe { .. }
            | Op::UseRevokedCredential { .. }
            | Op::ForbiddenWriteProbe { .. }
            | Op::ExportProbe { .. }
            | Op::SecurityAdminProbe { .. }
            | Op::AuditBarrierOp { .. } => vec!["security"],
            Op::AuditChainCheck { .. } => vec!["security", "audit"],
            Op::CreateLock { .. }
            | Op::ReleaseLock { .. }
            | Op::DeleteUnderLock { .. }
            | Op::GcUnderLock { .. } => vec!["security", "preservation"],
            _ => Vec::new(),
        }
    }

    #[must_use]
    pub fn rewrite_namespace_prefix(&self, old_prefix: &str, new_prefix: &str) -> Self {
        let rewrite = |value: &str| -> String {
            value.strip_prefix(old_prefix).map_or_else(
                || value.to_string(),
                |suffix| format!("{new_prefix}{suffix}"),
            )
        };
        match self {
            Op::CreateNamespace { actor, ns, spec } => Op::CreateNamespace {
                actor: *actor,
                ns: rewrite(ns),
                spec: spec.clone(),
            },
            Op::GetNamespace { actor, ns } => Op::GetNamespace {
                actor: *actor,
                ns: rewrite(ns),
            },
            Op::Upsert { actor, ns, vectors } => Op::Upsert {
                actor: *actor,
                ns: rewrite(ns),
                vectors: vectors.clone(),
            },
            Op::LateUpsert { actor, ns, records } => Op::LateUpsert {
                actor: *actor,
                ns: rewrite(ns),
                records: records.clone(),
            },
            Op::DeleteVectors { actor, ns, ids } => Op::DeleteVectors {
                actor: *actor,
                ns: rewrite(ns),
                ids: ids.clone(),
            },
            Op::FetchVectors {
                actor,
                ns,
                ids,
                consistency,
            } => Op::FetchVectors {
                actor: *actor,
                ns: rewrite(ns),
                ids: ids.clone(),
                consistency: *consistency,
            },
            Op::Query {
                actor,
                ns,
                q,
                as_of,
            } => Op::Query {
                actor: *actor,
                ns: rewrite(ns),
                q: q.clone(),
                as_of: as_of.clone(),
            },
            Op::LateQuery {
                actor,
                ns,
                query,
                top_k,
                filter,
                consistency,
            } => Op::LateQuery {
                actor: *actor,
                ns: rewrite(ns),
                query: query.clone(),
                top_k: *top_k,
                filter: filter.clone(),
                consistency: *consistency,
            },
            Op::BatchQuery { actor, ns, qs } => Op::BatchQuery {
                actor: *actor,
                ns: rewrite(ns),
                qs: qs.clone(),
            },
            Op::PaginateAll {
                actor,
                ns,
                q,
                page_size,
            } => Op::PaginateAll {
                actor: *actor,
                ns: rewrite(ns),
                q: q.clone(),
                page_size: *page_size,
            },
            Op::InvalidProbe { actor, ns, probe } => Op::InvalidProbe {
                actor: *actor,
                ns: rewrite(ns),
                probe: *probe,
            },
            Op::CompactEndpoint { actor, ns } => Op::CompactEndpoint {
                actor: *actor,
                ns: rewrite(ns),
            },
            Op::GcCycle {
                actor,
                ns,
                keep_count,
            } => Op::GcCycle {
                actor: *actor,
                ns: rewrite(ns),
                keep_count: *keep_count,
            },
            Op::CreateSnapshot { actor, ns, name } => Op::CreateSnapshot {
                actor: *actor,
                ns: rewrite(ns),
                name: name.clone(),
            },
            Op::GetSnapshot { actor, ns, name } => Op::GetSnapshot {
                actor: *actor,
                ns: rewrite(ns),
                name: name.clone(),
            },
            Op::ListSnapshots { actor, ns } => Op::ListSnapshots {
                actor: *actor,
                ns: rewrite(ns),
            },
            Op::DeleteSnapshot { actor, ns, name } => Op::DeleteSnapshot {
                actor: *actor,
                ns: rewrite(ns),
                name: name.clone(),
            },
            Op::CloneNamespace {
                actor,
                source,
                target,
                as_of,
            } => Op::CloneNamespace {
                actor: *actor,
                source: rewrite(source),
                target: rewrite(target),
                as_of: as_of.clone(),
            },
            Op::PatchIndexConfig { actor, ns, patch } => Op::PatchIndexConfig {
                actor: *actor,
                ns: rewrite(ns),
                patch: patch.clone(),
            },
            Op::Hydrate { actor, ns } => Op::Hydrate {
                actor: *actor,
                ns: rewrite(ns),
            },
            Op::DeleteNamespace { actor, ns } => Op::DeleteNamespace {
                actor: *actor,
                ns: rewrite(ns),
            },
            Op::Branching(operation) => {
                Op::Branching(operation.rewrite_namespace_prefix(old_prefix, new_prefix))
            }
            Op::ProbeSandwich {
                actor,
                ns,
                maintenance,
            } => Op::ProbeSandwich {
                actor: *actor,
                ns: rewrite(ns),
                maintenance: *maintenance,
            },
            Op::CompactInline { actor, ns } => Op::CompactInline {
                actor: *actor,
                ns: rewrite(ns),
            },
            Op::CreateKey {
                actor,
                subject,
                principal_kind,
                grants,
                expires_after_secs,
            } => Op::CreateKey {
                actor: *actor,
                subject: *subject,
                principal_kind: *principal_kind,
                grants: rewrite_security_grants(grants, &rewrite),
                expires_after_secs: *expires_after_secs,
            },
            Op::RotateKey { actor, key } => Op::RotateKey {
                actor: *actor,
                key: *key,
            },
            Op::RevokeKey { actor, key } => Op::RevokeKey {
                actor: *actor,
                key: *key,
            },
            Op::PublishGrantChange {
                actor,
                principal,
                grants,
                change,
            } => Op::PublishGrantChange {
                actor: *actor,
                principal: *principal,
                grants: rewrite_security_grants(grants, &rewrite),
                change: *change,
            },
            Op::MintToken {
                actor,
                token,
                narrowed,
            } => Op::MintToken {
                actor: *actor,
                token: *token,
                narrowed: DelegatedTokenSpec {
                    actions: narrowed.actions.clone(),
                    namespaces: narrowed
                        .namespaces
                        .iter()
                        .map(|namespace| rewrite(namespace))
                        .collect(),
                    mandatory_filter: narrowed.mandatory_filter.clone(),
                    purpose: narrowed.purpose.clone(),
                    expires_after_secs: narrowed.expires_after_secs,
                },
            },
            Op::UseToken { token, target_ns } => Op::UseToken {
                token: *token,
                target_ns: rewrite(target_ns),
            },
            Op::TokenExceedScopeProbe { token, target_ns } => Op::TokenExceedScopeProbe {
                token: *token,
                target_ns: rewrite(target_ns),
            },
            Op::UseExpiredToken { token, target_ns } => Op::UseExpiredToken {
                token: *token,
                target_ns: rewrite(target_ns),
            },
            Op::RevokeParentThenUseToken { token, target_ns } => Op::RevokeParentThenUseToken {
                token: *token,
                target_ns: rewrite(target_ns),
            },
            Op::TenantBoundaryProbe {
                actor,
                target_ns,
                surface,
            } => Op::TenantBoundaryProbe {
                actor: *actor,
                target_ns: rewrite(target_ns),
                surface: *surface,
            },
            Op::UseRevokedCredential { key } => Op::UseRevokedCredential { key: *key },
            Op::ForbiddenWriteProbe {
                actor,
                target_ns,
                kind,
            } => Op::ForbiddenWriteProbe {
                actor: *actor,
                target_ns: rewrite(target_ns),
                kind: *kind,
            },
            Op::ExportProbe { actor, target_ns } => Op::ExportProbe {
                actor: *actor,
                target_ns: rewrite(target_ns),
            },
            Op::SecurityAdminProbe { actor } => Op::SecurityAdminProbe { actor: *actor },
            Op::AuditBarrierOp { actor } => Op::AuditBarrierOp { actor: *actor },
            Op::AuditChainCheck { actor } => Op::AuditChainCheck { actor: *actor },
            Op::CreateLock { actor, lock, scope } => Op::CreateLock {
                actor: *actor,
                lock: *lock,
                scope: match scope {
                    PreservationScopeSpec::Global => PreservationScopeSpec::Global,
                    PreservationScopeSpec::Namespace { namespace } => {
                        PreservationScopeSpec::Namespace {
                            namespace: rewrite(namespace),
                        }
                    }
                    PreservationScopeSpec::NamespaceFilter { namespace, filter } => {
                        PreservationScopeSpec::NamespaceFilter {
                            namespace: rewrite(namespace),
                            filter: filter.clone(),
                        }
                    }
                },
            },
            Op::ReleaseLock { actor, lock } => Op::ReleaseLock {
                actor: *actor,
                lock: *lock,
            },
            Op::DeleteUnderLock {
                actor,
                lock,
                ns,
                surface,
            } => Op::DeleteUnderLock {
                actor: *actor,
                lock: *lock,
                ns: rewrite(ns),
                surface: *surface,
            },
            Op::GcUnderLock {
                actor,
                lock,
                ns,
                keep_count,
            } => Op::GcUnderLock {
                actor: *actor,
                lock: *lock,
                ns: rewrite(ns),
                keep_count: *keep_count,
            },
        }
    }
}

fn rewrite_security_grants(
    grants: &[SecurityGrantSpec],
    rewrite: &impl Fn(&str) -> String,
) -> Vec<SecurityGrantSpec> {
    grants
        .iter()
        .cloned()
        .map(|mut grant| {
            grant.namespace = grant.namespace.as_deref().map(rewrite);
            grant
        })
        .collect()
}

impl NamespaceSpec {
    #[must_use]
    pub fn create_body(&self, ns: &str) -> serde_json::Value {
        if self.late_interaction.is_some() {
            return json!({
                "name": ns,
                "dimensions": 0,
                "index_type": "late_interaction_fde",
                "late_interaction": {
                    "accepted_modalities": ["text"]
                },
                "distance_metric": DistanceMetric::DotProduct,
                "full_text_search": {}
            });
        }
        let full_text_search = self
            .fts_fields
            .iter()
            .map(|field| (field.clone(), json!({})))
            .collect::<serde_json::Map<_, _>>();
        json!({
            "name": ns,
            "dimensions": self.dims,
            "distance_metric": self.metric,
            "full_text_search": full_text_search,
            "index_config": {
                "nlist": self.num_centroids,
                "quantization": self.quantization,
                "pq_m": 1,
                "hierarchical": false,
                "fts_index": !self.fts_fields.is_empty(),
                "bitmap_index": self.bitmap,
            }
        })
    }

    #[must_use]
    pub fn is_exact(&self) -> bool {
        self.late_interaction.is_none() && self.quantization == QuantizationType::None
    }
}

impl InvalidProbe {
    #[must_use]
    pub fn expected_status(self) -> u16 {
        match self {
            Self::OversizedBatch => 413,
            Self::AsOfGenZero | Self::AsOfGenFuture => 410,
            _ => 400,
        }
    }

    #[must_use]
    pub fn expected_code(self) -> &'static str {
        match self {
            Self::OversizedBatch => "PAYLOAD_TOO_LARGE",
            Self::AsOfGenZero | Self::AsOfGenFuture => "POINT_IN_TIME_NOT_RETAINED",
            _ => "VALIDATION_ERROR",
        }
    }

    #[must_use]
    pub fn is_write_shaped(self) -> bool {
        matches!(
            self,
            Self::WrongDims | Self::BadIdCharset | Self::EmptyBatch
        )
    }

    #[must_use]
    pub fn tag(self) -> &'static str {
        match self {
            Self::NanVector => "nan-vector",
            Self::WrongDims => "wrong-dims",
            Self::BadIdCharset => "bad-id-charset",
            Self::EmptyBatch => "empty-batch",
            Self::OversizedBatch => "oversized-batch",
            Self::UnknownField => "unknown-field",
            Self::BadCursorToken => "bad-cursor-token",
            Self::GroupingPlusCursor => "grouping-plus-cursor",
            Self::WeightsLenMismatch => "weights-len-mismatch",
            Self::AsOfGenZero => "as-of-410",
            Self::AsOfGenFuture => "as-of-410",
        }
    }
}

impl MaintenanceKind {
    #[must_use]
    pub fn tag(self) -> &'static str {
        match self {
            Self::CompactInline => "sandwich-compact-inline",
            Self::CompactEndpoint => "sandwich-compact-endpoint",
            Self::GcCycle => "sandwich-gc-cycle",
            Self::Hydrate => "sandwich-hydrate",
        }
    }
}

impl GeneratedQuery {
    #[must_use]
    pub fn top_k(&self) -> Option<usize> {
        match self.class {
            QueryOracleClass::ExactAnn { top_k, .. } => Some(top_k),
            QueryOracleClass::Membership { .. } => self
                .body
                .get("top_k")
                .and_then(serde_json::Value::as_u64)
                .map(|value| value as usize),
            QueryOracleClass::ExpectError { .. }
            | QueryOracleClass::Unauthorized
            | QueryOracleClass::Forbidden => None,
        }
    }

    #[must_use]
    pub fn consistency(&self) -> Option<Consistency> {
        match self.class {
            QueryOracleClass::ExactAnn { consistency, .. }
            | QueryOracleClass::Membership { consistency } => Some(consistency),
            QueryOracleClass::ExpectError { .. }
            | QueryOracleClass::Unauthorized
            | QueryOracleClass::Forbidden => None,
        }
    }
}

impl fmt::Display for AsOfTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AsOfTarget::Generation(generation) => write!(f, "{generation}"),
            AsOfTarget::Timestamp(timestamp) => write!(f, "{timestamp}"),
            AsOfTarget::Snapshot(name) => write!(f, "snapshot:{name}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn late_spec() -> NamespaceSpec {
        NamespaceSpec {
            dims: 16,
            metric: DistanceMetric::DotProduct,
            quantization: QuantizationType::None,
            num_centroids: 2,
            fts_fields: Vec::new(),
            bitmap: false,
            late_interaction: Some(LateInteractionSpec {
                candidate_kind: LateCandidateKind::FlatSq8,
                nlist: 2,
                probe_budget: 2,
                candidate_k: 64,
                kmeans_max_iterations: 10,
                max_matrix_object_bytes: 1024 * 1024,
                max_cluster_object_bytes: 1024 * 1024,
                max_resident_bootstrap_bytes: 1024 * 1024,
                read_gap_budget_bytes: 16 * 1024,
                read_max_request_bytes: 1024 * 1024,
                read_max_concurrency: 2,
            }),
        }
    }

    #[test]
    fn late_namespace_body_is_additive_and_legacy_specs_default_dense() {
        let spec = late_spec();
        let body = spec.create_body("seed-late");
        assert_eq!(body["dimensions"], 0);
        assert_eq!(body["index_type"], "late_interaction_fde");
        assert_eq!(body["late_interaction"]["accepted_modalities"][0], "text");
        assert!(body.get("index_config").is_none());
        assert!(!spec.is_exact());

        let legacy: NamespaceSpec = serde_json::from_value(json!({
            "dims": 8,
            "metric": "cosine",
            "quantization": "none",
            "num_centroids": 4,
            "fts_fields": [],
            "bitmap": false
        }))
        .unwrap();
        assert!(legacy.late_interaction.is_none());
        assert!(legacy.is_exact());
        assert!(legacy.create_body("legacy").get("index_config").is_some());
    }

    #[test]
    fn late_operations_wire_replay_metadata() {
        let upsert = Op::LateUpsert {
            actor: ActorSel(3),
            ns: "old-late".to_string(),
            records: vec![LateGenRecord {
                id: "row-1".to_string(),
                values: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
                attributes: None,
            }],
        };
        assert_eq!(upsert.actor(), ActorSel(3));
        assert!(!upsert.is_read_only_request());
        assert_eq!(upsert.kind(), "late_upsert");
        assert!(!upsert.is_security_op());
        assert_eq!(upsert.namespace(), "old-late");
        assert!(upsert.is_mutating());
        assert_eq!(upsert.tags(), vec!["late-interaction"]);
        assert_eq!(
            upsert.rewrite_namespace_prefix("old-", "new-").namespace(),
            "new-late"
        );

        let query = Op::LateQuery {
            actor: ActorSel(4),
            ns: "old-late".to_string(),
            query: vec![vec![1.0, 0.0]],
            top_k: 3,
            filter: None,
            consistency: ConsistencyLevel::Strong,
        };
        assert_eq!(query.actor(), ActorSel(4));
        assert!(query.is_read_only_request());
        assert_eq!(query.kind(), "late_query");
        assert!(!query.is_security_op());
        assert_eq!(query.namespace(), "old-late");
        assert!(!query.is_mutating());
        assert_eq!(query.tags(), vec!["late-interaction"]);
        assert_eq!(
            query.rewrite_namespace_prefix("old-", "new-").namespace(),
            "new-late"
        );
    }

    #[test]
    fn op_record_serializes_execution_and_defaults_legacy_metadata() {
        let record = OpRecord {
            index: 3,
            wall_ms: 0,
            op: Op::GetNamespace {
                actor: ActorSel::ADMIN,
                ns: "ns".to_string(),
            },
            method: "GET".to_string(),
            path: "/v1/namespaces/ns".to_string(),
            status: 200,
            response: serde_json::json!({}),
            outcome: "applied".to_string(),
            target_node: 0,
            execution: ExecutionMetadata {
                phase: ExecutionPhase::Workload,
                hold: Some(HeldExecutionMetadata {
                    event_id: "sched-manifest-hold".to_string(),
                    window_op: 3,
                    scheduled_release_op: Some(7),
                    actual_join_op: 7,
                    release_cause: HoldReleaseCause::LogicalOp,
                }),
            },
            gen_after: None,
            duration_ms: 1,
            violations: Vec::new(),
        };
        let mut encoded = serde_json::to_value(&record).unwrap();
        assert_eq!(encoded["target_node"], 0);
        assert_eq!(encoded["execution"]["phase"], "workload");
        assert_eq!(
            encoded["execution"]["hold"],
            serde_json::json!({
                "event_id": "sched-manifest-hold",
                "window_op": 3,
                "scheduled_release_op": 7,
                "actual_join_op": 7,
                "release_cause": "logical_op",
            })
        );

        encoded.as_object_mut().unwrap().remove("execution");
        encoded.as_object_mut().unwrap().remove("target_node");
        encoded["op"]["GetNamespace"]
            .as_object_mut()
            .expect("GetNamespace payload must be an object")
            .remove("actor");
        let legacy: OpRecord = serde_json::from_value(encoded).unwrap();
        assert_eq!(legacy.target_node, 0);
        assert_eq!(legacy.execution, ExecutionMetadata::default());
        assert_eq!(legacy.execution.phase, ExecutionPhase::Legacy);
        assert_eq!(legacy.execution.hold, None);
        assert_eq!(legacy.op.actor(), ActorSel::ADMIN);
    }

    #[test]
    fn op_record_rejects_target_node_outside_two_node_domain() {
        let record = OpRecord {
            index: 3,
            wall_ms: 0,
            op: Op::GetNamespace {
                actor: ActorSel::ADMIN,
                ns: "ns".to_string(),
            },
            method: "GET".to_string(),
            path: "/v1/namespaces/ns".to_string(),
            status: 200,
            response: serde_json::json!({}),
            outcome: "applied".to_string(),
            target_node: 0,
            execution: ExecutionMetadata::workload(),
            gen_after: None,
            duration_ms: 1,
            violations: Vec::new(),
        };
        let mut encoded = serde_json::to_value(record).unwrap();
        encoded["target_node"] = serde_json::json!(2);

        let error = serde_json::from_value::<OpRecord>(encoded).unwrap_err();
        assert!(error.to_string().contains("target_node"), "{error}");
        assert!(error.to_string().contains("0 or 1"), "{error}");
    }

    #[test]
    fn execution_metadata_separates_scheduled_release_from_quiesced_join() {
        let execution = ExecutionMetadata {
            phase: ExecutionPhase::DeferredDrain,
            hold: Some(HeldExecutionMetadata {
                event_id: "sched-boundary-hold".to_string(),
                window_op: 3,
                scheduled_release_op: Some(7),
                actual_join_op: 5,
                release_cause: HoldReleaseCause::Quiesce,
            }),
        };

        let encoded = serde_json::to_value(&execution).unwrap();
        assert_eq!(encoded["phase"], "deferred_drain");
        assert_eq!(encoded["hold"]["scheduled_release_op"], 7);
        assert_eq!(encoded["hold"]["actual_join_op"], 5);
        assert_eq!(encoded["hold"]["release_cause"], "quiesce");
    }
}
