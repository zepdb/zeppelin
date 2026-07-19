//! Security-to-graph deletion contract.
//!
//! This module contains the typed envelope and callbacks consumed by the
//! authoritative namespace graph. It deliberately carries decisions and
//! governance hooks, never bearer credentials or caller-supplied roots.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sha2::{Digest, Sha256};

use crate::error::Result;
use crate::namespace::manager::NamespaceDeletionIntent;
use crate::namespace::NamespaceId;
use crate::namespace::{BranchId, NamespaceIncarnationId};
use crate::security::{
    DecisionId, PolicyVersion, PreservationGuard, PreservationHeadProof, PrincipalId,
    RootReleaseAuditProgress,
};
use crate::storage::{CreateOnlyOutcome, ZeppelinStore};
use serde::{Deserialize, Serialize};

use super::activation::BranchActivationRecovery;

/// One authorization decision passed from the security adapter to the graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeletionDecision {
    /// Principal that requested deletion.
    pub actor: PrincipalId,
    /// Optional approving principal.
    pub approver: Option<PrincipalId>,
    /// Stable authorization decision identity.
    pub decision_id: DecisionId,
    /// Policy version used for the decision.
    pub policy_version: PolicyVersion,
    /// Opaque durable audit linkage.
    pub decision_evidence_ref: String,
}

/// Governance hooks required at every destructive boundary.
#[async_trait]
pub(crate) trait DeletionGovernance: Send + Sync {
    /// Read fresh preservation authority for the next mutation boundary.
    async fn preservation_boundary(
        &self,
        namespace: &NamespaceId,
        boundary: DeletionBoundary,
    ) -> Result<(PreservationGuard, PreservationHeadProof)>;

    /// Decide whether a child may be disclosed to the current caller.
    fn disclose_child(&self, target: &NamespaceId) -> Result<bool>;

    /// Persist lifecycle audit evidence.
    async fn settle_lifecycle_audit(&self, event: DeletionLifecycleAudit) -> Result<()>;
}

pub(crate) type PreservationCallback = dyn Fn(
        NamespaceId,
        DeletionBoundary,
    )
        -> Pin<Box<dyn Future<Output = Result<(PreservationGuard, PreservationHeadProof)>> + Send>>
    + Send
    + Sync;
pub(crate) type AuditCallback = dyn Fn(DeletionLifecycleAudit) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
    + Send
    + Sync;
pub(crate) type DisclosureCallback = dyn Fn(&NamespaceId) -> Result<bool> + Send + Sync;

/// Source-authorized direct-child listing request with per-target disclosure.
///
/// The closure owns request-scoped security context but never a bearer
/// credential. The graph must invoke it before reading each target's metadata.
pub(crate) struct AuthorizedBranchList {
    /// Source namespace whose bounded direct-root map is listed.
    pub source: NamespaceId,
    disclosure: Arc<DisclosureCallback>,
}

impl AuthorizedBranchList {
    /// Assemble the graph request after source-read authorization succeeds.
    #[must_use]
    pub(crate) fn new(source: NamespaceId, disclosure: Arc<DisclosureCallback>) -> Self {
        Self { source, disclosure }
    }

    /// Apply the request principal's current target-read disclosure decision.
    pub(crate) fn disclose_child(&self, target: &NamespaceId) -> Result<bool> {
        (self.disclosure)(target)
    }
}

/// Callback-backed governance adapter used by the security/server boundary.
pub(crate) struct CallbackDeletionGovernance {
    preservation: Arc<PreservationCallback>,
    disclose: Arc<DisclosureCallback>,
    audit: Arc<AuditCallback>,
}

impl CallbackDeletionGovernance {
    /// Assemble callbacks without carrying bearer credentials into the graph.
    #[must_use]
    pub(crate) fn new(
        preservation: Arc<PreservationCallback>,
        disclose: Arc<DisclosureCallback>,
        audit: Arc<AuditCallback>,
    ) -> Self {
        Self {
            preservation,
            disclose,
            audit,
        }
    }
}

#[async_trait]
impl DeletionGovernance for CallbackDeletionGovernance {
    async fn preservation_boundary(
        &self,
        namespace: &NamespaceId,
        boundary: DeletionBoundary,
    ) -> Result<(PreservationGuard, PreservationHeadProof)> {
        (self.preservation)(namespace.clone(), boundary).await
    }

    fn disclose_child(&self, target: &NamespaceId) -> Result<bool> {
        (self.disclose)(target)
    }

    async fn settle_lifecycle_audit(&self, event: DeletionLifecycleAudit) -> Result<()> {
        (self.audit)(event).await
    }
}

/// Boundary at which preservation authority must be freshly observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeletionBoundary {
    /// Before fencing a never-active fork with its cancellation intent.
    CancellationIntent,
    /// Before publishing the manifest fence.
    Fence,
    /// Before tombstoning metadata.
    Tombstone,
    /// Before removing live visibility.
    VisibilityRemoval,
    /// Before releasing a parent branch root.
    RootRelease,
    /// Before one bounded cleanup batch.
    CleanupBatch,
    /// Before deleting metadata last.
    MetadataRemoval,
}

/// Closed lifecycle-audit vocabulary accepted by deletion governance.
///
/// Keeping this separate from request-level [`crate::security::AuditParams`]
/// makes it impossible for a cleanup worker to emit an unrelated security
/// event through the deletion adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DeletionLifecycleAudit {
    /// Durable progress for a branch deletion's parent-root release.
    NamespaceDeleteRootRelease {
        /// Branch target retaining the deletion intent.
        namespace: NamespaceId,
        /// Typed progress or redacted failure classification.
        progress: RootReleaseAuditProgress,
        /// Opaque canonical linkage to the original authorization decision.
        decision_evidence_ref: String,
    },
    /// A governed deletion cleanup pass exhausted its bounded work budget.
    NamespaceDeleteCleanupIncomplete {
        /// Namespace retaining the durable deletion intent.
        namespace: NamespaceId,
        /// Approximate remaining object count; never a caller disclosure.
        remaining: usize,
        /// Opaque canonical linkage to the original authorization decision.
        decision_evidence_ref: String,
    },
}

/// Immutable security decision evidence installed before a deletion intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeletionDecisionEvidence {
    domain: String,
    decision: DeletionDecision,
}

impl DeletionDecisionEvidence {
    const DOMAIN: &'static str = "zeppelin.namespace-deletion-decision.v1";
}

/// Immutable lifecycle evidence emitted by retry and maintenance workers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeletionLifecycleAuditRecord {
    domain: String,
    event_id: String,
    ts: DateTime<Utc>,
    params: DeletionLifecycleAudit,
}

impl DeletionLifecycleAuditRecord {
    const DOMAIN: &'static str = "zeppelin.namespace-deletion-lifecycle.v1";
}

/// Return the deterministic immutable key referenced by a deletion intent.
#[must_use]
pub(crate) fn deletion_decision_evidence_key(decision_id: DecisionId) -> String {
    format!("_audit/deletion-decisions/{}.json", decision_id.get())
}

/// Create or verify the exact authorization evidence used by one deletion.
pub(crate) async fn persist_deletion_decision_evidence(
    store: &ZeppelinStore,
    decision: &DeletionDecision,
) -> Result<()> {
    let expected_key = deletion_decision_evidence_key(decision.decision_id);
    if decision.decision_evidence_ref != expected_key {
        return Err(crate::error::ZeppelinError::Validation(
            "deletion decision evidence reference is not canonical".to_string(),
        ));
    }
    let record = DeletionDecisionEvidence {
        domain: DeletionDecisionEvidence::DOMAIN.to_string(),
        decision: decision.clone(),
    };
    let body = serde_json::to_vec(&record)
        .map(Bytes::from)
        .map_err(|error| {
            crate::error::ZeppelinError::Serialization(format!(
                "deletion decision evidence encode: {error}"
            ))
        })?;
    match store
        .put_create_outcome(&expected_key, body.clone())
        .await?
    {
        CreateOnlyOutcome::Created { .. } => Ok(()),
        CreateOnlyOutcome::AlreadyExists => {
            let existing = store.get(&expected_key).await?;
            if existing == body {
                Ok(())
            } else {
                Err(crate::error::ZeppelinError::Validation(format!(
                    "deletion decision evidence {expected_key} has conflicting bytes"
                )))
            }
        }
    }
}

/// Load and verify the decision referenced by a durable deletion intent.
pub(crate) async fn load_deletion_decision_evidence(
    store: &ZeppelinStore,
    key: &str,
) -> Result<DeletionDecision> {
    let bytes = store.get(key).await?;
    let record: DeletionDecisionEvidence = serde_json::from_slice(&bytes).map_err(|error| {
        crate::error::ZeppelinError::Serialization(format!(
            "deletion decision evidence decode: {error}"
        ))
    })?;
    if record.domain != DeletionDecisionEvidence::DOMAIN
        || record.decision.decision_evidence_ref != key
        || deletion_decision_evidence_key(record.decision.decision_id) != key
    {
        return Err(crate::error::ZeppelinError::Validation(format!(
            "deletion decision evidence {key} failed identity validation"
        )));
    }
    Ok(record.decision)
}

/// Persist a truthful non-request lifecycle event without synthesizing a user.
pub(crate) async fn persist_deletion_lifecycle_audit(
    store: &ZeppelinStore,
    clock: &crate::time::Clock,
    params: DeletionLifecycleAudit,
) -> Result<()> {
    let event_id = ulid::Ulid::new().to_string();
    let key = format!("_audit/deletion-lifecycle/{event_id}.json");
    let record = DeletionLifecycleAuditRecord {
        domain: DeletionLifecycleAuditRecord::DOMAIN.to_string(),
        event_id,
        ts: clock.now(),
        params,
    };
    let body = serde_json::to_vec(&record)
        .map(Bytes::from)
        .map_err(|error| {
            crate::error::ZeppelinError::Serialization(format!(
                "deletion lifecycle audit encode: {error}"
            ))
        })?;
    match store.put_create_outcome(&key, body.clone()).await? {
        CreateOnlyOutcome::Created { .. } => Ok(()),
        CreateOnlyOutcome::AlreadyExists => {
            let existing = store.get(&key).await?;
            if existing == body {
                Ok(())
            } else {
                Err(crate::error::ZeppelinError::Validation(format!(
                    "deletion lifecycle audit {key} has conflicting bytes"
                )))
            }
        }
    }
}

/// Kernel-minted authorization envelope consumed by `NamespaceGraph::delete`.
pub(crate) struct AuthorizedNamespaceDelete {
    /// Namespace selected for deletion.
    pub namespace: NamespaceId,
    /// Typed authorization decision.
    pub decision: DeletionDecision,
    /// Strong governance hooks for the destructive lifecycle.
    pub governance: Arc<dyn DeletionGovernance>,
    /// Mechanical policy-guard recovery for an ActivationPending target.
    pub activation_recovery: Arc<dyn BranchActivationRecovery>,
}

impl AuthorizedNamespaceDelete {
    /// Construct an envelope after the security layer has completed
    /// authorization and assembled its governance callbacks.
    #[must_use]
    pub(crate) fn new(
        namespace: NamespaceId,
        decision: DeletionDecision,
        governance: Arc<dyn DeletionGovernance>,
        activation_recovery: Arc<dyn BranchActivationRecovery>,
    ) -> Self {
        Self {
            namespace,
            decision,
            governance,
            activation_recovery,
        }
    }
}

/// Stable lifecycle marker proving that a branch target's live visibility was
/// removed. The body intentionally contains no process timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BranchVisibilityRemovalMarker {
    /// Schema discriminator for future marker evolution.
    pub domain: String,
    /// Exact branch edge being retired.
    pub branch_id: BranchId,
    /// Target namespace whose visibility was removed.
    pub target_namespace: NamespaceId,
    /// Target lifetime bound by the marker.
    pub target_incarnation: NamespaceIncarnationId,
    /// Exact manifest generation at which destructive writes were fenced.
    pub fenced_generation: u64,
    /// Immutable destruction evidence bound by the deletion intent.
    pub destruction_record_key: String,
    /// Canonical digest of the immutable portion of the deletion intent.
    pub intent_sha256: String,
    /// Canonical digest of the exact direct-parent branch root.
    pub parent_root_sha256: String,
    /// Exact checked reader-safety floor selected by the marker creator.
    pub reader_safety_floor_secs: u64,
}

impl BranchVisibilityRemovalMarker {
    /// Marker schema discriminator.
    pub const DOMAIN: &'static str = "zeppelin.branch-visibility-removed.v1";

    /// Deterministic marker key under the target-owned lifecycle prefix.
    #[must_use]
    pub(crate) fn key(
        target: &NamespaceId,
        branch_id: BranchId,
        incarnation: NamespaceIncarnationId,
    ) -> String {
        format!(
            "{target}/_lifecycle/branch_visibility_removed/{}.{}.json",
            branch_id.get(),
            incarnation.as_uuid().simple()
        )
    }

    /// Construct the canonical marker body from one fully fenced branch intent.
    pub(crate) fn from_intent(
        target: &NamespaceId,
        branch_id: BranchId,
        intent: &NamespaceDeletionIntent,
        reader_safety_floor_secs: u64,
    ) -> Result<Self> {
        if reader_safety_floor_secs == 0 {
            return Err(crate::error::ZeppelinError::Validation(format!(
                "branch visibility marker for {target} requires a nonzero grace floor"
            )));
        }
        let fenced_generation = intent.fenced_generation.ok_or_else(|| {
            crate::error::ZeppelinError::Validation(format!(
                "branch visibility marker for {target} requires a fenced generation"
            ))
        })?;
        if intent.incarnation.is_nil() {
            return Err(crate::error::ZeppelinError::Validation(format!(
                "branch visibility marker for {target} requires a non-nil target incarnation"
            )));
        }
        let parent_root = intent.parent_root.as_ref().ok_or_else(|| {
            crate::error::ZeppelinError::Validation(format!(
                "branch visibility marker for {target} requires an exact parent root"
            ))
        })?;
        if parent_root.branch_id != branch_id
            || &parent_root.target_namespace != target
            || parent_root.target_incarnation != intent.incarnation
        {
            return Err(crate::error::ZeppelinError::Validation(format!(
                "branch visibility marker for {target} does not match its exact parent root"
            )));
        }

        let mut immutable_intent = intent.clone();
        immutable_intent.visibility = None;
        immutable_intent.root_release = None;
        let intent_sha256 = canonical_json_sha256(&immutable_intent, "deletion intent")?;
        let parent_root_sha256 = canonical_json_sha256(parent_root, "parent branch root")?;

        Ok(Self {
            domain: Self::DOMAIN.to_string(),
            branch_id,
            target_namespace: target.clone(),
            target_incarnation: intent.incarnation.clone(),
            fenced_generation,
            destruction_record_key: intent.destruction_record_key.clone(),
            intent_sha256,
            parent_root_sha256,
            reader_safety_floor_secs,
        })
    }

    /// Validate a pre-existing marker as canonical bytes for this exact branch
    /// identity while adopting the floor durably selected by its creator.
    fn adopt_existing_bytes(&self, existing_bytes: &[u8]) -> Result<Self> {
        let existing: Self = serde_json::from_slice(existing_bytes).map_err(|error| {
            crate::error::ZeppelinError::Validation(format!(
                "existing branch visibility marker is invalid: {error}"
            ))
        })?;
        if existing.reader_safety_floor_secs == 0 {
            return Err(crate::error::ZeppelinError::Validation(
                "existing branch visibility marker has a zero grace floor".to_string(),
            ));
        }

        let mut expected = self.clone();
        expected.reader_safety_floor_secs = existing.reader_safety_floor_secs;
        let expected_bytes = serde_json::to_vec(&expected).map_err(|error| {
            crate::error::ZeppelinError::Serialization(format!("visibility marker encode: {error}"))
        })?;
        if existing_bytes != expected_bytes.as_slice() {
            return Err(crate::error::ZeppelinError::Validation(
                "existing branch visibility marker has conflicting bytes".to_string(),
            ));
        }
        Ok(existing)
    }
}

fn canonical_json_sha256<T: Serialize + ?Sized>(value: &T, label: &str) -> Result<String> {
    let value = serde_json::to_value(value).map_err(|error| {
        crate::error::ZeppelinError::Serialization(format!(
            "branch visibility marker {label} canonicalization failed: {error}"
        ))
    })?;
    let bytes = serde_json::to_vec(&canonicalize_json_value(value)).map_err(|error| {
        crate::error::ZeppelinError::Serialization(format!(
            "branch visibility marker {label} canonical encoding failed: {error}"
        ))
    })?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn canonicalize_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonicalize_json_value).collect())
        }
        serde_json::Value::Object(values) => {
            let ordered = values
                .into_iter()
                .map(|(key, value)| (key, canonicalize_json_value(value)))
                .collect::<std::collections::BTreeMap<_, _>>();
            serde_json::Value::Object(ordered.into_iter().collect())
        }
        scalar => scalar,
    }
}

/// Persist or adopt the exact branch visibility marker and derive its grace
/// deadline from the authoritative S3 object timestamp.
pub(crate) async fn persist_branch_visibility_removal(
    store: &ZeppelinStore,
    target: &NamespaceId,
    branch_id: BranchId,
    intent: &NamespaceDeletionIntent,
    grace_floor: Duration,
) -> Result<super::super::manager::VisibilityRemoval> {
    if grace_floor.subsec_nanos() != 0 {
        return Err(crate::error::ZeppelinError::Validation(
            "branch grace floor must be an exact number of seconds".to_string(),
        ));
    }
    let marker = BranchVisibilityRemovalMarker::from_intent(
        target,
        branch_id,
        intent,
        grace_floor.as_secs(),
    )?;
    let key = BranchVisibilityRemovalMarker::key(target, branch_id, intent.incarnation.clone());
    let body = serde_json::to_vec(&marker).map_err(|error| {
        crate::error::ZeppelinError::Serialization(format!("visibility marker encode: {error}"))
    })?;
    let marker = match store
        .put_create_outcome(&key, Bytes::from(body.clone()))
        .await?
    {
        CreateOnlyOutcome::Created { .. } => marker,
        CreateOnlyOutcome::AlreadyExists => {
            let existing = store.get(&key).await?;
            marker
                .adopt_existing_bytes(existing.as_ref())
                .map_err(|error| {
                    crate::error::ZeppelinError::Validation(format!(
                        "branch visibility marker {key} cannot be adopted: {error}"
                    ))
                })?
        }
    };
    let observed_at = store.head(&key).await?.last_modified;
    visibility_removal_from_marker(&key, &marker, observed_at)
}

/// Load and validate the exact durable visibility marker without recreating it.
///
/// Once the marker-derived deadline is present in namespace metadata, a resume
/// must only adopt the immutable object already bound by that deadline. Creating
/// the key again would mint a new S3 timestamp and could restore a grace window
/// after another worker has durably released the parent root.
pub(crate) async fn load_branch_visibility_removal(
    store: &ZeppelinStore,
    target: &NamespaceId,
    branch_id: BranchId,
    intent: &NamespaceDeletionIntent,
) -> Result<super::super::manager::VisibilityRemoval> {
    let expected_visibility = intent.visibility.as_ref().ok_or_else(|| {
        crate::error::ZeppelinError::Validation(format!(
            "branch visibility marker for {target} has no durable deadline"
        ))
    })?;
    let key = BranchVisibilityRemovalMarker::key(target, branch_id, intent.incarnation.clone());
    if expected_visibility.marker_key != key {
        return Err(crate::error::ZeppelinError::Serialization(format!(
            "branch visibility marker key for {target} does not match its durable intent"
        )));
    }
    let existing = store.get(&key).await?;
    // The creator's floor is encoded in the immutable bytes. A nonzero
    // placeholder is replaced by `adopt_existing_bytes` before comparison.
    let expected = BranchVisibilityRemovalMarker::from_intent(target, branch_id, intent, 1)?;
    let marker = expected
        .adopt_existing_bytes(existing.as_ref())
        .map_err(|error| {
            crate::error::ZeppelinError::Validation(format!(
                "branch visibility marker {key} cannot be adopted: {error}"
            ))
        })?;
    let observed_at = store.head(&key).await?.last_modified;
    let visibility = visibility_removal_from_marker(&key, &marker, observed_at)?;
    if &visibility != expected_visibility {
        return Err(crate::error::ZeppelinError::Validation(format!(
            "branch visibility marker {key} does not match its durable deadline"
        )));
    }
    Ok(visibility)
}

fn visibility_removal_from_marker(
    key: &str,
    marker: &BranchVisibilityRemovalMarker,
    observed_at: DateTime<Utc>,
) -> Result<super::super::manager::VisibilityRemoval> {
    let rounded = observed_at
        .checked_add_signed(ChronoDuration::seconds(1))
        .ok_or_else(|| {
            crate::error::ZeppelinError::Validation("marker timestamp overflow".to_string())
        })?
        .timestamp();
    let floor = ChronoDuration::from_std(Duration::from_secs(marker.reader_safety_floor_secs))
        .map_err(|_| {
            crate::error::ZeppelinError::Validation(
                "branch grace floor exceeds chrono range".to_string(),
            )
        })?;
    let not_before = DateTime::<Utc>::from_timestamp(rounded, 0)
        .ok_or_else(|| {
            crate::error::ZeppelinError::Validation("invalid marker timestamp".to_string())
        })?
        .checked_add_signed(floor)
        .ok_or_else(|| {
            crate::error::ZeppelinError::Validation("branch grace deadline overflow".to_string())
        })?;
    Ok(super::super::manager::VisibilityRemoval {
        marker_key: key.to_string(),
        observed_at,
        not_before,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::{
        BranchVisibilityRemovalMarker, CallbackDeletionGovernance, DeletionBoundary,
        DeletionGovernance, DeletionLifecycleAudit,
    };
    use crate::namespace::manager::{NamespaceDeletionIntent, RootReleaseState, VisibilityRemoval};
    use crate::namespace::{
        BranchId, BranchRoot, ForkViewDigest, ManifestDigest, ManifestGeneration, NamespaceId,
        NamespaceIncarnationId, SourceDataPlaneConfigDigest,
    };
    use crate::security::{PreservationGuard, PreservationHeadProof, RootReleaseAuditProgress};
    use chrono::{DateTime, Utc};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn marker_contract_fixture() -> (NamespaceId, BranchId, NamespaceDeletionIntent) {
        let target = NamespaceId::new("branch-target").expect("valid namespace");
        let branch = BranchId::from_ulid(
            "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .expect("valid branch ULID"),
        );
        let incarnation = NamespaceIncarnationId::from_uuid(
            uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555")
                .expect("valid incarnation UUID"),
        );
        let created_at = DateTime::parse_from_rfc3339("2026-07-17T12:34:56Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let parent_root = BranchRoot {
            branch_id: branch,
            source_generation: ManifestGeneration::new(5).expect("nonzero generation"),
            source_manifest_sha256: ManifestDigest::new([17; 32]),
            fork_view_sha256: ForkViewDigest::new([34; 32]),
            source_config_sha256: SourceDataPlaneConfigDigest::new([51; 32]),
            target_namespace: target.clone(),
            target_incarnation: incarnation.clone(),
            created_at,
        };
        let intent = NamespaceDeletionIntent {
            incarnation,
            destruction_record_key: "_audit/destruction/decision-01.json".to_string(),
            decision_evidence_ref: "_audit/deletion-decisions/01ARZ3NDEKTSV4RRFFQ69G5FAV.json"
                .to_string(),
            parent_root: Some(parent_root),
            fenced_generation: Some(7),
            visibility: None,
            root_release: None,
        };
        (target, branch, intent)
    }

    #[test]
    fn visibility_marker_binds_canonical_intent_and_exact_parent_root() {
        let (target, branch, intent) = marker_contract_fixture();

        let marker = BranchVisibilityRemovalMarker::from_intent(&target, branch, &intent, 31)
            .expect("valid branch deletion intent produces a marker");

        assert_eq!(marker.fenced_generation, 7);
        assert_eq!(
            marker.destruction_record_key,
            "_audit/destruction/decision-01.json"
        );
        assert_eq!(
            marker.intent_sha256,
            "4804e800c2f28164bb8f5a06ee718792e1255ec0fd9e12e1c370dd24122dd86e"
        );
        assert_eq!(
            marker.parent_root_sha256,
            "f830085b757e269821ba95e7ac87a411eceee4507c7c8de337d2f9dc06ef8eb4"
        );
        assert_eq!(marker.reader_safety_floor_secs, 31);
        assert_eq!(marker.intent_sha256.len(), 64);
        assert!(marker
            .intent_sha256
            .bytes()
            .all(|byte| { byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte) }));

        assert_eq!(
            serde_json::to_string(&marker).expect("marker encodes"),
            r#"{"domain":"zeppelin.branch-visibility-removed.v1","branch_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","target_namespace":"branch-target","target_incarnation":"11111111-2222-4333-8444-555555555555","fenced_generation":7,"destruction_record_key":"_audit/destruction/decision-01.json","intent_sha256":"4804e800c2f28164bb8f5a06ee718792e1255ec0fd9e12e1c370dd24122dd86e","parent_root_sha256":"f830085b757e269821ba95e7ac87a411eceee4507c7c8de337d2f9dc06ef8eb4","reader_safety_floor_secs":31}"#
        );
    }

    #[test]
    fn visibility_marker_hash_ignores_later_mutable_intent_fields() {
        let (target, branch, intent) = marker_contract_fixture();
        let expected = BranchVisibilityRemovalMarker::from_intent(&target, branch, &intent, 31)
            .expect("base intent produces a marker");
        let mut advanced = intent;
        advanced.visibility = Some(VisibilityRemoval {
            marker_key: "branch-target/_lifecycle/branch_visibility_removed/existing.json"
                .to_string(),
            observed_at: DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
            not_before: DateTime::parse_from_rfc3339("2026-07-19T00:00:00Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
        });
        advanced.root_release = Some(RootReleaseState::Pending);

        let observed = BranchVisibilityRemovalMarker::from_intent(&target, branch, &advanced, 31)
            .expect("advanced intent produces the same marker");

        assert_eq!(observed, expected);
    }

    #[test]
    fn visibility_marker_rejects_unbound_branch_intents() {
        let (target, branch, intent) = marker_contract_fixture();

        let mut unfenced = intent.clone();
        unfenced.fenced_generation = None;
        assert!(
            BranchVisibilityRemovalMarker::from_intent(&target, branch, &unfenced, 31).is_err()
        );

        let mut rootless = intent.clone();
        rootless.parent_root = None;
        assert!(
            BranchVisibilityRemovalMarker::from_intent(&target, branch, &rootless, 31).is_err()
        );

        let wrong_target = NamespaceId::new("other-target").expect("valid namespace");
        assert!(
            BranchVisibilityRemovalMarker::from_intent(&wrong_target, branch, &intent, 31).is_err()
        );

        let wrong_branch = BranchId::new();
        assert!(
            BranchVisibilityRemovalMarker::from_intent(&target, wrong_branch, &intent, 31).is_err()
        );

        let mut wrong_incarnation = intent.clone();
        wrong_incarnation
            .parent_root
            .as_mut()
            .expect("fixture has a parent root")
            .target_incarnation = NamespaceIncarnationId::new();
        assert!(BranchVisibilityRemovalMarker::from_intent(
            &target,
            branch,
            &wrong_incarnation,
            31
        )
        .is_err());

        assert!(BranchVisibilityRemovalMarker::from_intent(&target, branch, &intent, 0).is_err());
    }

    #[test]
    fn visibility_marker_adopts_creator_floor_without_relaxing_identity_bytes() {
        let (target, branch, intent) = marker_contract_fixture();
        let created = BranchVisibilityRemovalMarker::from_intent(&target, branch, &intent, 31)
            .expect("creator marker is valid");
        let retry = BranchVisibilityRemovalMarker::from_intent(&target, branch, &intent, 61)
            .expect("retry marker is valid");
        let created_bytes = serde_json::to_vec(&created).expect("creator marker encodes");

        let adopted = retry
            .adopt_existing_bytes(&created_bytes)
            .expect("retry adopts the creator-bound floor");

        assert_eq!(adopted, created);

        let mut conflicting = created.clone();
        conflicting.destruction_record_key = "_audit/destruction/other.json".to_string();
        let conflicting_bytes = serde_json::to_vec(&conflicting).expect("conflict marker encodes");
        assert!(retry.adopt_existing_bytes(&conflicting_bytes).is_err());

        let noncanonical_bytes =
            serde_json::to_vec_pretty(&created).expect("pretty marker encodes");
        assert!(retry.adopt_existing_bytes(&noncanonical_bytes).is_err());
    }

    #[test]
    fn visibility_marker_is_deterministic_and_strict() {
        let (target, branch, intent) = marker_contract_fixture();
        let incarnation = intent.incarnation.clone();
        let marker = BranchVisibilityRemovalMarker::from_intent(&target, branch, &intent, 31)
            .expect("valid branch deletion intent produces a marker");
        let encoded = serde_json::to_vec(&marker).expect("marker encodes");
        let decoded: BranchVisibilityRemovalMarker =
            serde_json::from_slice(&encoded).expect("marker decodes");
        assert_eq!(decoded, marker);
        assert_eq!(marker.domain, BranchVisibilityRemovalMarker::DOMAIN);
        assert_eq!(
            BranchVisibilityRemovalMarker::key(&target, branch, incarnation.clone()),
            format!(
                "branch-target/_lifecycle/branch_visibility_removed/{}.{}.json",
                branch.get(),
                incarnation.as_uuid().simple()
            )
        );
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        value["unexpected"] = serde_json::Value::Bool(true);
        assert!(serde_json::from_value::<BranchVisibilityRemovalMarker>(value).is_err());
    }

    #[tokio::test]
    async fn callback_governance_forwards_all_hooks() {
        let preservation_calls = Arc::new(AtomicUsize::new(0));
        let audit_calls = Arc::new(AtomicUsize::new(0));
        let disclose_calls = Arc::new(AtomicUsize::new(0));
        let preservation_calls_for_cb = Arc::clone(&preservation_calls);
        let audit_calls_for_cb = Arc::clone(&audit_calls);
        let disclose_calls_for_cb = Arc::clone(&disclose_calls);
        let adapter = CallbackDeletionGovernance::new(
            Arc::new(move |_namespace, _boundary| {
                let calls = Arc::clone(&preservation_calls_for_cb);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok((
                        PreservationGuard::unlocked(),
                        PreservationHeadProof {
                            head_sha256: [0; 32],
                            e_tag: None,
                        },
                    ))
                })
            }),
            Arc::new(move |_target| {
                disclose_calls_for_cb.fetch_add(1, Ordering::SeqCst);
                Ok(true)
            }),
            Arc::new(move |_event| {
                let calls = Arc::clone(&audit_calls_for_cb);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            }),
        );
        let target = NamespaceId::new("adapter-target").unwrap();
        adapter
            .preservation_boundary(&target, DeletionBoundary::Fence)
            .await
            .unwrap();
        assert!(adapter.disclose_child(&target).unwrap());
        adapter
            .settle_lifecycle_audit(DeletionLifecycleAudit::NamespaceDeleteCleanupIncomplete {
                namespace: target,
                remaining: 1,
                decision_evidence_ref: "test-decision".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(preservation_calls.load(Ordering::SeqCst), 1);
        assert_eq!(disclose_calls.load(Ordering::SeqCst), 1);
        assert_eq!(audit_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn root_release_lifecycle_event_uses_typed_progress_not_a_boolean() {
        let params = DeletionLifecycleAudit::NamespaceDeleteRootRelease {
            namespace: NamespaceId::new("branch-target").unwrap(),
            progress: RootReleaseAuditProgress::Released,
            decision_evidence_ref: "decision-evidence".to_string(),
        };
        let value = serde_json::to_value(params).unwrap();
        assert_eq!(
            value["namespace_delete_root_release"]["progress"],
            "released"
        );
        assert!(value["namespace_delete_root_release"]
            .get("converged")
            .is_none());
        assert!(value["namespace_delete_root_release"]
            .get("error")
            .is_none());
    }
}
