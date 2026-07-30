//! Feature-gated synthetic foreign-origin fixtures for external integration tests.
//!
//! This module exposes feature-only adapters for exercising private branch-root
//! and deletion-fence publication primitives. Its synthetic foreign-origin
//! query fixtures still create target-bound manifests only in memory and hand
//! those snapshots to the same supplied-manifest paths used by production batch
//! and historical execution. Persisting synthetic target bytes still passes
//! through normal manifest admission and is rejected until lineage authorization
//! lands.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::cache::hydration::HydrationTarget;
use crate::cache::manifest_cache::ManifestCache;
use crate::cache::DiskCache;
use crate::config::{
    BranchingConfig, Config, IndexingConfig, SecurityMode, DEFAULT_RERANK_COALESCE_GAP_BYTES,
};
use crate::error::{Result, ZeppelinError};
use crate::fts::rank_by::RankBy;
use crate::fts::FtsFieldConfig;
use crate::query::{
    execute_bm25_query_with_manifest, execute_bm25_query_with_manifest_debug,
    execute_query_with_manifest, execute_query_with_manifest_debug, QueryParams,
};
use crate::storage::ZeppelinStore;
use crate::time::Clock;
use crate::types::{
    AttributeValue, ConsistencyLevel, DistanceMetric, Filter, IndexType, SearchResult, VectorId,
};
use crate::wal::manifest::{Manifest, ManifestBindingVersion};
use crate::wal::{LeaseManager, WalReader};
use chrono::{DateTime, Utc};

use super::activation::{BranchActivationGuard, BranchActivationRecovery, BranchActivationTarget};
use super::deletion::{
    deletion_decision_evidence_key, AuthorizedBranchList, AuthorizedNamespaceDelete,
    DeletionBoundary, DeletionDecision, DeletionGovernance, DeletionLifecycleAudit,
};
use super::{
    ActivationNonce, ArtifactOrigin, ArtifactOriginIndex, BranchActivationEvidence,
    BranchDescriptor, BranchId, BranchLineage, BranchMaintenanceReport, BranchPrepareStage,
    BranchRoot, ForkIdentity, ForkReservationIdentity, ForkViewDigest, ManifestGeneration,
    NamespaceCreationKind, NamespaceDeleteOutcome, PolicyHeadIdentity, PrepareForkOutcome,
    PrepareForkRequest,
};
use crate::namespace::branch_root::{
    insert_branch_root, remove_branch_root, source_data_plane_config_digest,
    InsertBranchRootRequest, RemoveBranchRootRequest,
};
use crate::namespace::graph::{BranchMaintenanceMemo, BranchMaintenancePolicy, NamespaceGraph};
use crate::namespace::manager::NamespaceIndexConfig;
use crate::namespace::{NamespaceId, NamespaceIncarnationId, NamespaceManager};
use crate::security::{
    DecisionId, PolicyVersion, PreservationGuard, PreservationHeadProof, PrincipalId,
};

/// Observable result of one ANN query through a synthetic target manifest.
#[derive(Debug, Clone)]
pub struct SyntheticForeignQueryResult {
    /// Ranked vector IDs returned by the production ANN merge path.
    pub ids: Vec<String>,
    /// Complete ranked hits, including projected attributes when requested.
    pub results: Vec<SearchResult>,
    /// Number of visible WAL fragments scored by the query.
    pub scanned_fragments: usize,
    /// Number of active immutable segments searched by the query.
    pub scanned_segments: usize,
    /// Whether the production diagnostic path emitted a debug block.
    pub debug_present: bool,
    /// Immutable-cache hits observed inside the diagnostic scope.
    pub cache_hits: u64,
    /// Immutable-cache misses observed inside the diagnostic scope.
    pub cache_misses: u64,
}

/// One exact by-ID record projected through the production lookup seam.
#[derive(Debug, Clone, PartialEq)]
pub struct SyntheticForeignFetchRecord {
    /// Stable logical vector ID.
    pub id: VectorId,
    /// Full coordinates when requested by the fixture projection.
    pub values: Option<Vec<f32>>,
    /// Complete or field-projected attributes when requested.
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

/// Observable exact-fetch result for one synthetic target view.
#[derive(Debug, Clone, PartialEq)]
pub struct SyntheticForeignFetchResult {
    /// Live projected records in request-relative order.
    pub records: Vec<SyntheticForeignFetchRecord>,
    /// Absent or tombstoned IDs in request-relative order.
    pub missing: Vec<VectorId>,
}

/// Test-only observation of one live manifest's branch-control state.
#[derive(Debug, Clone)]
pub struct BranchControlSnapshot {
    /// Exact direct-child roots in deterministic key order.
    pub roots: Vec<BranchRoot>,
    /// Whether the same authoritative manifest observation carried a delete fence.
    pub deletion_fenced: bool,
    /// Execution/control projection version bound to the live generation.
    pub binding_version: Option<ManifestBindingVersion>,
    /// Exact live manifest generation from the same observation.
    pub manifest_generation: u64,
    /// Writer fencing token from the same observation.
    pub fencing_token: u64,
}

/// Test-only observation of one non-visible fork reservation.
#[derive(Debug, Clone)]
pub struct BranchMetadataSnapshot {
    /// Persisted namespace visibility state.
    pub state: &'static str,
    /// Monotonic preparation milestone.
    pub prepare_stage: Option<BranchPrepareStage>,
    /// Immutable identity installed after the source root wins.
    pub branch_identity: Option<ForkIdentity>,
    /// Stable create-only reservation identity.
    pub reservation: ForkReservationIdentity,
}

/// Test-only logical view of one prepared target manifest.
#[derive(Debug, Clone)]
pub struct PreparedManifestSnapshot {
    /// Exact target manifest generation.
    pub generation: u64,
    /// Immutable target lineage.
    pub lineage: BranchLineage,
    /// Logical target namespace binding.
    pub target_namespace: String,
    /// Target lifetime binding.
    pub target_incarnation: NamespaceIncarnationId,
    /// Resolved physical owners of retained segments.
    pub segment_origins: Vec<ArtifactOrigin>,
    /// Resolved physical owners of retained WAL fragments.
    pub fragment_origins: Vec<ArtifactOrigin>,
}

fn graph_for_test(
    store: ZeppelinStore,
    indexing: IndexingConfig,
    branching: BranchingConfig,
) -> Result<NamespaceGraph> {
    let mut floor_config = Config::default();
    // Keep branch-grace tests fast without bypassing the production GC-floor
    // calculation or enabling the unsafe short-horizon override.
    floor_config.security.mode = SecurityMode::OpenUnsafe;
    floor_config.cache.manifest_cache_ttl_ms = 0;
    floor_config.cache.namespace_registry_ttl_ms = 0;
    floor_config.server.request_timeout_secs = 1;
    floor_config.gc.compaction_upload_window_secs = 1;
    floor_config.gc.skew_slop_secs = 0;
    floor_config.gc.horizon_secs = 2;
    floor_config.validate()?;
    let gc_horizon_floor_secs = floor_config.gc_horizon_floor_secs().ok_or_else(|| {
        ZeppelinError::Config("test GC reader-safety floor overflowed u64".to_string())
    })?;

    let clock = Clock::system();
    let namespace_manager = Arc::new(NamespaceManager::new(store.clone()));
    let lease_manager = Arc::new(LeaseManager::with_clock(
        store.clone(),
        format!("branch-prepare-test-{}", ulid::Ulid::new()),
        Duration::from_secs(30),
        clock.clone(),
    ));
    Ok(NamespaceGraph::new(
        store,
        namespace_manager,
        lease_manager,
        clock,
        Arc::new(ManifestCache::new(Duration::ZERO)),
        branching,
        indexing,
        Some(gc_horizon_floor_secs),
    ))
}

fn graph_for_test_with_config_and_clock(
    store: ZeppelinStore,
    config: &Config,
    clock: Clock,
) -> Result<NamespaceGraph> {
    config.validate()?;
    let gc_horizon_floor_secs = config.gc_horizon_floor_secs().ok_or_else(|| {
        ZeppelinError::Config("test GC reader-safety floor overflowed u64".to_string())
    })?;
    let namespace_manager = Arc::new(NamespaceManager::with_clock(
        store.clone(),
        Duration::from_millis(config.cache.namespace_registry_ttl_ms),
        clock.clone(),
    ));
    let lease_manager = Arc::new(LeaseManager::with_clock(
        store.clone(),
        format!("branch-delete-test-{}", ulid::Ulid::new()),
        Duration::from_secs(config.compaction.lease_duration_secs),
        clock.clone(),
    ));
    Ok(NamespaceGraph::new(
        store,
        namespace_manager,
        lease_manager,
        clock,
        Arc::new(ManifestCache::new(Duration::from_millis(
            config.cache.manifest_cache_ttl_ms,
        ))),
        config.branching.clone(),
        config.indexing.clone(),
        Some(gc_horizon_floor_secs),
    ))
}

struct TestDeletionGovernance;

struct TestBranchActivationRecovery;

/// Stateful feature-only branch-maintenance runner.
///
/// Integration tests retain this value across ticks to exercise the same
/// process-local memo lifecycle as [`crate::compaction::background::GovernedDeletionWorker`]
/// without starting the background scheduler.
pub struct BranchMaintenanceRunnerForTest {
    store: ZeppelinStore,
    clock: Clock,
    graph: NamespaceGraph,
    policy: BranchMaintenancePolicy,
    namespace_prefix: Option<String>,
    memo: Option<BranchMaintenanceMemo>,
}

impl BranchMaintenanceRunnerForTest {
    /// Compose one cold runner from an exact validated configuration.
    pub fn new(store: ZeppelinStore, config: &Config, clock: Clock) -> Result<Self> {
        Self::new_scoped(store, config, clock, None)
    }

    /// Compose one cold runner restricted to a lexical test namespace prefix.
    pub fn new_scoped(
        store: ZeppelinStore,
        config: &Config,
        clock: Clock,
        namespace_prefix: Option<String>,
    ) -> Result<Self> {
        let graph = graph_for_test_with_config_and_clock(store.clone(), config, clock.clone())?;
        let policy = BranchMaintenancePolicy::new(
            &config.branching,
            config.gc.horizon_secs,
            config.gc_horizon_floor_secs(),
            config.compaction.interval_secs,
        );
        Ok(Self {
            store,
            clock,
            graph,
            policy,
            namespace_prefix,
            memo: None,
        })
    }

    /// Run one tick while retaining disposable memo state on success.
    pub async fn run(&mut self, budget: Duration) -> Result<BranchMaintenanceReport> {
        self.graph
            .maintain_memoized(
                Arc::new(TestDeletionGovernance),
                Arc::new(TestBranchActivationRecovery),
                budget,
                &self.policy,
                self.namespace_prefix.as_deref(),
                &mut self.memo,
            )
            .await
    }

    /// Replace boot policy while retaining the prior memo for fail-cold testing.
    pub fn update_config(&mut self, config: &Config) -> Result<()> {
        self.graph =
            graph_for_test_with_config_and_clock(self.store.clone(), config, self.clock.clone())?;
        self.policy = BranchMaintenancePolicy::new(
            &config.branching,
            config.gc.horizon_secs,
            config.gc_horizon_floor_secs(),
            config.compaction.interval_secs,
        );
        Ok(())
    }
}

#[async_trait]
impl BranchActivationRecovery for TestBranchActivationRecovery {
    async fn retain_branch(
        &self,
        _target: &BranchActivationTarget,
    ) -> Result<Option<Box<dyn BranchActivationGuard>>> {
        Ok(None)
    }

    async fn retain_next_expired(&self) -> Result<Option<Box<dyn BranchActivationGuard>>> {
        Ok(None)
    }
}

#[async_trait]
impl DeletionGovernance for TestDeletionGovernance {
    async fn preservation_boundary(
        &self,
        _namespace: &NamespaceId,
        _boundary: DeletionBoundary,
    ) -> Result<(PreservationGuard, PreservationHeadProof)> {
        Ok((
            PreservationGuard::unlocked(),
            PreservationHeadProof {
                head_sha256: [0; 32],
                e_tag: None,
            },
        ))
    }

    fn disclose_child(&self, _target: &NamespaceId) -> Result<bool> {
        Ok(true)
    }

    async fn settle_lifecycle_audit(&self, _event: DeletionLifecycleAudit) -> Result<()> {
        Ok(())
    }
}

/// Run the private prepare-only graph protocol through its production seam.
pub async fn prepare_fork_for_test(
    store: ZeppelinStore,
    source: NamespaceId,
    target: NamespaceId,
    indexing: IndexingConfig,
    branching: BranchingConfig,
) -> Result<PrepareForkOutcome> {
    graph_for_test(store, indexing, branching)?
        .prepare_fork(PrepareForkRequest { source, target })
        .await
}

/// Stop deterministically at a retained activation attempt.
///
/// The target reaches [`BranchPrepareStage::ActivationPending`] and stays
/// there, which is the exact state whose later cancellation records an
/// activation nonce on the deletion intent. Tests use this to prove that
/// unattended maintenance still recovers an activation-cancelled fork.
pub async fn prepare_fork_until_activation_pending_for_test(
    store: ZeppelinStore,
    source: NamespaceId,
    target: NamespaceId,
    indexing: IndexingConfig,
    branching: BranchingConfig,
) -> Result<ActivationNonce> {
    let outcome =
        prepare_fork_for_test(store.clone(), source, target.clone(), indexing, branching).await?;
    let identity = match &outcome {
        PrepareForkOutcome::Prepared(branch) | PrepareForkOutcome::ExistingPrepared(branch) => {
            branch.identity.clone()
        }
    };
    let nonce = ActivationNonce::new();
    NamespaceManager::new(store)
        .begin_branch_activation(&target, &identity, nonce)
        .await?;
    Ok(nonce)
}

/// Prepare and activate a branch for compaction/query integration tests.
/// Production callers must supply the security and approval proof separately.
pub async fn activate_fork_for_test(
    store: ZeppelinStore,
    source: NamespaceId,
    target: NamespaceId,
    indexing: IndexingConfig,
    branching: BranchingConfig,
) -> Result<PrepareForkOutcome> {
    let outcome =
        prepare_fork_for_test(store.clone(), source, target.clone(), indexing, branching).await?;
    let manager = NamespaceManager::new(store);
    let identity = match &outcome {
        PrepareForkOutcome::Prepared(branch) | PrepareForkOutcome::ExistingPrepared(branch) => {
            branch.identity.clone()
        }
    };
    let nonce = ActivationNonce::new();
    manager
        .begin_branch_activation(&target, &identity, nonce)
        .await?;
    manager
        .commit_branch_activation(
            &target,
            &identity,
            BranchActivationEvidence {
                branch_id: identity.branch_id,
                target_namespace: identity.target_namespace.clone(),
                target_incarnation: identity.target_incarnation.clone(),
                policy_head: PolicyHeadIdentity::Boot {
                    activation_nonce: nonce,
                },
                decision_id: DecisionId::new(),
                approver: None,
                audit_evidence_ref: format!("test-boot-branch-activation:{nonce}"),
                activated_at: Utc::now(),
            },
        )
        .await?;
    Ok(outcome)
}

/// Stop deterministically after the exact source root CAS.
pub async fn prepare_fork_until_root_for_test(
    store: ZeppelinStore,
    source: NamespaceId,
    target: NamespaceId,
    indexing: IndexingConfig,
    branching: BranchingConfig,
) -> Result<()> {
    graph_for_test(store, indexing, branching)?
        .prepare_fork_until_root_for_test(PrepareForkRequest { source, target })
        .await
}

/// Stop deterministically after the create-only target reservation.
pub async fn prepare_fork_until_reserved_for_test(
    store: ZeppelinStore,
    source: NamespaceId,
    target: NamespaceId,
    indexing: IndexingConfig,
    branching: BranchingConfig,
) -> Result<()> {
    graph_for_test(store, indexing, branching)?
        .prepare_fork_until_reserved_for_test(PrepareForkRequest { source, target })
        .await
}

/// Run one bounded governed-deletion and branch-control maintenance pass.
pub async fn maintain_branches_for_test(
    store: ZeppelinStore,
    indexing: IndexingConfig,
    branching: BranchingConfig,
    budget: Duration,
) -> Result<BranchMaintenanceReport> {
    graph_for_test(store, indexing, branching)?
        .maintain(
            Arc::new(TestDeletionGovernance),
            Arc::new(TestBranchActivationRecovery),
            budget,
        )
        .await
}

/// Run bounded graph maintenance with an exact validated config and clock.
///
/// This keeps grace-boundary recovery tests deterministic without weakening
/// the production reader-safety floor or sleeping on wall time.
pub async fn maintain_branches_with_config_and_clock_for_test(
    store: ZeppelinStore,
    config: &Config,
    clock: Clock,
    budget: Duration,
) -> Result<BranchMaintenanceReport> {
    graph_for_test_with_config_and_clock(store, config, clock)?
        .maintain(
            Arc::new(TestDeletionGovernance),
            Arc::new(TestBranchActivationRecovery),
            budget,
        )
        .await
}

/// Exercise graph-owned deletion through the feature-only test seam.
pub async fn delete_namespace_for_test(
    store: ZeppelinStore,
    namespace: NamespaceId,
    indexing: IndexingConfig,
    branching: BranchingConfig,
) -> Result<NamespaceDeleteOutcome> {
    let decision_id = DecisionId::new();
    graph_for_test(store, indexing, branching)?
        .delete(AuthorizedNamespaceDelete {
            namespace,
            decision: DeletionDecision {
                actor: PrincipalId::new("test-delete-actor")
                    .unwrap_or_else(|_| panic!("test delete actor must be a valid principal")),
                approver: None,
                decision_id,
                policy_version: PolicyVersion::BOOT,
                decision_evidence_ref: deletion_decision_evidence_key(decision_id),
            },
            governance: Arc::new(TestDeletionGovernance),
            activation_recovery: Arc::new(TestBranchActivationRecovery),
        })
        .await
}

/// Resume an ordinary deleting namespace through the graph retry seam.
pub async fn resume_delete_for_test(
    store: ZeppelinStore,
    namespace: NamespaceId,
    indexing: IndexingConfig,
    branching: BranchingConfig,
    budget: Duration,
) -> Result<NamespaceDeleteOutcome> {
    graph_for_test(store, indexing, branching)?
        .resume_delete(
            &namespace,
            Arc::new(TestDeletionGovernance),
            Arc::new(TestBranchActivationRecovery),
            budget,
        )
        .await
}

/// Resume deletion with the caller's validated production config and clock.
///
/// This is intentionally feature-only: integration tests use it to exercise
/// exact grace-deadline and lease boundaries without sleeping.
pub async fn resume_delete_with_config_and_clock_for_test(
    store: ZeppelinStore,
    namespace: NamespaceId,
    config: &Config,
    clock: Clock,
    budget: Duration,
) -> Result<NamespaceDeleteOutcome> {
    graph_for_test_with_config_and_clock(store, config, clock)?
        .resume_delete(
            &namespace,
            Arc::new(TestDeletionGovernance),
            Arc::new(TestBranchActivationRecovery),
            budget,
        )
        .await
}

/// Exercise authoritative direct-child listing through the graph seam.
pub async fn list_children_for_test(
    store: ZeppelinStore,
    source: NamespaceId,
    indexing: IndexingConfig,
    branching: BranchingConfig,
) -> Result<Vec<BranchDescriptor>> {
    graph_for_test(store, indexing, branching)?
        .list_children(AuthorizedBranchList::new(
            source,
            Arc::new(|_target| Ok(true)),
        ))
        .await
}

/// Read one authoritative non-visible metadata reservation for assertions.
pub async fn branch_metadata_snapshot(
    store: &ZeppelinStore,
    target: &str,
) -> Result<BranchMetadataSnapshot> {
    let manager = NamespaceManager::new(store.clone());
    let (metadata, _) = manager.read_creating_intent_strong(target).await?;
    let NamespaceCreationKind::Fork(reservation) = metadata.creation_kind else {
        return Err(ZeppelinError::Serialization(format!(
            "test branch target {target} is not a fork reservation"
        )));
    };
    Ok(BranchMetadataSnapshot {
        state: metadata.state.as_str(),
        prepare_stage: metadata.branch_prepare.map(|prepare| prepare.stage),
        branch_identity: metadata.branch_identity,
        reservation,
    })
}

/// Read the live manifest's namespace-lifetime binding without migrating it.
pub async fn manifest_incarnation_for_test(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<Option<NamespaceIncarnationId>> {
    Ok(Manifest::read(store, namespace)
        .await?
        .and_then(|manifest| manifest.namespace_incarnation())
        .map(NamespaceIncarnationId::from_uuid))
}

/// Resolve every visible descriptor to its exact physical namespace lifetime.
pub async fn prepared_manifest_snapshot(
    store: &ZeppelinStore,
    target: &str,
) -> Result<PreparedManifestSnapshot> {
    let manager = NamespaceManager::new(store.clone());
    let (metadata, _) = manager.read_creating_intent_strong(target).await?;
    let target_incarnation = metadata.incarnation_id.ok_or_else(|| {
        ZeppelinError::Serialization(format!(
            "prepared branch target {target} has no incarnation"
        ))
    })?;
    let (manifest, _) = Manifest::read_versioned_required_for_incarnation(
        store,
        target,
        target_incarnation.as_uuid(),
    )
    .await?;
    let lineage = manifest.branch_lineage().cloned().ok_or_else(|| {
        ZeppelinError::Serialization(format!("prepared branch target {target} has no lineage"))
    })?;
    let segment_origins = manifest
        .segments
        .iter()
        .map(|segment| manifest.segment_origin(segment))
        .collect::<Result<Vec<_>>>()?;
    let fragment_origins = manifest
        .fragments
        .iter()
        .map(|fragment| manifest.fragment_origin(fragment))
        .collect::<Result<Vec<_>>>()?;
    Ok(PreparedManifestSnapshot {
        generation: manifest.version(),
        lineage,
        target_namespace: target.to_string(),
        target_incarnation,
        segment_origins,
        fragment_origins,
    })
}

/// Build the exact root body for the currently authoritative source head.
///
/// This feature-only adapter performs no mutation. It lets external MinIO
/// tests prepare one body and retry that identical body through the private
/// one-shot primitive, matching the Phase-05 orchestrator contract.
#[allow(clippy::too_many_arguments)]
pub async fn prepare_head_branch_root(
    store: ZeppelinStore,
    source_namespace: &str,
    branch_id: BranchId,
    target_namespace: &str,
    target_incarnation: uuid::Uuid,
    fork_view_sha256: ForkViewDigest,
    created_at: DateTime<Utc>,
) -> Result<BranchRoot> {
    let manager = NamespaceManager::new(store.clone());
    let metadata = manager
        .get_active_metadata_for_guarded_write(source_namespace)
        .await?;
    let source_incarnation = metadata.incarnation_id.as_ref().ok_or_else(|| {
        ZeppelinError::Serialization(format!(
            "active namespace {source_namespace} has no authoritative incarnation"
        ))
    })?;
    let (manifest, version) = Manifest::read_versioned_required_for_incarnation(
        &store,
        source_namespace,
        source_incarnation.as_uuid(),
    )
    .await?;
    if version.is_deletion_fenced() {
        return Err(ZeppelinError::NamespaceDeleting {
            namespace: source_namespace.to_string(),
        });
    }
    let target_namespace = NamespaceId::parse(target_namespace.to_string()).map_err(|_| {
        ZeppelinError::Validation(format!(
            "invalid branch target namespace: {target_namespace}"
        ))
    })?;
    let resolved_index_config = if metadata.index_type == IndexType::LateInteractionFde {
        None
    } else {
        Some(metadata.index_config.clone().unwrap_or_else(|| {
            NamespaceIndexConfig::from_indexing_config(&IndexingConfig::default())
        }))
    };
    Ok(BranchRoot {
        branch_id,
        source_generation: ManifestGeneration::new(manifest.version())?,
        source_manifest_sha256: version.exact_manifest_digest()?,
        fork_view_sha256,
        source_config_sha256: source_data_plane_config_digest(
            &metadata,
            resolved_index_config.as_ref(),
        )?,
        target_namespace,
        target_incarnation: NamespaceIncarnationId::from_uuid(target_incarnation),
        created_at,
    })
}

/// Publish one already-prepared exact root through the private production primitive.
pub async fn insert_prepared_branch_root(
    store: ZeppelinStore,
    source_namespace: &str,
    root: BranchRoot,
    max_children: usize,
) -> Result<BranchRoot> {
    let source_namespace = NamespaceId::parse(source_namespace.to_string()).map_err(|_| {
        ZeppelinError::Validation(format!(
            "invalid branch source namespace: {source_namespace}"
        ))
    })?;
    let namespace_manager = NamespaceManager::new(store.clone());
    let lease_manager = LeaseManager::new(
        store.clone(),
        format!("branch-test-{}", ulid::Ulid::new()),
        Duration::from_secs(30),
    );
    insert_branch_root(
        &store,
        &namespace_manager,
        &lease_manager,
        InsertBranchRootRequest {
            source_namespace,
            root,
            max_children,
        },
    )
    .await
}

/// Remove one exact root through the private production primitive.
pub async fn remove_prepared_branch_root(
    store: ZeppelinStore,
    source_namespace: &str,
    expected_root: BranchRoot,
) -> Result<()> {
    let source_namespace = NamespaceId::parse(source_namespace.to_string()).map_err(|_| {
        ZeppelinError::Validation(format!(
            "invalid branch source namespace: {source_namespace}"
        ))
    })?;
    let namespace_manager = NamespaceManager::new(store.clone());
    let source_metadata = namespace_manager
        .get_active_metadata_for_guarded_write(source_namespace.as_str())
        .await?;
    let expected_source_incarnation = source_metadata.incarnation_id.ok_or_else(|| {
        ZeppelinError::Serialization(format!(
            "active namespace {source_namespace} has no authoritative incarnation"
        ))
    })?;
    let lease_manager = LeaseManager::new(
        store.clone(),
        format!("branch-test-remove-{}", ulid::Ulid::new()),
        Duration::from_secs(30),
    );
    remove_branch_root(
        &store,
        &namespace_manager,
        &lease_manager,
        RemoveBranchRootRequest {
            source_namespace,
            expected_source_incarnation,
            expected_root,
        },
    )
    .await
}

/// Publish the normal governed-destruction fence through a feature-only adapter.
pub async fn publish_deletion_fence(
    store: ZeppelinStore,
    namespace: &str,
    destruction_record_key: &str,
) -> Result<()> {
    Manifest::fence_for_destruction(&store, namespace, destruction_record_key)
        .await
        .map(|_| ())
}

/// Read one exact live branch-control observation for external assertions.
pub async fn branch_control_snapshot(
    store: &ZeppelinStore,
    namespace: &str,
) -> Result<BranchControlSnapshot> {
    let (manifest, version) = Manifest::read_versioned_required(store, namespace).await?;
    Ok(BranchControlSnapshot {
        roots: manifest.branch_roots().values().cloned().collect(),
        deletion_fenced: version.is_deletion_fenced(),
        binding_version: manifest.manifest_binding_version(),
        manifest_generation: manifest.version(),
        fencing_token: manifest.fencing_token(),
    })
}

/// Publish a higher manifest fencing token for a stale-writer regression.
pub async fn publish_manifest_fencing_token(
    store: &ZeppelinStore,
    namespace: &str,
    fencing_token: u64,
) -> Result<()> {
    let (mut manifest, version) = Manifest::read_versioned_required(store, namespace).await?;
    if fencing_token <= manifest.fencing_token() {
        return Err(ZeppelinError::Validation(format!(
            "test fencing token {fencing_token} must exceed live token {}",
            manifest.fencing_token()
        )));
    }
    manifest.fencing_token = fencing_token;
    manifest
        .write_conditional(store, namespace, &version)
        .await
        .map(|_| ())
}

/// One positional query request executed against a shared synthetic manifest.
#[derive(Debug, Clone)]
pub enum SyntheticForeignQuerySpec {
    /// Approximate-neighbor retrieval.
    Ann {
        /// Query coordinates.
        query: Vec<f32>,
        /// Maximum returned candidates.
        top_k: usize,
        /// IVF or hierarchical probe width.
        nprobe: usize,
    },
    /// Full-text retrieval.
    Bm25 {
        /// Lexical scoring expression.
        rank_by: RankBy,
        /// Maximum returned candidates.
        top_k: usize,
    },
    /// ANN plus BM25 reduced by the production default RRF implementation.
    Hybrid {
        /// ANN query coordinates.
        query: Vec<f32>,
        /// Lexical scoring expression.
        rank_by: RankBy,
        /// Maximum fused candidates.
        top_k: usize,
        /// IVF or hierarchical probe width.
        nprobe: usize,
    },
}

/// Opaque target-bound view whose descriptors point at one source lifetime.
///
/// The view is never written to object storage. Callers can execute the
/// production supplied-manifest query seam and can independently confirm that
/// normal persisted-manifest decoding remains fail-closed.
#[derive(Clone)]
pub struct SyntheticForeignOriginView {
    store: ZeppelinStore,
    target_namespace: String,
    target_origin: ArtifactOrigin,
    manifest: Manifest,
}

impl SyntheticForeignOriginView {
    /// Load a source manifest and build an unpublished target-bound view.
    ///
    /// Every fragment and segment descriptor is assigned the source's physical
    /// origin. No source object is copied and no target manifest is published.
    pub async fn from_source(
        store: ZeppelinStore,
        source_namespace: &str,
        target_namespace: &str,
    ) -> Result<Self> {
        let source_manifest = Manifest::read(&store, source_namespace)
            .await?
            .ok_or_else(|| ZeppelinError::ManifestNotFound {
                namespace: source_namespace.to_string(),
            })?;
        let source_origin = source_manifest.local_origin()?;

        let target_namespace_id =
            NamespaceId::parse(target_namespace.to_string()).map_err(|_| {
                ZeppelinError::Validation(format!(
                    "invalid synthetic target namespace: {target_namespace}"
                ))
            })?;
        let (target_origin, target_incarnation, target_base, local_tail) =
            match Manifest::read(&store, target_namespace).await? {
                Some(mut existing) => {
                    if !existing.segments.is_empty() || existing.active_segment.is_some() {
                        return Err(ZeppelinError::Validation(
                            "synthetic target may contain local WAL but not a local segment"
                                .to_string(),
                        ));
                    }
                    let origin = existing.local_origin()?;
                    let incarnation = origin.incarnation.as_uuid();
                    let local_tail = std::mem::take(&mut existing.fragments);
                    (origin, incarnation, existing, local_tail)
                }
                None => {
                    let incarnation = uuid::Uuid::new_v4();
                    let origin = ArtifactOrigin {
                        namespace: target_namespace_id,
                        incarnation: NamespaceIncarnationId::from_uuid(incarnation),
                    };
                    let mut base = Manifest::new();
                    base.bind_namespace_incarnation(incarnation)?;
                    (origin, incarnation, base, Vec::new())
                }
            };

        let mut manifest = source_manifest;
        manifest.reset_version_for_clone();
        manifest.prepare_clone_publication(target_namespace, target_incarnation, &target_base)?;
        manifest.artifact_origins = vec![source_origin];
        for fragment in &mut manifest.fragments {
            fragment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        }
        for segment in &mut manifest.segments {
            segment.artifact_origin = Some(ArtifactOriginIndex::new(0));
        }
        manifest.fragments.extend(local_tail);
        manifest.bind_synthetic_origin_binding_for_test_support();

        // Validate the exact view once at construction. This is structural
        // resolution only; it intentionally does not open persisted admission.
        manifest.artifact_origin_resolver(&target_origin)?;

        Ok(Self {
            store,
            target_namespace: target_namespace.to_string(),
            target_origin,
            manifest,
        })
    }

    /// Execute ANN through the production supplied-manifest query path.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_ann(
        &self,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
        distance_metric: DistanceMetric,
        consistency: ConsistencyLevel,
    ) -> Result<SyntheticForeignQueryResult> {
        self.query_ann_with_options(
            query,
            top_k,
            nprobe,
            distance_metric,
            consistency,
            None,
            false,
            None,
            false,
        )
        .await
    }

    /// Execute configurable ANN through the same supplied-manifest path.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_ann_with_options(
        &self,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
        distance_metric: DistanceMetric,
        consistency: ConsistencyLevel,
        filter: Option<&Filter>,
        include_attributes: bool,
        cache: Option<&Arc<DiskCache>>,
        emit_debug: bool,
    ) -> Result<SyntheticForeignQueryResult> {
        let response = self
            .execute_ann_with_manifest(
                self.manifest.clone(),
                query,
                top_k,
                nprobe,
                distance_metric,
                consistency,
                filter,
                include_attributes,
                cache,
                emit_debug,
            )
            .await?;
        Ok(Self::query_result(response))
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_ann_with_manifest(
        &self,
        manifest: Manifest,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
        distance_metric: DistanceMetric,
        consistency: ConsistencyLevel,
        filter: Option<&Filter>,
        include_attributes: bool,
        cache: Option<&Arc<DiskCache>>,
        emit_debug: bool,
    ) -> Result<crate::query::QueryResponse> {
        let wal_reader = WalReader::new(self.store.clone());
        let params = QueryParams {
            store: &self.store,
            wal_reader: &wal_reader,
            namespace: &self.target_namespace,
            query,
            top_k,
            nprobe,
            filter,
            consistency,
            distance_metric,
            oversample_factor: 3,
            rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
            cache,
            manifest_cache: None,
            include_attributes,
        };
        let response = if emit_debug {
            execute_query_with_manifest_debug(
                params,
                manifest,
                None,
                None,
                Some(self.target_origin.clone()),
            )
            .await?
        } else {
            execute_query_with_manifest(
                params,
                manifest,
                None,
                None,
                Some(self.target_origin.clone()),
            )
            .await?
        };
        Ok(response)
    }

    /// Execute BM25 against the synthetic target's foreign segment and WAL.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_bm25(
        &self,
        rank_by: &RankBy,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        top_k: usize,
        filter: Option<&Filter>,
        consistency: ConsistencyLevel,
        include_attributes: bool,
        emit_debug: bool,
    ) -> Result<SyntheticForeignQueryResult> {
        let response = self
            .execute_bm25_with_manifest(
                self.manifest.clone(),
                rank_by,
                fts_configs,
                top_k,
                filter,
                consistency,
                include_attributes,
                emit_debug,
            )
            .await?;
        Ok(Self::query_result(response))
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_bm25_with_manifest(
        &self,
        manifest: Manifest,
        rank_by: &RankBy,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        top_k: usize,
        filter: Option<&Filter>,
        consistency: ConsistencyLevel,
        include_attributes: bool,
        emit_debug: bool,
    ) -> Result<crate::query::QueryResponse> {
        let wal_reader = WalReader::new(self.store.clone());
        let response = if emit_debug {
            execute_bm25_query_with_manifest_debug(
                &self.store,
                &wal_reader,
                &self.target_namespace,
                rank_by,
                fts_configs,
                top_k,
                filter,
                None,
                consistency,
                false,
                None,
                None,
                None,
                None,
                0,
                0,
                include_attributes,
                manifest,
                Some(self.target_origin.clone()),
            )
            .await?
        } else {
            execute_bm25_query_with_manifest(
                &self.store,
                &wal_reader,
                &self.target_namespace,
                rank_by,
                fts_configs,
                top_k,
                filter,
                None,
                consistency,
                false,
                None,
                None,
                None,
                None,
                0,
                0,
                include_attributes,
                manifest,
                Some(self.target_origin.clone()),
            )
            .await?
        };
        Ok(response)
    }

    /// Execute independent positional entries against clones of one manifest.
    ///
    /// An entry error is retained at its original index and does not prevent
    /// later entries from executing. Hybrid entries execute ANN and BM25 from
    /// the same manifest clone and delegate reduction to the production fusion
    /// function used by retrieval algebra.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_batch(
        &self,
        specs: &[SyntheticForeignQuerySpec],
        fts_configs: &HashMap<String, FtsFieldConfig>,
        distance_metric: DistanceMetric,
        consistency: ConsistencyLevel,
        include_attributes: bool,
        emit_debug: bool,
    ) -> Vec<Result<SyntheticForeignQueryResult>> {
        let mut entries = Vec::with_capacity(specs.len());
        for spec in specs {
            let response = self
                .execute_query_spec(
                    spec,
                    fts_configs,
                    distance_metric,
                    consistency,
                    include_attributes,
                    emit_debug,
                    self.manifest.clone(),
                )
                .await
                .map(Self::query_result);
            entries.push(response);
        }
        entries
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_query_spec(
        &self,
        spec: &SyntheticForeignQuerySpec,
        fts_configs: &HashMap<String, FtsFieldConfig>,
        distance_metric: DistanceMetric,
        consistency: ConsistencyLevel,
        include_attributes: bool,
        emit_debug: bool,
        manifest: Manifest,
    ) -> Result<crate::query::QueryResponse> {
        match spec {
            SyntheticForeignQuerySpec::Ann {
                query,
                top_k,
                nprobe,
            } => {
                self.execute_ann_with_manifest(
                    manifest,
                    query,
                    *top_k,
                    *nprobe,
                    distance_metric,
                    consistency,
                    None,
                    include_attributes,
                    None,
                    emit_debug,
                )
                .await
            }
            SyntheticForeignQuerySpec::Bm25 { rank_by, top_k } => {
                self.execute_bm25_with_manifest(
                    manifest,
                    rank_by,
                    fts_configs,
                    *top_k,
                    None,
                    consistency,
                    include_attributes,
                    emit_debug,
                )
                .await
            }
            SyntheticForeignQuerySpec::Hybrid {
                query,
                rank_by,
                top_k,
                nprobe,
            } => {
                let ann = self
                    .execute_ann_with_manifest(
                        manifest.clone(),
                        query,
                        *top_k,
                        *nprobe,
                        distance_metric,
                        consistency,
                        None,
                        include_attributes,
                        None,
                        emit_debug,
                    )
                    .await?;
                let bm25 = self
                    .execute_bm25_with_manifest(
                        manifest,
                        rank_by,
                        fts_configs,
                        *top_k,
                        None,
                        consistency,
                        include_attributes,
                        emit_debug,
                    )
                    .await?;
                crate::server::handlers::query::fuse_ann_bm25_for_test_support(
                    ann,
                    bm25,
                    *top_k,
                    *nprobe,
                    distance_metric,
                    consistency,
                    include_attributes,
                    emit_debug,
                )
            }
        }
    }

    /// Fetch exact IDs with vector and attribute projection through production lookup.
    pub async fn fetch_by_ids(
        &self,
        ids: &[VectorId],
        consistency: ConsistencyLevel,
        include_vector: bool,
        include_attributes: bool,
        attribute_fields: Option<&[String]>,
    ) -> Result<SyntheticForeignFetchResult> {
        let (response, _) = crate::server::handlers::vectors::fetch_vectors_by_id_for_test_support(
            &self.store,
            &self.target_namespace,
            ids,
            consistency,
            include_vector,
            include_attributes,
            attribute_fields,
            self.manifest.clone(),
            self.target_origin.clone(),
        )
        .await?;
        Ok(SyntheticForeignFetchResult {
            records: response
                .results
                .into_iter()
                .map(|record| SyntheticForeignFetchRecord {
                    id: record.id,
                    values: record.values,
                    attributes: record.attributes,
                })
                .collect(),
            missing: response.missing,
        })
    }

    /// Resolve the active segment into an owned logical/physical hydration target.
    pub fn hydration_target(&self) -> Result<Option<HydrationTarget>> {
        HydrationTarget::from_active_manifest_with_origin(&self.manifest, &self.target_origin)
    }

    /// Return the exact physical keys referenced by this logical target view.
    pub fn reachable_artifact_keys(&self) -> Result<BTreeSet<String>> {
        crate::compaction::gc::reachable_keys(&self.target_namespace, &self.manifest)
    }

    /// Classify one inventory key through the destructive target-GC guard.
    pub fn classify_target_sweep_candidate(&self, key: String) -> Result<String> {
        crate::compaction::gc::classify_target_owned_deletion_key_for_test_support(
            &self.target_namespace,
            key,
        )
    }

    /// Produce a structurally invalid origin-table reference for fail-loud tests.
    #[must_use]
    pub fn with_corrupt_active_segment_origin(mut self) -> Self {
        if let Some(active) = self.manifest.active_segment.clone() {
            if let Some(segment) = self
                .manifest
                .segments
                .iter_mut()
                .find(|segment| segment.id == active)
            {
                let invalid_index = match u32::try_from(self.manifest.artifact_origins.len()) {
                    Ok(index) => index,
                    Err(_) => panic!("synthetic artifact-origin table exceeds u32"),
                };
                segment.artifact_origin = Some(ArtifactOriginIndex::new(invalid_index));
            }
        }
        self
    }

    fn query_result(response: crate::query::QueryResponse) -> SyntheticForeignQueryResult {
        let ids = response
            .results
            .iter()
            .map(|result| result.id.clone())
            .collect();
        let (debug_present, cache_hits, cache_misses) =
            response.debug.as_ref().map_or((false, 0, 0), |debug| {
                (true, debug.cache.hits, debug.cache.misses)
            });
        SyntheticForeignQueryResult {
            ids,
            results: response.results,
            scanned_fragments: response.scanned_fragments,
            scanned_segments: response.scanned_segments,
            debug_present,
            cache_hits,
            cache_misses,
        }
    }

    /// Run the normal persisted-manifest decoder against this foreign view.
    ///
    /// Until lineage authorization is implemented this returns
    /// `BranchingNotReady`; an `Ok` result would mean the fixture accidentally
    /// weakened production admission.
    pub fn production_admission_result(&self) -> Result<()> {
        let bytes = self.manifest.to_bytes()?;
        Manifest::from_bytes_for_namespace(&bytes, &self.target_namespace).map(|_| ())
    }
}
