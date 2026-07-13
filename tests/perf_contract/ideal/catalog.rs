//! Deterministic catalog for exhaustive, perf-only S3 path analysis.
//!
//! Eligible frozen entries delegate to checked-in single-operation scenarios.
//! Compound or harness-contaminated guards remain in CI but are excluded here.
//! Every other entry names one missing production operation plus the smallest
//! world state known to change its object-store call shape. Execution belongs
//! to the runner; this module is deliberately data and validation only.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

/// Stable identifier written to ideal-analysis artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub(crate) struct IdealCaseId(&'static str);

impl IdealCaseId {
    pub(crate) const fn new(value: &'static str) -> Self {
        Self(value)
    }

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Domain grouping used for stable execution and report sections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IdealCaseGroup {
    Operational,
    NamespaceControl,
    VectorWrite,
    Query,
    BatchQuery,
    Fetch,
    SnapshotClone,
    Compaction,
    Lease,
    BackgroundMaintenance,
    GarbageCollection,
    ManifestCache,
}

/// One executable catalog row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct IdealCase {
    pub id: IdealCaseId,
    pub group: IdealCaseGroup,
    pub operation: IdealOperation,
}

/// Logical operation and typed pre-state selected by one catalog row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IdealOperation {
    /// Reuse a checked-in contract scenario without altering its specification.
    FrozenContract {
        scenario: &'static str,
    },
    Operational(OperationalCase),
    NamespaceControl(NamespaceControlCase),
    VectorWrite(VectorWriteCase),
    Query(QueryCase),
    BatchQuery(BatchQueryCase),
    Fetch(FetchCase),
    SnapshotClone(SnapshotCloneCase),
    Compaction(CompactionCase),
    Lease(LeaseCase),
    BackgroundMaintenance(BackgroundMaintenanceCase),
    GarbageCollection(GarbageCollectionCase),
    ManifestCache(ManifestCacheCase),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationalCase {
    /// Constructed-store startup probe: delimiter LIST at the store root.
    StartupStorageProbe,
    /// Health check: recursive LIST under the reserved health-check prefix.
    HealthCheckStorageList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NamespaceControlCase {
    CreateFresh,
    CreateIdempotentExisting,
    GetMetadataCold,
    GetMetadataResident,
    ScanEmpty,
    ScanActiveMany,
    ScanRecoverCreatingManifestPresent,
    ScanRecoverCreatingManifestMissing,
    PatchIndexConfig,
    CompactionStatus,
    DeletePublishTombstone,
    DeleteCleanupIncomplete,
    DeleteCleanupComplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum VectorWriteCase {
    UpsertIntoEmpty,
    UpsertIntoCompacted,
    DeleteBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QueryCase {
    AnnStrongWalOnly,
    AnnEventualWalOnly,
    AnnStrongCompactedAndWal,
    AnnEventualCompactedAndWal,
    AnnIncludeAttributes,
    AnnVectorRerank,
    AnnMultiRangeRerank,
    AsOfTimestamp,
    AsOfSnapshot,
    FlatNoneFilteredNoBitmap,
    FlatPqUnfilteredCurrent,
    FlatPqFilteredBitmap,
    HierarchicalNoneShallowUnfiltered,
    HierarchicalSqDeepFilteredNoBitmap,
    HierarchicalPqDeepFilteredBitmap,
    FlatLegacySqStandaloneSketch,
    FlatLegacyNoneNoSketch,
    FtsGlobalCold,
    FtsPerClusterFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BatchQueryCase {
    StrongCompacted,
    EventualCompactedAndWal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FetchCase {
    EventualCompacted,
    StrongWalOnly,
    StrongCompactedAndWal,
    StrongMiss,
    StrongWithAttributes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SnapshotCloneCase {
    SnapshotCreate,
    SnapshotGet,
    SnapshotList,
    SnapshotDelete,
    CloneCurrent,
    CloneGeneration,
    CloneTimestamp,
    CloneSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompactionCase {
    DirectNoop,
    DirectFull,
    DirectIncremental,
    LayoutRewriteNoWal,
    AllVectorsDeleted,
    FullWithFts,
    FencedFull,
    FencedIncremental,
    HttpNoop,
    FlatPqFull,
    FlatPqIncremental,
    FlatSqPopulatedBitmap,
    HierarchicalSqFull,
    HierarchicalPqFull,
    HierarchicalExistingSmallWalFullRewrite,
    HierarchicalFullWithFts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LeaseCase {
    AcquireNew,
    AcquireLiveHeld,
    AcquireExpiredTakeover,
    RenewOwned,
    ReleaseOwned,
    ReleaseTakenOver,
    ReleaseMissing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BackgroundMaintenanceCase {
    DiscoveryTickEmpty,
    DiscoveryTickActive,
    CachedTickIdle,
    TickResumeDelete,
    TickLeaseHeld,
    TickCompactionSuccess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GarbageCollectionCase {
    IdleWarmSecondCycle,
    IdleNewOrphan,
    IdleCandidateMaturity,
    IdlePendingDeleteMaturity,
    IdlePitrExpiry,
    IdleStagingLeaseExpiry,
    IdleChangedSnapshot,
    IdleChangedStaging,
    IdleChangedCandidateLedger,
    IdleBackwardClock,
    IdleShorterRetentionConfig,
    IdlePriorPartialFailure,
    PruneReuseEmptyPendingUncacheable,
    PruneReuseEligiblePendingRefresh,
    HistoryMemoNewGeneration,
    HistoryMemoChangedEtag,
    HistoryMemoMissingEtag,
    HistoryMemoDisappearsBetweenListAndGet,
    HistoryMemoUnpublishedOrphanOverwrite,
    HistoryMemoCorruptChangedBody,
    HistoryMemoNamespaceRecreated,
    HistoryMemoColdRunnerRestart,
    PendingDeleteYoung,
    PendingDeleteHistoryPinned,
    PendingDeleteEligible,
    OrphanMark,
    OrphanSweep,
    ManifestHistoryPrune,
    StagingWrite,
    StagingClear,
    ActiveStagingMissingLease,
    ActiveStagingExpiredLease,
    ActiveStagingMatchingToken,
    ActiveStagingMixedTokens,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestCacheCase {
    EventualExpired,
    StrongEtagChanged,
    StrongWriteThroughWithoutEtag,
    StrongConcurrentCoalesced,
    StrongRequiredMissing,
    StrongOptionalConditionalNotFound,
}

const fn ideal_case(
    id: &'static str,
    group: IdealCaseGroup,
    operation: IdealOperation,
) -> IdealCase {
    IdealCase {
        id: IdealCaseId::new(id),
        group,
        operation,
    }
}

const fn frozen(scenario: &'static str, group: IdealCaseGroup) -> IdealCase {
    ideal_case(scenario, group, IdealOperation::FrozenContract { scenario })
}

/// Complete catalog in stable execution order.
pub(crate) const IDEAL_CASES: &[IdealCase] = &[
    // Frozen regression-guard scenarios. Their IDs intentionally remain the
    // checked-in scenario names so existing artifacts and filters carry over.
    frozen("warm_query_strong", IdealCaseGroup::Query),
    frozen("cold_query_strong", IdealCaseGroup::Query),
    frozen("upsert_single", IdealCaseGroup::VectorWrite),
    frozen("warm_query_eventual", IdealCaseGroup::Query),
    frozen("filtered_query", IdealCaseGroup::Query),
    frozen("filtered_query_bitmap", IdealCaseGroup::Query),
    frozen("fts_query", IdealCaseGroup::Query),
    frozen("hybrid_query", IdealCaseGroup::Query),
    frozen("as_of_query", IdealCaseGroup::Query),
    frozen("paginate", IdealCaseGroup::Query),
    frozen("fetch_strong", IdealCaseGroup::Fetch),
    frozen("upsert_batch", IdealCaseGroup::VectorWrite),
    frozen("delete_single", IdealCaseGroup::VectorWrite),
    frozen("gc_cycle", IdealCaseGroup::GarbageCollection),
    frozen("cold_query_sketch_adc", IdealCaseGroup::Query),
    // Operational and namespace control plane.
    ideal_case(
        "operational.startup_storage_probe",
        IdealCaseGroup::Operational,
        IdealOperation::Operational(OperationalCase::StartupStorageProbe),
    ),
    ideal_case(
        "operational.health_check_storage_list",
        IdealCaseGroup::Operational,
        IdealOperation::Operational(OperationalCase::HealthCheckStorageList),
    ),
    ideal_case(
        "namespace.create_fresh",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::CreateFresh),
    ),
    ideal_case(
        "namespace.create_idempotent_existing",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::CreateIdempotentExisting),
    ),
    ideal_case(
        "namespace.get_metadata_cold",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::GetMetadataCold),
    ),
    ideal_case(
        "namespace.get_metadata_resident",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::GetMetadataResident),
    ),
    ideal_case(
        "namespace.scan_empty",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::ScanEmpty),
    ),
    ideal_case(
        "namespace.scan_active_many",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::ScanActiveMany),
    ),
    ideal_case(
        "namespace.scan_recover_creating_manifest_present",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::ScanRecoverCreatingManifestPresent),
    ),
    ideal_case(
        "namespace.scan_recover_creating_manifest_missing",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::ScanRecoverCreatingManifestMissing),
    ),
    ideal_case(
        "namespace.patch_index_config",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::PatchIndexConfig),
    ),
    ideal_case(
        "namespace.compaction_status",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::CompactionStatus),
    ),
    ideal_case(
        "namespace.delete_publish_tombstone",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::DeletePublishTombstone),
    ),
    ideal_case(
        "namespace.delete_cleanup_incomplete",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::DeleteCleanupIncomplete),
    ),
    ideal_case(
        "namespace.delete_cleanup_complete",
        IdealCaseGroup::NamespaceControl,
        IdealOperation::NamespaceControl(NamespaceControlCase::DeleteCleanupComplete),
    ),
    // Writes, query, batch-query, and fetch shapes missing from frozen cases.
    ideal_case(
        "vector.upsert_into_empty",
        IdealCaseGroup::VectorWrite,
        IdealOperation::VectorWrite(VectorWriteCase::UpsertIntoEmpty),
    ),
    ideal_case(
        "vector.upsert_into_compacted",
        IdealCaseGroup::VectorWrite,
        IdealOperation::VectorWrite(VectorWriteCase::UpsertIntoCompacted),
    ),
    ideal_case(
        "vector.delete_batch",
        IdealCaseGroup::VectorWrite,
        IdealOperation::VectorWrite(VectorWriteCase::DeleteBatch),
    ),
    ideal_case(
        "query.ann_strong_wal_only",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::AnnStrongWalOnly),
    ),
    ideal_case(
        "query.ann_eventual_wal_only",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::AnnEventualWalOnly),
    ),
    ideal_case(
        "query.ann_strong_compacted_and_wal",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::AnnStrongCompactedAndWal),
    ),
    ideal_case(
        "query.ann_eventual_compacted_and_wal",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::AnnEventualCompactedAndWal),
    ),
    ideal_case(
        "query.ann_include_attributes",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::AnnIncludeAttributes),
    ),
    ideal_case(
        "query.ann_vector_rerank",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::AnnVectorRerank),
    ),
    ideal_case(
        "query.ann_multi_range_rerank",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::AnnMultiRangeRerank),
    ),
    ideal_case(
        "query.as_of_timestamp",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::AsOfTimestamp),
    ),
    ideal_case(
        "query.as_of_snapshot",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::AsOfSnapshot),
    ),
    ideal_case(
        "query.flat_none_filtered_no_bitmap",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::FlatNoneFilteredNoBitmap),
    ),
    ideal_case(
        "query.flat_pq_unfiltered_current",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::FlatPqUnfilteredCurrent),
    ),
    ideal_case(
        "query.flat_pq_filtered_bitmap",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::FlatPqFilteredBitmap),
    ),
    ideal_case(
        "query.hierarchical_none_shallow_unfiltered",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::HierarchicalNoneShallowUnfiltered),
    ),
    ideal_case(
        "query.hierarchical_sq_deep_filtered_no_bitmap",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::HierarchicalSqDeepFilteredNoBitmap),
    ),
    ideal_case(
        "query.hierarchical_pq_deep_filtered_bitmap",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::HierarchicalPqDeepFilteredBitmap),
    ),
    ideal_case(
        "query.flat_legacy_sq_standalone_sketch",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::FlatLegacySqStandaloneSketch),
    ),
    ideal_case(
        "query.flat_legacy_none_no_sketch",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::FlatLegacyNoneNoSketch),
    ),
    ideal_case(
        "query.fts_global_cold",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::FtsGlobalCold),
    ),
    ideal_case(
        "query.fts_per_cluster_fallback",
        IdealCaseGroup::Query,
        IdealOperation::Query(QueryCase::FtsPerClusterFallback),
    ),
    ideal_case(
        "batch_query.strong_compacted",
        IdealCaseGroup::BatchQuery,
        IdealOperation::BatchQuery(BatchQueryCase::StrongCompacted),
    ),
    ideal_case(
        "batch_query.eventual_compacted_and_wal",
        IdealCaseGroup::BatchQuery,
        IdealOperation::BatchQuery(BatchQueryCase::EventualCompactedAndWal),
    ),
    ideal_case(
        "fetch.eventual_compacted",
        IdealCaseGroup::Fetch,
        IdealOperation::Fetch(FetchCase::EventualCompacted),
    ),
    ideal_case(
        "fetch.strong_wal_only",
        IdealCaseGroup::Fetch,
        IdealOperation::Fetch(FetchCase::StrongWalOnly),
    ),
    ideal_case(
        "fetch.strong_compacted_and_wal",
        IdealCaseGroup::Fetch,
        IdealOperation::Fetch(FetchCase::StrongCompactedAndWal),
    ),
    ideal_case(
        "fetch.strong_miss",
        IdealCaseGroup::Fetch,
        IdealOperation::Fetch(FetchCase::StrongMiss),
    ),
    ideal_case(
        "fetch.strong_with_attributes",
        IdealCaseGroup::Fetch,
        IdealOperation::Fetch(FetchCase::StrongWithAttributes),
    ),
    // Snapshots and cloning.
    ideal_case(
        "snapshot.create",
        IdealCaseGroup::SnapshotClone,
        IdealOperation::SnapshotClone(SnapshotCloneCase::SnapshotCreate),
    ),
    ideal_case(
        "snapshot.get",
        IdealCaseGroup::SnapshotClone,
        IdealOperation::SnapshotClone(SnapshotCloneCase::SnapshotGet),
    ),
    ideal_case(
        "snapshot.list",
        IdealCaseGroup::SnapshotClone,
        IdealOperation::SnapshotClone(SnapshotCloneCase::SnapshotList),
    ),
    ideal_case(
        "snapshot.delete",
        IdealCaseGroup::SnapshotClone,
        IdealOperation::SnapshotClone(SnapshotCloneCase::SnapshotDelete),
    ),
    ideal_case(
        "clone.current",
        IdealCaseGroup::SnapshotClone,
        IdealOperation::SnapshotClone(SnapshotCloneCase::CloneCurrent),
    ),
    ideal_case(
        "clone.generation",
        IdealCaseGroup::SnapshotClone,
        IdealOperation::SnapshotClone(SnapshotCloneCase::CloneGeneration),
    ),
    ideal_case(
        "clone.timestamp",
        IdealCaseGroup::SnapshotClone,
        IdealOperation::SnapshotClone(SnapshotCloneCase::CloneTimestamp),
    ),
    ideal_case(
        "clone.snapshot",
        IdealCaseGroup::SnapshotClone,
        IdealOperation::SnapshotClone(SnapshotCloneCase::CloneSnapshot),
    ),
    // Direct, fenced, HTTP, and post-success compaction states.
    ideal_case(
        "compaction.direct_noop",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::DirectNoop),
    ),
    ideal_case(
        "compaction.direct_full",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::DirectFull),
    ),
    ideal_case(
        "compaction.direct_incremental",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::DirectIncremental),
    ),
    ideal_case(
        "compaction.layout_rewrite_no_wal",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::LayoutRewriteNoWal),
    ),
    ideal_case(
        "compaction.all_vectors_deleted",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::AllVectorsDeleted),
    ),
    ideal_case(
        "compaction.full_with_fts",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::FullWithFts),
    ),
    ideal_case(
        "compaction.fenced_full",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::FencedFull),
    ),
    ideal_case(
        "compaction.fenced_incremental",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::FencedIncremental),
    ),
    ideal_case(
        "compaction.http_noop",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::HttpNoop),
    ),
    ideal_case(
        "compaction.flat_pq_full",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::FlatPqFull),
    ),
    ideal_case(
        "compaction.flat_pq_incremental",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::FlatPqIncremental),
    ),
    ideal_case(
        "compaction.flat_sq_populated_bitmap",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::FlatSqPopulatedBitmap),
    ),
    ideal_case(
        "compaction.hierarchical_sq_full",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::HierarchicalSqFull),
    ),
    ideal_case(
        "compaction.hierarchical_pq_full",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::HierarchicalPqFull),
    ),
    ideal_case(
        "compaction.hierarchical_existing_small_wal_full_rewrite",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::HierarchicalExistingSmallWalFullRewrite),
    ),
    ideal_case(
        "compaction.hierarchical_full_with_fts",
        IdealCaseGroup::Compaction,
        IdealOperation::Compaction(CompactionCase::HierarchicalFullWithFts),
    ),
    // Lease and complete scheduler tick shapes.
    ideal_case(
        "lease.acquire_new",
        IdealCaseGroup::Lease,
        IdealOperation::Lease(LeaseCase::AcquireNew),
    ),
    ideal_case(
        "lease.acquire_live_held",
        IdealCaseGroup::Lease,
        IdealOperation::Lease(LeaseCase::AcquireLiveHeld),
    ),
    ideal_case(
        "lease.acquire_expired_takeover",
        IdealCaseGroup::Lease,
        IdealOperation::Lease(LeaseCase::AcquireExpiredTakeover),
    ),
    ideal_case(
        "lease.renew_owned",
        IdealCaseGroup::Lease,
        IdealOperation::Lease(LeaseCase::RenewOwned),
    ),
    ideal_case(
        "lease.release_owned",
        IdealCaseGroup::Lease,
        IdealOperation::Lease(LeaseCase::ReleaseOwned),
    ),
    ideal_case(
        "lease.release_taken_over",
        IdealCaseGroup::Lease,
        IdealOperation::Lease(LeaseCase::ReleaseTakenOver),
    ),
    ideal_case(
        "lease.release_missing",
        IdealCaseGroup::Lease,
        IdealOperation::Lease(LeaseCase::ReleaseMissing),
    ),
    ideal_case(
        "background.discovery_tick_empty",
        IdealCaseGroup::BackgroundMaintenance,
        IdealOperation::BackgroundMaintenance(BackgroundMaintenanceCase::DiscoveryTickEmpty),
    ),
    ideal_case(
        "background.discovery_tick_active",
        IdealCaseGroup::BackgroundMaintenance,
        IdealOperation::BackgroundMaintenance(BackgroundMaintenanceCase::DiscoveryTickActive),
    ),
    ideal_case(
        "background.cached_tick_idle",
        IdealCaseGroup::BackgroundMaintenance,
        IdealOperation::BackgroundMaintenance(BackgroundMaintenanceCase::CachedTickIdle),
    ),
    ideal_case(
        "background.tick_resume_delete",
        IdealCaseGroup::BackgroundMaintenance,
        IdealOperation::BackgroundMaintenance(BackgroundMaintenanceCase::TickResumeDelete),
    ),
    ideal_case(
        "background.tick_lease_held",
        IdealCaseGroup::BackgroundMaintenance,
        IdealOperation::BackgroundMaintenance(BackgroundMaintenanceCase::TickLeaseHeld),
    ),
    ideal_case(
        "background.tick_compaction_success",
        IdealCaseGroup::BackgroundMaintenance,
        IdealOperation::BackgroundMaintenance(BackgroundMaintenanceCase::TickCompactionSuccess),
    ),
    // GC mark/sweep, pending-delete, history, and staging states.
    ideal_case(
        "gc.idle_warm_second_cycle",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdleWarmSecondCycle),
    ),
    ideal_case(
        "gc.idle_new_orphan",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdleNewOrphan),
    ),
    ideal_case(
        "gc.idle_candidate_maturity",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdleCandidateMaturity),
    ),
    ideal_case(
        "gc.idle_pending_delete_maturity",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdlePendingDeleteMaturity),
    ),
    ideal_case(
        "gc.idle_pitr_expiry",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdlePitrExpiry),
    ),
    ideal_case(
        "gc.idle_staging_lease_expiry",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdleStagingLeaseExpiry),
    ),
    ideal_case(
        "gc.idle_changed_snapshot",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdleChangedSnapshot),
    ),
    ideal_case(
        "gc.idle_changed_staging",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdleChangedStaging),
    ),
    ideal_case(
        "gc.idle_changed_candidate_ledger",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdleChangedCandidateLedger),
    ),
    ideal_case(
        "gc.idle_backward_clock",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdleBackwardClock),
    ),
    ideal_case(
        "gc.idle_shorter_retention_config",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdleShorterRetentionConfig),
    ),
    ideal_case(
        "gc.idle_prior_partial_failure",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::IdlePriorPartialFailure),
    ),
    ideal_case(
        "gc.prune_reuse_empty_pending_uncacheable",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::PruneReuseEmptyPendingUncacheable),
    ),
    ideal_case(
        "gc.prune_reuse_eligible_pending_refresh",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::PruneReuseEligiblePendingRefresh),
    ),
    ideal_case(
        "gc.history_memo_new_generation",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::HistoryMemoNewGeneration),
    ),
    ideal_case(
        "gc.history_memo_changed_etag",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::HistoryMemoChangedEtag),
    ),
    ideal_case(
        "gc.history_memo_missing_etag",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::HistoryMemoMissingEtag),
    ),
    ideal_case(
        "gc.history_memo_disappears_between_list_and_get",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(
            GarbageCollectionCase::HistoryMemoDisappearsBetweenListAndGet,
        ),
    ),
    ideal_case(
        "gc.history_memo_unpublished_orphan_overwrite",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(
            GarbageCollectionCase::HistoryMemoUnpublishedOrphanOverwrite,
        ),
    ),
    ideal_case(
        "gc.history_memo_corrupt_changed_body",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::HistoryMemoCorruptChangedBody),
    ),
    ideal_case(
        "gc.history_memo_namespace_recreated",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::HistoryMemoNamespaceRecreated),
    ),
    ideal_case(
        "gc.history_memo_cold_runner_restart",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::HistoryMemoColdRunnerRestart),
    ),
    ideal_case(
        "gc.pending_delete_young",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::PendingDeleteYoung),
    ),
    ideal_case(
        "gc.pending_delete_history_pinned",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::PendingDeleteHistoryPinned),
    ),
    ideal_case(
        "gc.pending_delete_eligible",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::PendingDeleteEligible),
    ),
    ideal_case(
        "gc.orphan_mark",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::OrphanMark),
    ),
    ideal_case(
        "gc.orphan_sweep",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::OrphanSweep),
    ),
    ideal_case(
        "gc.manifest_history_prune",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::ManifestHistoryPrune),
    ),
    ideal_case(
        "gc.staging_write",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::StagingWrite),
    ),
    ideal_case(
        "gc.staging_clear",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::StagingClear),
    ),
    ideal_case(
        "gc.active_staging_missing_lease",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::ActiveStagingMissingLease),
    ),
    ideal_case(
        "gc.active_staging_expired_lease",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::ActiveStagingExpiredLease),
    ),
    ideal_case(
        "gc.active_staging_matching_token",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::ActiveStagingMatchingToken),
    ),
    ideal_case(
        "gc.active_staging_mixed_tokens",
        IdealCaseGroup::GarbageCollection,
        IdealOperation::GarbageCollection(GarbageCollectionCase::ActiveStagingMixedTokens),
    ),
    // Manifest-cache branches not represented by cold strong, warm strong, and
    // warm eventual frozen scenarios.
    ideal_case(
        "manifest_cache.eventual_expired",
        IdealCaseGroup::ManifestCache,
        IdealOperation::ManifestCache(ManifestCacheCase::EventualExpired),
    ),
    ideal_case(
        "manifest_cache.strong_etag_changed",
        IdealCaseGroup::ManifestCache,
        IdealOperation::ManifestCache(ManifestCacheCase::StrongEtagChanged),
    ),
    ideal_case(
        "manifest_cache.strong_write_through_without_etag",
        IdealCaseGroup::ManifestCache,
        IdealOperation::ManifestCache(ManifestCacheCase::StrongWriteThroughWithoutEtag),
    ),
    ideal_case(
        "manifest_cache.strong_concurrent_coalesced",
        IdealCaseGroup::ManifestCache,
        IdealOperation::ManifestCache(ManifestCacheCase::StrongConcurrentCoalesced),
    ),
    ideal_case(
        "manifest_cache.strong_required_missing",
        IdealCaseGroup::ManifestCache,
        IdealOperation::ManifestCache(ManifestCacheCase::StrongRequiredMissing),
    ),
    ideal_case(
        "manifest_cache.strong_optional_conditional_not_found",
        IdealCaseGroup::ManifestCache,
        IdealOperation::ManifestCache(ManifestCacheCase::StrongOptionalConditionalNotFound),
    ),
];

#[must_use]
pub(crate) const fn all() -> &'static [IdealCase] {
    IDEAL_CASES
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CatalogError {
    InvalidId(&'static str),
    DuplicateId(&'static str),
    GroupMismatch {
        id: &'static str,
        declared: IdealCaseGroup,
        operation: IdealCaseGroup,
    },
    UnknownFrozenContract(&'static str),
    MissingFrozenContract(&'static str),
    DuplicateFrozenContract(&'static str),
}

/// Validate IDs, grouping, and exact reuse of eligible frozen contracts.
pub(crate) fn validate(cases: &[IdealCase]) -> Result<(), CatalogError> {
    let mut ids = BTreeSet::new();
    let frozen_names = analyzer_frozen_scenarios()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let mut frozen_counts = BTreeMap::<&'static str, usize>::new();

    for case in cases {
        let id = case.id.as_str();
        if id.is_empty()
            || !id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
            })
        {
            return Err(CatalogError::InvalidId(id));
        }
        if !ids.insert(id) {
            return Err(CatalogError::DuplicateId(id));
        }

        match case.operation {
            IdealOperation::FrozenContract { scenario } => {
                if !frozen_names.contains(scenario) {
                    return Err(CatalogError::UnknownFrozenContract(scenario));
                }
                *frozen_counts.entry(scenario).or_default() += 1;
            }
            operation => {
                let expected = operation_group(operation);
                if case.group != expected {
                    return Err(CatalogError::GroupMismatch {
                        id,
                        declared: case.group,
                        operation: expected,
                    });
                }
            }
        }
    }

    for scenario in analyzer_frozen_scenarios() {
        match frozen_counts.get(scenario).copied().unwrap_or(0) {
            0 => return Err(CatalogError::MissingFrozenContract(scenario)),
            1 => {}
            _ => return Err(CatalogError::DuplicateFrozenContract(scenario)),
        }
    }
    Ok(())
}

fn analyzer_frozen_scenarios() -> Vec<&'static str> {
    super::super::ALL_SCENARIOS
        .iter()
        .copied()
        .filter(|scenario| {
            !matches!(
                *scenario,
                "hydration" | "compaction_cycle" | "compaction_incremental"
            )
        })
        .collect()
}

fn operation_group(operation: IdealOperation) -> IdealCaseGroup {
    match operation {
        IdealOperation::FrozenContract { .. } => {
            panic!("frozen contracts retain their domain-specific report group")
        }
        IdealOperation::Operational(_) => IdealCaseGroup::Operational,
        IdealOperation::NamespaceControl(_) => IdealCaseGroup::NamespaceControl,
        IdealOperation::VectorWrite(_) => IdealCaseGroup::VectorWrite,
        IdealOperation::Query(_) => IdealCaseGroup::Query,
        IdealOperation::BatchQuery(_) => IdealCaseGroup::BatchQuery,
        IdealOperation::Fetch(_) => IdealCaseGroup::Fetch,
        IdealOperation::SnapshotClone(_) => IdealCaseGroup::SnapshotClone,
        IdealOperation::Compaction(_) => IdealCaseGroup::Compaction,
        IdealOperation::Lease(_) => IdealCaseGroup::Lease,
        IdealOperation::BackgroundMaintenance(_) => IdealCaseGroup::BackgroundMaintenance,
        IdealOperation::GarbageCollection(_) => IdealCaseGroup::GarbageCollection,
        IdealOperation::ManifestCache(_) => IdealCaseGroup::ManifestCache,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_catalog_is_valid_and_ids_are_unique() {
        validate(IDEAL_CASES).expect("checked-in ideal catalog must be valid");
        let ids = IDEAL_CASES
            .iter()
            .map(|case| case.id)
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), IDEAL_CASES.len());
    }

    #[test]
    fn frozen_contracts_are_reused_exactly_once() {
        let actual = IDEAL_CASES
            .iter()
            .filter_map(|case| match case.operation {
                IdealOperation::FrozenContract { scenario } => Some(scenario),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, analyzer_frozen_scenarios());
        for excluded in ["hydration", "compaction_cycle", "compaction_incremental"] {
            assert!(super::super::super::ALL_SCENARIOS.contains(&excluded));
            assert!(!actual.contains(&excluded));
        }
    }

    #[test]
    fn every_operation_group_has_an_incremental_execution_slice() {
        let actual = IDEAL_CASES
            .iter()
            .map(|case| case.group)
            .collect::<BTreeSet<_>>();
        let expected = [
            IdealCaseGroup::Operational,
            IdealCaseGroup::NamespaceControl,
            IdealCaseGroup::VectorWrite,
            IdealCaseGroup::Query,
            IdealCaseGroup::BatchQuery,
            IdealCaseGroup::Fetch,
            IdealCaseGroup::SnapshotClone,
            IdealCaseGroup::Compaction,
            IdealCaseGroup::Lease,
            IdealCaseGroup::BackgroundMaintenance,
            IdealCaseGroup::GarbageCollection,
            IdealCaseGroup::ManifestCache,
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn validation_rejects_duplicate_ids() {
        let mut cases = IDEAL_CASES.to_vec();
        cases.push(ideal_case(
            IDEAL_CASES[0].id.as_str(),
            IdealCaseGroup::Operational,
            IdealOperation::Operational(OperationalCase::StartupStorageProbe),
        ));
        assert_eq!(
            validate(&cases),
            Err(CatalogError::DuplicateId(IDEAL_CASES[0].id.as_str()))
        );
    }
}
