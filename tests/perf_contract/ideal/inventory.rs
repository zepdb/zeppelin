use std::collections::BTreeSet;

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum StorageMethodDisposition {
    ExercisesObjectStore,
    ExcludedNonS3 { reason: &'static str },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct StorageMethodInventory {
    pub method: StoreMethod,
    pub name: &'static str,
    pub disposition: StorageMethodDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StoreMethod {
    ProbeConfiguredEndpoint,
    Put,
    PutCreate,
    PutCreateOutcome,
    Get,
    GetRange,
    GetRanges,
    GetWithMeta,
    GetWithObjectMetadata,
    GetIfNoneMatch,
    PutIfMatch,
    PutIfMatchOutcome,
    PutIfMatchWithUserMetadata,
    PutIfNotExists,
    PutIfNotExistsWithUserMetadata,
    CopyIfNotExists,
    Delete,
    DeleteMany,
    ListPrefix,
    ListPrefixMeta,
    ListCommonPrefixes,
    Exists,
    Head,
    DeletePrefix,
    DeletePrefixPaged,
}

const STORAGE_METHOD_INVENTORY: &[StorageMethodInventory] = &[
    StorageMethodInventory {
        method: StoreMethod::ProbeConfiguredEndpoint,
        name: "probe_configured_endpoint",
        disposition: StorageMethodDisposition::ExcludedNonS3 {
            reason: "TCP/config reachability probe; it does not call ObjectStore",
        },
    },
    object_store_method(StoreMethod::Put, "put"),
    object_store_method(StoreMethod::PutCreate, "put_create"),
    object_store_method(StoreMethod::PutCreateOutcome, "put_create_outcome"),
    object_store_method(StoreMethod::Get, "get"),
    object_store_method(StoreMethod::GetRange, "get_range"),
    object_store_method(StoreMethod::GetRanges, "get_ranges"),
    object_store_method(StoreMethod::GetWithMeta, "get_with_meta"),
    object_store_method(
        StoreMethod::GetWithObjectMetadata,
        "get_with_object_metadata",
    ),
    object_store_method(StoreMethod::GetIfNoneMatch, "get_if_none_match"),
    object_store_method(StoreMethod::PutIfMatch, "put_if_match"),
    object_store_method(StoreMethod::PutIfMatchOutcome, "put_if_match_outcome"),
    object_store_method(
        StoreMethod::PutIfMatchWithUserMetadata,
        "put_if_match_with_user_metadata",
    ),
    object_store_method(StoreMethod::PutIfNotExists, "put_if_not_exists"),
    object_store_method(
        StoreMethod::PutIfNotExistsWithUserMetadata,
        "put_if_not_exists_with_user_metadata",
    ),
    object_store_method(StoreMethod::CopyIfNotExists, "copy_if_not_exists"),
    object_store_method(StoreMethod::Delete, "delete"),
    object_store_method(StoreMethod::DeleteMany, "delete_many"),
    object_store_method(StoreMethod::ListPrefix, "list_prefix"),
    object_store_method(StoreMethod::ListPrefixMeta, "list_prefix_meta"),
    object_store_method(StoreMethod::ListCommonPrefixes, "list_common_prefixes"),
    object_store_method(StoreMethod::Exists, "exists"),
    object_store_method(StoreMethod::Head, "head"),
    object_store_method(StoreMethod::DeletePrefix, "delete_prefix"),
    object_store_method(StoreMethod::DeletePrefixPaged, "delete_prefix_paged"),
];

const fn object_store_method(method: StoreMethod, name: &'static str) -> StorageMethodInventory {
    StorageMethodInventory {
        method,
        name,
        disposition: StorageMethodDisposition::ExercisesObjectStore,
    }
}

fn inventory_store_methods() -> BTreeSet<&'static str> {
    STORAGE_METHOD_INVENTORY
        .iter()
        .map(|method| method.name)
        .collect()
}

#[must_use]
pub(crate) const fn storage_methods() -> &'static [StorageMethodInventory] {
    STORAGE_METHOD_INVENTORY
}

fn source_public_async_store_methods() -> BTreeSet<&'static str> {
    include_str!("../../../src/storage/store.rs")
        .lines()
        .filter_map(|line| {
            line.trim_start()
                .strip_prefix("pub async fn ")
                .and_then(|suffix| suffix.split_once('('))
                .map(|(name, _)| name.trim())
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PhysicalVariant {
    GetFull,
    GetRange,
    GetSuffix,
    GetMultiRange,
    GetConditional,
    GetConditionalRange,
    GetConditionalSuffix,
    PutOverwrite,
    PutCreate,
    PutUpdate,
    Head,
    ListRecursive,
    ListDelimiter,
    Delete,
    DeleteBatch,
    CopyIfAbsent,
    CopyOverwrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum PathCoverage {
    ExistingFrozen {
        scenario: &'static str,
    },
    IdealScenario {
        scenario: &'static str,
    },
    ExplicitGap {
        catalog_case: Option<&'static str>,
        reason: &'static str,
    },
    NoProductionCaller {
        reason: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct ProductionPath {
    pub id: &'static str,
    pub source: &'static str,
    pub store_methods: &'static [StoreMethod],
    pub physical_variants: &'static [PhysicalVariant],
    pub coverage: PathCoverage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ScenarioKind {
    ExistingFrozen,
    IdealScenario,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct ScenarioReference {
    pub kind: ScenarioKind,
    pub id: &'static str,
}

const fn path(
    id: &'static str,
    source: &'static str,
    store_methods: &'static [StoreMethod],
    physical_variants: &'static [PhysicalVariant],
    coverage: PathCoverage,
) -> ProductionPath {
    ProductionPath {
        id,
        source,
        store_methods,
        physical_variants,
        coverage,
    }
}

const PRODUCTION_PATHS: &[ProductionPath] = &[
    path(
        "security.audit.persist_exact",
        "src/security/audit_sink.rs:persist_exact",
        &[StoreMethod::PutCreate, StoreMethod::Get],
        &[PhysicalVariant::PutCreate, PhysicalVariant::GetFull],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "security audit flush cost is owned by the Phase 2 counting-store tests; the detached sink has no ideal-runner completion handle",
        },
    ),
    path(
        "security.policy.bootstrap",
        "src/security/policy_store.rs:PolicyStore::bootstrap",
        &[
            StoreMethod::PutCreateOutcome,
            StoreMethod::GetWithMeta,
            StoreMethod::Get,
        ],
        &[PhysicalVariant::PutCreate, PhysicalVariant::GetFull],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "policy bootstrap is a security control-plane boot path covered by the real-store security boot suite, outside the data-plane ideal catalog",
        },
    ),
    path(
        "security.policy.load_current",
        "src/security/policy_store.rs:PolicyStore::load_current",
        &[StoreMethod::GetWithMeta, StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "authoritative policy head and immutable snapshot loading are covered by the Phase 3 real-store boot and policy-store tests, outside the data-plane ideal catalog",
        },
    ),
    path(
        "security.policy.publish",
        "src/security/policy_store.rs:PolicyStore::publish",
        &[
            StoreMethod::PutCreateOutcome,
            StoreMethod::PutIfMatchOutcome,
            StoreMethod::GetWithMeta,
            StoreMethod::Get,
        ],
        &[
            PhysicalVariant::PutCreate,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::GetFull,
        ],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "policy publication is an admin control-plane CAS path covered by Phase 3 policy-store tests, outside the data-plane ideal catalog",
        },
    ),
    path(
        "security.policy.refresh_changed",
        "src/security/policy_store.rs:PolicyStore::refresh",
        &[StoreMethod::GetIfNoneMatch, StoreMethod::Get],
        &[PhysicalVariant::GetConditional, PhysicalVariant::GetFull],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "changed policy-head refresh and immutable snapshot loading are covered by the Phase 3 real-store refresh tests, outside the data-plane ideal catalog",
        },
    ),
    path(
        "security.policy.refresh_unchanged",
        "src/security/policy_store.rs:PolicyStore::refresh",
        &[StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetConditional],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "unchanged conditional policy-head refresh is covered by the Phase 3 counting-store refresh tests, outside the data-plane ideal catalog",
        },
    ),
    path(
        "background.compaction_fenced",
        "src/compaction/background.rs:compact_namespace_under_lease_with_lifecycle",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
            StoreMethod::ListPrefix,
            StoreMethod::Delete,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::Delete,
        ],
        PathCoverage::IdealScenario {
            scenario: "compaction.fenced_full",
        },
    ),
    path(
        "background.discovery_tick",
        "src/compaction/background.rs:compaction_loop",
        &[
            StoreMethod::ListCommonPrefixes,
            StoreMethod::GetWithObjectMetadata,
        ],
        &[PhysicalVariant::ListDelimiter, PhysicalVariant::GetFull],
        PathCoverage::IdealScenario {
            scenario: "background.discovery_tick_active",
        },
    ),
    path(
        "clone.current",
        "src/server/handlers/namespace.rs:clone_namespace",
        &[
            StoreMethod::Get,
            StoreMethod::PutIfNotExists,
            StoreMethod::CopyIfNotExists,
            StoreMethod::Delete,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutCreate,
            PhysicalVariant::CopyIfAbsent,
            PhysicalVariant::Delete,
        ],
        PathCoverage::IdealScenario {
            scenario: "clone.current",
        },
    ),
    path(
        "clone.timestamp_history_scan",
        "src/server/handlers/as_of.rs:resolve_manifest",
        &[StoreMethod::Get, StoreMethod::ListPrefix],
        &[PhysicalVariant::GetFull, PhysicalVariant::ListRecursive],
        PathCoverage::IdealScenario {
            scenario: "clone.timestamp",
        },
    ),
    path(
        "compaction.direct_full",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
        ],
        PathCoverage::IdealScenario {
            scenario: "compaction.direct_full",
        },
    ),
    path(
        "compaction.direct_incremental",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
            StoreMethod::ListPrefix,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
            PhysicalVariant::ListRecursive,
        ],
        PathCoverage::IdealScenario {
            scenario: "compaction.direct_incremental",
        },
    ),
    path(
        "gc.active_staging",
        "src/compaction/gc.rs:active_staged_keys_at",
        &[StoreMethod::Get, StoreMethod::ListPrefix],
        &[PhysicalVariant::GetFull, PhysicalVariant::ListRecursive],
        PathCoverage::IdealScenario {
            scenario: "gc.active_staging_matching_token",
        },
    ),
    path(
        "gc.cycle_nothing_eligible",
        "src/compaction/gc.rs:run_gc_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::Put,
            StoreMethod::ListPrefix,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::ListRecursive,
        ],
        PathCoverage::ExistingFrozen {
            scenario: "gc_cycle",
        },
    ),
    path(
        "gc.pending_delete_eligible",
        "src/compaction/gc.rs:drain_pending_deletes_at",
        &[
            StoreMethod::Get,
            StoreMethod::DeleteMany,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
            StoreMethod::ListPrefix,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::DeleteBatch,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
            PhysicalVariant::ListRecursive,
        ],
        PathCoverage::IdealScenario {
            scenario: "gc.pending_delete_eligible",
        },
    ),
    path(
        "hydration.cold_segment",
        "src/cache/hydration.rs:hydrate_segment_once",
        &[StoreMethod::Head, StoreMethod::Get],
        &[PhysicalVariant::Head, PhysicalVariant::GetFull],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "the frozen hydration guard combines two detached triggers; the hydrator exposes no owned single-operation completion handle",
        },
    ),
    path(
        "hydration.over_capacity",
        "src/cache/hydration.rs:plan_hydration_items",
        &[StoreMethod::Head],
        &[PhysicalVariant::Head],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "detached hydrator exposes no owned completion handle for capacity refusal accounting",
        },
    ),
    path(
        "lease.acquire_new",
        "src/wal/lease.rs:LeaseManager::acquire",
        &[StoreMethod::GetWithMeta, StoreMethod::Put],
        &[PhysicalVariant::GetFull, PhysicalVariant::PutOverwrite],
        PathCoverage::IdealScenario {
            scenario: "lease.acquire_new",
        },
    ),
    path(
        "lease.renew_owned",
        "src/wal/lease.rs:LeaseManager::renew",
        &[StoreMethod::PutIfMatch],
        &[PhysicalVariant::PutUpdate],
        PathCoverage::IdealScenario {
            scenario: "lease.renew_owned",
        },
    ),
    path(
        "manifest_cache.strong_conditional",
        "src/cache/manifest_cache.rs:ManifestCache::get_strong",
        &[StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetConditional],
        PathCoverage::ExistingFrozen {
            scenario: "warm_query_strong",
        },
    ),
    path(
        "namespace.create_fresh",
        "src/namespace/manager.rs:NamespaceManager::create_with_fts_and_index_config",
        &[
            StoreMethod::PutIfNotExistsWithUserMetadata,
            StoreMethod::Put,
            StoreMethod::PutIfMatchWithUserMetadata,
        ],
        &[
            PhysicalVariant::PutCreate,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
        ],
        PathCoverage::IdealScenario {
            scenario: "namespace.create_fresh",
        },
    ),
    path(
        "namespace.delete_cleanup",
        "src/namespace/manager.rs:NamespaceManager::finish_delete",
        &[
            StoreMethod::DeletePrefixPaged,
            StoreMethod::DeleteMany,
            StoreMethod::ListPrefix,
            StoreMethod::Delete,
        ],
        &[
            PhysicalVariant::ListRecursive,
            PhysicalVariant::DeleteBatch,
            PhysicalVariant::Delete,
        ],
        PathCoverage::IdealScenario {
            scenario: "namespace.delete_cleanup_complete",
        },
    ),
    path(
        "namespace.get_cold",
        "src/server/handlers/namespace.rs:get_namespace",
        &[
            StoreMethod::GetWithObjectMetadata,
            StoreMethod::GetIfNoneMatch,
        ],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetConditional],
        PathCoverage::IdealScenario {
            scenario: "namespace.get_metadata_cold",
        },
    ),
    path(
        "namespace.patch_index_config",
        "src/namespace/manager.rs:NamespaceManager::update_index_config",
        &[
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::GetWithObjectMetadata,
            StoreMethod::PutIfMatchWithUserMetadata,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
        ],
        PathCoverage::IdealScenario {
            scenario: "namespace.patch_index_config",
        },
    ),
    path(
        "operational.readiness",
        "src/server/handlers/mod.rs:readiness_check",
        &[StoreMethod::ListPrefix],
        &[PhysicalVariant::ListRecursive],
        PathCoverage::IdealScenario {
            scenario: "operational.health_check_storage_list",
        },
    ),
    path(
        "operational.startup_probe",
        "src/startup.rs:probe_storage",
        &[StoreMethod::ListCommonPrefixes],
        &[PhysicalVariant::ListDelimiter],
        PathCoverage::IdealScenario {
            scenario: "operational.startup_storage_probe",
        },
    ),
    path(
        "query.ann_cold_full_and_range",
        "src/server/handlers/query.rs:query_namespace",
        &[StoreMethod::Get, StoreMethod::GetRange],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetRange],
        PathCoverage::ExistingFrozen {
            scenario: "cold_query_strong",
        },
    ),
    path(
        "query.ann_multi_range_rerank",
        "src/index/ivf_flat/search.rs:fetch_rerank_vectors_by_range",
        &[StoreMethod::GetRange],
        &[PhysicalVariant::GetRange],
        PathCoverage::IdealScenario {
            scenario: "query.ann_multi_range_rerank",
        },
    ),
    path(
        "query.as_of_timestamp",
        "src/server/handlers/as_of.rs:resolve_manifest",
        &[StoreMethod::Get, StoreMethod::ListPrefix],
        &[PhysicalVariant::GetFull, PhysicalVariant::ListRecursive],
        PathCoverage::IdealScenario {
            scenario: "query.as_of_timestamp",
        },
    ),
    path(
        "query.batch_shared_manifest",
        "src/server/handlers/query.rs:batch_query_namespace",
        &[StoreMethod::Get, StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetConditional],
        PathCoverage::IdealScenario {
            scenario: "batch_query.strong_compacted",
        },
    ),
    path(
        "snapshot.create",
        "src/wal/manifest.rs:NamedSnapshot::create_at",
        &[StoreMethod::Get, StoreMethod::PutIfNotExists],
        &[PhysicalVariant::GetFull, PhysicalVariant::PutCreate],
        PathCoverage::IdealScenario {
            scenario: "snapshot.create",
        },
    ),
    path(
        "snapshot.delete",
        "src/wal/manifest.rs:NamedSnapshot::delete",
        &[StoreMethod::Get, StoreMethod::Delete],
        &[PhysicalVariant::GetFull, PhysicalVariant::Delete],
        PathCoverage::IdealScenario {
            scenario: "snapshot.delete",
        },
    ),
    path(
        "snapshot.list_nonempty",
        "src/wal/manifest.rs:NamedSnapshot::list",
        &[StoreMethod::ListPrefix, StoreMethod::Get],
        &[PhysicalVariant::ListRecursive, PhysicalVariant::GetFull],
        PathCoverage::IdealScenario {
            scenario: "snapshot.list",
        },
    ),
    path(
        "storage.delete_prefix_no_production_caller",
        "src/storage/store.rs:ZeppelinStore::delete_prefix",
        &[StoreMethod::DeletePrefix],
        &[PhysicalVariant::ListRecursive, PhysicalVariant::Delete],
        PathCoverage::NoProductionCaller {
            reason: "only tests and offline binaries call the unbounded convenience method",
        },
    ),
    path(
        "storage.exists_no_production_caller",
        "src/storage/store.rs:ZeppelinStore::exists",
        &[StoreMethod::Exists],
        &[PhysicalVariant::Head],
        PathCoverage::NoProductionCaller {
            reason: "no production server or library caller; tests and offline evaluators only",
        },
    ),
    path(
        "storage.get_ranges_no_production_caller",
        "src/storage/store.rs:ZeppelinStore::get_ranges",
        &[StoreMethod::GetRanges],
        &[PhysicalVariant::GetMultiRange],
        PathCoverage::NoProductionCaller {
            reason: "the gateway method currently has no production caller",
        },
    ),
    path(
        "storage.list_prefix_meta_no_production_caller",
        "src/storage/store.rs:ZeppelinStore::list_prefix_meta",
        &[StoreMethod::ListPrefixMeta],
        &[PhysicalVariant::ListRecursive],
        PathCoverage::NoProductionCaller {
            reason: "metadata-preserving recursive LIST is introduced for the validated warm-GC inventory and has no production caller before that optimization lands",
        },
    ),
    path(
        "storage.get_suffix_no_gateway_method",
        "src/storage/store.rs:ZeppelinStore::get_range",
        &[StoreMethod::GetRange],
        &[PhysicalVariant::GetSuffix],
        PathCoverage::NoProductionCaller {
            reason: "ZeppelinStore exposes bounded ranges only; no production caller can issue an ObjectStore suffix GET",
        },
    ),
    path(
        "storage.get_conditional_range_no_gateway_method",
        "src/storage/store.rs:ZeppelinStore::get_if_none_match",
        &[StoreMethod::GetIfNoneMatch, StoreMethod::GetRange],
        &[
            PhysicalVariant::GetConditionalRange,
            PhysicalVariant::GetConditionalSuffix,
        ],
        PathCoverage::NoProductionCaller {
            reason: "ZeppelinStore exposes conditional full GET and bounded range GET as separate methods, never a combined conditional range or suffix request",
        },
    ),
    path(
        "storage.copy_overwrite_no_gateway_method",
        "src/storage/store.rs:ZeppelinStore::copy_if_not_exists",
        &[StoreMethod::CopyIfNotExists],
        &[PhysicalVariant::CopyOverwrite],
        PathCoverage::NoProductionCaller {
            reason: "the production storage gateway exposes copy-if-absent only, never overwrite COPY",
        },
    ),
    path(
        "observer.backend_transport_attempts",
        "src/storage/store.rs:ZeppelinStore::list_prefix",
        &[StoreMethod::Get, StoreMethod::ListPrefix],
        &[PhysicalVariant::GetFull, PhysicalVariant::ListRecursive],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "the ObjectStore trait exposes one adapter invocation, not backend HTTP retries or individual recursive-LIST pages; transport-attempt accounting needs a lower-level client hook",
        },
    ),
    path(
        "vector.fetch_projected",
        "src/server/handlers/vectors.rs:get_vectors",
        &[StoreMethod::Get, StoreMethod::GetRange],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetRange],
        PathCoverage::IdealScenario {
            scenario: "fetch.strong_with_attributes",
        },
    ),
    path(
        "vector.upsert",
        "src/server/handlers/vectors.rs:upsert_vectors",
        &[StoreMethod::Put, StoreMethod::GetWithMeta, StoreMethod::PutIfMatch],
        &[
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::GetFull,
            PhysicalVariant::PutUpdate,
        ],
        PathCoverage::ExistingFrozen {
            scenario: "upsert_single",
        },
    ),
    path(
        "vector.upsert_publication_failure_cleanup",
        "src/wal/writer.rs:WalWriter::cleanup_orphan_fragment",
        &[StoreMethod::Delete],
        &[PhysicalVariant::Delete],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "failure-only cleanup requires storage fault injection, which is excluded from the normal ideal-cost campaign",
        },
    ),
];

const fn frozen_case(scenario: &'static str) -> PathCoverage {
    PathCoverage::ExistingFrozen { scenario }
}

const fn ideal_case(scenario: &'static str) -> PathCoverage {
    PathCoverage::IdealScenario { scenario }
}

/// Additional source paths that make the catalog-to-production mapping total.
/// IDs use a `catalog.` prefix so they cannot collide with the older path IDs
/// above while the public iterator can sort both slices deterministically.
const ADDITIONAL_PRODUCTION_PATHS: &[ProductionPath] = &[
    path(
        "catalog.as_of_query",
        "src/server/handlers/as_of.rs:resolve_manifest",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        frozen_case("as_of_query"),
    ),
    path(
        "catalog.background.cached_tick_idle",
        "src/compaction/background.rs:compaction_loop",
        &[StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetConditional],
        ideal_case("background.cached_tick_idle"),
    ),
    path(
        "catalog.background.trigger_manifest_changed",
        "src/compaction/background.rs:compaction_loop",
        &[StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetConditional],
        ideal_case("background.trigger_manifest_changed"),
    ),
    path(
        "catalog.background.trigger_cache_invalidated",
        "src/compaction/background.rs:compaction_loop",
        &[StoreMethod::GetWithMeta],
        &[PhysicalVariant::GetFull],
        ideal_case("background.trigger_cache_invalidated"),
    ),
    path(
        "catalog.background.trigger_layout_change",
        "src/compaction/background.rs:compaction_loop",
        &[StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetConditional],
        ideal_case("background.trigger_layout_change"),
    ),
    path(
        "catalog.background.discovery_tick_empty",
        "src/compaction/background.rs:compaction_loop",
        &[StoreMethod::ListCommonPrefixes],
        &[PhysicalVariant::ListDelimiter],
        ideal_case("background.discovery_tick_empty"),
    ),
    path(
        "catalog.background.tick_compaction_success",
        "src/compaction/background.rs:compaction_loop",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
            StoreMethod::ListPrefix,
            StoreMethod::Delete,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::Delete,
        ],
        ideal_case("background.tick_compaction_success"),
    ),
    path(
        "catalog.background.tick_lease_held",
        "src/compaction/background.rs:compact_namespace_under_lease_with_lifecycle",
        &[StoreMethod::Get, StoreMethod::GetWithMeta],
        &[PhysicalVariant::GetFull],
        ideal_case("background.tick_lease_held"),
    ),
    path(
        "catalog.background.tick_resume_delete",
        "src/compaction/background.rs:compaction_loop",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefix,
            StoreMethod::DeletePrefixPaged,
            StoreMethod::DeleteMany,
            StoreMethod::Delete,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::DeleteBatch,
            PhysicalVariant::Delete,
        ],
        ideal_case("background.tick_resume_delete"),
    ),
    path(
        "catalog.batch_query.eventual_compacted_and_wal",
        "src/server/handlers/query.rs:batch_query_namespace",
        &[StoreMethod::Get, StoreMethod::GetRange],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetRange],
        ideal_case("batch_query.eventual_compacted_and_wal"),
    ),
    path(
        "catalog.clone.generation",
        "src/server/handlers/namespace.rs:clone_namespace",
        &[
            StoreMethod::Get,
            StoreMethod::PutIfNotExists,
            StoreMethod::CopyIfNotExists,
            StoreMethod::Delete,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutCreate,
            PhysicalVariant::CopyIfAbsent,
            PhysicalVariant::Delete,
        ],
        ideal_case("clone.generation"),
    ),
    path(
        "catalog.clone.snapshot",
        "src/server/handlers/namespace.rs:clone_namespace",
        &[
            StoreMethod::Get,
            StoreMethod::PutIfNotExists,
            StoreMethod::CopyIfNotExists,
            StoreMethod::Delete,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutCreate,
            PhysicalVariant::CopyIfAbsent,
            PhysicalVariant::Delete,
        ],
        ideal_case("clone.snapshot"),
    ),
    path(
        "catalog.cold_query_sketch_adc",
        "src/server/handlers/query.rs:query_namespace",
        &[StoreMethod::Get, StoreMethod::GetRange],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetRange],
        frozen_case("cold_query_sketch_adc"),
    ),
    path(
        "catalog.compaction.all_vectors_deleted",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
        ],
        ideal_case("compaction.all_vectors_deleted"),
    ),
    path(
        "catalog.compaction.direct_noop",
        "src/compaction/mod.rs:Compactor::compact",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("compaction.direct_noop"),
    ),
    path(
        "catalog.compaction.fenced_incremental",
        "src/compaction/background.rs:compact_namespace_under_lease_with_lifecycle",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
            StoreMethod::ListPrefix,
            StoreMethod::Delete,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::Delete,
        ],
        ideal_case("compaction.fenced_incremental"),
    ),
    path(
        "catalog.compaction.fragment_cache_warm",
        "src/compaction/background.rs:compact_namespace_under_lease_with_lifecycle",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
            StoreMethod::ListPrefix,
            StoreMethod::Delete,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::Delete,
        ],
        ideal_case("compaction.fragment_cache_warm"),
    ),
    path(
        "catalog.compaction.full_with_fts",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
        ],
        ideal_case("compaction.full_with_fts"),
    ),
    path(
        "catalog.compaction.http_accepted",
        "src/server/handlers/namespace.rs:compact_namespace",
        &[
            StoreMethod::GetIfNoneMatch,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetConditional,
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
        ],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "HTTP 202 spawns detached work without an owned completion handle",
        },
    ),
    path(
        "catalog.compaction.http_noop",
        "src/server/handlers/namespace.rs:compact_namespace",
        &[StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetConditional],
        ideal_case("compaction.http_noop"),
    ),
    path(
        "catalog.compaction.layout_rewrite_no_wal",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
            StoreMethod::ListPrefix,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
            PhysicalVariant::ListRecursive,
        ],
        ideal_case("compaction.layout_rewrite_no_wal"),
    ),
    path(
        "catalog.compaction.post_success_warm_cache_hit",
        "src/cache/manifest_cache.rs:ManifestCache::get_strong",
        &[StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetConditional],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "post-success cache state is not isolated from compaction setup yet",
        },
    ),
    path(
        "catalog.compaction.post_success_warm_cache_miss",
        "src/cache/manifest_cache.rs:ManifestCache::get_strong",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "post-success cache state is not isolated from compaction setup yet",
        },
    ),
    path(
        "catalog.delete_single",
        "src/server/handlers/vectors.rs:delete_vectors",
        &[
            StoreMethod::Put,
            StoreMethod::GetWithMeta,
            StoreMethod::PutIfMatch,
        ],
        &[
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::GetFull,
            PhysicalVariant::PutUpdate,
        ],
        frozen_case("delete_single"),
    ),
    path(
        "catalog.fetch.eventual_compacted",
        "src/server/handlers/vectors.rs:get_vectors",
        &[StoreMethod::Get, StoreMethod::GetRange],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetRange],
        ideal_case("fetch.eventual_compacted"),
    ),
    path(
        "catalog.fetch.strong_compacted_and_wal",
        "src/server/handlers/vectors.rs:get_vectors",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("fetch.strong_compacted_and_wal"),
    ),
    path(
        "catalog.fetch.strong_miss",
        "src/server/handlers/vectors.rs:get_vectors",
        &[StoreMethod::Get, StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetConditional],
        ideal_case("fetch.strong_miss"),
    ),
    path(
        "catalog.fetch.strong_mixed_hit_miss",
        "src/server/handlers/vectors.rs:get_vectors",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("fetch.strong_compacted_and_wal"),
    ),
    path(
        "catalog.fetch.strong_wal_only",
        "src/server/handlers/vectors.rs:get_vectors",
        &[StoreMethod::Get, StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetConditional],
        ideal_case("fetch.strong_wal_only"),
    ),
    path(
        "catalog.fetch_strong",
        "src/server/handlers/vectors.rs:get_vectors",
        &[StoreMethod::Get, StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetConditional],
        frozen_case("fetch_strong"),
    ),
    path(
        "catalog.filtered_query",
        "src/server/handlers/query.rs:query_namespace",
        &[
            StoreMethod::Get,
            StoreMethod::GetRange,
            StoreMethod::GetIfNoneMatch,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::GetRange,
            PhysicalVariant::GetConditional,
        ],
        frozen_case("filtered_query"),
    ),
    path(
        "catalog.filtered_query_bitmap",
        "src/server/handlers/query.rs:query_namespace",
        &[
            StoreMethod::Get,
            StoreMethod::GetRange,
            StoreMethod::GetIfNoneMatch,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::GetRange,
            PhysicalVariant::GetConditional,
        ],
        frozen_case("filtered_query_bitmap"),
    ),
    path(
        "catalog.fts_query",
        "src/server/handlers/query.rs:query_namespace",
        &[StoreMethod::Get, StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetConditional],
        frozen_case("fts_query"),
    ),
    path(
        "catalog.gc.active_staging_expired_lease",
        "src/compaction/gc.rs:active_staged_keys_at",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("gc.active_staging_expired_lease"),
    ),
    path(
        "catalog.gc.active_staging_missing_lease",
        "src/compaction/gc.rs:active_staged_keys_at",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("gc.active_staging_missing_lease"),
    ),
    path(
        "catalog.gc.active_staging_mixed_tokens",
        "src/compaction/gc.rs:active_staged_keys_at",
        &[StoreMethod::Get, StoreMethod::ListPrefix],
        &[PhysicalVariant::GetFull, PhysicalVariant::ListRecursive],
        ideal_case("gc.active_staging_mixed_tokens"),
    ),
    path(
        "catalog.gc.idle_warm_second_cycle",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[StoreMethod::ListPrefixMeta],
        &[PhysicalVariant::ListRecursive],
        ideal_case("gc.idle_warm_second_cycle"),
    ),
    path(
        "catalog.gc.idle_new_orphan",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
            StoreMethod::DeleteMany,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::DeleteBatch,
        ],
        ideal_case("gc.idle_new_orphan"),
    ),
    path(
        "catalog.gc.idle_candidate_maturity",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
            StoreMethod::DeleteMany,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::DeleteBatch,
        ],
        ideal_case("gc.idle_candidate_maturity"),
    ),
    path(
        "catalog.gc.idle_pending_delete_maturity",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
            StoreMethod::DeleteMany,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
            PhysicalVariant::DeleteBatch,
        ],
        ideal_case("gc.idle_pending_delete_maturity"),
    ),
    path(
        "catalog.gc.idle_pitr_expiry",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
            StoreMethod::DeleteMany,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::DeleteBatch,
        ],
        ideal_case("gc.idle_pitr_expiry"),
    ),
    path(
        "catalog.gc.idle_staging_lease_expiry",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
            StoreMethod::DeleteMany,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::DeleteBatch,
        ],
        ideal_case("gc.idle_staging_lease_expiry"),
    ),
    path(
        "catalog.gc.idle_changed_snapshot",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.idle_changed_snapshot"),
    ),
    path(
        "catalog.gc.parallel_snapshot_pins",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.parallel_snapshot_pins"),
    ),
    path(
        "catalog.gc.idle_changed_staging",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.idle_changed_staging"),
    ),
    path(
        "catalog.gc.idle_changed_candidate_ledger",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.idle_changed_candidate_ledger"),
    ),
    path(
        "catalog.gc.idle_backward_clock",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.idle_backward_clock"),
    ),
    path(
        "catalog.gc.idle_shorter_retention_config",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
            StoreMethod::DeleteMany,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::DeleteBatch,
        ],
        ideal_case("gc.idle_shorter_retention_config"),
    ),
    path(
        "catalog.gc.idle_prior_partial_failure",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.idle_prior_partial_failure"),
    ),
    path(
        "catalog.gc.prune_reuse_empty_pending_uncacheable",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.prune_reuse_empty_pending_uncacheable"),
    ),
    path(
        "catalog.gc.prune_reuse_eligible_pending_refresh",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
            StoreMethod::DeleteMany,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
            PhysicalVariant::DeleteBatch,
        ],
        ideal_case("gc.prune_reuse_eligible_pending_refresh"),
    ),
    path(
        "catalog.gc.history_memo_new_generation",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.history_memo_new_generation"),
    ),
    path(
        "catalog.gc.history_memo_changed_etag",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.history_memo_changed_etag"),
    ),
    path(
        "catalog.gc.history_memo_missing_etag",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.history_memo_missing_etag"),
    ),
    path(
        "catalog.gc.history_memo_disappears_between_list_and_get",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[StoreMethod::Get, StoreMethod::ListPrefixMeta],
        &[PhysicalVariant::GetFull, PhysicalVariant::ListRecursive],
        ideal_case("gc.history_memo_disappears_between_list_and_get"),
    ),
    path(
        "catalog.gc.history_memo_unpublished_orphan_overwrite",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.history_memo_unpublished_orphan_overwrite"),
    ),
    path(
        "catalog.gc.history_memo_corrupt_changed_body",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[StoreMethod::Get, StoreMethod::ListPrefixMeta],
        &[PhysicalVariant::GetFull, PhysicalVariant::ListRecursive],
        ideal_case("gc.history_memo_corrupt_changed_body"),
    ),
    path(
        "catalog.gc.history_memo_namespace_recreated",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.history_memo_namespace_recreated"),
    ),
    path(
        "catalog.gc.history_memo_cold_runner_restart",
        "src/compaction/gc.rs:GcRunner::run_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefixMeta,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.history_memo_cold_runner_restart"),
    ),
    path(
        "catalog.gc.manifest_history_prune",
        "src/wal/manifest.rs:Manifest::prune_history",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefix,
            StoreMethod::DeleteMany,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::DeleteBatch,
        ],
        ideal_case("gc.manifest_history_prune"),
    ),
    path(
        "catalog.gc.orphan_mark",
        "src/compaction/gc.rs:run_gc_cycle_at",
        &[StoreMethod::Get, StoreMethod::ListPrefix, StoreMethod::Put],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.orphan_mark"),
    ),
    path(
        "catalog.gc.orphan_sweep",
        "src/compaction/gc.rs:run_gc_cycle_at",
        &[
            StoreMethod::Get,
            StoreMethod::ListPrefix,
            StoreMethod::DeleteMany,
            StoreMethod::Put,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::DeleteBatch,
            PhysicalVariant::PutOverwrite,
        ],
        ideal_case("gc.orphan_sweep"),
    ),
    path(
        "catalog.gc.pending_delete_history_pinned",
        "src/compaction/gc.rs:drain_pending_deletes_at",
        &[StoreMethod::Get, StoreMethod::ListPrefix],
        &[PhysicalVariant::GetFull, PhysicalVariant::ListRecursive],
        ideal_case("gc.pending_delete_history_pinned"),
    ),
    path(
        "catalog.gc.pending_delete_young",
        "src/compaction/gc.rs:drain_pending_deletes_at",
        &[StoreMethod::Get, StoreMethod::ListPrefix],
        &[PhysicalVariant::GetFull, PhysicalVariant::ListRecursive],
        ideal_case("gc.pending_delete_young"),
    ),
    path(
        "catalog.gc.staging_clear",
        "src/compaction/gc.rs:clear_compaction_staging",
        &[StoreMethod::Delete],
        &[PhysicalVariant::Delete],
        ideal_case("gc.staging_clear"),
    ),
    path(
        "catalog.gc.staging_write",
        "src/compaction/gc.rs:write_compaction_staging",
        &[StoreMethod::Put],
        &[PhysicalVariant::PutOverwrite],
        ideal_case("gc.staging_write"),
    ),
    path(
        "catalog.hybrid_query",
        "src/server/handlers/query.rs:query_namespace",
        &[
            StoreMethod::Get,
            StoreMethod::GetRange,
            StoreMethod::GetIfNoneMatch,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::GetRange,
            PhysicalVariant::GetConditional,
        ],
        frozen_case("hybrid_query"),
    ),
    path(
        "catalog.hydration.accepted_bitmap",
        "src/cache/hydration.rs:hydrate_segment_once",
        &[StoreMethod::Head, StoreMethod::Get],
        &[PhysicalVariant::Head, PhysicalVariant::GetFull],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "detached hydrator exposes no owned completion handle",
        },
    ),
    path(
        "catalog.hydration.accepted_fts",
        "src/cache/hydration.rs:hydrate_segment_once",
        &[StoreMethod::Head, StoreMethod::Get],
        &[PhysicalVariant::Head, PhysicalVariant::GetFull],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "detached hydrator exposes no owned completion handle",
        },
    ),
    path(
        "catalog.hydration.incremental_refused",
        "src/cache/hydration.rs:hydrate_segment_once",
        &[StoreMethod::Head],
        &[PhysicalVariant::Head],
        PathCoverage::ExplicitGap {
            catalog_case: None,
            reason: "detached hydrator exposes no owned completion handle for refusal accounting",
        },
    ),
    path(
        "catalog.lease.acquire_expired_takeover",
        "src/wal/lease.rs:LeaseManager::acquire",
        &[
            StoreMethod::GetWithMeta,
            StoreMethod::PutIfMatch,
            StoreMethod::Get,
        ],
        &[PhysicalVariant::GetFull, PhysicalVariant::PutUpdate],
        ideal_case("lease.acquire_expired_takeover"),
    ),
    path(
        "catalog.lease.acquire_live_held",
        "src/wal/lease.rs:LeaseManager::acquire",
        &[StoreMethod::GetWithMeta],
        &[PhysicalVariant::GetFull],
        ideal_case("lease.acquire_live_held"),
    ),
    path(
        "catalog.lease.release_missing",
        "src/wal/lease.rs:LeaseManager::release",
        &[StoreMethod::GetWithMeta],
        &[PhysicalVariant::GetFull],
        ideal_case("lease.release_missing"),
    ),
    path(
        "catalog.lease.renew_double_conflict",
        "src/wal/lease.rs:LeaseManager::renew",
        &[StoreMethod::PutIfMatch, StoreMethod::GetWithMeta],
        &[PhysicalVariant::PutUpdate, PhysicalVariant::GetFull],
        ideal_case("lease.renew_double_conflict"),
    ),
    path(
        "catalog.lease.renew_etag_drift",
        "src/wal/lease.rs:LeaseManager::renew",
        &[StoreMethod::PutIfMatch, StoreMethod::GetWithMeta],
        &[PhysicalVariant::PutUpdate, PhysicalVariant::GetFull],
        ideal_case("lease.renew_etag_drift"),
    ),
    path(
        "catalog.lease.renew_missing",
        "src/wal/lease.rs:LeaseManager::renew",
        &[StoreMethod::PutIfMatch, StoreMethod::GetWithMeta],
        &[PhysicalVariant::PutUpdate, PhysicalVariant::GetFull],
        ideal_case("lease.renew_missing"),
    ),
    path(
        "catalog.lease.renew_cold",
        "src/wal/lease.rs:LeaseManager::renew",
        &[StoreMethod::GetWithMeta, StoreMethod::PutIfMatch],
        &[PhysicalVariant::GetFull, PhysicalVariant::PutUpdate],
        ideal_case("lease.renew_cold"),
    ),
    path(
        "catalog.lease.renew_put_etag_missing",
        "src/wal/lease.rs:LeaseManager::renew",
        &[StoreMethod::PutIfMatch, StoreMethod::GetWithMeta],
        &[PhysicalVariant::PutUpdate, PhysicalVariant::GetFull],
        ideal_case("lease.renew_put_etag_missing"),
    ),
    path(
        "catalog.lease.renew_taken_over",
        "src/wal/lease.rs:LeaseManager::renew",
        &[StoreMethod::PutIfMatch, StoreMethod::GetWithMeta],
        &[PhysicalVariant::PutUpdate, PhysicalVariant::GetFull],
        ideal_case("lease.renew_taken_over"),
    ),
    path(
        "catalog.lease.release_owned",
        "src/wal/lease.rs:LeaseManager::release",
        &[StoreMethod::GetWithMeta, StoreMethod::Put],
        &[PhysicalVariant::GetFull, PhysicalVariant::PutOverwrite],
        ideal_case("lease.release_owned"),
    ),
    path(
        "catalog.lease.release_taken_over",
        "src/wal/lease.rs:LeaseManager::release",
        &[StoreMethod::GetWithMeta],
        &[PhysicalVariant::GetFull],
        ideal_case("lease.release_taken_over"),
    ),
    path(
        "catalog.manifest_cache.eventual_expired",
        "src/cache/manifest_cache.rs:ManifestCache::get_required",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("manifest_cache.eventual_expired"),
    ),
    path(
        "catalog.manifest_cache.strong_concurrent_coalesced",
        "src/cache/manifest_cache.rs:ManifestCache::get_strong_required",
        &[StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetConditional],
        ideal_case("manifest_cache.strong_concurrent_coalesced"),
    ),
    path(
        "catalog.manifest_cache.strong_etag_changed",
        "src/cache/manifest_cache.rs:ManifestCache::get_strong_required",
        &[StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetConditional],
        ideal_case("manifest_cache.strong_etag_changed"),
    ),
    path(
        "catalog.manifest_cache.strong_optional_conditional_not_found",
        "src/cache/manifest_cache.rs:ManifestCache::get_strong",
        &[StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetConditional],
        ideal_case("manifest_cache.strong_optional_conditional_not_found"),
    ),
    path(
        "catalog.manifest_cache.strong_required_missing",
        "src/cache/manifest_cache.rs:ManifestCache::get_strong_required",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("manifest_cache.strong_required_missing"),
    ),
    path(
        "catalog.manifest_cache.strong_write_through_without_etag",
        "src/cache/manifest_cache.rs:ManifestCache::get_strong_required",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("manifest_cache.strong_write_through_without_etag"),
    ),
    path(
        "catalog.namespace.compaction_status",
        "src/server/handlers/namespace.rs:get_compaction_status",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("namespace.compaction_status"),
    ),
    path(
        "catalog.namespace.create_idempotent_existing",
        "src/namespace/manager.rs:NamespaceManager::create_idempotent_with_fts_and_index_config",
        &[
            StoreMethod::PutIfNotExistsWithUserMetadata,
            StoreMethod::GetWithObjectMetadata,
        ],
        &[PhysicalVariant::PutCreate, PhysicalVariant::GetFull],
        ideal_case("namespace.create_idempotent_existing"),
    ),
    path(
        "catalog.namespace.delete_cleanup_incomplete",
        "src/namespace/manager.rs:NamespaceManager::finish_delete",
        &[
            StoreMethod::DeletePrefixPaged,
            StoreMethod::DeleteMany,
            StoreMethod::ListPrefix,
            StoreMethod::Delete,
        ],
        &[
            PhysicalVariant::ListRecursive,
            PhysicalVariant::DeleteBatch,
            PhysicalVariant::Delete,
        ],
        ideal_case("namespace.delete_cleanup_incomplete"),
    ),
    path(
        "catalog.namespace.delete_publish_tombstone",
        "src/namespace/manager.rs:NamespaceManager::start_delete",
        &[
            StoreMethod::GetWithObjectMetadata,
            StoreMethod::PutIfMatchWithUserMetadata,
            StoreMethod::Delete,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::Delete,
        ],
        ideal_case("namespace.delete_publish_tombstone"),
    ),
    path(
        "catalog.namespace.get_metadata_resident",
        "src/server/handlers/namespace.rs:get_namespace",
        &[StoreMethod::GetIfNoneMatch],
        &[PhysicalVariant::GetConditional],
        ideal_case("namespace.get_metadata_resident"),
    ),
    path(
        "catalog.namespace.scan_active_many",
        "src/namespace/manager.rs:NamespaceManager::list",
        &[
            StoreMethod::ListCommonPrefixes,
            StoreMethod::GetWithObjectMetadata,
        ],
        &[PhysicalVariant::ListDelimiter, PhysicalVariant::GetFull],
        ideal_case("namespace.scan_active_many"),
    ),
    path(
        "catalog.namespace.scan_empty",
        "src/namespace/manager.rs:NamespaceManager::list",
        &[StoreMethod::ListCommonPrefixes],
        &[PhysicalVariant::ListDelimiter],
        ideal_case("namespace.scan_empty"),
    ),
    path(
        "catalog.namespace.scan_recover_creating_manifest_missing",
        "src/namespace/manager.rs:NamespaceManager::list",
        &[
            StoreMethod::ListCommonPrefixes,
            StoreMethod::GetWithObjectMetadata,
            StoreMethod::Put,
            StoreMethod::PutIfMatchWithUserMetadata,
        ],
        &[
            PhysicalVariant::ListDelimiter,
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
        ],
        ideal_case("namespace.scan_recover_creating_manifest_missing"),
    ),
    path(
        "catalog.namespace.scan_recover_creating_manifest_present",
        "src/namespace/manager.rs:NamespaceManager::list",
        &[
            StoreMethod::ListCommonPrefixes,
            StoreMethod::GetWithObjectMetadata,
            StoreMethod::PutIfMatchWithUserMetadata,
        ],
        &[
            PhysicalVariant::ListDelimiter,
            PhysicalVariant::GetFull,
            PhysicalVariant::PutUpdate,
        ],
        ideal_case("namespace.scan_recover_creating_manifest_present"),
    ),
    path(
        "catalog.paginate",
        "src/server/handlers/query.rs:query_namespace",
        &[
            StoreMethod::Get,
            StoreMethod::GetRange,
            StoreMethod::GetIfNoneMatch,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::GetRange,
            PhysicalVariant::GetConditional,
        ],
        frozen_case("paginate"),
    ),
    path(
        "catalog.query.ann_eventual_compacted_and_wal",
        "src/server/handlers/query.rs:query_namespace",
        &[StoreMethod::Get, StoreMethod::GetRange],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetRange],
        ideal_case("query.ann_eventual_compacted_and_wal"),
    ),
    path(
        "catalog.query.ann_eventual_wal_only",
        "src/server/handlers/query.rs:query_namespace",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.ann_eventual_wal_only"),
    ),
    path(
        "catalog.query.ann_include_attributes",
        "src/server/handlers/query.rs:query_namespace",
        &[
            StoreMethod::Get,
            StoreMethod::GetRange,
            StoreMethod::GetIfNoneMatch,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::GetRange,
            PhysicalVariant::GetConditional,
        ],
        ideal_case("query.ann_include_attributes"),
    ),
    path(
        "catalog.query.ann_strong_compacted_and_wal",
        "src/server/handlers/query.rs:query_namespace",
        &[
            StoreMethod::Get,
            StoreMethod::GetRange,
            StoreMethod::GetIfNoneMatch,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::GetRange,
            PhysicalVariant::GetConditional,
        ],
        ideal_case("query.ann_strong_compacted_and_wal"),
    ),
    path(
        "catalog.query.ann_strong_wal_only",
        "src/server/handlers/query.rs:query_namespace",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.ann_strong_wal_only"),
    ),
    path(
        "catalog.query.ann_vector_rerank",
        "src/server/handlers/query.rs:apply_vector_rerank",
        &[StoreMethod::Get, StoreMethod::GetRange],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetRange],
        ideal_case("query.ann_vector_rerank"),
    ),
    path(
        "catalog.query.as_of_snapshot",
        "src/server/handlers/as_of.rs:resolve_manifest",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.as_of_snapshot"),
    ),
    path(
        "catalog.snapshot.get",
        "src/wal/manifest.rs:NamedSnapshot::read",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("snapshot.get"),
    ),
    path(
        "catalog.upsert_batch",
        "src/server/handlers/vectors.rs:upsert_vectors",
        &[
            StoreMethod::Put,
            StoreMethod::GetWithMeta,
            StoreMethod::PutIfMatch,
        ],
        &[
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::GetFull,
            PhysicalVariant::PutUpdate,
        ],
        frozen_case("upsert_batch"),
    ),
    path(
        "catalog.vector.delete_batch",
        "src/server/handlers/vectors.rs:delete_vectors",
        &[
            StoreMethod::Put,
            StoreMethod::GetWithMeta,
            StoreMethod::PutIfMatch,
        ],
        &[
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::GetFull,
            PhysicalVariant::PutUpdate,
        ],
        ideal_case("vector.delete_batch"),
    ),
    path(
        "catalog.vector.upsert_into_compacted",
        "src/server/handlers/vectors.rs:upsert_vectors",
        &[
            StoreMethod::Put,
            StoreMethod::GetWithMeta,
            StoreMethod::PutIfMatch,
        ],
        &[
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::GetFull,
            PhysicalVariant::PutUpdate,
        ],
        ideal_case("vector.upsert_into_compacted"),
    ),
    path(
        "catalog.vector.upsert_into_empty",
        "src/server/handlers/vectors.rs:upsert_vectors",
        &[
            StoreMethod::Put,
            StoreMethod::GetWithMeta,
            StoreMethod::PutIfMatch,
        ],
        &[
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::GetFull,
            PhysicalVariant::PutUpdate,
        ],
        ideal_case("vector.upsert_into_empty"),
    ),
    path(
        "catalog.writer.group_commit_conflict",
        "src/wal/writer.rs:WalWriter::commit_pending_group",
        &[
            StoreMethod::Put,
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::PutIfNotExists,
            StoreMethod::PutIfMatch,
        ],
        &[
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::GetFull,
            PhysicalVariant::PutCreate,
            PhysicalVariant::PutUpdate,
        ],
        ideal_case("writer.group_commit_conflict"),
    ),
    path(
        "catalog.writer.group_commit_missing_put_etag",
        "src/wal/writer.rs:WalWriter::commit_pending_group",
        &[
            StoreMethod::Put,
            StoreMethod::GetWithMeta,
            StoreMethod::PutIfNotExists,
            StoreMethod::PutIfMatch,
        ],
        &[
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::GetFull,
            PhysicalVariant::PutCreate,
            PhysicalVariant::PutUpdate,
        ],
        ideal_case("writer.group_commit_missing_put_etag"),
    ),
    path(
        "catalog.writer.group_commit_warm",
        "src/wal/writer.rs:WalWriter::commit_pending_group",
        &[
            StoreMethod::Put,
            StoreMethod::PutIfNotExists,
            StoreMethod::PutIfMatch,
        ],
        &[
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutCreate,
            PhysicalVariant::PutUpdate,
        ],
        ideal_case("writer.group_commit_warm"),
    ),
    path(
        "catalog.warm_query_eventual",
        "src/server/handlers/query.rs:query_namespace",
        &[StoreMethod::Get, StoreMethod::GetRange],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetRange],
        frozen_case("warm_query_eventual"),
    ),
    path(
        "catalog.secured_query",
        "src/server/handlers/query.rs:query_namespace",
        &[StoreMethod::Get, StoreMethod::GetRange],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetRange],
        frozen_case("secured_query"),
    ),
    path(
        "catalog.secured_filtered_query",
        "src/server/handlers/query.rs:query_namespace",
        &[StoreMethod::Get, StoreMethod::GetRange],
        &[PhysicalVariant::GetFull, PhysicalVariant::GetRange],
        frozen_case("secured_filtered_query"),
    ),
    path(
        "catalog.query.flat_none_filtered_no_bitmap",
        "src/index/ivf_flat/search.rs:search_ivf_flat",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.flat_none_filtered_no_bitmap"),
    ),
    path(
        "catalog.query.flat_pq_unfiltered_current",
        "src/index/ivf_flat/search.rs:scan_clusters_pq",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.flat_pq_unfiltered_current"),
    ),
    path(
        "catalog.query.flat_pq_filtered_bitmap",
        "src/index/ivf_flat/search.rs:try_bitmap_prefilter",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.flat_pq_filtered_bitmap"),
    ),
    path(
        "catalog.query.hierarchical_none_shallow_unfiltered",
        "src/index/hierarchical/search.rs:search_hierarchical",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.hierarchical_none_shallow_unfiltered"),
    ),
    path(
        "catalog.query.hierarchical_sq_deep_filtered_no_bitmap",
        "src/index/hierarchical/search.rs:scan_clusters_sq",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.hierarchical_sq_deep_filtered_no_bitmap"),
    ),
    path(
        "catalog.query.hierarchical_pq_deep_filtered_bitmap",
        "src/index/hierarchical/search.rs:scan_clusters_pq",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.hierarchical_pq_deep_filtered_bitmap"),
    ),
    path(
        "catalog.query.flat_legacy_sq_standalone_sketch",
        "src/index/ivf_flat/search.rs:search_ivf_flat",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.flat_legacy_sq_standalone_sketch"),
    ),
    path(
        "catalog.query.flat_legacy_none_no_sketch",
        "src/index/ivf_flat/search.rs:search_ivf_flat",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.flat_legacy_none_no_sketch"),
    ),
    path(
        "catalog.query.fts_global_cold",
        "src/query.rs:execute_bm25_query_with_manifest",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.fts_global_cold"),
    ),
    path(
        "catalog.query.fts_per_cluster_fallback",
        "src/query.rs:execute_bm25_query_with_manifest",
        &[StoreMethod::Get],
        &[PhysicalVariant::GetFull],
        ideal_case("query.fts_per_cluster_fallback"),
    ),
    path(
        "catalog.compaction.flat_pq_full",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
        ],
        ideal_case("compaction.flat_pq_full"),
    ),
    path(
        "catalog.compaction.flat_pq_incremental",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
            StoreMethod::ListPrefix,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
            PhysicalVariant::ListRecursive,
        ],
        ideal_case("compaction.flat_pq_incremental"),
    ),
    path(
        "catalog.compaction.flat_sq_populated_bitmap",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
        ],
        ideal_case("compaction.flat_sq_populated_bitmap"),
    ),
    path(
        "catalog.compaction.hierarchical_sq_full",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
        ],
        ideal_case("compaction.hierarchical_sq_full"),
    ),
    path(
        "catalog.compaction.hierarchical_pq_full",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
        ],
        ideal_case("compaction.hierarchical_pq_full"),
    ),
    path(
        "catalog.compaction.hierarchical_existing_small_wal_full_rewrite",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
            StoreMethod::ListPrefix,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
            PhysicalVariant::ListRecursive,
        ],
        ideal_case("compaction.hierarchical_existing_small_wal_full_rewrite"),
    ),
    path(
        "catalog.compaction.hierarchical_full_with_fts",
        "src/compaction/mod.rs:Compactor::compact",
        &[
            StoreMethod::Get,
            StoreMethod::GetWithMeta,
            StoreMethod::Put,
            StoreMethod::PutIfMatch,
            StoreMethod::PutIfNotExists,
        ],
        &[
            PhysicalVariant::GetFull,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::PutCreate,
        ],
        ideal_case("compaction.hierarchical_full_with_fts"),
    ),
];

#[must_use]
pub(crate) fn production_paths() -> Vec<&'static ProductionPath> {
    let mut paths = PRODUCTION_PATHS
        .iter()
        .chain(ADDITIONAL_PRODUCTION_PATHS)
        .collect::<Vec<_>>();
    paths.sort_by_key(|path| path.id);
    paths
}

#[must_use]
pub(crate) fn scenario_references() -> BTreeSet<ScenarioReference> {
    production_paths()
        .into_iter()
        .filter_map(|path| match path.coverage {
            PathCoverage::ExistingFrozen { scenario } => Some(ScenarioReference {
                kind: ScenarioKind::ExistingFrozen,
                id: scenario,
            }),
            PathCoverage::IdealScenario { scenario } => Some(ScenarioReference {
                kind: ScenarioKind::IdealScenario,
                id: scenario,
            }),
            PathCoverage::ExplicitGap { .. } | PathCoverage::NoProductionCaller { .. } => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::catalog::{self, IdealOperation};
    use super::*;

    #[test]
    fn storage_gateway_inventory_matches_public_async_methods() {
        assert_eq!(
            inventory_store_methods(),
            source_public_async_store_methods(),
            "every public async ZeppelinStore method needs an explicit ideal-analysis disposition"
        );
    }

    #[test]
    fn non_s3_exclusions_are_explicit() {
        for method in STORAGE_METHOD_INVENTORY {
            if let StorageMethodDisposition::ExcludedNonS3 { reason } = method.disposition {
                assert!(
                    !reason.trim().is_empty(),
                    "excluded method {} needs a reason",
                    method.name
                );
            }
        }
    }

    #[test]
    fn production_path_ids_are_unique_and_stably_ordered() {
        let ids = production_paths()
            .iter()
            .map(|path| path.id)
            .collect::<Vec<_>>();
        let unique = ids.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(
            unique.len(),
            ids.len(),
            "production path IDs must be unique"
        );
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn every_s3_storage_method_has_a_path_or_explicit_no_caller_record() {
        for method in STORAGE_METHOD_INVENTORY {
            if method.disposition == StorageMethodDisposition::ExercisesObjectStore {
                assert!(
                    production_paths()
                        .iter()
                        .any(|path| path.store_methods.contains(&method.method)),
                    "S3 method {} is absent from the production-path inventory",
                    method.name
                );
            }
        }
    }

    #[test]
    fn security_policy_storage_states_have_explicit_paths() {
        let paths = production_paths()
            .into_iter()
            .map(|path| (path.id, path))
            .collect::<std::collections::BTreeMap<_, _>>();

        for (id, methods, variants) in [
            (
                "security.policy.load_current",
                &[StoreMethod::GetWithMeta, StoreMethod::Get][..],
                &[PhysicalVariant::GetFull][..],
            ),
            (
                "security.policy.refresh_unchanged",
                &[StoreMethod::GetIfNoneMatch][..],
                &[PhysicalVariant::GetConditional][..],
            ),
            (
                "security.policy.refresh_changed",
                &[StoreMethod::GetIfNoneMatch, StoreMethod::Get][..],
                &[PhysicalVariant::GetConditional, PhysicalVariant::GetFull][..],
            ),
        ] {
            let path = paths
                .get(id)
                .unwrap_or_else(|| panic!("missing state-specific policy path {id}"));
            assert_eq!(path.store_methods, methods, "method drift for {id}");
            assert_eq!(path.physical_variants, variants, "variant drift for {id}");
            assert!(matches!(path.coverage, PathCoverage::ExplicitGap { .. }));
        }
    }

    #[test]
    fn gaps_and_no_caller_records_are_explained() {
        for path in production_paths() {
            match path.coverage {
                PathCoverage::ExplicitGap { reason, .. }
                | PathCoverage::NoProductionCaller { reason } => {
                    assert!(!reason.trim().is_empty(), "path {} needs a reason", path.id);
                }
                PathCoverage::ExistingFrozen { scenario }
                | PathCoverage::IdealScenario { scenario } => {
                    assert!(
                        !scenario.trim().is_empty(),
                        "path {} needs a scenario ID",
                        path.id
                    );
                }
            }
        }
    }

    #[test]
    fn scenario_references_are_a_deterministic_typed_set() {
        let references = scenario_references();
        assert!(references.contains(&ScenarioReference {
            kind: ScenarioKind::ExistingFrozen,
            id: "warm_query_strong",
        }));
        assert!(references.contains(&ScenarioReference {
            kind: ScenarioKind::IdealScenario,
            id: "namespace.create_fresh",
        }));
        assert!(references.iter().all(|reference| !reference.id.is_empty()));
    }

    #[test]
    fn every_catalog_case_has_source_inventory_coverage() {
        let covered = production_paths()
            .iter()
            .filter_map(|path| match path.coverage {
                PathCoverage::ExistingFrozen { scenario }
                | PathCoverage::IdealScenario { scenario } => Some(scenario),
                PathCoverage::ExplicitGap {
                    catalog_case: Some(case),
                    ..
                } => Some(case),
                PathCoverage::ExplicitGap {
                    catalog_case: None, ..
                }
                | PathCoverage::NoProductionCaller { .. } => None,
            })
            .collect::<BTreeSet<_>>();
        let missing = catalog::all()
            .iter()
            .map(|case| case.id.as_str())
            .filter(|id| !covered.contains(id))
            .collect::<Vec<_>>();

        assert!(
            missing.is_empty(),
            "catalog cases missing inventory rows: {missing:?}"
        );
    }

    #[test]
    fn executable_references_name_matching_catalog_case_kinds() {
        for reference in scenario_references() {
            let case = catalog::all()
                .iter()
                .find(|case| case.id.as_str() == reference.id)
                .unwrap_or_else(|| {
                    panic!(
                        "inventory references absent catalog case {:?}",
                        reference.id
                    )
                });
            match (reference.kind, case.operation) {
                (ScenarioKind::ExistingFrozen, IdealOperation::FrozenContract { scenario }) => {
                    assert_eq!(scenario, reference.id);
                }
                (ScenarioKind::IdealScenario, IdealOperation::FrozenContract { .. }) => {
                    panic!(
                        "ideal inventory reference {:?} names a frozen case",
                        reference.id
                    )
                }
                (ScenarioKind::IdealScenario, _) => {}
                (ScenarioKind::ExistingFrozen, _) => {
                    panic!(
                        "frozen inventory reference {:?} names an ideal case",
                        reference.id
                    )
                }
            }
        }
    }

    #[test]
    fn every_physical_variant_has_a_production_disposition() {
        let variants = [
            PhysicalVariant::GetFull,
            PhysicalVariant::GetRange,
            PhysicalVariant::GetSuffix,
            PhysicalVariant::GetMultiRange,
            PhysicalVariant::GetConditional,
            PhysicalVariant::GetConditionalRange,
            PhysicalVariant::GetConditionalSuffix,
            PhysicalVariant::PutOverwrite,
            PhysicalVariant::PutCreate,
            PhysicalVariant::PutUpdate,
            PhysicalVariant::Head,
            PhysicalVariant::ListRecursive,
            PhysicalVariant::ListDelimiter,
            PhysicalVariant::Delete,
            PhysicalVariant::DeleteBatch,
            PhysicalVariant::CopyIfAbsent,
            PhysicalVariant::CopyOverwrite,
        ];
        for variant in variants {
            assert!(
                production_paths()
                    .iter()
                    .any(|path| path.physical_variants.contains(&variant)),
                "physical variant {variant:?} has no production or explicit no-caller row"
            );
        }
    }

    #[test]
    fn every_source_anchor_resolves_to_a_real_function() {
        for path in production_paths() {
            let (file, symbol) = path.source.split_once(':').unwrap_or_else(|| {
                panic!("inventory source anchor lacks ':' in {:?}", path.source)
            });
            let function = symbol.rsplit("::").next().expect("source symbol is empty");
            let source = std::fs::read_to_string(file).unwrap_or_else(|error| {
                panic!("inventory source file {file:?} is unreadable: {error}")
            });
            assert!(
                source.contains(&format!("fn {function}")),
                "inventory source anchor {:?} does not resolve to a function in {file}",
                path.source
            );
        }
    }
}
