//! Isolated direct measurements for compaction and garbage-collection work.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use ulid::Ulid;
use zeppelin::compaction::background::compact_namespace_under_lease;
use zeppelin::compaction::gc::{
    active_staged_keys_at, clear_compaction_staging, drain_pending_deletes_at, load_gc_candidates,
    run_gc_cycle_at, save_gc_candidates, write_compaction_staging, GcCandidate, GcCycleReport,
    GcNamespaceIncarnation, GcRunner,
};
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, GcConfig, IndexingConfig};
use zeppelin::error::ZeppelinError;
use zeppelin::fts::FtsFieldConfig;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::manager::{NamespaceIndexConfig, NamespaceManager};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::{Clock, TimeSource};
use zeppelin::types::{AttributeValue, DistanceMetric, VectorEntry};
use zeppelin::wal::fragment::WalFragment;
use zeppelin::wal::manifest::FragmentRef;
use zeppelin::wal::manifest::NamedSnapshot;
use zeppelin::wal::{LeaseManager, Manifest, WalReader, WalWriter};

use crate::common::counting::{perf_counting_store, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::perf_contract::depth::{depth_store, DepthTracker, PhysicalRequest, SpanKind};
use crate::perf_contract::scenario::RepeatCounters;

use super::artifacts::IdealSample;
use super::catalog::{
    BackgroundMaintenanceCase, CompactionCase, GarbageCollectionCase, IdealCase, IdealOperation,
};

/// Return whether this module owns a directly awaited maintenance scenario.
pub(crate) fn supports(case: &IdealCase) -> bool {
    matches!(
        case.operation,
        IdealOperation::Compaction(
            CompactionCase::DirectNoop
                | CompactionCase::DirectFull
                | CompactionCase::DirectIncremental
                | CompactionCase::LayoutRewriteNoWal
                | CompactionCase::AllVectorsDeleted
                | CompactionCase::FullWithFts
                | CompactionCase::FencedFull
                | CompactionCase::FencedIncremental
        ) | IdealOperation::GarbageCollection(_)
            | IdealOperation::BackgroundMaintenance(_)
    )
}

/// Execute one sound, directly awaited maintenance case.
///
/// The six scheduler shapes execute their owned production steps rather than
/// driving the infinite timer loop. Detached cache warming and hydration
/// deliberately remain unsupported because neither exposes an awaitable
/// completion boundary suitable for an isolated measurement interval.
pub(crate) async fn execute(case: &IdealCase) -> Option<IdealSample> {
    match case.operation {
        IdealOperation::Compaction(CompactionCase::DirectNoop) => {
            Some(execute_compaction_noop(case).await)
        }
        IdealOperation::Compaction(CompactionCase::DirectFull) => {
            Some(execute_compaction_full(case).await)
        }
        IdealOperation::Compaction(CompactionCase::DirectIncremental) => {
            Some(execute_compaction_incremental(case).await)
        }
        IdealOperation::Compaction(CompactionCase::LayoutRewriteNoWal) => {
            Some(execute_layout_rewrite(case).await)
        }
        IdealOperation::Compaction(CompactionCase::AllVectorsDeleted) => {
            Some(execute_all_vectors_deleted(case).await)
        }
        IdealOperation::Compaction(CompactionCase::FullWithFts) => {
            Some(execute_full_with_fts(case).await)
        }
        IdealOperation::Compaction(CompactionCase::FencedFull) => {
            Some(execute_fenced_full(case).await)
        }
        IdealOperation::Compaction(CompactionCase::FencedIncremental) => {
            Some(execute_fenced_incremental(case).await)
        }
        IdealOperation::GarbageCollection(operation) => execute_gc(case, operation).await,
        IdealOperation::BackgroundMaintenance(operation) => {
            Some(execute_background(case, operation).await)
        }
        IdealOperation::Compaction(_) => None,
        _ => None,
    }
}

#[derive(Debug)]
struct FixedTime(DateTime<Utc>);

impl TimeSource for FixedTime {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

struct MaintenanceWorld {
    harness: TestHarness,
    store: ZeppelinStore,
    counter: GetCounter,
    tracker: DepthTracker,
    now: DateTime<Utc>,
    cleanup_namespaces: Vec<String>,
}

impl MaintenanceWorld {
    async fn new() -> Self {
        let harness = TestHarness::new().await;
        let (depth_wrapped, tracker) = depth_store(&harness.store);
        let (store, counter) = perf_counting_store(&depth_wrapped);
        let now = DateTime::from_timestamp(1_800_000_000, 123_000_000)
            .expect("ideal maintenance fixed clock must be representable");
        Self {
            harness,
            store,
            counter,
            tracker,
            now,
            cleanup_namespaces: Vec::new(),
        }
    }

    fn namespace(&self, suffix: &str) -> String {
        self.harness.key(suffix)
    }

    fn managed_namespace(&mut self, suffix: &str) -> String {
        let suffix = suffix.replace(['.', '_'], "-");
        let namespace = format!("{}-{suffix}", self.harness.prefix);
        self.cleanup_namespaces.push(namespace.clone());
        namespace
    }

    fn clock(&self) -> Clock {
        Clock::from_source(Arc::new(FixedTime(self.now)))
    }

    async fn begin_measurement(&self) {
        await_tracker_idle(&self.tracker).await;
        self.counter.reset();
        self.tracker.reset();
    }

    async fn snapshot(&self, case: &IdealCase) -> IdealSample {
        await_tracker_idle(&self.tracker).await;
        let repeat = snapshot_repeat(&self.counter, &self.tracker);
        IdealSample::from_repeat(case.id.as_str(), &repeat)
    }

    async fn cleanup(self, sample: IdealSample) -> IdealSample {
        for namespace in &self.cleanup_namespaces {
            self.harness
                .store
                .delete_prefix(&format!("{namespace}/"))
                .await
                .expect("ideal managed namespace cleanup failed");
        }
        self.harness.cleanup().await;
        sample
    }

    async fn finish(self, case: &IdealCase) -> IdealSample {
        let sample = self.snapshot(case).await;
        self.cleanup(sample).await
    }
}

async fn execute_compaction_noop(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let indexing = IndexingConfig::default();
    let namespace =
        create_active_compaction_namespace(&mut world, case.id.as_str(), &indexing, HashMap::new())
            .await;
    let compactor = Compactor::with_clock(
        world.store.clone(),
        WalReader::new(world.store.clone()),
        CompactionConfig::default(),
        indexing,
        Duration::from_secs(300),
        world.clock(),
    );

    world.begin_measurement().await;
    let result = compactor
        .compact(&namespace)
        .await
        .expect("ideal direct no-op compaction failed");
    assert_eq!(result.vectors_compacted, 0);
    assert_eq!(result.fragments_removed, 0);
    world.finish(case).await
}

async fn execute_compaction_full(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let indexing = maintenance_indexing_config();
    let namespace =
        create_active_compaction_namespace(&mut world, case.id.as_str(), &indexing, HashMap::new())
            .await;
    WalWriter::with_clock(world.store.clone(), world.clock())
        .append(
            &namespace,
            maintenance_vectors("direct-full", 16),
            Vec::new(),
        )
        .await
        .expect("ideal direct-full WAL setup failed");
    let compactor = maintenance_compactor(&world, CompactionConfig::default());

    world.begin_measurement().await;
    let result = compactor
        .compact(&namespace)
        .await
        .expect("ideal direct full compaction failed");
    assert_eq!(result.vectors_compacted, 16);
    assert_eq!(result.fragments_removed, 1);
    assert!(result.old_segment_removed.is_none());
    assert!(result.segment_id.is_some());
    world.finish(case).await
}

async fn execute_compaction_incremental(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let indexing = maintenance_indexing_config();
    let namespace =
        create_active_compaction_namespace(&mut world, case.id.as_str(), &indexing, HashMap::new())
            .await;
    let writer = WalWriter::with_clock(world.store.clone(), world.clock());
    let resident_vectors = maintenance_vectors("direct-resident", 32);
    writer
        .append(&namespace, resident_vectors.clone(), Vec::new())
        .await
        .expect("ideal direct-incremental resident WAL setup failed");
    let compactor = maintenance_compactor(&world, trigger_each_fragment());
    let initial = compactor
        .compact(&namespace)
        .await
        .expect("ideal direct-incremental resident compaction setup failed");
    let initial_id = initial
        .segment_id
        .expect("ideal direct-incremental resident setup must publish a segment");
    writer
        .append(
            &namespace,
            maintenance_nearby_vectors("direct-added", &resident_vectors[0], 2),
            Vec::new(),
        )
        .await
        .expect("ideal direct-incremental WAL setup failed");

    world.begin_measurement().await;
    let result = compactor
        .compact(&namespace)
        .await
        .expect("ideal direct incremental compaction failed");
    assert_eq!(result.vectors_compacted, 34);
    assert_eq!(result.fragments_removed, 1);
    assert_eq!(
        result.old_segment_removed.as_deref(),
        Some(initial_id.as_str())
    );
    let segment_id = result
        .segment_id
        .expect("ideal direct incremental compaction must publish a segment");
    let sample = world.snapshot(case).await;

    // This production-state oracle deliberately uses the raw harness store
    // after the measured sample is frozen.
    let manifest = Manifest::read(&world.harness.store, &namespace)
        .await
        .expect("ideal direct-incremental manifest oracle failed")
        .expect("ideal direct-incremental manifest disappeared");
    assert_eq!(
        manifest.active_segment.as_deref(),
        Some(segment_id.as_str())
    );
    let segment = manifest
        .segments
        .iter()
        .find(|segment| segment.id == segment_id)
        .expect("ideal direct-incremental active segment ref missing");
    assert_eq!(segment.cluster_owners.len(), segment.cluster_count);
    assert!(segment
        .cluster_owners
        .iter()
        .any(|owner| owner == &initial_id));
    assert!(segment
        .cluster_owners
        .iter()
        .any(|owner| owner == &segment_id));
    world.cleanup(sample).await
}

async fn execute_layout_rewrite(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let namespace = world.managed_namespace(case.id.as_str());
    let manager = maintenance_namespace_manager(&world);
    let initial_indexing = maintenance_indexing_config();
    manager
        .create_with_fts_and_index_config(
            &namespace,
            4,
            DistanceMetric::Euclidean,
            HashMap::new(),
            Some(NamespaceIndexConfig::from_indexing_config(
                &initial_indexing,
            )),
        )
        .await
        .expect("ideal layout-rewrite namespace setup failed");
    WalWriter::with_clock(world.store.clone(), world.clock())
        .append(&namespace, maintenance_vectors("layout", 16), Vec::new())
        .await
        .expect("ideal layout-rewrite WAL setup failed");
    let compactor = maintenance_compactor(&world, CompactionConfig::default());
    let initial = compactor
        .compact(&namespace)
        .await
        .expect("ideal initial layout compaction setup failed");
    assert!(initial.segment_id.is_some());
    let mut desired = NamespaceIndexConfig::from_indexing_config(&initial_indexing);
    desired.quantization = QuantizationType::None;
    manager
        .update_index_config(&namespace, desired)
        .await
        .expect("ideal layout-rewrite config setup failed");

    world.begin_measurement().await;
    let result = compactor
        .compact(&namespace)
        .await
        .expect("ideal no-WAL layout rewrite failed");
    assert_eq!(result.vectors_compacted, 16);
    assert_eq!(result.fragments_removed, 0);
    assert!(result.segment_id.is_some());
    assert_eq!(result.old_segment_removed, initial.segment_id);
    world.finish(case).await
}

async fn execute_all_vectors_deleted(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let indexing = maintenance_indexing_config();
    let namespace =
        create_active_compaction_namespace(&mut world, case.id.as_str(), &indexing, HashMap::new())
            .await;
    let writer = WalWriter::with_clock(world.store.clone(), world.clock());
    let vectors = maintenance_vectors("deleted", 16);
    let deletes = vectors.iter().map(|vector| vector.id.clone()).collect();
    writer
        .append(&namespace, vectors, Vec::new())
        .await
        .expect("ideal all-deleted upsert setup failed");
    writer
        .append(&namespace, Vec::new(), deletes)
        .await
        .expect("ideal all-deleted tombstone setup failed");
    let compactor = maintenance_compactor(&world, CompactionConfig::default());

    world.begin_measurement().await;
    let result = compactor
        .compact(&namespace)
        .await
        .expect("ideal all-deleted compaction failed");
    assert_eq!(result.vectors_compacted, 0);
    assert_eq!(result.fragments_removed, 2);
    assert!(result.segment_id.is_none());
    world.finish(case).await
}

async fn execute_full_with_fts(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let fts_configs = HashMap::from([("content".to_string(), FtsFieldConfig::default())]);
    let mut indexing = maintenance_indexing_config();
    indexing.fts_index = true;
    indexing.bitmap_index = false;
    let namespace = create_active_compaction_namespace(
        &mut world,
        case.id.as_str(),
        &indexing,
        fts_configs.clone(),
    )
    .await;
    WalWriter::with_clock(world.store.clone(), world.clock())
        .append(&namespace, maintenance_fts_vectors(16), Vec::new())
        .await
        .expect("ideal FTS WAL setup failed");
    let compactor = maintenance_fts_compactor(&world);

    world.begin_measurement().await;
    let result = compactor
        .compact_with_fts(&namespace, None, &fts_configs)
        .await
        .expect("ideal full FTS compaction failed");
    assert_eq!(result.vectors_compacted, 16);
    assert_eq!(result.fragments_removed, 1);
    assert!(result.segment_id.is_some());
    world.finish(case).await
}

async fn execute_fenced_full(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let indexing = maintenance_indexing_config();
    let namespace =
        create_active_compaction_namespace(&mut world, case.id.as_str(), &indexing, HashMap::new())
            .await;
    WalWriter::with_clock(world.store.clone(), world.clock())
        .append(
            &namespace,
            maintenance_vectors("fenced-full", 16),
            Vec::new(),
        )
        .await
        .expect("ideal fenced-full WAL setup failed");
    let compactor = maintenance_compactor(&world, CompactionConfig::default());
    let lease_manager = maintenance_lease_manager(&world, "ideal-fenced-full");

    world.begin_measurement().await;
    let result =
        compact_namespace_under_lease(&compactor, &lease_manager, &namespace, &HashMap::new())
            .await
            .expect("ideal fenced full compaction failed");
    assert_eq!(result.vectors_compacted, 16);
    assert_eq!(result.fragments_removed, 1);
    assert!(result.segment_id.is_some());
    world.finish(case).await
}

async fn execute_fenced_incremental(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let indexing = maintenance_indexing_config();
    let namespace =
        create_active_compaction_namespace(&mut world, case.id.as_str(), &indexing, HashMap::new())
            .await;
    let writer = WalWriter::with_clock(world.store.clone(), world.clock());
    writer
        .append(&namespace, maintenance_vectors("resident", 32), Vec::new())
        .await
        .expect("ideal incremental resident WAL setup failed");
    let compactor = maintenance_compactor(&world, CompactionConfig::default());
    let initial = compactor
        .compact(&namespace)
        .await
        .expect("ideal incremental resident compaction setup failed");
    assert!(initial.segment_id.is_some());
    writer
        .append(
            &namespace,
            maintenance_vectors("incremental", 2),
            Vec::new(),
        )
        .await
        .expect("ideal incremental WAL setup failed");
    let lease_manager = maintenance_lease_manager(&world, "ideal-fenced-incremental");

    world.begin_measurement().await;
    let result =
        compact_namespace_under_lease(&compactor, &lease_manager, &namespace, &HashMap::new())
            .await
            .expect("ideal fenced incremental compaction failed");
    assert_eq!(result.vectors_compacted, 34);
    assert_eq!(result.fragments_removed, 1);
    assert!(result.segment_id.is_some());
    assert_eq!(result.old_segment_removed, initial.segment_id);
    world.finish(case).await
}

async fn execute_background(case: &IdealCase, operation: BackgroundMaintenanceCase) -> IdealSample {
    match operation {
        BackgroundMaintenanceCase::DiscoveryTickEmpty => execute_discovery_empty(case).await,
        BackgroundMaintenanceCase::DiscoveryTickActive => execute_discovery_active(case).await,
        BackgroundMaintenanceCase::CachedTickIdle => execute_cached_idle(case).await,
        BackgroundMaintenanceCase::TickResumeDelete => execute_resume_delete(case).await,
        BackgroundMaintenanceCase::TickLeaseHeld => execute_tick_lease_held(case).await,
        BackgroundMaintenanceCase::TickCompactionSuccess => {
            execute_tick_compaction_success(case).await
        }
    }
}

async fn execute_discovery_empty(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let manager = maintenance_namespace_manager(&world);

    world.begin_measurement().await;
    let discovered = manager
        .list(Some(&world.harness.prefix))
        .await
        .expect("ideal empty discovery tick failed");
    assert!(discovered.is_empty());
    world.finish(case).await
}

async fn execute_discovery_active(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let namespace = world.managed_namespace(case.id.as_str());
    let manager = maintenance_namespace_manager(&world);
    manager
        .create(&namespace, 4, DistanceMetric::Euclidean)
        .await
        .expect("ideal active discovery namespace setup failed");

    world.begin_measurement().await;
    let discovered = manager
        .list(Some(&world.harness.prefix))
        .await
        .expect("ideal active discovery tick failed");
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].name, namespace);
    world.finish(case).await
}

async fn execute_cached_idle(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let namespace = world.managed_namespace(case.id.as_str());
    let manager = maintenance_namespace_manager(&world);
    let indexing = maintenance_indexing_config();
    manager
        .create_with_fts_and_index_config(
            &namespace,
            4,
            DistanceMetric::Euclidean,
            HashMap::new(),
            Some(NamespaceIndexConfig::from_indexing_config(&indexing)),
        )
        .await
        .expect("ideal cached-idle namespace setup failed");
    let compactor = maintenance_compactor(&world, CompactionConfig::default());

    world.begin_measurement().await;
    let cached = manager.cached_namespaces(Some(&world.harness.prefix));
    assert_eq!(cached.len(), 1);
    assert_eq!(cached[0].name, namespace);
    assert!(!compactor
        .should_compact(&cached[0].name)
        .await
        .expect("ideal cached-idle trigger check failed"));
    world.finish(case).await
}

async fn execute_resume_delete(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let namespace = world.managed_namespace(case.id.as_str());
    let manager = maintenance_namespace_manager(&world);
    manager
        .create(&namespace, 4, DistanceMetric::Euclidean)
        .await
        .expect("ideal resumed-delete namespace setup failed");
    world
        .store
        .put(
            &format!("{namespace}/wal/pending.wal"),
            Bytes::from_static(b"pending"),
        )
        .await
        .expect("ideal resumed-delete object setup failed");
    manager
        .start_delete(&namespace)
        .await
        .expect("ideal resumed-delete tombstone setup failed");

    world.begin_measurement().await;
    let outcome = manager
        .finish_delete(&namespace, Duration::from_secs(25))
        .await
        .expect("ideal resumed-delete tick failed");
    assert!(outcome.complete);
    assert_eq!(outcome.deleted, 2);
    let sample = world.snapshot(case).await;
    assert_eq!(sample.total_get_ops, 1);
    assert_eq!(physical_mode_ops(&sample, "list_recursive"), 2);
    assert_eq!(physical_mode_ops(&sample, "delete_batch"), 1);
    assert_eq!(physical_mode_ops(&sample, "delete"), 1);
    assert_eq!(
        sample
            .physical_operations
            .iter()
            .filter(|operation| operation.request == PhysicalRequest::DeleteBatch)
            .count(),
        1
    );
    world.cleanup(sample).await
}

async fn execute_tick_lease_held(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let namespace = world.managed_namespace(case.id.as_str());
    let manager = maintenance_namespace_manager(&world);
    let indexing = maintenance_indexing_config();
    manager
        .create_with_fts_and_index_config(
            &namespace,
            4,
            DistanceMetric::Euclidean,
            HashMap::new(),
            Some(NamespaceIndexConfig::from_indexing_config(&indexing)),
        )
        .await
        .expect("ideal held-lease namespace setup failed");
    WalWriter::with_clock(world.store.clone(), world.clock())
        .append(&namespace, maintenance_vectors("held", 16), Vec::new())
        .await
        .expect("ideal held-lease WAL setup failed");
    let holder = maintenance_lease_manager(&world, "ideal-holder");
    holder
        .acquire(&namespace)
        .await
        .expect("ideal held-lease setup acquisition failed");
    let contender = maintenance_lease_manager(&world, "ideal-contender");
    let compactor = maintenance_compactor(&world, trigger_each_fragment());

    world.begin_measurement().await;
    assert!(compactor
        .should_compact(&namespace)
        .await
        .expect("ideal held-lease trigger check failed"));
    let result =
        compact_namespace_under_lease(&compactor, &contender, &namespace, &HashMap::new()).await;
    assert!(matches!(result, Err(ZeppelinError::LeaseHeld { .. })));
    world.finish(case).await
}

async fn execute_tick_compaction_success(case: &IdealCase) -> IdealSample {
    let mut world = MaintenanceWorld::new().await;
    let namespace = world.managed_namespace(case.id.as_str());
    let manager = maintenance_namespace_manager(&world);
    let indexing = maintenance_indexing_config();
    manager
        .create_with_fts_and_index_config(
            &namespace,
            4,
            DistanceMetric::Euclidean,
            HashMap::new(),
            Some(NamespaceIndexConfig::from_indexing_config(&indexing)),
        )
        .await
        .expect("ideal successful-tick namespace setup failed");
    WalWriter::with_clock(world.store.clone(), world.clock())
        .append(&namespace, maintenance_vectors("tick", 16), Vec::new())
        .await
        .expect("ideal successful-tick WAL setup failed");
    let compactor = maintenance_compactor(&world, trigger_each_fragment());
    let lease_manager = maintenance_lease_manager(&world, "ideal-tick");

    world.begin_measurement().await;
    assert!(compactor
        .should_compact(&namespace)
        .await
        .expect("ideal successful-tick trigger check failed"));
    let result =
        compact_namespace_under_lease(&compactor, &lease_manager, &namespace, &HashMap::new())
            .await
            .expect("ideal successful-tick compaction failed");
    assert_eq!(result.vectors_compacted, 16);
    assert_eq!(result.fragments_removed, 1);
    assert!(
        result.segment_id.is_some(),
        "awaited compaction core must report a published segment"
    );
    manager
        .record_compaction_success(&namespace)
        .await
        .expect("ideal successful-tick health publication failed");
    // Production's subsequent cache warm is intentionally not called: it is
    // detached and exposes no completion handle that can bound this interval.
    world.finish(case).await
}

fn maintenance_namespace_manager(world: &MaintenanceWorld) -> NamespaceManager {
    NamespaceManager::with_clock(
        world.store.clone(),
        Duration::from_secs(3_600),
        world.clock(),
    )
}

async fn create_active_compaction_namespace(
    world: &mut MaintenanceWorld,
    suffix: &str,
    indexing: &IndexingConfig,
    fts_configs: HashMap<String, FtsFieldConfig>,
) -> String {
    let namespace = world.managed_namespace(suffix);
    maintenance_namespace_manager(world)
        .create_with_fts_and_index_config(
            &namespace,
            4,
            DistanceMetric::Euclidean,
            fts_configs,
            Some(NamespaceIndexConfig::from_indexing_config(indexing)),
        )
        .await
        .expect("ideal active compaction namespace setup failed");
    namespace
}

fn maintenance_lease_manager(world: &MaintenanceWorld, holder: &str) -> Arc<LeaseManager> {
    Arc::new(LeaseManager::with_clock(
        world.store.clone(),
        holder.to_string(),
        Duration::from_secs(3_600),
        world.clock(),
    ))
}

fn maintenance_compactor(world: &MaintenanceWorld, config: CompactionConfig) -> Compactor {
    Compactor::with_clock(
        world.store.clone(),
        WalReader::new(world.store.clone()),
        config,
        maintenance_indexing_config(),
        Duration::from_secs(300),
        world.clock(),
    )
}

fn maintenance_fts_compactor(world: &MaintenanceWorld) -> Compactor {
    let mut indexing = maintenance_indexing_config();
    indexing.fts_index = true;
    indexing.bitmap_index = false;
    Compactor::with_clock(
        world.store.clone(),
        WalReader::new(world.store.clone()),
        CompactionConfig::default(),
        indexing,
        Duration::from_secs(300),
        world.clock(),
    )
}

fn maintenance_indexing_config() -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: 2,
        kmeans_max_iterations: 10,
        ..Default::default()
    }
}

fn trigger_each_fragment() -> CompactionConfig {
    CompactionConfig {
        max_wal_fragments_before_compact: 1,
        ..Default::default()
    }
}

fn maintenance_vectors(prefix: &str, count: usize) -> Vec<VectorEntry> {
    (0..count)
        .map(|index| VectorEntry {
            id: format!("{prefix}-{index}"),
            values: vec![index as f32, (index % 3) as f32, (index % 5) as f32, 1.0],
            attributes: None,
        })
        .collect()
}

fn maintenance_nearby_vectors(
    prefix: &str,
    anchor: &VectorEntry,
    count: usize,
) -> Vec<VectorEntry> {
    (0..count)
        .map(|index| VectorEntry {
            id: format!("{prefix}-{index}"),
            values: anchor
                .values
                .iter()
                .map(|value| value + (index + 1) as f32 * 0.000_01)
                .collect(),
            attributes: None,
        })
        .collect()
}

fn maintenance_fts_vectors(count: usize) -> Vec<VectorEntry> {
    maintenance_vectors("fts", count)
        .into_iter()
        .enumerate()
        .map(|(index, mut vector)| {
            vector.attributes = Some(HashMap::from([(
                "content".to_string(),
                AttributeValue::String(format!(
                    "rust storage document {index} deterministic compaction"
                )),
            )]));
            vector
        })
        .collect()
}

async fn execute_gc(case: &IdealCase, operation: GarbageCollectionCase) -> Option<IdealSample> {
    match operation {
        GarbageCollectionCase::IdleWarmSecondCycle => {
            Some(execute_idle_warm_second_cycle(case).await)
        }
        GarbageCollectionCase::IdleNewOrphan => Some(execute_idle_new_orphan(case).await),
        GarbageCollectionCase::IdleCandidateMaturity => {
            Some(execute_idle_candidate_maturity(case).await)
        }
        GarbageCollectionCase::IdlePendingDeleteMaturity => {
            Some(execute_idle_pending_delete_maturity(case).await)
        }
        GarbageCollectionCase::IdlePitrExpiry => Some(execute_idle_pitr_expiry(case).await),
        GarbageCollectionCase::IdleStagingLeaseExpiry => {
            Some(execute_idle_staging_lease_expiry(case).await)
        }
        GarbageCollectionCase::IdleChangedSnapshot => {
            Some(execute_idle_changed_control(case, IdleControlMutation::Snapshot).await)
        }
        GarbageCollectionCase::ParallelSnapshotPins => {
            Some(execute_idle_changed_control(case, IdleControlMutation::SnapshotPair).await)
        }
        GarbageCollectionCase::IdleChangedStaging => {
            Some(execute_idle_changed_control(case, IdleControlMutation::Staging).await)
        }
        GarbageCollectionCase::IdleChangedCandidateLedger => {
            Some(execute_idle_changed_control(case, IdleControlMutation::CandidateLedger).await)
        }
        GarbageCollectionCase::IdleBackwardClock => Some(execute_idle_backward_clock(case).await),
        GarbageCollectionCase::IdleShorterRetentionConfig => {
            Some(execute_idle_shorter_retention_config(case).await)
        }
        GarbageCollectionCase::IdlePriorPartialFailure => {
            Some(execute_idle_prior_partial_failure(case).await)
        }
        GarbageCollectionCase::PruneReuseEmptyPendingUncacheable => {
            Some(execute_prune_reuse_empty_pending_uncacheable(case).await)
        }
        GarbageCollectionCase::PruneReuseEligiblePendingRefresh => {
            Some(execute_prune_reuse_eligible_pending_refresh(case).await)
        }
        GarbageCollectionCase::HistoryMemoNewGeneration => {
            Some(execute_history_memo_new_generation(case).await)
        }
        GarbageCollectionCase::HistoryMemoChangedEtag => {
            Some(execute_history_memo_changed_etag(case).await)
        }
        GarbageCollectionCase::HistoryMemoMissingEtag => {
            Some(execute_history_memo_missing_etag(case).await)
        }
        GarbageCollectionCase::HistoryMemoDisappearsBetweenListAndGet => {
            Some(execute_history_memo_disappears_between_list_and_get(case).await)
        }
        GarbageCollectionCase::HistoryMemoUnpublishedOrphanOverwrite => {
            Some(execute_history_memo_unpublished_orphan_overwrite(case).await)
        }
        GarbageCollectionCase::HistoryMemoCorruptChangedBody => {
            Some(execute_history_memo_corrupt_changed_body(case).await)
        }
        GarbageCollectionCase::HistoryMemoNamespaceRecreated => {
            Some(execute_history_memo_namespace_recreated(case).await)
        }
        GarbageCollectionCase::HistoryMemoColdRunnerRestart => {
            Some(execute_history_memo_cold_runner_restart(case).await)
        }
        GarbageCollectionCase::PendingDeleteYoung => {
            Some(execute_pending_delete(case, PendingShape::Young).await)
        }
        GarbageCollectionCase::PendingDeleteHistoryPinned => {
            Some(execute_pending_delete(case, PendingShape::HistoryPinned).await)
        }
        GarbageCollectionCase::PendingDeleteEligible => {
            Some(execute_pending_delete(case, PendingShape::Eligible).await)
        }
        GarbageCollectionCase::OrphanMark => Some(execute_orphan_mark(case).await),
        GarbageCollectionCase::OrphanSweep => Some(execute_orphan_sweep(case).await),
        GarbageCollectionCase::ManifestHistoryPrune => Some(execute_history_prune(case).await),
        GarbageCollectionCase::StagingWrite => Some(execute_staging_write(case).await),
        GarbageCollectionCase::StagingClear => Some(execute_staging_clear(case).await),
        GarbageCollectionCase::ActiveStagingMissingLease => {
            Some(execute_active_staging(case, ActiveStagingShape::MissingLease).await)
        }
        GarbageCollectionCase::ActiveStagingExpiredLease => {
            Some(execute_active_staging(case, ActiveStagingShape::ExpiredLease).await)
        }
        GarbageCollectionCase::ActiveStagingMatchingToken => {
            Some(execute_active_staging(case, ActiveStagingShape::MatchingToken).await)
        }
        GarbageCollectionCase::ActiveStagingMixedTokens => {
            Some(execute_active_staging(case, ActiveStagingShape::MixedTokens).await)
        }
    }
}

async fn execute_idle_warm_second_cycle(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let incarnation = GcNamespaceIncarnation::new(namespace, world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal warm idle GC cycle failed");
    let sample = world.finish(case).await;
    assert_idle_gate_census(&sample);
    assert_eq!(report, GcCycleReport::default());
    sample
}

fn assert_idle_gate_census(sample: &IdealSample) {
    assert_history_memo_census(sample, 0, 0, 1, 0, 1, 0);
    assert_eq!(physical_verb_ops(sample, "delete"), 0);
}

async fn execute_idle_new_orphan(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    let orphan = idle_wal_key(
        &namespace,
        world.now - ChronoDuration::seconds(IDLE_DEADLINE_SECS),
        101,
    );
    world
        .store
        .put(&orphan, Bytes::from_static(b"idle-new-orphan"))
        .await
        .expect("ideal idle new-orphan setup failed");

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal idle new-orphan cycle failed");
    let sample = world.snapshot(case).await;
    assert_full_gc_census(&sample, 0, 6, 2, 2, 1, 3);
    assert_eq!(report.candidates_marked, 1);
    assert_eq!(report.objects_deleted, 1);
    assert!(
        !world
            .harness
            .store
            .exists(&orphan)
            .await
            .expect("ideal idle new-orphan absence oracle failed"),
        "a post-prime orphan must invalidate the idle gate and be collected"
    );
    world.cleanup(sample).await
}

async fn execute_idle_candidate_maturity(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    let manifest = seed_history_generations(&world, &namespace, 18).await;
    let candidate = idle_wal_key(
        &namespace,
        world.now - ChronoDuration::seconds(IDLE_DEADLINE_SECS * 2),
        102,
    );
    world
        .store
        .put(&candidate, Bytes::from_static(b"idle-maturing-candidate"))
        .await
        .expect("ideal idle candidate setup failed");
    save_gc_candidates(
        &world.store,
        &namespace,
        &[GcCandidate {
            key: candidate.clone(),
            first_seen_unreachable_at: world.now,
            unreachable_since_manifest_version: manifest.version(),
        }],
    )
    .await
    .expect("ideal idle candidate ledger setup failed");

    let incarnation = GcNamespaceIncarnation::new(namespace, world.now);
    let mut runner = GcRunner::new(world.store.clone(), idle_deadline_gc_config());
    let prime = runner
        .run_cycle_at(incarnation.clone(), world.now)
        .await
        .expect("ideal idle candidate prime failed");
    assert_eq!(prime.objects_deleted, 0);
    assert!(prime.candidates_skipped >= 1);

    let mature_at = world.now + ChronoDuration::seconds(IDLE_DEADLINE_SECS);
    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, mature_at)
        .await
        .expect("ideal idle candidate maturity cycle failed");
    let sample = world.snapshot(case).await;
    assert_full_gc_census(&sample, 0, 6, 2, 2, 1, 3);
    assert_eq!(report.candidates_marked, 0);
    assert_eq!(report.objects_deleted, 1);
    assert!(
        !world
            .harness
            .store
            .exists(&candidate)
            .await
            .expect("ideal idle candidate absence oracle failed"),
        "a candidate deadline must invalidate the idle gate"
    );
    world.cleanup(sample).await
}

async fn execute_idle_pending_delete_maturity(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    let pending = idle_wal_key(&namespace, world.now, 103);
    world
        .store
        .put(
            &pending,
            Bytes::from_static(b"idle-maturing-pending-delete"),
        )
        .await
        .expect("ideal idle pending-delete object setup failed");
    let mut manifest = persist_empty_manifest(&world, &namespace).await;
    manifest.pending_deletes.push(pending.clone());
    world
        .store
        .put(
            &Manifest::s3_key(&namespace),
            manifest
                .to_bytes()
                .expect("ideal idle pending-delete manifest encode failed"),
        )
        .await
        .expect("ideal idle pending-delete live manifest setup failed");

    let incarnation = GcNamespaceIncarnation::new(namespace, world.now);
    let mut runner = GcRunner::new(world.store.clone(), idle_deadline_gc_config());
    let prime = runner
        .run_cycle_at(incarnation.clone(), world.now)
        .await
        .expect("ideal idle pending-delete prime failed");
    assert_eq!(prime.pending_deletes_deleted, 0);
    assert_eq!(prime.pending_deletes_retained, 1);

    let mature_at = world.now + ChronoDuration::seconds(IDLE_DEADLINE_SECS);
    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, mature_at)
        .await
        .expect("ideal idle pending-delete maturity cycle failed");
    let sample = world.snapshot(case).await;
    assert_full_gc_census(&sample, 0, 2, 2, 2, 1, 2);
    assert_eq!(report.pending_deletes_deleted, 1);
    assert_eq!(report.pending_deletes_pruned, 1);
    assert_eq!(report.pending_deletes_retained, 0);
    assert!(
        !world
            .harness
            .store
            .exists(&pending)
            .await
            .expect("ideal idle pending-delete absence oracle failed"),
        "a pending-delete age deadline must invalidate the idle gate"
    );
    world.cleanup(sample).await
}

async fn execute_idle_pitr_expiry(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut gc = history_memo_gc_config();
    gc.manifest_history_keep_count = 1;
    gc.pitr_retention_secs = IDLE_DEADLINE_SECS as u64;
    let mut runner = GcRunner::new(world.store.clone(), gc);
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    let expired_at = world.now + ChronoDuration::seconds(IDLE_DEADLINE_SECS + 1);
    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, expired_at)
        .await
        .expect("ideal idle PITR-expiry cycle failed");
    assert_eq!(report.objects_deleted, 0);
    let sample = world.snapshot(case).await;
    assert_full_gc_census(&sample, 0, 6, 1, 2, 17, 3);
    let histories = Manifest::list_history(&world.harness.store, &namespace)
        .await
        .expect("ideal idle PITR-expiry history oracle failed");
    assert_eq!(histories.len(), 1);
    world.cleanup(sample).await
}

async fn execute_idle_staging_lease_expiry(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    persist_empty_manifest(&world, &namespace).await;
    let staged_id = Ulid::from_parts(
        (world.now - ChronoDuration::seconds(IDLE_DEADLINE_SECS * 2)).timestamp_millis() as u64,
        104,
    );
    let staged = format!("{namespace}/segments/seg_{staged_id}/bootstrap.bin");
    world
        .store
        .put(&staged, Bytes::from_static(b"idle-staged-upload"))
        .await
        .expect("ideal idle staged artifact setup failed");
    let lease_manager = LeaseManager::with_clock(
        world.store.clone(),
        "ideal-idle-staging-holder".to_string(),
        Duration::from_secs(IDLE_DEADLINE_SECS as u64),
        world.clock(),
    );
    let lease = lease_manager
        .acquire(&namespace)
        .await
        .expect("ideal idle staging lease setup failed");
    write_compaction_staging(
        &world.store,
        &namespace,
        lease.fencing_token,
        BTreeSet::from([staged.clone()]),
    )
    .await
    .expect("ideal idle staging record setup failed");

    let incarnation = GcNamespaceIncarnation::new(namespace, world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    let prime = runner
        .run_cycle_at(incarnation.clone(), world.now)
        .await
        .expect("ideal idle active-staging prime failed");
    assert_eq!(prime.objects_deleted, 0);

    let expired_at = world.now + ChronoDuration::seconds(IDLE_DEADLINE_SECS);
    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, expired_at)
        .await
        .expect("ideal idle staging lease-expiry cycle failed");
    let sample = world.snapshot(case).await;
    assert_full_gc_census(&sample, 0, 6, 2, 2, 1, 3);
    assert_eq!(report.candidates_marked, 1);
    assert_eq!(report.objects_deleted, 1);
    assert!(
        !world
            .harness
            .store
            .exists(&staged)
            .await
            .expect("ideal idle staged artifact absence oracle failed"),
        "lease expiry must invalidate the idle gate and release staging protection"
    );
    world.cleanup(sample).await
}

#[derive(Clone, Copy)]
enum IdleControlMutation {
    Snapshot,
    SnapshotPair,
    Staging,
    CandidateLedger,
}

async fn execute_idle_changed_control(
    case: &IdealCase,
    mutation: IdleControlMutation,
) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    let manifest = seed_history_generations(&world, &namespace, 18).await;
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    match mutation {
        IdleControlMutation::Snapshot | IdleControlMutation::SnapshotPair => {
            let names: &[&str] = match mutation {
                IdleControlMutation::Snapshot => &["idle-control-change"],
                IdleControlMutation::SnapshotPair => {
                    &["idle-control-change-a", "idle-control-change-b"]
                }
                IdleControlMutation::Staging | IdleControlMutation::CandidateLedger => {
                    unreachable!("snapshot branch must have snapshot mutation")
                }
            };
            for name in names {
                NamedSnapshot::create_at(
                    &world.store,
                    &namespace,
                    name,
                    manifest.version(),
                    world.now,
                )
                .await
                .expect("ideal idle changed-snapshot setup failed");
            }
        }
        IdleControlMutation::Staging => {
            write_compaction_staging(&world.store, &namespace, 777, BTreeSet::new())
                .await
                .expect("ideal idle changed-staging setup failed");
        }
        IdleControlMutation::CandidateLedger => {
            save_gc_candidates(
                &world.store,
                &namespace,
                &[GcCandidate {
                    key: idle_wal_key(
                        &namespace,
                        world.now - ChronoDuration::seconds(IDLE_DEADLINE_SECS * 2),
                        105,
                    ),
                    first_seen_unreachable_at: world.now,
                    unreachable_since_manifest_version: manifest.version(),
                }],
            )
            .await
            .expect("ideal idle changed-candidate-ledger setup failed");
        }
    }

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal idle changed-control cycle failed");
    assert_eq!(report.objects_deleted, 0);
    assert_eq!(report.pending_deletes_deleted, 0);
    let sample = world.snapshot(case).await;
    match mutation {
        IdleControlMutation::Snapshot | IdleControlMutation::SnapshotPair => {
            let (names, expected_gets, expected_bytes): (&[&str], u64, u64) = match mutation {
                IdleControlMutation::Snapshot => (&["idle-control-change"], 7, 375),
                IdleControlMutation::SnapshotPair => {
                    (&["idle-control-change-a", "idle-control-change-b"], 8, 406)
                }
                IdleControlMutation::Staging | IdleControlMutation::CandidateLedger => {
                    unreachable!("snapshot branch must have snapshot mutation")
                }
            };
            assert_full_gc_census(&sample, 0, expected_gets, 1, 2, 0, 4);
            assert_eq!(sample.total_get_bytes, expected_bytes);
            for name in names {
                assert!(NamedSnapshot::read(&world.harness.store, &namespace, name)
                    .await
                    .expect("ideal idle changed-snapshot oracle failed")
                    .is_some());
            }
        }
        IdleControlMutation::Staging => {
            assert_full_gc_census(&sample, 0, 6, 1, 2, 0, 3);
        }
        IdleControlMutation::CandidateLedger => {
            assert_full_gc_census(&sample, 0, 6, 1, 2, 0, 3);
            assert!(
                load_gc_candidates(&world.harness.store, &namespace)
                    .await
                    .expect("ideal idle changed-candidate-ledger oracle failed")
                    .is_empty(),
                "a changed candidate ledger must be revalidated instead of idled"
            );
        }
    }
    world.cleanup(sample).await
}

async fn execute_idle_backward_clock(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let incarnation = GcNamespaceIncarnation::new(namespace, world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now - ChronoDuration::microseconds(1))
        .await
        .expect("ideal idle backward-clock cycle failed");
    assert_eq!(report.objects_deleted, 0);
    let sample = world.finish(case).await;
    assert_full_gc_census(&sample, 0, 6, 1, 2, 0, 3);
    sample
}

async fn execute_idle_shorter_retention_config(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    let mut shorter = history_memo_gc_config();
    shorter.manifest_history_keep_count = 1;
    runner.update_config(shorter);

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal idle shorter-retention cycle failed");
    assert_eq!(report.objects_deleted, 0);
    let sample = world.snapshot(case).await;
    assert_full_gc_census(&sample, 0, 6, 1, 2, 17, 3);
    let histories = Manifest::list_history(&world.harness.store, &namespace)
        .await
        .expect("ideal idle shorter-retention history oracle failed");
    assert_eq!(histories.len(), 1);
    world.cleanup(sample).await
}

async fn execute_idle_prior_partial_failure(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    let target = Manifest::history_key(&namespace, 1);
    let original = world
        .store
        .get(&target)
        .await
        .expect("ideal idle partial-failure history read failed");
    world
        .store
        .put(&target, Bytes::from_static(b"corrupt-history"))
        .await
        .expect("ideal idle partial-failure corruption setup failed");
    let partial = runner
        .run_cycle_at(incarnation.clone(), world.now)
        .await
        .expect("ideal idle partial-failure cycle returned an error");
    assert_eq!(partial, GcCycleReport::default());
    world
        .store
        .put(&target, original)
        .await
        .expect("ideal idle partial-failure history restore failed");

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal idle post-failure retry failed");
    assert_eq!(report.objects_deleted, 0);
    let sample = world.finish(case).await;
    assert_full_gc_census(&sample, 0, 6, 1, 2, 0, 3);
    sample
}

const IDLE_DEADLINE_SECS: i64 = 30;

fn idle_deadline_gc_config() -> GcConfig {
    GcConfig {
        horizon_secs: IDLE_DEADLINE_SECS as u64,
        ..history_memo_gc_config()
    }
}

fn idle_wal_key(namespace: &str, created_at: DateTime<Utc>, entropy: u128) -> String {
    let id = Ulid::from_parts(created_at.timestamp_millis() as u64, entropy);
    format!("{namespace}/wal/{id}.wal")
}

async fn execute_prune_reuse_empty_pending_uncacheable(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace("gc.prune_reuse_history_fixture_xxxxxxxxx");
    seed_gc_cycle_sized_history(&world, &namespace).await;
    let (fault_store, control) = history_observation_fault_store(
        &world,
        Manifest::history_prefix(&namespace),
        HistoryObservationFault::MissingHistoryVersions,
    );
    control.arm();
    let incarnation = GcNamespaceIncarnation::new(namespace, world.now);
    let mut runner = GcRunner::new(fault_store, history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal GC prune-result reuse cycle failed");
    let sample = world.finish(case).await;
    assert_eq!(report.objects_deleted, 0);
    assert_eq!(report.pending_deletes_deleted, 0);
    assert_eq!(report.pending_deletes_pruned, 0);
    assert_eq!(report.pending_deletes_retained, 0);
    assert_prune_reuse_empty_pending_census(&sample);
    sample
}

fn assert_prune_reuse_empty_pending_census(sample: &IdealSample) {
    let history_gets = sample
        .physical_operations
        .iter()
        .filter(|operation| operation.verb == "get" && operation.key == "<generation>.msgpack")
        .count() as u64;
    let history_get_bytes = sample
        .physical_operations
        .iter()
        .filter(|operation| operation.verb == "get" && operation.key == "<generation>.msgpack")
        .map(|operation| operation.successful_bytes)
        .sum::<u64>();
    let calls = sample
        .physical_verb_mode_totals
        .iter()
        .map(|total| total.ops)
        .sum::<u64>();
    assert_eq!(
        (
            history_gets,
            history_get_bytes,
            sample.total_get_ops,
            sample.total_get_bytes,
            physical_mode_ops(sample, "list_recursive"),
            physical_verb_ops(sample, "put"),
            calls,
            sample.serial_get_chain.depth,
        ),
        (36, 18_278, 42, 18_673, 1, 2, 45, 5),
        "warm GC must preserve prune-result reuse traffic while overlapping independent retention, mark, and sweep reads",
    );
}

async fn execute_prune_reuse_eligible_pending_refresh(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace("gc.prune_reuse_history_fixture_xxxxxxxxx");
    let mut manifest = seed_gc_cycle_sized_history(&world, &namespace).await;
    let (fault_store, control) = history_observation_fault_store(
        &world,
        Manifest::history_prefix(&namespace),
        HistoryObservationFault::MissingHistoryVersions,
    );
    control.arm();
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(fault_store, history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    let pending = idle_wal_key(&namespace, world.now - ChronoDuration::seconds(60), 106);
    world
        .store
        .put(
            &pending,
            Bytes::from_static(b"prune-reuse-eligible-pending"),
        )
        .await
        .expect("ideal prune-reuse pending object setup failed");
    manifest.pending_deletes.push(pending.clone());
    world
        .store
        .put(
            &Manifest::s3_key(&namespace),
            manifest
                .to_bytes()
                .expect("ideal prune-reuse pending manifest encode failed"),
        )
        .await
        .expect("ideal prune-reuse pending manifest setup failed");

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal prune-reuse eligible-pending cycle failed");
    let sample = world.snapshot(case).await;
    let history_gets = sample
        .physical_operations
        .iter()
        .filter(|operation| operation.verb == "get" && operation.key == "<generation>.msgpack")
        .count() as u64;
    assert_eq!(
        (
            history_gets,
            sample.total_get_ops,
            sample.total_get_bytes,
            physical_mode_ops(&sample, "list_recursive"),
            physical_verb_ops(&sample, "put"),
            physical_verb_ops(&sample, "delete"),
            sample.serial_get_chain.depth,
        ),
        (18, 19, 9_377, 2, 0, 0, 2),
        "an eligible pending delete must fail closed when the fresh full inventory lacks history ETags",
    );
    assert_eq!(report.pending_deletes_deleted, 0);
    assert_eq!(report.pending_deletes_pruned, 0);
    assert!(world
        .harness
        .store
        .exists(&pending)
        .await
        .expect("ideal prune-reuse pending preservation oracle failed"));
    world.cleanup(sample).await
}

async fn execute_history_memo_new_generation(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    let mut manifest = seed_history_generations(&world, &namespace, 18).await;
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    manifest.updated_at = world.now + ChronoDuration::seconds(1);
    manifest
        .write(&world.store, &namespace)
        .await
        .expect("ideal GC new-history generation setup write failed");

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal GC new-history generation cycle failed");
    assert_eq!(report.objects_deleted, 0);
    assert_eq!(report.pending_deletes_deleted, 0);

    let sample = world.finish(case).await;
    assert_history_memo_census(&sample, 2, 8, 1, 2, 11, 5);
    assert_eq!(sample.total_get_bytes, 583);
    sample
}

async fn execute_history_memo_changed_etag(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let target = Manifest::history_key(&namespace, 1);
    let mut changed = Manifest::read_history(&world.store, &namespace, 1)
        .await
        .expect("ideal GC changed-ETag history setup read failed")
        .expect("ideal GC changed-ETag history setup object missing");
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    changed.updated_at = world.now + ChronoDuration::seconds(1);
    world
        .store
        .put(
            &target,
            changed
                .to_bytes()
                .expect("ideal GC changed-ETag history serialization failed"),
        )
        .await
        .expect("ideal GC changed-ETag history overwrite failed");

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal GC changed-ETag cycle failed");
    assert_eq!(report.objects_deleted, 0);
    assert_eq!(report.pending_deletes_deleted, 0);
    let sample = world.finish(case).await;
    assert_history_memo_census(&sample, 2, 8, 1, 2, 11, 5);
    sample
}

async fn execute_history_memo_missing_etag(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let target = Manifest::history_key(&namespace, 1);
    let (fault_store, control) =
        history_observation_fault_store(&world, target, HistoryObservationFault::MissingListEtag);
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(fault_store, history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    control.arm();
    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal GC missing-ETag cycle failed");
    assert_eq!(report.objects_deleted, 0);
    assert_eq!(report.pending_deletes_deleted, 0);
    let sample = world.finish(case).await;
    assert_history_memo_census(&sample, 2, 8, 1, 2, 11, 5);
    sample
}

async fn execute_history_memo_disappears_between_list_and_get(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let target = Manifest::history_key(&namespace, 1);
    let (fault_store, control) = history_observation_fault_store(
        &world,
        target,
        HistoryObservationFault::DeleteBeforeFirstGet,
    );
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(fault_store, history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    control.arm();
    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal GC disappearing-history cycle returned a storage error");
    let sample = world.finish(case).await;
    assert_history_failure_closed(&report, &sample);
    sample
}

async fn execute_history_memo_unpublished_orphan_overwrite(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    let mut orphan = seed_history_generations(&world, &namespace, 19).await;
    let authoritative = Manifest::read_history(&world.store, &namespace, 18)
        .await
        .expect("ideal GC orphan history authoritative setup read failed")
        .expect("ideal GC orphan history authoritative generation missing");
    world
        .store
        .put(
            &Manifest::s3_key(&namespace),
            authoritative
                .to_bytes()
                .expect("ideal GC orphan authoritative manifest serialization failed"),
        )
        .await
        .expect("ideal GC orphan authoritative manifest rewind failed");

    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;

    let protected_id = Ulid::from_parts(
        (world.now - ChronoDuration::seconds(60)).timestamp_millis() as u64,
        0xA11CE,
    );
    let protected_key = WalFragment::s3_key(&namespace, &protected_id);
    world
        .store
        .put(&protected_key, Bytes::from_static(b"orphan-history-root"))
        .await
        .expect("ideal GC orphan protected artifact setup failed");
    save_gc_candidates(
        &world.store,
        &namespace,
        &[GcCandidate {
            key: protected_key.clone(),
            first_seen_unreachable_at: world.now - ChronoDuration::seconds(60),
            unreachable_since_manifest_version: authoritative.version(),
        }],
    )
    .await
    .expect("ideal GC orphan candidate setup failed");
    orphan.add_fragment(FragmentRef {
        id: protected_id,
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 19,
    });
    world
        .store
        .put(
            &Manifest::history_key(&namespace, 19),
            orphan
                .to_bytes()
                .expect("ideal GC orphan overwrite serialization failed"),
        )
        .await
        .expect("ideal GC orphan history overwrite failed");

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal GC orphan overwrite cycle failed");
    let sample = world.snapshot(case).await;
    assert_eq!(report.objects_deleted, 0);
    assert_eq!(report.pending_deletes_deleted, 0);
    assert_history_memo_census(&sample, 2, 8, 1, 2, 11, 5);
    assert!(
        world
            .harness
            .store
            .exists(&protected_key)
            .await
            .expect("ideal GC orphan protected artifact oracle failed"),
        "changed unpublished history body must remain an authoritative GC root"
    );
    world.cleanup(sample).await
}

async fn execute_history_memo_corrupt_changed_body(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut runner, &incarnation, world.now).await;
    world
        .store
        .put(
            &Manifest::history_key(&namespace, 1),
            Bytes::from_static(b"corrupt changed history body"),
        )
        .await
        .expect("ideal GC corrupt history setup overwrite failed");

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal GC corrupt-history cycle returned a storage error");
    let sample = world.finish(case).await;
    assert_history_failure_closed(&report, &sample);
    sample
}

async fn execute_history_memo_namespace_recreated(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let original = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut runner, &original, world.now).await;

    world
        .store
        .delete_prefix(&format!("{namespace}/"))
        .await
        .expect("ideal GC recreated namespace old incarnation cleanup failed");
    seed_history_generations(&world, &namespace, 18).await;
    let recreated =
        GcNamespaceIncarnation::new(namespace, world.now + ChronoDuration::microseconds(1));

    world.begin_measurement().await;
    let report = runner
        .run_cycle_at(recreated, world.now)
        .await
        .expect("ideal GC recreated namespace cycle failed");
    assert_eq!(report.objects_deleted, 0);
    assert_eq!(report.pending_deletes_deleted, 0);
    let sample = world.finish(case).await;
    assert_history_memo_census(&sample, 54, 60, 5, 2, 67, 60);
    sample
}

async fn execute_history_memo_cold_runner_restart(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    seed_history_generations(&world, &namespace, 18).await;
    let incarnation = GcNamespaceIncarnation::new(namespace.clone(), world.now);
    let mut first_runner = GcRunner::new(world.store.clone(), history_memo_gc_config());
    prime_history_memo(&mut first_runner, &incarnation, world.now).await;
    drop(first_runner);
    let mut restarted = GcRunner::new(world.store.clone(), history_memo_gc_config());

    world.begin_measurement().await;
    let report = restarted
        .run_cycle_at(incarnation, world.now)
        .await
        .expect("ideal GC cold runner restart cycle failed");
    assert_eq!(report.objects_deleted, 0);
    assert_eq!(report.pending_deletes_deleted, 0);
    let sample = world.finish(case).await;
    assert_history_memo_census(&sample, 54, 60, 5, 2, 67, 60);
    sample
}

fn assert_history_failure_closed(report: &GcCycleReport, sample: &IdealSample) {
    assert_eq!(report, &GcCycleReport::default());
    assert_history_memo_census(sample, 1, 1, 1, 0, 2, 1);
    assert_eq!(physical_verb_ops(sample, "delete"), 0);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryObservationFault {
    MissingListEtag,
    MissingHistoryVersions,
    DeleteBeforeFirstGet,
}

#[derive(Debug, Default)]
struct HistoryObservationFaultControl {
    armed: AtomicBool,
    fired: AtomicBool,
}

impl HistoryObservationFaultControl {
    fn arm(&self) {
        self.fired.store(false, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct HistoryObservationFaultStore {
    inner: Arc<dyn ObjectStore>,
    mutation_inner: Arc<dyn ObjectStore>,
    target: String,
    fault: HistoryObservationFault,
    control: Arc<HistoryObservationFaultControl>,
}

fn history_observation_fault_store(
    world: &MaintenanceWorld,
    target: String,
    fault: HistoryObservationFault,
) -> (ZeppelinStore, Arc<HistoryObservationFaultControl>) {
    let control = Arc::new(HistoryObservationFaultControl::default());
    let store = HistoryObservationFaultStore {
        inner: world.store.inner(),
        mutation_inner: world.harness.store.inner(),
        target,
        fault,
        control: Arc::clone(&control),
    };
    (
        ZeppelinStore::new_with_native_batch_delete(Arc::new(store)),
        control,
    )
}

impl fmt::Display for HistoryObservationFaultStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "HistoryObservationFaultStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for HistoryObservationFaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        if self.control.armed.load(Ordering::SeqCst)
            && self.fault == HistoryObservationFault::DeleteBeforeFirstGet
            && location.as_ref() == self.target
            && !self.control.fired.swap(true, Ordering::SeqCst)
        {
            self.mutation_inner.delete(location).await?;
        }
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, OsResult<Path>>,
    ) -> BoxStream<'a, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        let armed = self.control.armed.load(Ordering::SeqCst);
        let target = self.target.clone();
        let fault = self.fault;
        self.inner
            .list(prefix)
            .map(move |result| {
                result.map(|mut object| {
                    let strip_version = armed
                        && match fault {
                            HistoryObservationFault::MissingListEtag
                            | HistoryObservationFault::DeleteBeforeFirstGet => {
                                object.location.as_ref() == target
                            }
                            HistoryObservationFault::MissingHistoryVersions => {
                                object.location.as_ref().starts_with(&target)
                            }
                        };
                    if strip_version {
                        object.e_tag = None;
                        object.version = None;
                    }
                    object
                })
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

async fn seed_history_generations(
    world: &MaintenanceWorld,
    namespace: &str,
    count: usize,
) -> Manifest {
    let mut manifest = Manifest::new_at(world.now);
    for _ in 0..count {
        manifest
            .write(&world.store, namespace)
            .await
            .expect("ideal GC history-memo fixture write failed");
    }
    manifest
}

async fn seed_gc_cycle_sized_history(world: &MaintenanceWorld, namespace: &str) -> Manifest {
    let mut manifest = Manifest::new_at(world.now);
    for index in 0..18 {
        let padding_len = if index < 17 { 388 } else { 347 };
        manifest.pending_deletes = vec!["x".repeat(padding_len)];
        manifest
            .write(&world.store, namespace)
            .await
            .expect("ideal GC sized-history fixture write failed");
    }
    manifest.pending_deletes.clear();
    world
        .store
        .put(
            &Manifest::s3_key(namespace),
            manifest
                .to_bytes()
                .expect("ideal GC sized-history live manifest encode failed"),
        )
        .await
        .expect("ideal GC sized-history live manifest setup failed");

    let mut bytes = 0_u64;
    for generation in 1..=18 {
        bytes += world
            .store
            .get(&Manifest::history_key(namespace, generation))
            .await
            .expect("ideal GC sized-history verification GET failed")
            .len() as u64;
    }
    assert_eq!(bytes, 9_139, "ideal GC history fixture drifted");
    manifest
}

fn history_memo_gc_config() -> GcConfig {
    GcConfig {
        horizon_secs: 0,
        compaction_upload_window_secs: 0,
        skew_slop_secs: 0,
        allow_unsafe_short_horizon: true,
        manifest_history_keep_count: 1_024,
        pitr_retention_secs: 0,
    }
}

async fn prime_history_memo(
    runner: &mut GcRunner,
    incarnation: &GcNamespaceIncarnation,
    now: DateTime<Utc>,
) {
    let report = runner
        .run_cycle_at(incarnation.clone(), now)
        .await
        .expect("ideal cold GC history-memo prime failed");
    assert_eq!(report.objects_deleted, 0);
    assert_eq!(report.pending_deletes_deleted, 0);
}

fn assert_history_memo_census(
    sample: &IdealSample,
    history_gets: u64,
    total_gets: u64,
    lists: u64,
    puts: u64,
    calls: u64,
    get_depth: u32,
) {
    let actual_history_gets = sample
        .physical_operations
        .iter()
        .filter(|operation| operation.verb == "get" && operation.key == "<generation>.msgpack")
        .count() as u64;
    assert_eq!(
        actual_history_gets, history_gets,
        "unexpected history GETs in physical operations: {:?}",
        sample.physical_operations
    );
    assert_eq!(sample.total_get_ops, total_gets);
    assert_eq!(sample.serial_get_chain.depth, get_depth);
    assert_eq!(physical_mode_ops(sample, "list_recursive"), lists);
    assert_eq!(physical_verb_ops(sample, "put"), puts);
    assert_eq!(
        sample
            .physical_verb_mode_totals
            .iter()
            .map(|total| total.ops)
            .sum::<u64>(),
        calls
    );
}

fn assert_full_gc_census(
    sample: &IdealSample,
    history_gets: u64,
    total_gets: u64,
    lists: u64,
    puts: u64,
    deletes: u64,
    get_depth: u32,
) {
    let calls = total_gets + lists + puts + deletes;
    assert_history_memo_census(
        sample,
        history_gets,
        total_gets,
        lists,
        puts,
        calls,
        get_depth,
    );
    assert_eq!(physical_verb_ops(sample, "delete"), deletes);
}

fn physical_verb_ops(sample: &IdealSample, verb: &str) -> u64 {
    sample
        .physical_verb_mode_totals
        .iter()
        .filter(|total| total.verb == verb)
        .map(|total| total.ops)
        .sum()
}

#[derive(Clone, Copy)]
enum PendingShape {
    Young,
    HistoryPinned,
    Eligible,
}

async fn execute_pending_delete(case: &IdealCase, shape: PendingShape) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    let object_id = Ulid::from_parts(world.now.timestamp_millis() as u64, 1);
    let pending_key = format!("{namespace}/wal/{object_id}.wal");
    world
        .store
        .put(&pending_key, Bytes::from_static(b"pending-delete"))
        .await
        .expect("ideal pending-delete object setup failed");

    let mut manifest = Manifest::new_at(world.now);
    manifest.pending_deletes.push(pending_key);
    match shape {
        PendingShape::HistoryPinned => manifest
            .write(&world.store, &namespace)
            .await
            .expect("ideal history-pinned manifest setup failed"),
        PendingShape::Young | PendingShape::Eligible => world
            .store
            .put(
                &Manifest::s3_key(&namespace),
                manifest
                    .to_bytes()
                    .expect("ideal pending-delete manifest serialization failed"),
            )
            .await
            .expect("ideal pending-delete live manifest setup failed"),
    }

    let mut gc = GcConfig::default();
    if matches!(shape, PendingShape::HistoryPinned | PendingShape::Eligible) {
        gc.horizon_secs = 0;
        gc.allow_unsafe_short_horizon = true;
    }
    world.begin_measurement().await;
    let report = drain_pending_deletes_at(&world.store, &namespace, &gc, world.now)
        .await
        .expect("ideal pending-delete drain failed");
    match shape {
        PendingShape::Young | PendingShape::HistoryPinned => {
            assert_eq!(report.objects_deleted, 0);
            assert_eq!(report.entries_retained, 1);
        }
        PendingShape::Eligible => {
            assert_eq!(report.objects_deleted, 1);
            assert_eq!(report.entries_pruned, 1);
        }
    }
    world.finish(case).await
}

async fn execute_orphan_mark(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    persist_empty_manifest(&world, &namespace).await;
    let orphan_id = Ulid::from_parts(world.now.timestamp_millis() as u64, 2);
    let orphan = format!("{namespace}/wal/{orphan_id}.wal");
    world
        .store
        .put(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("ideal orphan-mark object setup failed");

    world.begin_measurement().await;
    let report = run_gc_cycle_at(&world.store, &namespace, &GcConfig::default(), world.now)
        .await
        .expect("ideal orphan mark cycle failed");
    assert_eq!(report.candidates_marked, 1);
    assert_eq!(report.objects_deleted, 0);
    world.finish(case).await
}

async fn execute_orphan_sweep(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    let manifest = persist_empty_manifest(&world, &namespace).await;
    let gc = GcConfig::default();
    let old = world.now
        - ChronoDuration::seconds(
            i64::try_from(gc.horizon_secs).expect("GC horizon must fit i64") + 1,
        );
    let orphan_id = Ulid::from_parts(old.timestamp_millis() as u64, 3);
    let orphan = format!("{namespace}/wal/{orphan_id}.wal");
    world
        .store
        .put(&orphan, Bytes::from_static(b"old-orphan"))
        .await
        .expect("ideal orphan-sweep object setup failed");
    save_gc_candidates(
        &world.store,
        &namespace,
        &[GcCandidate {
            key: orphan,
            first_seen_unreachable_at: old,
            unreachable_since_manifest_version: manifest.version(),
        }],
    )
    .await
    .expect("ideal orphan-sweep candidate setup failed");

    world.begin_measurement().await;
    let report = run_gc_cycle_at(&world.store, &namespace, &gc, world.now)
        .await
        .expect("ideal orphan sweep cycle failed");
    assert_eq!(report.objects_deleted, 1);
    world.finish(case).await
}

async fn execute_history_prune(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    let mut manifest = Manifest::new_at(world.now - ChronoDuration::seconds(3));
    for _ in 0..3 {
        manifest
            .write(&world.store, &namespace)
            .await
            .expect("ideal manifest-history setup write failed");
    }

    world.begin_measurement().await;
    let pruned = Manifest::prune_history(&world.store, &namespace, 1)
        .await
        .expect("ideal manifest-history prune failed");
    assert_eq!(pruned, 2);
    world.finish(case).await
}

async fn execute_staging_write(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    world.begin_measurement().await;
    write_compaction_staging(&world.store, &namespace, 7, staged_keys(&namespace))
        .await
        .expect("ideal staging write failed");
    world.finish(case).await
}

async fn execute_staging_clear(case: &IdealCase) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    write_compaction_staging(&world.store, &namespace, 7, staged_keys(&namespace))
        .await
        .expect("ideal staging-clear setup failed");
    world.begin_measurement().await;
    clear_compaction_staging(&world.store, &namespace, 7)
        .await
        .expect("ideal staging clear failed");
    world.finish(case).await
}

#[derive(Clone, Copy)]
enum ActiveStagingShape {
    MissingLease,
    ExpiredLease,
    MatchingToken,
    MixedTokens,
}

async fn execute_active_staging(case: &IdealCase, shape: ActiveStagingShape) -> IdealSample {
    let world = MaintenanceWorld::new().await;
    let namespace = world.namespace(case.id.as_str());
    let expected = staged_keys(&namespace);
    let mut active_token = 1;

    match shape {
        ActiveStagingShape::MissingLease => {
            write_compaction_staging(&world.store, &namespace, active_token, expected.clone())
                .await
                .expect("ideal missing-lease staging setup failed");
        }
        ActiveStagingShape::ExpiredLease => {
            let manager = LeaseManager::with_clock(
                world.store.clone(),
                "ideal-staging-holder".to_string(),
                Duration::ZERO,
                world.clock(),
            );
            active_token = manager
                .acquire(&namespace)
                .await
                .expect("ideal expired-staging lease setup failed")
                .fencing_token;
            write_compaction_staging(&world.store, &namespace, active_token, expected.clone())
                .await
                .expect("ideal expired-lease staging setup failed");
        }
        ActiveStagingShape::MatchingToken | ActiveStagingShape::MixedTokens => {
            let manager = LeaseManager::with_clock(
                world.store.clone(),
                "ideal-staging-holder".to_string(),
                Duration::from_secs(3_600),
                world.clock(),
            );
            active_token = manager
                .acquire(&namespace)
                .await
                .expect("ideal active-staging lease setup failed")
                .fencing_token;
            write_compaction_staging(&world.store, &namespace, active_token, expected.clone())
                .await
                .expect("ideal active-token staging setup failed");
            if matches!(shape, ActiveStagingShape::MixedTokens) {
                write_compaction_staging(
                    &world.store,
                    &namespace,
                    active_token + 1,
                    BTreeSet::from([format!("{namespace}/segments/stale/bootstrap.bin")]),
                )
                .await
                .expect("ideal stale-token staging setup failed");
            }
        }
    }

    world.begin_measurement().await;
    let observed = active_staged_keys_at(&world.store, &namespace, world.now)
        .await
        .expect("ideal active staging lookup failed");
    match shape {
        ActiveStagingShape::MissingLease | ActiveStagingShape::ExpiredLease => {
            assert!(observed.is_empty());
        }
        ActiveStagingShape::MatchingToken | ActiveStagingShape::MixedTokens => {
            assert_eq!(observed, expected);
        }
    }
    let sample = world.finish(case).await;
    if matches!(shape, ActiveStagingShape::MixedTokens) {
        assert_eq!(
            (
                sample.total_get_ops,
                sample.total_get_bytes,
                physical_mode_ops(&sample, "list_recursive"),
                sample.serial_get_chain.depth,
            ),
            (3, 575, 1, 2),
            "staging records must retain their exact census while sharing one bounded GET stage after the lease",
        );
        assert_eq!(physical_verb_ops(&sample, "put"), 0);
        assert_eq!(physical_verb_ops(&sample, "delete"), 0);
    }
    sample
}

fn staged_keys(namespace: &str) -> BTreeSet<String> {
    BTreeSet::from([
        format!("{namespace}/segments/seg_active/bootstrap.bin"),
        format!("{namespace}/segments/seg_active/cluster_group_0.bin"),
    ])
}

async fn persist_empty_manifest(world: &MaintenanceWorld, namespace: &str) -> Manifest {
    let mut manifest = Manifest::new_at(world.now);
    manifest
        .write(&world.store, namespace)
        .await
        .expect("ideal empty manifest setup failed");
    manifest
}

async fn await_tracker_idle(tracker: &DepthTracker) {
    const MAX_YIELDS: usize = 4_096;
    const REQUIRED_ZERO_STREAK: usize = 8;
    let mut zero_streak = 0;
    for _ in 0..MAX_YIELDS {
        tokio::task::yield_now().await;
        if tracker.active_operations() == 0 {
            zero_streak += 1;
            if zero_streak == REQUIRED_ZERO_STREAK {
                return;
            }
        } else {
            zero_streak = 0;
        }
    }
    panic!(
        "ideal maintenance measurement did not quiesce: active_operations={}",
        tracker.active_operations()
    );
}

fn snapshot_repeat(counter: &GetCounter, tracker: &DepthTracker) -> RepeatCounters {
    let classes = counter
        .class_breakdown()
        .into_iter()
        .map(|(class, stats)| (class.name().to_string(), stats))
        .collect::<BTreeMap<_, _>>();
    let totals = classes
        .values()
        .copied()
        .fold(ClassStats::default(), |mut total, stats| {
            total.get_ops = total
                .get_ops
                .checked_add(stats.get_ops)
                .expect("ideal maintenance GET operation total overflowed");
            total.get_bytes = total
                .get_bytes
                .checked_add(stats.get_bytes)
                .expect("ideal maintenance GET byte total overflowed");
            total.put_ops = total
                .put_ops
                .checked_add(stats.put_ops)
                .expect("ideal maintenance PUT operation total overflowed");
            total.put_bytes = total
                .put_bytes
                .checked_add(stats.put_bytes)
                .expect("ideal maintenance PUT byte total overflowed");
            total
        });
    let cutoff_us = tracker.elapsed_us();
    let spans = tracker.take_spans();
    let get_path = DepthTracker::critical_path(&spans, &[SpanKind::Get], Some(cutoff_us));
    let put_get_path =
        DepthTracker::critical_path(&spans, &[SpanKind::Get, SpanKind::Put], Some(cutoff_us));
    let op_counts = [
        ("head", SpanKind::Head),
        ("list", SpanKind::List),
        ("copy", SpanKind::Copy),
        ("delete", SpanKind::Delete),
    ]
    .into_iter()
    .map(|(name, kind)| {
        (
            name.to_string(),
            spans.iter().filter(|span| span.kind == kind).count() as u64,
        )
    })
    .collect();

    RepeatCounters {
        classes,
        totals,
        raw_get_path: get_path.clone(),
        raw_put_get_path: put_get_path.clone(),
        get_path,
        put_get_path,
        spans,
        op_counts,
        labeled: Vec::new(),
        wall_elapsed_us: 0,
        response_cutoff_us: cutoff_us,
    }
}

fn physical_mode_ops(sample: &IdealSample, mode: &str) -> u64 {
    sample
        .physical_verb_mode_totals
        .iter()
        .filter(|total| total.mode == mode)
        .map(|total| total.ops)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::super::catalog;
    use super::*;

    #[test]
    fn owns_exactly_the_awaited_maintenance_cases() {
        let supported = catalog::all()
            .iter()
            .filter(|case| supports(case))
            .map(|case| case.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            supported,
            vec![
                "compaction.direct_noop",
                "compaction.direct_full",
                "compaction.direct_incremental",
                "compaction.layout_rewrite_no_wal",
                "compaction.all_vectors_deleted",
                "compaction.full_with_fts",
                "compaction.fenced_full",
                "compaction.fenced_incremental",
                "background.discovery_tick_empty",
                "background.discovery_tick_active",
                "background.cached_tick_idle",
                "background.tick_resume_delete",
                "background.tick_lease_held",
                "background.tick_compaction_success",
                "gc.idle_warm_second_cycle",
                "gc.idle_new_orphan",
                "gc.idle_candidate_maturity",
                "gc.idle_pending_delete_maturity",
                "gc.idle_pitr_expiry",
                "gc.idle_staging_lease_expiry",
                "gc.idle_changed_snapshot",
                "gc.parallel_snapshot_pins",
                "gc.idle_changed_staging",
                "gc.idle_changed_candidate_ledger",
                "gc.idle_backward_clock",
                "gc.idle_shorter_retention_config",
                "gc.idle_prior_partial_failure",
                "gc.prune_reuse_empty_pending_uncacheable",
                "gc.prune_reuse_eligible_pending_refresh",
                "gc.history_memo_new_generation",
                "gc.history_memo_changed_etag",
                "gc.history_memo_missing_etag",
                "gc.history_memo_disappears_between_list_and_get",
                "gc.history_memo_unpublished_orphan_overwrite",
                "gc.history_memo_corrupt_changed_body",
                "gc.history_memo_namespace_recreated",
                "gc.history_memo_cold_runner_restart",
                "gc.pending_delete_young",
                "gc.pending_delete_history_pinned",
                "gc.pending_delete_eligible",
                "gc.orphan_mark",
                "gc.orphan_sweep",
                "gc.manifest_history_prune",
                "gc.staging_write",
                "gc.staging_clear",
                "gc.active_staging_missing_lease",
                "gc.active_staging_expired_lease",
                "gc.active_staging_matching_token",
                "gc.active_staging_mixed_tokens",
            ]
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn all_maintenance_cases_use_real_storage() {
        for case in catalog::all().iter().filter(|case| supports(case)) {
            let sample = execute(case).await.unwrap_or_else(|| {
                panic!("maintenance case {} was not executed", case.id.as_str())
            });
            assert_eq!(sample.scenario_id, case.id.as_str());
        }
    }
}
