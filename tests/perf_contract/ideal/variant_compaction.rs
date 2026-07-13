//! Isolated compaction measurements for quantization and hierarchy variants.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::fts::global_index::global_fts_key;
use zeppelin::fts::FtsFieldConfig;
use zeppelin::index::bitmap::bitmap_key;
use zeppelin::index::hierarchical::tree_meta_key;
use zeppelin::index::quantization::pq::pq_codebook_key;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::manager::{NamespaceIndexConfig, NamespaceManager};
use zeppelin::storage::ZeppelinStore;
use zeppelin::time::{Clock, TimeSource};
use zeppelin::types::{AttributeValue, DistanceMetric, VectorEntry};
use zeppelin::wal::manifest::SegmentRef;
use zeppelin::wal::{Manifest, WalReader, WalWriter};

use crate::common::counting::{perf_counting_store, ClassStats, GetCounter};
use crate::common::harness::TestHarness;
use crate::perf_contract::depth::{depth_store, DepthTracker, SpanKind};
use crate::perf_contract::scenario::RepeatCounters;

use super::artifacts::IdealSample;
use super::catalog::{CompactionCase, IdealCase, IdealOperation};

const DIMENSIONS: usize = 16;
const BASE_VECTORS: usize = 64;
const SMALL_WAL_VECTORS: usize = 2;

/// Return whether this module owns the quantization/hierarchy compaction case.
pub(crate) fn supports(case: &IdealCase) -> bool {
    matches!(
        case.operation,
        IdealOperation::Compaction(
            CompactionCase::FlatPqFull
                | CompactionCase::FlatPqIncremental
                | CompactionCase::FlatSqPopulatedBitmap
                | CompactionCase::HierarchicalSqFull
                | CompactionCase::HierarchicalPqFull
                | CompactionCase::HierarchicalExistingSmallWalFullRewrite
                | CompactionCase::HierarchicalFullWithFts
        )
    )
}

/// Execute one fully awaited production compaction variant.
pub(crate) async fn execute(case: &IdealCase) -> Option<IdealSample> {
    match case.operation {
        IdealOperation::Compaction(CompactionCase::FlatPqFull) => {
            Some(execute_flat_pq_full(case).await)
        }
        IdealOperation::Compaction(CompactionCase::FlatPqIncremental) => {
            Some(execute_flat_pq_incremental(case).await)
        }
        IdealOperation::Compaction(CompactionCase::FlatSqPopulatedBitmap) => {
            Some(execute_flat_sq_populated_bitmap(case).await)
        }
        IdealOperation::Compaction(CompactionCase::HierarchicalSqFull) => {
            Some(execute_hierarchical_full(case, QuantizationType::Scalar).await)
        }
        IdealOperation::Compaction(CompactionCase::HierarchicalPqFull) => {
            Some(execute_hierarchical_full(case, QuantizationType::Product).await)
        }
        IdealOperation::Compaction(CompactionCase::HierarchicalExistingSmallWalFullRewrite) => {
            Some(execute_hierarchical_existing_rewrite(case).await)
        }
        IdealOperation::Compaction(CompactionCase::HierarchicalFullWithFts) => {
            Some(execute_hierarchical_full_with_fts(case).await)
        }
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

struct VariantWorld {
    harness: TestHarness,
    store: ZeppelinStore,
    counter: GetCounter,
    tracker: DepthTracker,
    now: DateTime<Utc>,
    cleanup_namespaces: Vec<String>,
}

impl VariantWorld {
    async fn new() -> Self {
        let harness = TestHarness::new().await;
        let (depth_wrapped, tracker) = depth_store(&harness.store);
        let (store, counter) = perf_counting_store(&depth_wrapped);
        let now = DateTime::from_timestamp(1_800_000_000, 456_000_000)
            .expect("ideal variant-compaction fixed clock must be representable");
        Self {
            harness,
            store,
            counter,
            tracker,
            now,
            cleanup_namespaces: Vec::new(),
        }
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
        IdealSample::from_repeat(
            case.id.as_str(),
            &snapshot_repeat(&self.counter, &self.tracker),
        )
    }

    async fn finish(self, sample: IdealSample) -> IdealSample {
        for namespace in &self.cleanup_namespaces {
            self.harness
                .store
                .delete_prefix(&format!("{namespace}/"))
                .await
                .expect("ideal variant-compaction managed namespace cleanup failed");
        }
        self.harness.cleanup().await;
        sample
    }
}

async fn execute_flat_pq_full(case: &IdealCase) -> IdealSample {
    let mut world = VariantWorld::new().await;
    let indexing = flat_config(QuantizationType::Product, false);
    let namespace = setup_active_namespace_and_wal(
        &mut world,
        case.id.as_str(),
        &indexing,
        HashMap::new(),
        variant_vectors("flat-pq-full", BASE_VECTORS),
    )
    .await;
    let compactor = variant_compactor(&world, CompactionConfig::default(), indexing);

    world.begin_measurement().await;
    let result = compactor
        .compact(&namespace)
        .await
        .expect("ideal flat PQ full compaction failed");
    assert_eq!(result.vectors_compacted, BASE_VECTORS);
    assert_eq!(result.fragments_removed, 1);
    assert!(result.old_segment_removed.is_none());
    let segment_id = result
        .segment_id
        .expect("ideal flat PQ full compaction must publish a segment");
    let sample = world.snapshot(case).await;

    let segment = read_active_segment(&world, &namespace, &segment_id).await;
    assert_flat_segment(&segment, QuantizationType::Product, BASE_VECTORS);
    assert!(segment.bitmap_fields.is_empty());
    assert!(world
        .harness
        .store
        .exists(&pq_codebook_key(&namespace, &segment_id))
        .await
        .expect("ideal flat PQ codebook existence check failed"));
    world.finish(sample).await
}

async fn execute_flat_pq_incremental(case: &IdealCase) -> IdealSample {
    let mut world = VariantWorld::new().await;
    let indexing = flat_config(QuantizationType::Product, false);
    let base = variant_vectors("flat-pq-resident", BASE_VECTORS);
    let namespace = setup_active_namespace_and_wal(
        &mut world,
        case.id.as_str(),
        &indexing,
        HashMap::new(),
        base.clone(),
    )
    .await;
    let compactor = variant_compactor(&world, incremental_config(), indexing);
    let initial = compactor
        .compact(&namespace)
        .await
        .expect("ideal flat PQ resident compaction setup failed");
    let initial_id = initial
        .segment_id
        .expect("ideal flat PQ resident setup must publish a segment");
    WalWriter::with_clock(world.store.clone(), world.clock())
        .append(
            &namespace,
            nearby_vectors("flat-pq-added", &base[0], SMALL_WAL_VECTORS),
            Vec::new(),
        )
        .await
        .expect("ideal flat PQ incremental WAL setup failed");

    world.begin_measurement().await;
    let result = compactor
        .compact(&namespace)
        .await
        .expect("ideal flat PQ incremental compaction failed");
    assert_eq!(result.vectors_compacted, BASE_VECTORS + SMALL_WAL_VECTORS);
    assert_eq!(result.fragments_removed, 1);
    assert_eq!(
        result.old_segment_removed.as_deref(),
        Some(initial_id.as_str())
    );
    let segment_id = result
        .segment_id
        .expect("ideal flat PQ incremental compaction must publish a segment");
    let sample = world.snapshot(case).await;

    let segment = read_active_segment(&world, &namespace, &segment_id).await;
    assert_flat_segment(
        &segment,
        QuantizationType::Product,
        BASE_VECTORS + SMALL_WAL_VECTORS,
    );
    assert_eq!(segment.cluster_owners.len(), segment.cluster_count);
    assert!(
        segment
            .cluster_owners
            .iter()
            .any(|owner| owner == &initial_id),
        "ideal flat PQ incremental compaction must carry an untouched cluster"
    );
    assert!(
        segment
            .cluster_owners
            .iter()
            .any(|owner| owner == &segment_id),
        "ideal flat PQ incremental compaction must rewrite a touched cluster"
    );
    assert!(world
        .harness
        .store
        .exists(&pq_codebook_key(&namespace, &segment_id))
        .await
        .expect("ideal incremental PQ codebook existence check failed"));
    world.finish(sample).await
}

async fn execute_flat_sq_populated_bitmap(case: &IdealCase) -> IdealSample {
    let mut world = VariantWorld::new().await;
    let indexing = flat_config(QuantizationType::Scalar, true);
    let namespace = setup_active_namespace_and_wal(
        &mut world,
        case.id.as_str(),
        &indexing,
        HashMap::new(),
        bitmap_vectors("flat-sq-bitmap", BASE_VECTORS),
    )
    .await;
    let compactor = variant_compactor(&world, CompactionConfig::default(), indexing);

    world.begin_measurement().await;
    let result = compactor
        .compact(&namespace)
        .await
        .expect("ideal populated SQ bitmap compaction failed");
    assert_eq!(result.vectors_compacted, BASE_VECTORS);
    assert_eq!(result.fragments_removed, 1);
    let segment_id = result
        .segment_id
        .expect("ideal populated SQ bitmap compaction must publish a segment");
    let sample = world.snapshot(case).await;

    let segment = read_active_segment(&world, &namespace, &segment_id).await;
    assert_flat_segment(&segment, QuantizationType::Scalar, BASE_VECTORS);
    assert_eq!(
        segment
            .bitmap_fields
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["priority".to_string(), "status".to_string()])
    );
    for cluster in 0..segment.cluster_count {
        assert!(world
            .harness
            .store
            .exists(&bitmap_key(&namespace, &segment_id, cluster))
            .await
            .expect("ideal populated bitmap existence check failed"));
    }
    world.finish(sample).await
}

async fn execute_hierarchical_full(
    case: &IdealCase,
    quantization: QuantizationType,
) -> IdealSample {
    let mut world = VariantWorld::new().await;
    let indexing = hierarchical_config(quantization, false);
    let namespace = setup_active_namespace_and_wal(
        &mut world,
        case.id.as_str(),
        &indexing,
        HashMap::new(),
        variant_vectors(case.id.as_str(), BASE_VECTORS),
    )
    .await;
    let compactor = variant_compactor(&world, CompactionConfig::default(), indexing);

    world.begin_measurement().await;
    let result = compactor
        .compact(&namespace)
        .await
        .expect("ideal hierarchical full compaction failed");
    assert_eq!(result.vectors_compacted, BASE_VECTORS);
    assert_eq!(result.fragments_removed, 1);
    assert!(result.old_segment_removed.is_none());
    let segment_id = result
        .segment_id
        .expect("ideal hierarchical full compaction must publish a segment");
    let sample = world.snapshot(case).await;

    let segment = read_active_segment(&world, &namespace, &segment_id).await;
    assert_hierarchical_segment(&segment, quantization, BASE_VECTORS);
    assert!(world
        .harness
        .store
        .exists(&tree_meta_key(&namespace, &segment_id))
        .await
        .expect("ideal hierarchical tree metadata existence check failed"));
    if quantization == QuantizationType::Product {
        assert!(world
            .harness
            .store
            .exists(&pq_codebook_key(&namespace, &segment_id))
            .await
            .expect("ideal hierarchical PQ codebook existence check failed"));
    }
    world.finish(sample).await
}

async fn execute_hierarchical_existing_rewrite(case: &IdealCase) -> IdealSample {
    let mut world = VariantWorld::new().await;
    let indexing = hierarchical_config(QuantizationType::Scalar, false);
    let base = variant_vectors("hierarchical-resident", BASE_VECTORS);
    let namespace = setup_active_namespace_and_wal(
        &mut world,
        case.id.as_str(),
        &indexing,
        HashMap::new(),
        base.clone(),
    )
    .await;
    let compactor = variant_compactor(&world, incremental_config(), indexing);
    let initial = compactor
        .compact(&namespace)
        .await
        .expect("ideal hierarchical resident compaction setup failed");
    let initial_id = initial
        .segment_id
        .expect("ideal hierarchical resident setup must publish a segment");
    WalWriter::with_clock(world.store.clone(), world.clock())
        .append(
            &namespace,
            nearby_vectors("hierarchical-added", &base[0], SMALL_WAL_VECTORS),
            Vec::new(),
        )
        .await
        .expect("ideal hierarchical small-WAL setup failed");

    world.begin_measurement().await;
    let result = compactor
        .compact(&namespace)
        .await
        .expect("ideal hierarchical existing+small-WAL rewrite failed");
    assert_eq!(result.vectors_compacted, BASE_VECTORS + SMALL_WAL_VECTORS);
    assert_eq!(result.fragments_removed, 1);
    assert_eq!(
        result.old_segment_removed.as_deref(),
        Some(initial_id.as_str())
    );
    let segment_id = result
        .segment_id
        .expect("ideal hierarchical full rewrite must publish a segment");
    let sample = world.snapshot(case).await;

    let segment = read_active_segment(&world, &namespace, &segment_id).await;
    assert_hierarchical_segment(
        &segment,
        QuantizationType::Scalar,
        BASE_VECTORS + SMALL_WAL_VECTORS,
    );
    assert!(
        segment.cluster_owners.is_empty(),
        "hierarchical existing+small-WAL compaction must fully rewrite, never carry clusters"
    );
    assert!(world
        .harness
        .store
        .exists(&tree_meta_key(&namespace, &segment_id))
        .await
        .expect("ideal rewritten hierarchical tree metadata existence check failed"));
    world.finish(sample).await
}

async fn execute_hierarchical_full_with_fts(case: &IdealCase) -> IdealSample {
    let mut world = VariantWorld::new().await;
    let configs = HashMap::from([("content".to_string(), FtsFieldConfig::default())]);
    let indexing = hierarchical_config(QuantizationType::Scalar, true);
    let namespace = setup_active_namespace_and_wal(
        &mut world,
        case.id.as_str(),
        &indexing,
        configs.clone(),
        fts_vectors("hierarchical-fts", BASE_VECTORS),
    )
    .await;
    let compactor = variant_compactor(&world, CompactionConfig::default(), indexing);

    world.begin_measurement().await;
    let result = compactor
        .compact_with_fts(&namespace, None, &configs)
        .await
        .expect("ideal hierarchical FTS full compaction failed");
    assert_eq!(result.vectors_compacted, BASE_VECTORS);
    assert_eq!(result.fragments_removed, 1);
    let segment_id = result
        .segment_id
        .expect("ideal hierarchical FTS full compaction must publish a segment");
    let sample = world.snapshot(case).await;

    let segment = read_active_segment(&world, &namespace, &segment_id).await;
    assert_hierarchical_segment(&segment, QuantizationType::Scalar, BASE_VECTORS);
    assert_eq!(segment.fts_fields, vec!["content".to_string()]);
    assert!(segment.has_global_fts);
    assert!(world
        .harness
        .store
        .exists(&tree_meta_key(&namespace, &segment_id))
        .await
        .expect("ideal hierarchical FTS tree metadata existence check failed"));
    assert!(world
        .harness
        .store
        .exists(&global_fts_key(&namespace, &segment_id))
        .await
        .expect("ideal hierarchical global FTS existence check failed"));
    world.finish(sample).await
}

async fn setup_active_namespace_and_wal(
    world: &mut VariantWorld,
    suffix: &str,
    indexing: &IndexingConfig,
    fts_configs: HashMap<String, FtsFieldConfig>,
    vectors: Vec<VectorEntry>,
) -> String {
    let namespace = world.managed_namespace(suffix);
    NamespaceManager::with_clock(
        world.store.clone(),
        Duration::from_secs(3_600),
        world.clock(),
    )
    .create_with_fts_and_index_config(
        &namespace,
        DIMENSIONS,
        DistanceMetric::Euclidean,
        fts_configs,
        Some(NamespaceIndexConfig::from_indexing_config(indexing)),
    )
    .await
    .expect("ideal variant-compaction active namespace setup failed");
    WalWriter::with_clock(world.store.clone(), world.clock())
        .append(&namespace, vectors, Vec::new())
        .await
        .expect("ideal variant-compaction WAL setup failed");
    namespace
}

fn variant_compactor(
    world: &VariantWorld,
    compaction: CompactionConfig,
    indexing: IndexingConfig,
) -> Compactor {
    Compactor::with_clock(
        world.store.clone(),
        WalReader::new(world.store.clone()),
        compaction,
        indexing,
        Duration::from_secs(300),
        world.clock(),
    )
}

fn incremental_config() -> CompactionConfig {
    CompactionConfig {
        max_wal_fragments_before_compact: 1,
        retrain_imbalance_threshold: 1_000.0,
        ..Default::default()
    }
}

fn flat_config(quantization: QuantizationType, bitmap_index: bool) -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        quantization,
        pq_m: 4,
        hierarchical: false,
        bitmap_index,
        fts_index: false,
        ..Default::default()
    }
}

fn hierarchical_config(quantization: QuantizationType, fts_index: bool) -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        quantization,
        pq_m: 4,
        hierarchical: true,
        leaf_size: Some(10),
        bitmap_index: false,
        fts_index,
        ..Default::default()
    }
}

fn variant_vectors(prefix: &str, count: usize) -> Vec<VectorEntry> {
    (0..count)
        .map(|index| {
            let cluster = index % 4;
            let row = index / 4;
            VectorEntry {
                id: format!("{prefix}-{index}"),
                values: (0..DIMENSIONS)
                    .map(|dimension| {
                        cluster as f32 * 12.0 + row as f32 * 0.01 + dimension as f32 * 0.000_1
                    })
                    .collect(),
                attributes: None,
            }
        })
        .collect()
}

fn nearby_vectors(prefix: &str, anchor: &VectorEntry, count: usize) -> Vec<VectorEntry> {
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

fn bitmap_vectors(prefix: &str, count: usize) -> Vec<VectorEntry> {
    variant_vectors(prefix, count)
        .into_iter()
        .enumerate()
        .map(|(index, mut vector)| {
            vector.attributes = Some(HashMap::from([
                (
                    "status".to_string(),
                    AttributeValue::String(if index % 2 == 0 {
                        "active".to_string()
                    } else {
                        "inactive".to_string()
                    }),
                ),
                (
                    "priority".to_string(),
                    AttributeValue::Integer(index as i64),
                ),
            ]));
            vector
        })
        .collect()
}

fn fts_vectors(prefix: &str, count: usize) -> Vec<VectorEntry> {
    variant_vectors(prefix, count)
        .into_iter()
        .enumerate()
        .map(|(index, mut vector)| {
            vector.attributes = Some(HashMap::from([(
                "content".to_string(),
                AttributeValue::String(format!(
                    "hierarchical storage document {index} deterministic search"
                )),
            )]));
            vector
        })
        .collect()
}

async fn read_active_segment(
    world: &VariantWorld,
    namespace: &str,
    expected_id: &str,
) -> SegmentRef {
    let manifest = Manifest::read(&world.harness.store, namespace)
        .await
        .expect("ideal variant-compaction manifest oracle failed")
        .expect("ideal variant-compaction manifest disappeared");
    assert_eq!(manifest.active_segment.as_deref(), Some(expected_id));
    manifest
        .segments
        .iter()
        .find(|segment| segment.id == expected_id)
        .cloned()
        .expect("ideal variant-compaction active segment ref missing")
}

fn assert_flat_segment(segment: &SegmentRef, quantization: QuantizationType, vectors: usize) {
    assert_eq!(segment.vector_count, vectors);
    assert_eq!(segment.quantization, quantization);
    assert!(!segment.hierarchical);
    assert_eq!(segment.cluster_count, 4);
    assert!(segment.sketch.is_some());
    assert!(segment.bootstrap.is_some());
    assert!(segment.membership.is_some());
}

fn assert_hierarchical_segment(
    segment: &SegmentRef,
    quantization: QuantizationType,
    vectors: usize,
) {
    assert_eq!(segment.vector_count, vectors);
    assert_eq!(segment.quantization, quantization);
    assert!(segment.hierarchical);
    assert!(segment.cluster_count > 1);
    assert!(segment.sketch.is_none());
    assert!(segment.bootstrap.is_none());
    assert!(segment.membership.is_none());
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
        "ideal variant-compaction measurement did not quiesce: active_operations={}",
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
                .expect("ideal variant-compaction GET operation total overflowed");
            total.get_bytes = total
                .get_bytes
                .checked_add(stats.get_bytes)
                .expect("ideal variant-compaction GET byte total overflowed");
            total.put_ops = total
                .put_ops
                .checked_add(stats.put_ops)
                .expect("ideal variant-compaction PUT operation total overflowed");
            total.put_bytes = total
                .put_bytes
                .checked_add(stats.put_bytes)
                .expect("ideal variant-compaction PUT byte total overflowed");
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

#[cfg(test)]
mod tests {
    use super::super::catalog;
    use super::*;

    #[test]
    fn owns_exactly_the_seven_variant_compaction_cases() {
        let supported = catalog::all()
            .iter()
            .filter(|case| supports(case))
            .map(|case| case.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            supported,
            vec![
                "compaction.flat_pq_full",
                "compaction.flat_pq_incremental",
                "compaction.flat_sq_populated_bitmap",
                "compaction.hierarchical_sq_full",
                "compaction.hierarchical_pq_full",
                "compaction.hierarchical_existing_small_wal_full_rewrite",
                "compaction.hierarchical_full_with_fts",
            ]
        );
    }

    #[tokio::test]
    #[ignore = "requires TEST_BACKEND=minio"]
    async fn all_variant_compaction_cases_use_real_storage() {
        for case in catalog::all().iter().filter(|case| supports(case)) {
            let sample = execute(case).await.unwrap_or_else(|| {
                panic!(
                    "variant compaction case {} was not executed",
                    case.id.as_str()
                )
            });
            assert_eq!(sample.scenario_id, case.id.as_str());
            assert!(!sample.physical_operations.is_empty());
        }
    }
}
