#![cfg(feature = "branching-test-support")]

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::counting::counting_store;
use common::fault_injection::{pause_next_get_matching, toggle_get_failure_matching};
use common::harness::TestHarness;
use zeppelin::cache::hydration::{
    HydrationConfig, HydrationTarget, SegmentHydrator, SessionWindowPolicy,
};
use zeppelin::cache::DiskCache;
use zeppelin::compaction::Compactor;
use zeppelin::config::{
    BranchingConfig, CompactionConfig, IndexingConfig, DEFAULT_RERANK_COALESCE_GAP_BYTES,
};
use zeppelin::fts::rank_by::RankBy;
use zeppelin::fts::FtsFieldConfig;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::branching::test_support::{
    activate_fork_for_test, publish_deletion_fence, SyntheticForeignQuerySpec,
};
use zeppelin::namespace::manager::NamespaceIndexConfig;
use zeppelin::namespace::{NamespaceId, NamespaceManager};
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, VectorEntry};
use zeppelin::wal::{Manifest, WalReader, WalWriter};

fn flat_compactor(store: &zeppelin::storage::ZeppelinStore) -> Compactor {
    configured_compactor(
        store,
        IndexingConfig {
            default_num_centroids: 1,
            kmeans_max_iterations: 5,
            quantization: QuantizationType::None,
            bitmap_index: false,
            ..Default::default()
        },
    )
}

fn configured_compactor(
    store: &zeppelin::storage::ZeppelinStore,
    indexing: IndexingConfig,
) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        indexing,
        common::default_gc_upload_window(),
    )
}

fn source_vectors() -> Vec<VectorEntry> {
    (0..32)
        .map(|index| VectorEntry {
            id: format!("source-{index:02}"),
            values: vec![index as f32, 0.0, 0.0, 0.0],
            attributes: None,
        })
        .collect()
}

fn hydration_indexing_config() -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: 1,
        kmeans_max_iterations: 5,
        quantization: QuantizationType::None,
        bitmap_index: false,
        ..Default::default()
    }
}

async fn activated_foreign_hydration_target(
    harness: &TestHarness,
    source_suffix: &str,
    target_suffix: &str,
) -> (String, String, HydrationTarget) {
    let source = harness.artifact_origin_namespace(source_suffix);
    let target = harness.artifact_origin_namespace(target_suffix);
    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Euclidean)
        .await
        .unwrap();
    WalWriter::new(harness.store.clone())
        .append(&source, source_vectors(), vec![])
        .await
        .unwrap();
    configured_compactor(&harness.store, hydration_indexing_config())
        .compact(&source)
        .await
        .unwrap();
    activate_fork_for_test(
        harness.store.clone(),
        NamespaceId::new(source.clone()).unwrap(),
        NamespaceId::new(target.clone()).unwrap(),
        hydration_indexing_config(),
        BranchingConfig {
            enabled: true,
            ..BranchingConfig::default()
        },
    )
    .await
    .unwrap();
    let manifest = Manifest::read(&harness.store, &target)
        .await
        .unwrap()
        .expect("activated branch must have an authoritative manifest");
    let hydration_target = HydrationTarget::from_active_manifest(&manifest)
        .unwrap()
        .expect("activated compacted branch must expose a hydration target");
    assert_eq!(hydration_target.logical_namespace(), target);
    assert_eq!(hydration_target.physical_namespace(), source);
    assert_eq!(hydration_target.manifest_generation(), manifest.version());
    (source, target, hydration_target)
}

fn hydration_config(job_timeout: Duration) -> HydrationConfig {
    HydrationConfig {
        parallelism: 2,
        max_segment_fraction: 1.0,
        max_retries: 0,
        retry_backoff: Duration::from_millis(1),
        job_timeout,
    }
}

fn indexed_vectors(count: usize) -> Vec<VectorEntry> {
    (0..count)
        .map(|index| {
            let mut attributes = HashMap::new();
            attributes.insert(
                "status".to_string(),
                AttributeValue::String(if index % 2 == 0 {
                    "active".to_string()
                } else {
                    "inactive".to_string()
                }),
            );
            attributes.insert(
                "content".to_string(),
                AttributeValue::String(if index % 3 == 0 {
                    "rust systems programming".to_string()
                } else {
                    "python data processing".to_string()
                }),
            );
            attributes.insert("ordinal".to_string(), AttributeValue::Integer(index as i64));
            VectorEntry {
                id: format!("indexed-{index:03}"),
                values: vec![index as f32 / count as f32, 0.0, 0.0, 0.0],
                attributes: Some(attributes),
            }
        })
        .collect()
}

fn fts_configs() -> HashMap<String, FtsFieldConfig> {
    HashMap::from([(
        "content".to_string(),
        FtsFieldConfig {
            stemming: false,
            remove_stopwords: false,
            ..Default::default()
        },
    )])
}

async fn create_configured_namespace(
    harness: &TestHarness,
    namespace: &str,
    indexing: &IndexingConfig,
    fts: HashMap<String, FtsFieldConfig>,
) {
    NamespaceManager::new(harness.store.clone())
        .create_with_fts_and_index_config(
            namespace,
            4,
            DistanceMetric::Euclidean,
            fts,
            Some(NamespaceIndexConfig::from_indexing_config(indexing)),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn synthetic_target_queries_foreign_flat_segment_without_opening_admission() {
    assert_eq!(
        std::env::var("TEST_BACKEND").as_deref(),
        Ok("minio"),
        "artifact-origin routing requires real MinIO"
    );

    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("source");
    let target = harness.artifact_origin_namespace("target");

    NamespaceManager::new(harness.store.clone())
        .create(&source, 4, DistanceMetric::Euclidean)
        .await
        .unwrap();
    WalWriter::new(harness.store.clone())
        .append(&source, source_vectors(), vec![])
        .await
        .unwrap();
    flat_compactor(&harness.store)
        .compact(&source)
        .await
        .unwrap();

    let view = harness
        .synthetic_foreign_origin_view(&source, &target)
        .await
        .unwrap();
    let response = view
        .query_ann(
            &[0.0, 0.0, 0.0, 0.0],
            4,
            1,
            DistanceMetric::Euclidean,
            ConsistencyLevel::Strong,
        )
        .await
        .unwrap();

    assert_eq!(response.ids.first().map(String::as_str), Some("source-00"));
    assert_eq!(response.scanned_segments, 1);
    assert_eq!(response.scanned_fragments, 0);
    let admission_error = view.production_admission_result().unwrap_err();
    assert!(
        admission_error
            .to_string()
            .contains("foreign artifact origin admission"),
        "production decode must stay fail-closed: {admission_error}"
    );

    let reachable = view.reachable_artifact_keys().unwrap();
    let missing_key = reachable
        .iter()
        .find(|key| {
            key.ends_with("/bootstrap.bin")
                || key.ends_with("/centroids.bin")
                || key.contains("/cluster_")
        })
        .expect("the manifest must reference one required ANN object")
        .clone();
    harness.store.delete(&missing_key).await.unwrap();

    let missing_error = view
        .query_ann(
            &[0.0, 0.0, 0.0, 0.0],
            4,
            1,
            DistanceMetric::Euclidean,
            ConsistencyLevel::Strong,
        )
        .await
        .expect_err("a missing foreign object must fail the complete query");
    assert!(
        missing_error.to_string().contains(&missing_key),
        "missing-object error must name the physical source key {missing_key}: {missing_error}"
    );

    harness.cleanup_artifact_origin_namespace(&source).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn synthetic_target_routes_flat_sq_bitmap_attrs_bm25_cache_and_reachability() {
    assert_eq!(std::env::var("TEST_BACKEND").as_deref(), Ok("minio"));

    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("indexed-source");
    let target = harness.artifact_origin_namespace("indexed-target");
    let indexing = IndexingConfig {
        default_num_centroids: 2,
        kmeans_max_iterations: 5,
        quantization: QuantizationType::Scalar,
        bitmap_index: true,
        fts_index: true,
        ..Default::default()
    };
    let fts = fts_configs();
    create_configured_namespace(&harness, &source, &indexing, fts.clone()).await;
    WalWriter::new(harness.store.clone())
        .append(&source, indexed_vectors(96), vec![])
        .await
        .unwrap();
    configured_compactor(&harness.store, indexing)
        .compact_with_fts(&source, None, &fts)
        .await
        .unwrap();

    let view = harness
        .synthetic_foreign_origin_view(&source, &target)
        .await
        .unwrap();
    let projected_fields = vec!["status".to_string()];
    let fetched = view
        .fetch_by_ids(
            &["indexed-000".to_string(), "missing-id".to_string()],
            ConsistencyLevel::Strong,
            true,
            true,
            Some(&projected_fields),
        )
        .await
        .unwrap();
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].id, "indexed-000");
    assert_eq!(
        fetched.records[0].values.as_deref(),
        Some([0.0, 0.0, 0.0, 0.0].as_slice())
    );
    assert_eq!(
        fetched.records[0]
            .attributes
            .as_ref()
            .and_then(|attributes| attributes.get("status")),
        Some(&AttributeValue::String("active".to_string()))
    );
    assert_eq!(fetched.records[0].attributes.as_ref().unwrap().len(), 1);
    assert_eq!(fetched.missing, vec!["missing-id"]);
    let active = Filter::Eq {
        field: "status".to_string(),
        value: AttributeValue::String("active".to_string()),
    };
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().join("shared"), 64 * 1024 * 1024).unwrap(),
    );
    let wal_reader = WalReader::new(harness.store.clone());

    execute_query(QueryParams {
        store: &harness.store,
        wal_reader: &wal_reader,
        namespace: &source,
        query: &[0.0, 0.0, 0.0, 0.0],
        top_k: 8,
        nprobe: 2,
        filter: Some(&active),
        consistency: ConsistencyLevel::Eventual,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 3,
        rerank_coalesce_gap_bytes: DEFAULT_RERANK_COALESCE_GAP_BYTES,
        cache: Some(&cache),
        manifest_cache: None,
        include_attributes: true,
    })
    .await
    .unwrap();

    let ann = view
        .query_ann_with_options(
            &[0.0, 0.0, 0.0, 0.0],
            8,
            2,
            DistanceMetric::Euclidean,
            ConsistencyLevel::Eventual,
            Some(&active),
            true,
            Some(&cache),
            true,
        )
        .await
        .unwrap();
    assert!(ann.debug_present);
    assert!(
        ann.cache_hits > 0,
        "target should reuse source physical cache entries"
    );
    assert!(ann.results.iter().all(|result| {
        result
            .attributes
            .as_ref()
            .and_then(|attrs| attrs.get("status"))
            == Some(&AttributeValue::String("active".to_string()))
    }));
    let lexical = view
        .query_bm25(
            &RankBy::Bm25 {
                field: "content".to_string(),
                query: "rust programming".to_string(),
            },
            &fts,
            10,
            Some(&active),
            ConsistencyLevel::Eventual,
            true,
            true,
        )
        .await
        .unwrap();
    assert!(!lexical.results.is_empty());
    assert!(lexical.debug_present);
    let reachable = view.reachable_artifact_keys().unwrap();
    assert!(!reachable.is_empty());
    assert!(reachable
        .iter()
        .all(|key| key.starts_with(&format!("{source}/"))));
    for key in &reachable {
        let error = view
            .classify_target_sweep_candidate(key.clone())
            .expect_err("foreign reachable artifacts must never enter the target sweep");
        assert!(error.to_string().contains("cannot delete unowned key"));
    }
    let target_owned_candidate = format!("{target}/segments/local-probe/manifest.json");
    assert_eq!(
        view.classify_target_sweep_candidate(target_owned_candidate.clone())
            .unwrap(),
        target_owned_candidate
    );

    let corrupt = view
        .clone()
        .with_corrupt_active_segment_origin()
        .query_ann(
            &[0.0, 0.0, 0.0, 0.0],
            4,
            2,
            DistanceMetric::Euclidean,
            ConsistencyLevel::Eventual,
        )
        .await
        .unwrap_err();
    assert!(corrupt.to_string().contains("artifact origin index"));

    harness.cleanup().await;
}

#[tokio::test]
async fn synthetic_target_hybrid_batch_shares_foreign_snapshot_and_isolates_entry_errors() {
    assert_eq!(std::env::var("TEST_BACKEND").as_deref(), Ok("minio"));

    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("hybrid-source");
    let target = harness.artifact_origin_namespace("hybrid-target");
    let indexing = IndexingConfig {
        default_num_centroids: 2,
        kmeans_max_iterations: 5,
        quantization: QuantizationType::None,
        fts_index: true,
        ..Default::default()
    };
    let fts = fts_configs();
    create_configured_namespace(&harness, &source, &indexing, fts.clone()).await;
    WalWriter::new(harness.store.clone())
        .append(&source, indexed_vectors(96), vec![])
        .await
        .unwrap();
    configured_compactor(&harness.store, indexing)
        .compact_with_fts(&source, None, &fts)
        .await
        .unwrap();

    let view = harness
        .synthetic_foreign_origin_view(&source, &target)
        .await
        .unwrap();
    let rank_by = RankBy::Bm25 {
        field: "content".to_string(),
        query: "rust programming".to_string(),
    };
    let entries = view
        .query_batch(
            &[
                SyntheticForeignQuerySpec::Hybrid {
                    query: vec![0.0, 0.0, 0.0, 0.0],
                    rank_by: rank_by.clone(),
                    top_k: 12,
                    nprobe: 2,
                },
                SyntheticForeignQuerySpec::Ann {
                    query: vec![0.0, 0.0, 0.0],
                    top_k: 4,
                    nprobe: 2,
                },
                SyntheticForeignQuerySpec::Bm25 { rank_by, top_k: 6 },
            ],
            &fts,
            DistanceMetric::Euclidean,
            ConsistencyLevel::Eventual,
            true,
            true,
        )
        .await;

    assert_eq!(entries.len(), 3);
    let hybrid = entries[0].as_ref().expect("hybrid entry must succeed");
    assert!(!hybrid.results.is_empty());
    assert!(hybrid.debug_present);
    assert_eq!(hybrid.scanned_segments, 2);
    let middle_error = entries[1]
        .as_ref()
        .expect_err("one malformed batch entry must remain an error in place");
    assert!(middle_error.to_string().contains("dimension mismatch"));

    let lexical = entries[2]
        .as_ref()
        .expect("a later entry must execute after an earlier entry error");
    assert!(!lexical.results.is_empty());
    assert!(lexical.debug_present);
    harness.cleanup().await;
}

#[tokio::test]
async fn synthetic_target_routes_hierarchical_pq_tree_nodes_and_codebooks() {
    assert_eq!(std::env::var("TEST_BACKEND").as_deref(), Ok("minio"));

    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("hierarchical-source");
    let target = harness.artifact_origin_namespace("hierarchical-target");
    let indexing = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 5,
        quantization: QuantizationType::Product,
        pq_m: 4,
        hierarchical: true,
        leaf_size: Some(12),
        ..Default::default()
    };
    create_configured_namespace(&harness, &source, &indexing, HashMap::new()).await;
    WalWriter::new(harness.store.clone())
        .append(&source, indexed_vectors(192), vec![])
        .await
        .unwrap();
    configured_compactor(&harness.store, indexing)
        .compact(&source)
        .await
        .unwrap();

    let result = harness
        .synthetic_foreign_origin_view(&source, &target)
        .await
        .unwrap()
        .query_ann(
            &[0.0, 0.0, 0.0, 0.0],
            10,
            4,
            DistanceMetric::Euclidean,
            ConsistencyLevel::Eventual,
        )
        .await
        .unwrap();

    assert!(!result.ids.is_empty());
    assert_eq!(result.scanned_segments, 1);
    harness.cleanup().await;
}

#[tokio::test]
async fn activated_branch_hydrates_physical_source_keys_under_logical_target_identity() {
    assert_eq!(std::env::var("TEST_BACKEND").as_deref(), Ok("minio"));

    let harness = TestHarness::new().await;
    let (source, _target, hydration_target) =
        activated_foreign_hydration_target(&harness, "hydration-source", "hydration-target").await;
    let store_key = hydration_target
        .segment()
        .cluster_objects
        .first()
        .expect("full compaction must publish a grouped cluster object")
        .key
        .clone();
    assert!(store_key.starts_with(&format!("{source}/")));
    let cache_key = hydration_target.cache_key(&store_key);

    let cache_dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().join("hydration"), 64 * 1024 * 1024)
            .unwrap(),
    );
    let hydrator = SegmentHydrator::start(
        harness.store.clone(),
        Arc::clone(&cache),
        Arc::new(SessionWindowPolicy::new(1, Duration::from_secs(60)).unwrap()),
        hydration_config(Duration::from_secs(30)),
    );
    hydrator.request_hydration(&hydration_target);

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if cache.get(&cache_key).await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("foreign-origin hydration must populate the physical cache key");

    harness.cleanup().await;
}

#[tokio::test]
async fn queued_branch_hydration_fenced_before_authority_read_never_touches_source() {
    assert_eq!(std::env::var("TEST_BACKEND").as_deref(), Ok("minio"));
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let (source, target, hydration_target) = activated_foreign_hydration_target(
        &harness,
        "hydration-fence-source",
        "hydration-fence-target",
    )
    .await;
    let first_cluster = hydration_target
        .segment()
        .cluster_objects
        .first()
        .expect("foreign hydration fixture must publish a grouped cluster object")
        .key
        .clone();

    let (counted_store, counter) = counting_store(&harness.store);
    let (paused_store, manifest_read) =
        pause_next_get_matching(&counted_store, Manifest::s3_key(&target));
    manifest_read.arm();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().join("hydration"), 64 * 1024 * 1024)
            .unwrap(),
    );
    let hydrator = SegmentHydrator::start(
        paused_store,
        Arc::clone(&cache),
        Arc::new(SessionWindowPolicy::new(1, Duration::from_secs(60)).unwrap()),
        hydration_config(Duration::from_secs(30)),
    );
    let hydration = {
        let hydrator = Arc::clone(&hydrator);
        let target = hydration_target.clone();
        tokio::spawn(async move { hydrator.request_hydration_and_wait_for_test(&target).await })
    };

    tokio::time::timeout(Duration::from_secs(15), manifest_read.wait_until_paused())
        .await
        .expect("hydration authority reread must reach the deterministic barrier");
    publish_deletion_fence(
        harness.store.clone(),
        &target,
        &format!("_audit/destruction/{}.json", uuid::Uuid::new_v4().simple()),
    )
    .await
    .unwrap();
    manifest_read.release();
    tokio::time::timeout(Duration::from_secs(15), hydration)
        .await
        .expect("fenced hydration job must terminate")
        .expect("hydration task must join")
        .expect("deterministic hydration enqueue must remain connected");

    assert_eq!(
        counter.gets_matching(&format!("{source}/")),
        0,
        "a target fenced before its authoritative reread must issue no source GET"
    );
    assert_eq!(
        counter.heads_matching(&format!("{source}/")),
        0,
        "a target fenced before its authoritative reread must issue no source HEAD"
    );
    assert_eq!(
        cache.get(&hydration_target.cache_key(&first_cluster)).await,
        None
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn branch_hydration_cancels_a_blocked_foreign_get_at_the_job_deadline() {
    assert_eq!(std::env::var("TEST_BACKEND").as_deref(), Ok("minio"));
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let (_source, _target, hydration_target) = activated_foreign_hydration_target(
        &harness,
        "hydration-deadline-source",
        "hydration-deadline-target",
    )
    .await;
    let first_cluster = hydration_target
        .segment()
        .cluster_objects
        .first()
        .expect("foreign hydration fixture must publish a grouped cluster object")
        .key
        .clone();
    let (paused_store, physical_read) =
        pause_next_get_matching(&harness.store, first_cluster.clone());
    physical_read.arm();
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().join("hydration"), 64 * 1024 * 1024)
            .unwrap(),
    );
    let hydrator = SegmentHydrator::start(
        paused_store,
        Arc::clone(&cache),
        Arc::new(SessionWindowPolicy::new(1, Duration::from_secs(60)).unwrap()),
        hydration_config(Duration::from_secs(2)),
    );
    let hydration = {
        let hydrator = Arc::clone(&hydrator);
        let target = hydration_target.clone();
        tokio::spawn(async move { hydrator.request_hydration_and_wait_for_test(&target).await })
    };

    tokio::time::timeout(Duration::from_secs(15), physical_read.wait_until_paused())
        .await
        .expect("foreign physical GET must reach the deterministic barrier");
    tokio::time::timeout(Duration::from_secs(15), hydration)
        .await
        .expect("hard hydration deadline must terminate the blocked job")
        .expect("hydration task must join")
        .expect("deterministic hydration enqueue must remain connected");
    physical_read.wait_until_exited().await;

    assert!(
        physical_read.was_cancelled_before_storage(),
        "the hard job deadline must cancel an in-flight physical GET"
    );
    assert_eq!(
        cache.get(&hydration_target.cache_key(&first_cluster)).await,
        None
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn hydration_retry_revalidates_fenced_branch_before_second_physical_attempt() {
    assert_eq!(std::env::var("TEST_BACKEND").as_deref(), Ok("minio"));
    zeppelin::metrics::init();

    let harness = TestHarness::new().await;
    let (source, target, hydration_target) = activated_foreign_hydration_target(
        &harness,
        "hydration-retry-fence-source",
        "hydration-retry-fence-target",
    )
    .await;
    let first_cluster = hydration_target
        .segment()
        .cluster_objects
        .first()
        .expect("foreign hydration fixture must publish a grouped cluster object")
        .key
        .clone();
    let (failing_store, first_get_failure) =
        toggle_get_failure_matching(&harness.store, first_cluster.clone());
    let (counted_store, counter) = counting_store(&failing_store);
    let (paused_store, physical_read) =
        pause_next_get_matching(&counted_store, first_cluster.clone());
    physical_read.arm();

    let cache_dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        DiskCache::new_with_max_bytes(cache_dir.path().join("hydration"), 64 * 1024 * 1024)
            .unwrap(),
    );
    let mut config = hydration_config(Duration::from_secs(15));
    config.parallelism = 1;
    config.max_retries = 1;
    config.retry_backoff = Duration::from_secs(1);
    let hydrator = SegmentHydrator::start(
        paused_store,
        Arc::clone(&cache),
        Arc::new(SessionWindowPolicy::new(1, Duration::from_secs(60)).unwrap()),
        config,
    );
    let hydration = {
        let hydrator = Arc::clone(&hydrator);
        let target = hydration_target.clone();
        tokio::spawn(async move { hydrator.request_hydration_and_wait_for_test(&target).await })
    };

    tokio::time::timeout(Duration::from_secs(15), physical_read.wait_until_paused())
        .await
        .expect("first foreign GET must reach the deterministic barrier");
    let source_prefix = format!("{source}/");
    let heads_before_fence = counter.heads_matching(&source_prefix);
    let gets_before_fence = counter.gets_matching(&source_prefix);
    assert!(
        heads_before_fence > 0,
        "the first attempt must finish physical planning before its GET pauses"
    );
    assert_eq!(
        gets_before_fence, 0,
        "the paused first GET must not have reached the counting/backend layer"
    );

    publish_deletion_fence(
        harness.store.clone(),
        &target,
        &format!("_audit/destruction/{}.json", uuid::Uuid::new_v4().simple()),
    )
    .await
    .unwrap();
    first_get_failure.enable();
    physical_read.release();

    tokio::time::timeout(Duration::from_secs(15), hydration)
        .await
        .expect("fenced hydration retry must terminate")
        .expect("hydration task must join")
        .expect("deterministic hydration enqueue must remain connected");
    assert_eq!(
        first_get_failure.failures_injected(),
        1,
        "only the already-admitted first physical GET may reach the failure boundary"
    );

    assert_eq!(
        counter.heads_matching(&source_prefix),
        heads_before_fence,
        "retry authority validation must stop before a second physical planning HEAD"
    );
    assert_eq!(
        counter.gets_matching(&source_prefix),
        gets_before_fence + 1,
        "only the already-started first physical GET may reach storage after fencing"
    );
    assert_eq!(
        cache.get(&hydration_target.cache_key(&first_cluster)).await,
        None,
        "a fenced retry must not populate the foreign artifact cache"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn synthetic_target_merges_foreign_and_local_wal_and_applies_local_tombstones() {
    assert_eq!(std::env::var("TEST_BACKEND").as_deref(), Ok("minio"));

    let harness = TestHarness::new().await;
    let source = harness.artifact_origin_namespace("mixed-source");
    let target = harness.artifact_origin_namespace("mixed-target");
    let manager = NamespaceManager::new(harness.store.clone());
    manager
        .create(&source, 4, DistanceMetric::Euclidean)
        .await
        .unwrap();
    let writer = WalWriter::new(harness.store.clone());
    writer
        .append(&source, source_vectors(), vec![])
        .await
        .unwrap();
    flat_compactor(&harness.store)
        .compact(&source)
        .await
        .unwrap();
    writer
        .append(
            &source,
            vec![VectorEntry {
                id: "foreign-wal".to_string(),
                values: vec![0.02, 0.0, 0.0, 0.0],
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    manager
        .create(&target, 4, DistanceMetric::Euclidean)
        .await
        .unwrap();
    writer
        .append(
            &target,
            vec![VectorEntry {
                id: "target-local-wal".to_string(),
                values: vec![0.01, 0.0, 0.0, 0.0],
                attributes: None,
            }],
            vec!["source-00".to_string()],
        )
        .await
        .unwrap();

    let result = harness
        .synthetic_foreign_origin_view(&source, &target)
        .await
        .unwrap()
        .query_ann(
            &[0.0, 0.0, 0.0, 0.0],
            10,
            1,
            DistanceMetric::Euclidean,
            ConsistencyLevel::Strong,
        )
        .await
        .unwrap();

    assert!(result.ids.iter().any(|id| id == "foreign-wal"));
    assert!(result.ids.iter().any(|id| id == "target-local-wal"));
    assert!(!result.ids.iter().any(|id| id == "source-00"));
    assert_eq!(result.scanned_fragments, 2);
    harness.cleanup().await;
}
