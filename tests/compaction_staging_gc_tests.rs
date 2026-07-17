mod common;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use zeppelin::compaction::background::{
    compact_namespace_under_lease_with_lifecycle, CompactionLifecycle,
};
use zeppelin::compaction::gc::{active_staged_keys, reachable_keys_with_staging};
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::error::ZeppelinError;
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::VectorEntry;
use zeppelin::wal::manifest::Manifest;
use zeppelin::wal::{LeaseManager, WalReader, WalWriter};

use common::harness::TestHarness;
use common::vectors::random_vectors;

fn prefixed_vectors(prefix: &str, n: usize, dims: usize) -> Vec<VectorEntry> {
    random_vectors(n, dims)
        .into_iter()
        .enumerate()
        .map(|(i, mut vector)| {
            vector.id = format!("{prefix}_{i}");
            vector
        })
        .collect()
}

fn staging_compactor(
    store: &ZeppelinStore,
    pre_cas_delay: Duration,
    upload_window_secs: u64,
) -> Compactor {
    let mut compactor = Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        IndexingConfig {
            default_num_centroids: 2,
            kmeans_max_iterations: 5,
            ..Default::default()
        },
        Duration::from_secs(upload_window_secs),
    );
    compactor.set_test_pre_cas_delay(pre_cas_delay);
    compactor
}

async fn wait_for_segment_uploads(store: &ZeppelinStore, namespace: &str) -> Vec<String> {
    let prefix = format!("{namespace}/segments/");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let keys = store.list_prefix(&prefix).await.unwrap();
        if !keys.is_empty() {
            return keys;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for compaction segment uploads"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_active_staging(store: &ZeppelinStore, namespace: &str) -> BTreeSet<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let staged = active_staged_keys(store, namespace).await.unwrap();
        if !staged.is_empty() {
            return staged;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for active compaction staging"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn gc_sweep_age_floor(
    store: &ZeppelinStore,
    namespace: &str,
    horizon: Duration,
) -> Vec<String> {
    let manifest = Manifest::read(store, namespace)
        .await
        .unwrap()
        .expect("manifest must exist");
    let staged = active_staged_keys(store, namespace).await.unwrap();
    let reachable = reachable_keys_with_staging(namespace, &manifest, &staged)
        .expect("manifest reachability must resolve");
    let mut deleted = Vec::new();
    for key in store
        .list_prefix(&format!("{namespace}/segments/"))
        .await
        .unwrap()
    {
        if reachable.contains(&key) {
            continue;
        }
        if horizon > Duration::ZERO {
            let meta = store.head(&key).await.unwrap();
            let Ok(age) = Utc::now()
                .signed_duration_since(meta.last_modified)
                .to_std()
            else {
                continue;
            };
            if age < horizon {
                continue;
            }
        }
        store.delete(&key).await.unwrap();
        deleted.push(key);
    }
    deleted
}

async fn assert_manifest_segment_objects_exist(store: &ZeppelinStore, namespace: &str) {
    let manifest = Manifest::read(store, namespace)
        .await
        .unwrap()
        .expect("manifest must exist");
    for segment in &manifest.segments {
        let mut keys = Vec::new();
        if segment.hierarchical {
            keys.push(format!(
                "{namespace}/segments/{}/tree_meta.json",
                segment.id
            ));
        } else {
            keys.push(format!("{namespace}/segments/{}/centroids.bin", segment.id));
        }
        if let Some(sketch) = &segment.sketch {
            keys.push(sketch.key.clone());
        }
        if let Some(bootstrap) = &segment.bootstrap {
            keys.push(bootstrap.key.clone());
        }
        if let Some(membership) = &segment.membership {
            keys.push(membership.key.clone());
        }
        if segment.cluster_objects.is_empty() {
            for cluster_idx in 0..segment.cluster_count {
                keys.push(format!(
                    "{namespace}/segments/{}/cluster_{cluster_idx}.bin",
                    segment.cluster_owner(cluster_idx)
                ));
            }
        } else {
            keys.extend(
                segment
                    .cluster_objects
                    .iter()
                    .map(|object| object.key.clone()),
            );
        }
        for cluster_idx in 0..segment.cluster_count {
            keys.push(format!(
                "{namespace}/segments/{}/attrs_{cluster_idx}.bin",
                segment.cluster_owner(cluster_idx)
            ));
        }

        for key in keys {
            assert!(
                store.exists(&key).await.unwrap(),
                "manifest references missing uploaded segment object after GC race: {key}"
            );
        }
    }
}

#[tokio::test]
async fn test_gc_does_not_delete_active_staged_compaction_uploads() {
    let harness = TestHarness::new().await;
    let ns = harness.key("gc-staging-break-b");
    let store = harness.store.clone();
    common::write_active_namespace_metadata(
        &store,
        &ns,
        16,
        zeppelin::types::DistanceMetric::Euclidean,
    )
    .await;
    Manifest::new().write(&store, &ns).await.unwrap();
    WalWriter::new(store.clone())
        .append(&ns, prefixed_vectors("break_b", 24, 16), vec![])
        .await
        .unwrap();

    let lease_manager = Arc::new(LeaseManager::new(
        store.clone(),
        "staging-node".to_string(),
        Duration::from_secs(10),
    ));
    let compactor = Arc::new(staging_compactor(&store, Duration::from_secs(2), 30));
    let compaction_lifecycle = CompactionLifecycle::new();
    let compaction = {
        let compactor = Arc::clone(&compactor);
        let lease_manager = Arc::clone(&lease_manager);
        let compaction_lifecycle = compaction_lifecycle.clone();
        let ns = ns.clone();
        tokio::spawn(async move {
            compact_namespace_under_lease_with_lifecycle(
                &compactor,
                &lease_manager,
                &ns,
                &HashMap::new(),
                zeppelin::wal::FragmentCachePolicy::Bypass,
                &compaction_lifecycle,
            )
            .await
        })
    };

    let uploaded = wait_for_segment_uploads(&store, &ns).await;
    wait_for_active_staging(&store, &ns).await;
    let deleted = gc_sweep_age_floor(&store, &ns, Duration::ZERO).await;

    assert!(
        deleted.is_empty(),
        "R4-I1 violated: active staged uploads were deleted: {deleted:?}; uploaded={uploaded:?}"
    );
    compaction.await.unwrap().unwrap();
    compaction_lifecycle
        .close_and_abort_heartbeats()
        .await
        .unwrap();
    assert!(
        active_staged_keys(&store, &ns).await.unwrap().is_empty(),
        "R4-I2: staging entry must clear after successful CAS"
    );
    assert_manifest_segment_objects_exist(&store, &ns).await;

    harness.cleanup().await;
}

#[tokio::test]
async fn test_stolen_lease_staging_drops_and_orphans_obey_horizon() {
    let harness = TestHarness::new().await;
    let ns = harness.key("gc-staging-stolen");
    let store = harness.store.clone();
    common::write_active_namespace_metadata(
        &store,
        &ns,
        16,
        zeppelin::types::DistanceMetric::Euclidean,
    )
    .await;
    Manifest::new().write(&store, &ns).await.unwrap();
    WalWriter::new(store.clone())
        .append(&ns, prefixed_vectors("stolen_staging", 24, 16), vec![])
        .await
        .unwrap();

    let lease_manager = Arc::new(LeaseManager::new(
        store.clone(),
        "victim-node".to_string(),
        Duration::from_secs(2),
    ));
    let compactor = Arc::new(staging_compactor(&store, Duration::from_secs(5), 30));
    let compaction_lifecycle = CompactionLifecycle::new();
    let compaction = {
        let compactor = Arc::clone(&compactor);
        let lease_manager = Arc::clone(&lease_manager);
        let compaction_lifecycle = compaction_lifecycle.clone();
        let ns = ns.clone();
        tokio::spawn(async move {
            compact_namespace_under_lease_with_lifecycle(
                &compactor,
                &lease_manager,
                &ns,
                &HashMap::new(),
                zeppelin::wal::FragmentCachePolicy::Bypass,
                &compaction_lifecycle,
            )
            .await
        })
    };

    let uploaded = wait_for_segment_uploads(&store, &ns).await;
    wait_for_active_staging(&store, &ns).await;

    let lease_key = format!("{ns}/lease.json");
    let mut lease_json: serde_json::Value =
        serde_json::from_slice(&store.get(&lease_key).await.unwrap()).unwrap();
    let victim_token = lease_json["fencing_token"].as_u64().unwrap();
    lease_json["holder_id"] = serde_json::Value::from("thief-node");
    lease_json["fencing_token"] = serde_json::Value::from(victim_token + 1);
    lease_json["expires_at"] =
        serde_json::Value::from((Utc::now() + chrono::Duration::seconds(60)).to_rfc3339());
    store
        .put(
            &lease_key,
            Bytes::from(serde_json::to_vec(&lease_json).unwrap()),
        )
        .await
        .unwrap();

    assert!(
        active_staged_keys(&store, &ns).await.unwrap().is_empty(),
        "R4-I3: staging from a stolen lease must not pin objects under the new active lease"
    );
    let inside_window_deleted = gc_sweep_age_floor(&store, &ns, Duration::from_secs(60)).await;
    assert!(
        inside_window_deleted.is_empty(),
        "R4-I2: abandoned uploads must not be collected inside the normal horizon"
    );
    for key in &uploaded {
        assert!(
            store.exists(key).await.unwrap(),
            "orphan disappeared inside horizon: {key}"
        );
    }

    let after_horizon_deleted = gc_sweep_age_floor(&store, &ns, Duration::ZERO).await;
    assert!(
        !after_horizon_deleted.is_empty(),
        "R4-I2: abandoned staged uploads become ordinary collectible orphans after horizon"
    );

    let result = compaction.await.unwrap();
    compaction_lifecycle
        .close_and_abort_heartbeats()
        .await
        .unwrap();
    assert!(
        matches!(
            result,
            Err(ZeppelinError::LeaseExpired { .. }) | Err(ZeppelinError::FencingTokenStale { .. })
        ),
        "stolen lease compaction must abort before CAS, got {result:?}"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_compaction_aborts_before_cas_when_upload_window_exceeds_horizon() {
    let harness = TestHarness::new().await;
    let ns = harness.key("gc-upload-window-abort");
    let store = harness.store.clone();
    common::write_active_namespace_metadata(
        &store,
        &ns,
        16,
        zeppelin::types::DistanceMetric::Euclidean,
    )
    .await;
    Manifest::new().write(&store, &ns).await.unwrap();
    let (fragment_ref, _) = WalWriter::new(store.clone())
        .append(&ns, prefixed_vectors("window_abort", 24, 16), vec![])
        .await
        .unwrap();

    let compactor = staging_compactor(&store, Duration::from_secs(2), 1);
    let result = compactor.compact(&ns).await;
    assert!(
        result.is_err(),
        "R4-I4/19C: compaction must abort before CAS when upload window exceeds horizon"
    );

    let manifest = Manifest::read(&store, &ns).await.unwrap().unwrap();
    assert!(
        manifest.fragments.iter().any(|f| f.id == fragment_ref.id),
        "aborted upload-window compaction must leave WAL fragment in manifest"
    );
    assert!(
        manifest.segments.is_empty(),
        "aborted upload-window compaction must not commit a segment"
    );

    let orphan_keys = store.list_prefix(&format!("{ns}/segments/")).await.unwrap();
    assert!(
        !orphan_keys.is_empty(),
        "aborted upload-window compaction leaves uploaded objects as GC orphans"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_active_staged_keys_excludes_expired_lease_staging() {
    let harness = TestHarness::new().await;
    let ns = harness.key("gc-staging-expired");
    let store = harness.store.clone();
    let staged_key = format!("{ns}/segments/seg_expired/centroids.bin");
    let lease = serde_json::json!({
        "holder_id": "expired-node",
        "fencing_token": 7u64,
        "acquired_at": (Utc::now() - chrono::Duration::seconds(120)).to_rfc3339(),
        "expires_at": (Utc::now() - chrono::Duration::seconds(60)).to_rfc3339(),
        "etag": "",
    });
    store
        .put(
            &format!("{ns}/lease.json"),
            Bytes::from(serde_json::to_vec(&lease).unwrap()),
        )
        .await
        .unwrap();
    store
        .put(
            &format!("{ns}/_staging/7.json"),
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "fencing_token": 7u64,
                    "keys": BTreeSet::from([staged_key.clone()]),
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap();

    let staged = active_staged_keys(&store, &ns).await.unwrap();
    assert!(
        staged.is_empty(),
        "R4-I3: expired lease staging must not pin uploaded objects forever"
    );

    harness.cleanup().await;
}
