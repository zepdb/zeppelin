//! Task 9 — consumed read failures must fail loud, except verified WAL GC races.

mod common;

use common::server::{cleanup_ns, create_ns_api_with, start_test_server_with_compactor};

use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::types::{AttributeValue, VectorEntry};
use zeppelin::wal::fragment::WalFragment;
use zeppelin::wal::manifest::Manifest;
use zeppelin::wal::{WalReader, WalWriter};

const DIM: usize = 4;

fn one_cluster_flat_config() -> Config {
    let mut config = Config::load(None).unwrap();
    config.indexing.default_num_centroids = 1;
    config.indexing.default_nprobe = 1;
    config.indexing.quantization = QuantizationType::None;
    config.indexing.bitmap_index = false;
    config
}

fn vectors_with_tenant(count: usize, tenant: &str) -> Vec<Value> {
    (0..count)
        .map(|i| {
            json!({
                "id": format!("{tenant}_{i}"),
                "values": [i as f32, 0.0, 0.0, 0.0],
                "attributes": { "tenant": tenant },
            })
        })
        .collect()
}

async fn upsert_json(client: &reqwest::Client, base_url: &str, ns: &str, vectors: Vec<Value>) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "upsert failed");
}

fn active_segment(manifest: &Manifest) -> &zeppelin::wal::manifest::SegmentRef {
    let active = manifest.active_segment.as_ref().unwrap();
    manifest.segments.iter().find(|s| s.id == *active).unwrap()
}

fn cluster_key(ns: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{ns}/segments/{segment_id}/cluster_{cluster_idx}.bin")
}

fn attrs_key(ns: &str, segment_id: &str, cluster_idx: usize) -> String {
    format!("{ns}/segments/{segment_id}/attrs_{cluster_idx}.bin")
}

/// I1: if a compacted segment cluster blob referenced by the manifest is
/// missing, the query must fail with a 5xx rather than returning partial 200
/// results.
#[tokio::test]
async fn test_missing_cluster_blob_fails_query() {
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(one_cluster_flat_config())).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({ "dimensions": DIM, "distance_metric": "euclidean" }),
    )
    .await;

    upsert_json(&client, &base_url, &ns, vectors_with_tenant(8, "hot")).await;
    compactor.compact(&ns).await.unwrap();

    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let seg = active_segment(&manifest);
    let owner = seg.cluster_owner(0);
    let key = cluster_key(&ns, owner, 0);
    harness.store.delete(&key).await.unwrap();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 4,
            "nprobe": 1,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(
        status.is_server_error(),
        "missing consumed cluster blob must fail with 5xx, got {status}: {body}"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

/// I3: a filtered query that needs attrs must fail on an attrs read error.
/// It must never reinterpret the read failure as "no attrs match" and return
/// an empty 200.
#[tokio::test]
async fn test_missing_attrs_blob_fails_filtered_query() {
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(one_cluster_flat_config())).await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({ "dimensions": DIM, "distance_metric": "euclidean" }),
    )
    .await;

    upsert_json(&client, &base_url, &ns, vectors_with_tenant(8, "hot")).await;
    compactor.compact(&ns).await.unwrap();

    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let seg = active_segment(&manifest);
    let owner = seg.cluster_owner(0);
    let key = attrs_key(&ns, owner, 0);
    harness.store.delete(&key).await.unwrap();

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": [0.0, 0.0, 0.0, 0.0],
            "top_k": 4,
            "nprobe": 1,
            "consistency": "strong",
            "filter": { "op": "eq", "field": "tenant", "value": "hot" },
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(
        status.is_server_error(),
        "missing consumed attrs blob must fail with 5xx, got {status}: {body}"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

fn gc_race_metric_value(namespace: &str) -> f64 {
    prometheus::gather()
        .into_iter()
        .find(|family| family.name() == "zeppelin_wal_fragment_gc_race_skipped_total")
        .and_then(|family| {
            family
                .get_metric()
                .iter()
                .find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "namespace" && label.value() == namespace)
                })
                .map(|metric| metric.get_counter().get_value())
        })
        .unwrap_or(0.0)
}

/// I2/I4: a missing WAL fragment is tolerated only when a fresh manifest
/// confirms that exact fragment ref is no longer live. The tolerated skip must
/// increment the namespace-labeled GC-race metric.
#[tokio::test]
async fn test_wal_fragment_notfound_tolerated_only_after_manifest_reread_and_metric() {
    zeppelin::metrics::init();

    let harness = common::harness::TestHarness::new().await;
    let ns = harness.key("wal-gc-race");
    Manifest::new().write(&harness.store, &ns).await.unwrap();

    let writer = WalWriter::new(harness.store.clone());
    let vecs = vec![VectorEntry {
        id: "raced".to_string(),
        values: vec![1.0, 0.0, 0.0, 0.0],
        attributes: Some(
            [(
                "tenant".to_string(),
                AttributeValue::String("hot".to_string()),
            )]
            .into_iter()
            .collect(),
        ),
    }];
    writer.append(&ns, vecs, vec![]).await.unwrap();

    let old_manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let old_refs = old_manifest.uncompacted_fragments().to_vec();
    assert_eq!(old_refs.len(), 1);

    let compactor = zeppelin::compaction::Compactor::new(
        harness.store.clone(),
        WalReader::new(harness.store.clone()),
        Default::default(),
        one_cluster_flat_config().indexing,
    );
    compactor.compact(&ns).await.unwrap();

    let new_manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert!(
        new_manifest.uncompacted_fragments().is_empty(),
        "new manifest must confirm the old fragment was compacted away"
    );

    let frag_key = WalFragment::s3_key(&ns, &old_refs[0].id);
    harness.store.delete(&frag_key).await.unwrap();

    let before = gc_race_metric_value(&ns);
    let reader = WalReader::new(harness.store.clone());
    let fragments = reader
        .read_fragments_from_refs_unchecked(&ns, &old_refs, None)
        .await
        .unwrap();
    let after = gc_race_metric_value(&ns);

    assert!(
        fragments.is_empty(),
        "verified compacted-away fragment should be skipped"
    );
    assert_eq!(
        after,
        before + 1.0,
        "verified GC-race skip must increment metric exactly once"
    );

    harness.cleanup().await;
}

/// I2 boundary (the data-loss guard): a fragment NotFound whose id is STILL
/// referenced by the live manifest — i.e. NOT compacted away — must be an
/// ERROR, never a silent skip, and must NOT touch the GC-race metric. This is
/// the line between "tolerated race" and "silent data loss wearing a GC-race
/// costume". Exercised on BOTH the checked and unchecked read paths (they
/// share `finish_fragment_results`).
#[tokio::test]
async fn test_wal_fragment_notfound_still_referenced_is_error_not_skip() {
    zeppelin::metrics::init();

    let harness = common::harness::TestHarness::new().await;
    let ns = harness.key("wal-notfound-live");
    Manifest::new().write(&harness.store, &ns).await.unwrap();

    let writer = WalWriter::new(harness.store.clone());
    writer
        .append(
            &ns,
            vec![VectorEntry {
                id: "live".to_string(),
                values: vec![1.0, 0.0, 0.0, 0.0],
                attributes: None,
            }],
            vec![],
        )
        .await
        .unwrap();

    // No compaction: the manifest STILL references this fragment.
    let manifest = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let refs = manifest.uncompacted_fragments().to_vec();
    assert_eq!(refs.len(), 1);

    // Delete the fragment blob out from under the still-live manifest ref.
    let frag_key = WalFragment::s3_key(&ns, &refs[0].id);
    harness.store.delete(&frag_key).await.unwrap();

    let before = gc_race_metric_value(&ns);
    let reader = WalReader::new(harness.store.clone());

    // Unchecked path must error.
    let unchecked = reader
        .read_fragments_from_refs_unchecked(&ns, &refs, None)
        .await;
    assert!(
        unchecked.is_err(),
        "a NotFound on a still-referenced fragment must ERROR (unchecked), got {unchecked:?}"
    );
    // Checked path must error too.
    let checked = reader.read_fragments_from_refs(&ns, &refs, None).await;
    assert!(
        checked.is_err(),
        "a NotFound on a still-referenced fragment must ERROR (checked), got {checked:?}"
    );

    assert_eq!(
        gc_race_metric_value(&ns),
        before,
        "an errored (non-tolerated) NotFound must NOT increment the GC-race metric"
    );

    harness.cleanup().await;
}
