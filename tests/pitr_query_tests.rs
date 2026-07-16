mod common;

use chrono::{Duration, Utc};
use common::fault_injection::fail_put_once_matching;
use common::server::{
    cleanup_ns, create_ns_api_with, start_test_server, start_test_server_with_compactor,
};
use serde_json::{json, Value};
use ulid::Ulid;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::storage::ZeppelinStore;
use zeppelin::wal::manifest::FragmentRef;
use zeppelin::wal::Manifest;

async fn upsert(client: &reqwest::Client, base_url: &str, ns: &str, id: &str, values: [f32; 2]) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": id,
                "values": values,
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

async fn query(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    as_of: Option<&str>,
    vector: [f32; 2],
) -> (reqwest::StatusCode, Value) {
    query_with_consistency(client, base_url, ns, as_of, vector, "strong").await
}

async fn query_with_consistency(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    as_of: Option<&str>,
    vector: [f32; 2],
    consistency: &str,
) -> (reqwest::StatusCode, Value) {
    let mut request = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": vector,
            "top_k": 1,
            "consistency": consistency,
        }));
    if let Some(as_of) = as_of {
        request = request.query(&[("as_of", as_of)]);
    }
    let resp = request.send().await.unwrap();
    let status = resp.status();
    let body = resp.json::<Value>().await.unwrap();
    (status, body)
}

async fn rewrite_history_updated_at(
    store: &ZeppelinStore,
    ns: &str,
    manifest: &Manifest,
    updated_at: chrono::DateTime<Utc>,
) {
    let mut rewritten = manifest.clone();
    rewritten.updated_at = updated_at;
    store
        .put(
            &Manifest::history_key(ns, rewritten.version()),
            rewritten.to_bytes().unwrap(),
        )
        .await
        .unwrap();
}

fn first_id(body: &Value) -> &str {
    body["results"][0]["id"].as_str().unwrap()
}

fn fragment(id: u128) -> FragmentRef {
    FragmentRef {
        id: Ulid::from_parts(75_000, id),
        vector_count: 1,
        delete_count: 0,
        sequence_number: 0,
        size_bytes: 16,
    }
}

#[tokio::test]
async fn query_as_of_generation_timestamp_and_snapshot_read_historical_manifest() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    upsert(&client, &base_url, &ns, "old", [0.0, 0.0]).await;
    let g1 = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    let g1_version = g1.version();
    let g1_timestamp = g1.updated_at.to_rfc3339();

    let snapshot = client
        .put(format!(
            "{base_url}/v1/namespaces/{ns}/snapshots/before-new"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(snapshot.status(), 201);

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    upsert(&client, &base_url, &ns, "new", [10.0, 10.0]).await;

    let (live_status, live) = query(&client, &base_url, &ns, None, [10.0, 10.0]).await;
    assert_eq!(live_status, 200);
    assert_eq!(first_id(&live), "new");
    let current_timestamp = Manifest::read(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap()
        .updated_at
        .to_rfc3339();
    let (status, by_current_timestamp) = query(
        &client,
        &base_url,
        &ns,
        Some(&current_timestamp),
        [10.0, 10.0],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(first_id(&by_current_timestamp), "new");

    let generation = g1_version.to_string();
    let (status, by_generation) =
        query(&client, &base_url, &ns, Some(&generation), [10.0, 10.0]).await;
    assert_eq!(status, 200);
    assert_eq!(first_id(&by_generation), "old");

    let (status, by_timestamp) =
        query(&client, &base_url, &ns, Some(&g1_timestamp), [10.0, 10.0]).await;
    assert_eq!(status, 200);
    assert_eq!(first_id(&by_timestamp), "old");

    let (status, by_snapshot) = query(
        &client,
        &base_url,
        &ns,
        Some("snapshot:before-new"),
        [10.0, 10.0],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(first_id(&by_snapshot), "old");

    harness
        .store
        .delete(&Manifest::history_key(&ns, g1_version))
        .await
        .unwrap();
    let (status, error) = query(&client, &base_url, &ns, Some(&generation), [10.0, 10.0]).await;
    assert_eq!(status, 410);
    assert_eq!(error["code"], "POINT_IN_TIME_NOT_RETAINED");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn query_as_of_timestamp_scans_full_history_under_clock_skew() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    upsert(&client, &base_url, &ns, "first", [0.0, 0.0]).await;
    let first = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    upsert(&client, &base_url, &ns, "second", [10.0, 10.0]).await;
    let second = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    upsert(&client, &base_url, &ns, "third", [20.0, 20.0]).await;

    let base = Utc::now() - Duration::minutes(10);
    rewrite_history_updated_at(&harness.store, &ns, &first, base + Duration::seconds(100)).await;
    rewrite_history_updated_at(&harness.store, &ns, &second, base + Duration::seconds(50)).await;

    let target = (base + Duration::seconds(75)).to_rfc3339();
    let (status, body) = query(&client, &base_url, &ns, Some(&target), [10.0, 10.0]).await;

    assert_eq!(status, 200);
    assert_eq!(first_id(&body), "second");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn query_rejects_unknown_as_of_query_param() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;
    upsert(&client, &base_url, &ns, "live", [0.0, 0.0]).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .query(&[("as_off", "1")])
        .json(&json!({
            "vector": [0.0, 0.0],
            "top_k": 1,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn query_as_of_eventual_reads_historical_compacted_manifest() {
    let mut config = zeppelin::config::Config::default();
    config.indexing.default_num_centroids = 2;
    config.indexing.default_nprobe = 2;
    config.indexing.max_nprobe = 8;
    config.indexing.quantization = QuantizationType::None;
    config.indexing.bitmap_index = false;
    config.indexing.fts_index = false;

    let (base_url, harness, _cache, _cache_dir, compactor, admin_bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    for (id, values) in [
        ("old-0", [0.0, 0.0]),
        ("old-1", [0.1, 0.0]),
        ("old-2", [0.0, 0.1]),
        ("old-3", [1.0, 1.0]),
    ] {
        upsert(&client, &base_url, &ns, id, values).await;
    }
    compactor.compact(&ns).await.unwrap();
    let compacted = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert!(!compacted.segments.is_empty());
    assert!(compacted.fragments.is_empty());

    upsert(&client, &base_url, &ns, "new", [10.0, 10.0]).await;
    let (live_status, live) = query(&client, &base_url, &ns, None, [10.0, 10.0]).await;
    assert_eq!(live_status, 200);
    assert_eq!(first_id(&live), "new");

    let generation = compacted.version().to_string();
    let (historical_status, historical) = query_with_consistency(
        &client,
        &base_url,
        &ns,
        Some(&generation),
        [0.0, 0.0],
        "eventual",
    )
    .await;
    assert_eq!(historical_status, 200);
    assert_eq!(first_id(&historical), "old-0");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn query_as_of_failed_live_put_never_exposes_candidate_generation() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    let (mut pending, version) = Manifest::read_versioned(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    let live_version = pending.version();
    pending.add_fragment(fragment(1));
    let orphan_generation = live_version + 1;

    let (failing_store, failures) = fail_put_once_matching(&harness.store, Manifest::s3_key(&ns));
    pending
        .write_conditional(&failing_store, &ns, &version)
        .await
        .unwrap_err();
    assert_eq!(failures.failures_injected(), 1);

    let live = Manifest::read(&harness.store, &ns).await.unwrap().unwrap();
    assert_eq!(live.version(), live_version);
    assert!(
        Manifest::read_history(&harness.store, &ns, orphan_generation)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!harness
        .store
        .exists(&Manifest::history_key(&ns, orphan_generation))
        .await
        .unwrap());

    let generation = orphan_generation.to_string();
    let (status, error) = query(&client, &base_url, &ns, Some(&generation), [0.0, 0.0]).await;
    assert_eq!(status, 410);
    assert_eq!(error["code"], "POINT_IN_TIME_NOT_RETAINED");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
