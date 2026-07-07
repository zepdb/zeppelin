mod common;

use common::server::{cleanup_ns, create_ns_api_with, start_test_server};
use serde_json::{json, Value};
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
    let mut request = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": vector,
            "top_k": 1,
            "consistency": "strong",
        }));
    if let Some(as_of) = as_of {
        request = request.query(&[("as_of", as_of)]);
    }
    let resp = request.send().await.unwrap();
    let status = resp.status();
    let body = resp.json::<Value>().await.unwrap();
    (status, body)
}

fn first_id(body: &Value) -> &str {
    body["results"][0]["id"].as_str().unwrap()
}

#[tokio::test]
async fn query_as_of_generation_timestamp_and_snapshot_read_historical_manifest() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
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
