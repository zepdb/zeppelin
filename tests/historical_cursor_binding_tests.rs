mod common;

use common::server::{cleanup_ns, create_ns_api_with, start_test_server};
use serde_json::{json, Value};

fn page_request(cursor: Value) -> Value {
    json!({
        "sources": [{
            "type": "ann",
            "vector": [0.0, 0.0]
        }],
        "top_k": 1,
        "cursor": cursor,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    })
}

async fn upsert(client: &reqwest::Client, base_url: &str, namespace: &str, vectors: Value) {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({"vectors": vectors}))
        .send()
        .await
        .expect("upsert request must complete");
    assert_eq!(
        response.status(),
        200,
        "upsert failed: {}",
        response.text().await.expect("upsert response body")
    );
}

async fn put_snapshot(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    name: &str,
) -> Value {
    let response = client
        .put(format!(
            "{base_url}/v1/namespaces/{namespace}/snapshots/{name}"
        ))
        .send()
        .await
        .expect("snapshot PUT must complete");
    let status = response.status();
    let body = response
        .bytes()
        .await
        .expect("snapshot PUT response body must be readable");
    assert_eq!(
        status,
        201,
        "snapshot PUT failed: {}",
        String::from_utf8_lossy(&body)
    );
    serde_json::from_slice(&body).expect("snapshot PUT must return JSON")
}

#[tokio::test]
async fn historical_cursor_binds_snapshot_alias_to_its_resolved_generation() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean"
        }),
    )
    .await;

    upsert(
        &client,
        &base_url,
        &namespace,
        json!([
            {"id": "a", "values": [0.1, 0.0]},
            {"id": "b", "values": [0.2, 0.0]},
            {"id": "c", "values": [0.3, 0.0]}
        ]),
    )
    .await;
    let snapshot_name = "cursor-alias";
    let first_snapshot = put_snapshot(&client, &base_url, &namespace, snapshot_name).await;
    let as_of = format!("snapshot:{snapshot_name}");

    let first_page_response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .query(&[("as_of", as_of.as_str())])
        .json(&page_request(json!({"type": "none"})))
        .send()
        .await
        .expect("historical first-page query must complete");
    assert_eq!(first_page_response.status(), 200);
    let first_page: Value = first_page_response
        .json()
        .await
        .expect("historical first page must return JSON");
    assert_eq!(first_page["results"][0]["id"], "a");
    let token = first_page["next_cursor"]
        .as_str()
        .expect("historical first page must issue a cursor")
        .to_string();

    let mut forged = token.clone();
    let final_byte = forged.pop().expect("cursor must contain an HMAC tag");
    forged.push(if final_byte == '0' { '1' } else { '0' });
    let query_counter = zeppelin::metrics::QUERIES_TOTAL.with_label_values(&[&namespace]);
    let before_forged = query_counter.get();
    let forged_response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .query(&[("as_of", as_of.as_str())])
        .json(&page_request(json!({
            "type": "after",
            "token": forged
        })))
        .send()
        .await
        .expect("forged historical continuation must complete");
    assert_eq!(forged_response.status(), 400);
    assert_eq!(
        query_counter.get(),
        before_forged,
        "historical cursor HMAC rejection must precede query metrics"
    );

    let delete_response = client
        .delete(format!(
            "{base_url}/v1/namespaces/{namespace}/snapshots/{snapshot_name}"
        ))
        .send()
        .await
        .expect("snapshot DELETE must complete");
    assert_eq!(delete_response.status(), 204);
    upsert(
        &client,
        &base_url,
        &namespace,
        json!([{"id": "new-neighbor", "values": [0.15, 0.0]}]),
    )
    .await;
    let second_snapshot = put_snapshot(&client, &base_url, &namespace, snapshot_name).await;
    assert_ne!(
        first_snapshot["generation"], second_snapshot["generation"],
        "the same raw snapshot selector must resolve to a new generation"
    );

    let continuation_response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .query(&[("as_of", as_of.as_str())])
        .json(&page_request(json!({
            "type": "after",
            "token": token
        })))
        .send()
        .await
        .expect("re-aliased historical continuation must complete");
    assert_eq!(continuation_response.status(), 400);
    let error: Value = continuation_response
        .json()
        .await
        .expect("cursor mismatch must return JSON");
    assert_eq!(error["code"], "VALIDATION_ERROR");
    assert!(error["error"]
        .as_str()
        .expect("validation error text")
        .contains("cursor token does not match query"));

    cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
}
