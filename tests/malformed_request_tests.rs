mod common;

use common::server::create_ns_api_with;
use serde_json::json;

#[tokio::test]
async fn test_empty_body_upsert() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(&client, &base_url, json!({
        "dimensions": 4,
        "distance_metric": "euclidean"
    })).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();

    // Missing `vectors` field should fail deserialization (422)
    assert_eq!(resp.status().as_u16(), 422);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_empty_vectors_array() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(&client, &base_url, json!({
        "dimensions": 4,
        "distance_metric": "euclidean"
    })).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({ "vectors": [] }))
        .send()
        .await
        .unwrap();

    // Empty vectors should be rejected as 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_empty_delete_ids_array() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(&client, &base_url, json!({
        "dimensions": 4,
        "distance_metric": "euclidean"
    })).await;

    let resp = client
        .delete(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({ "ids": [] }))
        .send()
        .await
        .unwrap();

    // Empty ids should be rejected as 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_wrong_type_vector_values() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(&client, &base_url, json!({
        "dimensions": 4,
        "distance_metric": "euclidean"
    })).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": "test-1",
                "values": "not-a-vector"
            }]
        }))
        .send()
        .await
        .unwrap();

    // String instead of array should fail deserialization (422)
    assert_eq!(resp.status().as_u16(), 422);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_both_vector_and_rank_by() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(&client, &base_url, json!({
        "dimensions": 4,
        "distance_metric": "euclidean"
    })).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": [1.0, 2.0, 3.0, 4.0],
            "rank_by": ["title", "BM25", "hello"]
        }))
        .send()
        .await
        .unwrap();

    // Providing both should return 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_query_neither_vector_nor_rank_by() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(&client, &base_url, json!({
        "dimensions": 4,
        "distance_metric": "euclidean"
    })).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({ "top_k": 10 }))
        .send()
        .await
        .unwrap();

    // Providing neither should return 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_invalid_vector_id_characters() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(&client, &base_url, json!({
        "dimensions": 4,
        "distance_metric": "euclidean"
    })).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": "bad\x00id",
                "values": [1.0, 2.0, 3.0, 4.0]
            }]
        }))
        .send()
        .await
        .unwrap();

    // Invalid characters should return 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_nprobe_exceeds_max() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(&client, &base_url, json!({
        "dimensions": 4,
        "distance_metric": "euclidean"
    })).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": [1.0, 2.0, 3.0, 4.0],
            "nprobe": 9999
        }))
        .send()
        .await
        .unwrap();

    // nprobe exceeding max should return 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_top_k_zero() {
    let (base_url, harness) = common::server::start_test_server().await;
    let client = reqwest::Client::new();
    let ns = create_ns_api_with(&client, &base_url, json!({
        "dimensions": 4,
        "distance_metric": "euclidean"
    })).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": [1.0, 2.0, 3.0, 4.0],
            "top_k": 0
        }))
        .send()
        .await
        .unwrap();

    // top_k=0 should return 400
    assert_eq!(resp.status().as_u16(), 400);

    harness.cleanup().await;
}
