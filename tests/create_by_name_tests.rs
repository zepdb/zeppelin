mod common;

use common::server::{api_ns, start_test_server};

use uuid::Uuid;

#[tokio::test]
async fn test_create_namespace_by_name_is_idempotent_for_same_config() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let name = api_ns(&harness, "create-by-name");

    let first = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": name,
            "dimensions": 4,
            "distance_metric": "cosine"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status().as_u16(), 201);
    let first_body: serde_json::Value = first.json().await.unwrap();
    assert_eq!(first_body["name"], name);
    assert_eq!(first_body["dimensions"], 4);
    assert_eq!(first_body["distance_metric"], "cosine");

    let second = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": name,
            "dimensions": 4,
            "distance_metric": "cosine"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status().as_u16(), 200);
    let second_body: serde_json::Value = second.json().await.unwrap();
    assert_eq!(second_body["name"], name);
    assert_eq!(second_body["created_at"], first_body["created_at"]);
    assert_eq!(second_body["updated_at"], first_body["updated_at"]);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_create_namespace_by_name_conflicts_for_different_config() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();
    let name = api_ns(&harness, "create-by-name-conflict");

    let created = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": name,
            "dimensions": 4,
            "distance_metric": "cosine"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status().as_u16(), 201);

    let conflict = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": name,
            "dimensions": 8,
            "distance_metric": "cosine"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status().as_u16(), 409);
    let body: serde_json::Value = conflict.json().await.unwrap();
    assert_eq!(body["code"], "NAMESPACE_ALREADY_EXISTS");
    assert_eq!(body["status"], 409);
    assert_eq!(body["retryable"], false);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("namespace already exists"),
        "unexpected error envelope: {body}"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn test_create_namespace_rejects_invalid_client_name() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    for name in [
        "",
        "tenant/a",
        "tenant a",
        "tenant%2Fa",
        "tenant?",
        "-tenant",
    ] {
        let resp = client
            .post(format!("{base_url}/v1/namespaces"))
            .json(&serde_json::json!({
                "name": name,
                "dimensions": 4
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status().as_u16(),
            400,
            "expected invalid name {name:?} to be rejected"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["code"], "VALIDATION_ERROR");
        assert_eq!(body["status"], 400);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .to_lowercase()
                .contains("namespace name"),
            "unexpected error envelope for {name:?}: {body}"
        );
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn test_create_namespace_without_name_still_generates_uuid_name() {
    let (base_url, harness) = start_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({ "dimensions": 4 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    let name = body["name"].as_str().unwrap();
    assert!(
        Uuid::parse_str(name).is_ok(),
        "omitted-name path must keep returning a UUID namespace, got {name:?}"
    );

    harness.cleanup().await;
}
