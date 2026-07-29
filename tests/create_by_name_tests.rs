mod common;

use std::time::Duration;

use common::fault_injection::fail_put_once_matching;
use common::harness::TestHarness;
use common::server::{api_ns, start_test_server, start_test_server_on_store};
use reqwest::StatusCode;

use uuid::Uuid;

#[tokio::test]
async fn recreated_namespace_recovers_after_initial_manifest_publish_failure() {
    let harness = TestHarness::new().await;
    let name = api_ns(&harness, "recreate-manifest-crash");
    let (initial_url, _initial_cache, _initial_cache_dir, admin_bearer) =
        start_test_server_on_store(&harness, harness.store.clone(), None).await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

    let created = client
        .post(format!("{initial_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": name,
            "dimensions": 4,
            "distance_metric": "cosine"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);

    let deleted = client
        .delete(format!("{initial_url}/v1/namespaces/{name}"))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::ACCEPTED);

    for _ in 0..100 {
        let status = client
            .get(format!("{initial_url}/v1/namespaces/{name}"))
            .send()
            .await
            .unwrap()
            .status();
        if status == StatusCode::NOT_FOUND {
            break;
        }
        assert_eq!(status, StatusCode::OK);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        client
            .get(format!("{initial_url}/v1/namespaces/{name}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND,
        "acknowledged deletion must finish before the same name is recreated"
    );

    let (failing_store, failure) =
        fail_put_once_matching(&harness.store, format!("{name}/manifest.json"));
    let (failing_url, _failing_cache, _failing_cache_dir, failing_admin_bearer) =
        start_test_server_on_store(&harness, failing_store, None).await;
    let failing_client = crate::common::server::client_with_bearer(&failing_admin_bearer);
    let interrupted = failing_client
        .post(format!("{failing_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": name,
            "dimensions": 4,
            "distance_metric": "cosine"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(interrupted.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(failure.failures_injected(), 1);

    let (restarted_url, _restarted_cache, _restarted_cache_dir, restarted_admin_bearer) =
        start_test_server_on_store(&harness, harness.store.clone(), None).await;
    let restarted_client = crate::common::server::client_with_bearer(&restarted_admin_bearer);
    let recovered = restarted_client
        .get(format!("{restarted_url}/v1/namespaces/{name}"))
        .send()
        .await
        .unwrap();
    let recovered_status = recovered.status();
    let recovered_body: serde_json::Value = recovered.json().await.unwrap();
    assert_eq!(
        recovered_status,
        StatusCode::OK,
        "restart must recover the reserved namespace instead of exposing a missing manifest: \
         {recovered_body}"
    );
    assert_eq!(recovered_body["name"], name);
    assert_eq!(recovered_body["state"], "active");
    assert_eq!(recovered_body["vector_count"], 0);

    harness.cleanup().await;
}

#[tokio::test]
async fn test_create_namespace_by_name_is_idempotent_for_same_config() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

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
        assert_eq!(body["code"], "invalid_namespace");
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
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);

    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({ "dimensions": 4 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    // The test server sets `namespace_name_prefix` so concurrent suites cannot
    // collide in one bucket, so the response carries that prefix ahead of the
    // generated identifier.
    let name = body["name"].as_str().unwrap();
    let generated = name
        .strip_prefix(&format!("{}-", harness.prefix))
        .unwrap_or_else(|| panic!("generated name must carry the isolation prefix, got: {name}"));
    assert!(
        Uuid::parse_str(generated).is_ok(),
        "omitted-name path must keep returning a UUID namespace, got {name:?}"
    );

    harness.cleanup().await;
}
