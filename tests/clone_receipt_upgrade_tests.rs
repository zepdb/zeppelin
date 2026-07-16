mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use common::server::{cleanup_ns, client_with_bearer, create_ns_api_with, start_test_server};
use reqwest::StatusCode;
use serde_json::{json, Value};
use zeppelin::wal::Manifest;

async fn wait_for_compaction(client: &reqwest::Client, base_url: &str, namespace: &str) {
    let accepted = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/compact"))
        .send()
        .await
        .expect("manual compaction request must complete");
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);

    for _ in 0..200 {
        let response = client
            .get(format!(
                "{base_url}/v1/namespaces/{namespace}/compact/status"
            ))
            .send()
            .await
            .expect("compaction status request must complete");
        assert_eq!(response.status(), StatusCode::OK);
        let status: Value = response
            .json()
            .await
            .expect("compaction status must return JSON");
        if status["uncompacted_fragments"] == 0 && status["ready"] == true {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("manual compaction did not reach quiescence");
}

#[tokio::test]
async fn legacy_hierarchical_clone_hydrates_and_copies_routing_nodes() {
    let (base_url, harness, bearer) = start_test_server().await;
    let client = client_with_bearer(&bearer);
    let source = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean",
            "index_config": {
                "nlist": 4,
                "quantization": "scalar",
                "hierarchical": true,
                "fts_index": false,
                "bitmap_index": false
            }
        }),
    )
    .await;
    let target = format!("{}-legacy-hierarchical-clone", harness.prefix);
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{source}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "legacy-tree-a", "values": [0.0, 0.0]},
                {"id": "legacy-tree-b", "values": [1.0, 0.0]},
                {"id": "legacy-tree-c", "values": [0.0, 1.0]},
                {"id": "legacy-tree-d", "values": [1.0, 1.0]}
            ]
        }))
        .send()
        .await
        .expect("legacy hierarchical fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    wait_for_compaction(&client, &base_url, &source).await;

    let source_manifest = Manifest::read(&harness.store, &source)
        .await
        .expect("source manifest must read")
        .expect("source manifest must exist");
    let source_artifacts = source_manifest
        .receipt_artifacts(&source)
        .expect("fresh hierarchical manifest must have an exact inventory");
    let routing_artifacts = source_artifacts
        .iter()
        .filter(|(key, _)| {
            key.rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with("node_") && name.ends_with(".bin"))
        })
        .map(|(key, content_hash)| (key.clone(), *content_hash))
        .collect::<BTreeMap<_, _>>();
    assert!(
        !routing_artifacts.is_empty(),
        "hierarchical fixture must publish routing nodes"
    );

    let generation = source_manifest.version();
    let mut legacy = serde_json::to_value(source_manifest)
        .expect("source manifest must serialize for the legacy fixture");
    let object = legacy
        .as_object_mut()
        .expect("serialized manifest must be an object");
    for field in [
        "artifact_hashes",
        "merkle_root",
        "root_signature",
        "root_signer_node",
        "hierarchical_routing_nodes",
    ] {
        assert!(
            object.remove(field).is_some(),
            "source manifest must contain {field}"
        );
    }
    harness
        .store
        .put(
            &Manifest::history_key(&source, generation),
            Bytes::from(
                serde_json::to_vec(&legacy).expect("legacy history manifest must encode as JSON"),
            ),
        )
        .await
        .expect("legacy fixture must replace the retained history generation");
    let retained_legacy = Manifest::read_history(&harness.store, &source, generation)
        .await
        .expect("legacy history generation must read")
        .expect("legacy history generation must remain retained");
    assert!(
        retained_legacy.receipt_artifacts(&source).is_err(),
        "fixture must require authoritative receipt hydration"
    );

    let cloned = client
        .post(format!("{base_url}/v1/namespaces/{source}/clone"))
        .json(&json!({
            "target": target,
            "as_of": generation.to_string()
        }))
        .send()
        .await
        .expect("legacy hierarchical clone must complete");
    let cloned_status = cloned.status();
    let cloned_body: Value = cloned
        .json()
        .await
        .expect("legacy hierarchical clone must return JSON");
    assert_eq!(cloned_status, StatusCode::CREATED, "{cloned_body}");

    let target_manifest = Manifest::read(&harness.store, &target)
        .await
        .expect("target manifest must read")
        .expect("target manifest must exist");
    let target_artifacts = target_manifest
        .receipt_artifacts(&target)
        .expect("target must publish the hydrated exact inventory");
    let source_prefix = format!("{source}/");
    for (source_key, content_hash) in routing_artifacts {
        let suffix = source_key
            .strip_prefix(&source_prefix)
            .expect("source routing key must remain namespace scoped");
        let target_key = format!("{target}/{suffix}");
        assert_eq!(
            target_artifacts.get(&target_key),
            Some(&content_hash),
            "clone must retain the routing-node content hash for {target_key}"
        );
        assert!(
            harness
                .store
                .exists(&target_key)
                .await
                .expect("target routing-node existence check must complete"),
            "clone must copy routing node {target_key}"
        );
    }

    let query_document = json!({
        "vector": [0.0, 0.0],
        "top_k": 4,
        "receipt": true
    });
    let query = client
        .post(format!("{base_url}/v1/namespaces/{target}/query"))
        .json(&query_document)
        .send()
        .await
        .expect("cloned hierarchical receipt query must complete");
    let query_status = query.status();
    let query_body: Value = query
        .json()
        .await
        .expect("cloned hierarchical query must return JSON");
    assert_eq!(query_status, StatusCode::OK, "{query_body}");
    assert_eq!(query_body["results"].as_array().unwrap().len(), 4);
    assert!(query_body["receipt"]["touched"]
        .as_array()
        .expect("receipt must carry touched artifacts")
        .iter()
        .any(|artifact| artifact["key"].as_str().is_some_and(|key| key
            .rsplit('/')
            .next()
            .is_some_and(|name| { name.starts_with("node_") && name.ends_with(".bin") }))));

    let verify = client
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": query_body["receipt"].clone(),
            "results": query_body["results"].clone(),
            "query": query_document,
            "refetch": true
        }))
        .send()
        .await
        .expect("cloned hierarchical receipt verification must complete");
    let verify_status = verify.status();
    let verify_body: Value = verify
        .json()
        .await
        .expect("cloned hierarchical verification must return JSON");
    assert_eq!(verify_status, StatusCode::OK, "{verify_body}");
    assert_eq!(verify_body["valid"], true, "{verify_body}");

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
}
