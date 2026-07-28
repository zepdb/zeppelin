mod common;

use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::counting::counting_store;
use common::harness::TestHarness;
use common::server::{
    cleanup_ns, client_with_bearer, create_ns_api_with, start_test_server_full,
    start_test_server_full_without_rate_limit_override_and_admin_bearer,
    start_test_server_with_compactor, start_test_server_with_config,
    start_test_server_with_entitlements, test_entitlements,
};
use proptest::prelude::*;
use reqwest::StatusCode;
use serde_json::{json, Map, Value};
use zeppelin::config::{Config, IndexingConfig};
use zeppelin::fts::global_index::global_fts_key;
use zeppelin::index::ivf_flat::build::attrs_key;
use zeppelin::index::quantization::sq::{serialize_sq_cluster, SqCalibration};
use zeppelin::index::quantization::QuantizationType;
use zeppelin::namespace::branching::{ArtifactOrigin, ArtifactOriginIndex};
use zeppelin::namespace::{NamespaceId, NamespaceIncarnationId, NamespaceManager};
use zeppelin::security::Feature;
use zeppelin::security::{MerkleTree, VerificationMode};
use zeppelin::wal::manifest::{ReceiptBindingVersion, SegmentRef};
use zeppelin::wal::{Manifest, WalFragment};

fn hash(byte: u8) -> [u8; 32] {
    [byte; 32]
}

fn receipts_config() -> Config {
    let mut config = Config::default();
    config.receipts.enabled = true;
    config
}

#[test]
fn merkle_inclusion_proofs_bind_key_and_content_hash() {
    let artifacts = BTreeMap::from([
        ("ns/segments/a/cluster_0.bin".to_string(), hash(1)),
        ("ns/segments/a/cluster_1.bin".to_string(), hash(2)),
        ("ns/wal/fragment.wal".to_string(), hash(3)),
    ]);
    let tree = MerkleTree::build(&artifacts).expect("a nonempty artifact set must build");

    for (key, content_hash) in &artifacts {
        let path = tree.proof(key).expect("every input key must have a proof");
        assert!(path.verify(key, content_hash, &tree.root()));

        let mut wrong_position = path.clone();
        wrong_position.leaf_index = wrong_position.leaf_index.saturating_add(1);
        assert!(!wrong_position.verify(key, content_hash, &tree.root()));

        let mut tampered = *content_hash;
        tampered[0] ^= 0xff;
        assert!(!path.verify(key, &tampered, &tree.root()));
        assert!(!path.verify(&format!("{key}.forged"), content_hash, &tree.root()));
    }
}

#[test]
fn merkle_root_is_canonical_and_any_leaf_mutation_changes_it() {
    let ordered = BTreeMap::from([
        ("a".to_string(), hash(10)),
        ("b".to_string(), hash(20)),
        ("c".to_string(), hash(30)),
        ("d".to_string(), hash(40)),
    ]);
    let original = MerkleTree::build(&ordered).unwrap().root();

    for key in ordered.keys() {
        let mut changed = ordered.clone();
        changed.get_mut(key).unwrap()[31] ^= 1;
        assert_ne!(MerkleTree::build(&changed).unwrap().root(), original);
    }
}

#[test]
fn phase_ten_receipts_are_explicitly_structural() {
    assert_eq!(
        serde_json::to_value(VerificationMode::Structural).unwrap(),
        serde_json::json!("structural")
    );
    assert!(
        serde_json::from_value::<VerificationMode>(serde_json::json!("deterministic")).is_err(),
        "deterministic verification must remain unrepresentable until replay exists"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn arbitrary_merkle_sets_have_exact_inclusion_proofs(
        artifacts in proptest::collection::btree_map("[a-z]{1,12}", any::<[u8; 32]>(), 1..64)
    ) {
        let tree = MerkleTree::build(&artifacts).expect("arbitrary nonempty tree must build");
        for (key, content_hash) in &artifacts {
            let proof = tree.proof(key).expect("every arbitrary leaf must have a proof");
            prop_assert!(proof.verify(key, content_hash, &tree.root()));

            let mut changed = *content_hash;
            changed[0] ^= 1;
            prop_assert!(!proof.verify(key, &changed, &tree.root()));
        }
    }
}

async fn query_with_receipt(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    top_k: usize,
) -> Value {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({
            "vector": [0.0, 0.0],
            "top_k": top_k,
            "receipt": true
        }))
        .send()
        .await
        .expect("receipt query must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("receipt query must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

async fn verify_query_receipt(
    client: &reqwest::Client,
    base_url: &str,
    query_response: &Value,
    top_k: usize,
) -> Value {
    let response = client
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": query_response["receipt"].clone(),
            "results": query_response["results"].clone(),
            "query": {
                "vector": [0.0, 0.0],
                "top_k": top_k,
                "receipt": true
            }
        }))
        .send()
        .await
        .expect("receipt verification must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("receipt verification must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[tokio::test]
async fn eventual_receipt_proves_only_delete_bearing_wal_that_query_consumed() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        receipts_config(),
        false,
        None,
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api_with(
        &client,
        &server.base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let upsert = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({
            "vectors": [
                {"id": "keep", "values": [0.0, 0.0]},
                {"id": "delete-me", "values": [1.0, 1.0]}
            ]
        }))
        .send()
        .await
        .expect("eventual receipt fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    let delete = client
        .delete(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({"ids": ["delete-me"]}))
        .send()
        .await
        .expect("eventual receipt fixture delete must complete");
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("eventual receipt manifest must read")
        .expect("eventual receipt manifest must exist");
    let delete_free = manifest
        .uncompacted_fragments()
        .iter()
        .find(|fragment| fragment.delete_count == 0)
        .expect("fixture must retain one delete-free WAL fragment");
    let delete_bearing = manifest
        .uncompacted_fragments()
        .iter()
        .find(|fragment| fragment.delete_count > 0)
        .expect("fixture must retain one delete-bearing WAL fragment");
    let delete_free_key = WalFragment::s3_key(&namespace, &delete_free.id);
    let delete_bearing_key = WalFragment::s3_key(&namespace, &delete_bearing.id);

    let query_document = json!({
        "vector": [0.0, 0.0],
        "top_k": 2,
        "consistency": "eventual",
        "receipt": true
    });
    let response = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&query_document)
        .send()
        .await
        .expect("eventual receipt query must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("eventual receipt query must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");
    let touched = body["receipt"]["touched"]
        .as_array()
        .expect("eventual receipt must carry touched proofs")
        .iter()
        .map(|artifact| artifact["key"].as_str().unwrap())
        .collect::<HashSet<_>>();

    assert!(
        touched.contains(delete_bearing_key.as_str()),
        "Eventual query consumed the delete-bearing WAL fragment"
    );
    assert!(
        !touched.contains(delete_free_key.as_str()),
        "Eventual query skipped the delete-free WAL fragment"
    );

    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn v2_origin_receipt_verifies_and_rejects_origin_table_tamper() {
    let harness = TestHarness::new().await;
    let config = receipts_config();
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        None,
    )
    .await;
    let bearer = server.admin_bearer.clone();
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &server.base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let upsert = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({
            "vectors": [{"id": "v2-origin-receipt", "values": [0.0, 0.0]}]
        }))
        .send()
        .await
        .expect("V2 origin receipt fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);

    let metadata = NamespaceManager::new(harness.store.clone())
        .get(&namespace)
        .await
        .expect("V2 origin receipt metadata must read");
    let incarnation = metadata
        .incarnation_id
        .expect("new namespace metadata must carry an incarnation");
    let (mut manifest, version) = Manifest::read_versioned(&harness.store, &namespace)
        .await
        .expect("V2 origin receipt manifest must read")
        .expect("V2 origin receipt manifest must exist");
    assert_eq!(manifest.fragments.len(), 1);
    manifest.artifact_origins = vec![ArtifactOrigin {
        namespace: NamespaceId::new(namespace.clone()).expect("fixture namespace must be valid"),
        incarnation,
    }];
    manifest.fragments[0].artifact_origin = Some(ArtifactOriginIndex::new(0));
    manifest
        .write_conditional(&harness.store, &namespace, &version)
        .await
        .expect("explicit local origin must publish");
    assert_eq!(
        manifest.receipt_binding_version(),
        Some(ReceiptBindingVersion::V2Origins)
    );
    assert!(manifest.root_signature().is_some());
    server.shutdown().await;

    let restarted_store = zeppelin::storage::ZeppelinStore::new(harness.store.inner());
    let restarted = start_test_server_full_without_rate_limit_override_and_admin_bearer(
        restarted_store,
        Some(harness.prefix.clone()),
        config,
        &bearer,
    )
    .await;
    let client = client_with_bearer(&bearer);
    let query = query_with_receipt(&client, &restarted.base_url, &namespace, 1).await;
    assert_eq!(query["receipt"]["manifest_binding_version"], "v2_origins");
    let verified = verify_query_receipt(&client, &restarted.base_url, &query, 1).await;
    assert_eq!(verified["valid"], true, "{verified}");
    assert_eq!(verified["manifest_history_checked"], true, "{verified}");

    let receipt_version = query["receipt"]["manifest_version"]
        .as_u64()
        .expect("receipt generation must be numeric");
    let history_key = Manifest::history_key(&namespace, receipt_version);
    let mut history = Manifest::read_history(&harness.store, &namespace, receipt_version)
        .await
        .expect("V2 origin receipt history must read")
        .expect("V2 origin receipt history must exist");
    let original_history = history
        .to_bytes()
        .expect("V2 origin receipt history must encode");
    assert_eq!(history.artifact_origins.len(), 1);
    let tamper_incarnation: NamespaceIncarnationId =
        serde_json::from_value(json!("00000000-0000-0000-0000-000000000002"))
            .expect("non-nil tamper incarnation must decode");
    history.artifact_origins.push(ArtifactOrigin {
        namespace: NamespaceId::new(format!("{namespace}-zz-tamper"))
            .expect("tamper namespace must be valid"),
        incarnation: tamper_incarnation,
    });
    harness
        .store
        .put(
            &history_key,
            history
                .to_bytes()
                .expect("tampered V2 origin receipt history must encode"),
        )
        .await
        .expect("harness must simulate V2 origin-table history tamper");

    let tampered = verify_query_receipt(&client, &restarted.base_url, &query, 1).await;
    assert_eq!(tampered["valid"], false, "{tampered}");
    assert_eq!(
        tampered["first_divergence"], "manifest_history",
        "{tampered}"
    );
    assert_eq!(tampered["manifest_history_checked"], true, "{tampered}");

    harness
        .store
        .put(&history_key, original_history)
        .await
        .expect("V2 origin receipt history must restore before teardown");
    restarted.shutdown().await;
    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
#[ignore = "explicit Phase 10 receipt-on CPU and storage-parity measurement"]
async fn receipt_on_cpu_measurement_adds_no_storage_operations() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let server = start_test_server_full(
        store,
        Some(harness.prefix.clone()),
        receipts_config(),
        false,
        None,
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api_with(
        &client,
        &server.base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let upsert = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({
            "vectors": [{"id": "receipt-cpu", "values": [0.0, 0.0]}]
        }))
        .send()
        .await
        .expect("measurement fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);

    for receipt in [false, true] {
        let warm = client
            .post(format!(
                "{}/v1/namespaces/{namespace}/query",
                server.base_url
            ))
            .json(&json!({"vector": [0.0, 0.0], "top_k": 1, "receipt": receipt}))
            .send()
            .await
            .expect("measurement warmup must complete");
        assert_eq!(warm.status(), StatusCode::OK);
        let _: Value = warm.json().await.expect("warmup response must be JSON");
    }

    let mut off_ns = Vec::with_capacity(64);
    let mut on_ns = Vec::with_capacity(64);
    let mut off_ops = Vec::with_capacity(64);
    let mut on_ops = Vec::with_capacity(64);
    for _ in 0..64 {
        for (receipt, times, ops) in [
            (false, &mut off_ns, &mut off_ops),
            (true, &mut on_ns, &mut on_ops),
        ] {
            counter.reset();
            let started = Instant::now();
            let response = client
                .post(format!(
                    "{}/v1/namespaces/{namespace}/query",
                    server.base_url
                ))
                .json(&json!({"vector": [0.0, 0.0], "top_k": 1, "receipt": receipt}))
                .send()
                .await
                .expect("measured query must complete");
            assert_eq!(response.status(), StatusCode::OK);
            let _: Value = response
                .json()
                .await
                .expect("measured response must be JSON");
            times.push(started.elapsed().as_nanos());
            ops.push((counter.total_observed_gets(), counter.total_observed_puts()));
        }
    }

    assert_eq!(
        off_ops, on_ops,
        "receipt issuance must add CPU/response bytes only, never storage operations"
    );
    off_ns.sort_unstable();
    on_ns.sort_unstable();
    let off_p50_ns = off_ns[off_ns.len() / 2];
    let on_p50_ns = on_ns[on_ns.len() / 2];
    let delta_ns = on_p50_ns.saturating_sub(off_p50_ns);
    println!(
        "RECEIPT_CPU_MEASUREMENT iterations=64 off_p50_ns={off_p50_ns} \
         on_p50_ns={on_p50_ns} delta_ns={delta_ns} storage_ops={:?}",
        off_ops[0]
    );

    server.shutdown().await;
    cleanup_ns(&harness.store, &namespace).await;
}

async fn wait_for_compaction(client: &reqwest::Client, base_url: &str, namespace: &str) -> Value {
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
            return status;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("manual compaction did not reach quiescence");
}

async fn assert_receipt_refetch_detects_tamper(
    client: &reqwest::Client,
    base_url: &str,
    store: &zeppelin::storage::ZeppelinStore,
    query_body: &Value,
    query_document: &Value,
    artifact_key: &str,
) {
    let original = store
        .get(artifact_key)
        .await
        .expect("receipt-touched artifact must exist");
    let mut corrupted = original.to_vec();
    corrupted[0] ^= 1;
    store
        .put(artifact_key, Bytes::from(corrupted))
        .await
        .expect("test-only artifact corruption must succeed");
    let response = client
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": query_body["receipt"].clone(),
            "results": query_body["results"].clone(),
            "query": query_document,
            "refetch": true
        }))
        .send()
        .await
        .expect("tampered-artifact verification must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("tampered-artifact verification must return JSON");
    store
        .put(artifact_key, original)
        .await
        .expect("test-only artifact restoration must succeed");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["valid"], false, "{body}");
    assert_eq!(body["first_divergence"], "artifact_refetch", "{body}");
}

async fn assert_receipt_refetches(
    client: &reqwest::Client,
    base_url: &str,
    query_body: &Value,
    query_document: &Value,
) {
    let response = client
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": query_body["receipt"].clone(),
            "results": query_body["results"].clone(),
            "query": query_document,
            "refetch": true
        }))
        .send()
        .await
        .expect("receipt refetch verification must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("receipt refetch verification must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["valid"], true, "{body}");
}

#[tokio::test]
async fn receipt_round_trip_verifies_and_reports_first_divergence() {
    let (base_url, harness, _cache, _cache_dir, bearer) =
        start_test_server_with_config(Some(receipts_config())).await;
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;

    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [{"id": "receipt-doc", "values": [0.0, 0.0]}]
        }))
        .send()
        .await
        .expect("receipt fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);

    let query_document = json!({
        "vector": [0.0, 0.0],
        "top_k": 1,
        "receipt": true
    });
    let query = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&query_document)
        .send()
        .await
        .expect("receipt query must complete");
    let query_status = query.status();
    let query_body: Value = query.json().await.expect("receipt query must return JSON");
    assert_eq!(query_status, StatusCode::OK, "{query_body}");
    assert_eq!(query_body["receipt"]["verification_mode"], "structural");
    assert_eq!(query_body["results"][0]["id"], "receipt-doc");

    let mut unsupported_receipt = query_body["receipt"].clone();
    unsupported_receipt["verification_mode"] = json!("deterministic");
    let unsupported = client
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": unsupported_receipt,
            "results": query_body["results"].clone(),
            "query": query_document
        }))
        .send()
        .await
        .expect("unsupported verification-mode request must complete");
    assert!(
        unsupported.status().is_client_error(),
        "deterministic verification must be rejected before structural checks"
    );

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
        .expect("receipt verification must complete");
    let verify_status = verify.status();
    let verify_body: Value = verify.json().await.expect("verification must return JSON");
    assert_eq!(verify_status, StatusCode::OK, "{verify_body}");
    assert_eq!(verify_body["valid"], true, "{verify_body}");
    assert_eq!(verify_body["first_divergence"], Value::Null);
    assert_eq!(verify_body["refetched_artifacts"], 1);

    let mut tampered_results = query_body["results"].clone();
    tampered_results[0]["id"] = json!("forged-doc");
    let tampered = client
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": query_body["receipt"].clone(),
            "results": tampered_results,
            "query": {
                "vector": [0.0, 0.0],
                "top_k": 1,
                "receipt": true
            }
        }))
        .send()
        .await
        .expect("tampered verification must complete");
    let tampered_body: Value = tampered
        .json()
        .await
        .expect("tampered response must be JSON");
    assert_eq!(tampered_body["valid"], false);
    assert_eq!(tampered_body["first_divergence"], "result_digest");

    let query_mismatch = client
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": query_body["receipt"].clone(),
            "results": query_body["results"].clone(),
            "query": {
                "vector": [1.0, 0.0],
                "top_k": 1,
                "receipt": true
            }
        }))
        .send()
        .await
        .expect("query-divergence verification must complete");
    let query_mismatch_body: Value = query_mismatch
        .json()
        .await
        .expect("query-divergence response must be JSON");
    assert_eq!(query_mismatch_body["valid"], false);
    assert_eq!(query_mismatch_body["first_divergence"], "query_hash");

    let mut forged_receipt = query_body["receipt"].clone();
    let first_signature_byte = forged_receipt["signature"][0]
        .as_u64()
        .expect("receipt signature must contain byte values");
    forged_receipt["signature"][0] = json!(first_signature_byte ^ 1);
    let forged = client
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": forged_receipt,
            "results": query_body["results"].clone(),
            "query": {
                "vector": [0.0, 0.0],
                "top_k": 1,
                "receipt": true
            }
        }))
        .send()
        .await
        .expect("forged verification must complete");
    let forged_body: Value = forged.json().await.expect("forged response must be JSON");
    assert_eq!(forged_body["valid"], false);
    assert_eq!(forged_body["first_divergence"], "signature");

    let root = client
        .get(format!(
            "{base_url}/v1/namespaces/{namespace}/manifest/root"
        ))
        .send()
        .await
        .expect("manifest-root request must complete");
    let root_body: Value = root.json().await.expect("manifest root must be JSON");
    assert_eq!(
        root_body["manifest_version"],
        query_body["receipt"]["manifest_version"]
    );
    assert_eq!(
        root_body["merkle_root"],
        query_body["receipt"]["manifest_root"]
    );

    harness
        .store
        .delete(&format!(
            "{}/_security/signer-slots/00.json",
            harness.prefix
        ))
        .await
        .expect("test-only authoritative signer-slot removal must succeed");
    let missing_signer = client
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": query_body["receipt"].clone(),
            "results": query_body["results"].clone(),
            "query": query_document
        }))
        .send()
        .await
        .expect("missing-signer verification must complete");
    let missing_signer_body: Value = missing_signer
        .json()
        .await
        .expect("missing-signer response must be JSON");
    assert_eq!(missing_signer_body["valid"], false);
    assert_eq!(missing_signer_body["first_divergence"], "signature");

    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn refetch_reports_the_first_tampered_artifact() {
    let (base_url, harness, _cache, _cache_dir, bearer) =
        start_test_server_with_config(Some(receipts_config())).await;
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let disposable = format!("{}-receipt-tamper-clone", harness.prefix);
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [{"id": "tamper-doc", "values": [0.0, 0.0]}]
        }))
        .send()
        .await
        .expect("tamper fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);

    let source_manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("tamper source manifest must read")
        .expect("tamper source manifest must exist");
    let cloned = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/clone"))
        .json(&json!({
            "target": disposable,
            "as_of": source_manifest.version().to_string()
        }))
        .send()
        .await
        .expect("disposable tamper clone must complete");
    let cloned_status = cloned.status();
    let cloned_body: Value = cloned
        .json()
        .await
        .expect("disposable tamper clone must return JSON");
    assert_eq!(cloned_status, StatusCode::CREATED, "{cloned_body}");

    let query = query_with_receipt(&client, &base_url, &disposable, 1).await;
    let artifact_key = query["receipt"]["touched"][0]["key"]
        .as_str()
        .expect("receipt must name one touched artifact")
        .to_string();
    let original = harness
        .store
        .get(&artifact_key)
        .await
        .expect("touched artifact must exist");
    let mut corrupted = original.to_vec();
    corrupted[0] ^= 1;
    harness
        .store
        .put(&artifact_key, Bytes::from(corrupted))
        .await
        .expect("out-of-band corruption must be injected");

    let verify = client
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": query["receipt"].clone(),
            "results": query["results"].clone(),
            "query": {
                "vector": [0.0, 0.0],
                "top_k": 1,
                "receipt": true
            },
            "refetch": true
        }))
        .send()
        .await
        .expect("tampered-artifact verification must complete");
    let body: Value = verify
        .json()
        .await
        .expect("tampered-artifact result must be JSON");
    assert_eq!(body["valid"], false, "{body}");
    assert_eq!(body["first_divergence"], "artifact_refetch", "{body}");
    assert_eq!(body["refetched_artifacts"], 1, "{body}");

    cleanup_ns(&harness.store, &namespace).await;
    cleanup_ns(&harness.store, &disposable).await;
}

#[tokio::test]
async fn unhashed_legacy_manifest_is_refused_until_compaction_rewrites_it() {
    let harness = TestHarness::new().await;
    let config = receipts_config();
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        None,
    )
    .await;
    let bearer = server.admin_bearer.clone();
    let client = client_with_bearer(&bearer);
    let base_url = server.base_url.clone();
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [{"id": "legacy-doc", "values": [0.0, 0.0]}]
        }))
        .send()
        .await
        .expect("legacy fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    let compacted = wait_for_compaction(&client, &base_url, &namespace).await;
    assert_eq!(compacted["segment_count"], 1, "{compacted}");

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("current manifest must read")
        .expect("current manifest must exist");
    let legacy_version = manifest.version();
    let mut legacy = serde_json::to_value(manifest).expect("manifest must serialize as JSON");
    let object = legacy
        .as_object_mut()
        .expect("serialized manifest must be an object");
    for field in [
        "artifact_hashes",
        "merkle_root",
        "root_signature",
        "root_signer_node",
    ] {
        assert!(
            object.remove(field).is_some(),
            "manifest must contain {field}"
        );
    }
    let legacy_bytes =
        Bytes::from(serde_json::to_vec(&legacy).expect("legacy manifest must encode"));
    harness
        .store
        .put(&Manifest::s3_key(&namespace), legacy_bytes.clone())
        .await
        .expect("legacy manifest fixture must replace the live pointer");
    harness
        .store
        .put(
            &Manifest::history_key(&namespace, legacy_version),
            legacy_bytes,
        )
        .await
        .expect("legacy fixture must keep current history byte-identical to live authority");
    server.shutdown().await;

    let restarted_store = zeppelin::storage::ZeppelinStore::new(harness.store.inner());
    let restarted = start_test_server_full_without_rate_limit_override_and_admin_bearer(
        restarted_store,
        Some(harness.prefix.clone()),
        config,
        &bearer,
    )
    .await;
    let base_url = restarted.base_url.clone();
    let client = client_with_bearer(&bearer);

    let refused = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({"vector": [0.0, 0.0], "top_k": 1, "receipt": true}))
        .send()
        .await
        .expect("legacy receipt request must complete");
    let refused_status = refused.status();
    let refused_body: Value = refused
        .json()
        .await
        .expect("legacy receipt refusal must be JSON");
    assert_eq!(refused_status, StatusCode::CONFLICT, "{refused_body}");
    assert_eq!(
        refused_body["code"], "receipts_unavailable_unhashed",
        "{refused_body}"
    );

    let new_wal = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [{"id": "post-legacy-wal", "values": [1.0, 1.0]}]
        }))
        .send()
        .await
        .expect("post-legacy WAL fixture upsert must complete");
    assert_eq!(new_wal.status(), StatusCode::OK);

    let upgraded = wait_for_compaction(&client, &base_url, &namespace).await;
    assert_eq!(upgraded["active_segment_vector_count"], 2, "{upgraded}");
    assert_eq!(upgraded["uncompacted_fragments"], 0, "{upgraded}");
    for _ in 0..200 {
        let current = Manifest::read(&harness.store, &namespace)
            .await
            .expect("upgraded manifest poll must read")
            .expect("upgraded manifest must remain present");
        if current.merkle_root().is_some() && current.root_signature().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let repaired = query_with_receipt(&client, &base_url, &namespace, 2).await;
    let repaired_ids = repaired["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["id"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        repaired_ids,
        std::collections::BTreeSet::from(["legacy-doc", "post-legacy-wal"])
    );
    assert!(repaired["receipt"]["manifest_root"].is_array());

    cleanup_ns(&harness.store, &namespace).await;
    restarted.shutdown().await;
}

#[tokio::test]
async fn empty_legacy_namespace_upgrades_once_and_issues_an_empty_receipt() {
    let harness = TestHarness::new().await;
    let config = receipts_config();
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        None,
    )
    .await;
    let bearer = server.admin_bearer.clone();
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &server.base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("empty current manifest must read")
        .expect("empty current manifest must exist");
    let initial_version = manifest.version();
    let mut legacy = serde_json::to_value(manifest).expect("empty manifest must serialize");
    let object = legacy
        .as_object_mut()
        .expect("serialized empty manifest must be an object");
    for field in [
        "artifact_hashes",
        "merkle_root",
        "root_signature",
        "root_signer_node",
    ] {
        assert!(
            object.remove(field).is_some(),
            "manifest must contain {field}"
        );
    }
    let legacy_bytes =
        Bytes::from(serde_json::to_vec(&legacy).expect("legacy empty manifest must encode"));
    harness
        .store
        .put(&Manifest::s3_key(&namespace), legacy_bytes.clone())
        .await
        .expect("legacy empty manifest must replace the live pointer");
    harness
        .store
        .put(
            &Manifest::history_key(&namespace, initial_version),
            legacy_bytes,
        )
        .await
        .expect("legacy fixture must keep current history byte-identical to live authority");
    server.shutdown().await;

    let restarted_store = zeppelin::storage::ZeppelinStore::new(harness.store.inner());
    let restarted = start_test_server_full_without_rate_limit_override_and_admin_bearer(
        restarted_store,
        Some(harness.prefix.clone()),
        config,
        &bearer,
    )
    .await;
    let client = client_with_bearer(&bearer);
    let accepted = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/compact",
            restarted.base_url
        ))
        .send()
        .await
        .expect("empty legacy upgrade request must complete");
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    for _ in 0..200 {
        let current = Manifest::read(&harness.store, &namespace)
            .await
            .expect("empty upgraded manifest poll must read")
            .expect("empty upgraded manifest must remain present");
        if current.version() > initial_version
            && current.merkle_root().is_some()
            && current.root_signature().is_some()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let receipt = query_with_receipt(&client, &restarted.base_url, &namespace, 1).await;
    assert!(receipt["results"].as_array().unwrap().is_empty());
    assert!(receipt["receipt"]["touched"].as_array().unwrap().is_empty());
    assert_eq!(receipt["receipt"]["derived_root"], Value::Null);

    let noop = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/compact",
            restarted.base_url
        ))
        .send()
        .await
        .expect("post-upgrade empty compaction must complete");
    let noop_status = noop.status();
    let noop_body: Value = noop
        .json()
        .await
        .expect("empty compaction noop must be JSON");
    assert_eq!(noop_status, StatusCode::OK, "{noop_body}");
    assert_eq!(noop_body["status"], "noop", "{noop_body}");

    cleanup_ns(&harness.store, &namespace).await;
    restarted.shutdown().await;
}

#[tokio::test]
async fn compaction_resigns_the_new_root_and_old_receipt_history_still_verifies() {
    let (base_url, harness, _cache, _cache_dir, bearer) =
        start_test_server_with_config(Some(receipts_config())).await;
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "compact-a", "values": [0.0, 0.0]},
                {"id": "compact-b", "values": [1.0, 1.0]}
            ]
        }))
        .send()
        .await
        .expect("compaction fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);

    let overwrite = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [{"id": "compact-a", "values": [0.25, 0.25]}]
        }))
        .send()
        .await
        .expect("second compaction fixture upsert must complete");
    assert_eq!(overwrite.status(), StatusCode::OK);

    let before = query_with_receipt(&client, &base_url, &namespace, 2).await;
    let old_version = before["receipt"]["manifest_version"]
        .as_u64()
        .expect("receipt generation must be numeric");
    let old_root = before["receipt"]["manifest_root"].clone();
    let compacted = wait_for_compaction(&client, &base_url, &namespace).await;
    assert!(
        compacted["manifest_generation"].as_u64().unwrap() > old_version,
        "{compacted}"
    );

    let after = query_with_receipt(&client, &base_url, &namespace, 2).await;
    assert_ne!(after["receipt"]["manifest_root"], old_root);
    assert_ne!(
        after["receipt"]["manifest_root_signature"],
        before["receipt"]["manifest_root_signature"]
    );

    let verify_old_body = verify_query_receipt(&client, &base_url, &before, 2).await;
    assert_eq!(verify_old_body["valid"], true, "{verify_old_body}");
    assert_eq!(
        verify_old_body["manifest_history_checked"], true,
        "{verify_old_body}"
    );

    let history_key = Manifest::history_key(&namespace, old_version);
    let history = Manifest::read_history(&harness.store, &namespace, old_version)
        .await
        .expect("retained receipt generation must read")
        .expect("retained receipt generation must exist");
    let original_history = history
        .to_bytes()
        .expect("retained receipt generation must encode");

    let mut wrong_fence = history.clone();
    wrong_fence.fencing_token = wrong_fence
        .fencing_token
        .checked_add(1)
        .expect("test fencing token must advance");
    harness
        .store
        .put(
            &history_key,
            wrong_fence
                .to_bytes()
                .expect("wrong-fence history must encode"),
        )
        .await
        .expect("harness must simulate a corrupted immutable history body");
    let wrong_fence_verification = verify_query_receipt(&client, &base_url, &before, 2).await;
    assert_eq!(
        wrong_fence_verification["valid"], false,
        "{wrong_fence_verification}"
    );
    assert_eq!(
        wrong_fence_verification["first_divergence"], "manifest_history",
        "{wrong_fence_verification}"
    );
    assert_eq!(
        wrong_fence_verification["manifest_history_checked"], true,
        "{wrong_fence_verification}"
    );

    let mut wrong_fragment_order = history.clone();
    assert!(
        wrong_fragment_order.fragments.len() >= 2,
        "pre-compaction receipt history must preserve two ordered WAL fragments"
    );
    wrong_fragment_order.fragments.swap(0, 1);
    harness
        .store
        .put(
            &history_key,
            wrong_fragment_order
                .to_bytes()
                .expect("wrong-fragment-order history must encode"),
        )
        .await
        .expect("harness must simulate reordered immutable history topology");
    let wrong_order_verification = verify_query_receipt(&client, &base_url, &before, 2).await;
    assert_eq!(
        wrong_order_verification["valid"], false,
        "{wrong_order_verification}"
    );
    assert_eq!(
        wrong_order_verification["first_divergence"], "manifest_history",
        "{wrong_order_verification}"
    );

    let mut wrong_inventory = history.clone();
    assert!(
        wrong_inventory.fragments.pop().is_some(),
        "pre-compaction receipt history must reference its WAL fragment"
    );
    harness
        .store
        .put(
            &history_key,
            wrong_inventory
                .to_bytes()
                .expect("wrong-inventory history must encode"),
        )
        .await
        .expect("harness must simulate an inconsistent immutable history inventory");
    let wrong_inventory_verification = verify_query_receipt(&client, &base_url, &before, 2).await;
    assert_eq!(
        wrong_inventory_verification["valid"], false,
        "{wrong_inventory_verification}"
    );
    assert_eq!(
        wrong_inventory_verification["first_divergence"], "manifest_history",
        "{wrong_inventory_verification}"
    );
    assert_eq!(
        wrong_inventory_verification["manifest_history_checked"], true,
        "{wrong_inventory_verification}"
    );

    harness
        .store
        .put(&history_key, original_history)
        .await
        .expect("history fixture must restore before teardown");

    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn retained_history_rejects_active_segment_rebinding_with_same_artifact_root() {
    let (base_url, harness, _cache, _cache_dir, bearer) =
        start_test_server_with_config(Some(receipts_config())).await;
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;

    for (id, values) in [("segment-a", [0.0, 0.0]), ("segment-b", [1.0, 1.0])] {
        let upsert = client
            .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
            .json(&json!({"vectors": [{"id": id, "values": values}]}))
            .send()
            .await
            .expect("active-segment fixture upsert must complete");
        assert_eq!(upsert.status(), StatusCode::OK);
        wait_for_compaction(&client, &base_url, &namespace).await;
    }

    let receipt_response = query_with_receipt(&client, &base_url, &namespace, 2).await;
    let receipt_version = receipt_response["receipt"]["manifest_version"]
        .as_u64()
        .expect("active-segment receipt version must be numeric");
    let expected_root: [u8; 32] =
        serde_json::from_value(receipt_response["receipt"]["manifest_root"].clone())
            .expect("receipt root must decode");
    let expected_state_digest: [u8; 32] =
        serde_json::from_value(receipt_response["receipt"]["manifest_state_digest"].clone())
            .expect("receipt state digest must decode");

    let advance = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [{"id": "history-advance", "values": [2.0, 2.0]}]
        }))
        .send()
        .await
        .expect("history-advance upsert must complete");
    assert_eq!(advance.status(), StatusCode::OK);

    let history_key = Manifest::history_key(&namespace, receipt_version);
    let history = Manifest::read_history(&harness.store, &namespace, receipt_version)
        .await
        .expect("active-segment receipt history must read")
        .expect("active-segment receipt history must exist");
    assert_eq!(history.merkle_root(), Some(expected_root));
    assert_eq!(history.receipt_state_digest(), Some(expected_state_digest));
    let active = history
        .active_segment
        .as_deref()
        .expect("compacted receipt history must select an active segment");
    let alternate = history
        .segments
        .iter()
        .find(|segment| segment.id != active)
        .map(|segment| segment.id.clone())
        .expect("receipt history must retain an alternate segment descriptor");
    let original_history = history
        .to_bytes()
        .expect("active-segment receipt history must encode");
    let mut rebound = history;
    rebound.active_segment = Some(alternate);
    harness
        .store
        .put(
            &history_key,
            rebound
                .to_bytes()
                .expect("rebound active-segment history must encode"),
        )
        .await
        .expect("harness must simulate active-segment history rebinding");

    let verification = verify_query_receipt(&client, &base_url, &receipt_response, 2).await;
    assert_eq!(verification["valid"], false, "{verification}");
    assert_eq!(
        verification["first_divergence"], "manifest_history",
        "{verification}"
    );
    assert_eq!(
        verification["manifest_history_checked"], true,
        "{verification}"
    );

    harness
        .store
        .put(&history_key, original_history)
        .await
        .expect("active-segment history fixture must restore before teardown");
    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn clone_immediately_rewrites_and_resigns_the_complete_receipt_inventory() {
    let (base_url, harness, _cache, _cache_dir, bearer) =
        start_test_server_with_config(Some(receipts_config())).await;
    let client = client_with_bearer(&bearer);
    let source = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let target = format!("{}-receipt-clone", harness.prefix);
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{source}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "clone-a", "values": [0.0, 0.0]},
                {"id": "clone-b", "values": [1.0, 1.0]}
            ]
        }))
        .send()
        .await
        .expect("clone receipt fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    wait_for_compaction(&client, &base_url, &source).await;
    let source_manifest = Manifest::read(&harness.store, &source)
        .await
        .expect("source manifest must read")
        .expect("source manifest must exist");

    let cloned = client
        .post(format!("{base_url}/v1/namespaces/{source}/clone"))
        .json(&json!({
            "target": target,
            "as_of": source_manifest.version().to_string()
        }))
        .send()
        .await
        .expect("receipt-aware clone must complete");
    let cloned_status = cloned.status();
    let cloned_body: Value = cloned.json().await.expect("clone must return JSON");
    assert_eq!(cloned_status, StatusCode::CREATED, "{cloned_body}");

    let target_query = query_with_receipt(&client, &base_url, &target, 2).await;
    assert_eq!(target_query["results"].as_array().unwrap().len(), 2);
    let touched = target_query["receipt"]["touched"]
        .as_array()
        .expect("clone receipt must carry its artifact inventory");
    assert!(!touched.is_empty());
    assert!(touched.iter().all(|artifact| artifact["key"]
        .as_str()
        .is_some_and(|key| key.starts_with(&format!("{target}/")))));
    assert_eq!(target_query["receipt"]["traversal"]["top_k"], 2);
    assert_eq!(
        target_query["receipt"]["traversal"]["sources"][0]["kind"],
        "ann"
    );
    assert!(target_query["receipt"]["traversal"]["sources"][0]["nprobe"].is_number());
    assert!(
        !target_query["receipt"]["traversal"]["sources"][0]["probed_centroids"]
            .as_array()
            .expect("compacted ANN traversal must carry probe indexes")
            .is_empty()
    );

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
}

#[tokio::test]
async fn hierarchical_receipts_bind_routing_nodes_and_survive_clone() {
    let (base_url, harness, _cache, _cache_dir, bearer) =
        start_test_server_with_config(Some(receipts_config())).await;
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
    let target = format!("{}-hierarchical-receipt-clone", harness.prefix);
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{source}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "tree-a", "values": [0.0, 0.0]},
                {"id": "tree-b", "values": [1.0, 0.0]},
                {"id": "tree-c", "values": [0.0, 1.0]},
                {"id": "tree-d", "values": [1.0, 1.0]}
            ]
        }))
        .send()
        .await
        .expect("hierarchical receipt fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    wait_for_compaction(&client, &base_url, &source).await;

    let source_query = query_with_receipt(&client, &base_url, &source, 4).await;
    let source_keys = source_query["receipt"]["touched"]
        .as_array()
        .expect("hierarchical receipt must carry touched artifacts")
        .iter()
        .map(|artifact| artifact["key"].as_str().unwrap())
        .collect::<Vec<_>>();
    let probed_routing_nodes = source_query["receipt"]["traversal"]["sources"][0]
        ["probed_routing_nodes"]
        .as_array()
        .expect("hierarchical traversal must carry exact routing-node IDs")
        .iter()
        .map(|node| node.as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(!probed_routing_nodes.is_empty());
    assert_eq!(
        probed_routing_nodes
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .len(),
        probed_routing_nodes.len(),
        "routing-node trace must not contain duplicates"
    );
    assert!(source_keys
        .iter()
        .any(|key| key.ends_with("/tree_meta.json")));
    for node_id in &probed_routing_nodes {
        let suffix = format!("/node_{node_id}.bin");
        assert!(
            source_keys.iter().any(|key| key.ends_with(&suffix)),
            "receipt must prove probed routing node {node_id}"
        );
    }
    assert!(source_keys
        .iter()
        .filter(|key| key.rsplit('/').next().unwrap().starts_with("node_"))
        .all(|key| probed_routing_nodes
            .iter()
            .any(|node_id| key.ends_with(&format!("/node_{node_id}.bin")))));
    assert!(source_keys.iter().all(|key| !key.contains("/sq_")));

    let source_manifest = Manifest::read(&harness.store, &source)
        .await
        .expect("hierarchical source manifest must read")
        .expect("hierarchical source manifest must exist");
    let cloned = client
        .post(format!("{base_url}/v1/namespaces/{source}/clone"))
        .json(&json!({
            "target": target,
            "as_of": source_manifest.version().to_string()
        }))
        .send()
        .await
        .expect("hierarchical receipt clone must complete");
    let cloned_status = cloned.status();
    let cloned_body: Value = cloned
        .json()
        .await
        .expect("hierarchical clone must return JSON");
    assert_eq!(cloned_status, StatusCode::CREATED, "{cloned_body}");

    let target_query = query_with_receipt(&client, &base_url, &target, 4).await;
    let verify = client
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": target_query["receipt"].clone(),
            "results": target_query["results"].clone(),
            "query": {"vector": [0.0, 0.0], "top_k": 4, "receipt": true},
            "refetch": true
        }))
        .send()
        .await
        .expect("cloned hierarchical receipt verification must complete");
    let verify_body: Value = verify
        .json()
        .await
        .expect("cloned hierarchical verification must return JSON");
    assert_eq!(verify_body["valid"], true, "{verify_body}");

    cleanup_ns(&harness.store, &source).await;
    cleanup_ns(&harness.store, &target).await;
}

#[tokio::test]
async fn retrieval_algebra_receipt_preserves_each_sources_actual_traversal() {
    let config = Config {
        indexing: IndexingConfig {
            fts_index: true,
            default_num_centroids: 4,
            ..Default::default()
        },
        ..receipts_config()
    };
    let (base_url, harness, _cache, _cache_dir, _compactor, bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean",
            "full_text_search": {
                "content": {"stemming": false, "remove_stopwords": false}
            }
        }),
    )
    .await;
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "hybrid-a", "values": [0.0, 0.0], "attributes": {"content": "search"}},
                {"id": "hybrid-b", "values": [1.0, 1.0], "attributes": {"content": "search search"}}
            ]
        }))
        .send()
        .await
        .expect("algebra traversal fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    wait_for_compaction(&client, &base_url, &namespace).await;

    let query_document = json!({
        "sources": [
            {"type": "ann", "vector": [0.0, 0.0], "nprobe": 1},
            {"type": "bm25", "rank_by": ["content", "BM25", "search"]}
        ],
        "fusion": {"type": "rrf", "k": 20},
        "candidate_k": 2,
        "top_k": 2,
        "receipt": true
    });
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&query_document)
        .send()
        .await
        .expect("algebra traversal query must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("algebra query must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");
    let sources = body["receipt"]["traversal"]["sources"]
        .as_array()
        .expect("algebra receipt must carry source traversals");
    assert_eq!(sources.len(), 2);
    assert_eq!(sources[0]["source_index"], 0);
    assert_eq!(sources[0]["kind"], "ann");
    assert_eq!(sources[0]["nprobe"], 1);
    assert_eq!(sources[0]["attributes_loaded"], true);
    assert!(!sources[0]["probed_centroids"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(sources[1]["source_index"], 1);
    assert_eq!(sources[1]["kind"], "bm25");
    assert_eq!(sources[1]["nprobe"], Value::Null);
    assert_eq!(sources[1]["attributes_loaded"], true);
    let bm25_clusters = sources[1]["probed_centroids"]
        .as_array()
        .expect("BM25 traversal must carry exact row-ID cluster reads")
        .iter()
        .map(|cluster| cluster.as_u64().unwrap() as usize)
        .collect::<HashSet<_>>();
    assert!(!bm25_clusters.is_empty());

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("algebra traversal manifest must read")
        .expect("algebra traversal manifest must exist");
    let active_id = manifest.active_segment.as_deref().unwrap();
    let segment = manifest
        .segments
        .iter()
        .find(|segment| segment.id == active_id)
        .unwrap();
    let touched = body["receipt"]["touched"]
        .as_array()
        .unwrap()
        .iter()
        .map(|artifact| artifact["key"].as_str().unwrap())
        .collect::<HashSet<_>>();
    let global_key = global_fts_key(&namespace, active_id);
    assert!(touched.contains(global_key.as_str()));
    for object in &segment.cluster_objects {
        if object
            .clusters
            .iter()
            .any(|cluster| bm25_clusters.contains(cluster))
        {
            assert!(
                touched.contains(object.key.as_str()),
                "receipt must prove BM25 packed row-ID object {}",
                object.key
            );
        }
    }
    for cluster in &bm25_clusters {
        let owner = segment.cluster_owner(*cluster);
        if segment.cluster_objects.is_empty() {
            let key = format!("{namespace}/segments/{owner}/cluster_{cluster}.bin");
            assert!(
                touched.contains(key.as_str()),
                "receipt must prove BM25 row-ID cluster {cluster}"
            );
        }
        let key = attrs_key(&namespace, owner, *cluster);
        assert!(
            touched.contains(key.as_str()),
            "receipt must prove BM25 attribute cluster {cluster}"
        );
    }

    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn grouped_flat_receipt_proves_the_physical_object_for_scanned_siblings() {
    let config = Config {
        indexing: IndexingConfig {
            default_num_centroids: 4,
            ..Default::default()
        },
        ..receipts_config()
    };
    let (base_url, harness, _cache, _cache_dir, _compactor, bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean",
            "index_config": {"nlist": 4}
        }),
    )
    .await;
    let vectors = (0..16)
        .map(|index| {
            let x = f64::from(index % 4) * 10.0 + f64::from(index / 4) * 0.01;
            let y = f64::from(index / 4) * 10.0;
            json!({"id": format!("grouped-{index}"), "values": [x, y]})
        })
        .collect::<Vec<_>>();
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({"vectors": vectors}))
        .send()
        .await
        .expect("grouped receipt fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    wait_for_compaction(&client, &base_url, &namespace).await;

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("grouped receipt manifest must read")
        .expect("grouped receipt manifest must exist");
    let segment = manifest
        .segments
        .iter()
        .find(|segment| Some(segment.id.as_str()) == manifest.active_segment.as_deref())
        .expect("grouped receipt fixture must have an active segment");
    let multi_cluster_objects = segment
        .cluster_objects
        .iter()
        .filter(|object| object.clusters.len() > 1)
        .collect::<Vec<_>>();
    assert!(
        !multi_cluster_objects.is_empty(),
        "fixture must publish at least one grouped sibling object: {:?}",
        segment.cluster_objects
    );

    let mut selected = None;
    for vector in &vectors {
        let query_document = json!({
            "vector": vector["values"].clone(),
            "top_k": 1,
            "nprobe": 1,
            "include_attributes": false,
            "receipt": true
        });
        let response = client
            .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
            .json(&query_document)
            .send()
            .await
            .expect("grouped receipt query must complete");
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .expect("grouped receipt query must return JSON");
        assert_eq!(status, StatusCode::OK, "{body}");
        let source = &body["receipt"]["traversal"]["sources"][0];
        let probed = source["probed_centroids"]
            .as_array()
            .expect("grouped traversal must carry probed centroids")
            .iter()
            .map(|cluster| cluster.as_u64().unwrap() as usize)
            .collect::<HashSet<_>>();
        if let Some(object) = multi_cluster_objects.iter().find(|object| {
            object
                .clusters
                .iter()
                .any(|cluster| probed.contains(cluster))
        }) {
            selected = Some((query_document, body, *object, probed));
            break;
        }
    }
    let (query_document, body, object, probed) =
        selected.expect("one fixture vector must route into a grouped physical object");
    let scanned = body["receipt"]["traversal"]["sources"][0]["scanned_clusters"]
        .as_array()
        .expect("grouped traversal must carry physical scan membership")
        .iter()
        .map(|cluster| cluster.as_u64().unwrap() as usize)
        .collect::<HashSet<_>>();
    assert!(
        object
            .clusters
            .iter()
            .all(|cluster| scanned.contains(cluster)),
        "scanned cluster trace must expand to physical siblings: object={:?} scanned={scanned:?}",
        object.clusters
    );
    assert!(
        object
            .clusters
            .iter()
            .any(|cluster| !probed.contains(cluster)),
        "fixture must exercise a sibling outside the logical nprobe set"
    );
    let touched = body["receipt"]["touched"]
        .as_array()
        .expect("grouped receipt must carry touched proofs");
    assert!(
        touched.iter().any(|artifact| artifact["key"] == object.key),
        "receipt must prove the exact grouped object {}",
        object.key
    );
    assert_receipt_refetches(&client, &base_url, &body, &query_document).await;

    cleanup_ns(&harness.store, &namespace).await;
}

fn legacy_centroids_bytes(centroids: &[Vec<f32>], dimensions: usize) -> Bytes {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &u32::try_from(centroids.len())
            .expect("legacy centroid count must fit u32")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(dimensions)
            .expect("legacy dimensions must fit u32")
            .to_le_bytes(),
    );
    for centroid in centroids {
        assert_eq!(centroid.len(), dimensions);
        for value in centroid {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    Bytes::from(bytes)
}

fn legacy_cluster_bytes(id: &str, vector: &[f32]) -> Bytes {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(
        &u32::try_from(vector.len())
            .expect("legacy dimensions must fit u32")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(id.len())
            .expect("legacy ID length must fit u32")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(id.as_bytes());
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    Bytes::from(bytes)
}

#[tokio::test]
async fn legacy_scalar_receipt_proves_the_standalone_calibration_read() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        receipts_config(),
        false,
        None,
    )
    .await;
    let client = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api_with(
        &client,
        &server.base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let segment_id = "legacy-sq-receipt";
    let vectors = [
        ("legacy-0", vec![0.0_f32, 0.0_f32]),
        ("legacy-1", vec![1.0_f32, 0.0_f32]),
        ("legacy-2", vec![0.0_f32, 1.0_f32]),
        ("legacy-3", vec![1.0_f32, 1.0_f32]),
    ];
    let centroids = vectors
        .iter()
        .map(|(_, vector)| vector.clone())
        .collect::<Vec<_>>();
    let refs = vectors
        .iter()
        .map(|(_, vector)| vector.as_slice())
        .collect::<Vec<_>>();
    let calibration = SqCalibration::calibrate(&refs, 2);
    let calibration_key = format!("{namespace}/segments/{segment_id}/sq_calibration.bin");
    server
        .store
        .put(
            &format!("{namespace}/segments/{segment_id}/centroids.bin"),
            legacy_centroids_bytes(&centroids, 2),
        )
        .await
        .expect("legacy centroids must publish");
    server
        .store
        .put(&calibration_key, calibration.to_bytes())
        .await
        .expect("legacy SQ calibration must publish");
    for (cluster_idx, (id, vector)) in vectors.iter().enumerate() {
        let codes = calibration.encode_batch(&[vector.as_slice()]);
        server
            .store
            .put(
                &format!("{namespace}/segments/{segment_id}/cluster_{cluster_idx}.bin"),
                legacy_cluster_bytes(id, vector),
            )
            .await
            .expect("legacy full-vector cluster must publish");
        server
            .store
            .put(
                &format!("{namespace}/segments/{segment_id}/sq_cluster_{cluster_idx}.bin"),
                serialize_sq_cluster(&[(*id).to_string()], &codes, 2)
                    .expect("legacy SQ cluster must encode"),
            )
            .await
            .expect("legacy SQ cluster must publish");
        server
            .store
            .put(
                &attrs_key(&namespace, segment_id, cluster_idx),
                Bytes::from(
                    serde_json::to_vec(&vec![Value::Null])
                        .expect("legacy attrs fixture must encode"),
                ),
            )
            .await
            .expect("legacy attrs must publish");
    }
    let mut manifest = Manifest::read(&server.store, &namespace)
        .await
        .expect("legacy SQ manifest must read")
        .expect("legacy SQ manifest must exist");
    manifest.add_segment(SegmentRef {
        id: segment_id.to_string(),
        vector_count: vectors.len(),
        cluster_count: vectors.len(),
        quantization: QuantizationType::Scalar,
        hierarchical: false,
        bitmap_fields: Vec::new(),
        fts_fields: Vec::new(),
        has_global_fts: false,
        cluster_owners: Vec::new(),
        cluster_objects: Vec::new(),
        sketch: None,
        bootstrap: None,
        membership: None,
        artifact_origin: None,
    });
    manifest
        .write(&server.store, &namespace)
        .await
        .expect("legacy SQ manifest must publish");
    server.manifest_cache.invalidate(&namespace);

    let query_document = json!({
        "vector": [0.0, 0.0],
        "top_k": 4,
        "nprobe": 4,
        "include_attributes": false,
        "receipt": true
    });
    let response = client
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            server.base_url
        ))
        .json(&query_document)
        .send()
        .await
        .expect("legacy SQ receipt query must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("legacy SQ receipt query must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"].as_array().map(Vec::len), Some(4));
    let touched = body["receipt"]["touched"]
        .as_array()
        .expect("legacy SQ receipt must carry touched proofs");
    assert!(
        touched
            .iter()
            .any(|artifact| { artifact["key"].as_str() == Some(calibration_key.as_str()) }),
        "receipt must prove the exact standalone calibration read: {touched:?}"
    );
    assert_receipt_refetches(&client, &server.base_url, &body, &query_document).await;

    server.shutdown().await;
    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn global_bm25_receipt_proves_and_refetches_result_cluster_data() {
    let config = Config {
        indexing: IndexingConfig {
            fts_index: true,
            default_num_centroids: 4,
            ..Default::default()
        },
        ..receipts_config()
    };
    let (base_url, harness, _cache, _cache_dir, _compactor, bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean",
            "full_text_search": {
                "content": {"stemming": false, "remove_stopwords": false}
            }
        }),
    )
    .await;
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "bm25-a", "values": [0.0, 0.0], "attributes": {"content": "search search"}},
                {"id": "bm25-b", "values": [1.0, 0.0], "attributes": {"content": "search"}},
                {"id": "bm25-c", "values": [0.0, 1.0], "attributes": {"content": "other"}},
                {"id": "bm25-d", "values": [1.0, 1.0], "attributes": {"content": "other"}}
            ]
        }))
        .send()
        .await
        .expect("global BM25 receipt fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    wait_for_compaction(&client, &base_url, &namespace).await;

    let query_document = json!({
        "rank_by": ["content", "BM25", "search"],
        "top_k": 2,
        "include_attributes": false,
        "receipt": true
    });
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&query_document)
        .send()
        .await
        .expect("global BM25 receipt query must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("global BM25 receipt query must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!body["results"].as_array().unwrap().is_empty(), "{body}");

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("BM25 manifest must read")
        .expect("BM25 manifest must exist");
    let segment = manifest
        .segments
        .iter()
        .find(|segment| Some(segment.id.as_str()) == manifest.active_segment.as_deref())
        .expect("compacted BM25 fixture must have an active segment");
    let touched = body["receipt"]["touched"]
        .as_array()
        .expect("BM25 receipt must carry touched proofs");
    assert!(touched.iter().any(|artifact| artifact["key"]
        .as_str()
        .is_some_and(|key| key.ends_with("/global_fts.bin"))));
    let cluster_key = touched
        .iter()
        .filter_map(|artifact| artifact["key"].as_str())
        .find(|key| {
            segment
                .cluster_objects
                .iter()
                .any(|object| object.key == *key)
                || key.rsplit('/').next().is_some_and(|name| {
                    name.starts_with("cluster_") || name.starts_with("clusters_")
                })
        })
        .expect("global BM25 receipt must prove the cluster object used for result IDs")
        .to_string();

    assert_receipt_refetch_detects_tamper(
        &client,
        &base_url,
        &harness.store,
        &body,
        &query_document,
        &cluster_key,
    )
    .await;
    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn by_id_and_vector_rerank_receipt_proves_membership_and_cluster_reads() {
    let config = Config {
        indexing: IndexingConfig {
            default_num_centroids: 4,
            ..Default::default()
        },
        ..receipts_config()
    };
    let (base_url, harness, _cache, _cache_dir, _compactor, bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let upsert = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "seed", "values": [0.0, 0.0]},
                {"id": "near", "values": [1.0, 0.0]},
                {"id": "far", "values": [10.0, 0.0]},
                {"id": "other", "values": [0.0, 10.0]}
            ]
        }))
        .send()
        .await
        .expect("by-ID receipt fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    wait_for_compaction(&client, &base_url, &namespace).await;

    let query_document = json!({
        "sources": [{"type": "ann", "id": "seed", "nprobe": 4}],
        "candidate_k": 3,
        "top_k": 2,
        "rerank": {"type": "vector", "vector": [10.0, 0.0]},
        "projection": {"include_attributes": false},
        "receipt": true
    });
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&query_document)
        .send()
        .await
        .expect("by-ID rerank receipt query must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("by-ID rerank receipt query must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("by-ID manifest must read")
        .expect("by-ID manifest must exist");
    let segment = manifest
        .segments
        .iter()
        .find(|segment| Some(segment.id.as_str()) == manifest.active_segment.as_deref())
        .expect("compacted by-ID fixture must have an active segment");
    let membership_key = segment
        .membership
        .as_ref()
        .expect("compacted segment must publish membership")
        .key
        .clone();
    let touched_keys = body["receipt"]["touched"]
        .as_array()
        .expect("by-ID receipt must carry touched proofs")
        .iter()
        .filter_map(|artifact| artifact["key"].as_str())
        .collect::<HashSet<_>>();
    assert!(touched_keys.contains(membership_key.as_str()));
    let cluster_key = touched_keys
        .iter()
        .copied()
        .find(|key| {
            segment
                .cluster_objects
                .iter()
                .any(|object| object.key == *key)
                || key.rsplit('/').next().is_some_and(|name| {
                    name.starts_with("cluster_") || name.starts_with("clusters_")
                })
        })
        .expect("by-ID/rerank receipt must prove a fetched cluster object")
        .to_string();

    assert_receipt_refetch_detects_tamper(
        &client,
        &base_url,
        &harness.store,
        &body,
        &query_document,
        &membership_key,
    )
    .await;
    assert_receipt_refetch_detects_tamper(
        &client,
        &base_url,
        &harness.store,
        &body,
        &query_document,
        &cluster_key,
    )
    .await;
    cleanup_ns(&harness.store, &namespace).await;
}

async fn create_query_principal_with_filter(
    admin: &reqwest::Client,
    base_url: &str,
    namespace: &str,
) -> (String, reqwest::Client) {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let principal_id = format!("service:phase10-receipt-{suffix}");
    let principal = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "phase10 receipt tenant"
        }))
        .send()
        .await
        .expect("tenant principal creation must complete");
    assert_eq!(principal.status(), StatusCode::CREATED);

    let key = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({"principal_id": principal_id, "name": "phase10 receipt key"}))
        .send()
        .await
        .expect("tenant key creation must complete");
    assert_eq!(key.status(), StatusCode::CREATED);
    let key_body: Value = key.json().await.expect("tenant key must return JSON");
    let bearer = key_body["api_key"]
        .as_str()
        .expect("tenant key must be returned once")
        .to_string();

    let mut grant = Map::new();
    grant.insert("principal_id".to_string(), json!(principal_id));
    grant.insert(
        "scope".to_string(),
        json!({"kind": "namespace", "namespace": namespace}),
    );
    grant.insert(
        "actions".to_string(),
        json!({"kind": "selected", "actions": ["Query"]}),
    );
    grant.insert(
        "mandatory_filter".to_string(),
        json!({"op": "eq", "field": "tenant_id", "value": "acme"}),
    );
    let grant_response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&Value::Object(grant))
        .send()
        .await
        .expect("tenant grant creation must complete");
    assert_eq!(grant_response.status(), StatusCode::CREATED);

    (principal_id, client_with_bearer(&bearer))
}

#[tokio::test]
async fn empty_scoped_ann_receipt_proves_its_descriptor_and_refetches() {
    let config = Config {
        indexing: IndexingConfig {
            default_num_centroids: 4,
            ..Default::default()
        },
        ..receipts_config()
    };
    let (base_url, harness, _cache, _cache_dir, _compactor, bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let admin = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &admin,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let upsert = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [{
                "id": "bravo-only",
                "values": [1.0, 1.0],
                "attributes": {"tenant_id": "bravo"}
            }]
        }))
        .send()
        .await
        .expect("empty scoped ANN fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    wait_for_compaction(&admin, &base_url, &namespace).await;
    let (_principal_id, tenant) =
        create_query_principal_with_filter(&admin, &base_url, &namespace).await;
    let query_document = json!({
        "vector": [1.0, 1.0],
        "top_k": 1,
        "nprobe": 1,
        "include_attributes": false,
        "receipt": true
    });
    let response = tenant
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&query_document)
        .send()
        .await
        .expect("empty scoped ANN receipt query must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("empty scoped ANN receipt query must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["results"].as_array().unwrap().is_empty(), "{body}");
    let derived = body["receipt"]["derived_touched"]
        .as_array()
        .expect("empty scoped ANN receipt must carry its descriptor proof");
    assert_eq!(derived.len(), 1, "{derived:?}");
    assert!(derived[0]["key"]
        .as_str()
        .is_some_and(|key| key.contains("/security_scopes/ann/") && key.ends_with(".json")));
    assert_receipt_refetches(&admin, &base_url, &body, &query_document).await;

    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn multi_child_scoped_ann_receipt_proves_only_the_physical_search_path() {
    let config = Config {
        indexing: IndexingConfig {
            default_num_centroids: 4,
            default_nprobe: 1,
            hierarchical: true,
            leaf_size: Some(8),
            ..Default::default()
        },
        ..receipts_config()
    };
    let (base_url, harness, _cache, _cache_dir, _compactor, bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let admin = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &admin,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean",
            "index_config": {
                "nlist": 4,
                "hierarchical": true,
                "leaf_size": 8,
                "fts_index": false,
                "bitmap_index": false
            }
        }),
    )
    .await;
    let vectors = (0..48_u32)
        .map(|index| {
            json!({
                "id": format!("acme-hierarchical-{index:02}"),
                "values": [f64::from(index % 8), f64::from(index / 8)],
                "attributes": {"tenant_id": "acme"}
            })
        })
        .collect::<Vec<_>>();
    let upsert = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({"vectors": vectors}))
        .send()
        .await
        .expect("multi-child scoped ANN fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    wait_for_compaction(&admin, &base_url, &namespace).await;
    let (_principal_id, tenant) =
        create_query_principal_with_filter(&admin, &base_url, &namespace).await;
    let query_document = json!({
        "vector": [0.0, 0.0],
        "top_k": 1,
        "nprobe": 1,
        "include_attributes": false,
        "receipt": true
    });
    let response = tenant
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&query_document)
        .send()
        .await
        .expect("multi-child scoped ANN receipt query must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("multi-child scoped ANN receipt query must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"].as_array().map(Vec::len), Some(1));

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("multi-child source manifest must read")
        .expect("multi-child source manifest must exist");
    let source_segment = manifest
        .active_segment
        .as_deref()
        .expect("multi-child source must have an active segment");
    let scope_prefix = format!("{namespace}/segments/{source_segment}/security_scopes/");
    let scope_artifacts = harness
        .store
        .list_prefix(&scope_prefix)
        .await
        .expect("scoped ANN artifact inventory must list");
    let derived = body["receipt"]["derived_touched"]
        .as_array()
        .expect("multi-child scoped ANN receipt must carry derived proofs");
    let touched_keys = derived
        .iter()
        .map(|artifact| {
            artifact["key"]
                .as_str()
                .expect("derived proof key must be a string")
        })
        .collect::<HashSet<_>>();
    assert!(
        scope_artifacts.len() > touched_keys.len(),
        "one hierarchical search path must prove fewer objects than the complete derived inventory: inventory={scope_artifacts:?} touched={touched_keys:?}"
    );
    assert!(
        scope_artifacts.iter().any(|key| key
            .rsplit('/')
            .next()
            .is_some_and(|name| name.starts_with("node_"))),
        "fixture must publish multiple hierarchical routing children"
    );
    assert!(derived.iter().any(|artifact| artifact["key"]
        .as_str()
        .is_some_and(|key| key.ends_with("/tree_meta.json"))));
    assert_receipt_refetches(&admin, &base_url, &body, &query_document).await;

    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn scoped_bm25_receipt_binds_and_refetches_the_lazy_policy_artifact() {
    let config = Config {
        indexing: IndexingConfig {
            fts_index: true,
            default_num_centroids: 4,
            ..Default::default()
        },
        ..receipts_config()
    };
    let (base_url, harness, _cache, _cache_dir, _compactor, bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let admin = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &admin,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean",
            "full_text_search": {
                "content": {"stemming": false, "remove_stopwords": false}
            }
        }),
    )
    .await;
    let upsert = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "acme-text", "values": [0.0, 0.0], "attributes": {"tenant_id": "acme", "content": "search search"}},
                {"id": "bravo-text", "values": [1.0, 1.0], "attributes": {"tenant_id": "bravo", "content": "search hidden"}}
            ]
        }))
        .send()
        .await
        .expect("scoped BM25 receipt fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    wait_for_compaction(&admin, &base_url, &namespace).await;
    let (_principal_id, tenant) =
        create_query_principal_with_filter(&admin, &base_url, &namespace).await;
    let query_document = json!({
        "rank_by": ["content", "BM25", "search"],
        "top_k": 2,
        "consistency": "eventual",
        "receipt": true
    });
    let response = tenant
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&query_document)
        .send()
        .await
        .expect("scoped BM25 receipt query must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("scoped BM25 receipt query must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"][0]["id"], "acme-text");
    assert!(body["receipt"]["derived_root"].is_array());
    let derived = body["receipt"]["derived_touched"]
        .as_array()
        .expect("scoped BM25 receipt must carry derived artifact proofs");
    assert_eq!(derived.len(), 1, "{derived:?}");
    assert!(derived[0]["key"]
        .as_str()
        .is_some_and(|key| key.contains("/security_scopes/fts/")));

    let verify = admin
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": body["receipt"].clone(),
            "results": body["results"].clone(),
            "query": query_document,
            "refetch": true
        }))
        .send()
        .await
        .expect("scoped BM25 receipt verification must complete");
    let verify_body: Value = verify
        .json()
        .await
        .expect("scoped BM25 verification must return JSON");
    assert_eq!(verify_body["valid"], true, "{verify_body}");
    assert_eq!(
        verify_body["refetched_artifacts"].as_u64(),
        Some(
            u64::try_from(body["receipt"]["touched"].as_array().unwrap().len() + derived.len())
                .unwrap()
        )
    );

    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn transient_scoped_bm25_receipt_proves_segment_and_wal_materialization_reads() {
    let config = Config {
        indexing: IndexingConfig {
            fts_index: true,
            default_num_centroids: 4,
            ..Default::default()
        },
        ..receipts_config()
    };
    let (base_url, harness, _cache, _cache_dir, _compactor, bearer) =
        start_test_server_with_compactor(Some(config)).await;
    let admin = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &admin,
        &base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean",
            "full_text_search": {
                "content": {"stemming": false, "remove_stopwords": false}
            }
        }),
    )
    .await;
    let base_upsert = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "base-acme-a", "values": [0.0, 0.0], "attributes": {"tenant_id": "acme", "content": "search base"}},
                {"id": "base-acme-b", "values": [1.0, 0.0], "attributes": {"tenant_id": "acme", "content": "search search"}},
                {"id": "base-bravo-a", "values": [0.0, 1.0], "attributes": {"tenant_id": "bravo", "content": "search hidden"}},
                {"id": "base-bravo-b", "values": [1.0, 1.0], "attributes": {"tenant_id": "bravo", "content": "other"}}
            ]
        }))
        .send()
        .await
        .expect("transient scoped BM25 base upsert must complete");
    assert_eq!(base_upsert.status(), StatusCode::OK);
    wait_for_compaction(&admin, &base_url, &namespace).await;
    let wal_upsert = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "wal-acme", "values": [2.0, 2.0], "attributes": {"tenant_id": "acme", "content": "search frontier"}},
                {"id": "wal-bravo", "values": [3.0, 3.0], "attributes": {"tenant_id": "bravo", "content": "search forbidden"}}
            ]
        }))
        .send()
        .await
        .expect("transient scoped BM25 WAL upsert must complete");
    assert_eq!(wal_upsert.status(), StatusCode::OK);
    let (_principal_id, tenant) =
        create_query_principal_with_filter(&admin, &base_url, &namespace).await;
    let query_document = json!({
        "rank_by": ["content", "BM25", "search"],
        "top_k": 4,
        "consistency": "strong",
        "include_attributes": false,
        "receipt": true
    });
    let response = tenant
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&query_document)
        .send()
        .await
        .expect("transient scoped BM25 receipt query must complete");
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .expect("transient scoped BM25 receipt query must return JSON");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|result| result["id"] == "wal-acme"));
    assert_eq!(body["receipt"]["derived_root"], Value::Null);
    assert!(body["receipt"]["derived_touched"]
        .as_array()
        .expect("transient receipt must carry explicit empty derived proofs")
        .is_empty());

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("transient scoped BM25 manifest must read")
        .expect("transient scoped BM25 manifest must exist");
    let segment = manifest
        .segments
        .iter()
        .find(|segment| Some(segment.id.as_str()) == manifest.active_segment.as_deref())
        .expect("transient scoped BM25 fixture must have an active segment");
    assert!(!manifest.uncompacted_fragments().is_empty());
    let touched = body["receipt"]["touched"]
        .as_array()
        .expect("transient scoped BM25 receipt must carry base proofs")
        .iter()
        .map(|artifact| artifact["key"].as_str().unwrap())
        .collect::<HashSet<_>>();
    for fragment in manifest.uncompacted_fragments() {
        let key = WalFragment::s3_key(&namespace, &fragment.id);
        assert!(
            touched.contains(key.as_str()),
            "receipt must prove WAL fragment {}",
            key
        );
    }
    for cluster_idx in 0..segment.cluster_count {
        let owner = segment.cluster_owner(cluster_idx);
        assert!(
            touched.contains(attrs_key(&namespace, owner, cluster_idx).as_str()),
            "receipt must prove materialized attrs cluster {cluster_idx}"
        );
        if segment.cluster_objects.is_empty() {
            let key = format!("{namespace}/segments/{owner}/cluster_{cluster_idx}.bin");
            assert!(
                touched.contains(key.as_str()),
                "receipt must prove materialized legacy cluster {cluster_idx}"
            );
        }
    }
    for object in &segment.cluster_objects {
        assert!(
            touched.contains(object.key.as_str()),
            "receipt must prove materialized grouped object {}",
            object.key
        );
    }
    let scanned = body["receipt"]["traversal"]["sources"][0]["scanned_clusters"]
        .as_array()
        .expect("transient scoped BM25 traversal must carry all scanned clusters")
        .iter()
        .map(|cluster| cluster.as_u64().unwrap() as usize)
        .collect::<HashSet<_>>();
    assert_eq!(scanned, (0..segment.cluster_count).collect::<HashSet<_>>());
    assert_receipt_refetches(&admin, &base_url, &body, &query_document).await;

    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn receipt_binds_principal_policy_and_only_the_filter_hash() {
    let (base_url, harness, _cache, _cache_dir, bearer) =
        start_test_server_with_config(Some(receipts_config())).await;
    let admin = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &admin,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let upsert = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({
            "vectors": [
                {"id": "acme-doc", "values": [0.1, 0.0], "attributes": {"tenant_id": "acme"}},
                {"id": "bravo-doc", "values": [0.0, 0.0], "attributes": {"tenant_id": "bravo"}}
            ]
        }))
        .send()
        .await
        .expect("policy fixture upsert must complete");
    assert_eq!(upsert.status(), StatusCode::OK);
    wait_for_compaction(&admin, &base_url, &namespace).await;
    let (principal_id, tenant) =
        create_query_principal_with_filter(&admin, &base_url, &namespace).await;

    let tenant_query = query_with_receipt(&tenant, &base_url, &namespace, 2).await;
    let admin_query = query_with_receipt(&admin, &base_url, &namespace, 2).await;
    assert_eq!(tenant_query["results"].as_array().unwrap().len(), 1);
    assert_eq!(tenant_query["results"][0]["id"], "acme-doc");
    assert_eq!(tenant_query["receipt"]["principal_id"], principal_id);
    assert!(tenant_query["receipt"]["policy_checksum"].is_string());
    assert!(tenant_query["receipt"]["enforced_filter_hash"].is_array());
    assert!(tenant_query["receipt"]["policy_filter_hash"].is_array());
    assert_eq!(admin_query["receipt"]["enforced_filter_hash"], Value::Null);
    assert!(tenant_query["receipt"]["derived_root"].is_array());
    let derived = tenant_query["receipt"]["derived_touched"]
        .as_array()
        .expect("scoped ANN receipt must carry derived artifact proofs");
    assert!(!derived.is_empty());
    assert!(derived.iter().all(|artifact| artifact["key"]
        .as_str()
        .is_some_and(|key| key.contains("/security_scopes/"))));
    assert!(derived.iter().any(|artifact| artifact["key"]
        .as_str()
        .is_some_and(|key| key.contains("/security_scopes/ann/"))));
    assert_eq!(admin_query["receipt"]["derived_root"], Value::Null);
    assert!(admin_query["receipt"]["derived_touched"]
        .as_array()
        .expect("unscoped receipt must carry an explicit empty derived inventory")
        .is_empty());
    assert_ne!(
        tenant_query["receipt"]["result_digest"],
        admin_query["receipt"]["result_digest"]
    );
    let receipt_text = tenant_query["receipt"].to_string();
    assert!(!receipt_text.contains("tenant_id"));
    assert!(!receipt_text.contains("acme"));

    let verified = admin
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": tenant_query["receipt"].clone(),
            "results": tenant_query["results"].clone(),
            "query": {
                "vector": [0.0, 0.0],
                "top_k": 2,
                "receipt": true
            },
            "refetch": true
        }))
        .send()
        .await
        .expect("privileged historical-filter verification must complete");
    let verified_body: Value = verified
        .json()
        .await
        .expect("privileged historical-filter verification must return JSON");
    assert_eq!(verified_body["valid"], true, "{verified_body}");
    assert_eq!(verified_body["policy_filter_check"], "checked");
    assert_eq!(
        verified_body["refetched_artifacts"].as_u64(),
        Some(
            u64::try_from(
                tenant_query["receipt"]["touched"].as_array().unwrap().len()
                    + tenant_query["receipt"]["derived_touched"]
                        .as_array()
                        .unwrap()
                        .len()
            )
            .unwrap()
        )
    );

    let head_bytes = harness
        .store
        .get(&format!("{}/_security/heads/policy.json", harness.prefix))
        .await
        .expect("authoritative policy head must remain readable");
    let head: zeppelin::security::PolicyHead =
        serde_json::from_slice(&head_bytes).expect("authoritative policy head must decode");
    harness
        .store
        .delete(&format!("{}/{}", harness.prefix, head.object_key()))
        .await
        .expect("historical policy snapshot removal must succeed");
    let missing_snapshot = admin
        .post(format!("{base_url}/v1/verify"))
        .json(&json!({
            "receipt": tenant_query["receipt"].clone(),
            "results": tenant_query["results"].clone(),
            "query": {
                "vector": [0.0, 0.0],
                "top_k": 2,
                "receipt": true
            }
        }))
        .send()
        .await
        .expect("missing-policy verification must complete");
    let missing_snapshot_body: Value = missing_snapshot
        .json()
        .await
        .expect("missing-policy verification must return JSON");
    assert_eq!(
        missing_snapshot_body["valid"], false,
        "{missing_snapshot_body}"
    );
    assert_eq!(
        missing_snapshot_body["first_divergence"], "policy_filter_hash",
        "{missing_snapshot_body}"
    );
    assert_eq!(
        missing_snapshot_body["policy_filter_check"], "checked",
        "{missing_snapshot_body}"
    );

    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn receipt_request_requires_the_receipts_entitlement() {
    let entitlements = test_entitlements([Feature::Rbac, Feature::Delegation]);
    let (base_url, harness, _cache, _cache_dir, bearer) =
        start_test_server_with_entitlements(receipts_config(), entitlements).await;
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;

    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({"vector": [0.0, 0.0], "receipt": true}))
        .send()
        .await
        .expect("unlicensed receipt request must complete");
    let status = response.status();
    let body: Value = response.json().await.expect("license denial must be JSON");
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["code"], "feature_not_licensed");

    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn disabled_receipts_deny_query_root_and_verification() {
    let entitlements = test_entitlements(Feature::ALL);
    let (base_url, harness, _cache, _cache_dir, bearer) =
        start_test_server_with_entitlements(Config::default(), entitlements).await;
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;

    let responses = [
        client
            .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
            .json(&json!({"vector": [0.0, 0.0], "receipt": true}))
            .send()
            .await
            .expect("disabled receipt query denial must complete"),
        client
            .get(format!(
                "{base_url}/v1/namespaces/{namespace}/manifest/root"
            ))
            .send()
            .await
            .expect("disabled manifest-root denial must complete"),
        client
            .post(format!("{base_url}/v1/verify"))
            .json(&json!({}))
            .send()
            .await
            .expect("disabled receipt verification denial must complete"),
    ];
    for response in responses {
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .expect("disabled receipt denial must return JSON");
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["code"], "receipts_disabled", "{body}");
    }

    cleanup_ns(&harness.store, &namespace).await;
}

#[tokio::test]
async fn unsigned_empty_namespace_compaction_converges_to_noop() {
    let entitlements = test_entitlements([Feature::Rbac]);
    let (base_url, harness, _cache, _cache_dir, bearer) =
        start_test_server_with_entitlements(Config::default(), entitlements).await;
    let client = client_with_bearer(&bearer);
    let namespace = create_ns_api_with(
        &client,
        &base_url,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    let before = Manifest::read(&harness.store, &namespace)
        .await
        .expect("unsigned empty manifest must read")
        .expect("unsigned empty manifest must exist")
        .version();
    for _ in 0..2 {
        let response = client
            .post(format!("{base_url}/v1/namespaces/{namespace}/compact"))
            .send()
            .await
            .expect("unsigned empty compaction must complete");
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .expect("unsigned empty compaction must return JSON");
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["status"], "noop", "{body}");
    }
    let after = Manifest::read(&harness.store, &namespace)
        .await
        .expect("unsigned empty manifest must reread")
        .expect("unsigned empty manifest must remain present")
        .version();
    assert_eq!(after, before);

    cleanup_ns(&harness.store, &namespace).await;
}
