//! Phase 4 regression: hidden rows must not perturb policy-visible ANN results.
//!
//! The security contract covers ordering as well as membership. A mandatory
//! filter may remove hidden rows after an IVF cluster has been selected, but
//! that is insufficient if those rows trained the shared centroids: hidden-only
//! writes plus recompaction can move the policy-visible rows between clusters.
//! With a bounded `nprobe`, the caller can then observe the hidden mutation as a
//! changed visible result frontier even though every visible vector is unchanged.
//!
//! This fixture deliberately starts with four visible rows and four centroids,
//! so each row begins in its own logical cluster. Physical cluster objects hold
//! at most three clusters, making a one-probe query necessarily approximate.
//! Three large, far-away hidden modes are then added and a forced full retrain
//! changes the shared partition. An all-cluster query is the positive control:
//! its exact visible ID/order/score bits must remain stable across the rewrite.

mod common;

use std::collections::BTreeSet;

use bytes::Bytes;
use common::counting::counting_store;
use common::harness::TestHarness;
use common::server::{
    cleanup_ns, client_with_bearer, create_ns_api_with, start_test_server_full,
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer,
    start_test_server_with_compactor,
};
use reqwest::StatusCode;
use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::index::hierarchical::{
    deserialize_tree_node, tree_meta_key, tree_node_key, TreeMeta,
};
use zeppelin::wal::Manifest;

const DIMENSIONS: usize = 2;
const NLIST: usize = 4;
const TOP_K: usize = 4;
const VISIBLE_TENANT: &str = "acme";
const HIDDEN_TENANT: &str = "bravo";
const HIDDEN_ROWS_PER_MODE: usize = 32;

fn isolation_config() -> Config {
    let mut config = Config::default();
    // Any non-empty follow-up WAL must rebuild the centroids. Reusing the old
    // centroids would test incremental membership, not hidden-corpus training.
    config.compaction.retrain_imbalance_threshold = 0.0;
    config
}

async fn create_namespace(admin: &reqwest::Client, base_url: &str) -> String {
    create_namespace_with_hierarchical(admin, base_url, false).await
}

async fn create_namespace_with_hierarchical(
    admin: &reqwest::Client,
    base_url: &str,
    hierarchical: bool,
) -> String {
    create_ns_api_with(
        admin,
        base_url,
        json!({
            "dimensions": DIMENSIONS,
            "distance_metric": "euclidean",
            "index_config": {
                "nlist": NLIST,
                "quantization": "none",
                "hierarchical": hierarchical,
                "fts_index": false,
                "bitmap_index": false
            }
        }),
    )
    .await
}

async fn expect_status(response: reqwest::Response, expected: StatusCode, context: &str) -> Value {
    let actual = response.status();
    let bytes = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("{context} response body must be readable: {error}"));
    assert_eq!(
        actual,
        expected,
        "{context}: {}",
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("{context} response must be JSON: {error}"))
    }
}

async fn constrained_query_client(
    admin: &reqwest::Client,
    base_url: &str,
    namespace: &str,
) -> reqwest::Client {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let principal_id = format!("service:phase4-ann-isolation-{suffix}");

    let response = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "phase4-ann-isolation"
        }))
        .send()
        .await
        .expect("principal creation request must complete");
    expect_status(response, StatusCode::CREATED, "principal creation").await;

    let response = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": principal_id,
            "name": "phase4-ann-isolation-primary"
        }))
        .send()
        .await
        .expect("key creation request must complete");
    let key = expect_status(response, StatusCode::CREATED, "key creation").await;
    let bearer = key["api_key"]
        .as_str()
        .expect("key creation must return the one-time api_key");

    let response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["Query"]},
            "mandatory_filter": tenant_filter(VISIBLE_TENANT)
        }))
        .send()
        .await
        .expect("constrained grant creation request must complete");
    expect_status(response, StatusCode::CREATED, "constrained grant creation").await;

    client_with_bearer(bearer)
}

fn tenant_filter(tenant: &str) -> Value {
    json!({"op": "eq", "field": "tenant_id", "value": tenant})
}

fn visible_rows() -> Vec<Value> {
    [0.0_f32, 10.0, 20.0, 30.0]
        .into_iter()
        .map(|x| {
            json!({
                "id": format!("acme-{x:02.0}"),
                "values": [x, 0.0],
                "attributes": {"tenant_id": VISIBLE_TENANT}
            })
        })
        .collect()
}

fn hidden_rows() -> Vec<Value> {
    [-3_000.0_f32, -2_000.0, -1_000.0]
        .into_iter()
        .enumerate()
        .flat_map(|(mode, x)| {
            (0..HIDDEN_ROWS_PER_MODE).map(move |row| {
                json!({
                    "id": format!("bravo-{mode}-{row:02}"),
                    "values": [x, 0.0],
                    "attributes": {"tenant_id": HIDDEN_TENANT}
                })
            })
        })
        .collect()
}

async fn upsert(admin: &reqwest::Client, base_url: &str, namespace: &str, vectors: &[Value]) {
    let response = admin
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&json!({"vectors": vectors}))
        .send()
        .await
        .expect("fixture upsert request must complete");
    expect_status(response, StatusCode::OK, "fixture upsert").await;
}

async fn compact_and_assert_layout(
    harness: &common::harness::TestHarness,
    compactor: &zeppelin::compaction::Compactor,
    namespace: &str,
    expected_vectors: usize,
) {
    let result = compactor
        .compact(namespace)
        .await
        .expect("fixture compaction must succeed");
    assert!(
        result.segment_id.is_some(),
        "fixture compaction must publish a new active segment"
    );

    let manifest = Manifest::read(&harness.store, namespace)
        .await
        .expect("fixture manifest read must succeed")
        .expect("fixture manifest must exist");
    assert!(
        manifest.uncompacted_fragments().is_empty(),
        "isolation comparison must not include a WAL frontier"
    );
    let active_id = manifest
        .active_segment
        .as_ref()
        .expect("compaction must set an active segment");
    let active = manifest
        .segments
        .iter()
        .find(|segment| &segment.id == active_id)
        .expect("active segment descriptor must exist");
    assert_eq!(active.cluster_count, NLIST);
    assert_eq!(active.vector_count, expected_vectors);
}

fn ann_body(nprobe: usize, caller_filter: Option<Value>) -> Value {
    let mut body = json!({
        "vector": [15.0, 0.0],
        "top_k": TOP_K,
        "nprobe": nprobe,
        "consistency": "strong",
        "include_attributes": false
    });
    if let Some(filter) = caller_filter {
        body["filter"] = filter;
    }
    body
}

fn fused_ann_body(nprobe: usize) -> Value {
    json!({
        "sources": [
            {"type": "ann", "vector": [15.0, 0.0], "nprobe": nprobe},
            {"type": "ann", "vector": [15.0, 0.0], "nprobe": nprobe}
        ],
        "fusion": {"type": "rrf", "k": 60},
        "candidate_k": TOP_K,
        "top_k": TOP_K,
        "consistency": "strong",
        "include_attributes": false
    })
}

async fn query(client: &reqwest::Client, base_url: &str, namespace: &str, body: Value) -> Value {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&body)
        .send()
        .await
        .expect("query request must complete");
    expect_status(response, StatusCode::OK, "query").await
}

fn result_signature(body: &Value) -> Vec<(String, u32)> {
    body["results"]
        .as_array()
        .expect("query response must contain results")
        .iter()
        .map(|result| {
            let id = result["id"]
                .as_str()
                .expect("query result must contain an id")
                .to_string();
            let score = result["score"]
                .as_f64()
                .expect("query result must contain a numeric score") as f32;
            (id, score.to_bits())
        })
        .collect()
}

fn assert_visible_only(results: &[(String, u32)]) {
    assert!(
        results.iter().all(|(id, _)| id.starts_with("acme-")),
        "mandatory-filter response exposed a hidden ID: {results:?}"
    );
}

fn result_ids(results: &[(String, u32)]) -> BTreeSet<&str> {
    results.iter().map(|(id, _)| id.as_str()).collect()
}

#[tokio::test]
async fn scoped_ann_preserves_hierarchical_index_configuration() {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        isolation_config(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_namespace_with_hierarchical(&admin, &server.base_url, true).await;
    let constrained = constrained_query_client(&admin, &server.base_url, &namespace).await;
    let visible = visible_rows();
    upsert(&admin, &server.base_url, &namespace, &visible).await;
    server
        .compactor
        .compact(&namespace)
        .await
        .expect("hierarchical fixture compaction must succeed");

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("fixture manifest read must succeed")
        .expect("fixture manifest must exist");
    let active_id = manifest
        .active_segment
        .as_ref()
        .expect("hierarchical fixture must have an active segment");
    let active = manifest
        .segments
        .iter()
        .find(|segment| &segment.id == active_id)
        .expect("active segment descriptor must exist");
    assert!(active.hierarchical, "fixture source must be hierarchical");

    let results = result_signature(
        &query(
            &constrained,
            &server.base_url,
            &namespace,
            ann_body(1, None),
        )
        .await,
    );
    assert_visible_only(&results);
    let scope_prefix = format!("{namespace}/segments/{}/security_scopes/", active.id);
    let scope_keys = harness
        .store
        .list_prefix(&scope_prefix)
        .await
        .expect("scope artifact listing must succeed");
    assert!(
        scope_keys.iter().any(|key| key.ends_with("/tree_meta.json")),
        "a hierarchical namespace must produce a hierarchical policy-slice artifact: {scope_keys:?}"
    );
    assert!(
        scope_keys
            .iter()
            .all(|key| !key.ends_with("/centroids.bin")),
        "scope construction must not silently downgrade configured hierarchical ANN"
    );

    server.shutdown().await;
    cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn scoped_hierarchical_traversal_rejects_paired_node_inventory_omission() {
    let harness = TestHarness::new().await;
    let mut config = isolation_config();
    config.indexing.leaf_size = Some(1);
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config.clone(),
        false,
        None,
    )
    .await;
    let admin_bearer = server.admin_bearer.clone();
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace_with_hierarchical(&admin, &server.base_url, true).await;
    let constrained = constrained_query_client(&admin, &server.base_url, &namespace).await;
    let rows = (0..128)
        .map(|row| {
            json!({
                "id": format!("acme-tree-{row:03}"),
                "values": [row as f32, (row % 7) as f32],
                "attributes": {"tenant_id": VISIBLE_TENANT}
            })
        })
        .collect::<Vec<_>>();
    upsert(&admin, &server.base_url, &namespace, &rows).await;
    server
        .compactor
        .compact(&namespace)
        .await
        .expect("multi-level hierarchical fixture compaction must succeed");

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("multi-level fixture manifest must read")
        .expect("multi-level fixture manifest must exist");
    let active_id = manifest.active_segment.as_deref().unwrap();
    let scope_prefix = format!("{namespace}/segments/{active_id}/security_scopes/");
    let _ = query(
        &constrained,
        &server.base_url,
        &namespace,
        ann_body(NLIST, None),
    )
    .await;
    let descriptor_key = harness
        .store
        .list_prefix(&scope_prefix)
        .await
        .expect("scoped descriptor listing must succeed")
        .into_iter()
        .find(|key| key.contains("/ann/") && key.ends_with(".json"))
        .expect("scoped hierarchical query must publish one descriptor");
    let mut descriptor: Value = serde_json::from_slice(
        &harness
            .store
            .get(&descriptor_key)
            .await
            .expect("scoped descriptor must read"),
    )
    .expect("scoped descriptor must decode");
    let artifact_namespace = descriptor["artifact_namespace"]
        .as_str()
        .unwrap()
        .to_string();
    let artifact_id = descriptor["artifact_id"].as_str().unwrap().to_string();
    let meta: TreeMeta = serde_json::from_slice(
        &harness
            .store
            .get(&tree_meta_key(&artifact_namespace, &artifact_id))
            .await
            .expect("scoped tree metadata must read"),
    )
    .expect("scoped tree metadata must decode");
    let root = deserialize_tree_node(
        &harness
            .store
            .get(&tree_node_key(
                &artifact_namespace,
                &artifact_id,
                &meta.root_node_id,
            ))
            .await
            .expect("scoped root routing node must read"),
    )
    .expect("scoped root routing node must decode");
    let omitted_node = root
        .children
        .iter()
        .find(|child| child.parse::<usize>().is_err())
        .expect("fixture must contain a root-reachable internal child")
        .clone();
    let omitted_key = tree_node_key(&artifact_namespace, &artifact_id, &omitted_node);
    let routing_ids = descriptor["routing_node_ids"].as_array_mut().unwrap();
    let original_len = routing_ids.len();
    routing_ids.retain(|node| node.as_str() != Some(omitted_node.as_str()));
    assert_eq!(routing_ids.len(), original_len - 1);
    assert!(descriptor["artifact_hashes"]
        .as_object_mut()
        .unwrap()
        .remove(&omitted_key)
        .is_some());

    server.shutdown().await;
    harness
        .store
        .put(
            &descriptor_key,
            Bytes::from(serde_json::to_vec(&descriptor).unwrap()),
        )
        .await
        .expect("paired-omission descriptor tamper must succeed");
    let (store, counter) = counting_store(&harness.store);
    let restarted = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        Some(harness.prefix.clone()),
        config,
        false,
        None,
        100 * 1024 * 1024,
        &admin_bearer,
    )
    .await;
    counter.reset();
    let response = constrained
        .post(format!(
            "{}/v1/namespaces/{namespace}/query",
            restarted.base_url
        ))
        .json(&ann_body(NLIST, None))
        .send()
        .await
        .expect("paired-omission query must complete loudly");
    let body = expect_status(
        response,
        StatusCode::INTERNAL_SERVER_ERROR,
        "paired routing-node inventory omission",
    )
    .await;
    assert_eq!(body["code"], "INTERNAL_ERROR", "{body}");
    assert_eq!(
        counter.gets_matching(&omitted_key),
        0,
        "traversal must reject the parent-discovered node before consuming omitted bytes"
    );

    restarted.shutdown().await;
    cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn mandatory_filter_ann_is_invariant_to_hidden_only_recompaction() {
    let (base_url, harness, _cache, cache_dir, compactor, admin_bearer) =
        start_test_server_with_compactor(Some(isolation_config())).await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_namespace(&admin, &base_url).await;
    let constrained = constrained_query_client(&admin, &base_url, &namespace).await;

    let visible = visible_rows();
    upsert(&admin, &base_url, &namespace, &visible).await;
    compact_and_assert_layout(&harness, &compactor, &namespace, visible.len()).await;

    let exact_before = result_signature(
        &query(
            &admin,
            &base_url,
            &namespace,
            ann_body(NLIST, Some(tenant_filter(VISIBLE_TENANT))),
        )
        .await,
    );
    assert_eq!(
        result_ids(&exact_before),
        ["acme-00", "acme-10", "acme-20", "acme-30"]
            .into_iter()
            .collect(),
        "all-cluster control must see the complete policy-visible corpus"
    );

    let scoped_ann_before =
        result_signature(&query(&constrained, &base_url, &namespace, ann_body(1, None)).await);
    let scoped_fused_before =
        result_signature(&query(&constrained, &base_url, &namespace, fused_ann_body(1)).await);
    assert_visible_only(&scoped_ann_before);
    assert_visible_only(&scoped_fused_before);
    assert!(
        scoped_ann_before.len() < TOP_K,
        "one-probe baseline must remain approximate; fixture unexpectedly scanned every cluster"
    );

    let hidden = hidden_rows();
    upsert(&admin, &base_url, &namespace, &hidden).await;
    compact_and_assert_layout(
        &harness,
        &compactor,
        &namespace,
        visible.len() + hidden.len(),
    )
    .await;

    let exact_after = result_signature(
        &query(
            &admin,
            &base_url,
            &namespace,
            ann_body(NLIST, Some(tenant_filter(VISIBLE_TENANT))),
        )
        .await,
    );
    let scoped_ann_after =
        result_signature(&query(&constrained, &base_url, &namespace, ann_body(1, None)).await);
    let scoped_fused_after =
        result_signature(&query(&constrained, &base_url, &namespace, fused_ann_body(1)).await);
    assert_visible_only(&scoped_ann_after);
    assert_visible_only(&scoped_fused_after);

    // The equality below is the intentional RED assertion. Clean remote test
    // state first so a correctly failing regression does not leak its prefix.
    cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
    drop(cache_dir);

    assert_eq!(
        exact_after, exact_before,
        "positive control failed: hidden-only rows changed an all-cluster exact visible result"
    );
    assert_eq!(
        (&scoped_ann_after, &scoped_fused_after),
        (&scoped_ann_before, &scoped_fused_before),
        "hidden-only writes and full recompaction changed policy-visible ANN/fusion IDs, order, or score bits"
    );
}

#[tokio::test]
async fn scoped_ann_artifact_survives_restart_without_rescanning_source_segment() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let server = start_test_server_full(
        store.clone(),
        Some(harness.prefix.clone()),
        isolation_config(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_namespace(&admin, &server.base_url).await;
    let constrained = constrained_query_client(&admin, &server.base_url, &namespace).await;
    let visible = visible_rows();
    upsert(&admin, &server.base_url, &namespace, &visible).await;
    compact_and_assert_layout(&harness, &server.compactor, &namespace, visible.len()).await;
    server.manifest_cache.invalidate(&namespace);

    let manifest = Manifest::read(&harness.store, &namespace)
        .await
        .expect("fixture manifest read must succeed")
        .expect("fixture manifest must exist");
    let active_id = manifest
        .active_segment
        .as_ref()
        .expect("compacted fixture must have an active segment");
    let active = manifest
        .segments
        .iter()
        .find(|segment| &segment.id == active_id)
        .expect("active segment descriptor must exist");
    let source_cluster_keys: Vec<String> = active
        .cluster_objects
        .iter()
        .map(|object| object.key.clone())
        .collect();
    let scope_prefix = format!("{namespace}/segments/{}/security_scopes/", active.id);

    counter.reset();
    let before = query(
        &constrained,
        &server.base_url,
        &namespace,
        ann_body(1, None),
    )
    .await;
    assert!(
        counter.puts_matching("/security_scopes/") > 0,
        "first scoped ANN query must publish immutable scope artifacts"
    );
    assert_eq!(
        counter.create_puts_matching("/security_scopes/ann/"),
        1,
        "scope publication must use one create-only descriptor"
    );
    assert!(
        !harness
            .store
            .list_prefix(&scope_prefix)
            .await
            .expect("scope artifact listing must succeed")
            .is_empty(),
        "scoped ANN artifacts must share their source segment's GC lifecycle prefix"
    );

    let admin_bearer = server.admin_bearer.clone();
    server.shutdown().await;
    let restarted = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        Some(harness.prefix.clone()),
        isolation_config(),
        false,
        None,
        100 * 1024 * 1024,
        &admin_bearer,
    )
    .await;
    counter.reset();
    let after = query(
        &constrained,
        &restarted.base_url,
        &namespace,
        ann_body(1, None),
    )
    .await;

    assert_eq!(
        after, before,
        "restart must preserve exact scoped ANN output"
    );
    assert_eq!(
        counter.puts_matching("/security_scopes/"),
        0,
        "restart must load the published scope artifact without rebuilding it"
    );
    for key in source_cluster_keys {
        assert_eq!(
            counter.gets_matching(&key),
            0,
            "restart must not rescan source cluster object {key}"
        );
    }

    restarted.shutdown().await;
    cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
}
