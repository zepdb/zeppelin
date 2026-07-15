mod common;

use std::collections::HashMap;

use common::counting::counting_store;
use common::harness::TestHarness;
use common::server::{
    cleanup_ns, client_with_bearer, create_ns_api_fts, start_test_server_full,
    start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer,
    start_test_server_with_compactor,
};
use reqwest::{Response, StatusCode};
use serde_json::{json, Value};
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, Config, IndexingConfig};
use zeppelin::fts::FtsFieldConfig;
use zeppelin::wal::Manifest;

const TENANT_A: &str = "acme";
const TENANT_B: &str = "bravo";

struct SecurityBm25Fixture {
    base_url: String,
    harness: TestHarness,
    _cache_dir: tempfile::TempDir,
    compactor: std::sync::Arc<Compactor>,
    admin: reqwest::Client,
    tenant: reqwest::Client,
    namespace: String,
}

fn fts_config() -> Config {
    Config {
        compaction: CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        indexing: IndexingConfig {
            default_num_centroids: 2,
            kmeans_max_iterations: 5,
            fts_index: true,
            bitmap_index: false,
            // A constrained legacy query must not reveal the manifest's hidden
            // corpus size by succeeding below these thresholds and failing
            // above them. The secure compatibility path scans unconditionally.
            bm25_max_full_scan_clusters: 1,
            bm25_max_full_scan_vectors: 1,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn fts_configs() -> HashMap<String, FtsFieldConfig> {
    HashMap::from([
        (
            "content".to_string(),
            FtsFieldConfig {
                stemming: false,
                remove_stopwords: false,
                ..Default::default()
            },
        ),
        (
            "title".to_string(),
            FtsFieldConfig {
                stemming: false,
                remove_stopwords: false,
                ..Default::default()
            },
        ),
    ])
}

async fn expect_json(response: Response, expected: StatusCode, context: &str) -> Value {
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("{context} body must be readable: {error}"));
    assert_eq!(
        status,
        expected,
        "{context}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("{context} body must be JSON: {error}"))
}

async fn create_tenant_principal(
    admin: &reqwest::Client,
    base_url: &str,
    namespace: &str,
) -> reqwest::Client {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let principal_id = format!("service:phase4-bm25-isolation-{suffix}");

    let response = admin
        .post(format!("{base_url}/v1/security/principals"))
        .json(&json!({
            "principal_id": principal_id,
            "kind": "service",
            "display_name": "phase4-bm25-isolation"
        }))
        .send()
        .await
        .expect("principal creation must complete");
    expect_json(response, StatusCode::CREATED, "principal creation").await;

    let response = admin
        .post(format!("{base_url}/v1/security/keys"))
        .json(&json!({
            "principal_id": principal_id,
            "name": "phase4-bm25-isolation-primary"
        }))
        .send()
        .await
        .expect("key creation must complete");
    let key = expect_json(response, StatusCode::CREATED, "key creation").await;
    let bearer = key["api_key"]
        .as_str()
        .expect("key creation must return its one-time API key");

    let response = admin
        .post(format!("{base_url}/v1/security/grants"))
        .json(&json!({
            "principal_id": principal_id,
            "scope": {"kind": "namespace", "namespace": namespace},
            "actions": {"kind": "selected", "actions": ["Query"]},
            "mandatory_filter": {
                "op": "eq",
                "field": "tenant_id",
                "value": TENANT_A
            }
        }))
        .send()
        .await
        .expect("constrained grant creation must complete");
    expect_json(response, StatusCode::CREATED, "constrained grant creation").await;

    client_with_bearer(bearer)
}

async fn setup() -> SecurityBm25Fixture {
    let (base_url, harness, _cache, cache_dir, compactor, admin_bearer) =
        start_test_server_with_compactor(Some(fts_config())).await;
    let admin = client_with_bearer(&admin_bearer);
    let namespace = create_ns_api_fts(
        &admin,
        &base_url,
        2,
        json!({
            "content": {
                "stemming": false,
                "remove_stopwords": false
            },
            "title": {
                "stemming": false,
                "remove_stopwords": false
            }
        }),
    )
    .await;
    let tenant = create_tenant_principal(&admin, &base_url, &namespace).await;
    SecurityBm25Fixture {
        base_url,
        harness,
        _cache_dir: cache_dir,
        compactor,
        admin,
        tenant,
        namespace,
    }
}

async fn upsert(fixture: &SecurityBm25Fixture, vectors: Value) {
    let response = fixture
        .admin
        .post(format!(
            "{}/v1/namespaces/{}/vectors",
            fixture.base_url, fixture.namespace
        ))
        .json(&json!({"vectors": vectors}))
        .send()
        .await
        .expect("fixture upsert must complete");
    expect_json(response, StatusCode::OK, "fixture upsert").await;
}

async fn bm25_query(fixture: &SecurityBm25Fixture, consistency: &str) -> Value {
    bm25_query_body(
        fixture,
        json!({
            "rank_by": ["content", "BM25", "needle"],
            "top_k": 16,
            "consistency": consistency
        }),
    )
    .await
}

async fn bm25_query_body(fixture: &SecurityBm25Fixture, body: Value) -> Value {
    let response = fixture
        .tenant
        .post(format!(
            "{}/v1/namespaces/{}/query",
            fixture.base_url, fixture.namespace
        ))
        .json(&body)
        .send()
        .await
        .expect("BM25 query must complete");
    expect_json(response, StatusCode::OK, "BM25 query").await
}

async fn bm25_query_at(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    consistency: &str,
) -> Value {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&json!({
            "rank_by": ["content", "BM25", "needle"],
            "top_k": 16,
            "consistency": consistency
        }))
        .send()
        .await
        .expect("BM25 query must complete");
    expect_json(response, StatusCode::OK, "BM25 query").await
}

async fn caller_filtered_bm25_query(fixture: &SecurityBm25Fixture, consistency: &str) -> Value {
    let response = fixture
        .admin
        .post(format!(
            "{}/v1/namespaces/{}/query",
            fixture.base_url, fixture.namespace
        ))
        .json(&json!({
            "rank_by": ["content", "BM25", "needle"],
            "filter": {
                "op": "eq",
                "field": "tenant_id",
                "value": TENANT_A
            },
            "top_k": 16,
            "consistency": consistency
        }))
        .send()
        .await
        .expect("caller-filtered BM25 query must complete");
    expect_json(response, StatusCode::OK, "caller-filtered BM25 query").await
}

async fn caller_filtered_bm25_status(
    fixture: &SecurityBm25Fixture,
    consistency: &str,
) -> StatusCode {
    fixture
        .admin
        .post(format!(
            "{}/v1/namespaces/{}/query",
            fixture.base_url, fixture.namespace
        ))
        .json(&json!({
            "rank_by": ["content", "BM25", "needle"],
            "filter": {
                "op": "eq",
                "field": "tenant_id",
                "value": TENANT_A
            },
            "top_k": 16,
            "consistency": consistency
        }))
        .send()
        .await
        .expect("caller-filtered BM25 query must complete")
        .status()
}

async fn force_active_segment_legacy(fixture: &SecurityBm25Fixture) {
    let (mut manifest, version) =
        Manifest::read_versioned(&fixture.harness.store, &fixture.namespace)
            .await
            .expect("legacy manifest read must succeed")
            .expect("legacy manifest must exist");
    let active_id = manifest
        .active_segment
        .as_deref()
        .expect("legacy fixture must have an active segment");
    let active = manifest
        .segments
        .iter_mut()
        .find(|segment| segment.id == active_id)
        .expect("legacy active segment must have a retained descriptor");
    assert!(
        active.has_global_fts,
        "compaction must first create both modern and per-cluster FTS artifacts"
    );
    active.has_global_fts = false;
    manifest
        .write_conditional(&fixture.harness.store, &fixture.namespace, &version)
        .await
        .expect("legacy manifest publication must succeed");
}

fn scored_results(body: &Value) -> Vec<(String, u32)> {
    body["results"]
        .as_array()
        .expect("BM25 response must contain results")
        .iter()
        .map(|result| {
            let id = result["id"]
                .as_str()
                .expect("BM25 result must contain an id")
                .to_string();
            let score = result["score"]
                .as_f64()
                .expect("BM25 result must contain a numeric score") as f32;
            (id, score.to_bits())
        })
        .collect()
}

fn visible_vectors() -> Value {
    json!([
        {
            "id": "acme-short",
            "values": [0.0, 0.0],
            "attributes": {
                "tenant_id": TENANT_A,
                "content": "needle short",
                "title": "signal short"
            }
        },
        {
            "id": "acme-dense",
            "values": [1.0, 1.0],
            "attributes": {
                "tenant_id": TENANT_A,
                "content": "needle needle needle dense text",
                "title": "signal signal dense"
            }
        }
    ])
}

fn hidden_vectors() -> Value {
    json!([
        {
            "id": "bravo-long-no-match",
            "values": [2.0, 2.0],
            "attributes": {
                "tenant_id": TENANT_B,
                "content": "hidden words outside the visible corpus are deliberately very long"
            }
        },
        {
            "id": "bravo-query-match",
            "values": [3.0, 3.0],
            "attributes": {
                "tenant_id": TENANT_B,
                "content": "needle needle needle needle needle"
            }
        }
    ])
}

#[tokio::test]
async fn hidden_global_documents_cannot_change_prefix_multi_field_scores_or_order() {
    let fixture = setup().await;
    let mut initial = visible_vectors()
        .as_array()
        .expect("visible fixture must be an array")
        .clone();
    initial.push(json!({
        "id": "bravo-changing",
        "values": [2.0, 2.0],
        "attributes": {
            "tenant_id": TENANT_B,
            "content": "hidden baseline",
            "title": "private baseline"
        }
    }));
    upsert(&fixture, Value::Array(initial)).await;
    fixture
        .compactor
        .compact_with_fts(&fixture.namespace, None, &fts_configs())
        .await
        .expect("initial prefix FTS compaction must succeed");

    let query = json!({
        "rank_by": ["Sum", [
            ["content", "BM25", "need"],
            ["title", "BM25", "sig"]
        ]],
        "last_as_prefix": true,
        "top_k": 16,
        "consistency": "eventual"
    });
    let baseline = scored_results(&bm25_query_body(&fixture, query.clone()).await);
    assert_visible_fixture(&baseline);

    upsert(
        &fixture,
        json!([{
            "id": "bravo-changing",
            "values": [2.0, 2.0],
            "attributes": {
                "tenant_id": TENANT_B,
                "content": "needle needle needful hidden changed",
                "title": "signal signal significant private"
            }
        }]),
    )
    .await;
    fixture
        .compactor
        .compact_with_fts(&fixture.namespace, None, &fts_configs())
        .await
        .expect("changed prefix FTS compaction must succeed");

    let after_hidden_change = scored_results(&bm25_query_body(&fixture, query).await);
    assert_visible_fixture(&after_hidden_change);
    assert_eq!(
        after_hidden_change, baseline,
        "hidden prefix or second-field terms changed visible RankBy score bits"
    );

    cleanup_ns(&fixture.harness.store, &fixture.namespace).await;
    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn hidden_legacy_documents_cannot_change_scoped_segment_scores_or_breaker_outcome() {
    let fixture = setup().await;
    let mut initial = visible_vectors()
        .as_array()
        .expect("visible fixture must be an array")
        .clone();
    initial.push(json!({
        "id": "bravo-changing",
        "values": [2.0, 2.0],
        "attributes": {
            "tenant_id": TENANT_B,
            "content": "hidden baseline",
            "title": "private baseline"
        }
    }));
    upsert(&fixture, Value::Array(initial)).await;
    fixture
        .compactor
        .compact_with_fts(&fixture.namespace, None, &fts_configs())
        .await
        .expect("initial legacy FTS compaction must succeed");
    force_active_segment_legacy(&fixture).await;

    assert_eq!(
        caller_filtered_bm25_status(&fixture, "eventual").await,
        StatusCode::SERVICE_UNAVAILABLE,
        "a caller-only filter must retain the optimized legacy breaker path"
    );

    let baseline = scored_results(&bm25_query(&fixture, "eventual").await);
    assert_visible_fixture(&baseline);

    upsert(
        &fixture,
        json!([{
            "id": "bravo-changing",
            "values": [2.0, 2.0],
            "attributes": {
                "tenant_id": TENANT_B,
                "content": "needle needle needle needle hidden changed",
                "title": "signal private"
            }
        }]),
    )
    .await;
    fixture
        .compactor
        .compact_with_fts(&fixture.namespace, None, &fts_configs())
        .await
        .expect("changed legacy FTS compaction must succeed");
    force_active_segment_legacy(&fixture).await;

    let after_hidden_change = scored_results(&bm25_query(&fixture, "eventual").await);
    assert_visible_fixture(&after_hidden_change);
    assert_eq!(
        after_hidden_change, baseline,
        "hidden legacy rows changed visible segment-wide BM25 score bits"
    );

    cleanup_ns(&fixture.harness.store, &fixture.namespace).await;
    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn hidden_only_compaction_cannot_move_visible_scores_between_wal_and_segment_scorers() {
    let fixture = setup().await;
    let mut rows = visible_vectors()
        .as_array()
        .expect("visible fixture must be an array")
        .clone();
    rows.push(json!({
        "id": "bravo-hidden",
        "values": [2.0, 2.0],
        "attributes": {
            "tenant_id": TENANT_B,
            "content": "needle hidden corpus row with a very different length",
            "title": "signal private"
        }
    }));
    upsert(&fixture, Value::Array(rows)).await;

    let wal_scored = scored_results(&bm25_query(&fixture, "strong").await);
    assert_visible_fixture(&wal_scored);
    fixture
        .compactor
        .compact_with_fts(&fixture.namespace, None, &fts_configs())
        .await
        .expect("hidden-only tier transition compaction must succeed");

    let segment_scored = scored_results(&bm25_query(&fixture, "strong").await);
    assert_visible_fixture(&segment_scored);
    assert_eq!(
        segment_scored, wal_scored,
        "compaction changed visible score bits by switching scorer tiers"
    );

    cleanup_ns(&fixture.harness.store, &fixture.namespace).await;
    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn strong_hidden_wal_override_removes_stale_segment_row_before_scoped_statistics() {
    let fixture = setup().await;
    upsert(
        &fixture,
        json!([{
            "id": "acme-dense",
            "values": [1.0, 1.0],
            "attributes": {
                "tenant_id": TENANT_A,
                "content": "needle needle needle dense text",
                "title": "signal signal dense"
            }
        }]),
    )
    .await;
    fixture
        .compactor
        .compact_with_fts(&fixture.namespace, None, &fts_configs())
        .await
        .expect("single-row baseline compaction must succeed");
    let baseline = scored_results(&bm25_query(&fixture, "strong").await);
    assert_eq!(baseline.len(), 1);
    assert_eq!(baseline[0].0, "acme-dense");

    upsert(
        &fixture,
        json!([{
            "id": "moving-row",
            "values": [2.0, 2.0],
            "attributes": {
                "tenant_id": TENANT_A,
                "content": "needle short",
                "title": "signal short"
            }
        }]),
    )
    .await;
    fixture
        .compactor
        .compact_with_fts(&fixture.namespace, None, &fts_configs())
        .await
        .expect("two-row segment compaction must succeed");
    upsert(
        &fixture,
        json!([{
            "id": "moving-row",
            "values": [3.0, 3.0],
            "attributes": {
                "tenant_id": TENANT_B,
                "content": "needle needle needle hidden replacement",
                "title": "signal hidden replacement"
            }
        }]),
    )
    .await;

    let after_hidden_override = scored_results(&bm25_query(&fixture, "strong").await);
    assert_eq!(after_hidden_override.len(), 1);
    assert_eq!(after_hidden_override[0].0, "acme-dense");
    assert_eq!(
        after_hidden_override, baseline,
        "stale segment attributes influenced statistics before the out-of-scope WAL override"
    );

    cleanup_ns(&fixture.harness.store, &fixture.namespace).await;
    fixture.harness.cleanup().await;
}

fn assert_visible_fixture(scored: &[(String, u32)]) {
    let mut ids: Vec<&str> = scored.iter().map(|(id, _)| id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, ["acme-dense", "acme-short"]);
}

#[tokio::test]
async fn hidden_wal_documents_cannot_change_mandatory_filtered_bm25_scores_or_order() {
    let fixture = setup().await;
    upsert(&fixture, visible_vectors()).await;

    let baseline = bm25_query(&fixture, "strong").await;
    let baseline_scored = scored_results(&baseline);
    assert_visible_fixture(&baseline_scored);
    let wal_manifest = Manifest::read(&fixture.harness.store, &fixture.namespace)
        .await
        .expect("WAL manifest read must succeed")
        .expect("WAL manifest must exist");
    assert!(
        !wal_manifest.uncompacted_fragments().is_empty() && wal_manifest.active_segment.is_none(),
        "the isolation fixture must exercise uncompacted WAL scoring"
    );

    upsert(&fixture, hidden_vectors()).await;

    let after_hidden_insert = bm25_query(&fixture, "strong").await;
    let after_hidden_scored = scored_results(&after_hidden_insert);
    assert_visible_fixture(&after_hidden_scored);
    assert_eq!(
        after_hidden_scored, baseline_scored,
        "out-of-scope WAL rows changed visible BM25 ordering or score bits"
    );

    cleanup_ns(&fixture.harness.store, &fixture.namespace).await;
    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn caller_only_filter_retains_full_wal_bm25_corpus_statistics() {
    let fixture = setup().await;
    upsert(&fixture, visible_vectors()).await;

    let baseline = caller_filtered_bm25_query(&fixture, "strong").await;
    let baseline_scored = scored_results(&baseline);
    assert_visible_fixture(&baseline_scored);

    upsert(&fixture, hidden_vectors()).await;
    let after_hidden_insert = caller_filtered_bm25_query(&fixture, "strong").await;
    let after_hidden_scored = scored_results(&after_hidden_insert);
    assert_visible_fixture(&after_hidden_scored);
    assert_ne!(
        after_hidden_scored, baseline_scored,
        "a caller-only filter incorrectly rescoped historical full-corpus BM25 statistics"
    );

    cleanup_ns(&fixture.harness.store, &fixture.namespace).await;
    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn caller_filter_only_narrows_candidates_not_mandatory_bm25_corpus_statistics() {
    let fixture = setup().await;
    upsert(&fixture, visible_vectors()).await;

    let policy_corpus = scored_results(&bm25_query(&fixture, "strong").await);
    let expected = policy_corpus
        .iter()
        .find(|(id, _)| id == "acme-short")
        .cloned()
        .expect("policy-visible corpus must score acme-short");
    let caller_narrowed = scored_results(
        &bm25_query_body(
            &fixture,
            json!({
                "rank_by": ["content", "BM25", "needle"],
                "filter": {
                    "op": "eq",
                    "field": "title",
                    "value": "signal short"
                },
                "top_k": 16,
                "consistency": "strong"
            }),
        )
        .await,
    );

    assert_eq!(
        caller_narrowed,
        vec![expected],
        "caller filtering changed policy-scoped BM25 score bits instead of only narrowing candidates"
    );

    cleanup_ns(&fixture.harness.store, &fixture.namespace).await;
    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn hidden_compacted_document_changes_cannot_change_mandatory_filtered_global_bm25_scores_or_order(
) {
    let fixture = setup().await;
    let mut initial = visible_vectors()
        .as_array()
        .expect("visible fixture must be an array")
        .clone();
    initial.push(json!({
        "id": "bravo-changing",
        "values": [2.0, 2.0],
        "attributes": {
            "tenant_id": TENANT_B,
            "content": "hidden baseline without query term"
        }
    }));
    upsert(&fixture, Value::Array(initial)).await;

    let compacted = fixture
        .compactor
        .compact_with_fts(&fixture.namespace, None, &fts_configs())
        .await
        .expect("initial FTS compaction must succeed");
    assert!(compacted.segment_id.is_some());
    let manifest = Manifest::read(&fixture.harness.store, &fixture.namespace)
        .await
        .expect("compacted manifest read must succeed")
        .expect("compacted manifest must exist");
    let active_id = manifest
        .active_segment
        .as_deref()
        .expect("compaction must publish an active segment");
    let active = manifest
        .segments
        .iter()
        .find(|segment| segment.id == active_id)
        .expect("active segment must have a retained descriptor");
    assert!(
        active.has_global_fts,
        "the compacted isolation fixture must exercise the global FTS index"
    );

    let baseline = bm25_query(&fixture, "eventual").await;
    let baseline_scored = scored_results(&baseline);
    assert_visible_fixture(&baseline_scored);

    upsert(
        &fixture,
        json!([{
            "id": "bravo-changing",
            "values": [2.0, 2.0],
            "attributes": {
                "tenant_id": TENANT_B,
                "content": "needle needle needle needle needle hidden changed text"
            }
        }]),
    )
    .await;
    fixture
        .compactor
        .compact_with_fts(&fixture.namespace, None, &fts_configs())
        .await
        .expect("incremental FTS compaction must succeed");
    let changed_manifest = Manifest::read(&fixture.harness.store, &fixture.namespace)
        .await
        .expect("incrementally compacted manifest read must succeed")
        .expect("incrementally compacted manifest must exist");
    let changed_active_id = changed_manifest
        .active_segment
        .as_deref()
        .expect("incremental compaction must publish an active segment");
    assert!(
        changed_manifest
            .segments
            .iter()
            .find(|segment| segment.id == changed_active_id)
            .expect("incremental active segment must have a retained descriptor")
            .has_global_fts,
        "the post-change isolation query must still exercise global FTS"
    );

    let after_hidden_change = bm25_query(&fixture, "eventual").await;
    let after_hidden_scored = scored_results(&after_hidden_change);
    assert_visible_fixture(&after_hidden_scored);
    assert_eq!(
        after_hidden_scored, baseline_scored,
        "out-of-scope compacted rows changed visible BM25 ordering or score bits"
    );

    cleanup_ns(&fixture.harness.store, &fixture.namespace).await;
    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn mandatory_filtered_bm25_fails_when_active_segment_descriptor_is_missing() {
    let fixture = setup().await;
    upsert(&fixture, visible_vectors()).await;
    fixture
        .compactor
        .compact_with_fts(&fixture.namespace, None, &fts_configs())
        .await
        .expect("FTS compaction must succeed");

    let mut malformed = Manifest::read(&fixture.harness.store, &fixture.namespace)
        .await
        .expect("manifest read must succeed")
        .expect("manifest must exist");
    assert!(malformed.active_segment.is_some());
    malformed.segments.clear();
    malformed
        .write(&fixture.harness.store, &fixture.namespace)
        .await
        .expect("malformed authoritative manifest write must succeed for the regression fixture");

    let response = fixture
        .tenant
        .post(format!(
            "{}/v1/namespaces/{}/query",
            fixture.base_url, fixture.namespace
        ))
        .json(&json!({
            "rank_by": ["content", "BM25", "needle"],
            "top_k": 16,
            "consistency": "strong"
        }))
        .send()
        .await
        .expect("BM25 query must complete");
    let status = response.status();
    let body = response
        .text()
        .await
        .expect("BM25 failure body must be readable");
    assert!(
        status.is_server_error(),
        "malformed active segment reference must fail loud, got {status}: {body}"
    );

    cleanup_ns(&fixture.harness.store, &fixture.namespace).await;
    fixture.harness.cleanup().await;
}

#[tokio::test]
async fn scoped_bm25_artifact_survives_restart_without_rescanning_source_segment() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let server = start_test_server_full(
        store.clone(),
        Some(harness.prefix.clone()),
        fts_config(),
        false,
        None,
    )
    .await;
    let admin = client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api_fts(
        &admin,
        &server.base_url,
        2,
        json!({
            "content": {"stemming": false, "remove_stopwords": false},
            "title": {"stemming": false, "remove_stopwords": false}
        }),
    )
    .await;
    let tenant = create_tenant_principal(&admin, &server.base_url, &namespace).await;
    let response = admin
        .post(format!(
            "{}/v1/namespaces/{namespace}/vectors",
            server.base_url
        ))
        .json(&json!({"vectors": visible_vectors()}))
        .send()
        .await
        .expect("fixture upsert must complete");
    expect_json(response, StatusCode::OK, "fixture upsert").await;

    counter.reset();
    let _ = bm25_query_at(&tenant, &server.base_url, &namespace, "strong").await;
    assert_eq!(
        counter.puts_matching("/security_scopes/"),
        0,
        "mutable WAL-frontier scope indexes must remain bounded cache entries, not durable objects"
    );
    server.clear_decoded_artifact_cache();

    server
        .compactor
        .compact_with_fts(&namespace, None, &fts_configs())
        .await
        .expect("fixture FTS compaction must succeed");
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
    let before = bm25_query_at(&tenant, &server.base_url, &namespace, "eventual").await;
    assert_eq!(
        counter.create_puts_matching("/security_scopes/fts/"),
        1,
        "first scoped BM25 query must create-publish one immutable artifact"
    );
    assert!(
        !harness
            .store
            .list_prefix(&scope_prefix)
            .await
            .expect("scope artifact listing must succeed")
            .is_empty(),
        "scoped BM25 artifact must share its source segment's GC lifecycle prefix"
    );

    let admin_bearer = server.admin_bearer.clone();
    server.shutdown().await;
    let restarted = start_test_server_full_with_disk_cache_max_bytes_and_admin_bearer(
        store,
        Some(harness.prefix.clone()),
        fts_config(),
        false,
        None,
        100 * 1024 * 1024,
        &admin_bearer,
    )
    .await;
    counter.reset();
    let after = bm25_query_at(&tenant, &restarted.base_url, &namespace, "eventual").await;

    assert_eq!(
        after, before,
        "restart must preserve exact scoped BM25 output"
    );
    assert_eq!(
        counter.puts_matching("/security_scopes/"),
        0,
        "restart must load the published BM25 artifact without rebuilding it"
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
