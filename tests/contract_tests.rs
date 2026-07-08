mod common;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{TimeZone, Utc};
use common::server::{cleanup_ns, start_test_server_with_config};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::namespace::manager::{
    CompactionHealth, NamespaceIndexConfig, NamespaceMetadata, NamespaceState,
};
use zeppelin::types::{DistanceMetric, IndexType};
use zeppelin::wal::Manifest;

const FIXTURE_VERSION: &str = "v0.3.0";

const ROUTED_OPERATIONS: &[(&str, &str)] = &[
    ("get", "/healthz"),
    ("get", "/readyz"),
    ("get", "/metrics"),
    ("get", "/v1/config/query"),
    ("put", "/v1/config/query"),
    ("patch", "/v1/config/query"),
    ("post", "/v1/namespaces"),
    ("get", "/v1/namespaces/{ns}"),
    ("delete", "/v1/namespaces/{ns}"),
    ("get", "/v1/namespaces/{ns}/snapshots"),
    ("put", "/v1/namespaces/{ns}/snapshots/{name}"),
    ("get", "/v1/namespaces/{ns}/snapshots/{name}"),
    ("delete", "/v1/namespaces/{ns}/snapshots/{name}"),
    ("post", "/v1/namespaces/{ns}/clone"),
    ("patch", "/v1/namespaces/{ns}/index_config"),
    ("post", "/v1/namespaces/{ns}/compact"),
    ("get", "/v1/namespaces/{ns}/compact/status"),
    ("post", "/v1/namespaces/{ns}/hydrate"),
    ("post", "/v1/namespaces/{ns}/vectors"),
    ("delete", "/v1/namespaces/{ns}/vectors"),
    ("post", "/v1/namespaces/{ns}/vectors/get"),
    ("post", "/v1/namespaces/{ns}/query"),
    ("post", "/v1/namespaces/{ns}/query/batch"),
];

const FIXTURE_CASES: &[&str] = &[
    "create_namespace",
    "get_namespace",
    "patch_index_config",
    "get_query_config",
    "patch_query_config",
    "upsert_vectors",
    "get_vectors",
    "delete_vectors",
    "query_ann",
    "query_by_id",
    "query_hybrid_rrf",
    "query_weighted_fusion",
    "query_vector_rerank",
    "query_bm25_rerank",
    "query_cursor_page1",
    "query_cursor_page2",
    "query_grouping",
    "query_facets",
    "query_explain_plan",
    "query_explain_full",
    "batch_query",
    "compact_status",
    "compact_namespace_accepted",
    "hydrate_namespace_disabled",
    "delete_namespace_accepted",
    "error_validation_400",
    "error_not_found_404",
    "error_conflict_409",
    "error_deleting_410",
    "error_payload_too_large_413",
    "error_not_implemented_501",
];

#[derive(Debug, Clone)]
struct Fixture {
    name: &'static str,
    method: &'static str,
    path: String,
    status: u16,
    request: Value,
    response: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FixtureManifest {
    version: String,
    generated_by: String,
    cases: Vec<FixtureManifestCase>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FixtureManifestCase {
    name: String,
    method: String,
    path: String,
    status: u16,
    request: String,
    response: String,
}

#[test]
fn openapi_documents_exact_routed_surface() {
    let documented = documented_operations(include_str!("../api/zeppelin-api.yaml"));
    let routed = ROUTED_OPERATIONS
        .iter()
        .map(|(method, path)| ((*method).to_string(), (*path).to_string()))
        .collect::<BTreeSet<_>>();

    let missing = routed.difference(&documented).cloned().collect::<Vec<_>>();
    let undocumented = documented.difference(&routed).cloned().collect::<Vec<_>>();

    assert!(
        missing.is_empty() && undocumented.is_empty(),
        "OpenAPI route drift\nmissing from api.yaml: {missing:#?}\nnot routed: {undocumented:#?}"
    );
}

#[test]
fn contract_fixture_inventory_is_complete() {
    if std::env::var("UPDATE_CONTRACT_FIXTURES").as_deref() == Ok("1") {
        return;
    }
    let fixture_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("contract")
        .join("fixtures")
        .join(FIXTURE_VERSION);
    assert!(
        fixture_root.is_dir(),
        "fixture directory missing: {}",
        fixture_root.display()
    );
    assert!(
        fixture_root.join("manifest.json").is_file(),
        "fixture manifest missing"
    );

    for case in FIXTURE_CASES {
        let req = fixture_root.join(format!("{case}.req.json"));
        let resp = fixture_root.join(format!("{case}.resp.json"));
        assert!(req.is_file(), "request fixture missing: {}", req.display());
        assert!(
            resp.is_file(),
            "response fixture missing: {}",
            resp.display()
        );
    }
}

#[tokio::test]
async fn contract_fixtures_match_real_engine_output() {
    let fixtures = build_contract_fixtures().await;
    assert_eq!(
        fixtures.len(),
        FIXTURE_CASES.len(),
        "fixture builder and inventory diverged"
    );

    let names = fixtures
        .iter()
        .map(|fixture| fixture.name)
        .collect::<BTreeSet<_>>();
    let expected = FIXTURE_CASES.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(names, expected, "fixture builder missed a curated case");

    let api = include_str!("../api/zeppelin-api.yaml");
    for fixture in &fixtures {
        assert_operation_status_documented(api, fixture);
        assert_response_contract_shape(fixture);
    }

    write_or_compare_fixtures(&fixtures);
}

fn documented_operations(api: &str) -> BTreeSet<(String, String)> {
    let mut operations = BTreeSet::new();
    let mut in_paths = false;
    let mut current_path: Option<String> = None;

    for line in api.lines() {
        if line == "paths:" {
            in_paths = true;
            continue;
        }
        if line == "components:" {
            break;
        }
        if !in_paths {
            continue;
        }
        if let Some(path) = line
            .strip_prefix("  /")
            .and_then(|rest| rest.strip_suffix(':'))
        {
            current_path = Some(format!("/{path}"));
            continue;
        }
        let Some(path) = current_path.as_ref() else {
            continue;
        };
        let Some(method) = line
            .strip_prefix("    ")
            .and_then(|rest| rest.strip_suffix(':'))
        else {
            continue;
        };
        if matches!(method, "get" | "post" | "put" | "patch" | "delete") {
            operations.insert((method.to_string(), path.clone()));
        }
    }

    operations
}

async fn build_contract_fixtures() -> Vec<Fixture> {
    let mut config = Config::load(None).unwrap();
    config.indexing.default_num_centroids = 4;
    config.indexing.default_nprobe = 4;
    config.indexing.max_nprobe = 32;
    config.server.default_top_k = 3;
    config.server.max_top_k = 100;
    config.server.max_query_batch_size = 2;

    let (base_url, harness, _cache, cache_dir) = start_test_server_with_config(Some(config)).await;
    let client = reqwest::Client::new();

    let main_ns = format!("{}-contract-main", harness.prefix);
    let compact_ns = format!("{}-contract-compact", harness.prefix);
    let delete_ns = format!("{}-contract-delete", harness.prefix);
    let conflict_ns = format!("{}-contract-conflict", harness.prefix);
    let deleting_ns = format!("{}-contract-deleting", harness.prefix);
    let missing_ns = format!("{}-contract-missing", harness.prefix);

    let replacements = vec![
        (main_ns.clone(), "contract-main".to_string()),
        (compact_ns.clone(), "contract-compact".to_string()),
        (delete_ns.clone(), "contract-delete".to_string()),
        (conflict_ns.clone(), "contract-conflict".to_string()),
        (deleting_ns.clone(), "contract-deleting".to_string()),
        (missing_ns.clone(), "contract-missing".to_string()),
    ];

    let mut fixtures = Vec::new();

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "create_namespace",
            "post",
            "/v1/namespaces",
            "/v1/namespaces",
            201,
            json!({
                "name": main_ns,
                "dimensions": 2,
                "distance_metric": "euclidean",
                "full_text_search": {"title": {}}
            }),
            json!({
                "name": "contract-main",
                "dimensions": 2,
                "distance_metric": "euclidean",
                "full_text_search": {"title": {}}
            }),
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "get_query_config",
            "get",
            "/v1/config/query",
            "/v1/config/query",
            200,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "patch_query_config",
            "patch",
            "/v1/config/query",
            "/v1/config/query",
            200,
            json!({
                "default_top_k": 3,
                "cost_latency_profile": "low_latency"
            }),
            json!({
                "default_top_k": 3,
                "cost_latency_profile": "low_latency"
            }),
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "patch_index_config",
            "patch",
            &format!("/v1/namespaces/{main_ns}/index_config"),
            "/v1/namespaces/contract-main/index_config",
            202,
            json!({
                "nlist": 4,
                "quantization": "scalar",
                "bitmap_index": true
            }),
            json!({
                "nlist": 4,
                "quantization": "scalar",
                "bitmap_index": true
            }),
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "upsert_vectors",
            "post",
            &format!("/v1/namespaces/{main_ns}/vectors"),
            "/v1/namespaces/contract-main/vectors",
            200,
            main_vectors(),
            main_vectors_canonical(),
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "get_namespace",
            "get",
            &format!("/v1/namespaces/{main_ns}"),
            "/v1/namespaces/contract-main",
            200,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "get_vectors",
            "post",
            &format!("/v1/namespaces/{main_ns}/vectors/get"),
            "/v1/namespaces/contract-main/vectors/get",
            200,
            json!({
                "ids": ["seed", "near", "missing"],
                "include_vector": true,
                "include_attributes": true,
                "attribute_fields": ["category", "title"],
                "consistency": "strong"
            }),
            json!({
                "ids": ["seed", "near", "missing"],
                "include_vector": true,
                "include_attributes": true,
                "attribute_fields": ["category", "title"],
                "consistency": "strong"
            }),
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.extend(query_fixtures(&client, &base_url, &main_ns, &replacements).await);

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "delete_vectors",
            "delete",
            &format!("/v1/namespaces/{main_ns}/vectors"),
            "/v1/namespaces/contract-main/vectors",
            204,
            json!({"ids": ["other"]}),
            json!({"ids": ["other"]}),
            &replacements,
            &[],
        )
        .await,
    );

    setup_namespace(
        &client,
        &base_url,
        &compact_ns,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    send_json(
        &client,
        &base_url,
        "post",
        &format!("/v1/namespaces/{compact_ns}/vectors"),
        "setup_compact_vectors",
        json!({
            "vectors": [{"id": "compact-a", "values": [0.0, 0.0]}]
        }),
    )
    .await;

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "compact_status",
            "get",
            &format!("/v1/namespaces/{compact_ns}/compact/status"),
            "/v1/namespaces/contract-compact/compact/status",
            200,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );
    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "compact_namespace_accepted",
            "post",
            &format!("/v1/namespaces/{compact_ns}/compact"),
            "/v1/namespaces/contract-compact/compact",
            202,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "hydrate_namespace_disabled",
            "post",
            &format!("/v1/namespaces/{main_ns}/hydrate"),
            "/v1/namespaces/contract-main/hydrate",
            409,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );

    setup_namespace(
        &client,
        &base_url,
        &delete_ns,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    fixtures.push(
        capture_json(
            &client,
            &base_url,
            "delete_namespace_accepted",
            "delete",
            &format!("/v1/namespaces/{delete_ns}"),
            "/v1/namespaces/contract-delete",
            202,
            Value::Null,
            Value::Null,
            &replacements,
            &[],
        )
        .await,
    );

    setup_namespace(
        &client,
        &base_url,
        &conflict_ns,
        json!({"dimensions": 2, "distance_metric": "euclidean"}),
    )
    .await;
    seed_deleting_namespace(&harness.store, &deleting_ns).await;

    fixtures.extend(
        error_fixtures(
            &client,
            &base_url,
            &main_ns,
            &conflict_ns,
            &deleting_ns,
            &missing_ns,
            &replacements,
        )
        .await,
    );

    for ns in [
        &main_ns,
        &compact_ns,
        &delete_ns,
        &conflict_ns,
        &deleting_ns,
    ] {
        cleanup_ns(&harness.store, ns).await;
    }
    harness.cleanup().await;
    drop(cache_dir);

    fixtures
}

async fn query_fixtures(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    replacements: &[(String, String)],
) -> Vec<Fixture> {
    let actual_path = format!("/v1/namespaces/{ns}/query");
    let canonical_path = "/v1/namespaces/contract-main/query";
    let mut fixtures = Vec::new();

    let ann = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_ann",
            "post",
            &actual_path,
            canonical_path,
            200,
            ann.clone(),
            ann,
            replacements,
            &[],
        )
        .await,
    );

    let by_id = json!({
        "sources": [{"type": "ann", "id": "seed"}],
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_by_id",
            "post",
            &actual_path,
            canonical_path,
            200,
            by_id.clone(),
            by_id,
            replacements,
            &[],
        )
        .await,
    );

    let hybrid = json!({
        "sources": [
            {"type": "ann", "vector": [0.0, 0.0]},
            {"type": "bm25", "rank_by": ["title", "BM25", "search"]}
        ],
        "fusion": {"type": "rrf", "k": 20},
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_hybrid_rrf",
            "post",
            &actual_path,
            canonical_path,
            200,
            hybrid.clone(),
            hybrid,
            replacements,
            &[],
        )
        .await,
    );

    let weighted = json!({
        "sources": [
            {"type": "ann", "vector": [0.0, 0.0]},
            {"type": "bm25", "rank_by": ["title", "BM25", "search"]}
        ],
        "fusion": {"type": "weighted", "weights": [0.7, 0.3]},
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_weighted_fusion",
            "post",
            &actual_path,
            canonical_path,
            200,
            weighted.clone(),
            weighted,
            replacements,
            &[],
        )
        .await,
    );

    let vector_rerank = json!({
        "sources": [{"type": "ann", "vector": [0.6, 0.0]}],
        "rerank": {"type": "vector", "vector": [0.0, 0.0]},
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_vector_rerank",
            "post",
            &actual_path,
            canonical_path,
            200,
            vector_rerank.clone(),
            vector_rerank,
            replacements,
            &[],
        )
        .await,
    );

    let bm25_rerank = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "rerank": {"type": "bm25", "rank_by": ["title", "BM25", "search"]},
        "top_k": 3,
        "candidate_k": 5,
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_bm25_rerank",
            "post",
            &actual_path,
            canonical_path,
            200,
            bm25_rerank.clone(),
            bm25_rerank,
            replacements,
            &[],
        )
        .await,
    );

    let cursor_page1 = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 3,
        "cursor": {"type": "none"},
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    let (page1_status, mut page1_response) = send_json(
        client,
        base_url,
        "post",
        &actual_path,
        "query_cursor_page1",
        cursor_page1.clone(),
    )
    .await;
    assert_eq!(
        page1_status, 200,
        "fixture query_cursor_page1 expected status 200, got {page1_status}: {page1_response}"
    );
    let cursor = page1_response
        .get("next_cursor")
        .and_then(Value::as_str)
        .expect("cursor page 1 must return a cursor")
        .to_string();
    normalize_contract_value(
        &mut page1_response,
        replacements,
        std::slice::from_ref(&cursor),
    );
    fixtures.push(Fixture {
        name: "query_cursor_page1",
        method: "post",
        path: canonical_path.to_string(),
        status: page1_status,
        request: cursor_page1,
        response: page1_response,
    });

    let cursor_page2_actual = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 3,
        "cursor": {"type": "after", "token": cursor},
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    let cursor_page2_canonical = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 3,
        "cursor": {"type": "after", "token": "contract-cursor-page-1"},
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_cursor_page2",
            "post",
            &actual_path,
            canonical_path,
            200,
            cursor_page2_actual,
            cursor_page2_canonical,
            replacements,
            &[],
        )
        .await,
    );

    let grouping = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 2,
        "candidate_k": 5,
        "grouping": {"type": "field", "field": "category", "max_per_group": 2},
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_grouping",
            "post",
            &actual_path,
            canonical_path,
            200,
            grouping.clone(),
            grouping,
            replacements,
            &[],
        )
        .await,
    );

    let facets = json!({
        "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
        "top_k": 3,
        "candidate_k": 5,
        "facets": ["category", "tags"],
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_facets",
            "post",
            &actual_path,
            canonical_path,
            200,
            facets.clone(),
            facets,
            replacements,
            &[],
        )
        .await,
    );

    let explain_plan = json!({
        "sources": [
            {"type": "ann", "vector": [0.0, 0.0]},
            {"type": "bm25", "rank_by": ["title", "BM25", "search"]}
        ],
        "fusion": {"type": "rrf", "k": 20},
        "top_k": 3,
        "candidate_k": 5,
        "explain": "plan",
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_explain_plan",
            "post",
            &actual_path,
            canonical_path,
            200,
            explain_plan.clone(),
            explain_plan,
            replacements,
            &[],
        )
        .await,
    );

    let explain_full = json!({
        "sources": [
            {"type": "ann", "vector": [0.0, 0.0]},
            {"type": "bm25", "rank_by": ["title", "BM25", "search"]}
        ],
        "fusion": {"type": "weighted", "weights": [0.5, 0.5]},
        "top_k": 3,
        "candidate_k": 5,
        "explain": "full",
        "consistency": "strong"
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "query_explain_full",
            "post",
            &actual_path,
            canonical_path,
            200,
            explain_full.clone(),
            explain_full,
            replacements,
            &[],
        )
        .await,
    );

    let batch = json!({
        "queries": [
            {
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "top_k": 1,
                "consistency": "strong"
            },
            {
                "sources": [{"type": "ann", "vector": [0.0]}],
                "top_k": 1,
                "consistency": "strong"
            }
        ]
    });
    fixtures.push(
        capture_json(
            client,
            base_url,
            "batch_query",
            "post",
            &format!("/v1/namespaces/{ns}/query/batch"),
            "/v1/namespaces/contract-main/query/batch",
            200,
            batch.clone(),
            batch,
            replacements,
            &[],
        )
        .await,
    );

    fixtures
}

async fn error_fixtures(
    client: &reqwest::Client,
    base_url: &str,
    main_ns: &str,
    conflict_ns: &str,
    deleting_ns: &str,
    missing_ns: &str,
    replacements: &[(String, String)],
) -> Vec<Fixture> {
    let mut fixtures = Vec::new();

    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_validation_400",
            "post",
            &format!("/v1/namespaces/{main_ns}/query"),
            "/v1/namespaces/contract-main/query",
            400,
            json!({
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "top_k": 0
            }),
            json!({
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "top_k": 0
            }),
            replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_not_found_404",
            "get",
            &format!("/v1/namespaces/{missing_ns}"),
            "/v1/namespaces/contract-missing",
            404,
            Value::Null,
            Value::Null,
            replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_conflict_409",
            "post",
            "/v1/namespaces",
            "/v1/namespaces",
            409,
            json!({
                "name": conflict_ns,
                "dimensions": 3,
                "distance_metric": "euclidean"
            }),
            json!({
                "name": "contract-conflict",
                "dimensions": 3,
                "distance_metric": "euclidean"
            }),
            replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_deleting_410",
            "post",
            &format!("/v1/namespaces/{deleting_ns}/vectors"),
            "/v1/namespaces/contract-deleting/vectors",
            410,
            json!({
                "vectors": [{"id": "blocked", "values": [0.0, 0.0]}]
            }),
            json!({
                "vectors": [{"id": "blocked", "values": [0.0, 0.0]}]
            }),
            replacements,
            &[],
        )
        .await,
    );

    let too_many_queries = (0..3)
        .map(|_| json!({"sources": [{"type": "ann", "vector": [0.0, 0.0]}]}))
        .collect::<Vec<_>>();
    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_payload_too_large_413",
            "post",
            &format!("/v1/namespaces/{main_ns}/query/batch"),
            "/v1/namespaces/contract-main/query/batch",
            413,
            json!({"queries": too_many_queries}),
            json!({
                "queries": (0..3)
                    .map(|_| json!({"sources": [{"type": "ann", "vector": [0.0, 0.0]}]}))
                    .collect::<Vec<_>>()
            }),
            replacements,
            &[],
        )
        .await,
    );

    fixtures.push(
        capture_json(
            client,
            base_url,
            "error_not_implemented_501",
            "post",
            &format!("/v1/namespaces/{main_ns}/query"),
            "/v1/namespaces/contract-main/query",
            501,
            json!({
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "projection": {"include_vectors": true}
            }),
            json!({
                "sources": [{"type": "ann", "vector": [0.0, 0.0]}],
                "projection": {"include_vectors": true}
            }),
            replacements,
            &[],
        )
        .await,
    );

    fixtures
}

#[allow(clippy::too_many_arguments)]
async fn capture_json(
    client: &reqwest::Client,
    base_url: &str,
    name: &'static str,
    method: &'static str,
    actual_path: &str,
    canonical_path: &str,
    expected_status: u16,
    actual_request: Value,
    canonical_request: Value,
    replacements: &[(String, String)],
    cursor_tokens: &[String],
) -> Fixture {
    let (status, mut response) =
        send_json(client, base_url, method, actual_path, name, actual_request).await;
    normalize_contract_value(&mut response, replacements, cursor_tokens);
    assert_eq!(
        status, expected_status,
        "fixture {name} expected status {expected_status}, got {status}: {response}"
    );
    Fixture {
        name,
        method,
        path: canonical_path.to_string(),
        status,
        request: canonical_request,
        response,
    }
}

async fn send_json(
    client: &reqwest::Client,
    base_url: &str,
    method: &str,
    path: &str,
    name: &str,
    request: Value,
) -> (u16, Value) {
    let method = method.to_uppercase();
    let method = Method::from_bytes(method.as_bytes()).unwrap();
    let mut builder = client
        .request(method, format!("{base_url}{path}"))
        .header("x-request-id", format!("contract-{name}"));
    if !request.is_null() {
        builder = builder.json(&request);
    }
    let response = builder.send().await.unwrap();
    let status = response.status().as_u16();
    let bytes = response.bytes().await.unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|err| panic!("fixture {name} response is not JSON: {err}"))
    };
    (status, body)
}

async fn setup_namespace(client: &reqwest::Client, base_url: &str, ns: &str, mut body: Value) {
    body["name"] = json!(ns);
    let (status, response) = send_json(
        client,
        base_url,
        "post",
        "/v1/namespaces",
        "setup_namespace",
        body,
    )
    .await;
    assert!(
        status == 201 || status == 200,
        "setup namespace {ns} failed with {status}: {response}"
    );
}

async fn seed_deleting_namespace(store: &zeppelin::storage::ZeppelinStore, ns: &str) {
    let timestamp = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let meta = NamespaceMetadata {
        name: ns.to_string(),
        dimensions: 2,
        distance_metric: DistanceMetric::Euclidean,
        index_type: IndexType::default(),
        vector_count: 0,
        created_at: timestamp,
        updated_at: timestamp,
        state: NamespaceState::Deleting,
        full_text_search: Default::default(),
        index_config: Some(NamespaceIndexConfig::from_indexing_config(
            &Config::default().indexing,
        )),
        compaction_health: CompactionHealth::default(),
    };
    store
        .put(&NamespaceMetadata::s3_key(ns), meta.to_bytes().unwrap())
        .await
        .unwrap();
    Manifest::new().write(store, ns).await.unwrap();
}

fn normalize_contract_value(
    value: &mut Value,
    replacements: &[(String, String)],
    cursor_tokens: &[String],
) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                match key.as_str() {
                    "created_at" | "updated_at" => {
                        if child.is_string() {
                            *child = json!("2026-01-01T00:00:00+00:00");
                        }
                    }
                    "last_compaction_at" => {
                        if child.is_string() {
                            *child = json!("2026-01-01T00:00:00+00:00");
                        }
                    }
                    "latency_ms" | "wal_ms" | "segment_ms" | "merge_ms" => {
                        if child.is_number() {
                            *child = json!(0);
                        }
                    }
                    "next_cursor" => {
                        if child.is_string() {
                            *child = json!("contract-cursor-page-1");
                        }
                    }
                    "active_segment" | "segment_id" => {
                        if child.is_string() {
                            *child = json!("contract-segment");
                        }
                    }
                    _ => normalize_contract_value(child, replacements, cursor_tokens),
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_contract_value(item, replacements, cursor_tokens);
            }
        }
        Value::String(text) => {
            for (actual, canonical) in replacements {
                *text = text.replace(actual, canonical);
            }
            for token in cursor_tokens {
                if text == token {
                    *text = "contract-cursor-page-1".to_string();
                }
            }
        }
        _ => {}
    }
}

fn main_vectors() -> Value {
    json!({
        "vectors": [
            {
                "id": "seed",
                "values": [0.0, 0.0],
                "attributes": {
                    "category": "anchor",
                    "tags": ["blue"],
                    "title": "anchor search"
                }
            },
            {
                "id": "near",
                "values": [0.1, 0.0],
                "attributes": {
                    "category": "alpha",
                    "tags": ["fresh", "red"],
                    "title": "alpha search"
                }
            },
            {
                "id": "mid",
                "values": [0.2, 0.0],
                "attributes": {
                    "category": "alpha",
                    "tags": ["fresh", "blue"],
                    "title": "beta search"
                }
            },
            {
                "id": "far",
                "values": [1.0, 0.0],
                "attributes": {
                    "category": "beta",
                    "tags": ["archive"],
                    "title": "gamma archive"
                }
            },
            {
                "id": "other",
                "values": [0.0, 1.0],
                "attributes": {
                    "category": "beta",
                    "tags": ["fresh"],
                    "title": "search document"
                }
            }
        ]
    })
}

fn main_vectors_canonical() -> Value {
    main_vectors()
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("contract")
        .join("fixtures")
        .join(FIXTURE_VERSION)
}

fn write_or_compare_fixtures(fixtures: &[Fixture]) {
    let fixture_root = fixture_root();
    let manifest = FixtureManifest {
        version: FIXTURE_VERSION.to_string(),
        generated_by: "cargo test --test contract_tests contract_fixtures_match_real_engine_output"
            .to_string(),
        cases: fixtures
            .iter()
            .map(|fixture| FixtureManifestCase {
                name: fixture.name.to_string(),
                method: fixture.method.to_uppercase(),
                path: fixture.path.clone(),
                status: fixture.status,
                request: format!("{}.req.json", fixture.name),
                response: format!("{}.resp.json", fixture.name),
            })
            .collect(),
    };

    if std::env::var("UPDATE_CONTRACT_FIXTURES").as_deref() == Ok("1") {
        fs::create_dir_all(&fixture_root).unwrap();
        write_json_file(&fixture_root.join("manifest.json"), &manifest);
        for fixture in fixtures {
            write_json_file(
                &fixture_root.join(format!("{}.req.json", fixture.name)),
                &fixture.request,
            );
            write_json_file(
                &fixture_root.join(format!("{}.resp.json", fixture.name)),
                &fixture.response,
            );
        }
        return;
    }

    let actual_manifest: FixtureManifest = read_json_file(&fixture_root.join("manifest.json"))
        .unwrap_or_else(|err| {
            panic!(
                "failed to read fixture manifest {}: {err}",
                fixture_root.join("manifest.json").display()
            )
        });
    assert_eq!(actual_manifest, manifest, "fixture manifest drifted");

    for fixture in fixtures {
        let request_path = fixture_root.join(format!("{}.req.json", fixture.name));
        let response_path = fixture_root.join(format!("{}.resp.json", fixture.name));
        let request: Value = read_json_file(&request_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", request_path.display()));
        let response: Value = read_json_file(&response_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", response_path.display()));
        assert_eq!(
            request, fixture.request,
            "{} request fixture drifted",
            fixture.name
        );
        assert_eq!(
            response, fixture.response,
            "{} response fixture drifted",
            fixture.name
        );
    }
}

fn write_json_file<T: Serialize>(path: &Path, value: &T) {
    let mut body = serde_json::to_string_pretty(value).unwrap();
    body.push('\n');
    fs::write(path, body).unwrap();
}

fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> std::io::Result<T> {
    let body = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&body).unwrap())
}

fn assert_operation_status_documented(api: &str, fixture: &Fixture) {
    let statuses = documented_statuses(api, fixture.method, &fixture.openapi_path());
    assert!(
        statuses.contains(&fixture.status),
        "fixture {} uses undocumented status {} for {} {} (documented: {statuses:?})",
        fixture.name,
        fixture.status,
        fixture.method,
        fixture.openapi_path(),
    );
}

fn assert_response_contract_shape(fixture: &Fixture) {
    if fixture.status == 204 {
        assert!(
            fixture.response.is_null(),
            "{} 204 response must have no JSON body",
            fixture.name
        );
        return;
    }
    if fixture.status >= 400 {
        assert_error_envelope(&fixture.response, fixture.status, fixture.name);
        return;
    }
    if fixture.name.starts_with("query_") {
        assert_query_response(&fixture.response, fixture.name);
    }
    if fixture.name == "batch_query" {
        let results = fixture.response["results"].as_array().unwrap();
        assert!(!results.is_empty(), "batch fixture must include entries");
        assert!(results.iter().any(|entry| entry["ok"] == true));
        assert!(results.iter().any(|entry| entry["ok"] == false));
    }
    if fixture.name.ends_with("query_config") {
        for key in [
            "rerank_coalesce_gap_bytes",
            "default_nprobe",
            "default_top_k",
            "bm25_max_full_scan_clusters",
            "bm25_max_full_scan_vectors",
        ] {
            assert!(
                fixture.response.get(key).is_some(),
                "{} missing runtime query knob {key}",
                fixture.name
            );
        }
    }
}

fn assert_query_response(response: &Value, name: &str) {
    assert!(
        response["results"].is_array(),
        "{name} missing results array"
    );
    assert!(
        response["scanned_fragments"].is_u64(),
        "{name} missing scanned_fragments"
    );
    assert!(
        response["scanned_segments"].is_u64(),
        "{name} missing scanned_segments"
    );
}

fn assert_error_envelope(response: &Value, status: u16, name: &str) {
    assert!(response["code"].is_string(), "{name} missing code");
    assert!(response["error"].is_string(), "{name} missing error");
    assert_eq!(response["status"].as_u64(), Some(u64::from(status)));
    assert!(
        response["retryable"].is_boolean(),
        "{name} missing retryable"
    );
}

fn documented_statuses(api: &str, method: &str, path: &str) -> BTreeSet<u16> {
    let mut statuses = BTreeSet::new();
    let mut in_target_path = false;
    let mut in_target_method = false;
    let mut in_responses = false;
    let path_line = format!("  {path}:");
    let method_line = format!("    {method}:");

    for line in api.lines() {
        if line.starts_with("  /") {
            in_target_path = line == path_line;
            in_target_method = false;
            in_responses = false;
            continue;
        }
        if !in_target_path {
            continue;
        }
        if line.starts_with("    ") && !line.starts_with("      ") {
            in_target_method = line == method_line;
            in_responses = false;
            continue;
        }
        if !in_target_method {
            continue;
        }
        if line == "      responses:" {
            in_responses = true;
            continue;
        }
        if !in_responses {
            continue;
        }
        if let Some(status) = line
            .trim()
            .strip_prefix('"')
            .and_then(|rest| rest.split_once('"'))
            .and_then(|(status, _)| status.parse::<u16>().ok())
        {
            statuses.insert(status);
        }
    }

    statuses
}

impl Fixture {
    fn openapi_path(&self) -> String {
        self.path
            .replace("contract-main", "{ns}")
            .replace("contract-compact", "{ns}")
            .replace("contract-delete", "{ns}")
            .replace("contract-deleting", "{ns}")
            .replace("contract-missing", "{ns}")
    }
}
