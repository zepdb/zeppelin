mod common;

use common::server::{cleanup_ns, create_ns_api_with, start_test_server};
use serde_json::{json, Value};

fn fts_config() -> Value {
    json!({
        "content": {
            "stemming": false,
            "remove_stopwords": false
        }
    })
}

async fn create_hybrid_namespace(client: &reqwest::Client, base_url: &str) -> String {
    create_ns_api_with(
        client,
        base_url,
        json!({
            "dimensions": 2,
            "distance_metric": "euclidean",
            "full_text_search": fts_config(),
        }),
    )
    .await
}

async fn upsert(client: &reqwest::Client, base_url: &str, ns: &str, vectors: Value) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "upsert failed: {vectors}");
}

async fn query(client: &reqwest::Client, base_url: &str, ns: &str, body: Value) -> Value {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "query failed: {}",
        resp.text().await.unwrap()
    );
    resp.json().await.unwrap()
}

fn base_sources() -> Value {
    json!([
        {
            "type": "ann",
            "vector": [0.0, 0.0],
            "nprobe": 3
        },
        {
            "type": "bm25",
            "rank_by": ["content", "BM25", "hybrid"]
        }
    ])
}

async fn seed_fixture(client: &reqwest::Client, base_url: &str, ns: &str) {
    upsert(
        client,
        base_url,
        ns,
        json!([
            {
                "id": "doc-ann",
                "values": [0.0, 0.0],
                "attributes": {"content": "plain vector"}
            },
            {
                "id": "doc-middle",
                "values": [0.1, 0.0],
                "attributes": {"content": "hybrid"}
            },
            {
                "id": "doc-bm25",
                "values": [10.0, 10.0],
                "attributes": {"content": "hybrid hybrid hybrid hybrid"}
            }
        ]),
    )
    .await;
}

fn strip_explain(mut body: Value) -> Value {
    body.as_object_mut().unwrap().remove("explain");
    body
}

#[tokio::test]
async fn explain_true_returns_plan_without_changing_results() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_hybrid_namespace(&client, &base_url).await;
    seed_fixture(&client, &base_url, &ns).await;

    let request = json!({
        "sources": base_sources(),
        "fusion": {"type": "rrf", "k": 60},
        "candidate_k": 2,
        "top_k": 3,
        "consistency": "strong",
        "projection": {"include_attributes": false}
    });
    let without_explain = query(&client, &base_url, &ns, request.clone()).await;
    let mut explain_request = request;
    explain_request["explain"] = json!(true);
    let with_explain = query(&client, &base_url, &ns, explain_request).await;

    assert_eq!(strip_explain(with_explain.clone()), without_explain);
    assert_eq!(with_explain["explain"]["mode"], "plan");
    assert!(with_explain["explain"].get("results").is_none());
    assert_eq!(with_explain["explain"]["plan"]["path"], "algebra_hybrid");
    assert_eq!(with_explain["explain"]["plan"]["candidate_k"], 2);
    assert_eq!(with_explain["explain"]["plan"]["top_k"], 3);
    assert_eq!(with_explain["explain"]["plan"]["consistency"], "strong");
    assert_eq!(
        with_explain["explain"]["plan"]["fusion"],
        json!({"type": "rrf", "k": 60})
    );
    assert_eq!(with_explain["explain"]["plan"]["sources"][0]["type"], "ann");
    assert_eq!(with_explain["explain"]["plan"]["sources"][0]["nprobe"], 3);
    assert_eq!(
        with_explain["explain"]["plan"]["sources"][1]["type"],
        "bm25"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn explain_full_includes_source_and_rerank_provenance() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_hybrid_namespace(&client, &base_url).await;
    seed_fixture(&client, &base_url, &ns).await;

    let body = query(
        &client,
        &base_url,
        &ns,
        json!({
            "sources": base_sources(),
            "fusion": {"type": "weighted", "weights": [1.0, 1.0]},
            "rerank": {"type": "vector", "vector": [0.0, 0.0]},
            "candidate_k": 3,
            "top_k": 3,
            "consistency": "strong",
            "projection": {"include_attributes": false},
            "explain": "full"
        }),
    )
    .await;

    assert_eq!(body["explain"]["mode"], "full");
    assert_eq!(body["explain"]["plan"]["rerank"], json!({"type": "vector"}));
    let result_count = body["results"].as_array().unwrap().len();
    assert_eq!(
        body["explain"]["results"].as_array().unwrap().len(),
        result_count
    );
    let middle = body["explain"]["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|result| result["id"] == "doc-middle")
        .expect("doc-middle should be returned");
    assert_eq!(middle["sources"].as_array().unwrap().len(), 2);
    assert!(middle["sources"][0]["raw_score"].is_number());
    assert!(middle["sources"][0]["contribution"].is_number());
    assert!(middle["fused_score"].is_number());
    assert!(middle["rerank_score"].is_number());

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

#[tokio::test]
async fn explain_false_and_none_are_omitted() {
    let (base_url, harness, admin_bearer) = start_test_server().await;
    let client = crate::common::server::client_with_bearer(&admin_bearer);
    let ns = create_hybrid_namespace(&client, &base_url).await;
    seed_fixture(&client, &base_url, &ns).await;

    for explain in [json!(false), json!("none")] {
        let body = query(
            &client,
            &base_url,
            &ns,
            json!({
                "sources": [base_sources()[0].clone()],
                "top_k": 1,
                "consistency": "strong",
                "projection": {"include_attributes": false},
                "explain": explain
            }),
        )
        .await;
        assert!(body.get("explain").is_none());
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
