mod common;

use common::server::{api_ns, cleanup_ns, start_test_server_with_compactor};

use std::collections::HashMap;
use zeppelin::config::{CompactionConfig, Config, IndexingConfig};
use zeppelin::fts::types::FtsFieldConfig;
use zeppelin::types::{AttributeValue, VectorEntry};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Test config with small centroids for fast compaction and FTS indexing enabled.
fn fts_test_config() -> Config {
    let mut config = Config::load(None).unwrap();
    config.compaction = CompactionConfig {
        max_wal_fragments_before_compact: 3,
        ..Default::default()
    };
    config.indexing = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        fts_index: true,
        bitmap_index: false,
        ..Default::default()
    };
    config
}

/// Create a VectorEntry with a single "content" text attribute.
fn content_doc(id: &str, text: &str) -> VectorEntry {
    let mut attrs = HashMap::new();
    attrs.insert(
        "content".to_string(),
        AttributeValue::String(text.to_string()),
    );
    VectorEntry {
        id: id.to_string(),
        values: vec![0.1, 0.2, 0.3, 0.4],
        attributes: Some(attrs),
    }
}

/// Create a VectorEntry with both "title" and "content" text attributes.
fn title_content_doc(id: &str, title: &str, content: &str) -> VectorEntry {
    let mut attrs = HashMap::new();
    attrs.insert(
        "title".to_string(),
        AttributeValue::String(title.to_string()),
    );
    attrs.insert(
        "content".to_string(),
        AttributeValue::String(content.to_string()),
    );
    VectorEntry {
        id: id.to_string(),
        values: vec![0.1, 0.2, 0.3, 0.4],
        attributes: Some(attrs),
    }
}

/// Build FTS field configs for use with compact_with_fts().
fn content_fts_configs() -> HashMap<String, FtsFieldConfig> {
    let mut m = HashMap::new();
    m.insert(
        "content".to_string(),
        FtsFieldConfig {
            stemming: true,
            remove_stopwords: true,
            ..Default::default()
        },
    );
    m
}

/// Create an FTS-enabled namespace via the HTTP API.
async fn create_fts_namespace(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    fts_fields: serde_json::Value,
) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces"))
        .json(&serde_json::json!({
            "name": ns,
            "dimensions": 4,
            "full_text_search": fts_fields,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "namespace creation failed: {}",
        resp.text().await.unwrap()
    );
}

/// Upsert vectors via the HTTP API.
async fn upsert_docs(client: &reqwest::Client, base_url: &str, ns: &str, vectors: &[VectorEntry]) {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "vectors": vectors }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "upsert failed: {}",
        resp.text().await.unwrap()
    );
}

/// Delete vectors by IDs via the HTTP API (DELETE method).
async fn delete_docs(client: &reqwest::Client, base_url: &str, ns: &str, ids: &[&str]) {
    let resp = client
        .delete(format!("{base_url}/v1/namespaces/{ns}/vectors"))
        .json(&serde_json::json!({ "ids": ids }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "delete failed: {}",
        resp.text().await.unwrap()
    );
}

/// Execute a BM25 query and return the parsed JSON response body.
/// Asserts that the status is 200.
async fn bm25_query(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "BM25 query failed: {}",
        resp.text().await.unwrap()
    );
    resp.json().await.unwrap()
}

/// Extract result IDs from a query response.
fn result_ids(body: &serde_json::Value) -> Vec<String> {
    body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect()
}

// ---------------------------------------------------------------------------
// Test 1: Basic WAL BM25 scan — upsert docs with text, query via BM25
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_wal_scan_basic() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-wal-basic");

    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "content": {"language": "english", "stemming": true, "remove_stopwords": true}
        }),
    )
    .await;

    let docs = vec![
        content_doc("doc1", "Rust programming language is fast and safe"),
        content_doc("doc2", "Python programming language is dynamic"),
        content_doc("doc3", "Cooking recipes for Italian pasta"),
        content_doc("doc4", "Rust and systems programming overview"),
        content_doc("doc5", "The weather forecast for today"),
    ];
    upsert_docs(&client, &base_url, &ns, &docs).await;

    // BM25 query for "rust programming" — strong consistency scans WAL
    let body = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", "rust programming"],
            "top_k": 10,
            "consistency": "strong",
        }),
    )
    .await;

    let ids = result_ids(&body);
    // "rust" appears in doc1 and doc4; "programming" appears in doc1, doc2, doc4
    assert!(
        !ids.is_empty(),
        "should return results for 'rust programming'"
    );
    assert!(ids.contains(&"doc1".to_string()), "doc1 should match");
    assert!(ids.contains(&"doc4".to_string()), "doc4 should match");
    assert!(
        !ids.contains(&"doc3".to_string()),
        "doc3 (cooking) should not match"
    );
    assert!(
        !ids.contains(&"doc5".to_string()),
        "doc5 (weather) should not match"
    );

    // Verify scores are positive and ordered descending
    let results = body["results"].as_array().unwrap();
    for w in results.windows(2) {
        let s0 = w[0]["score"].as_f64().unwrap();
        let s1 = w[1]["score"].as_f64().unwrap();
        assert!(s0 > 0.0);
        assert!(s0 >= s1, "results should be sorted by score descending");
    }

    // WAL fragments should have been scanned
    assert!(
        body["scanned_fragments"].as_u64().unwrap() > 0,
        "strong consistency should scan WAL fragments"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 2: WAL scan with deletes — deleted docs should not appear
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_wal_scan_with_deletes() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-wal-del");

    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "content": {"language": "english", "stemming": true, "remove_stopwords": true}
        }),
    )
    .await;

    let docs = vec![
        content_doc("doc1", "Rust programming language"),
        content_doc("doc2", "Rust systems programming"),
        content_doc("doc3", "Rust web framework actix"),
    ];
    upsert_docs(&client, &base_url, &ns, &docs).await;

    // Delete doc2
    delete_docs(&client, &base_url, &ns, &["doc2"]).await;

    // Query for "rust"
    let body = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", "rust"],
            "top_k": 10,
            "consistency": "strong",
        }),
    )
    .await;

    let ids = result_ids(&body);
    assert!(ids.contains(&"doc1".to_string()), "doc1 should remain");
    assert!(ids.contains(&"doc3".to_string()), "doc3 should remain");
    assert!(
        !ids.contains(&"doc2".to_string()),
        "doc2 was deleted and should not appear in results"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 3: Segment search after compaction — query hits FTS inverted index
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_segment_search_after_compaction() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-seg");

    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "content": {"language": "english", "stemming": true, "remove_stopwords": true}
        }),
    )
    .await;

    let docs = vec![
        content_doc("doc1", "Rust programming language is fast and memory safe"),
        content_doc(
            "doc2",
            "Python programming language is interpreted and dynamic",
        ),
        content_doc("doc3", "Java programming language runs on the JVM"),
        content_doc(
            "doc4",
            "Cooking delicious Italian pasta requires fresh ingredients",
        ),
        content_doc(
            "doc5",
            "Rust compiler ensures memory safety without garbage collection",
        ),
        content_doc(
            "doc6",
            "Go programming language is designed for concurrency",
        ),
        content_doc("doc7", "JavaScript runs in web browsers and Node"),
        content_doc(
            "doc8",
            "Rust ownership model prevents data races at compile time",
        ),
    ];
    upsert_docs(&client, &base_url, &ns, &docs).await;

    // Compact with FTS configs to build inverted indexes
    let fts_configs = content_fts_configs();
    let result = compactor
        .compact_with_fts(&ns, None, &fts_configs)
        .await
        .unwrap();
    assert_eq!(result.vectors_compacted, 8);

    // Verify FTS index exists in manifest
    let manifest = zeppelin::wal::Manifest::read(&harness.store, &ns)
        .await
        .unwrap()
        .unwrap();
    let active_seg = manifest.active_segment.as_ref().unwrap();
    let seg_ref = manifest
        .segments
        .iter()
        .find(|s| &s.id == active_seg)
        .unwrap();
    assert!(
        seg_ref.fts_fields.contains(&"content".to_string()),
        "segment should have 'content' in fts_fields"
    );

    // Query via eventual consistency (segment only, no WAL)
    let body = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", "rust programming"],
            "top_k": 10,
            "consistency": "eventual",
        }),
    )
    .await;

    let ids = result_ids(&body);
    assert!(!ids.is_empty(), "segment BM25 search should return results");

    // At least one Rust doc should be in the results
    let rust_docs: Vec<&String> = ids
        .iter()
        .filter(|id| ["doc1", "doc5", "doc8"].contains(&id.as_str()))
        .collect();
    assert!(
        !rust_docs.is_empty(),
        "at least one Rust doc should match 'rust programming'"
    );

    // Cooking doc should not match
    assert!(
        !ids.contains(&"doc4".to_string()),
        "cooking doc should not match 'rust programming'"
    );

    // Should have scanned segments, not fragments
    assert_eq!(body["scanned_fragments"].as_u64().unwrap(), 0);
    assert!(body["scanned_segments"].as_u64().unwrap() >= 1);

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 4: Strong consistency — WAL + segment results combined
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_strong_consistency_wal_plus_segment() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-strong");

    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "content": {"language": "english", "stemming": true, "remove_stopwords": true}
        }),
    )
    .await;

    // Phase 1: upsert and compact (goes into segment)
    let segment_docs = vec![
        content_doc("seg1", "Rust programming language"),
        content_doc("seg2", "Systems programming with Rust"),
        content_doc("seg3", "Cooking Italian food"),
    ];
    upsert_docs(&client, &base_url, &ns, &segment_docs).await;

    let fts_configs = content_fts_configs();
    compactor
        .compact_with_fts(&ns, None, &fts_configs)
        .await
        .unwrap();

    // Phase 2: upsert more (stays in WAL, not compacted)
    let wal_docs = vec![
        content_doc("wal1", "Rust async programming with tokio"),
        content_doc("wal2", "Baking French bread recipes"),
    ];
    upsert_docs(&client, &base_url, &ns, &wal_docs).await;

    // Strong query should see both segment AND WAL
    let body = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", "rust programming"],
            "top_k": 10,
            "consistency": "strong",
        }),
    )
    .await;

    let ids = result_ids(&body);

    // WAL doc "wal1" has "rust" + "programming" — must be found
    assert!(
        ids.contains(&"wal1".to_string()),
        "strong query should find WAL doc 'wal1'"
    );
    // Segment docs seg1 and/or seg2 should also be found
    assert!(
        ids.contains(&"seg1".to_string()) || ids.contains(&"seg2".to_string()),
        "strong query should find segment docs about Rust"
    );
    // Non-matching docs should be absent
    assert!(
        !ids.contains(&"seg3".to_string()),
        "cooking doc should not match"
    );
    assert!(
        !ids.contains(&"wal2".to_string()),
        "baking doc should not match"
    );

    // Both WAL and segment should have been scanned
    assert!(
        body["scanned_fragments"].as_u64().unwrap() > 0,
        "strong query should scan WAL fragments"
    );
    assert!(
        body["scanned_segments"].as_u64().unwrap() >= 1,
        "strong query should scan segments"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 5: Eventual consistency — only sees segment, misses WAL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_eventual_consistency() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-eventual");

    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "content": {"language": "english", "stemming": true, "remove_stopwords": true}
        }),
    )
    .await;

    // Phase 1: upsert and compact
    let segment_docs = vec![
        content_doc("seg1", "Rust programming language"),
        content_doc("seg2", "Systems programming with Rust"),
    ];
    upsert_docs(&client, &base_url, &ns, &segment_docs).await;

    let fts_configs = content_fts_configs();
    compactor
        .compact_with_fts(&ns, None, &fts_configs)
        .await
        .unwrap();

    // Phase 2: upsert WAL-only doc
    let wal_docs = vec![content_doc("wal1", "Rust async runtime tokio")];
    upsert_docs(&client, &base_url, &ns, &wal_docs).await;

    // Eventual query should only see segment
    let body = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", "rust"],
            "top_k": 10,
            "consistency": "eventual",
        }),
    )
    .await;

    let ids = result_ids(&body);

    // WAL-only doc should NOT appear
    assert!(
        !ids.contains(&"wal1".to_string()),
        "eventual query should not return WAL-only doc 'wal1'"
    );

    // Segment docs should appear
    assert!(
        ids.contains(&"seg1".to_string()) || ids.contains(&"seg2".to_string()),
        "eventual query should return segment docs"
    );

    assert_eq!(
        body["scanned_fragments"].as_u64().unwrap(),
        0,
        "eventual query should not scan fragments"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 6: Multi-field Sum — BM25 scores across title + content summed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_multi_field_sum() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-sum");

    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "title": {"language": "english", "stemming": true, "remove_stopwords": true},
            "content": {"language": "english", "stemming": true, "remove_stopwords": true}
        }),
    )
    .await;

    // docA: "rust" in both title and content (should rank highest)
    // docB: "rust" only in content
    // docC: "rust" only in title
    // docD: no "rust" at all
    let docs = vec![
        title_content_doc("docA", "Rust Language", "Rust is fast and safe programming"),
        title_content_doc(
            "docB",
            "Systems Overview",
            "Rust gives memory safety guarantees",
        ),
        title_content_doc(
            "docC",
            "Rust Guide",
            "A comprehensive guide to systems programming",
        ),
        title_content_doc(
            "docD",
            "Cooking Tips",
            "How to make delicious Italian pasta",
        ),
    ];
    upsert_docs(&client, &base_url, &ns, &docs).await;

    // Sum of title BM25 + content BM25
    let body = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["Sum", [
                ["title", "BM25", "rust"],
                ["content", "BM25", "rust"]
            ]],
            "top_k": 10,
            "consistency": "strong",
        }),
    )
    .await;

    let ids = result_ids(&body);
    assert!(!ids.is_empty(), "Sum query should return results");

    // docA has "rust" in both fields — should rank first
    assert_eq!(
        ids[0], "docA",
        "docA should rank first with 'rust' in both fields"
    );

    // docD (no rust) should not appear
    assert!(
        !ids.contains(&"docD".to_string()),
        "docD (cooking) should not match 'rust'"
    );

    // docB and docC should appear (rust in one field each)
    assert!(
        ids.contains(&"docB".to_string()),
        "docB should match (rust in content)"
    );
    assert!(
        ids.contains(&"docC".to_string()),
        "docC should match (rust in title)"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 7: Product-weighted BM25 expression
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_product_weighted() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-product");

    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "title": {"language": "english", "stemming": true, "remove_stopwords": true},
            "content": {"language": "english", "stemming": true, "remove_stopwords": true}
        }),
    )
    .await;

    let docs = vec![
        title_content_doc("docA", "Rust Language", "General programming overview"),
        title_content_doc("docB", "General Guide", "Rust programming language details"),
    ];
    upsert_docs(&client, &base_url, &ns, &docs).await;

    // Product(2.0, title BM25): weight title 2x
    // docA has "rust" in title, docB does not
    let body = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["Product", 2.0, ["title", "BM25", "rust"]],
            "top_k": 10,
            "consistency": "strong",
        }),
    )
    .await;

    let ids = result_ids(&body);
    let results = body["results"].as_array().unwrap();

    // docA has "rust" in title, so it should appear
    assert!(
        ids.contains(&"docA".to_string()),
        "docA should match 'rust' in title"
    );

    // Score should be positive (2.0 * BM25 > 0)
    if !results.is_empty() {
        let top_score = results[0]["score"].as_f64().unwrap();
        assert!(top_score > 0.0, "weighted score should be positive");
    }

    // If docB appears, it should rank lower (no "rust" in title)
    if ids.contains(&"docB".to_string()) {
        let idx_a = ids.iter().position(|id| id == "docA").unwrap();
        let idx_b = ids.iter().position(|id| id == "docB").unwrap();
        assert!(
            idx_a < idx_b,
            "docA should rank higher than docB when weighting title"
        );
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 8: last_as_prefix — prefix matching on last query token
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_last_as_prefix() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-prefix");

    // Use stemming=false so we can test exact prefix matching without stemmer interference
    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "content": {
                "language": "english",
                "stemming": false,
                "remove_stopwords": false
            }
        }),
    )
    .await;

    let docs = vec![
        content_doc("doc1", "programming programs programmer"),
        content_doc("doc2", "testing testable tester"),
        content_doc("doc3", "progress progressive progression"),
    ];
    upsert_docs(&client, &base_url, &ns, &docs).await;

    // Query "prog" with last_as_prefix=true: should match tokens starting with "prog"
    let body = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", "prog"],
            "top_k": 10,
            "consistency": "strong",
            "last_as_prefix": true,
        }),
    )
    .await;

    let ids = result_ids(&body);
    assert!(
        ids.contains(&"doc1".to_string()),
        "doc1 should match prefix 'prog' (programming, programs, programmer)"
    );
    assert!(
        ids.contains(&"doc3".to_string()),
        "doc3 should match prefix 'prog' (progress, progressive, progression)"
    );
    assert!(
        !ids.contains(&"doc2".to_string()),
        "doc2 should not match prefix 'prog' (testing, testable, tester)"
    );

    // Without prefix mode, bare "prog" should match nothing (no exact token "prog")
    let body_no_prefix = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", "prog"],
            "top_k": 10,
            "consistency": "strong",
            "last_as_prefix": false,
        }),
    )
    .await;

    let ids_no_prefix = result_ids(&body_no_prefix);
    assert!(
        ids_no_prefix.is_empty(),
        "without prefix mode, 'prog' should match no exact token"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 9: ContainsAllTokens filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_contains_all_tokens_filter() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-all-tokens");

    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "content": {"language": "english", "stemming": true, "remove_stopwords": true}
        }),
    )
    .await;

    let docs = vec![
        content_doc("doc1", "Rust programming language is fast"),
        content_doc("doc2", "Python programming language is dynamic"),
        content_doc("doc3", "Rust systems design and architecture"),
    ];
    upsert_docs(&client, &base_url, &ns, &docs).await;

    // BM25 query for "programming" with ContainsAllTokens filter requiring both "rust" and "fast"
    let body = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", "programming"],
            "top_k": 10,
            "consistency": "strong",
            "filter": {
                "op": "contains_all_tokens",
                "field": "content",
                "tokens": ["rust", "fast"]
            }
        }),
    )
    .await;

    let ids = result_ids(&body);
    // Only doc1 has both "rust" AND "fast" in content
    assert!(
        ids.contains(&"doc1".to_string()),
        "doc1 should pass ContainsAllTokens filter (has 'rust' + 'fast')"
    );
    // doc2 has no "rust", doc3 has no "fast"
    assert!(
        !ids.contains(&"doc2".to_string()),
        "doc2 should fail filter (no 'rust')"
    );
    assert!(
        !ids.contains(&"doc3".to_string()),
        "doc3 should fail filter (no 'fast')"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 10: ContainsTokenSequence filter (phrase matching)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_contains_token_sequence_filter() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-token-seq");

    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "content": {"language": "english", "stemming": true, "remove_stopwords": true}
        }),
    )
    .await;

    // "programming language" will be stemmed to ["program", "languag"] (adjacent, in order)
    let docs = vec![
        content_doc("doc1", "Rust programming language overview"),
        content_doc("doc2", "Language of Rust programming"),
        content_doc("doc3", "Systems design patterns in Rust"),
    ];
    upsert_docs(&client, &base_url, &ns, &docs).await;

    // Filter for adjacent token sequence "programming language"
    // doc1 has the exact phrase "programming language" (stemmed: ["program", "languag"] adjacent)
    let body = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", "rust"],
            "top_k": 10,
            "consistency": "strong",
            "filter": {
                "op": "contains_token_sequence",
                "field": "content",
                "tokens": ["programming", "language"]
            }
        }),
    )
    .await;

    let ids = result_ids(&body);

    // doc1 should match: has "programming language" as adjacent phrase
    assert!(
        ids.contains(&"doc1".to_string()),
        "doc1 should match token sequence 'programming language'"
    );

    // doc3 has no "programming" or "language" at all
    assert!(
        !ids.contains(&"doc3".to_string()),
        "doc3 should not pass sequence filter"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 11: Empty BM25 query string returns empty results
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_empty_query() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-empty");

    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "content": {"language": "english", "stemming": true, "remove_stopwords": true}
        }),
    )
    .await;

    let docs = vec![
        content_doc("doc1", "Rust programming language"),
        content_doc("doc2", "Python programming language"),
    ];
    upsert_docs(&client, &base_url, &ns, &docs).await;

    // Query with empty string — tokenizer produces no tokens, so no results
    let body = bm25_query(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "rank_by": ["content", "BM25", ""],
            "top_k": 10,
            "consistency": "strong",
        }),
    )
    .await;

    let ids = result_ids(&body);
    assert!(
        ids.is_empty(),
        "empty query string should return no results, got: {:?}",
        ids
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

// ---------------------------------------------------------------------------
// Test 12: BM25 on a field not configured for FTS returns 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fts_field_not_configured_error() {
    let config = fts_test_config();
    let (base_url, harness, _cache, _dir, _compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = api_ns(&harness, "fts-not-configured");

    // Create namespace with FTS only on "content" — NOT on "title"
    create_fts_namespace(
        &client,
        &base_url,
        &ns,
        serde_json::json!({
            "content": {"language": "english", "stemming": true, "remove_stopwords": true}
        }),
    )
    .await;

    let docs = vec![content_doc("doc1", "Some text content")];
    upsert_docs(&client, &base_url, &ns, &docs).await;

    // Query BM25 on "title" which is NOT configured for FTS — should return 400
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&serde_json::json!({
            "rank_by": ["title", "BM25", "some query"],
            "top_k": 10,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "querying BM25 on unconfigured field should return 400"
    );

    let error_body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = error_body["error"].as_str().unwrap_or("");
    // Error should mention the field name
    assert!(
        error_msg.contains("title"),
        "error should mention the unconfigured field 'title', got: {error_msg}"
    );

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
