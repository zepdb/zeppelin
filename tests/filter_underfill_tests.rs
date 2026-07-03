//! Task 6 — filtered quantized queries must not silently under-fill top_k.
//!
//! SQ8/PQ search ranks coarse candidates by approximate distance, truncates to
//! `fetch_k * 4`, THEN the attribute post-filter runs. With a selective filter
//! nearly all of the truncated set is filtered out, so the client gets far
//! fewer than top_k results even though plenty of matching vectors sit in the
//! probed clusters. The fix applies the (non-bitmap) attribute filter DURING
//! the coarse scan, before truncation.
//!
//! The reproduction is DETERMINISTIC and adversarial: the filter-matching
//! vectors are placed FARTHER from the query than a large mass of non-matching
//! vectors. Coarse ranking by approximate distance therefore puts every match
//! beyond the `fetch_k * 4` truncation window, so the pre-fix code (filter
//! AFTER truncate) discards them all and returns 0 results — even though far
//! more than top_k matches exist in the probed clusters. The fix (filter
//! DURING the coarse scan) returns exactly top_k.
//!
//! Two confounds are deliberately removed:
//! - `bitmap_index` is disabled for the RED tests: a bitmap-resolved filter
//!   pre-restricts candidates BEFORE truncation and is already correct. The
//!   bug only affects filters evaluated via the attribute post-filter.
//! - The namespaces use EUCLIDEAN distance: the adversarial vectors are all
//!   parallel (uniform value x ones), so cosine would rank them equidistant.

mod common;

use common::server::{cleanup_ns, create_ns_api_with, start_test_server_with_compactor};

use serde_json::{json, Value};
use zeppelin::config::Config;
use zeppelin::index::quantization::QuantizationType;

const DIM: usize = 8;
const NCLUSTERS: usize = 16; // <= max_nprobe (128) so a query can probe ALL clusters

const MATCHING: usize = 200; // "tenant=hot" vectors, placed FAR from the query
const NEAR_DECOYS: usize = 2000; // "tenant=cold" vectors placed NEAR the query
const TOP_K: usize = 50; // << MATCHING, so top_k is always fillable

// Pre-fix arithmetic (defaults: oversample_factor=3, rerank = fetch_k*4):
//   fetch_k = TOP_K * 3 = 150, rerank_count = 600 < NEAR_DECOYS = 2000.
// All 600 coarse survivors are decoys, so the post-filter yields 0 matches.

/// Test config: NCLUSTERS centroids (so nprobe can cover every cluster and any
/// shortfall is truncation, not IVF recall loss) and NO bitmap indexes (so the
/// filter goes through the attribute post-filter path under test).
fn adversarial_config() -> Config {
    let mut config = Config::load(None).unwrap();
    config.indexing.default_num_centroids = NCLUSTERS;
    config.indexing.bitmap_index = false;
    config
}

/// A `DIM`-length vector whose every component is `v`.
fn uniform_vec(v: f32) -> Vec<f32> {
    vec![v; DIM]
}

/// Create a namespace with EUCLIDEAN distance (see module docs).
async fn create_euclidean_ns(client: &reqwest::Client, base_url: &str) -> String {
    create_ns_api_with(
        client,
        base_url,
        json!({ "dimensions": DIM, "distance_metric": "euclidean" }),
    )
    .await
}

/// Seed a namespace with the adversarial layout:
///   - `NEAR_DECOYS` vectors at magnitude ~1.0 from the origin, `tenant=cold`
///   - `MATCHING` vectors at magnitude ~5.0 from the origin, `tenant=hot`
///
/// then compacts into a segment (the quantized scan path only runs against
/// compacted segments; the WAL scan is brute-force flat). A `tenant=hot`
/// filtered query from the origin must surface the far matches despite the
/// near decoys dominating the approximate-distance ranking.
async fn seed_adversarial_ns(
    client: &reqwest::Client,
    base_url: &str,
    ns: &str,
    compactor: &zeppelin::compaction::Compactor,
) {
    let mut vectors: Vec<Value> = Vec::with_capacity(NEAR_DECOYS + MATCHING);
    for i in 0..NEAR_DECOYS {
        // Near the query (origin); tiny per-vector jitter to avoid exact ties.
        let jitter = (i % 50) as f32 * 0.001;
        vectors.push(json!({
            "id": format!("cold_{i}"),
            "values": uniform_vec(1.0 + jitter),
            "attributes": { "tenant": "cold" },
        }));
    }
    for i in 0..MATCHING {
        let jitter = (i % 50) as f32 * 0.001;
        vectors.push(json!({
            "id": format!("hot_{i}"),
            "values": uniform_vec(5.0 + jitter),
            "attributes": { "tenant": "hot" },
        }));
    }
    for chunk in vectors.chunks(500) {
        let resp = client
            .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
            .json(&json!({ "vectors": chunk }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "upsert failed");
    }
    compactor.compact(ns).await.unwrap();
}

/// Run a `tenant=hot` filtered query from the origin with nprobe = NCLUSTERS
/// and return the response body. Asserts the segment path actually ran
/// (scanned_segments = 1, scanned_fragments = 0).
async fn query_hot(client: &reqwest::Client, base_url: &str, ns: &str) -> Value {
    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": uniform_vec(0.0),
            "top_k": TOP_K,
            "nprobe": NCLUSTERS, // probe every cluster -> no IVF recall loss
            "consistency": "strong",
            "filter": { "op": "eq", "field": "tenant", "value": "hot" },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["scanned_segments"].as_u64(),
        Some(1),
        "query must hit the compacted segment path"
    );
    assert_eq!(
        body["scanned_fragments"].as_u64(),
        Some(0),
        "no WAL fragments should remain after compaction"
    );
    body
}

/// Assert the filtered query returned exactly TOP_K results, all real matches.
fn assert_full_hot_top_k(body: &Value, path: &str) {
    let results = body["results"].as_array().unwrap();
    assert_eq!(
        results.len(),
        TOP_K,
        "{path}: filtered query under-filled: got {} of {TOP_K} \
         (>= {MATCHING} matches exist in the probed clusters)",
        results.len()
    );
    for r in results {
        let id = r["id"].as_str().unwrap();
        assert!(
            id.starts_with("hot_"),
            "{path}: non-matching id {id} returned by tenant=hot filter"
        );
    }
}

/// I1 (SQ8, the default quantization): a selective non-bitmap filter with
/// nprobe = ALL clusters must fill top_k. Pre-fix: coarse truncation to
/// fetch_k*4 keeps only near decoys, the post-filter drops them all -> 0
/// results. Post-fix: exactly TOP_K results, all `hot_`.
#[tokio::test]
async fn test_sq8_filtered_query_fills_top_k() {
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(adversarial_config())).await;
    let client = reqwest::Client::new();
    let ns = create_euclidean_ns(&client, &base_url).await;
    seed_adversarial_ns(&client, &base_url, &ns, &compactor).await;

    let body = query_hot(&client, &base_url, &ns).await;
    assert_full_hot_top_k(&body, "SQ8");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

/// I3: an UNFILTERED SQ8 query still returns top_k (the coarse truncation is
/// only for rerank-cost control and must not change unfiltered results).
#[tokio::test]
async fn test_sq8_unfiltered_query_unchanged() {
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(adversarial_config())).await;
    let client = reqwest::Client::new();
    let ns = create_euclidean_ns(&client, &base_url).await;
    seed_adversarial_ns(&client, &base_url, &ns, &compactor).await;

    let resp = client
        .post(format!("{base_url}/v1/namespaces/{ns}/query"))
        .json(&json!({
            "vector": uniform_vec(0.0),
            "top_k": TOP_K,
            "nprobe": NCLUSTERS,
            "consistency": "strong",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert_eq!(
        results.len(),
        TOP_K,
        "unfiltered SQ8 query must return top_k"
    );
    // The nearest vectors are the decoys; unfiltered results must be them.
    for r in results {
        let id = r["id"].as_str().unwrap();
        assert!(
            id.starts_with("cold_"),
            "unfiltered query should return the nearest (decoy) vectors, got {id}"
        );
    }

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

/// I1 (PQ): same adversarial layout with Product Quantization. dim=8 with the
/// default pq_m=8 gives 1-dim subquantizers; 2200 vectors are ample training
/// data for the 256-entry codebooks.
#[tokio::test]
async fn test_pq_filtered_query_fills_top_k() {
    let mut config = adversarial_config();
    config.indexing.quantization = QuantizationType::Product;
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_euclidean_ns(&client, &base_url).await;
    seed_adversarial_ns(&client, &base_url, &ns, &compactor).await;

    let body = query_hot(&client, &base_url, &ns).await;
    assert_full_hot_top_k(&body, "PQ");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

/// I1 (hierarchical + SQ8): the same truncation bug lives in the hierarchical
/// leaf scan (`src/index/hierarchical/search.rs`). A `leaf_size` larger than
/// the dataset yields a single leaf cluster, so beam search trivially reaches
/// every vector and any shortfall is the coarse truncation — deterministic,
/// no beam-recall confound.
#[tokio::test]
async fn test_hierarchical_sq8_filtered_query_fills_top_k() {
    let mut config = adversarial_config();
    config.indexing.hierarchical = true;
    config.indexing.leaf_size = Some(NEAR_DECOYS + MATCHING + 100);
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_euclidean_ns(&client, &base_url).await;
    seed_adversarial_ns(&client, &base_url, &ns, &compactor).await;

    let body = query_hot(&client, &base_url, &ns).await;
    assert_full_hot_top_k(&body, "hierarchical SQ8");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}

/// I2 pin: with bitmap indexes ENABLED the same adversarial layout already
/// returns full top_k — the bitmap prefilter restricts candidates BEFORE
/// truncation. This must hold before and after the fix (fast path unchanged).
#[tokio::test]
async fn test_bitmap_filtered_query_unaffected() {
    let mut config = adversarial_config();
    config.indexing.bitmap_index = true;
    let (base_url, harness, _cache, _dir, compactor) =
        start_test_server_with_compactor(Some(config)).await;
    let client = reqwest::Client::new();
    let ns = create_euclidean_ns(&client, &base_url).await;
    seed_adversarial_ns(&client, &base_url, &ns, &compactor).await;

    let body = query_hot(&client, &base_url, &ns).await;
    assert_full_hot_top_k(&body, "SQ8+bitmap");

    cleanup_ns(&harness.store, &ns).await;
    harness.cleanup().await;
}
