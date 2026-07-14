mod common;

use bytes::Bytes;
use common::harness::TestHarness;
use common::server::{cleanup_ns, create_ns_api_fts, start_test_server_full, FullTestServer};
use std::collections::HashMap;
use zeppelin::cache::decoded_cache::DecodedArtifactCache;
use zeppelin::config::{CacheConfig, CompactionConfig, Config, IndexingConfig};
use zeppelin::fts::global_index::global_fts_key;
use zeppelin::fts::rank_by::RankBy;
use zeppelin::fts::FtsFieldConfig;
use zeppelin::query::execute_bm25_query;
use zeppelin::types::{AttributeValue, VectorEntry};
use zeppelin::wal::{Manifest, WalReader};

fn fts_config() -> Config {
    Config {
        compaction: CompactionConfig {
            max_wal_fragments_before_compact: 1,
            ..Default::default()
        },
        indexing: IndexingConfig {
            default_num_centroids: 4,
            kmeans_max_iterations: 10,
            fts_index: true,
            bitmap_index: false,
            bm25_max_full_scan_clusters: 64,
            bm25_max_full_scan_vectors: 10_000,
            ..Default::default()
        },
        cache: CacheConfig {
            manifest_cache_ttl_ms: 0,
            decoded_artifact_cache_max_mb: 64,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn fts_fields() -> HashMap<String, FtsFieldConfig> {
    HashMap::from([(
        "content".to_string(),
        FtsFieldConfig {
            stemming: true,
            remove_stopwords: true,
            ..Default::default()
        },
    )])
}

fn document(id: &str, text: &str) -> VectorEntry {
    VectorEntry {
        id: id.to_string(),
        values: vec![0.1, 0.2, 0.3, 0.4],
        attributes: Some(HashMap::from([(
            "content".to_string(),
            AttributeValue::String(text.to_string()),
        )])),
    }
}

async fn query_body(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/query"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "query failed: {}",
        response.text().await.unwrap()
    );
    response.json().await.unwrap()
}

async fn query(client: &reqwest::Client, base_url: &str, namespace: &str) -> serde_json::Value {
    query_body(
        client,
        base_url,
        namespace,
        serde_json::json!({
            "rank_by": ["content", "BM25", "rust programming"],
            "top_k": 10,
            "consistency": "eventual",
            "include_attributes": true,
        }),
    )
    .await
}

async fn upsert(
    client: &reqwest::Client,
    base_url: &str,
    namespace: &str,
    documents: &[VectorEntry],
) {
    let response = client
        .post(format!("{base_url}/v1/namespaces/{namespace}/vectors"))
        .json(&serde_json::json!({ "vectors": documents }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "upsert failed: {}",
        response.text().await.unwrap()
    );
}

async fn compacted_fixture(
    config: Config,
    documents: &[VectorEntry],
) -> (TestHarness, FullTestServer, reqwest::Client, String) {
    let harness = TestHarness::new().await;
    let server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        config,
        false,
        None,
    )
    .await;
    let client = crate::common::server::client_with_bearer(&server.admin_bearer);
    let namespace = create_ns_api_fts(
        &client,
        &server.base_url,
        4,
        serde_json::json!({
            "content": {
                "language": "english",
                "stemming": true,
                "remove_stopwords": true
            }
        }),
    )
    .await;
    upsert(&client, &server.base_url, &namespace, documents).await;
    server
        .compactor
        .compact_with_fts(&namespace, None, &fts_fields())
        .await
        .unwrap();
    server.manifest_cache.invalidate(&namespace);
    (harness, server, client, namespace)
}

fn hybrid_body() -> serde_json::Value {
    serde_json::json!({
        "sources": [
            {
                "type": "ann",
                "vector": [0.1, 0.2, 0.3, 0.4],
                "nprobe": 4
            },
            {
                "type": "bm25",
                "rank_by": ["content", "BM25", "rust programming"]
            }
        ],
        "fusion": {"type": "rrf", "k": 60},
        "candidate_k": 10,
        "top_k": 10,
        "consistency": "eventual",
        "projection": {"include_attributes": true}
    })
}

fn result_ids(body: &serde_json::Value) -> Vec<String> {
    body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn global_fts_decode_is_reused_and_cache_clear_preserves_results() {
    let documents = vec![
        document("rust-1", "Rust programming is fast and safe"),
        document("python-1", "Python programming is dynamic"),
        document("rust-2", "Systems programming with Rust"),
        document("food-1", "Italian pasta recipes"),
    ];
    let (harness, server, client, namespace) = compacted_fixture(fts_config(), &documents).await;

    let cold = query(&client, &server.base_url, &namespace).await;
    assert_eq!(server.decoded_artifact_cache_decode_count(), 1);
    assert_eq!(server.decoded_artifact_cache_global_decode_count(), 1);
    assert_eq!(server.decoded_artifact_cache_len(), 1);

    let uncached = execute_bm25_query(
        &harness.store,
        &WalReader::new(harness.store.clone()),
        &namespace,
        &RankBy::Bm25 {
            field: "content".to_string(),
            query: "rust programming".to_string(),
        },
        &fts_fields(),
        10,
        None,
        zeppelin::types::ConsistencyLevel::Eventual,
        false,
        None,
        None,
        None,
        None,
        Some(&server.cache),
        64,
        10_000,
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        uncached.scanned_fragments,
        cold["scanned_fragments"].as_u64().unwrap() as usize
    );
    assert_eq!(
        uncached.scanned_segments,
        cold["scanned_segments"].as_u64().unwrap() as usize
    );
    let cold_results = cold["results"].as_array().unwrap();
    assert_eq!(uncached.results.len(), cold_results.len());
    for (domain, http) in uncached.results.iter().zip(cold_results) {
        assert_eq!(domain.id, http["id"].as_str().unwrap());
        assert_eq!(
            serde_json::to_value(&domain.attributes).unwrap(),
            http["attributes"]
        );
        let http_score = http["score"].as_f64().unwrap() as f32;
        assert_eq!(domain.score.to_bits(), http_score.to_bits());
    }

    let warm = query(&client, &server.base_url, &namespace).await;
    assert_eq!(warm, cold);
    assert_eq!(server.decoded_artifact_cache_decode_count(), 1);

    server.clear_decoded_artifact_cache();
    assert_eq!(server.decoded_artifact_cache_len(), 0);
    let after_clear = query(&client, &server.base_url, &namespace).await;
    assert_eq!(after_clear, cold);
    assert_eq!(server.decoded_artifact_cache_decode_count(), 2);

    server.clear_decoded_artifact_cache();
    let hybrid_cold = query_body(&client, &server.base_url, &namespace, hybrid_body()).await;
    assert_eq!(server.decoded_artifact_cache_decode_count(), 3);
    let hybrid_warm = query_body(&client, &server.base_url, &namespace, hybrid_body()).await;
    assert_eq!(hybrid_warm, hybrid_cold);
    assert_eq!(server.decoded_artifact_cache_decode_count(), 3);

    let mut disabled_config = fts_config();
    disabled_config.cache.decoded_artifact_cache_max_mb = 0;
    let disabled_server = start_test_server_full(
        harness.store.clone(),
        Some(harness.prefix.clone()),
        disabled_config,
        false,
        None,
    )
    .await;
    let disabled_client = crate::common::server::client_with_bearer(&disabled_server.admin_bearer);
    let disabled_bm25 = query(&disabled_client, &disabled_server.base_url, &namespace).await;
    assert_eq!(disabled_bm25, cold);
    let disabled_hybrid = query_body(
        &disabled_client,
        &disabled_server.base_url,
        &namespace,
        hybrid_body(),
    )
    .await;
    assert_eq!(disabled_hybrid, hybrid_cold);
    assert_eq!(disabled_server.decoded_artifact_cache_len(), 0);

    cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
    disabled_server.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn segment_replacement_uses_new_key_and_capacity_evicts_old_decodes() {
    let documents = vec![
        document("rust-1", "Rust programming is fast and safe"),
        document("python-1", "Python programming is dynamic"),
        document("rust-2", "Systems programming with Rust"),
        document("food-1", "Italian pasta recipes"),
    ];
    let (harness, server, client, namespace) = compacted_fixture(fts_config(), &documents).await;

    let first_manifest = Manifest::read(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    let first_segment = first_manifest.active_segment.as_ref().unwrap();
    let first_key = global_fts_key(&namespace, first_segment);
    let first = query(&client, &server.base_url, &namespace).await;
    assert_eq!(server.decoded_artifact_cache_decode_count(), 1);

    upsert(
        &client,
        &server.base_url,
        &namespace,
        &[document(
            "rust-new",
            "Rust programming adds a newly compacted document",
        )],
    )
    .await;
    server
        .compactor
        .compact_with_fts(&namespace, None, &fts_fields())
        .await
        .unwrap();
    server.manifest_cache.invalidate(&namespace);

    let second_manifest = Manifest::read(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    let second_segment = second_manifest.active_segment.as_ref().unwrap();
    let second_key = global_fts_key(&namespace, second_segment);
    assert_ne!(first_key, second_key);

    let second = query(&client, &server.base_url, &namespace).await;
    assert_ne!(second, first);
    assert!(result_ids(&second).contains(&"rust-new".to_string()));
    assert_eq!(server.decoded_artifact_cache_decode_count(), 2);
    let warm_second = query(&client, &server.base_url, &namespace).await;
    assert_eq!(warm_second, second);
    assert_eq!(server.decoded_artifact_cache_decode_count(), 2);

    let first_probe = DecodedArtifactCache::new(usize::MAX);
    first_probe
        .get_or_decode_global_fts(&first_key, || harness.store.get(&first_key))
        .await
        .unwrap();
    let first_size = first_probe.total_size();
    let second_probe = DecodedArtifactCache::new(usize::MAX);
    second_probe
        .get_or_decode_global_fts(&second_key, || harness.store.get(&second_key))
        .await
        .unwrap();
    let second_size = second_probe.total_size();
    let budget = first_size.max(second_size);

    let bounded = DecodedArtifactCache::new(budget);
    bounded
        .get_or_decode_global_fts(&first_key, || harness.store.get(&first_key))
        .await
        .unwrap();
    bounded
        .get_or_decode_global_fts(&second_key, || harness.store.get(&second_key))
        .await
        .unwrap();
    assert_eq!(bounded.len(), 1);
    assert!(bounded.total_size() <= budget);
    assert_ne!(bounded.contains(&first_key), bounded.contains(&second_key));

    let evicted_key = if bounded.contains(&first_key) {
        second_key.as_str()
    } else {
        first_key.as_str()
    };
    bounded
        .get_or_decode_global_fts(evicted_key, || harness.store.get(evicted_key))
        .await
        .unwrap();
    assert_eq!(bounded.decode_count(), 3);
    assert_eq!(bounded.len(), 1);

    cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
    server.shutdown().await;
}

fn legacy_cluster_bytes(documents: &[VectorEntry]) -> Bytes {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&u32::try_from(documents.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(&4_u32.to_le_bytes());
    for document in documents {
        bytes.extend_from_slice(&u32::try_from(document.id.len()).unwrap().to_le_bytes());
        bytes.extend_from_slice(document.id.as_bytes());
        for value in &document.values {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
    Bytes::from(bytes)
}

#[tokio::test]
async fn legacy_cluster_fts_decode_is_reused_without_changing_budget_checks() {
    let documents = vec![
        document("rust-1", "Rust programming is fast and safe"),
        document("python-1", "Python programming is dynamic"),
        document("rust-2", "Systems programming with Rust"),
        document("food-1", "Italian pasta recipes"),
    ];
    let mut config = fts_config();
    config.indexing.default_num_centroids = 1;
    let (harness, server, client, namespace) = compacted_fixture(config, &documents).await;

    let mut manifest = Manifest::read(&harness.store, &namespace)
        .await
        .unwrap()
        .unwrap();
    let active_id = manifest.active_segment.clone().unwrap();
    let active = manifest
        .segments
        .iter()
        .find(|segment| segment.id == active_id)
        .unwrap();
    assert_eq!(active.cluster_count, 1);
    assert!(active.has_global_fts);

    let cluster_key = format!("{namespace}/segments/{active_id}/cluster_0.bin");
    server
        .store
        .put(&cluster_key, legacy_cluster_bytes(&documents))
        .await
        .unwrap();
    let active = manifest
        .segments
        .iter_mut()
        .find(|segment| segment.id == active_id)
        .unwrap();
    active.has_global_fts = false;
    active.cluster_objects.clear();
    active.bootstrap = None;
    active.sketch = None;
    active.membership = None;
    manifest.write(&server.store, &namespace).await.unwrap();
    server.manifest_cache.invalidate(&namespace);

    let cold = query(&client, &server.base_url, &namespace).await;
    assert_eq!(server.decoded_artifact_cache_global_decode_count(), 0);
    assert_eq!(server.decoded_artifact_cache_cluster_decode_count(), 1);
    assert_eq!(server.decoded_artifact_cache_len(), 1);

    let warm = query(&client, &server.base_url, &namespace).await;
    assert_eq!(warm, cold);
    assert_eq!(server.decoded_artifact_cache_cluster_decode_count(), 1);

    server.clear_decoded_artifact_cache();
    let after_clear = query(&client, &server.base_url, &namespace).await;
    assert_eq!(after_clear, cold);
    assert_eq!(server.decoded_artifact_cache_cluster_decode_count(), 2);

    cleanup_ns(&harness.store, &namespace).await;
    harness.cleanup().await;
    server.shutdown().await;
}
