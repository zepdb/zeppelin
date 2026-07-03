//! F1/H12 — per-artifact-class GET/byte attribution in CountingStore.
//!
//! Verifies the key→class bucketing against the REAL key builders and that a
//! real ingest → compact → query flow produces a sane per-class breakdown.

mod common;

use common::counting::{classify, counting_store, ArtifactClass, ALL_CLASSES};
use common::harness::TestHarness;
use common::vectors::random_vectors;

use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::types::{ConsistencyLevel, DistanceMetric};
use zeppelin::wal::manifest::Manifest;
use zeppelin::wal::{WalReader, WalWriter};

/// Bucketing unit test: every real key builder pattern maps to its class.
#[test]
fn test_classify_real_key_patterns() {
    let cases = [
        // src/index/ivf_flat/build.rs
        ("ns/segments/seg1/cluster_0.bin", ArtifactClass::Cluster),
        ("ns/segments/seg1/cluster_12.bin", ArtifactClass::Cluster),
        ("ns/segments/seg1/attrs_0.bin", ArtifactClass::Attrs),
        ("ns/segments/seg1/centroids.bin", ArtifactClass::Centroids),
        // src/index/quantization/{sq,pq}.rs
        ("ns/segments/seg1/sq_calibration.bin", ArtifactClass::Sq),
        ("ns/segments/seg1/sq_cluster_3.bin", ArtifactClass::Sq),
        ("ns/segments/seg1/pq_codebook.bin", ArtifactClass::Sq),
        ("ns/segments/seg1/pq_cluster_3.bin", ArtifactClass::Sq),
        // src/index/bitmap/mod.rs
        ("ns/segments/seg1/bitmap_0.bin", ArtifactClass::Bitmap),
        // src/fts/{global_index,inverted_index}.rs
        ("ns/segments/seg1/global_fts.bin", ArtifactClass::Fts),
        ("ns/segments/seg1/fts_index_2.bin", ArtifactClass::Fts),
        ("ns/segments/seg1/fts_meta.json", ArtifactClass::Fts),
        // src/wal/fragment.rs
        (
            "ns/wal/01J0000000000000000000000A.wal",
            ArtifactClass::Wal,
        ),
        // src/wal/manifest.rs
        ("ns/manifest.json", ArtifactClass::Manifest),
        // Everything else
        ("ns/meta.json", ArtifactClass::Other),
        ("ns/lease.json", ArtifactClass::Other),
        ("ns/segments/seg1/tree_meta.json", ArtifactClass::Other),
        ("ns/segments/seg1/node_abc.bin", ArtifactClass::Other),
    ];
    for (key, expected) in cases {
        assert_eq!(classify(key), expected, "key {key} misclassified");
    }
    // A namespace whose NAME contains a pattern must not confuse the
    // classifier — only the filename segment matters.
    assert_eq!(
        classify("cluster_lovers/manifest.json"),
        ArtifactClass::Manifest
    );
}

fn test_compactor(store: &zeppelin::storage::ZeppelinStore) -> Compactor {
    let wal_reader = WalReader::new(store.clone());
    let compaction_config = CompactionConfig {
        max_wal_fragments_before_compact: 3,
        ..Default::default()
    };
    let indexing_config = IndexingConfig {
        default_num_centroids: 4,
        kmeans_max_iterations: 10,
        ..Default::default()
    };
    Compactor::new(
        store.clone(),
        wal_reader,
        compaction_config,
        indexing_config,
    )
}

/// End-to-end: ingest + compact a tiny namespace, run a query, and assert
/// the per-class GET/byte breakdown is sane.
#[tokio::test]
async fn test_query_class_breakdown_is_sane() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("class-attribution");
    let writer = WalWriter::new(store.clone());
    let wal_reader = WalReader::new(store.clone());

    Manifest::new().write(&store, &ns).await.unwrap();
    let vecs = random_vectors(100, 16);
    let query_vec = vecs[0].values.clone();
    writer.append(&ns, vecs, vec![]).await.unwrap();

    // Ingest wrote through the counting store: WAL + manifest PUT bytes
    // must be attributed.
    assert!(
        counter.puts_for(ArtifactClass::Wal) >= 1,
        "ingest must PUT at least one WAL fragment"
    );
    assert!(
        counter.put_bytes_for(ArtifactClass::Wal) > 0,
        "WAL PUT bytes must be non-zero"
    );
    assert!(
        counter.puts_for(ArtifactClass::Manifest) >= 1,
        "ingest must PUT the manifest"
    );

    // Compaction writes segment artifacts.
    let compactor = test_compactor(&store);
    compactor.compact(&ns).await.unwrap();
    assert!(
        counter.put_bytes_for(ArtifactClass::Cluster) > 0,
        "compaction must write cluster bytes"
    );
    assert!(
        counter.put_bytes_for(ArtifactClass::Attrs) > 0,
        "compaction must write attrs bytes"
    );
    assert!(
        counter.put_bytes_for(ArtifactClass::Centroids) > 0,
        "compaction must write centroids bytes"
    );

    // Fresh slate for the query-side breakdown.
    counter.reset();
    let results = execute_query(QueryParams {
        store: &store,
        wal_reader: &wal_reader,
        namespace: &ns,
        query: &query_vec,
        top_k: 5,
        nprobe: 4,
        filter: None,
        consistency: ConsistencyLevel::Strong,
        distance_metric: DistanceMetric::Euclidean,
        oversample_factor: 3,
        cache: None,
        manifest_cache: None,
    })
    .await
    .unwrap();
    assert!(!results.results.is_empty(), "query must return results");

    println!("per-class breakdown for one cold strong query:");
    println!("{}", counter.report());
    println!(
        "breakdown json: {}",
        serde_json::to_string(&counter.class_breakdown()).unwrap()
    );

    // A query reads the manifest and at least one cluster blob.
    assert!(
        counter.gets_for(ArtifactClass::Manifest) >= 1,
        "query must GET the manifest"
    );
    assert!(
        counter.gets_for(ArtifactClass::Cluster) >= 1,
        "query must GET at least one cluster blob"
    );
    assert!(
        counter.gets_for(ArtifactClass::Centroids) >= 1,
        "query must GET the centroids blob (no cache configured)"
    );

    // Bytes-per-class are non-zero for every class that was actually read.
    for class in ALL_CLASSES {
        let ops = counter.gets_for(class);
        let bytes = counter.get_bytes_for(class);
        assert_eq!(
            ops > 0,
            bytes > 0,
            "class {class}: ops={ops} but bytes={bytes} — byte attribution broken"
        );
    }

    // `Other` must stay empty for a normal query. If it does not, the
    // classifier is missing a real key pattern.
    assert_eq!(
        counter.gets_for(ArtifactClass::Other),
        0,
        "Other GETs must be zero for a normal query; classifier is missing a key pattern"
    );

    // The legacy substring API and the class API must agree. Note the
    // leading `/`: plain "cluster_" also matches sq_cluster_ keys, which is
    // exactly the ambiguity the class API removes.
    assert_eq!(
        counter.gets_matching("/cluster_"),
        counter.gets_for(ArtifactClass::Cluster),
        "gets_matching(\"/cluster_\") must equal gets_for(Cluster)"
    );
    assert_eq!(
        counter.gets_matching("manifest.json"),
        counter.gets_for(ArtifactClass::Manifest),
    );

    // reset() clears the class counters too.
    counter.reset();
    assert_eq!(counter.total_gets(), 0);
    assert_eq!(counter.total_get_bytes(), 0);

    harness.cleanup().await;
}
