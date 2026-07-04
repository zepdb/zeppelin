//! C.0a — lazy final-result attrs loading for IVF/hierarchical search.
//!
//! These tests pin the read-path contract directly at the index layer:
//! unfiltered queries enrich returned results from attrs blobs, but they only
//! fetch attrs for clusters that actually contain the final top-k results.
//! Filtered SQ8 queries keep the existing eager attrs profile because attrs
//! are required before coarse-candidate truncation.

mod common;

use std::collections::HashMap;

use common::counting::{counting_store, ArtifactClass};
use common::harness::TestHarness;

use zeppelin::config::IndexingConfig;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::index::{HierarchicalIndex, IvfFlatIndex, VectorIndex};
use zeppelin::types::{AttributeValue, DistanceMetric, Filter, SearchResult, VectorEntry};

const IVF_PREFIX_ATTRS_GETS: u64 = 4;
const FILTERED_SQ8_ATTRS_GETS: u64 = 5;
const TOP_K: usize = 1;
const NPROBE_ALL: usize = 4;

fn attr_map(id: &str, ordinal: i64, tenant: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("doc_id".to_string(), AttributeValue::String(id.to_string())),
        (
            "tenant".to_string(),
            AttributeValue::String(tenant.to_string()),
        ),
        ("ordinal".to_string(), AttributeValue::Integer(ordinal)),
    ])
}

fn ivf_vectors() -> Vec<VectorEntry> {
    [
        ("v0", [1.0, 0.0, 0.0, 0.0], "keep"),
        ("v1", [100.0, 0.0, 0.0, 0.0], "drop"),
        ("v2", [0.0, 100.0, 0.0, 0.0], "drop"),
        ("v3", [0.0, 0.0, 100.0, 0.0], "drop"),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, (id, values, tenant))| VectorEntry {
        id: id.to_string(),
        values: values.to_vec(),
        attributes: Some(attr_map(id, i as i64, tenant)),
    })
    .collect()
}

fn hierarchical_vectors() -> Vec<VectorEntry> {
    let mut vectors = Vec::new();
    let centers = [
        [1.0, 0.0, 0.0, 0.0],
        [100.0, 0.0, 0.0, 0.0],
        [0.0, 100.0, 0.0, 0.0],
        [0.0, 0.0, 100.0, 0.0],
    ];

    for (cluster, center) in centers.into_iter().enumerate() {
        for offset in 0..8 {
            let id = if cluster == 0 && offset == 0 {
                "h0".to_string()
            } else {
                format!("h{cluster}_{offset}")
            };
            let mut values = center.to_vec();
            values[3] = offset as f32 * 0.01;
            vectors.push(VectorEntry {
                id: id.clone(),
                values,
                attributes: Some(attr_map(&id, (cluster * 8 + offset) as i64, "keep")),
            });
        }
    }

    vectors
}

fn attrs_by_id(
    vectors: &[VectorEntry],
) -> HashMap<String, Option<HashMap<String, AttributeValue>>> {
    vectors
        .iter()
        .map(|v| (v.id.clone(), v.attributes.clone()))
        .collect()
}

fn assert_attrs_match_vectors(
    results: &[SearchResult],
    expected: &HashMap<String, Option<HashMap<String, AttributeValue>>>,
) {
    for result in results {
        assert_eq!(
            &result.attributes,
            expected
                .get(&result.id)
                .unwrap_or_else(|| panic!("unexpected result id {}", result.id)),
            "attributes for {} changed",
            result.id
        );
    }
}

fn ivf_config(quantization: QuantizationType) -> IndexingConfig {
    IndexingConfig {
        default_num_centroids: NPROBE_ALL,
        kmeans_max_iterations: 10,
        quantization,
        pq_m: 2,
        bitmap_index: false,
        ..Default::default()
    }
}

#[tokio::test]
async fn ivf_unfiltered_attrs_are_lazy_but_enrichment_is_identical() {
    for quantization in [
        QuantizationType::None,
        QuantizationType::Scalar,
        QuantizationType::Product,
    ] {
        let harness = TestHarness::new().await;
        let (store, counter) = counting_store(&harness.store);
        let ns = harness.key(&format!("ivf-lazy-{quantization:?}"));
        let vectors = ivf_vectors();
        let expected_attrs = attrs_by_id(&vectors);

        let index =
            IvfFlatIndex::build(&vectors, &ivf_config(quantization), &store, &ns, "seg_lazy")
                .await
                .unwrap();

        counter.reset();
        let results = index
            .search(
                &vectors[0].values,
                TOP_K,
                NPROBE_ALL,
                None,
                DistanceMetric::Euclidean,
                &store,
            )
            .await
            .unwrap();

        assert_eq!(results.len(), TOP_K);
        assert_eq!(results[0].id, "v0");
        assert_attrs_match_vectors(&results, &expected_attrs);

        let attrs_gets = counter.gets_for(ArtifactClass::Attrs);
        assert_eq!(
            attrs_gets, 1,
            "unfiltered {quantization:?} search should fetch attrs only for the final top-k cluster",
        );
        assert!(
            attrs_gets < IVF_PREFIX_ATTRS_GETS,
            "unfiltered {quantization:?} search must reduce attrs GETs from the pre-fix eager baseline",
        );

        harness.cleanup().await;
    }
}

#[tokio::test]
async fn ivf_filtered_sq8_attrs_get_count_is_unchanged() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("ivf-filtered-sq8-unchanged");
    let vectors = ivf_vectors();
    let expected_attrs = attrs_by_id(&vectors);
    let index = IvfFlatIndex::build(
        &vectors,
        &ivf_config(QuantizationType::Scalar),
        &store,
        &ns,
        "seg_filtered",
    )
    .await
    .unwrap();
    let filter = Filter::Eq {
        field: "tenant".to_string(),
        value: AttributeValue::String("keep".to_string()),
    };

    counter.reset();
    let results = index
        .search(
            &vectors[0].values,
            TOP_K,
            NPROBE_ALL,
            Some(&filter),
            DistanceMetric::Euclidean,
            &store,
        )
        .await
        .unwrap();

    assert_eq!(results.len(), TOP_K);
    assert_eq!(results[0].id, "v0");
    assert_attrs_match_vectors(&results, &expected_attrs);
    assert_eq!(
        counter.gets_for(ArtifactClass::Attrs),
        FILTERED_SQ8_ATTRS_GETS,
        "filtered SQ8 must keep the pre-fix attrs GET profile: four coarse attrs GETs plus one rerank attrs GET",
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn hierarchical_flat_unfiltered_attrs_are_lazy_but_enrichment_is_identical() {
    let harness = TestHarness::new().await;
    let (store, counter) = counting_store(&harness.store);
    let ns = harness.key("hierarchical-flat-lazy");
    let vectors = hierarchical_vectors();
    let expected_attrs = attrs_by_id(&vectors);
    let config = IndexingConfig {
        default_num_centroids: NPROBE_ALL,
        kmeans_max_iterations: 10,
        quantization: QuantizationType::None,
        hierarchical: true,
        leaf_size: Some(8),
        bitmap_index: false,
        ..Default::default()
    };

    let index = HierarchicalIndex::build(&vectors, &config, &store, &ns, "seg_h_lazy")
        .await
        .unwrap();

    counter.reset();
    let results = index
        .search(
            &vectors[0].values,
            TOP_K,
            NPROBE_ALL,
            None,
            DistanceMetric::Euclidean,
            &store,
        )
        .await
        .unwrap();

    assert_eq!(results.len(), TOP_K);
    assert_eq!(results[0].id, "h0");
    assert_attrs_match_vectors(&results, &expected_attrs);

    let attrs_gets = counter.gets_for(ArtifactClass::Attrs);
    assert_eq!(
        attrs_gets, 1,
        "unfiltered hierarchical flat search should fetch attrs only for the final top-k cluster",
    );
    assert!(
        attrs_gets < IVF_PREFIX_ATTRS_GETS,
        "unfiltered hierarchical flat search must reduce attrs GETs from the pre-fix eager baseline",
    );

    harness.cleanup().await;
}
