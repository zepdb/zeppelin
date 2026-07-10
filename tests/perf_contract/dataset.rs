//! Deterministic synthetic data with closed-form cluster expectations.

use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

const CENTER_SCALE: f32 = 100.0;
const POINT_JITTER: f32 = 0.5;

/// Closed, stem-stable vocabulary used only by performance-contract datasets.
pub const PERF_VOCAB: [&str; 32] = [
    "alpha", "beta", "gamma", "delta", "vector", "matrix", "query", "index", "forest", "river",
    "mountain", "planet", "signal", "orbit", "canvas", "copper", "silver", "gold", "crystal",
    "ember", "velvet", "anchor", "bridge", "circle", "dragon", "engine", "fabric", "galaxy",
    "harbor", "island", "jungle", "kernel",
];

/// Shape of generated filterable attributes.
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttrShape {
    None,
    Category { cardinality: usize },
}

/// Shape of generated full-text-search documents.
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FtsShape {
    None,
    Vocab { words: usize, doc_len: usize },
}

/// Complete input to deterministic dataset generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetSpec {
    pub vectors: usize,
    pub dims: usize,
    pub nlist: usize,
    pub seed: u64,
    pub attrs: AttrShape,
    pub fts: FtsShape,
}

/// Stable small shape shared by the Tier-1 catalog and later scaling checks.
pub const SHAPE_SMALL: DatasetSpec = DatasetSpec {
    vectors: 4_096,
    dims: 64,
    nlist: 8,
    seed: 7,
    attrs: AttrShape::None,
    fts: FtsShape::None,
};

/// Stable medium shape reserved for Phase-3 shape-scaling validation.
pub const SHAPE_MEDIUM: DatasetSpec = DatasetSpec {
    vectors: 32_768,
    dims: 128,
    nlist: 32,
    seed: 7,
    attrs: AttrShape::None,
    fts: FtsShape::None,
};

/// A generated vector in the row-oriented HTTP upsert shape.
///
/// `BTreeMap` keeps future attribute generation and artifact serialization in
/// canonical key order. Phase 1 always emits `None`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenVector {
    pub id: String,
    pub values: Vec<f32>,
    pub attributes: Option<BTreeMap<String, serde_json::Value>>,
}

/// A deterministic query and its full nearest synthetic-blob order.
///
/// Scenario code truncates `probe_clusters` to its configured `nprobe`. These
/// are construction-time blob labels, not persisted IVF cluster indexes:
/// k-means may deterministically permute labels. Any runner that needs persisted
/// labels must derive that mapping from segment membership.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedQuery {
    pub vector: Vec<f32>,
    pub probe_clusters: Vec<usize>,
}

/// Closed-form values written to `expected.json` with run artifacts.
///
/// `probe_clusters` uses construction-time blob labels. Every blob has the same
/// byte size, so label permutation during k-means does not change byte totals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetExpectations {
    pub rows_per_cluster: usize,
    pub cluster_f32_bytes: u64,
    pub probe_clusters: Vec<Vec<usize>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category_matches_per_value: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fts_vocab_words: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fts_doc_len: Option<usize>,
}

/// Generated rows, their construction-time blobs, planned probes, and all
/// values that can be computed without reading object storage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedDataset {
    pub spec: DatasetSpec,
    pub vectors: Vec<GenVector>,
    pub blob_of: Vec<usize>,
    pub queries: Vec<PlannedQuery>,
    pub expected: DatasetExpectations,
}

/// Generates balanced, well-separated blobs in a fixed blob-major row order.
///
/// Each blob center is `100.0` on its own dimension and zero elsewhere. Every
/// point receives seeded per-coordinate jitter in `[-0.5, 0.5]`. This makes the
/// center and midpoint query order computable directly from the geometry.
#[must_use]
pub fn generate(spec: DatasetSpec) -> GeneratedDataset {
    validate_spec(&spec);

    let rows_per_cluster = spec.vectors / spec.nlist;
    let centers = blob_centers(spec.nlist, spec.dims);
    let mut rng = StdRng::seed_from_u64(spec.seed);
    let mut vectors = Vec::with_capacity(spec.vectors);
    let mut blob_of = Vec::with_capacity(spec.vectors);

    for (blob, center) in centers.iter().enumerate() {
        for row in 0..rows_per_cluster {
            let values = center
                .iter()
                .map(|coordinate| coordinate + rng.gen_range(-POINT_JITTER..=POINT_JITTER))
                .collect();
            let vector_index = blob * rows_per_cluster + row;
            let attributes = generated_attributes(&spec, vector_index);
            vectors.push(GenVector {
                id: format!("perf-{vector_index:08}"),
                values,
                attributes,
            });
            blob_of.push(blob);
        }
    }

    let mut queries = Vec::with_capacity(spec.nlist + 1);
    for (blob, center) in centers.iter().enumerate() {
        queries.push(PlannedQuery {
            vector: center.clone(),
            probe_clusters: center_probe_order(blob, spec.nlist),
        });
    }
    queries.push(PlannedQuery {
        vector: first_pair_midpoint(&centers),
        probe_clusters: midpoint_probe_order(spec.nlist),
    });

    let cluster_f32_bytes = u64::try_from(rows_per_cluster)
        .expect("rows_per_cluster must fit in u64")
        .checked_mul(u64::try_from(spec.dims).expect("dims must fit in u64"))
        .and_then(|values| values.checked_mul(4))
        .expect("cluster f32 byte count overflowed u64");
    let probe_clusters = queries
        .iter()
        .map(|query| query.probe_clusters.clone())
        .collect();
    let category_matches_per_value = match &spec.attrs {
        AttrShape::None => None,
        AttrShape::Category { cardinality } => Some(spec.vectors / cardinality),
    };
    let fts_vocab_words = match &spec.fts {
        FtsShape::None => None,
        FtsShape::Vocab { words, .. } => Some(*words),
    };
    let fts_doc_len = match &spec.fts {
        FtsShape::None => None,
        FtsShape::Vocab { doc_len, .. } => Some(*doc_len),
    };

    GeneratedDataset {
        spec,
        vectors,
        blob_of,
        queries,
        expected: DatasetExpectations {
            rows_per_cluster,
            cluster_f32_bytes,
            probe_clusters,
            category_matches_per_value,
            fts_vocab_words,
            fts_doc_len,
        },
    }
}

fn generated_attributes(
    spec: &DatasetSpec,
    vector_index: usize,
) -> Option<BTreeMap<String, serde_json::Value>> {
    let mut attributes = BTreeMap::new();
    if let AttrShape::Category { cardinality } = spec.attrs {
        attributes.insert(
            "cat".to_string(),
            serde_json::Value::String(format!("c{}", vector_index % cardinality)),
        );
    }
    if let FtsShape::Vocab { words, doc_len } = spec.fts {
        let content = (0..doc_len)
            .map(|offset| PERF_VOCAB[(vector_index + offset) % words])
            .collect::<Vec<_>>()
            .join(" ");
        attributes.insert("content".to_string(), serde_json::Value::String(content));
    }
    (!attributes.is_empty()).then_some(attributes)
}

fn validate_spec(spec: &DatasetSpec) {
    assert!(
        spec.vectors > 0,
        "perf dataset requires at least one vector"
    );
    assert!(
        spec.nlist >= 2,
        "perf dataset requires nlist >= 2 for its midpoint query"
    );
    assert!(
        spec.dims >= spec.nlist,
        "perf dataset requires dims >= nlist for distinct one-hot blob centers"
    );
    assert_eq!(
        spec.vectors % spec.nlist,
        0,
        "perf dataset vectors must be an exact multiple of nlist"
    );
    if let AttrShape::Category { cardinality } = spec.attrs {
        assert!(cardinality > 0, "category cardinality must be nonzero");
        assert_eq!(
            spec.vectors % cardinality,
            0,
            "category cardinality must divide the vector count"
        );
    }
    if let FtsShape::Vocab { words, doc_len } = spec.fts {
        assert!(words > 0, "FTS vocabulary size must be nonzero");
        assert!(
            words <= PERF_VOCAB.len(),
            "FTS vocabulary size exceeds the closed vocabulary"
        );
        assert!(doc_len > 0, "FTS document length must be nonzero");
    }
}

fn blob_centers(nlist: usize, dims: usize) -> Vec<Vec<f32>> {
    (0..nlist)
        .map(|blob| {
            let mut center = vec![0.0; dims];
            center[blob] = CENTER_SCALE;
            center
        })
        .collect()
}

fn center_probe_order(blob: usize, nlist: usize) -> Vec<usize> {
    let mut order = Vec::with_capacity(nlist);
    order.push(blob);
    order.extend((0..nlist).filter(|candidate| *candidate != blob));
    order
}

fn first_pair_midpoint(centers: &[Vec<f32>]) -> Vec<f32> {
    centers[0]
        .iter()
        .zip(&centers[1])
        .map(|(left, right)| (left + right) / 2.0)
        .collect()
}

fn midpoint_probe_order(nlist: usize) -> Vec<usize> {
    (0..nlist).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_spec() -> DatasetSpec {
        DatasetSpec {
            vectors: 16,
            dims: 8,
            nlist: 4,
            seed: 7,
            attrs: AttrShape::None,
            fts: FtsShape::None,
        }
    }

    #[test]
    #[ignore = "perf-contract selftest; run explicitly"]
    fn generation_is_byte_identical_for_the_same_spec() {
        let spec = test_spec();
        let first = serde_json::to_vec(&generate(spec.clone())).unwrap();
        let second = serde_json::to_vec(&generate(spec)).unwrap();

        assert_eq!(first, second);
    }

    #[test]
    #[ignore = "perf-contract selftest; run explicitly"]
    fn expectations_cover_every_center_and_the_first_midpoint() {
        let generated = generate(test_spec());

        assert_eq!(generated.expected.rows_per_cluster, 4);
        assert_eq!(generated.expected.cluster_f32_bytes, 4 * 8 * 4);
        assert_eq!(generated.queries.len(), 5);
        assert_eq!(generated.queries[0].probe_clusters, vec![0, 1, 2, 3]);
        assert_eq!(generated.queries[2].probe_clusters, vec![2, 0, 1, 3]);
        assert_eq!(generated.queries[4].probe_clusters, vec![0, 1, 2, 3]);
        assert_eq!(
            generated.expected.probe_clusters,
            generated
                .queries
                .iter()
                .map(|query| query.probe_clusters.clone())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    #[ignore = "perf-contract selftest; run explicitly"]
    #[should_panic(expected = "vectors must be an exact multiple of nlist")]
    fn rejects_unbalanced_blobs() {
        let mut spec = test_spec();
        spec.vectors = 17;
        let _ = generate(spec);
    }

    #[test]
    #[ignore = "perf-contract selftest; run explicitly"]
    fn category_shape_is_uniform_and_deterministic() {
        let mut spec = test_spec();
        spec.attrs = AttrShape::Category { cardinality: 4 };
        let generated = generate(spec);

        assert_eq!(generated.expected.category_matches_per_value, Some(4));
        for (index, vector) in generated.vectors.iter().enumerate() {
            assert_eq!(
                vector.attributes.as_ref().unwrap()["cat"],
                serde_json::json!(format!("c{}", index % 4))
            );
        }
    }

    #[test]
    #[ignore = "perf-contract selftest; run explicitly"]
    fn fts_shape_uses_only_the_closed_vocabulary() {
        let mut spec = test_spec();
        spec.fts = FtsShape::Vocab {
            words: 4,
            doc_len: 6,
        };
        let generated = generate(spec);

        assert_eq!(generated.expected.fts_vocab_words, Some(4));
        assert_eq!(generated.expected.fts_doc_len, Some(6));
        assert_eq!(
            generated.vectors[0].attributes.as_ref().unwrap()["content"],
            serde_json::json!("alpha beta gamma delta alpha beta")
        );
        assert_eq!(
            generated.vectors[1].attributes.as_ref().unwrap()["content"],
            serde_json::json!("beta gamma delta alpha beta gamma")
        );
    }

    #[test]
    #[ignore = "perf-contract selftest; run explicitly"]
    fn vocabulary_tokens_are_stable_and_distinct() {
        use zeppelin::fts::tokenizer::tokenize_text;
        use zeppelin::fts::FtsFieldConfig;

        let config = FtsFieldConfig::default();
        let mut stems = BTreeMap::new();
        for word in PERF_VOCAB {
            let tokens = tokenize_text(word, &config, false);
            assert_eq!(tokens.len(), 1, "{word} produced {tokens:?}");
            let stem = tokens[0].clone();
            assert!(!stem.is_empty(), "{word} was removed by the tokenizer");
            assert_eq!(
                tokenize_text(&stem, &config, false),
                vec![stem.clone()],
                "{word} stem {stem} was not stable"
            );
            assert!(
                stems.insert(stem.clone(), word).is_none(),
                "{word} collided on stem {stem}"
            );
        }
    }

    #[test]
    #[ignore = "perf-contract selftest; run explicitly"]
    fn standard_shapes_remain_pinned() {
        assert_eq!(
            (SHAPE_SMALL.vectors, SHAPE_SMALL.dims, SHAPE_SMALL.nlist),
            (4_096, 64, 8)
        );
        assert_eq!(
            (SHAPE_MEDIUM.vectors, SHAPE_MEDIUM.dims, SHAPE_MEDIUM.nlist),
            (32_768, 128, 32)
        );
    }
}
