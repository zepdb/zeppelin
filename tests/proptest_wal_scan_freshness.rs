mod common;

use std::collections::{HashMap, HashSet};

use proptest::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use zeppelin::compaction::Compactor;
use zeppelin::config::{CompactionConfig, IndexingConfig};
use zeppelin::index::distance::compute_distance;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::query::{execute_query, QueryParams};
use zeppelin::storage::ZeppelinStore;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter, VectorEntry};
use zeppelin::wal::{Manifest, WalReader, WalWriter};

use common::harness::TestHarness;

const DIM: usize = 4;

#[derive(Debug, Clone)]
struct ModelVector {
    id: String,
    values: Vec<f32>,
    attributes: HashMap<String, AttributeValue>,
}

#[derive(Debug, Clone)]
struct FragmentSpec {
    upserts: Vec<ModelVector>,
    deletes: Vec<String>,
}

#[derive(Debug, Clone)]
struct WalFreshnessCase {
    segment_vectors: Vec<ModelVector>,
    fragments: Vec<FragmentSpec>,
    query: Vec<f32>,
}

fn test_compactor(store: &ZeppelinStore) -> Compactor {
    Compactor::new(
        store.clone(),
        WalReader::new(store.clone()),
        CompactionConfig::default(),
        IndexingConfig {
            default_num_centroids: 1,
            default_nprobe: 1,
            quantization: QuantizationType::None,
            bitmap_index: false,
            ..Default::default()
        },
        common::default_gc_upload_window(),
    )
}

fn attrs(tenant: &str, version: i64) -> HashMap<String, AttributeValue> {
    let mut attrs = HashMap::new();
    attrs.insert(
        "tenant".to_string(),
        AttributeValue::String(tenant.to_string()),
    );
    attrs.insert("version".to_string(), AttributeValue::Integer(version));
    attrs
}

fn model_vector(id: &str, values: [f32; DIM], tenant: &str, version: i64) -> ModelVector {
    ModelVector {
        id: id.to_string(),
        values: values.to_vec(),
        attributes: attrs(tenant, version),
    }
}

fn random_model_vector(
    rng: &mut StdRng,
    id: &str,
    fragment_idx: usize,
    op_idx: usize,
) -> ModelVector {
    let base = 1.0 + fragment_idx as f32 * 0.73 + op_idx as f32 * 0.19;
    let jitter = rng.gen_range(0..=300) as f32 / 10_000.0;
    let values = [
        base + jitter,
        rng.gen_range(-100..=100) as f32 / 100.0,
        rng.gen_range(-100..=100) as f32 / 100.0,
        rng.gen_range(-100..=100) as f32 / 100.0,
    ];
    let tenant = if rng.gen_bool(0.65) { "keep" } else { "drop" };
    model_vector(id, values, tenant, (fragment_idx * 100 + op_idx) as i64)
}

fn build_case(seed: u64) -> WalFreshnessCase {
    let mut rng = StdRng::seed_from_u64(seed);
    let random_ids: Vec<String> = (0..12).map(|idx| format!("rand_{idx}")).collect();

    let segment_vectors = vec![model_vector(
        "filtered_override",
        [0.001, 0.0, 0.0, 0.0],
        "keep",
        0,
    )];

    let mut fragments = vec![
        FragmentSpec {
            upserts: vec![
                model_vector("hot", [3.0, 0.0, 0.0, 0.0], "keep", 1),
                model_vector("visible_keep", [0.05, 0.0, 0.0, 0.0], "keep", 1),
            ],
            deletes: Vec::new(),
        },
        FragmentSpec {
            upserts: vec![model_vector("hot", [2.0, 0.25, 0.0, 0.0], "drop", 2)],
            deletes: vec!["reupsert".to_string()],
        },
        FragmentSpec {
            upserts: vec![
                model_vector("reupsert", [0.10, 0.0, 0.0, 0.0], "keep", 3),
                model_vector("hot", [0.20, 0.0, 0.0, 0.0], "keep", 3),
            ],
            deletes: Vec::new(),
        },
        FragmentSpec {
            upserts: vec![model_vector(
                "filtered_override",
                [0.0001, 0.0, 0.0, 0.0],
                "drop",
                4,
            )],
            deletes: Vec::new(),
        },
    ];

    let extra_fragment_count = rng.gen_range(1..=4);
    for extra_idx in 0..extra_fragment_count {
        let fragment_idx = fragments.len() + extra_idx;
        let upsert_count = rng.gen_range(1..=5);
        let mut upsert_ids = HashSet::new();
        let mut upserts = Vec::with_capacity(upsert_count);
        while upserts.len() < upsert_count {
            let id = random_ids[rng.gen_range(0..random_ids.len())].clone();
            if upsert_ids.insert(id.clone()) {
                upserts.push(random_model_vector(
                    &mut rng,
                    &id,
                    fragment_idx,
                    upserts.len(),
                ));
            }
        }

        let delete_count = rng.gen_range(0..=3);
        let mut deletes = Vec::with_capacity(delete_count);
        let mut delete_ids = HashSet::new();
        while deletes.len() < delete_count {
            let id = random_ids[rng.gen_range(0..random_ids.len())].clone();
            if upsert_ids.contains(&id) {
                continue;
            }
            if delete_ids.insert(id.clone()) {
                deletes.push(id);
            }
        }

        fragments.push(FragmentSpec { upserts, deletes });
    }

    WalFreshnessCase {
        segment_vectors,
        fragments,
        query: vec![0.0; DIM],
    }
}

fn to_entry(vector: &ModelVector) -> VectorEntry {
    VectorEntry {
        id: vector.id.clone(),
        values: vector.values.clone(),
        attributes: Some(vector.attributes.clone()),
    }
}

fn tenant_keep_filter() -> Filter {
    Filter::Eq {
        field: "tenant".to_string(),
        value: AttributeValue::String("keep".to_string()),
    }
}

fn passes_filter(vector: &ModelVector, filter_enabled: bool) -> bool {
    if !filter_enabled {
        return true;
    }
    vector.attributes.get("tenant") == Some(&AttributeValue::String("keep".to_string()))
}

fn materialize(case: &WalFreshnessCase) -> HashMap<String, ModelVector> {
    let mut latest = HashMap::new();
    for vector in &case.segment_vectors {
        latest.insert(vector.id.clone(), vector.clone());
    }

    for fragment in &case.fragments {
        for id in &fragment.deletes {
            latest.remove(id);
        }
        for vector in &fragment.upserts {
            latest.insert(vector.id.clone(), vector.clone());
        }
    }

    latest
}

fn oracle(case: &WalFreshnessCase, filter_enabled: bool, top_k: usize) -> Vec<(String, f32)> {
    let mut results: Vec<(String, f32)> = materialize(case)
        .into_values()
        .filter(|vector| passes_filter(vector, filter_enabled))
        .map(|vector| {
            (
                vector.id,
                compute_distance(
                    case.query.as_slice(),
                    vector.values.as_slice(),
                    DistanceMetric::Euclidean,
                ),
            )
        })
        .collect();
    results.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    results.truncate(top_k);
    results
}

async fn write_case(
    store: &ZeppelinStore,
    namespace: &str,
    case: &WalFreshnessCase,
) -> Result<(), String> {
    common::write_active_namespace_metadata(store, namespace, DIM, DistanceMetric::Euclidean).await;
    Manifest::new()
        .write(store, namespace)
        .await
        .map_err(|e| format!("write manifest: {e}"))?;

    let writer = WalWriter::new(store.clone());
    writer
        .append(
            namespace,
            case.segment_vectors.iter().map(to_entry).collect(),
            Vec::new(),
        )
        .await
        .map_err(|e| format!("append baseline segment vectors: {e}"))?;
    test_compactor(store)
        .compact(namespace)
        .await
        .map_err(|e| format!("compact baseline segment: {e}"))?;

    for (fragment_idx, fragment) in case.fragments.iter().enumerate() {
        writer
            .append(
                namespace,
                fragment.upserts.iter().map(to_entry).collect(),
                fragment.deletes.clone(),
            )
            .await
            .map_err(|e| format!("append WAL fragment {fragment_idx}: {e}"))?;
    }

    Ok(())
}

async fn assert_case(seed: u64, random_top_k: usize) -> Result<(), String> {
    let case = build_case(seed);
    let harness = TestHarness::new().await;
    let namespace = harness.key(&format!("wal-scan-freshness-{seed}"));

    let result = async {
        write_case(&harness.store, &namespace, &case).await?;
        let wal_reader = WalReader::new(harness.store.clone());

        for filter_enabled in [false, true] {
            let survivor_count = oracle(&case, filter_enabled, usize::MAX).len();
            let mut top_ks = vec![1, random_top_k.max(1), survivor_count + 5];
            top_ks.sort_unstable();
            top_ks.dedup();

            for top_k in top_ks {
                let filter = filter_enabled.then(tenant_keep_filter);
                let response = execute_query(QueryParams {
                    store: &harness.store,
                    wal_reader: &wal_reader,
                    namespace: &namespace,
                    query: &case.query,
                    top_k,
                    nprobe: 1,
                    filter: filter.as_ref(),
                    consistency: ConsistencyLevel::Strong,
                    distance_metric: DistanceMetric::Euclidean,
                    oversample_factor: 3,
                    rerank_coalesce_gap_bytes: zeppelin::config::DEFAULT_RERANK_COALESCE_GAP_BYTES,
                    cache: None,
                    manifest_cache: None,
                    include_attributes: true,
                })
                .await
                .map_err(|e| format!("execute query: {e}"))?;

                let expected = oracle(&case, filter_enabled, top_k);
                let actual: Vec<(String, f32)> = response
                    .results
                    .iter()
                    .map(|result| (result.id.clone(), result.score))
                    .collect();

                if actual.len() != expected.len() {
                    return Err(format!(
                        "seed={seed} filter_enabled={filter_enabled} top_k={top_k}: \
                         result length mismatch\ncase={case:#?}\nexpected={expected:?}\nactual={actual:?}"
                    ));
                }
                for ((actual_id, actual_score), (expected_id, expected_score)) in
                    actual.iter().zip(expected.iter())
                {
                    if actual_id != expected_id || (actual_score - expected_score).abs() > 1e-6 {
                        return Err(format!(
                            "seed={seed} filter_enabled={filter_enabled} top_k={top_k}: \
                             result mismatch\ncase={case:#?}\nexpected={expected:?}\nactual={actual:?}"
                        ));
                    }
                }
            }
        }

        Ok(())
    }
    .await;

    harness.cleanup().await;
    result
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn wal_scan_matches_full_materialization_oracle(
        seed in any::<u64>(),
        random_top_k in 1usize..=20,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for WAL freshness proptest");
        let result = runtime.block_on(assert_case(seed, random_top_k));
        prop_assert!(result.is_ok(), "{}", result.unwrap_err());
    }
}
