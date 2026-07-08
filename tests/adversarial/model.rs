use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use zeppelin::types::{AttributeValue, DistanceMetric};

use super::ops::{GenVector, GeneratedQuery, NamespaceSpec, Op};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum OracleMutation {
    DropDelete,
    SkewScore,
    PhantomId,
    LeakTombstone,
    FilterSkew,
    GcEatsLiveKey,
    StaleCheckpoint,
}

impl OracleMutation {
    #[must_use]
    pub fn from_key(key: &str) -> Self {
        match key {
            "drop-delete" => Self::DropDelete,
            "skew-score" => Self::SkewScore,
            "phantom-id" => Self::PhantomId,
            "leak-tombstone" => Self::LeakTombstone,
            "filter-skew" => Self::FilterSkew,
            "gc-eats-live-key" => Self::GcEatsLiveKey,
            "stale-checkpoint" => Self::StaleCheckpoint,
            other => panic!("unknown ZEPPELIN_ADVERSARIAL_SELFTEST mutation: {other}"),
        }
    }

    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            Self::DropDelete => "drop-delete",
            Self::SkewScore => "skew-score",
            Self::PhantomId => "phantom-id",
            Self::LeakTombstone => "leak-tombstone",
            Self::FilterSkew => "filter-skew",
            Self::GcEatsLiveKey => "gc-eats-live-key",
            Self::StaleCheckpoint => "stale-checkpoint",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Model {
    pub namespaces: BTreeMap<String, NsModel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRecord {
    pub values: Vec<f32>,
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsModel {
    pub spec: NamespaceSpec,
    pub oracle_class: NsOracleClass,
    pub live: BTreeMap<String, ModelRecord>,
    pub compacted_live: BTreeMap<String, ModelRecord>,
    pub wal_tombstones: BTreeSet<String>,
    pub checkpoints: BTreeMap<u64, BTreeMap<String, ModelRecord>>,
    pub snapshots: BTreeMap<String, u64>,
    pub retained_generations: BTreeSet<u64>,
    pub live_generation: u64,
    pub indeterminate: BTreeMap<String, IndeterminateWrite>,
    pub canonical_queries: Vec<GeneratedQuery>,
    pub deleted_ever: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NsOracleClass {
    ExactAnn,
    Membership,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndeterminateWrite {
    pub reason: String,
}

impl Model {
    #[must_use]
    pub fn namespace_names(&self) -> Vec<String> {
        self.namespaces.keys().cloned().collect()
    }

    pub fn apply(
        &mut self,
        op: &Op,
        status: u16,
        gen_after: Option<u64>,
        response: &serde_json::Value,
        mutation: Option<OracleMutation>,
    ) {
        if !(200..300).contains(&status) {
            return;
        }

        match op {
            Op::CreateNamespace { ns, spec } => {
                let generation = gen_after.unwrap_or(0);
                self.namespaces
                    .entry(ns.clone())
                    .or_insert_with(|| NsModel::new(spec.clone(), generation));
            }
            Op::Upsert { ns, vectors } => {
                let Some(model) = self.namespaces.get_mut(ns) else {
                    panic!("upsert acked for unknown namespace {ns}");
                };
                for vector in vectors {
                    model
                        .live
                        .insert(vector.id.clone(), ModelRecord::from(vector));
                    model.deleted_ever.remove(&vector.id);
                }
                if mutation == Some(OracleMutation::PhantomId) {
                    model.insert_phantom_once();
                }
                model.checkpoint(gen_after);
                if mutation == Some(OracleMutation::StaleCheckpoint) {
                    model.corrupt_latest_checkpoint();
                }
            }
            Op::DeleteVectors { ns, ids } => {
                let Some(model) = self.namespaces.get_mut(ns) else {
                    panic!("delete acked for unknown namespace {ns}");
                };
                if mutation != Some(OracleMutation::DropDelete) {
                    for (idx, id) in ids.iter().enumerate() {
                        model.live.remove(id);
                        if mutation != Some(OracleMutation::LeakTombstone) || idx > 0 {
                            model.wal_tombstones.insert(id.clone());
                        }
                        model.deleted_ever.insert(id.clone());
                    }
                }
                model.checkpoint(gen_after);
                if mutation == Some(OracleMutation::StaleCheckpoint) {
                    model.corrupt_latest_checkpoint();
                }
            }
            Op::CompactInline { ns } => {
                let Some(model) = self.namespaces.get_mut(ns) else {
                    panic!("compaction acked for unknown namespace {ns}");
                };
                model.compacted_live = model.live.clone();
                model.wal_tombstones.clear();
                model.checkpoint(gen_after);
                if mutation == Some(OracleMutation::StaleCheckpoint) {
                    model.corrupt_latest_checkpoint();
                }
            }
            Op::CompactEndpoint { ns } | Op::ProbeSandwich { ns, .. } => {
                let Some(model) = self.namespaces.get_mut(ns) else {
                    panic!("maintenance acked for unknown namespace {ns}");
                };
                model.compacted_live = model.live.clone();
                model.wal_tombstones.clear();
                model.checkpoint(gen_after);
                if mutation == Some(OracleMutation::StaleCheckpoint) {
                    model.corrupt_latest_checkpoint();
                }
            }
            Op::CreateSnapshot { ns, name } => {
                let Some(model) = self.namespaces.get_mut(ns) else {
                    panic!("snapshot acked for unknown namespace {ns}");
                };
                let generation = response["generation"]
                    .as_u64()
                    .or(gen_after)
                    .unwrap_or_else(|| {
                        panic!("snapshot ack missing generation for namespace {ns}")
                    });
                model.snapshots.insert(name.clone(), generation);
            }
            Op::DeleteSnapshot { ns, name } => {
                if let Some(model) = self.namespaces.get_mut(ns) {
                    model.snapshots.remove(name);
                }
            }
            Op::GcCycle { ns, .. } => {
                if let Some(model) = self.namespaces.get_mut(ns) {
                    model.retained_generations = response["retained_generations"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|value| {
                            value.as_u64().unwrap_or_else(|| {
                                panic!("gc retained generation was not a u64: {value}")
                            })
                        })
                        .collect();
                }
            }
            Op::CloneNamespace { source, target, .. } => {
                let Some(source_model) = self.namespaces.get(source) else {
                    panic!("clone acked for unknown source namespace {source}");
                };
                let generation = response["generation"]
                    .as_u64()
                    .or(gen_after)
                    .unwrap_or(source_model.live_generation);
                let live = source_model
                    .checkpoints
                    .get(&generation)
                    .cloned()
                    .unwrap_or_else(|| source_model.live.clone());
                let mut target_model = NsModel::new(source_model.spec.clone(), 1);
                target_model.live = live.clone();
                target_model.compacted_live = live.clone();
                target_model.checkpoints.insert(1, live);
                target_model.live_generation = 1;
                self.namespaces.insert(target.clone(), target_model);
            }
            Op::DeleteNamespace { ns } => {
                self.namespaces.remove(ns);
            }
            Op::GetNamespace { .. }
            | Op::FetchVectors { .. }
            | Op::Query { .. }
            | Op::BatchQuery { .. }
            | Op::PaginateAll { .. }
            | Op::InvalidProbe { .. }
            | Op::GetSnapshot { .. }
            | Op::ListSnapshots { .. }
            | Op::PatchIndexConfig { .. }
            | Op::Hydrate { .. } => {}
        }
    }
}

impl NsModel {
    #[must_use]
    pub fn new(spec: NamespaceSpec, generation: u64) -> Self {
        let oracle_class = if spec.is_exact() {
            NsOracleClass::ExactAnn
        } else {
            NsOracleClass::Membership
        };
        let mut checkpoints = BTreeMap::new();
        checkpoints.insert(generation, BTreeMap::new());
        Self {
            spec,
            oracle_class,
            live: BTreeMap::new(),
            compacted_live: BTreeMap::new(),
            wal_tombstones: BTreeSet::new(),
            checkpoints,
            snapshots: BTreeMap::new(),
            retained_generations: BTreeSet::new(),
            live_generation: generation,
            indeterminate: BTreeMap::new(),
            canonical_queries: Vec::new(),
            deleted_ever: BTreeSet::new(),
        }
    }

    pub fn checkpoint(&mut self, gen_after: Option<u64>) {
        let generation = gen_after.unwrap_or(self.live_generation);
        self.live_generation = generation;
        self.checkpoints.insert(generation, self.live.clone());
    }

    fn insert_phantom_once(&mut self) {
        let id = "__phantom_id".to_string();
        if self.live.contains_key(&id) {
            return;
        }
        let mut attributes = HashMap::new();
        attributes.insert("phantom".to_string(), AttributeValue::Bool(true));
        self.live.insert(
            id,
            ModelRecord {
                values: vec![99.0; self.spec.dims],
                attributes: Some(attributes),
            },
        );
    }

    fn corrupt_latest_checkpoint(&mut self) {
        let Some((_, checkpoint)) = self.checkpoints.iter_mut().next_back() else {
            return;
        };
        let Some(record) = checkpoint.values_mut().next() else {
            return;
        };
        let Some(first) = record.values.first_mut() else {
            return;
        };
        *first += 10_000.0;
    }
}

impl From<&GenVector> for ModelRecord {
    fn from(vector: &GenVector) -> Self {
        Self {
            values: vector.values.clone(),
            attributes: vector.attributes.clone(),
        }
    }
}

/// Scalar distance used by the adversarial oracle.
///
/// Pinned by `model_distance_matches_production_query_scores`: three known
/// vectors are queried through the HTTP API in `quantization=none` namespaces
/// for all three metrics, and these scalar scores must match the returned
/// production scores within the oracle epsilon.
#[must_use]
pub fn model_distance(metric: DistanceMetric, query: &[f32], values: &[f32]) -> f32 {
    assert_eq!(
        query.len(),
        values.len(),
        "model distance dimension mismatch"
    );
    match metric {
        DistanceMetric::Cosine => {
            let mut dot = 0.0f32;
            let mut norm_query = 0.0f32;
            let mut norm_values = 0.0f32;
            for (left, right) in query.iter().zip(values.iter()) {
                dot += left * right;
                norm_query += left * left;
                norm_values += right * right;
            }
            let denom = (norm_query * norm_values).sqrt();
            if denom < f32::EPSILON {
                1.0
            } else {
                1.0 - (dot / denom).clamp(-1.0, 1.0)
            }
        }
        DistanceMetric::Euclidean => query
            .iter()
            .zip(values.iter())
            .map(|(left, right)| {
                let delta = left - right;
                delta * delta
            })
            .sum(),
        DistanceMetric::DotProduct => -query
            .iter()
            .zip(values.iter())
            .map(|(left, right)| left * right)
            .sum::<f32>(),
    }
}

#[cfg(test)]
mod tests {
    use reqwest::Client;
    use serde_json::json;
    use zeppelin::config::Config;
    use zeppelin::index::quantization::QuantizationType;
    use zeppelin::types::DistanceMetric;

    use crate::adversarial::oracle::{score_close, SCORE_ABS_EPS, SCORE_REL_EPS};
    use crate::common::server::{cleanup_ns, start_test_server_with_compactor};

    use super::model_distance;

    #[tokio::test]
    #[ignore]
    async fn model_distance_matches_production_query_scores() {
        let metrics = [
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::DotProduct,
        ];
        for metric in metrics {
            let mut config = Config::load(None).unwrap();
            config.cache.manifest_cache_ttl_ms = 0;
            config.indexing.default_num_centroids = 1;
            let (base_url, harness, _cache, _cache_dir, _compactor) =
                start_test_server_with_compactor(Some(config)).await;
            let client = Client::new();
            let ns = format!("{}-distance-{}", harness.prefix, metric);
            let create = client
                .post(format!("{base_url}/v1/namespaces"))
                .json(&json!({
                    "name": ns,
                    "dimensions": 2,
                    "distance_metric": metric,
                    "index_config": {
                        "nlist": 1,
                        "quantization": QuantizationType::None,
                        "pq_m": 1,
                        "hierarchical": false,
                        "fts_index": false,
                        "bitmap_index": false
                    }
                }))
                .send()
                .await
                .unwrap();
            assert_eq!(create.status().as_u16(), 201);

            let vectors = [
                ("a", vec![1.0f32, 0.0]),
                ("b", vec![0.0f32, 2.0]),
                ("c", vec![2.0f32, 2.0]),
            ];
            let upsert = client
                .post(format!("{base_url}/v1/namespaces/{ns}/vectors"))
                .json(&json!({
                    "vectors": vectors.iter().map(|(id, values)| {
                        json!({ "id": id, "values": values })
                    }).collect::<Vec<_>>()
                }))
                .send()
                .await
                .unwrap();
            assert_eq!(upsert.status().as_u16(), 200);

            let query_vector = vec![1.0f32, 1.0];
            let response: serde_json::Value = client
                .post(format!("{base_url}/v1/namespaces/{ns}/query"))
                .json(&json!({
                    "vector": query_vector,
                    "top_k": 3,
                    "candidate_k": 3,
                    "nprobe": 1,
                    "consistency": "strong",
                    "include_attributes": true
                }))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            for result in response["results"].as_array().unwrap() {
                let id = result["id"].as_str().unwrap();
                let values = vectors
                    .iter()
                    .find(|(candidate, _)| *candidate == id)
                    .unwrap()
                    .1
                    .as_slice();
                let expected = model_distance(metric, &[1.0, 1.0], values);
                let actual = result["score"].as_f64().unwrap() as f32;
                assert!(
                    score_close(actual, expected),
                    "metric={metric:?} id={id} actual={actual} expected={expected} eps=({}, {})",
                    SCORE_ABS_EPS,
                    SCORE_REL_EPS
                );
            }

            cleanup_ns(&harness.store, &ns).await;
            harness.cleanup().await;
        }
    }
}
