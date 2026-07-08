use std::collections::{BTreeMap, HashMap, VecDeque};

use rand::rngs::StdRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::json;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric};

use super::model::Model;
use super::ops::{GenVector, GeneratedQuery, NamespaceSpec, Op, QueryOracleClass};

const DELETE_REUPSERT_TAG: &str = "delete-then-reupsert";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Coverage {
    pub op_counts: BTreeMap<String, u64>,
    pub tag_counts: BTreeMap<String, u64>,
}

impl Coverage {
    pub fn record(&mut self, op: &Op) {
        *self.op_counts.entry(op.kind().to_string()).or_default() += 1;
        for tag in op.tags() {
            *self.tag_counts.entry(tag.to_string()).or_default() += 1;
        }
    }

    pub fn merge(&mut self, other: &Coverage) {
        for (kind, count) in &other.op_counts {
            *self.op_counts.entry(kind.clone()).or_default() += count;
        }
        for (tag, count) in &other.tag_counts {
            *self.tag_counts.entry(tag.clone()).or_default() += count;
        }
    }
}

pub struct AdversarialGenerator {
    rng: StdRng,
    namespaces: Vec<GenNamespace>,
    pending: VecDeque<Op>,
    scenario: ScenarioState,
}

#[derive(Debug, Clone)]
struct GenNamespace {
    name: String,
    spec: NamespaceSpec,
    next_id: u64,
}

#[derive(Debug, Clone, Default)]
enum ScenarioState {
    #[default]
    Waiting,
    Delete,
    FetchDeleted,
    QueryDeleted,
    Reupsert,
    QueryReupserted,
    Compact,
    QueryCompacted,
    Done,
}

impl AdversarialGenerator {
    #[must_use]
    pub fn new(seed: u64, namespace_prefix: &str) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let namespace_count = rng.gen_range(2..=4);
        let mut namespaces = Vec::new();
        let mut pending = VecDeque::new();

        for index in 0..namespace_count {
            let dims = *[2usize, 4, 8].choose(&mut rng).unwrap();
            let metric = *[
                DistanceMetric::Cosine,
                DistanceMetric::Euclidean,
                DistanceMetric::DotProduct,
            ]
            .choose(&mut rng)
            .unwrap();
            let quantization = if index == 0 {
                QuantizationType::None
            } else {
                *[QuantizationType::Scalar, QuantizationType::Product]
                    .choose(&mut rng)
                    .unwrap()
            };
            let spec = NamespaceSpec {
                dims,
                metric,
                quantization,
                num_centroids: 4,
                fts_fields: Vec::new(),
                bitmap: rng.gen_bool(0.5),
            };
            let name = format!("{namespace_prefix}-adv-{seed}-{index}");
            pending.push_back(Op::CreateNamespace {
                ns: name.clone(),
                spec: spec.clone(),
            });
            namespaces.push(GenNamespace {
                name,
                spec,
                next_id: 0,
            });
        }

        let mut generator = Self {
            rng,
            namespaces,
            pending,
            scenario: ScenarioState::Waiting,
        };
        let exact_ns = generator
            .namespaces
            .iter()
            .position(|namespace| namespace.spec.is_exact())
            .unwrap();
        let ns = generator.namespaces[exact_ns].name.clone();
        let vectors = generator.make_vectors(exact_ns, 8);
        generator.pending.push_back(Op::Upsert { ns, vectors });
        generator
    }

    #[must_use]
    pub fn specs(&self) -> BTreeMap<String, NamespaceSpec> {
        self.namespaces
            .iter()
            .map(|namespace| (namespace.name.clone(), namespace.spec.clone()))
            .collect()
    }

    pub fn next(&mut self, model: &Model) -> Op {
        if let Some(op) = self.pending.pop_front() {
            return op;
        }
        if let Some(op) = self.next_scenario_op(model) {
            return op;
        }
        self.weighted_op(model)
    }

    pub fn exhaustive_query(
        &mut self,
        model: &Model,
        ns: &str,
        tag: Option<&str>,
    ) -> GeneratedQuery {
        let namespace = self.namespace(ns).clone();
        let (candidate_count, exact_allowed) = model
            .namespaces
            .get(ns)
            .map(|ns_model| {
                (
                    ns_model.live.len() + ns_model.wal_tombstones.len(),
                    ns_model.wal_tombstones.is_empty(),
                )
            })
            .unwrap_or((0, true));
        self.query_for(
            &namespace,
            candidate_count.max(1),
            candidate_count.max(1),
            exact_allowed,
            tag,
        )
    }

    fn next_scenario_op(&mut self, model: &Model) -> Option<Op> {
        let exact_index = self
            .namespaces
            .iter()
            .position(|namespace| namespace.spec.is_exact())?;
        let ns = self.namespaces[exact_index].name.clone();
        let live_ids: Vec<String> = model
            .namespaces
            .get(&ns)?
            .live
            .keys()
            .filter(|id| !id.starts_with("__phantom"))
            .take(3)
            .cloned()
            .collect();
        match self.scenario {
            ScenarioState::Waiting if live_ids.len() >= 3 => {
                self.scenario = ScenarioState::FetchDeleted;
                Some(Op::DeleteVectors {
                    ns,
                    ids: live_ids.into_iter().take(2).collect(),
                })
            }
            ScenarioState::FetchDeleted => {
                self.scenario = ScenarioState::QueryDeleted;
                let ids = model.namespaces[&ns].live.keys().cloned().collect();
                Some(Op::FetchVectors {
                    ns,
                    ids,
                    consistency: ConsistencyLevel::Strong,
                })
            }
            ScenarioState::QueryDeleted => {
                self.scenario = ScenarioState::Reupsert;
                let q = self.exhaustive_query(model, &ns, Some(DELETE_REUPSERT_TAG));
                Some(Op::Query { ns, q, as_of: None })
            }
            ScenarioState::Reupsert => {
                self.scenario = ScenarioState::QueryReupserted;
                let deleted: Vec<String> = model.namespaces[&ns]
                    .deleted_ever
                    .iter()
                    .take(2)
                    .cloned()
                    .collect();
                let dims = self.namespaces[exact_index].spec.dims;
                let vectors = deleted
                    .into_iter()
                    .map(|id| GenVector {
                        id,
                        values: self.random_values(dims),
                        attributes: Some(self.random_attributes()),
                    })
                    .collect();
                Some(Op::Upsert { ns, vectors })
            }
            ScenarioState::QueryReupserted => {
                self.scenario = ScenarioState::Compact;
                let q = self.exhaustive_query(model, &ns, Some(DELETE_REUPSERT_TAG));
                Some(Op::Query { ns, q, as_of: None })
            }
            ScenarioState::Compact => {
                self.scenario = ScenarioState::QueryCompacted;
                Some(Op::CompactInline { ns })
            }
            ScenarioState::QueryCompacted => {
                self.scenario = ScenarioState::Done;
                let q = self.exhaustive_query(model, &ns, Some(DELETE_REUPSERT_TAG));
                Some(Op::Query { ns, q, as_of: None })
            }
            _ => None,
        }
    }

    fn weighted_op(&mut self, model: &Model) -> Op {
        let roll = self.rng.gen_range(0..100);
        if roll < 30 {
            self.random_upsert(model)
        } else if roll < 40 {
            self.random_delete(model)
        } else if roll < 55 {
            self.random_fetch(model)
        } else if roll < 85 {
            self.random_query(model)
        } else if roll < 95 {
            self.random_compact(model)
        } else {
            self.random_get_namespace()
        }
    }

    fn random_upsert(&mut self, model: &Model) -> Op {
        let index = self.random_namespace_index();
        let ns = self.namespaces[index].name.clone();
        let live = model
            .namespaces
            .get(&ns)
            .map(|ns_model| ns_model.live.len())
            .unwrap_or(0);
        let capacity = 600usize.saturating_sub(live).max(1);
        let count = self.rng.gen_range(1..=20).min(capacity);
        let vectors = self.make_vectors(index, count);
        Op::Upsert { ns, vectors }
    }

    fn random_delete(&mut self, model: &Model) -> Op {
        let candidates = self.namespaces_with_live(model);
        if candidates.is_empty() {
            return self.random_upsert(model);
        }
        let deletable: Vec<String> = candidates
            .into_iter()
            .filter(|ns| {
                let namespace = self.namespace(ns);
                let live_len = model.namespaces[ns].live.len();
                !namespace.spec.is_exact() || live_len > 1
            })
            .collect();
        if deletable.is_empty() {
            return self.random_upsert(model);
        }
        let ns = deletable.choose(&mut self.rng).unwrap().clone();
        let live_ids: Vec<String> = model.namespaces[&ns].live.keys().cloned().collect();
        let namespace = self.namespace(&ns);
        let max_count = if namespace.spec.is_exact() {
            live_ids.len().saturating_sub(1).min(10)
        } else {
            live_ids.len().min(10)
        };
        let count = self.rng.gen_range(1..=max_count);
        let ids = live_ids
            .choose_multiple(&mut self.rng, count)
            .cloned()
            .collect();
        Op::DeleteVectors { ns, ids }
    }

    fn random_fetch(&mut self, model: &Model) -> Op {
        let ns = self.random_namespace_name();
        let mut ids = Vec::new();
        if let Some(ns_model) = model.namespaces.get(&ns) {
            ids.extend(
                ns_model
                    .live
                    .keys()
                    .choose_multiple(&mut self.rng, ns_model.live.len().min(6))
                    .into_iter()
                    .cloned(),
            );
            ids.extend(
                ns_model
                    .deleted_ever
                    .iter()
                    .choose_multiple(&mut self.rng, ns_model.deleted_ever.len().min(2))
                    .into_iter()
                    .cloned(),
            );
        }
        while ids.len() < 8 {
            ids.push(format!("missing-{}", self.rng.gen::<u64>()));
        }
        ids.shuffle(&mut self.rng);
        Op::FetchVectors {
            ns,
            ids,
            consistency: ConsistencyLevel::Strong,
        }
    }

    fn random_query(&mut self, model: &Model) -> Op {
        let ns = self.random_namespace_name();
        let (candidate_count, exact_allowed) = model
            .namespaces
            .get(&ns)
            .map(|ns_model| {
                (
                    ns_model.live.len() + ns_model.wal_tombstones.len(),
                    ns_model.wal_tombstones.is_empty(),
                )
            })
            .unwrap_or((0, true));
        let top_k = self.rng.gen_range(1..=50);
        let q = {
            let namespace = self.namespace(&ns).clone();
            self.query_for(&namespace, top_k, candidate_count, exact_allowed, None)
        };
        Op::Query { ns, q, as_of: None }
    }

    fn random_compact(&mut self, model: &Model) -> Op {
        let candidates: Vec<String> = self
            .namespaces
            .iter()
            .filter(|namespace| {
                let Some(ns_model) = model.namespaces.get(&namespace.name) else {
                    return true;
                };
                !(namespace.spec.is_exact()
                    && ns_model.live.is_empty()
                    && !ns_model.wal_tombstones.is_empty())
            })
            .map(|namespace| namespace.name.clone())
            .collect();
        Op::CompactInline {
            ns: candidates
                .choose(&mut self.rng)
                .cloned()
                .unwrap_or_else(|| self.random_namespace_name()),
        }
    }

    fn random_get_namespace(&mut self) -> Op {
        Op::GetNamespace {
            ns: self.random_namespace_name(),
        }
    }

    fn query_for(
        &mut self,
        namespace: &GenNamespace,
        top_k: usize,
        candidate_count: usize,
        exact_allowed: bool,
        tag: Option<&str>,
    ) -> GeneratedQuery {
        let body = json!({
            "sources": [{
                "type": "ann",
                "vector": self.random_values(namespace.spec.dims),
                "nprobe": namespace.spec.num_centroids
            }],
            "fusion": { "type": "none" },
            "top_k": top_k,
            "candidate_k": candidate_count.max(1),
            "consistency": "strong",
            "include_attributes": true
        });
        let class = if namespace.spec.is_exact() && exact_allowed {
            QueryOracleClass::ExactAnn {
                top_k,
                consistency: ConsistencyLevel::Strong,
                filter: None,
            }
        } else {
            QueryOracleClass::Membership {
                consistency: ConsistencyLevel::Strong,
            }
        };
        GeneratedQuery {
            body,
            class,
            pattern_tags: tag.into_iter().map(str::to_string).collect(),
        }
    }

    fn make_vectors(&mut self, namespace_index: usize, count: usize) -> Vec<GenVector> {
        let dims = self.namespaces[namespace_index].spec.dims;
        let ns_name = self.namespaces[namespace_index].name.clone();
        (0..count)
            .map(|_| {
                let id = format!("{ns_name}-v{}", self.namespaces[namespace_index].next_id);
                self.namespaces[namespace_index].next_id += 1;
                GenVector {
                    id,
                    values: self.random_values(dims),
                    attributes: Some(self.random_attributes()),
                }
            })
            .collect()
    }

    fn random_values(&mut self, dims: usize) -> Vec<f32> {
        (0..dims)
            .map(|_| self.rng.gen_range(-10.0f32..10.0f32))
            .collect()
    }

    fn random_attributes(&mut self) -> HashMap<String, AttributeValue> {
        let mut attributes = HashMap::new();
        attributes.insert(
            "group".to_string(),
            AttributeValue::String(format!("g{}", self.rng.gen_range(0..4))),
        );
        attributes.insert(
            "bucket".to_string(),
            AttributeValue::Integer(self.rng.gen_range(0..16)),
        );
        attributes.insert(
            "flag".to_string(),
            AttributeValue::Bool(self.rng.gen_bool(0.5)),
        );
        attributes
    }

    fn random_namespace_index(&mut self) -> usize {
        self.rng.gen_range(0..self.namespaces.len())
    }

    fn random_namespace_name(&mut self) -> String {
        let index = self.random_namespace_index();
        self.namespaces[index].name.clone()
    }

    fn namespace(&self, ns: &str) -> &GenNamespace {
        self.namespaces
            .iter()
            .find(|namespace| namespace.name == ns)
            .unwrap_or_else(|| panic!("unknown generated namespace {ns}"))
    }

    fn namespaces_with_live(&self, model: &Model) -> Vec<String> {
        self.namespaces
            .iter()
            .filter(|namespace| {
                model
                    .namespaces
                    .get(&namespace.name)
                    .is_some_and(|ns_model| !ns_model.live.is_empty())
            })
            .map(|namespace| namespace.name.clone())
            .collect()
    }
}
