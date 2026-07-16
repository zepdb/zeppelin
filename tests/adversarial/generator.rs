use std::collections::{BTreeMap, HashMap, VecDeque};

use rand::rngs::StdRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::json;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter};

use super::model::Model;
use super::ops::{
    ActorSel, AsOfTarget, GenVector, GeneratedQuery, InvalidProbe, MaintenanceKind, NamespaceSpec,
    Op, QueryOracleClass,
};
use super::security_program::{SecurityProgramConfig, SECURITY_PROGRAM_START_OP};
use super::vocab;

const DELETE_REUPSERT_TAG: &str = "delete-then-reupsert";
const EVENTUAL_TOMBSTONE_TAG: &str = "eventual-tombstone";
const FILTER_TAG: &str = "filter";
const FTS_TAG: &str = "fts";
const PAGINATION_TAG: &str = "pagination";
const BATCH_TAG: &str = "batch";
const SKETCH_ADC_TAG: &str = "sketch-adc-v4";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Coverage {
    pub op_counts: BTreeMap<String, u64>,
    pub tag_counts: BTreeMap<String, u64>,
    #[serde(default)]
    pub security_oracle_counts: BTreeMap<String, u64>,
}

impl Coverage {
    pub fn record(&mut self, op: &Op) {
        *self.op_counts.entry(op.kind().to_string()).or_default() += 1;
        for tag in op.tags() {
            *self.tag_counts.entry(tag.to_string()).or_default() += 1;
        }
        if op.is_security_op() {
            self.record_security_oracle("I22");
        }
        match op {
            Op::TenantBoundaryProbe { .. } | Op::TokenExceedScopeProbe { .. } => {
                self.record_security_oracle("I23");
            }
            Op::UseToken { .. } => {
                self.record_security_oracle("I23");
                self.record_security_oracle("I27");
            }
            Op::RevokeParentThenUseToken { .. } => {
                self.record_security_oracle("I24");
            }
            Op::UseRevokedCredential { .. } | Op::UseExpiredToken { .. } => {
                self.record_security_oracle("I24");
            }
            Op::AuditBarrierOp { .. } => self.record_security_oracle("I25"),
            Op::QueryWithReceipt { .. }
            | Op::VerifyReceipt { .. }
            | Op::TamperArtifactThenVerify { .. }
            | Op::AuditChainCheck { .. } => self.record_security_oracle("I29"),
            _ => {}
        }
    }

    pub fn record_security_oracle(&mut self, id: &str) {
        *self
            .security_oracle_counts
            .entry(id.to_string())
            .or_default() += 1;
    }

    pub fn merge(&mut self, other: &Coverage) {
        for (kind, count) in &other.op_counts {
            *self.op_counts.entry(kind.clone()).or_default() += count;
        }
        for (tag, count) in &other.tag_counts {
            *self.tag_counts.entry(tag.clone()).or_default() += count;
        }
        for (id, count) in &other.security_oracle_counts {
            *self.security_oracle_counts.entry(id.clone()).or_default() += count;
        }
    }
}

pub struct AdversarialGenerator {
    rng: StdRng,
    namespaces: Vec<GenNamespace>,
    pending: VecDeque<Op>,
    scenario: ScenarioState,
    phase3_started: bool,
    security_program: Option<SecurityProgramConfig>,
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
        Self::new_with_security(seed, namespace_prefix, false, false)
    }

    #[must_use]
    pub fn new_security(seed: u64, namespace_prefix: &str) -> Self {
        Self::new_with_security(seed, namespace_prefix, true, false)
    }

    #[must_use]
    pub fn new_security_profile(seed: u64, namespace_prefix: &str) -> Self {
        Self::new_with_security(seed, namespace_prefix, true, true)
    }

    fn new_with_security(
        seed: u64,
        namespace_prefix: &str,
        security_enabled: bool,
        expiry_scenario: bool,
    ) -> Self {
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
                fts_fields: if index == 1 {
                    vec!["body".to_string()]
                } else {
                    Vec::new()
                },
                bitmap: rng.gen_bool(0.5),
            };
            let name = format!("{namespace_prefix}-adv-{seed}-{index}");
            pending.push_back(Op::CreateNamespace {
                actor: ActorSel::ADMIN,
                ns: name.clone(),
                spec: spec.clone(),
            });
            pending.push_back(Op::GetNamespace {
                actor: ActorSel::ADMIN,
                ns: name.clone(),
            });
            namespaces.push(GenNamespace {
                name,
                spec,
                next_id: 0,
            });
        }

        let sketch_spec = NamespaceSpec {
            dims: 8,
            metric: DistanceMetric::Euclidean,
            quantization: QuantizationType::None,
            num_centroids: 16,
            fts_fields: Vec::new(),
            bitmap: false,
        };
        let sketch_name = format!("{namespace_prefix}-adv-{seed}-sketch");
        pending.push_back(Op::CreateNamespace {
            actor: ActorSel::ADMIN,
            ns: sketch_name.clone(),
            spec: sketch_spec.clone(),
        });
        pending.push_back(Op::GetNamespace {
            actor: ActorSel::ADMIN,
            ns: sketch_name.clone(),
        });
        namespaces.push(GenNamespace {
            name: sketch_name,
            spec: sketch_spec,
            next_id: 0,
        });

        let mut generator = Self {
            rng,
            namespaces,
            pending,
            scenario: ScenarioState::Waiting,
            phase3_started: false,
            security_program: None,
        };
        let exact_ns = generator
            .namespaces
            .iter()
            .position(|namespace| namespace.spec.is_exact())
            .unwrap();
        let mut exact_vectors = Vec::new();
        for index in 0..namespace_count {
            let ns = generator.namespaces[index].name.clone();
            let count = if index == exact_ns { 8 } else { 12 };
            let vectors = generator.make_vectors(index, count);
            if index == exact_ns {
                exact_vectors = vectors.clone();
            }
            generator.pending.push_back(Op::Upsert {
                actor: ActorSel::ADMIN,
                ns: ns.clone(),
                vectors,
            });
            if !generator.namespaces[index].spec.fts_fields.is_empty() {
                generator.pending.push_back(Op::CompactInline {
                    actor: ActorSel::ADMIN,
                    ns,
                });
            }
        }
        if security_enabled {
            // Keep the security program on fixed logical indexes across the
            // seeded namespace-count variation. Fault choreography can then
            // target grant publication, bounded staleness, and the audit
            // barrier without coupling the scheduler to generator internals.
            let security_start_op = usize::try_from(SECURITY_PROGRAM_START_OP)
                .expect("security program start index must fit usize");
            let padding_ns = generator.namespaces[0].name.clone();
            while generator.pending.len() < security_start_op {
                generator.pending.push_back(Op::GetNamespace {
                    actor: ActorSel::ADMIN,
                    ns: padding_ns.clone(),
                });
            }
            assert_eq!(generator.pending.len(), security_start_op);
            let namespace_names = generator
                .namespaces
                .iter()
                .take(namespace_count)
                .map(|namespace| namespace.name.clone())
                .collect::<Vec<_>>();
            let security_program = if expiry_scenario {
                SecurityProgramConfig::for_security_profile(namespace_prefix, &namespace_names)
            } else {
                SecurityProgramConfig::for_seed(namespace_prefix, &namespace_names)
            };
            generator.pending.extend(security_program.scripted_ops());
            generator.security_program = Some(security_program);
            let receipt_namespace = generator.namespaces[0].clone();
            let receipt_query = Self::fixed_membership_query(
                &receipt_namespace,
                vec![0.0; receipt_namespace.spec.dims],
                8,
                receipt_namespace.spec.num_centroids,
                &["receipt"],
            );
            generator.pending.extend([
                Op::QueryWithReceipt {
                    actor: ActorSel::ADMIN,
                    ns: receipt_namespace.name.clone(),
                    q: receipt_query,
                },
                Op::VerifyReceipt {
                    actor: ActorSel::ADMIN,
                    ns: receipt_namespace.name.clone(),
                },
                Op::TamperArtifactThenVerify {
                    actor: ActorSel::ADMIN,
                    ns: receipt_namespace.name,
                },
                Op::AuditChainCheck {
                    actor: ActorSel::ADMIN,
                },
            ]);
        }
        generator.enqueue_phase2_script(exact_ns, &exact_vectors);
        generator.enqueue_sketch_adc_script(namespace_count);
        generator
    }

    #[must_use]
    pub fn security_program(&self) -> Option<&SecurityProgramConfig> {
        self.security_program.as_ref()
    }

    fn enqueue_sketch_adc_script(&mut self, namespace_index: usize) {
        let namespace = self.namespaces[namespace_index].clone();
        let initial = self.sketch_blob_vectors(&namespace, "initial", 0..16, 8);
        let query_vector = initial[0].values.clone();

        self.pending.push_back(Op::Upsert {
            actor: ActorSel::ADMIN,
            ns: namespace.name.clone(),
            vectors: initial,
        });
        self.pending.push_back(Op::CompactInline {
            actor: ActorSel::ADMIN,
            ns: namespace.name.clone(),
        });
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: namespace.name.clone(),
            q: Self::fixed_membership_query(
                &namespace,
                query_vector.clone(),
                10,
                128,
                &[SKETCH_ADC_TAG],
            ),
            as_of: None,
        });

        let incremental = self.sketch_blob_vectors(&namespace, "incremental", 0..4, 4);
        self.pending.push_back(Op::Upsert {
            actor: ActorSel::ADMIN,
            ns: namespace.name.clone(),
            vectors: incremental,
        });
        self.pending.push_back(Op::CompactInline {
            actor: ActorSel::ADMIN,
            ns: namespace.name.clone(),
        });
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: namespace.name.clone(),
            q: Self::fixed_membership_query(&namespace, query_vector, 10, 144, &[SKETCH_ADC_TAG]),
            as_of: None,
        });
    }

    fn sketch_blob_vectors(
        &mut self,
        namespace: &GenNamespace,
        batch: &str,
        blobs: std::ops::Range<usize>,
        rows_per_blob: usize,
    ) -> Vec<GenVector> {
        let mut vectors = Vec::with_capacity(blobs.len() * rows_per_blob);
        for blob in blobs {
            for row in 0..rows_per_blob {
                let mut values = (0..namespace.spec.dims)
                    .map(|_| self.rng.gen_range(-0.01f32..=0.01f32))
                    .collect::<Vec<_>>();
                values[0] += blob as f32 * 1_000.0;
                vectors.push(GenVector {
                    id: format!("{}-{batch}-b{blob:02}-r{row:02}", namespace.name),
                    values,
                    attributes: None,
                });
            }
        }
        vectors
    }

    fn enqueue_phase2_script(&mut self, exact_index: usize, exact_vectors: &[GenVector]) {
        if exact_vectors.len() < 3 {
            return;
        }
        let exact = self.namespaces[exact_index].clone();
        let delete_ids = vec![exact_vectors[0].id.clone(), exact_vectors[1].id.clone()];

        self.pending.push_back(Op::CompactInline {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
        });
        self.pending.push_back(Op::DeleteVectors {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            ids: delete_ids.clone(),
        });
        self.pending.push_back(Op::FetchVectors {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            ids: vec![
                exact_vectors[0].id.clone(),
                exact_vectors[1].id.clone(),
                exact_vectors[2].id.clone(),
            ],
            consistency: ConsistencyLevel::Eventual,
        });
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            q: Self::fixed_ann_query(
                &exact,
                exact_vectors[0].values.clone(),
                3,
                exact_vectors.len(),
                ConsistencyLevel::Eventual,
                None,
                &[EVENTUAL_TOMBSTONE_TAG],
            ),
            as_of: None,
        });

        let reupserted = delete_ids
            .iter()
            .map(|id| GenVector {
                id: id.clone(),
                values: self.random_values(exact.spec.dims),
                attributes: Some(self.random_attributes(!exact.spec.fts_fields.is_empty())),
            })
            .collect::<Vec<_>>();
        let reupsert_query = reupserted[0].values.clone();
        self.pending.push_back(Op::Upsert {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            vectors: reupserted,
        });
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            q: Self::fixed_ann_query(
                &exact,
                reupsert_query.clone(),
                3,
                exact_vectors.len(),
                ConsistencyLevel::Eventual,
                None,
                &[EVENTUAL_TOMBSTONE_TAG],
            ),
            as_of: None,
        });
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            q: Self::fixed_ann_query(
                &exact,
                reupsert_query.clone(),
                3,
                exact_vectors.len(),
                ConsistencyLevel::Strong,
                None,
                &[EVENTUAL_TOMBSTONE_TAG],
            ),
            as_of: None,
        });
        self.pending.push_back(Op::CompactInline {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
        });
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            q: Self::fixed_ann_query(
                &exact,
                reupsert_query.clone(),
                3,
                exact_vectors.len(),
                ConsistencyLevel::Eventual,
                None,
                &[EVENTUAL_TOMBSTONE_TAG],
            ),
            as_of: None,
        });

        let filtered = Filter::Eq {
            field: "group".to_string(),
            value: AttributeValue::String("g1".to_string()),
        };
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            q: Self::fixed_ann_query(
                &exact,
                exact_vectors[2].values.clone(),
                5,
                exact_vectors.len(),
                ConsistencyLevel::Strong,
                Some(filtered),
                &[FILTER_TAG],
            ),
            as_of: None,
        });

        let valid_one = Self::fixed_ann_query(
            &exact,
            exact_vectors[2].values.clone(),
            3,
            exact_vectors.len(),
            ConsistencyLevel::Strong,
            None,
            &[BATCH_TAG],
        );
        let valid_two = Self::fixed_ann_query(
            &exact,
            exact_vectors[3].values.clone(),
            2,
            exact_vectors.len(),
            ConsistencyLevel::Strong,
            None,
            &[BATCH_TAG],
        );
        let invalid = GeneratedQuery {
            body: json!({
                "sources": [{
                    "type": "ann",
                    "vector": vec![0.0f32; exact.spec.dims.saturating_sub(1).max(1)]
                }],
                "fusion": { "type": "none" },
                "top_k": 1,
                "consistency": "strong"
            }),
            class: QueryOracleClass::ExpectError {
                status: 400,
                code: "DIMENSION_MISMATCH".to_string(),
            },
            pattern_tags: vec![BATCH_TAG.to_string()],
        };
        self.pending.push_back(Op::BatchQuery {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            qs: vec![valid_one, invalid, valid_two],
        });
        self.pending.push_back(Op::PaginateAll {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            q: Self::fixed_ann_query(
                &exact,
                exact_vectors[2].values.clone(),
                6,
                exact_vectors.len(),
                ConsistencyLevel::Strong,
                None,
                &[PAGINATION_TAG],
            ),
            page_size: 2,
        });

        if let Some(fts) = self
            .namespaces
            .iter()
            .find(|namespace| !namespace.spec.fts_fields.is_empty())
            .cloned()
        {
            let q = self.fts_query(&fts, 5, &[FTS_TAG]);
            self.pending.push_back(Op::Query {
                actor: ActorSel::ADMIN,
                ns: fts.name.clone(),
                q,
                as_of: None,
            });
        }

        for probe in [
            InvalidProbe::NanVector,
            InvalidProbe::WrongDims,
            InvalidProbe::BadIdCharset,
            InvalidProbe::EmptyBatch,
            InvalidProbe::OversizedBatch,
            InvalidProbe::UnknownField,
            InvalidProbe::BadCursorToken,
            InvalidProbe::GroupingPlusCursor,
            InvalidProbe::WeightsLenMismatch,
        ] {
            self.pending.push_back(Op::InvalidProbe {
                actor: ActorSel::ADMIN,
                ns: exact.name.clone(),
                probe,
            });
        }
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
        if let Some(op) = self.maybe_start_phase3_script(model) {
            return op;
        }
        if let Some(op) = self.next_scenario_op(model) {
            return op;
        }
        self.weighted_op(model)
    }

    fn maybe_start_phase3_script(&mut self, model: &Model) -> Option<Op> {
        if self.phase3_started {
            return None;
        }
        let exact_index = self
            .namespaces
            .iter()
            .position(|namespace| namespace.spec.is_exact())?;
        let exact = self.namespaces[exact_index].clone();
        let ns_model = model.namespaces.get(&exact.name)?;
        if ns_model.live.is_empty() || ns_model.live_generation == 0 {
            return None;
        }

        self.phase3_started = true;
        let generation = ns_model.live_generation;
        let snapshot = format!("snap-{generation}");
        let clone_target = format!("{}-clone-{generation}", exact.name);
        let clone_ids = ns_model.live.keys().cloned().collect::<Vec<_>>();
        self.namespaces.push(GenNamespace {
            name: clone_target.clone(),
            spec: exact.spec.clone(),
            next_id: 0,
        });

        let q_generation = self.exhaustive_query(model, &exact.name, Some("as-of-200"));
        let q_snapshot = self.exhaustive_query(model, &exact.name, Some("as-of-200"));
        let mut q_zero = self.exhaustive_query(model, &exact.name, Some("as-of-410"));
        q_zero.class = QueryOracleClass::ExpectError {
            status: 410,
            code: "POINT_IN_TIME_NOT_RETAINED".to_string(),
        };
        let mut q_future = self.exhaustive_query(model, &exact.name, Some("as-of-410"));
        q_future.class = QueryOracleClass::ExpectError {
            status: 410,
            code: "POINT_IN_TIME_NOT_RETAINED".to_string(),
        };
        let q_clone = self.exhaustive_query(model, &exact.name, Some("clone-independence"));
        self.pending.push_back(Op::CreateSnapshot {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            name: snapshot.clone(),
        });
        self.pending.push_back(Op::GetSnapshot {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            name: snapshot.clone(),
        });
        self.pending.push_back(Op::ListSnapshots {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
        });
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            q: q_generation,
            as_of: Some(AsOfTarget::Generation(generation)),
        });
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            q: q_snapshot,
            as_of: Some(AsOfTarget::Snapshot(snapshot.clone())),
        });
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            q: q_zero,
            as_of: Some(AsOfTarget::Generation(0)),
        });
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            q: q_future,
            as_of: Some(AsOfTarget::Generation(generation + 10_000)),
        });
        self.pending.push_back(Op::GcCycle {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            keep_count: 4,
        });
        self.pending.push_back(Op::CompactEndpoint {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
        });
        self.pending.push_back(Op::PatchIndexConfig {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            patch: json!({ "nlist": exact.spec.num_centroids }),
        });
        self.pending.push_back(Op::Hydrate {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
        });
        self.pending.push_back(Op::CloneNamespace {
            actor: ActorSel::ADMIN,
            source: exact.name.clone(),
            target: clone_target.clone(),
            as_of: AsOfTarget::Generation(generation),
        });
        self.pending.push_back(Op::CompactEndpoint {
            actor: ActorSel::ADMIN,
            ns: clone_target.clone(),
        });
        self.pending.push_back(Op::FetchVectors {
            actor: ActorSel::ADMIN,
            ns: clone_target.clone(),
            ids: clone_ids,
            consistency: ConsistencyLevel::Strong,
        });
        self.pending.push_back(Op::Query {
            actor: ActorSel::ADMIN,
            ns: clone_target.clone(),
            q: q_clone,
            as_of: None,
        });
        self.pending.push_back(Op::ProbeSandwich {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            maintenance: MaintenanceKind::GcCycle,
        });
        self.pending.push_back(Op::DeleteSnapshot {
            actor: ActorSel::ADMIN,
            ns: exact.name.clone(),
            name: snapshot,
        });
        self.pending.push_back(Op::DeleteNamespace {
            actor: ActorSel::ADMIN,
            ns: clone_target.clone(),
        });
        self.pending.push_back(Op::CreateNamespace {
            actor: ActorSel::ADMIN,
            ns: clone_target,
            spec: exact.spec.clone(),
        });
        self.pending.pop_front()
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
            ConsistencyLevel::Strong,
            None,
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
                    actor: ActorSel::ADMIN,
                    ns,
                    ids: live_ids.into_iter().take(2).collect(),
                })
            }
            ScenarioState::FetchDeleted => {
                self.scenario = ScenarioState::QueryDeleted;
                let ids = model.namespaces[&ns].live.keys().cloned().collect();
                Some(Op::FetchVectors {
                    actor: ActorSel::ADMIN,
                    ns,
                    ids,
                    consistency: ConsistencyLevel::Strong,
                })
            }
            ScenarioState::QueryDeleted => {
                self.scenario = ScenarioState::Reupsert;
                let q = self.exhaustive_query(model, &ns, Some(DELETE_REUPSERT_TAG));
                Some(Op::Query {
                    actor: ActorSel::ADMIN,
                    ns,
                    q,
                    as_of: None,
                })
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
                        attributes: Some(self.random_attributes(
                            !self.namespaces[exact_index].spec.fts_fields.is_empty(),
                        )),
                    })
                    .collect();
                Some(Op::Upsert {
                    actor: ActorSel::ADMIN,
                    ns,
                    vectors,
                })
            }
            ScenarioState::QueryReupserted => {
                self.scenario = ScenarioState::Compact;
                let q = self.exhaustive_query(model, &ns, Some(DELETE_REUPSERT_TAG));
                Some(Op::Query {
                    actor: ActorSel::ADMIN,
                    ns,
                    q,
                    as_of: None,
                })
            }
            ScenarioState::Compact => {
                self.scenario = ScenarioState::QueryCompacted;
                Some(Op::CompactInline {
                    actor: ActorSel::ADMIN,
                    ns,
                })
            }
            ScenarioState::QueryCompacted => {
                self.scenario = ScenarioState::Done;
                let q = self.exhaustive_query(model, &ns, Some(DELETE_REUPSERT_TAG));
                Some(Op::Query {
                    actor: ActorSel::ADMIN,
                    ns,
                    q,
                    as_of: None,
                })
            }
            _ => None,
        }
    }

    fn weighted_op(&mut self, model: &Model) -> Op {
        let roll = self.rng.gen_range(0..100);
        if roll < 25 {
            self.random_upsert(model)
        } else if roll < 35 {
            self.random_delete(model)
        } else if roll < 50 {
            self.random_fetch(model)
        } else if roll < 74 {
            self.random_query(model)
        } else if roll < 82 {
            self.random_batch_query(model)
        } else if roll < 89 {
            self.random_paginate(model)
        } else if roll < 97 {
            self.random_compact(model)
        } else if roll < 99 {
            self.random_invalid_probe()
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
        Op::Upsert {
            actor: ActorSel::ADMIN,
            ns,
            vectors,
        }
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
        Op::DeleteVectors {
            actor: ActorSel::ADMIN,
            ns,
            ids,
        }
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
            actor: ActorSel::ADMIN,
            ns,
            ids,
            consistency: self.random_consistency(),
        }
    }

    fn random_query(&mut self, model: &Model) -> Op {
        let ns = self.random_namespace_name();
        let namespace = self.namespace(&ns).clone();
        if !namespace.spec.fts_fields.is_empty() && self.rng.gen_bool(0.35) {
            let top_k = self.rng.gen_range(1..=20);
            let q = self.fts_query(&namespace, top_k, &[FTS_TAG]);
            return Op::Query {
                actor: ActorSel::ADMIN,
                ns,
                q,
                as_of: None,
            };
        }
        let consistency = self.random_consistency();
        let (candidate_count, exact_allowed) = model
            .namespaces
            .get(&ns)
            .map(|ns_model| {
                let candidate_count = match consistency {
                    ConsistencyLevel::Strong => ns_model.live.len() + ns_model.wal_tombstones.len(),
                    ConsistencyLevel::Eventual => ns_model.compacted_live.len(),
                };
                let exact_allowed = match consistency {
                    ConsistencyLevel::Strong => ns_model.wal_tombstones.is_empty(),
                    ConsistencyLevel::Eventual => true,
                };
                (candidate_count, exact_allowed)
            })
            .unwrap_or((0, true));
        let top_k = self.rng.gen_range(1..=50);
        let filter = self.rng.gen_bool(0.25).then(|| self.random_filter(0));
        let q = {
            let tag = filter.as_ref().map(|_| FILTER_TAG);
            self.query_for(
                &namespace,
                top_k,
                candidate_count,
                exact_allowed,
                consistency,
                filter,
                tag,
            )
        };
        Op::Query {
            actor: ActorSel::ADMIN,
            ns,
            q,
            as_of: None,
        }
    }

    fn random_batch_query(&mut self, model: &Model) -> Op {
        let ns = self.random_namespace_name();
        let namespace = self.namespace(&ns).clone();
        let candidate_count = model
            .namespaces
            .get(&ns)
            .map(|ns_model| ns_model.live.len() + ns_model.wal_tombstones.len())
            .unwrap_or(1)
            .max(1);
        let count = self.rng.gen_range(2..=5);
        let mut qs = Vec::with_capacity(count);
        for index in 0..count {
            if index == 1 && self.rng.gen_bool(0.35) {
                qs.push(self.invalid_query_for_batch(&namespace));
                continue;
            }
            let top_k = self.rng.gen_range(1..=10);
            qs.push(
                self.query_for(
                    &namespace,
                    top_k,
                    candidate_count,
                    namespace.spec.is_exact()
                        && model
                            .namespaces
                            .get(&ns)
                            .is_none_or(|ns_model| ns_model.wal_tombstones.is_empty()),
                    ConsistencyLevel::Strong,
                    None,
                    Some(BATCH_TAG),
                ),
            );
        }
        Op::BatchQuery {
            actor: ActorSel::ADMIN,
            ns,
            qs,
        }
    }

    fn random_paginate(&mut self, model: &Model) -> Op {
        let ns = self.random_namespace_name();
        let namespace = self.namespace(&ns).clone();
        let candidate_count = model
            .namespaces
            .get(&ns)
            .map(|ns_model| ns_model.live.len() + ns_model.wal_tombstones.len())
            .unwrap_or(1)
            .max(1);
        let total = candidate_count.clamp(1, 25);
        let q = self.query_for(
            &namespace,
            total,
            total,
            false,
            ConsistencyLevel::Strong,
            None,
            Some(PAGINATION_TAG),
        );
        Op::PaginateAll {
            actor: ActorSel::ADMIN,
            ns,
            q,
            page_size: self.rng.gen_range(1..=5).min(total),
        }
    }

    fn random_invalid_probe(&mut self) -> Op {
        let ns = self.random_namespace_name();
        let probe = *[
            InvalidProbe::NanVector,
            InvalidProbe::WrongDims,
            InvalidProbe::BadIdCharset,
            InvalidProbe::EmptyBatch,
            InvalidProbe::OversizedBatch,
            InvalidProbe::UnknownField,
            InvalidProbe::BadCursorToken,
            InvalidProbe::GroupingPlusCursor,
            InvalidProbe::WeightsLenMismatch,
        ]
        .choose(&mut self.rng)
        .unwrap();
        Op::InvalidProbe {
            actor: ActorSel::ADMIN,
            ns,
            probe,
        }
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
            actor: ActorSel::ADMIN,
            ns: candidates
                .choose(&mut self.rng)
                .cloned()
                .unwrap_or_else(|| self.random_namespace_name()),
        }
    }

    fn random_get_namespace(&mut self) -> Op {
        Op::GetNamespace {
            actor: ActorSel::ADMIN,
            ns: self.random_namespace_name(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn query_for(
        &mut self,
        namespace: &GenNamespace,
        top_k: usize,
        candidate_count: usize,
        exact_allowed: bool,
        consistency: ConsistencyLevel,
        filter: Option<Filter>,
        tag: Option<&str>,
    ) -> GeneratedQuery {
        let mut body = json!({
            "sources": [{
                "type": "ann",
                "vector": self.random_values(namespace.spec.dims),
                "nprobe": namespace.spec.num_centroids
            }],
            "fusion": { "type": "none" },
            "top_k": top_k,
            "candidate_k": candidate_count.max(1),
            "consistency": consistency,
            "include_attributes": true
        });
        if let Some(filter) = filter.as_ref() {
            body.as_object_mut()
                .expect("query body is object")
                .insert("filter".to_string(), serde_json::to_value(filter).unwrap());
        }
        let mut tags = tag.into_iter().map(str::to_string).collect::<Vec<_>>();
        if consistency == ConsistencyLevel::Eventual {
            tags.push("eventual".to_string());
        }
        if filter.is_some() {
            tags.push(FILTER_TAG.to_string());
        }
        let class = if namespace.spec.is_exact()
            && !namespace.name.ends_with("-sketch")
            && exact_allowed
            && consistency == ConsistencyLevel::Strong
        {
            QueryOracleClass::ExactAnn {
                top_k,
                consistency,
                filter,
            }
        } else {
            QueryOracleClass::Membership { consistency }
        };
        GeneratedQuery {
            body,
            class,
            pattern_tags: tags,
        }
    }

    fn fixed_ann_query(
        namespace: &GenNamespace,
        vector: Vec<f32>,
        top_k: usize,
        candidate_count: usize,
        consistency: ConsistencyLevel,
        filter: Option<Filter>,
        tags: &[&str],
    ) -> GeneratedQuery {
        let mut body = json!({
            "sources": [{
                "type": "ann",
                "vector": vector,
                "nprobe": namespace.spec.num_centroids
            }],
            "fusion": { "type": "none" },
            "top_k": top_k,
            "candidate_k": candidate_count.max(1),
            "consistency": consistency,
            "include_attributes": true
        });
        if let Some(filter) = filter.as_ref() {
            body.as_object_mut()
                .expect("query body is object")
                .insert("filter".to_string(), serde_json::to_value(filter).unwrap());
        }
        let mut pattern_tags = tags
            .iter()
            .map(|tag| (*tag).to_string())
            .collect::<Vec<_>>();
        if consistency == ConsistencyLevel::Eventual {
            pattern_tags.push("eventual".to_string());
        }
        if filter.is_some() {
            pattern_tags.push(FILTER_TAG.to_string());
        }
        GeneratedQuery {
            body,
            class: if consistency == ConsistencyLevel::Strong {
                QueryOracleClass::ExactAnn {
                    top_k,
                    consistency,
                    filter,
                }
            } else {
                QueryOracleClass::Membership { consistency }
            },
            pattern_tags,
        }
    }

    fn fixed_membership_query(
        namespace: &GenNamespace,
        vector: Vec<f32>,
        top_k: usize,
        candidate_count: usize,
        tags: &[&str],
    ) -> GeneratedQuery {
        GeneratedQuery {
            body: json!({
                "sources": [{
                    "type": "ann",
                    "vector": vector,
                    "nprobe": namespace.spec.num_centroids
                }],
                "fusion": { "type": "none" },
                "top_k": top_k,
                "candidate_k": candidate_count.max(1),
                "consistency": ConsistencyLevel::Strong,
                "include_attributes": true
            }),
            class: QueryOracleClass::Membership {
                consistency: ConsistencyLevel::Strong,
            },
            pattern_tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
        }
    }

    fn fts_query(
        &mut self,
        namespace: &GenNamespace,
        top_k: usize,
        tags: &[&str],
    ) -> GeneratedQuery {
        let field = namespace.spec.fts_fields[0].clone();
        let query_words = self.query_words();
        let rank_by = match self.rng.gen_range(0..4) {
            0 => json!([field, "BM25", query_words]),
            1 => json!(["Sum", [[field, "BM25", query_words]]]),
            2 => json!(["Max", [[field, "BM25", query_words]]]),
            _ => json!(["Product", 1.5, [field, "BM25", query_words]]),
        };
        let mode = self.rng.gen_range(0..4);
        let body = match mode {
            0 => json!({
                "rank_by": rank_by,
                "top_k": top_k,
                "consistency": "strong",
                "include_attributes": true
            }),
            1 => json!({
                "sources": [{
                    "type": "bm25",
                    "rank_by": rank_by
                }],
                "fusion": { "type": "none" },
                "top_k": top_k,
                "candidate_k": top_k.max(10),
                "consistency": "strong",
                "include_attributes": true,
                "facets": ["group", "bucket", "flag"],
                "debug": true,
                "explain": "plan"
            }),
            2 => json!({
                "sources": [{
                    "type": "ann",
                    "vector": self.random_values(namespace.spec.dims),
                    "nprobe": namespace.spec.num_centroids
                }, {
                    "type": "bm25",
                    "rank_by": rank_by
                }],
                "fusion": { "type": "rrf", "k": 60 },
                "rerank": { "type": "default" },
                "top_k": top_k,
                "candidate_k": top_k.max(10),
                "consistency": "strong",
                "include_attributes": true,
                "explain": "full"
            }),
            _ => json!({
                "sources": [{
                    "type": "ann",
                    "vector": self.random_values(namespace.spec.dims),
                    "nprobe": namespace.spec.num_centroids
                }, {
                    "type": "bm25",
                    "rank_by": rank_by
                }],
                "fusion": { "type": "weighted", "weights": [0.25, 0.75] },
                "top_k": top_k,
                "candidate_k": top_k.max(10),
                "consistency": "strong",
                "include_attributes": true,
                "grouping": { "type": "field", "field": "group", "max_per_group": 2 },
                "debug": true
            }),
        };
        let mut pattern_tags = tags
            .iter()
            .map(|tag| (*tag).to_string())
            .collect::<Vec<_>>();
        pattern_tags.push(FTS_TAG.to_string());
        GeneratedQuery {
            body,
            class: QueryOracleClass::Membership {
                consistency: ConsistencyLevel::Strong,
            },
            pattern_tags,
        }
    }

    fn invalid_query_for_batch(&self, namespace: &GenNamespace) -> GeneratedQuery {
        GeneratedQuery {
            body: json!({
                "sources": [{
                    "type": "ann",
                    "vector": vec![0.0f32; namespace.spec.dims + 1]
                }],
                "fusion": { "type": "none" },
                "top_k": 1,
                "consistency": "strong"
            }),
            class: QueryOracleClass::ExpectError {
                status: 400,
                code: "DIMENSION_MISMATCH".to_string(),
            },
            pattern_tags: vec![BATCH_TAG.to_string()],
        }
    }

    fn make_vectors(&mut self, namespace_index: usize, count: usize) -> Vec<GenVector> {
        let dims = self.namespaces[namespace_index].spec.dims;
        let ns_name = self.namespaces[namespace_index].name.clone();
        let include_body = !self.namespaces[namespace_index].spec.fts_fields.is_empty();
        (0..count)
            .map(|_| {
                let id = format!("{ns_name}-v{}", self.namespaces[namespace_index].next_id);
                self.namespaces[namespace_index].next_id += 1;
                GenVector {
                    id,
                    values: self.random_values(dims),
                    attributes: (!self.rng.gen_bool(0.12))
                        .then(|| self.random_attributes(include_body)),
                }
            })
            .collect()
    }

    fn random_values(&mut self, dims: usize) -> Vec<f32> {
        (0..dims)
            .map(|_| self.rng.gen_range(-10.0f32..10.0f32))
            .collect()
    }

    fn random_attributes(&mut self, include_body: bool) -> HashMap<String, AttributeValue> {
        let mut attributes = HashMap::new();
        if self.rng.gen_bool(0.85) {
            attributes.insert(
                "group".to_string(),
                AttributeValue::String(format!("g{}", self.rng.gen_range(0..4))),
            );
        }
        if self.rng.gen_bool(0.85) {
            attributes.insert(
                "bucket".to_string(),
                AttributeValue::Integer(self.rng.gen_range(0..16)),
            );
        }
        if self.rng.gen_bool(0.8) {
            attributes.insert(
                "score".to_string(),
                AttributeValue::Float(self.rng.gen_range(0.0..100.0)),
            );
        }
        if self.rng.gen_bool(0.8) {
            attributes.insert(
                "flag".to_string(),
                AttributeValue::Bool(self.rng.gen_bool(0.5)),
            );
        }
        if self.rng.gen_bool(0.75) {
            attributes.insert(
                "tags".to_string(),
                AttributeValue::StringList(vec![
                    format!("t{}", self.rng.gen_range(0..5)),
                    format!("t{}", self.rng.gen_range(0..5)),
                ]),
            );
        }
        if self.rng.gen_bool(0.65) {
            attributes.insert(
                "nums".to_string(),
                AttributeValue::IntegerList(vec![
                    self.rng.gen_range(0..10),
                    self.rng.gen_range(10..20),
                ]),
            );
        }
        if self.rng.gen_bool(0.65) {
            attributes.insert(
                "ratios".to_string(),
                AttributeValue::FloatList(vec![
                    self.rng.gen_range(0.0..1.0),
                    self.rng.gen_range(1.0..2.0),
                ]),
            );
        }
        if include_body {
            attributes.insert("body".to_string(), AttributeValue::String(self.sentence()));
        }
        attributes
    }

    fn random_consistency(&mut self) -> ConsistencyLevel {
        if self.rng.gen_bool(0.25) {
            ConsistencyLevel::Eventual
        } else {
            ConsistencyLevel::Strong
        }
    }

    fn random_filter(&mut self, depth: usize) -> Filter {
        let leaf_roll = if depth >= 2 {
            self.rng.gen_range(0..8)
        } else {
            self.rng.gen_range(0..11)
        };
        match leaf_roll {
            0 => Filter::Eq {
                field: "group".to_string(),
                value: AttributeValue::String(format!("g{}", self.rng.gen_range(0..4))),
            },
            1 => Filter::NotEq {
                field: "flag".to_string(),
                value: AttributeValue::Bool(self.rng.gen_bool(0.5)),
            },
            2 => {
                let low = self.rng.gen_range(0.0..8.0);
                Filter::Range {
                    field: if self.rng.gen_bool(0.5) {
                        "bucket".to_string()
                    } else {
                        "score".to_string()
                    },
                    gte: Some(low),
                    lte: Some(low + self.rng.gen_range(1.0..8.0)),
                    gt: None,
                    lt: None,
                }
            }
            3 => Filter::In {
                field: "group".to_string(),
                values: vec![
                    AttributeValue::String("g0".to_string()),
                    AttributeValue::String("g2".to_string()),
                ],
            },
            4 => Filter::NotIn {
                field: "bucket".to_string(),
                values: vec![AttributeValue::Integer(1), AttributeValue::Integer(3)],
            },
            5 => Filter::Contains {
                field: "tags".to_string(),
                value: AttributeValue::String(format!("t{}", self.rng.gen_range(0..5))),
            },
            6 => Filter::ContainsAllTokens {
                field: "body".to_string(),
                tokens: vec![self.word().to_string()],
            },
            7 => Filter::ContainsTokenSequence {
                field: "body".to_string(),
                tokens: vec![self.word().to_string(), self.word().to_string()],
            },
            8 => Filter::And {
                filters: vec![self.random_filter(depth + 1), self.random_filter(depth + 1)],
            },
            9 => Filter::Or {
                filters: vec![self.random_filter(depth + 1), self.random_filter(depth + 1)],
            },
            _ => Filter::Not {
                filter: Box::new(self.random_filter(depth + 1)),
            },
        }
    }

    fn sentence(&mut self) -> String {
        let count = self.rng.gen_range(3..=10);
        (0..count)
            .map(|_| self.word().to_string())
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn query_words(&mut self) -> String {
        let count = self.rng.gen_range(1..=3);
        (0..count)
            .map(|_| self.word().to_string())
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn word(&mut self) -> &'static str {
        vocab::words().choose(&mut self.rng).copied().unwrap()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_queue_covers_v4_sketch_adc_and_incremental_compaction() {
        let generator = AdversarialGenerator::new(7, "test");
        let sketch = generator
            .namespaces
            .iter()
            .find(|namespace| namespace.name.ends_with("-sketch"))
            .expect("generator must include the fixed sketch namespace");

        assert_eq!(sketch.spec.dims, 8);
        assert_eq!(sketch.spec.metric, DistanceMetric::Euclidean);
        assert_eq!(sketch.spec.quantization, QuantizationType::None);
        assert_eq!(sketch.spec.num_centroids, 16);
        assert!(sketch.spec.fts_fields.is_empty());
        assert!(!sketch.spec.bitmap);

        let ops = generator
            .pending
            .iter()
            .filter(|op| op.namespace() == sketch.name)
            .collect::<Vec<_>>();
        assert_eq!(
            ops.iter()
                .filter(|op| matches!(op, Op::CompactInline { .. }))
                .count(),
            2
        );
        let tagged_queries = ops
            .iter()
            .filter_map(|op| match op {
                Op::Query { q, .. } if q.pattern_tags.iter().any(|tag| tag == "sketch-adc-v4") => {
                    Some(q)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(tagged_queries.len(), 2);
        for query in tagged_queries {
            assert_eq!(query.body["sources"][0]["nprobe"], 16);
            assert!(matches!(
                query.class,
                QueryOracleClass::Membership {
                    consistency: ConsistencyLevel::Strong
                }
            ));
        }
    }
}
