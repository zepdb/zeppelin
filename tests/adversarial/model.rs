use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use zeppelin::types::{AttributeValue, DistanceMetric};

use super::ops::{AsOfTarget, GenVector, GeneratedQuery, MaintenanceKind, NamespaceSpec, Op};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AmbiguityReason {
    HttpTimeout,
    ConnectionError,
    JsonParse,
    ServerError { status: u16 },
    ServerCrashed,
    HeldInFlight,
}

impl AmbiguityReason {
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::HttpTimeout => "http_timeout".to_string(),
            Self::ConnectionError => "connection_error".to_string(),
            Self::JsonParse => "json_parse".to_string(),
            Self::ServerError { status } => format!("server_error_{status}"),
            Self::ServerCrashed => "server_crashed".to_string(),
            Self::HeldInFlight => "held_in_flight".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpOutcome {
    Applied {
        status: u16,
        response: serde_json::Value,
    },
    NotApplied {
        status: u16,
        response: serde_json::Value,
    },
    Ambiguous {
        reason: AmbiguityReason,
        status: Option<u16>,
    },
}

impl OpOutcome {
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Applied { .. } => "applied".to_string(),
            Self::NotApplied { .. } => "not_applied".to_string(),
            Self::Ambiguous { reason, .. } => format!("ambiguous:{}", reason.label()),
        }
    }
}

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
    ChaosLostWrite,
    PostCommitLostWrite,
    IndetResolutionLie,
    DroppedResponseLostWrite,
    CrashLostAck,
    ClockGcEatsLive,
    SwallowCorruption,
    MisdirectedWriteReachability,
    /// Admit one stale-token writer result so lineage must report I21.
    DualWriterFencing,
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
            "chaos-lost-write" => Self::ChaosLostWrite,
            "post-commit-lost-write" => Self::PostCommitLostWrite,
            "indet-resolution-lie" => Self::IndetResolutionLie,
            "dropped-response-lost-write" => Self::DroppedResponseLostWrite,
            "crash-lost-ack" => Self::CrashLostAck,
            "clock-gc-eats-live" => Self::ClockGcEatsLive,
            "swallow-corruption" => Self::SwallowCorruption,
            "misdirected-write-reachability" => Self::MisdirectedWriteReachability,
            "dual-writer-fencing" => Self::DualWriterFencing,
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
            Self::ChaosLostWrite => "chaos-lost-write",
            Self::PostCommitLostWrite => "post-commit-lost-write",
            Self::IndetResolutionLie => "indet-resolution-lie",
            Self::DroppedResponseLostWrite => "dropped-response-lost-write",
            Self::CrashLostAck => "crash-lost-ack",
            Self::ClockGcEatsLive => "clock-gc-eats-live",
            Self::SwallowCorruption => "swallow-corruption",
            Self::MisdirectedWriteReachability => "misdirected-write-reachability",
            Self::DualWriterFencing => "dual-writer-fencing",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Model {
    pub namespaces: BTreeMap<String, NsModel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelRecord {
    pub values: Vec<f32>,
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

impl ModelRecord {
    #[must_use]
    pub fn semantically_eq(&self, other: &Self) -> bool {
        self.values == other.values && model_attributes_equal(&self.attributes, &other.attributes)
    }
}

fn model_attributes_equal(
    left: &Option<HashMap<String, AttributeValue>>,
    right: &Option<HashMap<String, AttributeValue>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) if left.len() == right.len() => {
            left.iter().all(|(key, left_value)| {
                right.get(key).is_some_and(|right_value| {
                    model_attribute_values_equal(left_value, right_value)
                })
            })
        }
        _ => false,
    }
}

fn model_attribute_values_equal(left: &AttributeValue, right: &AttributeValue) -> bool {
    match (left, right) {
        (AttributeValue::String(left), AttributeValue::String(right)) => left == right,
        (AttributeValue::Integer(left), AttributeValue::Integer(right)) => left == right,
        (AttributeValue::Float(left), AttributeValue::Float(right)) => (left - right).abs() <= 1e-9,
        (AttributeValue::Bool(left), AttributeValue::Bool(right)) => left == right,
        (AttributeValue::StringList(left), AttributeValue::StringList(right)) => left == right,
        (AttributeValue::IntegerList(left), AttributeValue::IntegerList(right)) => left == right,
        (AttributeValue::FloatList(left), AttributeValue::FloatList(right))
            if left.len() == right.len() =>
        {
            left.iter()
                .zip(right.iter())
                .all(|(left, right)| (left - right).abs() <= 1e-9)
        }
        _ => false,
    }
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
    pub indeterminate_ns: Vec<NsIndeterminate>,
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
    pub op_index: u64,
    pub reason: String,
    pub effect: IndetEffect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndetEffect {
    MaybeUpserted(ModelRecord),
    MaybeDeleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub enum NsIndeterminate {
    MaybeCreatedNs,
    MaybeSnapshot { name: String },
    MaybeSnapshotDeleted { name: String },
    MaybeCloned { target: String, as_of: AsOfTarget },
    MaybeDeletedNs,
    MaybeCompacted,
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
        self.apply_with_generation_checkpoints(op, status, gen_after, response, mutation, true);
    }

    fn apply_with_generation_checkpoints(
        &mut self,
        op: &Op,
        status: u16,
        gen_after: Option<u64>,
        response: &serde_json::Value,
        mutation: Option<OracleMutation>,
        generation_checkpoints: bool,
    ) {
        if !(200..300).contains(&status) {
            return;
        }

        match op {
            Op::CreateNamespace { ns, spec } => {
                if status == 201 {
                    // A fresh named create proves that an earlier ambiguous
                    // clone did not leave this target visible. An idempotent
                    // create returns 200 and must preserve the clone candidate.
                    for model in self.namespaces.values_mut() {
                        model.indeterminate_ns.retain(|entry| {
                            !matches!(
                                entry,
                                NsIndeterminate::MaybeCloned { target, .. } if target == ns
                            )
                        });
                    }
                }
                let generation = gen_after.unwrap_or(0);
                self.namespaces
                    .entry(ns.clone())
                    .or_insert_with(|| NsModel::new(spec.clone(), generation));
                if let Some(model) = self.namespaces.get_mut(ns) {
                    model
                        .indeterminate_ns
                        .retain(|entry| !matches!(entry, NsIndeterminate::MaybeCreatedNs));
                }
            }
            Op::Upsert { ns, vectors } => {
                let Some(model) = self.namespaces.get_mut(ns) else {
                    panic!("upsert acked for unknown namespace {ns}");
                };
                for vector in vectors {
                    model.indeterminate.remove(&vector.id);
                    model
                        .live
                        .insert(vector.id.clone(), ModelRecord::from(vector));
                    model.deleted_ever.remove(&vector.id);
                }
                if mutation == Some(OracleMutation::PhantomId) {
                    model.insert_phantom_once();
                }
                model.checkpoint_if_enabled(gen_after, generation_checkpoints);
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
                        model.indeterminate.remove(id);
                        model.live.remove(id);
                        if mutation != Some(OracleMutation::LeakTombstone) || idx > 0 {
                            model.wal_tombstones.insert(id.clone());
                        }
                        model.deleted_ever.insert(id.clone());
                    }
                }
                model.checkpoint_if_enabled(gen_after, generation_checkpoints);
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
                model
                    .indeterminate_ns
                    .retain(|entry| !matches!(entry, NsIndeterminate::MaybeCompacted));
                model.checkpoint_if_enabled(gen_after, generation_checkpoints);
                if mutation == Some(OracleMutation::StaleCheckpoint) {
                    model.corrupt_latest_checkpoint();
                }
            }
            Op::CompactEndpoint { ns }
            | Op::ProbeSandwich {
                ns,
                maintenance: MaintenanceKind::CompactInline | MaintenanceKind::CompactEndpoint,
            } => {
                let Some(model) = self.namespaces.get_mut(ns) else {
                    panic!("maintenance acked for unknown namespace {ns}");
                };
                model.compacted_live = model.live.clone();
                model.wal_tombstones.clear();
                model
                    .indeterminate_ns
                    .retain(|entry| !matches!(entry, NsIndeterminate::MaybeCompacted));
                model.checkpoint_if_enabled(gen_after, generation_checkpoints);
                if mutation == Some(OracleMutation::StaleCheckpoint) {
                    model.corrupt_latest_checkpoint();
                }
            }
            Op::ProbeSandwich { ns, .. } => {
                let Some(model) = self.namespaces.get_mut(ns) else {
                    panic!("maintenance acked for unknown namespace {ns}");
                };
                model.checkpoint_if_enabled(gen_after, generation_checkpoints);
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
                model.indeterminate_ns.retain(|entry| {
                    !matches!(entry, NsIndeterminate::MaybeSnapshot { name: pending } if pending == name)
                });
            }
            Op::DeleteSnapshot { ns, name } => {
                if let Some(model) = self.namespaces.get_mut(ns) {
                    model.snapshots.remove(name);
                    model.indeterminate_ns.retain(|entry| {
                        !matches!(entry, NsIndeterminate::MaybeSnapshotDeleted { name: pending } if pending == name)
                    });
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
                if let Some(source_model) = self.namespaces.get_mut(source) {
                    source_model.indeterminate_ns.retain(|entry| {
                        !matches!(entry, NsIndeterminate::MaybeCloned { target: pending, .. } if pending == target)
                    });
                }
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

    pub fn apply_outcome(
        &mut self,
        op: &Op,
        outcome: &OpOutcome,
        gen_after: Option<u64>,
        mutation: Option<OracleMutation>,
        op_index: u64,
    ) {
        self.apply_outcome_with_generation_checkpoints(
            op, outcome, gen_after, mutation, op_index, true,
        );
    }

    pub fn apply_outcome_with_generation_checkpoints(
        &mut self,
        op: &Op,
        outcome: &OpOutcome,
        gen_after: Option<u64>,
        mutation: Option<OracleMutation>,
        op_index: u64,
        generation_checkpoints: bool,
    ) {
        match outcome {
            OpOutcome::Applied { status, response } => {
                self.apply_with_generation_checkpoints(
                    op,
                    *status,
                    gen_after,
                    response,
                    mutation,
                    generation_checkpoints,
                );
            }
            OpOutcome::NotApplied { .. } => {}
            OpOutcome::Ambiguous { reason, .. } => {
                if mutation == Some(OracleMutation::CrashLostAck) {
                    self.apply_with_generation_checkpoints(
                        op,
                        200,
                        gen_after,
                        &serde_json::Value::Null,
                        mutation,
                        generation_checkpoints,
                    );
                } else if mutation != Some(OracleMutation::PostCommitLostWrite) {
                    self.record_indeterminate(op, op_index, reason);
                    if mutation == Some(OracleMutation::IndetResolutionLie) {
                        self.corrupt_indeterminate_candidate(op);
                    }
                }
            }
        }
    }

    /// Replaces a provisional held-operation ambiguity with its joined result.
    pub fn apply_joined_outcome(
        &mut self,
        op: &Op,
        outcome: &OpOutcome,
        gen_after: Option<u64>,
        mutation: Option<OracleMutation>,
        op_index: u64,
    ) {
        self.apply_joined_outcome_with_generation_checkpoints(
            op, outcome, gen_after, mutation, op_index, true,
        );
    }

    pub fn apply_joined_outcome_with_generation_checkpoints(
        &mut self,
        op: &Op,
        outcome: &OpOutcome,
        gen_after: Option<u64>,
        mutation: Option<OracleMutation>,
        op_index: u64,
        generation_checkpoints: bool,
    ) {
        self.clear_held_vector_effects(op, op_index);
        self.apply_outcome_with_generation_checkpoints(
            op,
            outcome,
            gen_after,
            mutation,
            op_index,
            generation_checkpoints,
        );
    }

    fn clear_held_vector_effects(&mut self, op: &Op, op_index: u64) {
        self.clear_held_namespace_effect(op);
        let (ns, ids): (&str, Vec<&str>) = match op {
            Op::Upsert { ns, vectors } => (
                ns,
                vectors.iter().map(|vector| vector.id.as_str()).collect(),
            ),
            Op::DeleteVectors { ns, ids } => (ns, ids.iter().map(String::as_str).collect()),
            _ => return,
        };
        let Some(model) = self.namespaces.get_mut(ns) else {
            return;
        };
        for id in ids {
            let held = model.indeterminate.get(id).is_some_and(|pending| {
                pending.op_index == op_index && pending.reason == "held_in_flight"
            });
            if held {
                model.indeterminate.remove(id);
            }
        }
    }

    fn clear_held_namespace_effect(&mut self, op: &Op) {
        let (namespace, matches_effect): (&str, fn(&NsIndeterminate) -> bool) = match op {
            Op::CreateSnapshot { ns, name } => {
                let name = name.clone();
                if let Some(model) = self.namespaces.get_mut(ns) {
                    model.indeterminate_ns.retain(|effect| {
                        !matches!(effect, NsIndeterminate::MaybeSnapshot { name: pending } if pending == &name)
                    });
                }
                return;
            }
            Op::DeleteSnapshot { ns, name } => {
                let name = name.clone();
                if let Some(model) = self.namespaces.get_mut(ns) {
                    model.indeterminate_ns.retain(|effect| {
                        !matches!(effect, NsIndeterminate::MaybeSnapshotDeleted { name: pending } if pending == &name)
                    });
                }
                return;
            }
            Op::CloneNamespace { source, target, .. } => {
                let target = target.clone();
                if let Some(model) = self.namespaces.get_mut(source) {
                    model.indeterminate_ns.retain(|effect| {
                        !matches!(effect, NsIndeterminate::MaybeCloned { target: pending, .. } if pending == &target)
                    });
                }
                return;
            }
            Op::DeleteNamespace { ns } => (ns, |effect| {
                matches!(effect, NsIndeterminate::MaybeDeletedNs)
            }),
            Op::CompactInline { ns }
            | Op::CompactEndpoint { ns }
            | Op::ProbeSandwich { ns, .. } => (ns, |effect| {
                matches!(effect, NsIndeterminate::MaybeCompacted)
            }),
            _ => return,
        };
        if let Some(model) = self.namespaces.get_mut(namespace) {
            model
                .indeterminate_ns
                .retain(|effect| !matches_effect(effect));
        }
    }

    fn corrupt_indeterminate_candidate(&mut self, op: &Op) {
        let Op::Upsert { ns, vectors } = op else {
            return;
        };
        let Some(model) = self.namespaces.get_mut(ns) else {
            return;
        };
        for vector in vectors {
            let Some(IndeterminateWrite {
                effect: IndetEffect::MaybeUpserted(candidate),
                ..
            }) = model.indeterminate.get_mut(&vector.id)
            else {
                continue;
            };
            if let Some(value) = candidate.values.first_mut() {
                *value += 10_000.0;
            }
        }
    }

    fn record_indeterminate(&mut self, op: &Op, op_index: u64, reason: &AmbiguityReason) {
        let reason = reason.label();
        match op {
            Op::CreateNamespace { ns, spec } => {
                let model = self
                    .namespaces
                    .entry(ns.clone())
                    .or_insert_with(|| NsModel::new(spec.clone(), 0));
                model.indeterminate_ns.push(NsIndeterminate::MaybeCreatedNs);
            }
            Op::Upsert { ns, vectors } => {
                let Some(model) = self.namespaces.get_mut(ns) else {
                    return;
                };
                for vector in vectors {
                    model.indeterminate.insert(
                        vector.id.clone(),
                        IndeterminateWrite {
                            op_index,
                            reason: reason.clone(),
                            effect: IndetEffect::MaybeUpserted(ModelRecord::from(vector)),
                        },
                    );
                }
            }
            Op::DeleteVectors { ns, ids } => {
                let Some(model) = self.namespaces.get_mut(ns) else {
                    return;
                };
                for id in ids {
                    model.indeterminate.insert(
                        id.clone(),
                        IndeterminateWrite {
                            op_index,
                            reason: reason.clone(),
                            effect: IndetEffect::MaybeDeleted,
                        },
                    );
                }
            }
            Op::CreateSnapshot { ns, name } => {
                if let Some(model) = self.namespaces.get_mut(ns) {
                    model
                        .indeterminate_ns
                        .push(NsIndeterminate::MaybeSnapshot { name: name.clone() });
                }
            }
            Op::DeleteSnapshot { ns, name } => {
                if let Some(model) = self.namespaces.get_mut(ns) {
                    model
                        .indeterminate_ns
                        .push(NsIndeterminate::MaybeSnapshotDeleted { name: name.clone() });
                }
            }
            Op::CloneNamespace {
                source,
                target,
                as_of,
            } => {
                if let Some(model) = self.namespaces.get_mut(source) {
                    model.indeterminate_ns.push(NsIndeterminate::MaybeCloned {
                        target: target.clone(),
                        as_of: as_of.clone(),
                    });
                }
            }
            Op::DeleteNamespace { ns } => {
                if let Some(model) = self.namespaces.get_mut(ns) {
                    model.indeterminate_ns.push(NsIndeterminate::MaybeDeletedNs);
                }
            }
            Op::CompactInline { ns }
            | Op::CompactEndpoint { ns }
            | Op::GcCycle { ns, .. }
            | Op::ProbeSandwich { ns, .. } => {
                if let Some(model) = self.namespaces.get_mut(ns) {
                    model.indeterminate_ns.push(NsIndeterminate::MaybeCompacted);
                }
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

    pub fn resolve_indeterminate_record(
        &mut self,
        ns: &str,
        id: &str,
        observed: Option<ModelRecord>,
    ) -> Result<(), String> {
        let Some(model) = self.namespaces.get_mut(ns) else {
            return Err(format!("indeterminate namespace disappeared: {ns}"));
        };
        let Some(pending) = model.indeterminate.remove(id) else {
            return Ok(());
        };
        let old = model.live.get(id).cloned();
        match pending.effect {
            IndetEffect::MaybeUpserted(candidate) => {
                if observed
                    .as_ref()
                    .is_some_and(|observed| observed.semantically_eq(&candidate))
                {
                    model.live.insert(id.to_string(), candidate);
                    model.deleted_ever.remove(id);
                    Ok(())
                } else if records_semantically_equal(observed.as_ref(), old.as_ref()) {
                    Ok(())
                } else {
                    Err(format!(
                        "observed state matched neither old nor new for {ns}/{id}"
                    ))
                }
            }
            IndetEffect::MaybeDeleted => {
                if observed.is_none() {
                    model.live.remove(id);
                    model.wal_tombstones.insert(id.to_string());
                    model.deleted_ever.insert(id.to_string());
                    Ok(())
                } else if records_semantically_equal(observed.as_ref(), old.as_ref()) {
                    Ok(())
                } else {
                    Err(format!(
                        "observed state matched neither old nor deleted for {ns}/{id}"
                    ))
                }
            }
        }
    }
}

fn records_semantically_equal(left: Option<&ModelRecord>, right: Option<&ModelRecord>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.semantically_eq(right),
        _ => false,
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
            indeterminate_ns: Vec::new(),
            canonical_queries: Vec::new(),
            deleted_ever: BTreeSet::new(),
        }
    }

    pub fn checkpoint(&mut self, gen_after: Option<u64>) {
        let generation = gen_after.unwrap_or(self.live_generation);
        self.live_generation = generation;
        self.checkpoints.insert(generation, self.live.clone());
    }

    fn checkpoint_if_enabled(&mut self, gen_after: Option<u64>, enabled: bool) {
        if enabled {
            self.checkpoint(gen_after);
        }
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
    use zeppelin::types::{AttributeValue, DistanceMetric};

    use crate::adversarial::oracle::{score_close, SCORE_ABS_EPS, SCORE_REL_EPS};
    use crate::common::server::{cleanup_ns, start_test_server_with_compactor};

    use crate::adversarial::ops::{AsOfTarget, GenVector, NamespaceSpec, Op};

    use super::{
        model_distance, AmbiguityReason, Model, ModelRecord, NsModel, OpOutcome, OracleMutation,
    };

    #[test]
    fn held_in_flight_ambiguity_has_stable_label_and_wire_key() {
        let reason = AmbiguityReason::HeldInFlight;

        assert_eq!(reason.label(), "held_in_flight");
        assert_eq!(
            serde_json::to_value(&reason).unwrap(),
            json!("held_in_flight")
        );
        assert_eq!(
            serde_json::from_value::<AmbiguityReason>(json!("held_in_flight")).unwrap(),
            reason
        );
    }

    #[test]
    fn dual_writer_fencing_mutation_key_round_trips() {
        let mutation = OracleMutation::from_key("dual-writer-fencing");

        assert_eq!(mutation, OracleMutation::DualWriterFencing);
        assert_eq!(mutation.key(), "dual-writer-fencing");
    }

    #[test]
    fn generation_checkpoints_pause_only_during_second_node_window() {
        let (mut model, ns) = model_with_old_record();
        let active_window_upsert = Op::Upsert {
            ns: ns.clone(),
            vectors: vec![GenVector {
                id: "during-window".to_string(),
                values: vec![0.0, 1.0],
                attributes: None,
            }],
        };
        model.apply_outcome_with_generation_checkpoints(
            &active_window_upsert,
            &OpOutcome::Applied {
                status: 200,
                response: json!({}),
            },
            Some(2),
            None,
            10,
            false,
        );

        let during = &model.namespaces[&ns];
        assert!(during.live.contains_key("during-window"));
        assert_eq!(during.live_generation, 1);
        assert!(!during.checkpoints.contains_key(&2));

        let after_window_upsert = Op::Upsert {
            ns: ns.clone(),
            vectors: vec![GenVector {
                id: "after-window".to_string(),
                values: vec![1.0, 1.0],
                attributes: None,
            }],
        };
        model.apply_outcome_with_generation_checkpoints(
            &after_window_upsert,
            &OpOutcome::Applied {
                status: 200,
                response: json!({}),
            },
            Some(3),
            None,
            11,
            true,
        );

        let after = &model.namespaces[&ns];
        assert_eq!(after.live_generation, 3);
        assert!(after.checkpoints[&3].contains_key("during-window"));
        assert!(after.checkpoints[&3].contains_key("after-window"));
    }

    #[test]
    fn ambiguous_delete_defers_tombstone_until_resolution() {
        let ns = "model-indeterminate".to_string();
        let mut model = Model::default();
        let mut ns_model = NsModel::new(
            NamespaceSpec {
                dims: 2,
                metric: DistanceMetric::Cosine,
                quantization: QuantizationType::None,
                num_centroids: 1,
                fts_fields: Vec::new(),
                bitmap: false,
            },
            1,
        );
        ns_model.live.insert(
            "id-1".to_string(),
            ModelRecord {
                values: vec![1.0, 0.0],
                attributes: None,
            },
        );
        model.namespaces.insert(ns.clone(), ns_model);

        model.apply_outcome(
            &Op::DeleteVectors {
                ns: ns.clone(),
                ids: vec!["id-1".to_string()],
            },
            &OpOutcome::Ambiguous {
                reason: AmbiguityReason::ServerError { status: 500 },
                status: Some(500),
            },
            None,
            None,
            7,
        );

        let ns_model = &model.namespaces[&ns];
        assert!(ns_model.live.contains_key("id-1"));
        assert!(ns_model.indeterminate.contains_key("id-1"));
        assert!(!ns_model.deleted_ever.contains("id-1"));

        model
            .resolve_indeterminate_record(&ns, "id-1", None)
            .expect("absence must resolve an ambiguous delete as applied");
        let ns_model = &model.namespaces[&ns];
        assert!(!ns_model.live.contains_key("id-1"));
        assert!(ns_model.deleted_ever.contains("id-1"));
    }

    #[test]
    fn indeterminate_resolution_tolerates_attribute_transport_roundoff() {
        let (mut model, ns) = model_with_old_record();
        model
            .namespaces
            .get_mut(&ns)
            .unwrap()
            .live
            .get_mut("id-1")
            .unwrap()
            .attributes = Some(
            [(
                "score".to_string(),
                AttributeValue::Float(99.547_459_052_870_35),
            )]
            .into_iter()
            .collect(),
        );
        model.apply_outcome(
            &Op::DeleteVectors {
                ns: ns.clone(),
                ids: vec!["id-1".to_string()],
            },
            &ambiguous_500(),
            None,
            None,
            8,
        );

        model
            .resolve_indeterminate_record(
                &ns,
                "id-1",
                Some(ModelRecord {
                    values: vec![1.0, 0.0],
                    attributes: Some(
                        [(
                            "score".to_string(),
                            AttributeValue::Float(99.547_459_052_870_37),
                        )]
                        .into_iter()
                        .collect(),
                    ),
                }),
            )
            .expect("transport-level float drift must still match the old record");
        assert!(model.namespaces[&ns].indeterminate.is_empty());
        assert!(model.namespaces[&ns].live.contains_key("id-1"));
    }

    #[test]
    fn ambiguous_upsert_promotes_observed_candidate() {
        let (mut model, ns) = model_with_old_record();
        let candidate = GenVector {
            id: "id-1".to_string(),
            values: vec![0.0, 1.0],
            attributes: None,
        };
        model.apply_outcome(
            &Op::Upsert {
                ns: ns.clone(),
                vectors: vec![candidate.clone()],
            },
            &ambiguous_500(),
            None,
            None,
            9,
        );

        model
            .resolve_indeterminate_record(&ns, "id-1", Some(ModelRecord::from(&candidate)))
            .unwrap();
        assert_eq!(model.namespaces[&ns].live["id-1"].values, vec![0.0, 1.0]);
        assert!(model.namespaces[&ns].indeterminate.is_empty());
    }

    #[test]
    fn ambiguous_upsert_reverts_when_old_value_is_observed() {
        let (mut model, ns) = model_with_old_record();
        let candidate = GenVector {
            id: "id-1".to_string(),
            values: vec![0.0, 1.0],
            attributes: None,
        };
        model.apply_outcome(
            &Op::Upsert {
                ns: ns.clone(),
                vectors: vec![candidate],
            },
            &ambiguous_500(),
            None,
            None,
            10,
        );

        model
            .resolve_indeterminate_record(
                &ns,
                "id-1",
                Some(ModelRecord {
                    values: vec![1.0, 0.0],
                    attributes: None,
                }),
            )
            .unwrap();
        assert_eq!(model.namespaces[&ns].live["id-1"].values, vec![1.0, 0.0]);
        assert!(model.namespaces[&ns].indeterminate.is_empty());
    }

    #[test]
    fn later_definite_upsert_implicitly_resolves_ambiguity() {
        let (mut model, ns) = model_with_old_record();
        let ambiguous = GenVector {
            id: "id-1".to_string(),
            values: vec![0.0, 1.0],
            attributes: None,
        };
        let definite = GenVector {
            id: "id-1".to_string(),
            values: vec![-1.0, 0.0],
            attributes: None,
        };
        model.apply_outcome(
            &Op::Upsert {
                ns: ns.clone(),
                vectors: vec![ambiguous],
            },
            &ambiguous_500(),
            None,
            None,
            11,
        );
        model.apply_outcome(
            &Op::Upsert {
                ns: ns.clone(),
                vectors: vec![definite.clone()],
            },
            &OpOutcome::Applied {
                status: 200,
                response: serde_json::json!({ "upserted": 1 }),
            },
            Some(3),
            None,
            12,
        );

        assert_eq!(model.namespaces[&ns].live["id-1"].values, definite.values);
        assert!(model.namespaces[&ns].indeterminate.is_empty());
    }

    #[test]
    fn joined_rejection_clears_held_upsert_without_changing_state() {
        let (mut model, ns) = model_with_old_record();
        let op = Op::Upsert {
            ns: ns.clone(),
            vectors: vec![GenVector {
                id: "id-1".to_string(),
                values: vec![0.0, 1.0],
                attributes: None,
            }],
        };
        model.apply_outcome(
            &op,
            &OpOutcome::Ambiguous {
                reason: AmbiguityReason::HeldInFlight,
                status: None,
            },
            None,
            None,
            12,
        );
        assert_eq!(
            model.namespaces[&ns].indeterminate["id-1"].reason,
            "held_in_flight"
        );

        model.apply_joined_outcome(
            &op,
            &OpOutcome::NotApplied {
                status: 409,
                response: json!({ "code": "CONFLICT_RETRY" }),
            },
            None,
            None,
            12,
        );

        assert!(model.namespaces[&ns].indeterminate.is_empty());
        assert_eq!(model.namespaces[&ns].live["id-1"].values, vec![1.0, 0.0]);
    }

    #[test]
    fn fresh_create_discards_stale_ambiguous_clone_for_target() {
        let (mut model, source) = model_with_old_record();
        let target = "fresh-after-ambiguous-clone".to_string();
        let spec = model.namespaces[&source].spec.clone();
        model.apply_outcome(
            &Op::CloneNamespace {
                source: source.clone(),
                target: target.clone(),
                as_of: AsOfTarget::Generation(1),
            },
            &ambiguous_500(),
            None,
            None,
            11,
        );

        model.apply_outcome(
            &Op::CreateNamespace {
                ns: target.clone(),
                spec,
            },
            &OpOutcome::Applied {
                status: 201,
                response: json!({}),
            },
            Some(1),
            None,
            12,
        );

        assert!(model.namespaces[&source].indeterminate_ns.is_empty());
        assert!(model.namespaces[&target].live.is_empty());
    }

    fn model_with_old_record() -> (Model, String) {
        let ns = "model-indeterminate-upsert".to_string();
        let mut model = Model::default();
        let mut ns_model = NsModel::new(
            NamespaceSpec {
                dims: 2,
                metric: DistanceMetric::Cosine,
                quantization: QuantizationType::None,
                num_centroids: 1,
                fts_fields: Vec::new(),
                bitmap: false,
            },
            1,
        );
        ns_model.live.insert(
            "id-1".to_string(),
            ModelRecord {
                values: vec![1.0, 0.0],
                attributes: None,
            },
        );
        model.namespaces.insert(ns.clone(), ns_model);
        (model, ns)
    }

    fn ambiguous_500() -> OpOutcome {
        OpOutcome::Ambiguous {
            reason: AmbiguityReason::ServerError { status: 500 },
            status: Some(500),
        }
    }

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
