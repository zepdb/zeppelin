use std::collections::HashMap;
use std::fmt;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::json;
use zeppelin::index::quantization::QuantizationType;
use zeppelin::types::{AttributeValue, ConsistencyLevel, DistanceMetric, Filter};

use super::oracle::ViolationId;

pub type Consistency = ConsistencyLevel;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    CreateNamespace {
        ns: String,
        spec: NamespaceSpec,
    },
    GetNamespace {
        ns: String,
    },
    Upsert {
        ns: String,
        vectors: Vec<GenVector>,
    },
    DeleteVectors {
        ns: String,
        ids: Vec<String>,
    },
    FetchVectors {
        ns: String,
        ids: Vec<String>,
        consistency: Consistency,
    },
    Query {
        ns: String,
        q: GeneratedQuery,
        as_of: Option<AsOfTarget>,
    },
    BatchQuery {
        ns: String,
        qs: Vec<GeneratedQuery>,
    },
    PaginateAll {
        ns: String,
        q: GeneratedQuery,
        page_size: usize,
    },
    InvalidProbe {
        ns: String,
        probe: InvalidProbe,
    },
    CompactEndpoint {
        ns: String,
    },
    GcCycle {
        ns: String,
        keep_count: u64,
    },
    CreateSnapshot {
        ns: String,
        name: String,
    },
    GetSnapshot {
        ns: String,
        name: String,
    },
    ListSnapshots {
        ns: String,
    },
    DeleteSnapshot {
        ns: String,
        name: String,
    },
    CloneNamespace {
        source: String,
        target: String,
        as_of: AsOfTarget,
    },
    PatchIndexConfig {
        ns: String,
        patch: serde_json::Value,
    },
    Hydrate {
        ns: String,
    },
    DeleteNamespace {
        ns: String,
    },
    ProbeSandwich {
        ns: String,
        maintenance: MaintenanceKind,
    },
    CompactInline {
        ns: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceSpec {
    pub dims: usize,
    pub metric: DistanceMetric,
    pub quantization: QuantizationType,
    pub num_centroids: usize,
    pub fts_fields: Vec<String>,
    pub bitmap: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenVector {
    pub id: String,
    pub values: Vec<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, AttributeValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratedQuery {
    pub body: serde_json::Value,
    pub class: QueryOracleClass,
    pub pattern_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryOracleClass {
    ExactAnn {
        top_k: usize,
        consistency: Consistency,
        filter: Option<Filter>,
    },
    Membership {
        consistency: Consistency,
    },
    ExpectError {
        status: u16,
        code: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AsOfTarget {
    Generation(u64),
    Timestamp(String),
    Snapshot(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum InvalidProbe {
    NanVector,
    WrongDims,
    BadIdCharset,
    EmptyBatch,
    OversizedBatch,
    UnknownField,
    BadCursorToken,
    GroupingPlusCursor,
    WeightsLenMismatch,
    AsOfGenZero,
    AsOfGenFuture,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum MaintenanceKind {
    CompactInline,
    CompactEndpoint,
    GcCycle,
    Hydrate,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPhase {
    #[default]
    Legacy,
    Workload,
    DeferredDrain,
    Quiescence,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HoldReleaseCause {
    #[default]
    Legacy,
    LogicalOp,
    Quiesce,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeldExecutionMetadata {
    pub event_id: String,
    pub window_op: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_release_op: Option<u64>,
    #[serde(alias = "release_op")]
    pub actual_join_op: u64,
    #[serde(default)]
    pub release_cause: HoldReleaseCause,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionMetadata {
    #[serde(default)]
    pub phase: ExecutionPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold: Option<HeldExecutionMetadata>,
}

impl ExecutionMetadata {
    #[must_use]
    pub fn workload() -> Self {
        Self {
            phase: ExecutionPhase::Workload,
            hold: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpRecord {
    pub index: u64,
    pub wall_ms: u64,
    pub op: Op,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub response: serde_json::Value,
    #[serde(default)]
    pub outcome: String,
    #[serde(default, deserialize_with = "deserialize_target_node")]
    pub target_node: u8,
    #[serde(default)]
    pub execution: ExecutionMetadata,
    pub gen_after: Option<u64>,
    pub duration_ms: u64,
    pub violations: Vec<ViolationId>,
}

fn deserialize_target_node<'de, D>(deserializer: D) -> Result<u8, D::Error>
where
    D: Deserializer<'de>,
{
    let target_node = u8::deserialize(deserializer)?;
    if target_node <= 1 {
        Ok(target_node)
    } else {
        Err(D::Error::custom(format!(
            "target_node must be 0 or 1, got {target_node}"
        )))
    }
}

impl Op {
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Op::CreateNamespace { .. } => "create_namespace",
            Op::GetNamespace { .. } => "get_namespace",
            Op::Upsert { .. } => "upsert",
            Op::DeleteVectors { .. } => "delete_vectors",
            Op::FetchVectors { .. } => "fetch_vectors",
            Op::Query { .. } => "query",
            Op::BatchQuery { .. } => "batch_query",
            Op::PaginateAll { .. } => "paginate_all",
            Op::InvalidProbe { .. } => "invalid_probe",
            Op::CompactEndpoint { .. } => "compact_endpoint",
            Op::GcCycle { .. } => "gc_cycle",
            Op::CreateSnapshot { .. } => "create_snapshot",
            Op::GetSnapshot { .. } => "get_snapshot",
            Op::ListSnapshots { .. } => "list_snapshots",
            Op::DeleteSnapshot { .. } => "delete_snapshot",
            Op::CloneNamespace { .. } => "clone_namespace",
            Op::PatchIndexConfig { .. } => "patch_index_config",
            Op::Hydrate { .. } => "hydrate",
            Op::DeleteNamespace { .. } => "delete_namespace",
            Op::ProbeSandwich { .. } => "probe_sandwich",
            Op::CompactInline { .. } => "compact_inline",
        }
    }

    #[must_use]
    pub fn namespace(&self) -> &str {
        match self {
            Op::CreateNamespace { ns, .. }
            | Op::GetNamespace { ns }
            | Op::Upsert { ns, .. }
            | Op::DeleteVectors { ns, .. }
            | Op::FetchVectors { ns, .. }
            | Op::Query { ns, .. }
            | Op::BatchQuery { ns, .. }
            | Op::PaginateAll { ns, .. }
            | Op::InvalidProbe { ns, .. }
            | Op::CompactEndpoint { ns }
            | Op::GcCycle { ns, .. }
            | Op::CreateSnapshot { ns, .. }
            | Op::GetSnapshot { ns, .. }
            | Op::ListSnapshots { ns }
            | Op::DeleteSnapshot { ns, .. }
            | Op::PatchIndexConfig { ns, .. }
            | Op::Hydrate { ns }
            | Op::DeleteNamespace { ns }
            | Op::ProbeSandwich { ns, .. }
            | Op::CompactInline { ns } => ns,
            Op::CloneNamespace { source, .. } => source,
        }
    }

    #[must_use]
    pub fn is_mutating(&self) -> bool {
        matches!(
            self,
            Op::CreateNamespace { .. }
                | Op::Upsert { .. }
                | Op::DeleteVectors { .. }
                | Op::CompactEndpoint { .. }
                | Op::CreateSnapshot { .. }
                | Op::DeleteSnapshot { .. }
                | Op::CloneNamespace { .. }
                | Op::PatchIndexConfig { .. }
                | Op::DeleteNamespace { .. }
                | Op::ProbeSandwich { .. }
                | Op::CompactInline { .. }
        )
    }

    #[must_use]
    pub fn tags(&self) -> Vec<&str> {
        match self {
            Op::Query { q, .. } => q.pattern_tags.iter().map(String::as_str).collect(),
            Op::BatchQuery { qs, .. } => qs
                .iter()
                .flat_map(|q| q.pattern_tags.iter().map(String::as_str))
                .collect(),
            Op::PaginateAll { q, .. } => q.pattern_tags.iter().map(String::as_str).collect(),
            Op::InvalidProbe { probe, .. } => vec!["invalid-probe", probe.tag()],
            Op::CompactEndpoint { .. } => vec!["compact-endpoint"],
            Op::GcCycle { .. } => vec!["gc-cycle"],
            Op::CreateSnapshot { .. } | Op::GetSnapshot { .. } | Op::ListSnapshots { .. } => {
                vec!["snapshot"]
            }
            Op::DeleteSnapshot { .. } => vec!["snapshot", "delete-snapshot"],
            Op::CloneNamespace { .. } => vec!["clone"],
            Op::PatchIndexConfig { .. } => vec!["config-patch"],
            Op::Hydrate { .. } => vec!["hydrate"],
            Op::DeleteNamespace { .. } => vec!["delete-recreate"],
            Op::ProbeSandwich { maintenance, .. } => vec!["sandwich", maintenance.tag()],
            _ => Vec::new(),
        }
    }

    #[must_use]
    pub fn rewrite_namespace_prefix(&self, old_prefix: &str, new_prefix: &str) -> Self {
        let rewrite = |value: &str| -> String {
            value.strip_prefix(old_prefix).map_or_else(
                || value.to_string(),
                |suffix| format!("{new_prefix}{suffix}"),
            )
        };
        match self {
            Op::CreateNamespace { ns, spec } => Op::CreateNamespace {
                ns: rewrite(ns),
                spec: spec.clone(),
            },
            Op::GetNamespace { ns } => Op::GetNamespace { ns: rewrite(ns) },
            Op::Upsert { ns, vectors } => Op::Upsert {
                ns: rewrite(ns),
                vectors: vectors.clone(),
            },
            Op::DeleteVectors { ns, ids } => Op::DeleteVectors {
                ns: rewrite(ns),
                ids: ids.clone(),
            },
            Op::FetchVectors {
                ns,
                ids,
                consistency,
            } => Op::FetchVectors {
                ns: rewrite(ns),
                ids: ids.clone(),
                consistency: *consistency,
            },
            Op::Query { ns, q, as_of } => Op::Query {
                ns: rewrite(ns),
                q: q.clone(),
                as_of: as_of.clone(),
            },
            Op::BatchQuery { ns, qs } => Op::BatchQuery {
                ns: rewrite(ns),
                qs: qs.clone(),
            },
            Op::PaginateAll { ns, q, page_size } => Op::PaginateAll {
                ns: rewrite(ns),
                q: q.clone(),
                page_size: *page_size,
            },
            Op::InvalidProbe { ns, probe } => Op::InvalidProbe {
                ns: rewrite(ns),
                probe: *probe,
            },
            Op::CompactEndpoint { ns } => Op::CompactEndpoint { ns: rewrite(ns) },
            Op::GcCycle { ns, keep_count } => Op::GcCycle {
                ns: rewrite(ns),
                keep_count: *keep_count,
            },
            Op::CreateSnapshot { ns, name } => Op::CreateSnapshot {
                ns: rewrite(ns),
                name: name.clone(),
            },
            Op::GetSnapshot { ns, name } => Op::GetSnapshot {
                ns: rewrite(ns),
                name: name.clone(),
            },
            Op::ListSnapshots { ns } => Op::ListSnapshots { ns: rewrite(ns) },
            Op::DeleteSnapshot { ns, name } => Op::DeleteSnapshot {
                ns: rewrite(ns),
                name: name.clone(),
            },
            Op::CloneNamespace {
                source,
                target,
                as_of,
            } => Op::CloneNamespace {
                source: rewrite(source),
                target: rewrite(target),
                as_of: as_of.clone(),
            },
            Op::PatchIndexConfig { ns, patch } => Op::PatchIndexConfig {
                ns: rewrite(ns),
                patch: patch.clone(),
            },
            Op::Hydrate { ns } => Op::Hydrate { ns: rewrite(ns) },
            Op::DeleteNamespace { ns } => Op::DeleteNamespace { ns: rewrite(ns) },
            Op::ProbeSandwich { ns, maintenance } => Op::ProbeSandwich {
                ns: rewrite(ns),
                maintenance: *maintenance,
            },
            Op::CompactInline { ns } => Op::CompactInline { ns: rewrite(ns) },
        }
    }
}

impl NamespaceSpec {
    #[must_use]
    pub fn create_body(&self, ns: &str) -> serde_json::Value {
        let full_text_search = self
            .fts_fields
            .iter()
            .map(|field| (field.clone(), json!({})))
            .collect::<serde_json::Map<_, _>>();
        json!({
            "name": ns,
            "dimensions": self.dims,
            "distance_metric": self.metric,
            "full_text_search": full_text_search,
            "index_config": {
                "nlist": self.num_centroids,
                "quantization": self.quantization,
                "pq_m": 1,
                "hierarchical": false,
                "fts_index": !self.fts_fields.is_empty(),
                "bitmap_index": self.bitmap,
            }
        })
    }

    #[must_use]
    pub fn is_exact(&self) -> bool {
        self.quantization == QuantizationType::None
    }
}

impl InvalidProbe {
    #[must_use]
    pub fn expected_status(self) -> u16 {
        match self {
            Self::OversizedBatch => 413,
            Self::AsOfGenZero | Self::AsOfGenFuture => 410,
            _ => 400,
        }
    }

    #[must_use]
    pub fn expected_code(self) -> &'static str {
        match self {
            Self::OversizedBatch => "PAYLOAD_TOO_LARGE",
            Self::AsOfGenZero | Self::AsOfGenFuture => "POINT_IN_TIME_NOT_RETAINED",
            _ => "VALIDATION_ERROR",
        }
    }

    #[must_use]
    pub fn is_write_shaped(self) -> bool {
        matches!(
            self,
            Self::WrongDims | Self::BadIdCharset | Self::EmptyBatch
        )
    }

    #[must_use]
    pub fn tag(self) -> &'static str {
        match self {
            Self::NanVector => "nan-vector",
            Self::WrongDims => "wrong-dims",
            Self::BadIdCharset => "bad-id-charset",
            Self::EmptyBatch => "empty-batch",
            Self::OversizedBatch => "oversized-batch",
            Self::UnknownField => "unknown-field",
            Self::BadCursorToken => "bad-cursor-token",
            Self::GroupingPlusCursor => "grouping-plus-cursor",
            Self::WeightsLenMismatch => "weights-len-mismatch",
            Self::AsOfGenZero => "as-of-410",
            Self::AsOfGenFuture => "as-of-410",
        }
    }
}

impl MaintenanceKind {
    #[must_use]
    pub fn tag(self) -> &'static str {
        match self {
            Self::CompactInline => "sandwich-compact-inline",
            Self::CompactEndpoint => "sandwich-compact-endpoint",
            Self::GcCycle => "sandwich-gc-cycle",
            Self::Hydrate => "sandwich-hydrate",
        }
    }
}

impl GeneratedQuery {
    #[must_use]
    pub fn top_k(&self) -> Option<usize> {
        match self.class {
            QueryOracleClass::ExactAnn { top_k, .. } => Some(top_k),
            QueryOracleClass::Membership { .. } => self
                .body
                .get("top_k")
                .and_then(serde_json::Value::as_u64)
                .map(|value| value as usize),
            QueryOracleClass::ExpectError { .. } => None,
        }
    }

    #[must_use]
    pub fn consistency(&self) -> Option<Consistency> {
        match self.class {
            QueryOracleClass::ExactAnn { consistency, .. }
            | QueryOracleClass::Membership { consistency } => Some(consistency),
            QueryOracleClass::ExpectError { .. } => None,
        }
    }
}

impl fmt::Display for AsOfTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AsOfTarget::Generation(generation) => write!(f, "{generation}"),
            AsOfTarget::Timestamp(timestamp) => write!(f, "{timestamp}"),
            AsOfTarget::Snapshot(name) => write!(f, "snapshot:{name}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_record_serializes_execution_and_defaults_legacy_metadata() {
        let record = OpRecord {
            index: 3,
            wall_ms: 0,
            op: Op::GetNamespace {
                ns: "ns".to_string(),
            },
            method: "GET".to_string(),
            path: "/v1/namespaces/ns".to_string(),
            status: 200,
            response: serde_json::json!({}),
            outcome: "applied".to_string(),
            target_node: 0,
            execution: ExecutionMetadata {
                phase: ExecutionPhase::Workload,
                hold: Some(HeldExecutionMetadata {
                    event_id: "sched-manifest-hold".to_string(),
                    window_op: 3,
                    scheduled_release_op: Some(7),
                    actual_join_op: 7,
                    release_cause: HoldReleaseCause::LogicalOp,
                }),
            },
            gen_after: None,
            duration_ms: 1,
            violations: Vec::new(),
        };
        let mut encoded = serde_json::to_value(&record).unwrap();
        assert_eq!(encoded["target_node"], 0);
        assert_eq!(encoded["execution"]["phase"], "workload");
        assert_eq!(
            encoded["execution"]["hold"],
            serde_json::json!({
                "event_id": "sched-manifest-hold",
                "window_op": 3,
                "scheduled_release_op": 7,
                "actual_join_op": 7,
                "release_cause": "logical_op",
            })
        );

        encoded.as_object_mut().unwrap().remove("execution");
        encoded.as_object_mut().unwrap().remove("target_node");
        let legacy: OpRecord = serde_json::from_value(encoded).unwrap();
        assert_eq!(legacy.target_node, 0);
        assert_eq!(legacy.execution, ExecutionMetadata::default());
        assert_eq!(legacy.execution.phase, ExecutionPhase::Legacy);
        assert_eq!(legacy.execution.hold, None);
    }

    #[test]
    fn op_record_rejects_target_node_outside_two_node_domain() {
        let record = OpRecord {
            index: 3,
            wall_ms: 0,
            op: Op::GetNamespace {
                ns: "ns".to_string(),
            },
            method: "GET".to_string(),
            path: "/v1/namespaces/ns".to_string(),
            status: 200,
            response: serde_json::json!({}),
            outcome: "applied".to_string(),
            target_node: 0,
            execution: ExecutionMetadata::workload(),
            gen_after: None,
            duration_ms: 1,
            violations: Vec::new(),
        };
        let mut encoded = serde_json::to_value(record).unwrap();
        encoded["target_node"] = serde_json::json!(2);

        let error = serde_json::from_value::<OpRecord>(encoded).unwrap_err();
        assert!(error.to_string().contains("target_node"), "{error}");
        assert!(error.to_string().contains("0 or 1"), "{error}");
    }

    #[test]
    fn execution_metadata_separates_scheduled_release_from_quiesced_join() {
        let execution = ExecutionMetadata {
            phase: ExecutionPhase::DeferredDrain,
            hold: Some(HeldExecutionMetadata {
                event_id: "sched-boundary-hold".to_string(),
                window_op: 3,
                scheduled_release_op: Some(7),
                actual_join_op: 5,
                release_cause: HoldReleaseCause::Quiesce,
            }),
        };

        let encoded = serde_json::to_value(&execution).unwrap();
        assert_eq!(encoded["phase"], "deferred_drain");
        assert_eq!(encoded["hold"]["scheduled_release_op"], 7);
        assert_eq!(encoded["hold"]["actual_join_op"], 5);
        assert_eq!(encoded["hold"]["release_cause"], "quiesce");
    }
}
