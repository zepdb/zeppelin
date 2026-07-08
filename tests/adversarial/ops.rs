use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpRecord {
    pub index: u64,
    pub wall_ms: u64,
    pub op: Op,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub response: serde_json::Value,
    pub gen_after: Option<u64>,
    pub duration_ms: u64,
    pub violations: Vec<ViolationId>,
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
