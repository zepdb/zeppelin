//! Checked-in performance-contract schema, validation, and cost checker.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize};

use super::dataset::DatasetSpec;
use super::depth::SpanKind;
use super::scenario::{RepeatCounters, ScenarioOutcome};
use crate::common::counting::{ClassStats, ALL_CLASSES};

const SCHEMA_VERSION: u32 = 1;
const COLD_MANIFEST_TTL_MS: u64 = 0;
const WARM_MANIFEST_TTL_MS: u64 = 3_600_000;

/// Complete checked-in TOML contract for one scenario.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ContractSpec {
    pub schema_version: u32,
    pub scenario: String,
    pub dataset: DatasetSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ns_config: Option<NsConfig>,
    pub server_config: ServerConfig,
    pub run: RunSpec,
    pub baseline: BaselineSpec,
    #[serde(rename = "assert")]
    pub assertions: AssertionSpec,
}

/// Namespace fields whose values affect storage costs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NsConfig {
    pub quantization: Quantization,
}

/// Quantization modes admitted by the Phase-1 contract schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Quantization {
    Sq8,
}

/// Server knobs pinned by a scenario contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nprobe: Option<usize>,
    pub manifest_cache_ttl_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_cache_max_mb: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wal_fragments_before_compact: Option<usize>,
}

/// Measurement lifecycle encoded in `[run]`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunSpec {
    pub cache_state: CacheState,
    pub measure: MeasureSpec,
    pub repeats: usize,
}

/// Cache state established immediately before measured operations begin.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CacheState {
    Cold,
    Warm,
    WarmHydrated,
}

/// The single operation measured by a Phase-1 scenario.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum MeasureSpec {
    Query {
        consistency: Consistency,
        top_k: usize,
        query_index: usize,
    },
    Upsert {
        batch: usize,
    },
}

/// Query consistency modes represented in contract TOML.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Consistency {
    Strong,
    Eventual,
}

/// Provenance and human approval for a captured baseline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BaselineSpec {
    pub git_rev: String,
    pub captured: String,
    pub approved_by: String,
    pub reason: String,
}

/// All exact and bounded assertions checked for every measured repeat.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AssertionSpec {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub depth: BTreeMap<String, DepthAssertion>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub gets: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub puts: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub get_bytes: BTreeMap<String, ByteBound>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub put_bytes: BTreeMap<String, ByteBound>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub get_keys: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub put_keys: BTreeMap<String, u64>,
}

/// Exact or upper-bound roundtrip-depth policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DepthMode {
    Exact,
    Max,
}

/// One named depth assertion (`get` or `put_get`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DepthAssertion {
    pub mode: DepthMode,
    pub value: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
}

/// An exact byte count or an explicitly justified byte band.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ByteBound {
    Exact {
        exact: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        why: Option<String>,
    },
    Band {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min: Option<u64>,
        max: u64,
        why: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExactByteBound {
    exact: u64,
    #[serde(default)]
    why: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BandedByteBound {
    #[serde(default)]
    min: Option<u64>,
    max: u64,
    why: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ParsedByteBound {
    Exact(ExactByteBound),
    Band(BandedByteBound),
}

impl<'de> Deserialize<'de> for ByteBound {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match ParsedByteBound::deserialize(deserializer)? {
            ParsedByteBound::Exact(bound) => Self::Exact {
                exact: bound.exact,
                why: bound.why,
            },
            ParsedByteBound::Band(bound) => Self::Band {
                min: bound.min,
                max: bound.max,
                why: bound.why,
            },
        })
    }
}

/// A single contract failure, shaped exactly like the performance plan.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum CostViolation {
    OpCount {
        class: String,
        kind: SpanKind,
        expected: u64,
        actual: u64,
    },
    KeyCount {
        substring: String,
        kind: SpanKind,
        expected: u64,
        actual: u64,
    },
    Bytes {
        class: String,
        bound: ByteBound,
        actual: u64,
    },
    Depth {
        kinds: String,
        mode: DepthMode,
        limit: u32,
        actual: u32,
        chain: Vec<String>,
    },
    RepeatDrift {
        repeat: usize,
        detail: String,
    },
    BaselineDrift {
        field: String,
        detail: String,
    },
}

/// Load and validate `tests/perf_contract/contracts/<scenario>.toml`.
pub fn load_contract(scenario: &str) -> Result<ContractSpec, String> {
    if scenario.is_empty()
        || !scenario
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(format!(
            "invalid performance-contract scenario name {scenario:?}"
        ));
    }

    let path = contract_path(scenario);
    let source = fs::read_to_string(&path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    parse_contract(scenario, &source)
        .map_err(|error| format!("invalid performance contract {}: {error}", path.display()))
}

fn contract_path(scenario: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/perf_contract/contracts")
        .join(format!("{scenario}.toml"))
}

fn parse_contract(scenario: &str, source: &str) -> Result<ContractSpec, String> {
    let contract: ContractSpec =
        toml::from_str(source).map_err(|error| format!("TOML schema error: {error}"))?;
    validate_contract(scenario, &contract)?;
    Ok(contract)
}

fn validate_contract(scenario: &str, contract: &ContractSpec) -> Result<(), String> {
    if contract.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "unsupported schema_version {}; expected {SCHEMA_VERSION}",
            contract.schema_version
        ));
    }
    if contract.scenario != scenario {
        return Err(format!(
            "scenario mismatch: requested {scenario:?}, contract declares {:?}",
            contract.scenario
        ));
    }
    if !matches!(
        contract.server_config.manifest_cache_ttl_ms,
        COLD_MANIFEST_TTL_MS | WARM_MANIFEST_TTL_MS
    ) {
        return Err(format!(
            "server_config.manifest_cache_ttl_ms must be 0 or 3600000, got {}",
            contract.server_config.manifest_cache_ttl_ms
        ));
    }
    if contract.run.repeats == 0 {
        return Err("run.repeats must be greater than zero".to_string());
    }
    if contract.baseline.approved_by.trim().is_empty() {
        return Err("baseline.approved_by must be non-empty".to_string());
    }
    if contract.baseline.reason.trim().is_empty() {
        return Err("baseline.reason must be non-empty".to_string());
    }

    validate_complete_class_map("assert.gets", &contract.assertions.gets)?;
    validate_complete_class_map("assert.puts", &contract.assertions.puts)?;
    validate_complete_class_map("assert.get_bytes", &contract.assertions.get_bytes)?;
    validate_complete_class_map("assert.put_bytes", &contract.assertions.put_bytes)?;

    for (name, assertion) in &contract.assertions.depth {
        if !matches!(name.as_str(), "get" | "put_get") {
            return Err(format!(
                "unknown assert.depth key {name:?}; expected get or put_get"
            ));
        }
        if assertion.mode == DepthMode::Max
            && assertion
                .why
                .as_deref()
                .is_none_or(|why| why.trim().is_empty())
        {
            return Err(format!(
                "assert.depth.{name} with mode=max requires a non-empty why"
            ));
        }
    }
    match &contract.run.measure {
        MeasureSpec::Query { .. } if !contract.assertions.depth.contains_key("get") => {
            return Err("Phase-1 query contracts require assert.depth.get".to_string());
        }
        MeasureSpec::Upsert { .. } if !contract.assertions.depth.contains_key("put_get") => {
            return Err("Phase-1 upsert contracts require assert.depth.put_get".to_string());
        }
        MeasureSpec::Upsert { .. } if !contract.assertions.put_keys.contains_key("manifests/") => {
            return Err(
                "Phase-1 upsert contracts require assert.put_keys[\"manifests/\"]".to_string(),
            );
        }
        _ => {}
    }
    for (name, bound) in contract
        .assertions
        .get_bytes
        .iter()
        .chain(&contract.assertions.put_bytes)
    {
        validate_byte_bound(name, bound)?;
    }

    Ok(())
}

fn validate_complete_class_map<T>(
    table: &str,
    assertions: &BTreeMap<String, T>,
) -> Result<(), String> {
    for name in assertions.keys() {
        if name != "total" && !ALL_CLASSES.iter().any(|class| class.name() == name) {
            return Err(format!("unknown {table} artifact class {name:?}"));
        }
    }
    for required in ALL_CLASSES
        .iter()
        .map(|class| class.name())
        .chain(std::iter::once("total"))
    {
        if !assertions.contains_key(required) {
            return Err(format!(
                "{table} must list every artifact class plus total; missing {required:?}"
            ));
        }
    }
    Ok(())
}

fn validate_byte_bound(name: &str, bound: &ByteBound) -> Result<(), String> {
    if let ByteBound::Band { min, max, why } = bound {
        if why.trim().is_empty() {
            return Err(format!("byte band for {name:?} requires a non-empty why"));
        }
        if min.is_some_and(|minimum| minimum > *max) {
            return Err(format!("byte band for {name:?} has min greater than max"));
        }
    }
    Ok(())
}

/// Check every repeat against the contract and report all cost violations.
#[must_use]
pub fn check_contract(contract: &ContractSpec, outcome: &ScenarioOutcome) -> Vec<CostViolation> {
    let mut violations = Vec::new();

    if contract.baseline.approved_by.trim().is_empty() {
        violations.push(CostViolation::BaselineDrift {
            field: "baseline.approved_by".to_string(),
            detail: "checked-in contracts require non-empty human approval".to_string(),
        });
    }
    if contract.baseline.reason.trim().is_empty() {
        violations.push(CostViolation::BaselineDrift {
            field: "baseline.reason".to_string(),
            detail: "checked-in contracts require a non-empty approval reason".to_string(),
        });
    }

    check_repeat_identity(contract, outcome, &mut violations);
    for (repeat_index, repeat) in outcome.per_repeat.iter().enumerate() {
        check_op_counts(
            repeat_index,
            repeat,
            "assert.gets",
            &contract.assertions.gets,
            SpanKind::Get,
            |stats| stats.get_ops,
            &mut violations,
        );
        check_op_counts(
            repeat_index,
            repeat,
            "assert.puts",
            &contract.assertions.puts,
            SpanKind::Put,
            |stats| stats.put_ops,
            &mut violations,
        );
        check_byte_bounds(
            repeat_index,
            repeat,
            "assert.get_bytes",
            &contract.assertions.get_bytes,
            |stats| stats.get_bytes,
            &mut violations,
        );
        check_byte_bounds(
            repeat_index,
            repeat,
            "assert.put_bytes",
            &contract.assertions.put_bytes,
            |stats| stats.put_bytes,
            &mut violations,
        );
        check_key_counts(
            repeat_index,
            repeat,
            "assert.get_keys",
            &contract.assertions.get_keys,
            SpanKind::Get,
            &mut violations,
        );
        check_key_counts(
            repeat_index,
            repeat,
            "assert.put_keys",
            &contract.assertions.put_keys,
            SpanKind::Put,
            &mut violations,
        );
        check_depths(
            repeat_index,
            repeat,
            &contract.assertions.depth,
            &mut violations,
        );
    }

    violations
}

fn check_repeat_identity(
    contract: &ContractSpec,
    outcome: &ScenarioOutcome,
    violations: &mut Vec<CostViolation>,
) {
    let Some(first) = outcome.per_repeat.first() else {
        violations.push(CostViolation::RepeatDrift {
            repeat: 0,
            detail: "scenario produced no measured repeats".to_string(),
        });
        return;
    };

    for (index, repeat) in outcome.per_repeat.iter().enumerate().skip(1) {
        let mut details = Vec::new();
        compare_repeat_counters(contract, first, repeat, &mut details);
        if repeat.get_path.depth != first.get_path.depth {
            details.push("GET/HEAD critical-path depth differs");
        }
        if repeat.put_get_path.depth != first.put_get_path.depth {
            details.push("PUT+GET critical-path depth differs");
        }
        if !details.is_empty() {
            violations.push(CostViolation::RepeatDrift {
                repeat: index,
                detail: details.join("; "),
            });
        }
    }
}

fn compare_repeat_counters(
    contract: &ContractSpec,
    first: &RepeatCounters,
    repeat: &RepeatCounters,
    details: &mut Vec<&'static str>,
) {
    if first.classes.keys().ne(repeat.classes.keys()) {
        details.push("artifact-class counter keys differ");
        return;
    }

    let is_upsert = matches!(&contract.run.measure, MeasureSpec::Upsert { .. });
    for (class, first_stats) in &first.classes {
        let repeat_stats = repeat
            .classes
            .get(class)
            .expect("matching artifact-class key sets were checked");
        if first_stats.get_ops != repeat_stats.get_ops
            || first_stats.put_ops != repeat_stats.put_ops
        {
            details.push("per-class operation counts differ");
            break;
        }
    }
    if first.totals.get_ops != repeat.totals.get_ops
        || first.totals.put_ops != repeat.totals.put_ops
    {
        details.push("total operation counts differ");
    }

    if is_upsert {
        for (class, first_stats) in &first.classes {
            if matches!(class.as_str(), "manifest" | "wal" | "other") {
                continue;
            }
            let repeat_stats = repeat
                .classes
                .get(class)
                .expect("matching artifact-class key sets were checked");
            if first_stats.get_bytes != repeat_stats.get_bytes
                || first_stats.put_bytes != repeat_stats.put_bytes
            {
                details.push("non-serialization artifact bytes differ");
                break;
            }
        }
    } else if first
        .classes
        .iter()
        .any(|(class, stats)| repeat.classes.get(class) != Some(stats))
        || first.totals != repeat.totals
    {
        details.push("query bytes or totals differ");
    }
}

#[allow(clippy::too_many_arguments)]
fn check_op_counts(
    repeat_index: usize,
    repeat: &RepeatCounters,
    table: &str,
    assertions: &BTreeMap<String, u64>,
    kind: SpanKind,
    select: fn(ClassStats) -> u64,
    violations: &mut Vec<CostViolation>,
) {
    for (class, expected) in assertions {
        let actual = select(stats_for(repeat, class));
        match actual.cmp(expected) {
            std::cmp::Ordering::Greater => violations.push(CostViolation::OpCount {
                class: class.clone(),
                kind,
                expected: *expected,
                actual,
            }),
            std::cmp::Ordering::Less => push_baseline_drift(
                violations,
                format!("{table}.{class}"),
                repeat_index,
                *expected,
                actual,
            ),
            std::cmp::Ordering::Equal => {}
        }
    }
}

fn check_byte_bounds(
    repeat_index: usize,
    repeat: &RepeatCounters,
    table: &str,
    assertions: &BTreeMap<String, ByteBound>,
    select: fn(ClassStats) -> u64,
    violations: &mut Vec<CostViolation>,
) {
    for (class, bound) in assertions {
        let actual = select(stats_for(repeat, class));
        match bound {
            ByteBound::Exact { exact, .. } if actual > *exact => {
                violations.push(CostViolation::Bytes {
                    class: class.clone(),
                    bound: bound.clone(),
                    actual,
                });
            }
            ByteBound::Exact { exact, .. } if actual < *exact => push_baseline_drift(
                violations,
                format!("{table}.{class}"),
                repeat_index,
                *exact,
                actual,
            ),
            ByteBound::Band { max, .. } if actual > *max => {
                violations.push(CostViolation::Bytes {
                    class: class.clone(),
                    bound: bound.clone(),
                    actual,
                });
            }
            ByteBound::Band {
                min: Some(minimum), ..
            } if actual < *minimum => push_baseline_drift(
                violations,
                format!("{table}.{class}"),
                repeat_index,
                *minimum,
                actual,
            ),
            _ => {}
        }
    }
}

fn check_key_counts(
    repeat_index: usize,
    repeat: &RepeatCounters,
    table: &str,
    assertions: &BTreeMap<String, u64>,
    kind: SpanKind,
    violations: &mut Vec<CostViolation>,
) {
    for (substring, expected) in assertions {
        let actual = repeat
            .spans
            .iter()
            .filter(|span| span.kind == kind && span.key.contains(substring))
            .count() as u64;
        match actual.cmp(expected) {
            std::cmp::Ordering::Greater => violations.push(CostViolation::KeyCount {
                substring: substring.clone(),
                kind,
                expected: *expected,
                actual,
            }),
            std::cmp::Ordering::Less => push_baseline_drift(
                violations,
                format!("{table}.{substring}"),
                repeat_index,
                *expected,
                actual,
            ),
            std::cmp::Ordering::Equal => {}
        }
    }
}

fn check_depths(
    repeat_index: usize,
    repeat: &RepeatCounters,
    assertions: &BTreeMap<String, DepthAssertion>,
    violations: &mut Vec<CostViolation>,
) {
    for (name, assertion) in assertions {
        let path = match name.as_str() {
            "get" => &repeat.get_path,
            "put_get" => &repeat.put_get_path,
            _ => panic!("unvalidated performance-contract depth key {name:?}"),
        };
        let chain = || path.chain.iter().map(|span| span.key.clone()).collect();

        match assertion.mode {
            DepthMode::Exact if path.depth > assertion.value => {
                violations.push(CostViolation::Depth {
                    kinds: name.clone(),
                    mode: assertion.mode,
                    limit: assertion.value,
                    actual: path.depth,
                    chain: chain(),
                });
            }
            DepthMode::Exact if path.depth < assertion.value => push_baseline_drift(
                violations,
                format!("assert.depth.{name}"),
                repeat_index,
                u64::from(assertion.value),
                u64::from(path.depth),
            ),
            DepthMode::Max if path.depth > assertion.value => {
                violations.push(CostViolation::Depth {
                    kinds: name.clone(),
                    mode: assertion.mode,
                    limit: assertion.value,
                    actual: path.depth,
                    chain: chain(),
                });
            }
            _ => {}
        }
    }
}

fn stats_for(repeat: &RepeatCounters, class: &str) -> ClassStats {
    if class == "total" {
        repeat.totals
    } else {
        *repeat
            .classes
            .get(class)
            .unwrap_or_else(|| panic!("scenario outcome omitted required artifact class {class:?}"))
    }
}

fn push_baseline_drift(
    violations: &mut Vec<CostViolation>,
    field: String,
    repeat: usize,
    expected: u64,
    actual: u64,
) {
    violations.push(CostViolation::BaselineDrift {
        field,
        detail: format!(
            "repeat {repeat}: actual {actual} is below contracted {expected}; the cost frontier moved and requires an approved re-baseline"
        ),
    });
}

/// Build a complete proposed contract from measured counters.
///
/// Capture never grants approval. Callers serialize this value beneath the
/// run artifact's `proposed/` directory, never the checked-in contracts path.
#[must_use]
pub fn capture_contract(
    source: &ContractSpec,
    outcome: &ScenarioOutcome,
    git_rev: impl Into<String>,
    captured: impl Into<String>,
) -> ContractSpec {
    assert!(
        !outcome.per_repeat.is_empty(),
        "cannot capture a performance contract without measured repeats"
    );

    let class_names = ALL_CLASSES
        .iter()
        .map(|class| class.name().to_string())
        .chain(std::iter::once("total".to_string()))
        .collect::<Vec<_>>();
    let gets = capture_exact_stats(outcome, &class_names, "GET ops", |stats| stats.get_ops);
    let puts = capture_exact_stats(outcome, &class_names, "PUT ops", |stats| stats.put_ops);
    let get_bytes = capture_byte_stats(
        source,
        outcome,
        &class_names,
        "GET bytes",
        |stats| stats.get_bytes,
        true,
    );
    let put_bytes = capture_byte_stats(
        source,
        outcome,
        &class_names,
        "PUT bytes",
        |stats| stats.put_bytes,
        false,
    );
    let depth = capture_depths(source, outcome);
    let get_keys = capture_keys(
        outcome,
        &source.assertions.get_keys,
        SpanKind::Get,
        "GET key count",
    );
    let put_keys = capture_keys(
        outcome,
        &source.assertions.put_keys,
        SpanKind::Put,
        "PUT key count",
    );
    let mut run = source.run.clone();
    run.repeats = outcome.per_repeat.len();

    ContractSpec {
        schema_version: SCHEMA_VERSION,
        scenario: source.scenario.clone(),
        dataset: source.dataset.clone(),
        ns_config: source.ns_config.clone(),
        server_config: source.server_config.clone(),
        run,
        baseline: BaselineSpec {
            git_rev: git_rev.into(),
            captured: captured.into(),
            approved_by: String::new(),
            reason: String::new(),
        },
        assertions: AssertionSpec {
            depth,
            gets,
            puts,
            get_bytes,
            put_bytes,
            get_keys,
            put_keys,
        },
    }
}

fn capture_exact_stats(
    outcome: &ScenarioOutcome,
    class_names: &[String],
    metric: &str,
    select: fn(ClassStats) -> u64,
) -> BTreeMap<String, u64> {
    class_names
        .iter()
        .map(|class| {
            let values = outcome
                .per_repeat
                .iter()
                .map(|repeat| select(stats_for(repeat, class)))
                .collect::<Vec<_>>();
            (class.clone(), require_identical(metric, class, &values))
        })
        .collect()
}

fn capture_byte_stats(
    source: &ContractSpec,
    outcome: &ScenarioOutcome,
    class_names: &[String],
    metric: &str,
    select: fn(ClassStats) -> u64,
    is_get: bool,
) -> BTreeMap<String, ByteBound> {
    let source_bounds = if is_get {
        &source.assertions.get_bytes
    } else {
        &source.assertions.put_bytes
    };
    class_names
        .iter()
        .map(|class| {
            let values = outcome
                .per_repeat
                .iter()
                .map(|repeat| select(stats_for(repeat, class)))
                .collect::<Vec<_>>();
            let minimum = *values.iter().min().expect("capture repeats are non-empty");
            let maximum = *values.iter().max().expect("capture repeats are non-empty");
            let bound = if minimum == maximum {
                ByteBound::Exact {
                    exact: minimum,
                    why: source_bounds.get(class).and_then(byte_bound_why),
                }
            } else {
                assert!(
                    matches!(class.as_str(), "manifest" | "wal" | "other" | "total"),
                    "cannot capture variable {metric} for {class:?}; byte bands are limited to manifest, wal, other, and total"
                );
                ByteBound::Band {
                    min: Some(minimum),
                    max: maximum,
                    why: format!("observed {metric} variation across measured repeats"),
                }
            };
            (class.clone(), bound)
        })
        .collect()
}

fn byte_bound_why(bound: &ByteBound) -> Option<String> {
    match bound {
        ByteBound::Exact { why, .. } => why.clone(),
        ByteBound::Band { why, .. } if !why.trim().is_empty() => Some(why.clone()),
        ByteBound::Band { .. } => None,
    }
}

fn capture_depths(
    source: &ContractSpec,
    outcome: &ScenarioOutcome,
) -> BTreeMap<String, DepthAssertion> {
    source
        .assertions
        .depth
        .iter()
        .map(|(name, previous)| {
            let values = outcome
                .per_repeat
                .iter()
                .map(|repeat| match name.as_str() {
                    "get" => repeat.get_path.depth,
                    "put_get" => repeat.put_get_path.depth,
                    _ => panic!("unvalidated performance-contract depth key {name:?}"),
                })
                .collect::<Vec<_>>();
            let maximum = *values.iter().max().expect("capture repeats are non-empty");
            let (value, why) = match previous.mode {
                DepthMode::Exact => (require_identical_depth(name, &values), previous.why.clone()),
                DepthMode::Max => (
                    maximum,
                    Some(
                        previous
                            .why
                            .clone()
                            .filter(|why| !why.trim().is_empty())
                            .unwrap_or_else(|| {
                                panic!("cannot capture max depth {name:?} without a non-empty why")
                            }),
                    ),
                ),
            };
            (
                name.clone(),
                DepthAssertion {
                    mode: previous.mode,
                    value,
                    why,
                },
            )
        })
        .collect()
}

fn capture_keys(
    outcome: &ScenarioOutcome,
    source: &BTreeMap<String, u64>,
    kind: SpanKind,
    metric: &str,
) -> BTreeMap<String, u64> {
    source
        .keys()
        .map(|substring| {
            let values = outcome
                .per_repeat
                .iter()
                .map(|repeat| {
                    repeat
                        .spans
                        .iter()
                        .filter(|span| span.kind == kind && span.key.contains(substring))
                        .count() as u64
                })
                .collect::<Vec<_>>();
            (
                substring.clone(),
                require_identical(metric, substring, &values),
            )
        })
        .collect()
}

fn require_identical(metric: &str, name: &str, values: &[u64]) -> u64 {
    let first = *values.first().expect("capture repeats are non-empty");
    assert!(
        values.iter().all(|value| *value == first),
        "cannot capture exact {metric} for {name:?}: repeat values were {values:?}"
    );
    first
}

fn require_identical_depth(name: &str, values: &[u32]) -> u32 {
    let first = *values.first().expect("capture repeats are non-empty");
    assert!(
        values.iter().all(|value| *value == first),
        "cannot capture exact depth for {name:?}: repeat values were {values:?}"
    );
    first
}

#[cfg(test)]
mod tests {
    use super::super::dataset::DatasetExpectations;
    use super::super::depth::CriticalPath;
    use super::*;

    const VALID_CONTRACT: &str = r#"
schema_version = 1
scenario = "unit"

[dataset]
vectors = 16
dims = 8
nlist = 4
seed = 7
attrs = "none"
fts = "none"

[server_config]
manifest_cache_ttl_ms = 0

[run]
cache_state = "warm"
repeats = 8
measure = { op = "upsert", batch = 1 }

[baseline]
git_rev = "abc123"
captured = "2026-07-09"
approved_by = "reviewer"
reason = "unit contract"

[assert.depth.put_get]
mode = "exact"
value = 1
why = "stable test depth"

[assert.gets]
cluster = 0
attrs = 0
centroids = 0
bootstrap = 0
sq = 0
bitmap = 0
sketch = 0
fts = 0
wal = 0
manifest = 0
other = 0
total = 0

[assert.puts]
cluster = 0
attrs = 0
centroids = 0
bootstrap = 0
sq = 0
bitmap = 0
sketch = 0
fts = 0
wal = 0
manifest = 0
other = 0
total = 0

[assert.get_bytes]
cluster = { exact = 0 }
attrs = { exact = 0 }
centroids = { exact = 0 }
bootstrap = { exact = 0 }
sq = { exact = 0 }
bitmap = { exact = 0 }
sketch = { exact = 0 }
fts = { exact = 0 }
wal = { exact = 0 }
manifest = { exact = 0 }
other = { exact = 0 }
total = { exact = 0 }

[assert.put_bytes]
cluster = { exact = 0 }
attrs = { exact = 0 }
centroids = { exact = 0 }
bootstrap = { exact = 0 }
sq = { exact = 0 }
bitmap = { exact = 0 }
sketch = { exact = 0 }
fts = { exact = 0 }
wal = { exact = 0 }
manifest = { exact = 0 }
other = { exact = 0 }
total = { exact = 0 }

[assert.put_keys]
"manifests/" = 1
"#;

    fn parsed_contract() -> ContractSpec {
        parse_contract("unit", VALID_CONTRACT).expect("test contract must be valid")
    }

    fn path(depth: u32) -> CriticalPath {
        CriticalPath {
            depth,
            chain: Vec::new(),
        }
    }

    fn repeat(get_depth: u32, put_get_depth: u32) -> RepeatCounters {
        RepeatCounters {
            classes: ALL_CLASSES
                .iter()
                .map(|class| (class.name().to_string(), ClassStats::default()))
                .collect(),
            totals: ClassStats::default(),
            get_path: path(get_depth),
            put_get_path: path(put_get_depth),
            spans: Vec::new(),
            response_cutoff_us: 0,
            raw_get_path: path(get_depth),
            raw_put_get_path: path(put_get_depth),
        }
    }

    fn set_get_bytes(repeat: &mut RepeatCounters, class: &str, bytes: u64) {
        repeat
            .classes
            .get_mut(class)
            .expect("test class must exist")
            .get_bytes = bytes;
        repeat.totals.get_bytes = repeat.classes.values().map(|stats| stats.get_bytes).sum();
    }

    fn outcome(per_repeat: Vec<RepeatCounters>) -> ScenarioOutcome {
        ScenarioOutcome {
            per_repeat,
            expected: DatasetExpectations {
                rows_per_cluster: 4,
                cluster_f32_bytes: 128,
                probe_clusters: Vec::new(),
            },
        }
    }

    #[test]
    #[ignore = "perf-contract schema selftest; run explicitly"]
    fn unknown_nested_keys_are_rejected() {
        let source = VALID_CONTRACT.replace(
            "manifest_cache_ttl_ms = 0",
            "manifest_cache_ttl_ms = 0\nunknown_knob = 1",
        );

        let error = parse_contract("unit", &source).unwrap_err();
        assert!(error.contains("unknown field"), "unexpected error: {error}");
    }

    #[test]
    #[ignore = "perf-contract schema selftest; run explicitly"]
    fn byte_bands_require_a_reason() {
        let source = VALID_CONTRACT.replacen(
            "manifest = { exact = 0 }",
            "manifest = { min = 1, max = 2 }",
            1,
        );

        let error = parse_contract("unit", &source).unwrap_err();
        assert!(
            error.contains("TOML schema error"),
            "unexpected error: {error}"
        );
    }

    #[test]
    #[ignore = "perf-contract schema selftest; run explicitly"]
    fn inverted_byte_bands_are_rejected() {
        let source = VALID_CONTRACT.replacen(
            "manifest = { exact = 0 }",
            "manifest = { min = 3, max = 2, why = \"variable JSON\" }",
            1,
        );

        let error = parse_contract("unit", &source).unwrap_err();
        assert!(
            error.contains("min greater than max"),
            "unexpected error: {error}"
        );
    }

    #[test]
    #[ignore = "perf-contract schema selftest; run explicitly"]
    fn checked_in_governance_requires_approval_and_reason() {
        for (field, replacement) in [
            ("baseline.approved_by", "approved_by = \"   \""),
            ("baseline.reason", "reason = \"   \""),
        ] {
            let needle = if field.ends_with("approved_by") {
                "approved_by = \"reviewer\""
            } else {
                "reason = \"unit contract\""
            };
            let source = VALID_CONTRACT.replace(needle, replacement);
            let error = parse_contract("unit", &source).unwrap_err();
            assert!(error.contains(field), "unexpected error: {error}");
        }
    }

    #[test]
    #[ignore = "perf-contract schema selftest; run explicitly"]
    fn max_depth_requires_a_reason() {
        let source = VALID_CONTRACT.replace(
            "mode = \"exact\"\nvalue = 1\nwhy = \"stable test depth\"",
            "mode = \"max\"\nvalue = 1",
        );

        let error = parse_contract("unit", &source).unwrap_err();
        assert!(
            error.contains("mode=max requires a non-empty why"),
            "unexpected error: {error}"
        );
    }

    #[test]
    #[ignore = "perf-contract schema selftest; run explicitly"]
    fn every_counter_and_byte_map_must_be_complete() {
        for table in [
            "assert.gets",
            "assert.puts",
            "assert.get_bytes",
            "assert.put_bytes",
        ] {
            let mut contract = parsed_contract();
            match table {
                "assert.gets" => {
                    contract.assertions.gets.remove("cluster");
                }
                "assert.puts" => {
                    contract.assertions.puts.remove("cluster");
                }
                "assert.get_bytes" => {
                    contract.assertions.get_bytes.remove("cluster");
                }
                "assert.put_bytes" => {
                    contract.assertions.put_bytes.remove("cluster");
                }
                _ => unreachable!(),
            }
            let error = validate_contract("unit", &contract).unwrap_err();
            assert!(
                error.contains(table) && error.contains("missing \"cluster\""),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    #[ignore = "perf-contract schema selftest; run explicitly"]
    fn checked_in_phase1_contracts_pass_strict_validation() {
        for scenario in ["warm_query_strong", "cold_query_strong", "upsert_single"] {
            load_contract(scenario)
                .unwrap_or_else(|error| panic!("checked-in {scenario} contract failed: {error}"));
        }
    }

    #[test]
    #[ignore = "perf-contract schema selftest; run explicitly"]
    fn phase1_measures_require_their_anatomy_assertions() {
        let no_depth = VALID_CONTRACT.replace(
            "[assert.depth.put_get]\nmode = \"exact\"\nvalue = 1\nwhy = \"stable test depth\"\n\n",
            "",
        );
        let error = parse_contract("unit", &no_depth).unwrap_err();
        assert!(error.contains("assert.depth.put_get"));

        let no_history = VALID_CONTRACT.replace("[assert.put_keys]\n\"manifests/\" = 1\n", "");
        let error = parse_contract("unit", &no_history).unwrap_err();
        assert!(error.contains("assert.put_keys[\"manifests/\"]"));

        let query = VALID_CONTRACT.replace(
            "measure = { op = \"upsert\", batch = 1 }",
            "measure = { op = \"query\", consistency = \"strong\", top_k = 10, query_index = 0 }",
        );
        let error = parse_contract("unit", &query).unwrap_err();
        assert!(error.contains("assert.depth.get"));
    }

    #[test]
    #[ignore = "perf-contract capture selftest; run explicitly"]
    fn capture_preserves_depth_policy_and_records_actual_repeats() {
        let mut source = parsed_contract();
        source.run.repeats = 99;
        let measured = outcome(vec![repeat(0, 4), repeat(0, 4)]);
        let proposed = capture_contract(&source, &measured, "new-rev", "now");
        let exact = proposed.assertions.depth.get("put_get").unwrap();
        assert_eq!(exact.mode, DepthMode::Exact);
        assert_eq!(exact.value, 4);
        assert_eq!(proposed.run.repeats, 2);

        let depth = source.assertions.depth.get_mut("put_get").unwrap();
        depth.mode = DepthMode::Max;
        depth.why = Some("documented scheduler bound".to_string());
        let measured = outcome(vec![repeat(0, 4), repeat(0, 6)]);
        let proposed = capture_contract(&source, &measured, "new-rev", "now");
        let maximum = proposed.assertions.depth.get("put_get").unwrap();
        assert_eq!(maximum.mode, DepthMode::Max);
        assert_eq!(maximum.value, 6);
        assert_eq!(maximum.why.as_deref(), Some("documented scheduler bound"));
    }

    #[test]
    #[ignore = "perf-contract capture selftest; run explicitly"]
    #[should_panic(expected = "cannot capture exact depth")]
    fn capture_refuses_variable_exact_depth() {
        let source = parsed_contract();
        let measured = outcome(vec![repeat(0, 4), repeat(0, 5)]);
        let _ = capture_contract(&source, &measured, "new-rev", "now");
    }

    #[test]
    #[ignore = "perf-contract capture selftest; run explicitly"]
    fn capture_bands_only_serialization_variable_bytes() {
        let source = parsed_contract();
        let mut first = repeat(0, 4);
        let mut second = repeat(0, 4);
        set_get_bytes(&mut first, "manifest", 10);
        set_get_bytes(&mut second, "manifest", 12);
        let measured = outcome(vec![first, second]);

        let proposed = capture_contract(&source, &measured, "new-rev", "now");
        assert_eq!(
            proposed.assertions.get_bytes.get("manifest"),
            Some(&ByteBound::Band {
                min: Some(10),
                max: 12,
                why: "observed GET bytes variation across measured repeats".to_string(),
            })
        );
        assert!(matches!(
            proposed.assertions.get_bytes.get("total"),
            Some(ByteBound::Band {
                min: Some(10),
                max: 12,
                ..
            })
        ));
    }

    #[test]
    #[ignore = "perf-contract capture selftest; run explicitly"]
    #[should_panic(expected = "byte bands are limited to manifest, wal, other, and total")]
    fn capture_refuses_variable_bytes_for_stable_classes() {
        let source = parsed_contract();
        let mut first = repeat(0, 4);
        let mut second = repeat(0, 4);
        set_get_bytes(&mut first, "cluster", 10);
        set_get_bytes(&mut second, "cluster", 12);
        let measured = outcome(vec![first, second]);
        let _ = capture_contract(&source, &measured, "new-rev", "now");
    }
}
