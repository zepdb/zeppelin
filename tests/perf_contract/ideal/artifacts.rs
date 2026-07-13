//! Deterministic, side-effect-free artifacts for exhaustive S3 analysis.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::common::counting::ArtifactClass;
use crate::perf_contract::depth::{OpSpan, PhysicalRequest, SpanKind, SpanOutcome};
use crate::perf_contract::scenario::RepeatCounters;

use super::observe::{serial_get_chain, stable_ideal_key, SerialGetChain};

/// One normalized measurement used by both machine-readable and Markdown reports.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct IdealSample {
    pub scenario_id: String,
    pub serial_get_chain: SerialGetChain,
    pub total_get_ops: u64,
    pub total_get_bytes: u64,
    pub physical_verb_mode_totals: Vec<PhysicalModeTotal>,
    pub physical_operations: Vec<PhysicalOperation>,
}

/// Attempt and byte totals for one physical verb and semantic request mode.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct PhysicalModeTotal {
    pub verb: String,
    pub mode: String,
    pub class: ArtifactClass,
    pub ops: u64,
    pub bytes: u64,
}

/// One normalized ObjectStore adapter invocation, including non-critical work.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct PhysicalOperation {
    pub invocation_ordinal: u64,
    pub verb: String,
    pub request: PhysicalRequest,
    pub class: ArtifactClass,
    pub key: String,
    pub successful_bytes: u64,
    pub elapsed_us: u64,
    pub start_seq: u64,
    pub end_seq: u64,
    pub outcome: SpanOutcome,
}

/// Deterministic aggregate for every measured sample of one scenario.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ScenarioSummary {
    pub scenario_id: String,
    pub sample_count: u64,
    pub max_serial_get_depth: u32,
    pub min_get_ops: u64,
    pub max_get_ops: u64,
    pub min_get_bytes: u64,
    pub max_get_bytes: u64,
    pub distinct_normalized_cost_vector_count: u64,
    pub representative_worst_sample: IdealSample,
}

/// Stable, latency-free representation suitable for byte comparisons.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct NormalizedScenarioCost {
    pub scenario_id: String,
    pub serial_get_depth: u32,
    pub total_get_ops: u64,
    pub total_get_bytes: u64,
    pub physical_verb_mode_totals: Vec<PhysicalModeTotal>,
    pub serial_get_chain: Vec<NormalizedGetLink>,
    pub physical_operations: Vec<NormalizedPhysicalOperation>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct NormalizedGetLink {
    pub ordinal: u32,
    pub request: PhysicalRequest,
    pub class: ArtifactClass,
    pub key: String,
    pub bytes: u64,
    pub outcome: SpanOutcome,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct NormalizedPhysicalOperation {
    pub verb: String,
    pub request: PhysicalRequest,
    pub class: ArtifactClass,
    pub key: String,
    pub successful_bytes: u64,
    pub outcome: SpanOutcome,
}

impl IdealSample {
    /// Normalize one measured repeat and reconcile its aggregate GET census.
    #[must_use]
    pub(crate) fn from_repeat(scenario_id: impl Into<String>, repeat: &RepeatCounters) -> Self {
        let scenario_id = scenario_id.into();
        assert!(
            !scenario_id.trim().is_empty(),
            "ideal-analysis scenario ID cannot be empty"
        );

        let span_get_ops = repeat
            .spans
            .iter()
            .filter(|span| span.kind == SpanKind::Get)
            .count() as u64;
        let span_get_bytes = repeat
            .spans
            .iter()
            .filter(|span| span.kind == SpanKind::Get)
            .try_fold(0_u64, |total, span| total.checked_add(span.bytes))
            .expect("ideal-analysis GET byte total overflowed u64");
        assert_eq!(
            repeat.totals.get_ops, span_get_ops,
            "ideal-analysis GET adapter invocations disagree between counters and spans"
        );
        assert_eq!(
            repeat.totals.get_bytes, span_get_bytes,
            "ideal-analysis GET bytes disagree between counters and spans"
        );

        Self {
            scenario_id,
            serial_get_chain: serial_get_chain(&repeat.spans),
            total_get_ops: repeat.totals.get_ops,
            total_get_bytes: repeat.totals.get_bytes,
            physical_verb_mode_totals: physical_mode_totals(&repeat.spans),
            physical_operations: physical_operations(&repeat.spans),
        }
    }
}

/// Rank worst samples first with a complete, deterministic tie-break order.
#[must_use]
pub(crate) fn rank_samples(mut samples: Vec<IdealSample>) -> Vec<IdealSample> {
    samples.sort_by(|left, right| {
        right
            .serial_get_chain
            .depth
            .cmp(&left.serial_get_chain.depth)
            .then_with(|| right.total_get_ops.cmp(&left.total_get_ops))
            .then_with(|| right.total_get_bytes.cmp(&left.total_get_bytes))
            .then_with(|| left.scenario_id.cmp(&right.scenario_id))
    });
    samples
}

/// Collapse repeated measurements into one deterministic row per scenario.
#[must_use]
pub(crate) fn aggregate_scenario_samples(samples: Vec<IdealSample>) -> Vec<ScenarioSummary> {
    let mut grouped = BTreeMap::<String, Vec<IdealSample>>::new();
    for sample in samples {
        assert!(
            !sample.scenario_id.trim().is_empty(),
            "ideal-analysis scenario ID cannot be empty"
        );
        grouped
            .entry(sample.scenario_id.clone())
            .or_default()
            .push(sample);
    }

    grouped
        .into_iter()
        .map(|(scenario_id, samples)| summarize_scenario(scenario_id, samples))
        .collect()
}

/// Rank scenario rows by their worst observed GET cost, then stable ID.
#[must_use]
pub(crate) fn rank_scenario_summaries(mut summaries: Vec<ScenarioSummary>) -> Vec<ScenarioSummary> {
    summaries.sort_by(|left, right| {
        right
            .max_serial_get_depth
            .cmp(&left.max_serial_get_depth)
            .then_with(|| right.max_get_ops.cmp(&left.max_get_ops))
            .then_with(|| right.max_get_bytes.cmp(&left.max_get_bytes))
            .then_with(|| left.scenario_id.cmp(&right.scenario_id))
    });
    summaries
}

#[must_use]
pub(crate) fn normalized_scenario_costs(
    summaries: &[ScenarioSummary],
) -> Vec<NormalizedScenarioCost> {
    summaries
        .iter()
        .map(|summary| normalized_scenario_cost(&summary.representative_worst_sample))
        .collect()
}

fn summarize_scenario(scenario_id: String, samples: Vec<IdealSample>) -> ScenarioSummary {
    assert!(
        !samples.is_empty(),
        "cannot summarize an ideal-analysis scenario without samples"
    );
    assert!(
        samples
            .iter()
            .all(|sample| sample.scenario_id == scenario_id),
        "ideal-analysis scenario group contains a mismatched sample ID"
    );

    let representative_worst_sample = samples
        .iter()
        .min_by(|left, right| compare_representative_samples(left, right))
        .expect("nonempty ideal-analysis scenario lost its representative")
        .clone();
    let max_serial_get_depth = samples
        .iter()
        .map(|sample| sample.serial_get_chain.depth)
        .max()
        .expect("nonempty ideal-analysis scenario lost its maximum GET depth");
    let min_get_ops = samples
        .iter()
        .map(|sample| sample.total_get_ops)
        .min()
        .expect("nonempty ideal-analysis scenario lost its minimum GET ops");
    let max_get_ops = samples
        .iter()
        .map(|sample| sample.total_get_ops)
        .max()
        .expect("nonempty ideal-analysis scenario lost its maximum GET ops");
    let min_get_bytes = samples
        .iter()
        .map(|sample| sample.total_get_bytes)
        .min()
        .expect("nonempty ideal-analysis scenario lost its minimum GET bytes");
    let max_get_bytes = samples
        .iter()
        .map(|sample| sample.total_get_bytes)
        .max()
        .expect("nonempty ideal-analysis scenario lost its maximum GET bytes");
    let distinct_normalized_cost_vector_count = u64::try_from(
        samples
            .iter()
            .map(normalized_cost_vector_key)
            .collect::<BTreeSet<_>>()
            .len(),
    )
    .expect("ideal-analysis distinct cost-vector count overflowed u64");

    ScenarioSummary {
        scenario_id,
        sample_count: u64::try_from(samples.len())
            .expect("ideal-analysis sample count overflowed u64"),
        max_serial_get_depth,
        min_get_ops,
        max_get_ops,
        min_get_bytes,
        max_get_bytes,
        distinct_normalized_cost_vector_count,
        representative_worst_sample,
    }
}

fn compare_representative_samples(left: &IdealSample, right: &IdealSample) -> Ordering {
    right
        .serial_get_chain
        .depth
        .cmp(&left.serial_get_chain.depth)
        .then_with(|| right.total_get_ops.cmp(&left.total_get_ops))
        .then_with(|| right.total_get_bytes.cmp(&left.total_get_bytes))
        .then_with(|| normalized_chain_key(left).cmp(&normalized_chain_key(right)))
        .then_with(|| normalized_cost_vector_key(left).cmp(&normalized_cost_vector_key(right)))
        .then_with(|| {
            serde_json::to_string(left)
                .expect("serialize ideal-analysis representative candidate")
                .cmp(
                    &serde_json::to_string(right)
                        .expect("serialize ideal-analysis representative candidate"),
                )
        })
}

fn normalized_chain_key(sample: &IdealSample) -> String {
    let links = sample
        .serial_get_chain
        .links
        .iter()
        .map(|link| {
            (
                link.ordinal,
                link.kind,
                link.request,
                link.class,
                link.key.as_str(),
                link.bytes,
                link.outcome,
            )
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&(sample.serial_get_chain.depth, links))
        .expect("serialize ideal-analysis normalized GET chain")
}

fn normalized_cost_vector_key(sample: &IdealSample) -> String {
    serde_json::to_string(&normalized_scenario_cost(sample))
        .expect("serialize ideal-analysis normalized cost vector")
}

fn normalized_scenario_cost(sample: &IdealSample) -> NormalizedScenarioCost {
    let serial_get_chain = sample
        .serial_get_chain
        .links
        .iter()
        .map(|link| NormalizedGetLink {
            ordinal: link.ordinal,
            request: link.request,
            class: link.class,
            key: link.key.clone(),
            bytes: link.bytes,
            outcome: link.outcome,
        })
        .collect();
    let mut physical_operations = sample
        .physical_operations
        .iter()
        .map(|operation| NormalizedPhysicalOperation {
            verb: operation.verb.clone(),
            request: operation.request,
            class: operation.class,
            key: operation.key.clone(),
            successful_bytes: operation.successful_bytes,
            outcome: operation.outcome,
        })
        .collect::<Vec<_>>();
    physical_operations.sort_by(|left, right| {
        left.verb
            .cmp(&right.verb)
            .then_with(|| mode_name(left.request).cmp(mode_name(right.request)))
            .then_with(|| {
                serde_json::to_string(&left.request)
                    .expect("serialize physical request")
                    .cmp(
                        &serde_json::to_string(&right.request).expect("serialize physical request"),
                    )
            })
            .then_with(|| left.class.cmp(&right.class))
            .then_with(|| left.key.cmp(&right.key))
            .then_with(|| left.successful_bytes.cmp(&right.successful_bytes))
            .then_with(|| format!("{:?}", left.outcome).cmp(&format!("{:?}", right.outcome)))
    });
    NormalizedScenarioCost {
        scenario_id: sample.scenario_id.clone(),
        serial_get_depth: sample.serial_get_chain.depth,
        total_get_ops: sample.total_get_ops,
        total_get_bytes: sample.total_get_bytes,
        physical_verb_mode_totals: sample.physical_verb_mode_totals.clone(),
        serial_get_chain,
        physical_operations,
    }
}

fn physical_operations(spans: &[OpSpan]) -> Vec<PhysicalOperation> {
    let mut spans = spans.iter().collect::<Vec<_>>();
    spans.sort_by(|left, right| {
        left.start_seq
            .cmp(&right.start_seq)
            .then_with(|| left.end_seq.cmp(&right.end_seq))
            .then_with(|| verb_name(left.kind).cmp(verb_name(right.kind)))
            .then_with(|| left.key.cmp(&right.key))
    });
    spans
        .into_iter()
        .enumerate()
        .map(|(index, span)| PhysicalOperation {
            invocation_ordinal: u64::try_from(index + 1)
                .expect("ideal-analysis physical operation ordinal overflowed u64"),
            verb: verb_name(span.kind).to_string(),
            request: span.request,
            class: span.class,
            key: stable_ideal_key(&span.key),
            successful_bytes: if span.outcome == SpanOutcome::Success {
                span.bytes
            } else {
                0
            },
            elapsed_us: span
                .wall_end_us
                .checked_sub(span.wall_start_us)
                .expect("object-store span ended before it started"),
            start_seq: span.start_seq,
            end_seq: span.end_seq,
            outcome: span.outcome,
        })
        .collect()
}

fn physical_mode_totals(spans: &[OpSpan]) -> Vec<PhysicalModeTotal> {
    let mut totals = BTreeMap::<(&'static str, &'static str, ArtifactClass), (u64, u64)>::new();
    for span in spans {
        let total = totals
            .entry((verb_name(span.kind), mode_name(span.request), span.class))
            .or_default();
        total.0 = total
            .0
            .checked_add(1)
            .expect("ideal-analysis physical operation total overflowed u64");
        let successful_bytes = if span.outcome == SpanOutcome::Success {
            span.bytes
        } else {
            0
        };
        total.1 = total
            .1
            .checked_add(successful_bytes)
            .expect("ideal-analysis physical byte total overflowed u64");
    }
    totals
        .into_iter()
        .map(|((verb, mode, class), (ops, bytes))| PhysicalModeTotal {
            verb: verb.to_string(),
            mode: mode.to_string(),
            class,
            ops,
            bytes,
        })
        .collect()
}

fn verb_name(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Get => "get",
        SpanKind::Head => "head",
        SpanKind::Put => "put",
        SpanKind::List => "list",
        SpanKind::Copy => "copy",
        SpanKind::Delete => "delete",
    }
}

fn mode_name(request: PhysicalRequest) -> &'static str {
    match request {
        PhysicalRequest::GetFull => "get_full",
        PhysicalRequest::GetRange { .. } => "get_range",
        PhysicalRequest::GetSuffix { .. } => "get_suffix",
        PhysicalRequest::GetConditional => "get_conditional",
        PhysicalRequest::GetConditionalRange { .. } => "get_conditional_range",
        PhysicalRequest::GetConditionalSuffix { .. } => "get_conditional_suffix",
        PhysicalRequest::PutOverwrite => "put_overwrite",
        PhysicalRequest::PutCreate => "put_create",
        PhysicalRequest::PutUpdate => "put_update",
        PhysicalRequest::Head => "head",
        PhysicalRequest::ListRecursive => "list_recursive",
        PhysicalRequest::ListDelimiter => "list_delimiter",
        PhysicalRequest::CopyOverwrite => "copy_overwrite",
        PhysicalRequest::CopyIfAbsent => "copy_if_absent",
        PhysicalRequest::Delete => "delete",
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;
    use crate::common::counting::{ArtifactClass, ClassStats};
    use crate::perf_contract::depth::CriticalPath;

    fn chain_sample(id: &str, keys: &[&str], get_ops: u64, get_bytes: u64) -> IdealSample {
        IdealSample {
            scenario_id: id.to_string(),
            serial_get_chain: SerialGetChain {
                depth: u32::try_from(keys.len()).expect("test chain depth fits u32"),
                links: keys
                    .iter()
                    .enumerate()
                    .map(|(index, key)| super::super::observe::SerialGetLink {
                        ordinal: u32::try_from(index + 1).expect("test chain ordinal fits u32"),
                        kind: SpanKind::Get,
                        request: PhysicalRequest::GetFull,
                        class: ArtifactClass::Other,
                        key: (*key).to_string(),
                        bytes: 10,
                        elapsed_us: 100,
                        outcome: SpanOutcome::Success,
                        ok: true,
                    })
                    .collect(),
            },
            total_get_ops: get_ops,
            total_get_bytes: get_bytes,
            physical_verb_mode_totals: vec![PhysicalModeTotal {
                verb: "get".to_string(),
                mode: "get_full".to_string(),
                class: ArtifactClass::Other,
                ops: get_ops,
                bytes: get_bytes,
            }],
            physical_operations: Vec::new(),
        }
    }

    fn empty_chain(depth: u32) -> SerialGetChain {
        SerialGetChain {
            depth,
            links: Vec::new(),
        }
    }

    fn sample(id: &str, depth: u32, get_ops: u64, get_bytes: u64) -> IdealSample {
        IdealSample {
            scenario_id: id.to_string(),
            serial_get_chain: empty_chain(depth),
            total_get_ops: get_ops,
            total_get_bytes: get_bytes,
            physical_verb_mode_totals: Vec::new(),
            physical_operations: Vec::new(),
        }
    }

    fn span(
        kind: SpanKind,
        request: PhysicalRequest,
        key: &str,
        start_seq: u64,
        end_seq: u64,
        bytes: u64,
    ) -> OpSpan {
        OpSpan {
            kind,
            request,
            class: ArtifactClass::Other,
            key: key.to_string(),
            start_seq,
            end_seq,
            bytes,
            ok: true,
            wall_start_us: start_seq * 10,
            wall_end_us: end_seq * 10,
            outcome: SpanOutcome::Success,
        }
    }

    fn repeat(spans: Vec<OpSpan>, get_ops: u64, get_bytes: u64) -> RepeatCounters {
        let empty_path = CriticalPath {
            depth: 0,
            chain: Vec::new(),
        };
        RepeatCounters {
            classes: BTreeMap::new(),
            totals: ClassStats {
                get_ops,
                get_bytes,
                put_ops: 1,
                put_bytes: 7,
            },
            get_path: empty_path.clone(),
            put_get_path: empty_path.clone(),
            spans,
            op_counts: BTreeMap::new(),
            labeled: Vec::new(),
            wall_elapsed_us: 0,
            response_cutoff_us: 0,
            raw_get_path: empty_path.clone(),
            raw_put_get_path: empty_path,
        }
    }

    #[test]
    fn ranking_uses_every_literal_tie_break_in_order() {
        let ranked = rank_samples(vec![
            sample("z-id", 2, 10, 100),
            sample("shallower", 1, 99, 999),
            sample("a-id", 2, 10, 100),
            sample("more-bytes", 2, 10, 101),
            sample("more-ops", 2, 11, 1),
            sample("deeper", 3, 1, 1),
        ]);

        assert_eq!(
            ranked
                .iter()
                .map(|sample| sample.scenario_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "deeper",
                "more-ops",
                "more-bytes",
                "a-id",
                "z-id",
                "shallower"
            ]
        );
    }

    #[test]
    fn normalization_aggregates_modes_and_has_stable_json() {
        let spans = vec![
            span(
                SpanKind::Get,
                PhysicalRequest::GetFull,
                "ns/manifest.json",
                0,
                1,
                10,
            ),
            span(
                SpanKind::Get,
                PhysicalRequest::GetRange {
                    start: 0,
                    end: Some(5),
                },
                "ns/segments/01/cluster_0.bin",
                1,
                2,
                5,
            ),
            span(
                SpanKind::Put,
                PhysicalRequest::PutCreate,
                "ns/meta.json",
                2,
                3,
                7,
            ),
        ];
        let normalized = IdealSample::from_repeat("alpha", &repeat(spans, 2, 15));

        assert_eq!(normalized.serial_get_chain.depth, 2);
        assert_eq!(normalized.physical_operations.len(), 3);
        assert_eq!(normalized.physical_operations[0].key, "manifest.json");
        assert_eq!(normalized.physical_operations[1].key, "cluster_<index>.bin");
        assert_eq!(normalized.physical_operations[2].verb, "put");
        assert_eq!(
            normalized.physical_verb_mode_totals,
            vec![
                PhysicalModeTotal {
                    verb: "get".to_string(),
                    mode: "get_full".to_string(),
                    class: ArtifactClass::Other,
                    ops: 1,
                    bytes: 10,
                },
                PhysicalModeTotal {
                    verb: "get".to_string(),
                    mode: "get_range".to_string(),
                    class: ArtifactClass::Other,
                    ops: 1,
                    bytes: 5,
                },
                PhysicalModeTotal {
                    verb: "put".to_string(),
                    mode: "put_create".to_string(),
                    class: ArtifactClass::Other,
                    ops: 1,
                    bytes: 7,
                },
            ]
        );
        assert_eq!(
            serde_json::to_string(&sample("stable", 4, 7, 99)).expect("serialize ideal sample"),
            r#"{"scenario_id":"stable","serial_get_chain":{"depth":4,"links":[]},"total_get_ops":7,"total_get_bytes":99,"physical_verb_mode_totals":[],"physical_operations":[]}"#
        );
    }

    #[test]
    #[should_panic(expected = "GET adapter invocations disagree between counters and spans")]
    fn normalization_fails_loudly_when_counters_and_spans_diverge() {
        let spans = vec![span(
            SpanKind::Get,
            PhysicalRequest::GetFull,
            "ns/manifest.json",
            0,
            1,
            10,
        )];

        let _ = IdealSample::from_repeat("broken", &repeat(spans, 2, 10));
    }

    #[test]
    fn physical_totals_count_invocations_but_only_successful_bytes() {
        let mut rejected = span(
            SpanKind::Put,
            PhysicalRequest::PutCreate,
            "ns/meta.json",
            0,
            1,
            77,
        );
        rejected.outcome = SpanOutcome::Precondition;
        rejected.ok = false;

        assert_eq!(
            physical_mode_totals(&[rejected]),
            vec![PhysicalModeTotal {
                verb: "put".to_string(),
                mode: "put_create".to_string(),
                class: ArtifactClass::Other,
                ops: 1,
                bytes: 0,
            }]
        );
    }

    #[test]
    fn rank_comparator_is_total_for_distinct_scenario_ids() {
        let left = sample("a", 1, 2, 3);
        let right = sample("b", 1, 2, 3);
        let ranked = rank_samples(vec![right, left]);

        assert_eq!(ranked[0].scenario_id, "a");
        assert_eq!(ranked[1].scenario_id, "b");
        assert_ne!(
            ranked[0].scenario_id.cmp(&ranked[1].scenario_id),
            Ordering::Equal
        );
    }

    #[test]
    fn scenario_aggregation_collapses_samples_and_picks_normalized_chain() {
        let summaries = aggregate_scenario_samples(vec![
            chain_sample("alpha", &["z.bin"], 7, 70),
            chain_sample("alpha", &["a.bin"], 7, 70),
            chain_sample("alpha", &["a.bin"], 3, 30),
            chain_sample("beta", &["only.bin"], 2, 20),
        ]);

        assert_eq!(summaries.len(), 2);
        let alpha = summaries
            .iter()
            .find(|summary| summary.scenario_id == "alpha")
            .expect("alpha summary");
        assert_eq!(alpha.sample_count, 3);
        assert_eq!(alpha.max_serial_get_depth, 1);
        assert_eq!((alpha.min_get_ops, alpha.max_get_ops), (3, 7));
        assert_eq!((alpha.min_get_bytes, alpha.max_get_bytes), (30, 70));
        assert_eq!(alpha.distinct_normalized_cost_vector_count, 3);
        assert_eq!(
            alpha.representative_worst_sample.serial_get_chain.links[0].key,
            "a.bin"
        );
    }

    #[test]
    fn scenario_summary_ranking_uses_worst_cost_then_id() {
        let summaries = aggregate_scenario_samples(vec![
            chain_sample("z-id", &["a", "b"], 10, 100),
            chain_sample("a-id", &["a", "b"], 10, 100),
            chain_sample("more-bytes", &["a", "b"], 10, 101),
            chain_sample("more-ops", &["a", "b"], 11, 1),
            chain_sample("deeper", &["a", "b", "c"], 1, 1),
            chain_sample("shallower", &["a"], 99, 999),
        ]);
        let ranked = rank_scenario_summaries(summaries);

        assert_eq!(
            ranked
                .iter()
                .map(|summary| summary.scenario_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "deeper",
                "more-ops",
                "more-bytes",
                "a-id",
                "z-id",
                "shallower"
            ]
        );
    }

    #[test]
    fn single_sample_summary_has_stable_json() {
        let summary = aggregate_scenario_samples(vec![sample("stable", 0, 7, 99)])
            .pop()
            .expect("stable scenario summary");

        assert_eq!(
            serde_json::to_string(&summary).expect("serialize scenario summary"),
            r#"{"scenario_id":"stable","sample_count":1,"max_serial_get_depth":0,"min_get_ops":7,"max_get_ops":7,"min_get_bytes":99,"max_get_bytes":99,"distinct_normalized_cost_vector_count":1,"representative_worst_sample":{"scenario_id":"stable","serial_get_chain":{"depth":0,"links":[]},"total_get_ops":7,"total_get_bytes":99,"physical_verb_mode_totals":[],"physical_operations":[]}}"#
        );
    }

    #[test]
    fn normalized_cost_ignores_latency_and_parallel_observation_order() {
        let mut left = chain_sample("stable", &["manifest.json"], 1, 10);
        left.physical_operations = vec![
            PhysicalOperation {
                invocation_ordinal: 1,
                verb: "get".to_string(),
                request: PhysicalRequest::GetFull,
                class: ArtifactClass::Manifest,
                key: "manifest.json".to_string(),
                successful_bytes: 10,
                elapsed_us: 1,
                start_seq: 0,
                end_seq: 3,
                outcome: SpanOutcome::Success,
            },
            PhysicalOperation {
                invocation_ordinal: 2,
                verb: "get".to_string(),
                request: PhysicalRequest::GetRange {
                    start: 0,
                    end: Some(4),
                },
                class: ArtifactClass::Cluster,
                key: "cluster_<index>.bin".to_string(),
                successful_bytes: 4,
                elapsed_us: 2,
                start_seq: 1,
                end_seq: 2,
                outcome: SpanOutcome::Success,
            },
        ];
        let mut right = left.clone();
        right.serial_get_chain.links[0].elapsed_us = 9_999;
        right.physical_operations.reverse();
        for (index, operation) in right.physical_operations.iter_mut().enumerate() {
            operation.invocation_ordinal =
                u64::try_from(index + 1).expect("test invocation ordinal fits u64");
            operation.elapsed_us = 8_888;
            operation.start_seq = 10 + index as u64;
            operation.end_seq = 20 + index as u64;
        }

        assert_eq!(
            normalized_scenario_cost(&left),
            normalized_scenario_cost(&right)
        );
    }
}
