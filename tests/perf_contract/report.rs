//! Run-scoped artifacts and the complete performance-contract report.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::common::counting::ClassStats;

use super::contract::CostViolation;
use super::depth::{CriticalPath, DepthStage, DepthTracker, OpSpan, SpanKind};
use super::scenario::{RepeatCounters, ScenarioOutcome};
use super::security::SecurityMeasurement;
use super::PerfEnv;

const SOUNDNESS_PRECONDITION: &str =
    "exactly one measured operation in flight; no unrelated background compaction, GC, or hydration";

/// All artifacts produced by one performance-contract entry invocation.
#[derive(Debug)]
pub struct RunArtifacts {
    root: PathBuf,
    run_id: String,
    entry: String,
    git_rev: String,
    test_backend: Option<String>,
    scenario_labels: Vec<String>,
    scenarios: Mutex<BTreeMap<String, ScenarioReport>>,
    proposed_diffs: Mutex<BTreeMap<String, Vec<ProposedDiff>>>,
}

#[derive(Debug, Serialize)]
struct RunMetadata<'a> {
    run_id: &'a str,
    entry: &'a str,
    git_rev: &'a str,
    backend: &'a Option<String>,
    env: PerfEnvEcho<'a>,
    scenario_labels: &'a [String],
    depth_soundness_precondition: DepthSoundnessPrecondition,
}

#[derive(Debug, Serialize)]
struct PerfEnvEcho<'a> {
    #[serde(rename = "TEST_BACKEND")]
    test_backend: &'a Option<String>,
    #[serde(rename = "ZEPPELIN_PERF_SCENARIOS")]
    scenarios: &'a [String],
    #[serde(rename = "ZEPPELIN_PERF_ARTIFACTS")]
    artifact_root: &'a str,
    #[serde(rename = "ZEPPELIN_PERF_CAPTURE")]
    capture: bool,
    #[serde(rename = "ZEPPELIN_PERF_SELFTEST")]
    selftest: &'a Option<String>,
    #[serde(rename = "ZEPPELIN_PERF_REPEATS")]
    repeats: usize,
}

#[derive(Debug, Serialize)]
struct DepthSoundnessPrecondition {
    exactly_one_client_request_in_flight: bool,
    spawn_compaction_loop: bool,
    background_gc: bool,
    background_hydration: bool,
    description: &'static str,
}

#[derive(Debug, Serialize)]
struct CountersArtifact<'a> {
    repeats: Vec<CounterRepeat<'a>>,
}

#[derive(Debug, Serialize)]
struct CounterRepeat<'a> {
    repeat: usize,
    classes: &'a BTreeMap<String, ClassStats>,
    totals: &'a ClassStats,
}

#[derive(Debug, Clone, Serialize)]
struct DepthArtifact {
    excludes: [&'static str; 4],
    chain_key_format: &'static str,
    repeats: Vec<DepthRepeat>,
}

#[derive(Debug, Clone, Serialize)]
struct DepthRepeat {
    repeat: usize,
    get: DepthSummary,
    put_get: DepthSummary,
    post_response_ops: usize,
}

#[derive(Debug, Clone, Serialize)]
struct DepthSummary {
    depth: u32,
    chain: Vec<DepthLink>,
    stages: Vec<DepthStage>,
}

#[derive(Debug, Clone, Serialize)]
struct DepthLink {
    class: String,
    key: String,
}

#[derive(Debug, Serialize)]
struct SpanLine<'a> {
    repeat: usize,
    #[serde(flatten)]
    span: &'a OpSpan,
}

#[derive(Debug, Clone)]
struct ScenarioReport {
    passed: bool,
    configuration_error: Option<String>,
    violations: Vec<CostViolation>,
    repeats: Vec<ReportRepeat>,
    security: Option<SecurityMeasurement>,
}

#[derive(Debug, Clone)]
struct ReportRepeat {
    classes: BTreeMap<String, ClassStats>,
    totals: ClassStats,
    get: DepthSummary,
    put_get: DepthSummary,
    post_response: Vec<PostResponseOp>,
}

#[derive(Debug, Clone)]
struct PostResponseOp {
    kind: String,
    class: String,
    key: String,
}

#[derive(Debug, Clone)]
struct ProposedDiff {
    field: String,
    old: String,
    new: String,
}

impl RunArtifacts {
    /// Create a unique run directory and write its immutable `run.json`.
    #[must_use]
    pub fn create(env: &PerfEnv, entry: &str, scenario_labels: &[String]) -> Self {
        assert!(
            !entry.is_empty(),
            "performance-contract entry cannot be empty"
        );
        assert!(
            !scenario_labels.is_empty(),
            "performance-contract run must include at least one scenario"
        );
        validate_unique_labels(scenario_labels);

        fs::create_dir_all(&env.artifact_root).unwrap_or_else(|error| {
            panic!(
                "failed to create performance-contract artifact root {}: {error}",
                env.artifact_root.display()
            )
        });
        let (root, run_id) = create_unique_run_dir(&env.artifact_root);
        let git_rev = git_rev();
        let test_backend = optional_env("TEST_BACKEND");
        let artifact_root = env
            .artifact_root
            .to_str()
            .expect("ZEPPELIN_PERF_ARTIFACTS path must be valid UTF-8");

        let artifacts = Self {
            root,
            run_id,
            entry: entry.to_string(),
            git_rev,
            test_backend,
            scenario_labels: scenario_labels.to_vec(),
            scenarios: Mutex::new(BTreeMap::new()),
            proposed_diffs: Mutex::new(BTreeMap::new()),
        };
        let metadata = RunMetadata {
            run_id: &artifacts.run_id,
            entry: &artifacts.entry,
            git_rev: &artifacts.git_rev,
            backend: &artifacts.test_backend,
            env: PerfEnvEcho {
                test_backend: &artifacts.test_backend,
                scenarios: &env.scenarios,
                artifact_root,
                capture: env.capture,
                selftest: &env.selftest,
                repeats: env.repeats,
            },
            scenario_labels: &artifacts.scenario_labels,
            depth_soundness_precondition: DepthSoundnessPrecondition {
                exactly_one_client_request_in_flight: true,
                spawn_compaction_loop: false,
                background_gc: false,
                background_hydration: false,
                description: SOUNDNESS_PRECONDITION,
            },
        };
        write_json(artifacts.root.join("run.json"), &metadata);
        artifacts
    }

    /// Return the unique directory containing this run's artifacts.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the revision recorded in `run.json` for capture provenance.
    #[must_use]
    pub fn git_rev(&self) -> &str {
        &self.git_rev
    }

    /// Write all deterministic and diagnostic artifacts for one scenario.
    pub fn write_scenario(
        &self,
        label: &str,
        outcome: &ScenarioOutcome,
        violations: &[CostViolation],
    ) {
        validate_component(label, "scenario label");
        assert!(
            self.scenario_labels.iter().any(|known| known == label),
            "cannot write undeclared performance-contract scenario {label:?}"
        );
        assert!(
            !outcome.per_repeat.is_empty(),
            "scenario {label:?} produced no measured repeats"
        );

        let mut reports = self
            .scenarios
            .lock()
            .expect("performance-contract report mutex poisoned");
        assert!(
            !reports.contains_key(label),
            "scenario artifacts already written for {label:?}"
        );

        let scenario_dir = self.root.join(label);
        fs::create_dir(&scenario_dir).unwrap_or_else(|error| {
            panic!(
                "failed to create scenario artifact dir {}: {error}",
                scenario_dir.display()
            )
        });

        let counters = CountersArtifact {
            repeats: outcome
                .per_repeat
                .iter()
                .enumerate()
                .map(|(repeat, counters)| CounterRepeat {
                    repeat,
                    classes: &counters.classes,
                    totals: &counters.totals,
                })
                .collect(),
        };
        write_json(scenario_dir.join("counters.json"), &counters);

        write_spans(&scenario_dir.join("spans.jsonl"), &outcome.per_repeat);
        let depth = depth_artifact(&outcome.per_repeat);
        write_json(scenario_dir.join("depth.json"), &depth);
        write_json(scenario_dir.join("expected.json"), &outcome.expected);
        if let Some(security) = &outcome.security {
            write_json(scenario_dir.join("security.json"), security);
        }
        if !violations.is_empty() {
            write_json(scenario_dir.join("violations.json"), violations);
        }

        reports.insert(
            label.to_string(),
            ScenarioReport {
                passed: violations.is_empty(),
                configuration_error: None,
                violations: violations.to_vec(),
                repeats: report_repeats(&outcome.per_repeat),
                security: outcome.security.clone(),
            },
        );
    }

    /// Record a checked-in contract configuration failure for the final report.
    pub fn write_contract_error(&self, label: &str, error: String) {
        validate_component(label, "scenario label");
        assert!(!error.trim().is_empty(), "contract error cannot be empty");
        let mut reports = self
            .scenarios
            .lock()
            .expect("performance-contract report mutex poisoned");
        assert!(
            !reports.contains_key(label),
            "scenario report already written for {label:?}"
        );
        reports.insert(
            label.to_string(),
            ScenarioReport {
                passed: false,
                configuration_error: Some(error),
                violations: Vec::new(),
                repeats: Vec::new(),
                security: None,
            },
        );
    }

    /// Write a proposed human-reviewed contract below this run directory.
    pub fn write_proposed(&self, name: &str, toml: &str) {
        validate_component(name, "proposed contract name");
        assert!(
            !toml.is_empty(),
            "proposed contract {name:?} cannot be empty"
        );
        let proposed = self.root.join("proposed");
        fs::create_dir_all(&proposed).unwrap_or_else(|error| {
            panic!(
                "failed to create proposed-contract dir {}: {error}",
                proposed.display()
            )
        });
        let path = proposed.join(format!("{name}.toml"));
        write_text(&path, toml, "proposed contract");
        let checked_in = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/perf_contract/contracts")
            .join(format!("{name}.toml"));
        let source = fs::read_to_string(&checked_in).unwrap_or_else(|error| {
            panic!(
                "failed to read checked-in contract {} for proposal diff: {error}",
                checked_in.display()
            )
        });
        let diffs = diff_toml(&source, toml);
        let mut proposed_diffs = self
            .proposed_diffs
            .lock()
            .expect("performance-contract proposal mutex poisoned");
        assert!(
            proposed_diffs.insert(name.to_string(), diffs).is_none(),
            "proposal diff already recorded for {name:?}"
        );
    }

    /// Write the complete Markdown report for all declared scenarios.
    pub fn write_report(&self) -> PathBuf {
        let scenarios = self
            .scenarios
            .lock()
            .expect("performance-contract report mutex poisoned");
        assert_all_scenarios_written(&self.scenario_labels, &scenarios);
        let report = build_report(self, &scenarios);
        let path = self.root.join("report.md");
        write_text(&path, &report, "performance-contract report");
        path
    }

    /// Write the runner-generated depth-stability study as Markdown.
    pub fn write_depth_stability(&self, markdown: &str) {
        assert!(
            !markdown.is_empty(),
            "depth-stability report cannot be empty"
        );
        let path = self.root.join("depth-stability.md");
        write_text(&path, markdown, "depth-stability report");
    }

    /// Write the deterministic Tier 2 table and append it to `report.md`.
    pub fn write_whatif(&self, markdown: &str) -> PathBuf {
        assert!(
            !markdown.trim().is_empty(),
            "what-if report cannot be empty"
        );
        let path = self.root.join("whatif.md");
        write_text(&path, markdown, "what-if report");
        append_report_section(&self.root.join("report.md"), markdown, "Tier 2 what-if");
        path
    }

    /// Write Tier 3 advisory tables plus the deterministic serial injection
    /// ledger, then append the table to the run report.
    pub fn write_latency_validation<T: Serialize>(
        &self,
        markdown: &str,
        ledger: &T,
    ) -> (PathBuf, PathBuf) {
        assert!(
            !markdown.trim().is_empty(),
            "latency-validation report cannot be empty"
        );
        let validation_path = self.root.join("latency-validation.md");
        write_text(
            &validation_path,
            markdown,
            "Tier 3 latency-validation report",
        );
        let ledger_path = self.root.join("latency-ledger.json");
        write_json(ledger_path.clone(), ledger);
        append_report_section(
            &self.root.join("report.md"),
            markdown,
            "Tier 3 latency validation",
        );
        (validation_path, ledger_path)
    }
}

fn depth_artifact(repeats: &[RepeatCounters]) -> DepthArtifact {
    DepthArtifact {
        excludes: ["wall_start_us", "wall_end_us", "start_seq", "end_seq"],
        chain_key_format: "stable artifact filename templates; raw keys are in spans.jsonl",
        repeats: repeats
            .iter()
            .enumerate()
            .map(|(repeat, counters)| DepthRepeat {
                repeat,
                get: summarize_depth(
                    &counters.get_path,
                    DepthTracker::stages(
                        &counters.spans,
                        &[SpanKind::Get, SpanKind::Head],
                        Some(counters.response_cutoff_us),
                    ),
                ),
                put_get: summarize_depth(
                    &counters.put_get_path,
                    DepthTracker::stages(
                        &counters.spans,
                        &[SpanKind::Get, SpanKind::Head, SpanKind::Put],
                        Some(counters.response_cutoff_us),
                    ),
                ),
                post_response_ops: post_response_ops(counters).len(),
            })
            .collect(),
    }
}

fn report_repeats(repeats: &[RepeatCounters]) -> Vec<ReportRepeat> {
    repeats
        .iter()
        .map(|counters| ReportRepeat {
            classes: counters.classes.clone(),
            totals: counters.totals,
            get: summarize_depth(
                &counters.get_path,
                DepthTracker::stages(
                    &counters.spans,
                    &[SpanKind::Get, SpanKind::Head],
                    Some(counters.response_cutoff_us),
                ),
            ),
            put_get: summarize_depth(
                &counters.put_get_path,
                DepthTracker::stages(
                    &counters.spans,
                    &[SpanKind::Get, SpanKind::Head, SpanKind::Put],
                    Some(counters.response_cutoff_us),
                ),
            ),
            post_response: post_response_ops(counters),
        })
        .collect()
}

fn post_response_ops(counters: &RepeatCounters) -> Vec<PostResponseOp> {
    counters
        .spans
        .iter()
        .filter(|span| span.wall_start_us > counters.response_cutoff_us)
        .map(|span| PostResponseOp {
            kind: format!("{:?}", span.kind).to_lowercase(),
            class: span.class.name().to_string(),
            key: stable_depth_key(&span.key),
        })
        .collect()
}

fn summarize_depth(path: &CriticalPath, stages: Vec<DepthStage>) -> DepthSummary {
    DepthSummary {
        depth: path.depth,
        chain: path
            .chain
            .iter()
            .map(|span| DepthLink {
                class: span.class.name().to_string(),
                // Raw keys, including run-specific IDs, remain in spans.jsonl.
                key: stable_depth_key(&span.key),
            })
            .collect(),
        stages,
    }
}

fn write_spans(path: &Path, repeats: &[RepeatCounters]) {
    let file = File::create_new(path)
        .unwrap_or_else(|error| panic!("failed to create spans file {}: {error}", path.display()));
    let mut writer = BufWriter::new(file);
    for (repeat, counters) in repeats.iter().enumerate() {
        for span in &counters.spans {
            serde_json::to_writer(&mut writer, &SpanLine { repeat, span }).unwrap_or_else(
                |error| panic!("failed to serialize span into {}: {error}", path.display()),
            );
            writer.write_all(b"\n").unwrap_or_else(|error| {
                panic!("failed to write span into {}: {error}", path.display())
            });
        }
    }
    writer
        .flush()
        .unwrap_or_else(|error| panic!("failed to flush spans file {}: {error}", path.display()));
}

fn build_report(artifacts: &RunArtifacts, scenarios: &BTreeMap<String, ScenarioReport>) -> String {
    let passed_count = scenarios
        .values()
        .filter(|scenario| scenario.passed)
        .count();
    let failed_count = scenarios.len() - passed_count;
    let passed = failed_count == 0;
    let mut out = String::new();
    out.push_str("# Zeppelin Performance Contract Report\n\n");
    out.push_str(&format!("- run: `{}`\n", artifacts.run_id));
    out.push_str(&format!("- entry: `{}`\n", markdown_cell(&artifacts.entry)));
    out.push_str(&format!("- git rev: `{}`\n", artifacts.git_rev));
    out.push_str(&format!(
        "- TEST_BACKEND: `{}`\n",
        artifacts.test_backend.as_deref().unwrap_or("unset")
    ));
    out.push_str(&format!("- scenarios run: {}\n", scenarios.len()));
    out.push_str(&format!("- scenarios passed: {passed_count}\n"));
    out.push_str(&format!("- scenarios failed: {failed_count}\n"));
    out.push_str(&format!(
        "- status: **{}**\n",
        if passed { "PASS" } else { "FAIL" }
    ));
    out.push_str(&format!(
        "- depth soundness: {}\n\n",
        SOUNDNESS_PRECONDITION
    ));
    out.push_str(
        "> CI command: `TEST_BACKEND=minio cargo test --release --test perf_contract_tests contracts -- --ignored`. `--release` is required: the security latency budgets are release-calibrated and a debug build misses them by an order of magnitude. Capture is never run in CI.\n\n",
    );

    let configuration_failures = scenarios
        .iter()
        .filter_map(|(label, scenario)| {
            scenario
                .configuration_error
                .as_ref()
                .map(|error| (label, error))
        })
        .collect::<Vec<_>>();
    if !configuration_failures.is_empty() {
        out.push_str("## Contract Configuration Failures\n\n");
        out.push_str("These checked-in contracts were rejected before execution.\n\n");
        for (label, error) in configuration_failures {
            out.push_str(&format!(
                "- **`{}`**: {}\n",
                markdown_cell(label),
                markdown_cell(error)
            ));
        }
        out.push('\n');
    }

    out.push_str("## Scenarios\n\n");
    out.push_str(
        "| scenario | status | GET depth + chain | GET ops by class | PUT ops by class | GET bytes | PUT bytes | delta vs baseline |\n",
    );
    out.push_str("| --- | --- | --- | --- | --- | ---: | ---: | --- |\n");
    for label in &artifacts.scenario_labels {
        let scenario = &scenarios[label];
        if let Some(first) = scenario.repeats.first() {
            out.push_str(&format!(
                "| `{}` | {} | {} | {} | {} | {} | {} | {} |\n",
                markdown_cell(label),
                if scenario.passed { "PASS" } else { "FAIL" },
                format_depth(&first.get),
                format_class_ops(&first.classes, true),
                format_class_ops(&first.classes, false),
                first.totals.get_bytes,
                first.totals.put_bytes,
                format_violation_deltas(&scenario.violations),
            ));
        } else {
            out.push_str(&format!(
                "| `{}` | FAIL | configuration error | n/a | n/a | 0 | 0 | {} |\n",
                markdown_cell(label),
                markdown_cell(
                    scenario
                        .configuration_error
                        .as_deref()
                        .expect("scenario without repeats must be a configuration failure")
                )
            ));
        }
    }
    out.push('\n');

    out.push_str("## Object-Store Totals\n\n");
    out.push_str("| class | get_ops | get_bytes | put_ops | put_bytes |\n");
    out.push_str("| --- | ---: | ---: | ---: | ---: |\n");
    let totals = aggregate_class_totals(scenarios);
    for (class, stats) in &totals {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} |\n",
            markdown_cell(class),
            stats.get_ops,
            stats.get_bytes,
            stats.put_ops,
            stats.put_bytes,
        ));
    }
    let grand = sum_class_stats(totals.values().copied());
    out.push_str(&format!(
        "| **TOTAL** | **{}** | **{}** | **{}** | **{}** |\n\n",
        grand.get_ops, grand.get_bytes, grand.put_ops, grand.put_bytes
    ));

    for label in &artifacts.scenario_labels {
        let scenario = &scenarios[label];
        out.push_str(&format!("## `{}`\n\n", markdown_cell(label)));
        out.push_str(&format!(
            "- status: **{}**\n",
            if scenario.passed { "PASS" } else { "FAIL" }
        ));
        if let Some(error) = &scenario.configuration_error {
            out.push_str(&format!(
                "- configuration error: **{}**\n\n",
                markdown_cell(error)
            ));
            continue;
        }
        let first = scenario
            .repeats
            .first()
            .expect("executed scenario report must have at least one repeat");
        out.push_str(&format!("- violations: {}\n", scenario.violations.len()));
        out.push_str(&format!("- measured repeats: {}\n", scenario.repeats.len()));
        out.push_str(&format!("- GET path: {}\n", format_depth(&first.get)));
        out.push_str(&format!(
            "- PUT+GET path: {}\n",
            format_depth(&first.put_get)
        ));
        out.push_str(&format!(
            "- delta vs baseline: {}\n",
            format_violation_deltas(&scenario.violations)
        ));
        if let Some(security) = &scenario.security {
            out.push_str(&format!(
                "- authn+authz p50 delta: {} ns\n",
                security.authn_authz_p50_delta_ns
            ));
            if let Some(delegated) = security.delegated_authn_authz_p50_delta_ns {
                out.push_str(&format!(
                    "- delegated-token authn+authz p50 delta: {delegated} ns\n"
                ));
            }
            out.push_str(&format!(
                "- object-store delta vs `{}`: GET {:+}, PUT {:+}\n",
                markdown_cell(&security.baseline_scenario),
                security.added_get_ops,
                security.added_put_ops
            ));
            if let (
                Some(samples),
                Some(caller_filtered_p50_ns),
                Some(policy_filtered_p50_ns),
                Some(regression_basis_points),
            ) = (
                security.paired_query_samples,
                security.caller_filtered_query_p50_ns,
                security.policy_filtered_query_p50_ns,
                security.query_p50_regression_basis_points,
            ) {
                out.push_str(&format!(
                    "- paired filtered-query samples: {samples}; caller p50: {caller_filtered_p50_ns} ns; policy p50: {policy_filtered_p50_ns} ns; regression: {:.2}%\n",
                    regression_basis_points as f64 / 100.0,
                ));
            }
        }
        out.push('\n');
        out.push_str("### Object-Store Totals\n\n");
        out.push_str("| class | get_ops | get_bytes | put_ops | put_bytes |\n");
        out.push_str("| --- | ---: | ---: | ---: | ---: |\n");
        for (class, stats) in &first.classes {
            out.push_str(&format!(
                "| `{}` | {} | {} | {} | {} |\n",
                markdown_cell(class),
                stats.get_ops,
                stats.get_bytes,
                stats.put_ops,
                stats.put_bytes,
            ));
        }
        out.push_str(&format!(
            "| **TOTAL** | **{}** | **{}** | **{}** | **{}** |\n\n",
            first.totals.get_ops,
            first.totals.get_bytes,
            first.totals.put_ops,
            first.totals.put_bytes,
        ));
        if !scenario.violations.is_empty() {
            out.push_str("### Violations\n\n");
            for violation in &scenario.violations {
                let encoded = serde_json::to_string(violation)
                    .expect("CostViolation must serialize into the report");
                out.push_str(&format!("- `{}`\n", markdown_cell(&encoded)));
            }
            out.push('\n');
        }
    }

    out.push_str("## Proposed Re-baselines\n\n");
    let proposed_diffs = artifacts
        .proposed_diffs
        .lock()
        .expect("performance-contract proposal mutex poisoned");
    if proposed_diffs.is_empty() {
        out.push_str("Capture was not requested; no proposal was generated.\n\n");
    } else {
        out.push_str("Proposals are run artifacts only and require human approval.\n\n");
        for label in &artifacts.scenario_labels {
            if let Some(diffs) = proposed_diffs.get(label) {
                out.push_str(&format!("### `{}`\n\n", markdown_cell(label)));
                if diffs.is_empty() {
                    out.push_str("No fields changed.\n\n");
                } else {
                    for diff in diffs {
                        out.push_str(&format!(
                            "- `{}`: `{}` -> `{}`\n",
                            markdown_cell(&diff.field),
                            markdown_cell(&diff.old),
                            markdown_cell(&diff.new)
                        ));
                    }
                    out.push('\n');
                }
            }
        }
    }

    out.push_str("## Post-response Ops\n\n");
    let mut excluded = 0usize;
    for label in &artifacts.scenario_labels {
        for (repeat, measured) in scenarios[label].repeats.iter().enumerate() {
            for operation in &measured.post_response {
                excluded += 1;
                out.push_str(&format!(
                    "- `{}` repeat {}: {} {} `{}`\n",
                    markdown_cell(label),
                    repeat,
                    markdown_cell(&operation.kind),
                    markdown_cell(&operation.class),
                    markdown_cell(&operation.key)
                ));
            }
        }
    }
    if excluded == 0 {
        out.push_str("No spans were excluded by the response cutoff.\n");
    }
    out
}

fn format_class_ops(classes: &BTreeMap<String, ClassStats>, gets: bool) -> String {
    let values = classes
        .iter()
        .filter_map(|(class, stats)| {
            let count = if gets { stats.get_ops } else { stats.put_ops };
            (count > 0).then(|| format!("{class}={count}"))
        })
        .collect::<Vec<_>>();
    let rendered = if values.is_empty() {
        "none".to_string()
    } else {
        values.join(", ")
    };
    markdown_cell(&rendered)
}

fn format_violation_deltas(violations: &[CostViolation]) -> String {
    if violations.is_empty() {
        return "none".to_string();
    }
    let values = violations
        .iter()
        .map(|violation| match violation {
            CostViolation::OpCount {
                class,
                kind,
                expected,
                actual,
            } => format!("{:?}.{class}: {expected}->{actual}", kind).to_lowercase(),
            CostViolation::KeyCount {
                substring,
                kind,
                expected,
                actual,
            } => format!("{:?}[{substring}]: {expected}->{actual}", kind).to_lowercase(),
            CostViolation::Bytes {
                class,
                bound,
                actual,
            } => format!(
                "bytes.{class}: {}->{actual}",
                serde_json::to_string(bound).expect("byte bound must serialize")
            ),
            CostViolation::Depth {
                kinds,
                mode,
                limit,
                actual,
                ..
            } => format!("depth.{kinds}({mode:?}): {limit}->{actual}"),
            CostViolation::RepeatDrift { repeat, detail } => {
                format!("repeat {repeat}: {detail}")
            }
            CostViolation::BaselineDrift { field, detail } => format!("{field}: {detail}"),
            CostViolation::SecurityBudget {
                metric,
                expected,
                actual,
            } => format!("security.{metric}: {expected}->{actual}"),
        })
        .collect::<Vec<_>>()
        .join("; ");
    markdown_cell(&values)
}

fn aggregate_class_totals(
    scenarios: &BTreeMap<String, ScenarioReport>,
) -> BTreeMap<String, ClassStats> {
    let mut totals = BTreeMap::new();
    for first in scenarios
        .values()
        .filter_map(|scenario| scenario.repeats.first())
    {
        for (class, stats) in &first.classes {
            let total = totals
                .entry(class.clone())
                .or_insert_with(ClassStats::default);
            total.get_ops += stats.get_ops;
            total.get_bytes += stats.get_bytes;
            total.put_ops += stats.put_ops;
            total.put_bytes += stats.put_bytes;
        }
    }
    totals
}

fn sum_class_stats(values: impl Iterator<Item = ClassStats>) -> ClassStats {
    values.fold(ClassStats::default(), |mut total, stats| {
        total.get_ops += stats.get_ops;
        total.get_bytes += stats.get_bytes;
        total.put_ops += stats.put_ops;
        total.put_bytes += stats.put_bytes;
        total
    })
}

fn diff_toml(old: &str, new: &str) -> Vec<ProposedDiff> {
    let old = old
        .parse::<toml::Value>()
        .unwrap_or_else(|error| panic!("checked-in contract is invalid TOML: {error}"));
    let new = new
        .parse::<toml::Value>()
        .unwrap_or_else(|error| panic!("proposed contract is invalid TOML: {error}"));
    let mut old_fields = BTreeMap::new();
    let mut new_fields = BTreeMap::new();
    flatten_toml("", &old, &mut old_fields);
    flatten_toml("", &new, &mut new_fields);
    old_fields
        .keys()
        .chain(new_fields.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|field| {
            let old = old_fields
                .get(field)
                .cloned()
                .unwrap_or_else(|| "<absent>".to_string());
            let new = new_fields
                .get(field)
                .cloned()
                .unwrap_or_else(|| "<absent>".to_string());
            (old != new).then(|| ProposedDiff {
                field: field.clone(),
                old,
                new,
            })
        })
        .collect()
}

fn flatten_toml(prefix: &str, value: &toml::Value, fields: &mut BTreeMap<String, String>) {
    if let toml::Value::Table(table) = value {
        if table.is_empty() && !prefix.is_empty() {
            fields.insert(prefix.to_string(), "{}".to_string());
        }
        for (name, child) in table {
            let path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}.{name}")
            };
            flatten_toml(&path, child, fields);
        }
    } else {
        fields.insert(prefix.to_string(), value.to_string());
    }
}

pub(crate) fn stable_depth_key(key: &str) -> String {
    let filename = key.rsplit('/').next().unwrap_or(key);
    if filename.ends_with(".wal") {
        "<wal>.wal".to_string()
    } else if filename
        .strip_suffix(".msgpack")
        .is_some_and(|stem| !stem.is_empty() && stem.bytes().all(|byte| byte.is_ascii_digit()))
    {
        "<generation>.msgpack".to_string()
    } else if let Some(template) = indexed_artifact_template(filename) {
        template
    } else if filename.is_empty() {
        "<root>".to_string()
    } else {
        filename.to_string()
    }
}

fn indexed_artifact_template(filename: &str) -> Option<String> {
    const PREFIXES: [&str; 9] = [
        "cluster_group_",
        "cluster_pair_",
        "cluster_",
        "attrs_",
        "bitmap_",
        "sq_cluster_",
        "pq_cluster_",
        "fts_index_",
        "node_",
    ];
    let stem = filename.strip_suffix(".bin")?;
    PREFIXES.iter().find_map(|prefix| {
        let index = stem.strip_prefix(prefix)?;
        (!index.is_empty() && index.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| format!("{prefix}<index>.bin"))
    })
}

fn format_depth(path: &DepthSummary) -> String {
    let chain = if path.chain.is_empty() {
        "empty".to_string()
    } else {
        path.chain
            .iter()
            .map(|link| format!("{}:{}", link.class, link.key))
            .collect::<Vec<_>>()
            .join(" -> ")
    };
    format!("{} ({})", path.depth, markdown_cell(&chain))
}

fn markdown_cell(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace(['\r', '\n'], " ")
        .replace('`', "\\`")
}

fn assert_all_scenarios_written(expected: &[String], actual: &BTreeMap<String, ScenarioReport>) {
    let expected = expected.iter().collect::<BTreeSet<_>>();
    let actual = actual.keys().collect::<BTreeSet<_>>();
    assert_eq!(
        actual, expected,
        "cannot write performance-contract report before every declared scenario is written"
    );
}

fn validate_unique_labels(labels: &[String]) {
    let mut unique = BTreeSet::new();
    for label in labels {
        validate_component(label, "scenario label");
        assert!(
            unique.insert(label),
            "duplicate performance-contract scenario label {label:?}"
        );
    }
}

fn validate_component(value: &str, kind: &str) {
    assert!(!value.is_empty(), "{kind} cannot be empty");
    assert!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')),
        "{kind} must contain only ASCII letters, digits, '_' or '-': {value:?}"
    );
}

fn create_unique_run_dir(artifact_root: &Path) -> (PathBuf, String) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch");
    let base = format!(
        "run-{}-{:09}-{}",
        now.as_secs(),
        now.subsec_nanos(),
        std::process::id()
    );
    for collision in 0..=u16::MAX {
        let run_id = if collision == 0 {
            base.clone()
        } else {
            format!("{base}-{collision}")
        };
        let root = artifact_root.join(&run_id);
        match fs::create_dir(&root) {
            Ok(()) => return (root, run_id),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!(
                "failed to create performance-contract run dir {}: {error}",
                root.display()
            ),
        }
    }
    panic!("exhausted unique performance-contract run directory names for {base}");
}

fn write_json<T: Serialize + ?Sized>(path: PathBuf, value: &T) {
    let file = File::create_new(&path).unwrap_or_else(|error| {
        panic!("failed to create JSON artifact {}: {error}", path.display())
    });
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value).unwrap_or_else(|error| {
        panic!(
            "failed to serialize JSON artifact {}: {error}",
            path.display()
        )
    });
    writer.write_all(b"\n").unwrap_or_else(|error| {
        panic!(
            "failed to terminate JSON artifact {}: {error}",
            path.display()
        )
    });
    writer.flush().unwrap_or_else(|error| {
        panic!("failed to flush JSON artifact {}: {error}", path.display())
    });
}

fn write_text(path: &Path, contents: &str, description: &str) {
    let mut file = File::create_new(path).unwrap_or_else(|error| {
        panic!("failed to create {description} {}: {error}", path.display())
    });
    file.write_all(contents.as_bytes()).unwrap_or_else(|error| {
        panic!("failed to write {description} {}: {error}", path.display())
    });
    file.flush().unwrap_or_else(|error| {
        panic!("failed to flush {description} {}: {error}", path.display())
    });
}

fn append_report_section(path: &Path, contents: &str, description: &str) {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to open {} for {description}: {error}",
                path.display()
            )
        });
    file.write_all(b"\n\n").unwrap_or_else(|error| {
        panic!(
            "failed to separate {description} in {}: {error}",
            path.display()
        )
    });
    file.write_all(contents.as_bytes()).unwrap_or_else(|error| {
        panic!(
            "failed to append {description} to {}: {error}",
            path.display()
        )
    });
    file.flush().unwrap_or_else(|error| {
        panic!(
            "failed to flush {description} in {}: {error}",
            path.display()
        )
    });
}

fn optional_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("failed to read {name}: {error}"),
    }
}

fn git_rev() -> String {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("failed to run git rev-parse HEAD");
    if !output.status.success() {
        let stderr =
            String::from_utf8(output.stderr).expect("git rev-parse HEAD emitted non-UTF8 stderr");
        panic!(
            "git rev-parse HEAD failed with status {}: {}",
            output.status,
            stderr.trim()
        );
    }
    let revision = String::from_utf8(output.stdout)
        .expect("git rev-parse HEAD emitted non-UTF8")
        .trim()
        .to_string();
    assert!(
        !revision.is_empty(),
        "git rev-parse HEAD emitted no revision"
    );
    revision
}
