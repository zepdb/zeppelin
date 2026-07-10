//! Run-scoped artifacts and the Phase-1 performance-contract report.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::common::counting::ClassStats;

use super::contract::CostViolation;
use super::depth::{CriticalPath, OpSpan};
use super::scenario::{RepeatCounters, ScenarioOutcome};
use super::PerfEnv;

const SOUNDNESS_PRECONDITION: &str =
    "exactly one client request in flight; no background compaction, GC, or hydration";

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
    violations: Vec<CostViolation>,
    repeats: Vec<ReportRepeat>,
}

#[derive(Debug, Clone)]
struct ReportRepeat {
    classes: BTreeMap<String, ClassStats>,
    totals: ClassStats,
    get: DepthSummary,
    put_get: DepthSummary,
    post_response_ops: usize,
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
        if !violations.is_empty() {
            write_json(scenario_dir.join("violations.json"), violations);
        }

        reports.insert(
            label.to_string(),
            ScenarioReport {
                passed: violations.is_empty(),
                violations: violations.to_vec(),
                repeats: report_repeats(&outcome.per_repeat),
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
    }

    /// Write the minimal Phase-1 Markdown report for all declared scenarios.
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
                get: summarize_depth(&counters.get_path),
                put_get: summarize_depth(&counters.put_get_path),
                post_response_ops: post_response_ops(counters),
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
            get: summarize_depth(&counters.get_path),
            put_get: summarize_depth(&counters.put_get_path),
            post_response_ops: post_response_ops(counters),
        })
        .collect()
}

fn post_response_ops(counters: &RepeatCounters) -> usize {
    counters
        .spans
        .iter()
        .filter(|span| span.wall_start_us > counters.response_cutoff_us)
        .count()
}

fn summarize_depth(path: &CriticalPath) -> DepthSummary {
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
    let passed = scenarios.values().all(|scenario| scenario.passed);
    let mut out = String::new();
    out.push_str("# Zeppelin Performance Contract Report\n\n");
    out.push_str(&format!("- run: `{}`\n", artifacts.run_id));
    out.push_str(&format!("- entry: `{}`\n", markdown_cell(&artifacts.entry)));
    out.push_str(&format!("- git rev: `{}`\n", artifacts.git_rev));
    out.push_str(&format!(
        "- TEST_BACKEND: `{}`\n",
        artifacts.test_backend.as_deref().unwrap_or("unset")
    ));
    out.push_str(&format!("- scenarios: {}\n", scenarios.len()));
    out.push_str(&format!(
        "- status: **{}**\n",
        if passed { "PASS" } else { "FAIL" }
    ));
    out.push_str(&format!(
        "- depth soundness: {}\n\n",
        SOUNDNESS_PRECONDITION
    ));

    out.push_str("## Scenarios\n\n");
    out.push_str(
        "| scenario | status | violations | GET depth + chain | PUT+GET depth + chain | get_ops | get_bytes | put_ops | put_bytes |\n",
    );
    out.push_str("| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: |\n");
    for label in &artifacts.scenario_labels {
        let scenario = &scenarios[label];
        let first = scenario
            .repeats
            .first()
            .expect("written scenario report must have at least one repeat");
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_cell(label),
            if scenario.passed { "PASS" } else { "FAIL" },
            scenario.violations.len(),
            format_depth(&first.get),
            format_depth(&first.put_get),
            first.totals.get_ops,
            first.totals.get_bytes,
            first.totals.put_ops,
            first.totals.put_bytes,
        ));
    }
    out.push('\n');

    for label in &artifacts.scenario_labels {
        let scenario = &scenarios[label];
        let first = scenario
            .repeats
            .first()
            .expect("written scenario report must have at least one repeat");
        out.push_str(&format!("## `{}`\n\n", markdown_cell(label)));
        out.push_str(&format!(
            "- status: **{}**\n",
            if scenario.passed { "PASS" } else { "FAIL" }
        ));
        out.push_str(&format!("- violations: {}\n", scenario.violations.len()));
        out.push_str(&format!("- measured repeats: {}\n", scenario.repeats.len()));
        out.push_str(&format!("- GET path: {}\n", format_depth(&first.get)));
        out.push_str(&format!(
            "- PUT+GET path: {}\n\n",
            format_depth(&first.put_get)
        ));
        out.push_str(&format!(
            "- post-response operations: {}\n\n",
            first.post_response_ops
        ));
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
    out
}

fn stable_depth_key(key: &str) -> String {
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
