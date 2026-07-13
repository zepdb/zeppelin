use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use thiserror::Error;

use super::artifacts::{
    aggregate_scenario_samples, normalized_scenario_costs, rank_samples, rank_scenario_summaries,
    IdealSample, PhysicalModeTotal, ScenarioSummary,
};
use super::catalog::{self, IdealCase, IdealOperation};
use super::observe::SerialGetChain;
use crate::perf_contract::contract::load_contract;
use crate::perf_contract::scenario::{run_scenario, ScenarioSpec};
use crate::perf_contract::{require_minio, scenarios};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdealEnv {
    pub seconds: u64,
    pub scenarios: Option<Vec<String>>,
    pub artifact_root: PathBuf,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum IdealEnvError {
    #[error("missing required ideal-analysis environment variable {0}")]
    Missing(&'static str),
    #[error("invalid ZEPPELIN_PERF_IDEAL_SECONDS {value:?}: {detail}")]
    InvalidBudget { value: String, detail: String },
    #[error("ZEPPELIN_PERF_IDEAL_SECONDS must be greater than zero")]
    ZeroBudget,
    #[error("ZEPPELIN_PERF_IDEAL_SCENARIOS must contain at least one scenario ID")]
    EmptyScenarioFilter,
    #[error("ideal analysis is incompatible with {0}")]
    Incompatible(&'static str),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum IdealRunError {
    #[error("invalid ideal-analysis catalog: {0}")]
    InvalidCatalog(String),
    #[error("unknown ideal-analysis scenario {0:?}")]
    UnknownScenario(String),
    #[error("failed to load frozen scenario {scenario:?}: {error}")]
    FrozenScenario { scenario: String, error: String },
    #[error("ideal-analysis scenario {0:?} has no executor yet")]
    UnimplementedScenario(String),
}

#[derive(Debug, Clone)]
pub struct IdealRunSummary {
    pub report: PathBuf,
    pub cycles_completed: u64,
    pub scenario_runs: u64,
}

impl IdealEnv {
    fn from_env() -> Result<Self, IdealEnvError> {
        Self::parse(|name| std::env::var(name).ok())
    }

    fn parse(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self, IdealEnvError> {
        if get("ZEPPELIN_PERF_CAPTURE").is_some() {
            return Err(IdealEnvError::Incompatible("ZEPPELIN_PERF_CAPTURE"));
        }
        if get("ZEPPELIN_PERF_SELFTEST").is_some() {
            return Err(IdealEnvError::Incompatible("ZEPPELIN_PERF_SELFTEST"));
        }

        let raw_seconds = get("ZEPPELIN_PERF_IDEAL_SECONDS")
            .ok_or(IdealEnvError::Missing("ZEPPELIN_PERF_IDEAL_SECONDS"))?;
        let seconds = raw_seconds
            .parse::<u64>()
            .map_err(|error| IdealEnvError::InvalidBudget {
                value: raw_seconds,
                detail: error.to_string(),
            })?;
        if seconds == 0 {
            return Err(IdealEnvError::ZeroBudget);
        }

        let scenarios = get("ZEPPELIN_PERF_IDEAL_SCENARIOS")
            .map(|raw| {
                let selected = raw
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                if selected.is_empty() {
                    Err(IdealEnvError::EmptyScenarioFilter)
                } else {
                    Ok(selected)
                }
            })
            .transpose()?;

        let artifact_root = get("ZEPPELIN_PERF_ARTIFACTS")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/perf-contract"));
        Ok(Self {
            seconds,
            scenarios,
            artifact_root,
        })
    }
}

fn select_cases(env: &IdealEnv) -> Result<Vec<&'static IdealCase>, IdealRunError> {
    catalog::validate(catalog::all())
        .map_err(|error| IdealRunError::InvalidCatalog(format!("{error:?}")))?;
    validate_executor_coverage(catalog::all()).map_err(IdealRunError::InvalidCatalog)?;
    let Some(selected) = &env.scenarios else {
        return Ok(catalog::all().iter().collect());
    };
    let selected = selected.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let known = catalog::all()
        .iter()
        .map(|case| case.id.as_str())
        .collect::<BTreeSet<_>>();
    if let Some(unknown) = selected.difference(&known).next() {
        return Err(IdealRunError::UnknownScenario((*unknown).to_string()));
    }
    Ok(catalog::all()
        .iter()
        .filter(|case| selected.contains(case.id.as_str()))
        .collect())
}

fn validate_executor_coverage(cases: &[IdealCase]) -> Result<(), String> {
    let mut invalid = Vec::new();
    for case in cases {
        let mut owners = Vec::new();
        if matches!(case.operation, IdealOperation::FrozenContract { .. }) {
            owners.push("frozen");
        } else {
            if super::query::supports(case) {
                owners.push("query");
            }
            if super::variant_query::supports(case) {
                owners.push("variant_query");
            }
            if super::maintenance::supports(case) {
                owners.push("maintenance");
            }
            if super::variant_compaction::supports(case) {
                owners.push("variant_compaction");
            }
            if super::namespace::supports(case) {
                owners.push("namespace");
            }
            if super::direct::supports(case) {
                owners.push("direct");
            }
            if super::http::supports(case) {
                owners.push("http");
            }
        }
        if owners.len() != 1 {
            invalid.push(format!("{}={owners:?}", case.id.as_str()));
        }
    }
    if invalid.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "every executable catalog case needs exactly one executor: {}",
            invalid.join(", ")
        ))
    }
}

/// Run the dedicated exhaustive analyzer. Configuration and measurement
/// failures are fatal because a partial cost catalog is unsafe to rank.
pub async fn run_ideal_analysis_entry() -> IdealRunSummary {
    require_minio();
    let env = IdealEnv::from_env().unwrap_or_else(|error| panic!("{error}"));
    let cases = select_cases(&env).unwrap_or_else(|error| panic!("{error}"));
    assert!(!cases.is_empty(), "ideal analysis selected no scenarios");

    let started = Instant::now();
    let budget = Duration::from_secs(env.seconds);
    let mut cycles_completed = 0_u64;
    let mut scenario_runs = 0_u64;
    let mut samples = Vec::new();
    loop {
        for case in &cases {
            let mut measured = execute_case(case)
                .await
                .unwrap_or_else(|error| panic!("{error}"));
            samples.append(&mut measured);
            scenario_runs = scenario_runs
                .checked_add(1)
                .expect("ideal-analysis scenario run count overflowed");
        }
        cycles_completed = cycles_completed
            .checked_add(1)
            .expect("ideal-analysis cycle count overflowed");
        if started.elapsed() >= budget {
            break;
        }
    }

    if env.scenarios.is_none() {
        let missing = missing_required_physical_modes(&samples);
        assert!(
            missing.is_empty(),
            "complete ideal-analysis pass missed required physical modes: {missing:?}"
        );
    }

    let ranked_samples = rank_samples(samples.clone());
    let ranked = rank_scenario_summaries(aggregate_scenario_samples(samples));
    let report = write_artifacts(
        &env,
        &cases,
        &ranked,
        &ranked_samples,
        cycles_completed,
        scenario_runs,
        started.elapsed(),
    );
    IdealRunSummary {
        report,
        cycles_completed,
        scenario_runs,
    }
}

fn missing_required_physical_modes(samples: &[IdealSample]) -> Vec<(&'static str, &'static str)> {
    const REQUIRED: [(&str, &str); 11] = [
        ("get", "get_full"),
        ("get", "get_range"),
        ("get", "get_conditional"),
        ("put", "put_overwrite"),
        ("put", "put_create"),
        ("put", "put_update"),
        ("list", "list_recursive"),
        ("list", "list_delimiter"),
        ("copy", "copy_if_absent"),
        ("delete", "delete"),
        ("delete", "delete_batch"),
    ];
    let observed = samples
        .iter()
        .flat_map(|sample| &sample.physical_verb_mode_totals)
        .filter(|total| total.ops > 0)
        .map(|total| (total.verb.as_str(), total.mode.as_str()))
        .collect::<BTreeSet<_>>();
    REQUIRED
        .into_iter()
        .filter(|mode| !observed.contains(mode))
        .collect()
}

async fn execute_case(case: &IdealCase) -> Result<Vec<IdealSample>, IdealRunError> {
    match case.operation {
        IdealOperation::FrozenContract { scenario } => {
            let spec = frozen_spec(scenario)?;
            let outcome = run_scenario(&spec, None).await;
            Ok(outcome
                .per_repeat
                .iter()
                .map(|repeat| IdealSample::from_repeat(case.id.as_str(), repeat))
                .collect())
        }
        _ => {
            if let Some(sample) = super::query::execute(case).await {
                return Ok(vec![sample]);
            }
            if let Some(sample) = super::variant_query::execute(case).await {
                return Ok(vec![sample]);
            }
            if let Some(sample) = super::maintenance::execute(case).await {
                return Ok(vec![sample]);
            }
            if let Some(sample) = super::variant_compaction::execute(case).await {
                return Ok(vec![sample]);
            }
            if let Some(sample) = super::namespace::execute(case).await {
                return Ok(vec![sample]);
            }
            if let Some(sample) = super::direct::execute(case).await {
                return Ok(vec![sample]);
            }
            if let Some(sample) = super::http::execute(case).await {
                return Ok(vec![sample]);
            }
            Err(IdealRunError::UnimplementedScenario(
                case.id.as_str().to_string(),
            ))
        }
    }
}

fn frozen_spec(scenario: &str) -> Result<ScenarioSpec, IdealRunError> {
    let contract = load_contract(scenario).map_err(|error| IdealRunError::FrozenScenario {
        scenario: scenario.to_string(),
        error,
    })?;
    Ok(scenarios::build(&contract, 1))
}

#[derive(Serialize)]
struct RunMetadata<'a> {
    entry: &'static str,
    git_rev: String,
    git_dirty: bool,
    command: String,
    budget_seconds: u64,
    catalog_fingerprint_fnv1a64: String,
    catalog_scenario_count: usize,
    elapsed_millis: u128,
    cycles_completed: u64,
    scenario_runs: u64,
    failures: u64,
    scenarios: Vec<&'a str>,
    soundness: &'static str,
}

#[derive(Serialize)]
struct ScenarioChainArtifact<'a> {
    scenario_id: &'a str,
    sample_count: u64,
    chain: &'a SerialGetChain,
}

#[derive(Serialize)]
struct ScenarioPhysicalTotalsArtifact<'a> {
    scenario_id: &'a str,
    sample_count: u64,
    totals: &'a [PhysicalModeTotal],
}

fn write_artifacts(
    env: &IdealEnv,
    cases: &[&IdealCase],
    ranked: &[ScenarioSummary],
    ranked_samples: &[IdealSample],
    cycles_completed: u64,
    scenario_runs: u64,
    elapsed: Duration,
) -> PathBuf {
    fs::create_dir_all(&env.artifact_root).unwrap_or_else(|error| {
        panic!(
            "failed to create ideal-analysis artifact root {}: {error}",
            env.artifact_root.display()
        )
    });
    let root = create_unique_run_dir(&env.artifact_root);
    let git_revision = git_rev();
    let git_dirty = git_is_dirty();
    write_json(
        root.join("run.json"),
        &RunMetadata {
            entry: "ideal_analysis",
            git_rev: git_revision.clone(),
            git_dirty,
            command: ideal_command(env),
            budget_seconds: env.seconds,
            catalog_fingerprint_fnv1a64: catalog_fingerprint(cases),
            catalog_scenario_count: cases.len(),
            elapsed_millis: elapsed.as_millis(),
            cycles_completed,
            scenario_runs,
            failures: 0,
            scenarios: cases.iter().map(|case| case.id.as_str()).collect(),
            soundness: "one isolated logical operation or explicitly named cursor flow; setup, verification, and cleanup are outside measurement; events are ObjectStore adapter invocations, so backend HTTP retries and recursive-LIST pages remain an explicit instrumentation gap",
        },
    );
    write_json(root.join("catalog.json"), &cases);
    write_json(
        root.join("storage-methods.json"),
        super::inventory::storage_methods(),
    );
    let production_paths = super::inventory::production_paths();
    write_json(root.join("inventory.json"), &production_paths);
    write_inventory_markdown(&root.join("inventory.md"), &production_paths);
    write_json(root.join("ranked.json"), ranked);
    write_json(
        root.join("normalized-costs.json"),
        &normalized_scenario_costs(ranked),
    );
    write_json(
        root.join("get-chains.json"),
        &ranked
            .iter()
            .map(|summary| ScenarioChainArtifact {
                scenario_id: &summary.scenario_id,
                sample_count: summary.sample_count,
                chain: &summary.representative_worst_sample.serial_get_chain,
            })
            .collect::<Vec<_>>(),
    );
    write_json(
        root.join("physical-totals.json"),
        &ranked
            .iter()
            .map(|summary| ScenarioPhysicalTotalsArtifact {
                scenario_id: &summary.scenario_id,
                sample_count: summary.sample_count,
                totals: &summary
                    .representative_worst_sample
                    .physical_verb_mode_totals,
            })
            .collect::<Vec<_>>(),
    );
    write_samples_jsonl(&root.join("scenario-samples.jsonl"), ranked_samples);

    let mut report = String::from("# Zeppelin Ideal S3 Analysis\n\n");
    report.push_str(&format!("- git rev: `{git_revision}`\n"));
    report.push_str(&format!("- git dirty: `{git_dirty}`\n"));
    report.push_str(&format!("- budget seconds: `{}`\n", env.seconds));
    report.push_str(&format!("- cycles completed: `{cycles_completed}`\n"));
    report.push_str(&format!("- scenario runs: `{scenario_runs}`\n\n"));
    report.push_str("- observer scope: one event per `ObjectStore` adapter invocation; backend HTTP retries and individual recursive-LIST pages are not visible\n");
    report.push_str("- chain semantics: longest GET chain under interval happens-before ordering inside the isolated operation; it is not semantic parent-span lineage\n\n");
    report.push_str("## Ranked serial GET chains\n\n");
    report.push_str(
        "| rank | scenario | samples | GET depth | GET ops min-max | GET bytes min-max | chain |\n",
    );
    report.push_str("| ---: | --- | ---: | ---: | ---: | ---: | --- |\n");
    for (index, summary) in ranked.iter().enumerate() {
        let sample = &summary.representative_worst_sample;
        let chain = sample
            .serial_get_chain
            .links
            .iter()
            .map(|link| format!("{}:{}", link.class.name(), link.key))
            .collect::<Vec<_>>()
            .join(" -> ");
        report.push_str(&format!(
            "| {} | `{}` | {} | {} | {}-{} | {}-{} | {} |\n",
            index + 1,
            summary.scenario_id,
            summary.sample_count,
            summary.max_serial_get_depth,
            summary.min_get_ops,
            summary.max_get_ops,
            summary.min_get_bytes,
            summary.max_get_bytes,
            chain
        ));
    }
    report.push_str("\n## Scenario details\n");
    for summary in ranked {
        let sample = &summary.representative_worst_sample;
        report.push_str(&format!("\n### `{}`\n\n", summary.scenario_id));
        report.push_str("Physical object-store work for the representative worst sample:\n\n");
        report.push_str("| verb | mode | class | adapter invocations | successful bytes |\n");
        report.push_str("| --- | --- | --- | ---: | ---: |\n");
        for total in &sample.physical_verb_mode_totals {
            report.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                total.verb,
                total.mode,
                total.class.name(),
                total.ops,
                total.bytes
            ));
        }
        if sample.physical_verb_mode_totals.is_empty() {
            report.push_str("| none | none | none | 0 | 0 |\n");
        }
        report.push_str("\nLongest serial GET chain:\n\n");
        report
            .push_str("| link | mode | class | normalized key | bytes | elapsed us | outcome |\n");
        report.push_str("| ---: | --- | --- | --- | ---: | ---: | --- |\n");
        for link in &sample.serial_get_chain.links {
            report.push_str(&format!(
                "| {} | `{:?}` | {} | `{}` | {} | {} | `{:?}` |\n",
                link.ordinal,
                link.request,
                link.class.name(),
                link.key,
                link.bytes,
                link.elapsed_us,
                link.outcome
            ));
        }
        if sample.serial_get_chain.links.is_empty() {
            report.push_str("| 0 | none | none | none | 0 | 0 | none |\n");
        }
    }
    let report_path = root.join("report.md");
    write_text(&report_path, &report);
    report_path
}

fn ideal_command(env: &IdealEnv) -> String {
    let mut environment = format!(
        "TEST_BACKEND=minio ZEPPELIN_PERF_IDEAL_SECONDS={}",
        env.seconds
    );
    if let Some(scenarios) = &env.scenarios {
        environment.push_str(" ZEPPELIN_PERF_IDEAL_SCENARIOS=");
        environment.push_str(&scenarios.join(","));
    }
    format!(
        "{environment} cargo test --release --test perf_contract_tests ideal_analysis -- --ignored --nocapture"
    )
}

fn catalog_fingerprint(cases: &[&IdealCase]) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for case in cases {
        for byte in case.id.as_str().bytes().chain(std::iter::once(0)) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    format!("{hash:016x}")
}

fn write_inventory_markdown(
    path: &std::path::Path,
    production_paths: &[&super::inventory::ProductionPath],
) {
    let mut markdown = String::from("# Source-derived S3 operation inventory\n\n");
    markdown.push_str(
        "| logical operation/state | production entry point | ZeppelinStore methods | physical S3 variants | perf coverage |\n",
    );
    markdown.push_str("| --- | --- | --- | --- | --- |\n");
    for production_path in production_paths {
        let methods = production_path
            .store_methods
            .iter()
            .map(|method| format!("`{method:?}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let variants = production_path
            .physical_variants
            .iter()
            .map(|variant| format!("`{variant:?}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let coverage = match production_path.coverage {
            super::inventory::PathCoverage::ExistingFrozen { scenario } => {
                format!("frozen `{scenario}`")
            }
            super::inventory::PathCoverage::IdealScenario { scenario } => {
                format!("ideal `{scenario}`")
            }
            super::inventory::PathCoverage::ExplicitGap { reason, .. } => {
                format!("GAP: {reason}")
            }
            super::inventory::PathCoverage::NoProductionCaller { reason } => {
                format!("no production caller: {reason}")
            }
        };
        markdown.push_str(&format!(
            "| `{}` | `{}` | {} | {} | {} |\n",
            production_path.id, production_path.source, methods, variants, coverage
        ));
    }
    write_text(path, &markdown);
}

fn create_unique_run_dir(root: &std::path::Path) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch");
    let base = format!(
        "ideal-run-{}-{:09}-{}",
        now.as_secs(),
        now.subsec_nanos(),
        std::process::id()
    );
    for collision in 0..=u16::MAX {
        let name = if collision == 0 {
            base.clone()
        } else {
            format!("{base}-{collision}")
        };
        let path = root.join(name);
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!(
                "failed to create ideal-analysis run {}: {error}",
                path.display()
            ),
        }
    }
    panic!("exhausted ideal-analysis run directory names for {base}");
}

fn git_rev() -> String {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap_or_else(|error| panic!("failed to run git rev-parse HEAD: {error}"));
    assert!(output.status.success(), "git rev-parse HEAD failed");
    String::from_utf8(output.stdout)
        .expect("git revision was not UTF-8")
        .trim()
        .to_string()
}

fn git_is_dirty() -> bool {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=all"])
        .output()
        .unwrap_or_else(|error| panic!("failed to run git status --porcelain: {error}"));
    assert!(output.status.success(), "git status --porcelain failed");
    !output.stdout.is_empty()
}

fn write_json<T: Serialize + ?Sized>(path: PathBuf, value: &T) {
    let file = File::create_new(&path)
        .unwrap_or_else(|error| panic!("failed to create {}: {error}", path.display()));
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)
        .unwrap_or_else(|error| panic!("failed to serialize {}: {error}", path.display()));
    writer
        .write_all(b"\n")
        .unwrap_or_else(|error| panic!("failed to finish {}: {error}", path.display()));
}

fn write_samples_jsonl(path: &std::path::Path, samples: &[IdealSample]) {
    let file = File::create_new(path)
        .unwrap_or_else(|error| panic!("failed to create {}: {error}", path.display()));
    let mut writer = BufWriter::new(file);
    for sample in samples {
        serde_json::to_writer(&mut writer, sample)
            .unwrap_or_else(|error| panic!("failed to serialize {}: {error}", path.display()));
        writer
            .write_all(b"\n")
            .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
    }
}

fn write_text(path: &std::path::Path, contents: &str) {
    let mut file = File::create_new(path)
        .unwrap_or_else(|error| panic!("failed to create {}: {error}", path.display()));
    file.write_all(contents.as_bytes())
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn parse(values: &[(&str, &str)]) -> Result<IdealEnv, IdealEnvError> {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<BTreeMap<_, _>>();
        IdealEnv::parse(|name| values.get(name).cloned())
    }

    #[test]
    fn ideal_budget_is_explicit_and_positive() {
        assert!(matches!(
            parse(&[]),
            Err(IdealEnvError::Missing("ZEPPELIN_PERF_IDEAL_SECONDS"))
        ));
        assert!(matches!(
            parse(&[("ZEPPELIN_PERF_IDEAL_SECONDS", "0")]),
            Err(IdealEnvError::ZeroBudget)
        ));
        assert_eq!(
            parse(&[("ZEPPELIN_PERF_IDEAL_SECONDS", "3600")])
                .expect("valid ideal environment")
                .seconds,
            3_600
        );
    }

    #[test]
    fn ideal_scenario_filter_is_sorted_and_duplicate_free() {
        let env = parse(&[
            ("ZEPPELIN_PERF_IDEAL_SECONDS", "1"),
            (
                "ZEPPELIN_PERF_IDEAL_SCENARIOS",
                "timestamp_as_of,ann_cold,timestamp_as_of",
            ),
        ])
        .expect("valid filtered ideal environment");

        assert_eq!(
            env.scenarios,
            Some(vec!["ann_cold".to_string(), "timestamp_as_of".to_string(),])
        );
    }

    #[test]
    fn ideal_filter_rejects_unknown_case_ids() {
        let env = IdealEnv {
            seconds: 1,
            scenarios: Some(vec!["does.not.exist".to_string()]),
            artifact_root: PathBuf::from("target/perf-contract"),
        };

        assert!(matches!(
            select_cases(&env),
            Err(IdealRunError::UnknownScenario(id)) if id == "does.not.exist"
        ));
    }

    #[test]
    fn every_catalog_case_has_exactly_one_executor() {
        validate_executor_coverage(catalog::all())
            .expect("checked-in ideal catalog must be completely executable");
    }

    #[test]
    fn complete_pass_requires_every_production_physical_mode() {
        let sample = IdealSample {
            scenario_id: "coverage".to_string(),
            serial_get_chain: crate::perf_contract::ideal::observe::SerialGetChain {
                depth: 0,
                links: Vec::new(),
            },
            total_get_ops: 0,
            total_get_bytes: 0,
            physical_verb_mode_totals: [
                ("get", "get_full"),
                ("get", "get_range"),
                ("get", "get_conditional"),
                ("put", "put_overwrite"),
                ("put", "put_create"),
                ("put", "put_update"),
                ("list", "list_recursive"),
                ("list", "list_delimiter"),
                ("copy", "copy_if_absent"),
                ("delete", "delete"),
                ("delete", "delete_batch"),
            ]
            .into_iter()
            .map(|(verb, mode)| PhysicalModeTotal {
                verb: verb.to_string(),
                mode: mode.to_string(),
                class: crate::common::counting::ArtifactClass::Other,
                ops: 1,
                bytes: 0,
            })
            .collect(),
            physical_operations: Vec::new(),
        };

        assert!(missing_required_physical_modes(std::slice::from_ref(&sample)).is_empty());
        assert_eq!(
            missing_required_physical_modes(&[IdealSample {
                physical_verb_mode_totals: sample
                    .physical_verb_mode_totals
                    .into_iter()
                    .filter(|total| total.mode != "delete")
                    .collect(),
                ..sample
            }]),
            vec![("delete", "delete")]
        );
    }

    #[test]
    fn catalog_fingerprint_is_order_sensitive_and_stable() {
        let selected = catalog::all().iter().take(2).collect::<Vec<_>>();
        let reversed = selected.iter().rev().copied().collect::<Vec<_>>();

        assert_eq!(
            catalog_fingerprint(&selected),
            catalog_fingerprint(&selected)
        );
        assert_ne!(
            catalog_fingerprint(&selected),
            catalog_fingerprint(&reversed)
        );
    }
}
