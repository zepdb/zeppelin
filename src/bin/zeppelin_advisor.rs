//! Offline deployment sizing and onboarding advisor.
//!
//! `plan` ranks cloud hardware with Zeppelin's embedded, snapshot-dated
//! catalog and calibrated analytic model. `catalog` exposes the exact inputs
//! used by the planner. The binary performs no network access and accepts only
//! explicit `--flag value` pairs; unknown, duplicate, or missing flags fail.

use std::collections::BTreeMap;
use std::process::ExitCode;

use serde::Serialize;
use zeppelin::sizing::advisor::{plan_embedded, ArchFilter, DataShape, PlanReport, PlanRequest};
use zeppelin::sizing::catalog::{Arch, Catalog, Cloud};
use zeppelin::sizing::rows::Quantization;

const PLAN_FLAGS: &[&str] = &[
    "--cloud",
    "--region",
    "--vectors",
    "--dims",
    "--replicas",
    "--qps",
    "--p99-ms",
    "--budget-month",
    "--filters",
    "--fts",
    "--quantization",
    "--nprobe",
    "--arch",
    "--clients",
    "--top",
    "--format",
];
const CATALOG_FLAGS: &[&str] = &["--cloud", "--region"];
const PREDICTION_BANNER: &str = "PREDICTION — calibrated on minio-local + s3-intra-region (GT-A ≤10%, GT-B ≤20%); non-AWS rows (EXTRAPOLATED)";

const USAGE: &str = "\
zeppelin_advisor — deployment sizing and onboarding advisor

USAGE:
  zeppelin_advisor plan --cloud <aws|gcp|azure> --region <region>
      --vectors <N> --dims <N> --replicas <N>
      [--qps <N>] [--p99-ms <N>] [--budget-month <USD>]
      [--filters <yes|no>] [--fts <yes|no>]
      [--quantization <rabitq-2bit|sq8|f32|all>]
      [--nprobe <N,N,...>] [--arch <any|x86_64|arm64>]
      [--clients <N>] [--top <N>] [--format <table|json>]

  zeppelin_advisor catalog [--cloud <aws|gcp|azure>] [--region <region>]

EXIT CODES:
  0  command completed
  1  usage or input failure
  2  no configuration meets plan constraints
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunOutcome {
    Success,
    NoConfiguration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Table,
    Json,
}

fn main() -> ExitCode {
    match run() {
        Ok(RunOutcome::Success) => ExitCode::SUCCESS,
        Ok(RunOutcome::NoConfiguration) => ExitCode::from(2),
        Err(error) => {
            eprintln!("zeppelin_advisor: {error}\n\n{USAGE}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<RunOutcome, String> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let Some(subcommand) = arguments.first() else {
        return Err("missing subcommand".to_string());
    };
    match subcommand.as_str() {
        "plan" => run_plan(&arguments[1..]),
        "catalog" => run_catalog(&arguments[1..]),
        "emit-config" => Err("`emit-config` lands in Phase 5 and is not available yet".to_string()),
        other => Err(format!("unknown subcommand {other:?}")),
    }
}

fn run_plan(arguments: &[String]) -> Result<RunOutcome, String> {
    let flags = parse_flags(arguments, PLAN_FLAGS)?;
    let cloud = parse_cloud(required(&flags, "--cloud")?)?;
    let region = required(&flags, "--region")?.to_string();
    let vectors = parse_usize("--vectors", required(&flags, "--vectors")?)?;
    let dims = parse_usize("--dims", required(&flags, "--dims")?)?;
    let replicas = parse_usize("--replicas", required(&flags, "--replicas")?)?;
    let min_qps = optional_f64(&flags, "--qps")?;
    let max_p99_ms = optional_f64(&flags, "--p99-ms")?;
    let max_monthly_usd = optional_f64(&flags, "--budget-month")?;
    let filters = optional_yes_no(&flags, "--filters", false)?;
    let fts = optional_yes_no(&flags, "--fts", false)?;
    let quantizations = parse_quantizations(optional(&flags, "--quantization", "rabitq-2bit"))?;
    let nprobes = parse_nprobes(optional(&flags, "--nprobe", "32,64,256"))?;
    let arch = parse_arch(optional(&flags, "--arch", "any"))?;
    let clients = parse_usize("--clients", optional(&flags, "--clients", "8"))?;
    let top = parse_usize("--top", optional(&flags, "--top", "12"))?;
    if top == 0 {
        return Err("--top must be nonzero".to_string());
    }
    let format = parse_format(optional(&flags, "--format", "table"))?;
    let request = PlanRequest {
        cloud,
        region,
        shape: DataShape {
            vectors,
            dims,
            filters,
            fts,
        },
        replicas,
        min_qps,
        max_p99_ms,
        max_monthly_usd,
        arch,
        clients,
        quantizations,
        nprobes,
    };
    let report = plan_embedded(&request).map_err(|error| error.to_string())?;
    match format {
        OutputFormat::Table => print_plan_table(&report, top),
        OutputFormat::Json => print_plan_json(&report, top)?,
    }
    if report.candidates.is_empty() {
        Ok(RunOutcome::NoConfiguration)
    } else {
        Ok(RunOutcome::Success)
    }
}

fn run_catalog(arguments: &[String]) -> Result<RunOutcome, String> {
    let flags = parse_flags(arguments, CATALOG_FLAGS)?;
    let cloud = flags
        .get("--cloud")
        .map(|value| parse_cloud(value))
        .transpose()?;
    let region = flags.get("--region").map(String::as_str);
    if region.is_some() && cloud.is_none() {
        return Err("--region requires --cloud for catalog".to_string());
    }
    let catalog = Catalog::embedded();
    println!(
        "CATALOG — embedded pricing snapshot {}",
        catalog.snapshot_date()
    );
    println!();
    println!("cloud | region | instance | arch | vCPU | GiB | NVMe GB | baseline Gbps | $/hour");
    println!("--- | --- | --- | --- | ---: | ---: | ---: | ---: | ---:");
    let mut instance_rows = 0usize;
    for (candidate_cloud, cloud_catalog) in &catalog.clouds {
        if cloud.is_some_and(|selected| selected != *candidate_cloud) {
            continue;
        }
        for instance in &cloud_catalog.instances {
            for (price_region, price) in &instance.price_hr {
                if region.is_some_and(|selected| selected != price_region) {
                    continue;
                }
                println!(
                    "{} | {} | {} | {} | {} | {:.1} | {} | {:.3} | {:.6}",
                    candidate_cloud.name(),
                    price_region,
                    instance.name,
                    arch_name(instance.arch),
                    instance.vcpus,
                    instance.mem_gb,
                    instance.nvme_gb,
                    instance.network_baseline_gbps,
                    price
                );
                instance_rows += 1;
            }
        }
    }
    if instance_rows == 0 {
        return Err("catalog filter matched no instance prices".to_string());
    }

    println!();
    println!("cloud | block tier | $/GB-month | max IOPS | max MB/s | region multiplier");
    println!("--- | --- | ---: | ---: | ---: | ---:");
    for (candidate_cloud, cloud_catalog) in &catalog.clouds {
        if cloud.is_some_and(|selected| selected != *candidate_cloud) {
            continue;
        }
        for tier in &cloud_catalog.block_storage {
            let multiplier = region
                .and_then(|selected| tier.price_multiplier.get(selected))
                .copied()
                .unwrap_or(1.0);
            println!(
                "{} | {} | {:.6} | {} | {} | {:.3}",
                candidate_cloud.name(),
                tier.name,
                tier.price_gb_month,
                tier.max_iops,
                tier.max_throughput_mbps,
                multiplier
            );
        }
    }

    println!();
    println!("cloud | object store | regions | $/GB-month | GET $/1k | p50/p99 ms | MB/s/conn");
    println!("--- | --- | --- | ---: | ---: | ---: | ---:");
    for store in &catalog.object_stores {
        if cloud.is_some_and(|selected| selected != store.cloud) {
            continue;
        }
        if region.is_some_and(|selected| !store.regions.iter().any(|item| item == selected)) {
            continue;
        }
        println!(
            "{} | {} | {} | {:.6} | {:.6} | {:.1}/{:.1} | {:.1}",
            store.cloud.name(),
            store.service,
            store.regions.join(","),
            store.storage_gb_month,
            store.get_per_1k,
            store.ttfb_ms.p50,
            store.ttfb_ms.p99,
            store.per_conn_mbps
        );
    }
    Ok(RunOutcome::Success)
}

fn print_plan_table(report: &PlanReport, top: usize) {
    println!("{PREDICTION_BANNER}");
    println!(
        "catalog snapshot {}; calibration snapshot {}; canonical nlist {}",
        report.catalog_snapshot_date, report.calibration_snapshot_date, report.nlist
    );
    println!();
    println!("rank | instance | cache | nprobe | QPS | p50 | p99 | $/query | $/month | bottleneck");
    println!("---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---");
    for (index, candidate) in report.candidates.iter().take(top).enumerate() {
        println!(
            "{} | {} | {}/{} | {} | {:.2} | {:.2} ms | {:.2} ms | {:.9} | {:.2} | {}",
            index + 1,
            candidate.instance.name,
            candidate.cache.label(),
            candidate.quantization.label(),
            candidate.nprobe,
            candidate.qps,
            candidate.prediction.p50_ms,
            candidate.prediction.p99_ms,
            candidate.cost_per_query,
            candidate.monthly.total,
            candidate.bottleneck.name()
        );
    }
    if report.candidates.is_empty() {
        println!("(no configuration meets all constraints)");
    } else if report.candidates.len() > top {
        println!(
            "... {} more viable rows omitted by --top",
            report.candidates.len() - top
        );
    }

    println!();
    println!(
        "rejected rows: {} (showing up to {})",
        report.rejected.len(),
        top
    );
    for rejected in report.rejected.iter().take(top) {
        println!(
            "- {} | {}/{} | nprobe {}: {}",
            rejected.instance,
            rejected.cache,
            rejected.quantization,
            rejected.nprobe,
            rejected.reasons.join("; ")
        );
    }
    for assumption in &report.assumptions {
        println!("{assumption}");
    }
}

#[derive(Serialize)]
struct JsonPlan<'a> {
    banner: &'static str,
    catalog_snapshot_date: &'a str,
    calibration_snapshot_date: &'a str,
    nlist: usize,
    viable_count: usize,
    rejected_count: usize,
    candidates: Vec<serde_json::Value>,
    rejected: Vec<serde_json::Value>,
    assumptions: &'a [String],
}

fn print_plan_json(report: &PlanReport, top: usize) -> Result<(), String> {
    let candidates = report
        .candidates
        .iter()
        .take(top)
        .enumerate()
        .map(|(index, candidate)| {
            serde_json::json!({
                "rank": index + 1,
                "instance": candidate.instance.name,
                "cache": candidate.cache.label(),
                "quantization": candidate.quantization.label(),
                "nprobe": candidate.nprobe,
                "qps": candidate.qps,
                "p50_ms": candidate.prediction.p50_ms,
                "p99_ms": candidate.prediction.p99_ms,
                "cost_per_query": candidate.cost_per_query,
                "monthly_usd": candidate.monthly.total,
                "monthly_components": {
                    "nodes": candidate.monthly.nodes,
                    "block_storage": candidate.monthly.block_storage,
                    "object_storage": candidate.monthly.object_storage,
                    "requests": candidate.monthly.requests,
                },
                "bottleneck": candidate.bottleneck.name(),
            })
        })
        .collect();
    let rejected = report
        .rejected
        .iter()
        .take(top)
        .map(|candidate| {
            serde_json::json!({
                "instance": candidate.instance,
                "cache": candidate.cache,
                "quantization": candidate.quantization,
                "nprobe": candidate.nprobe,
                "reasons": candidate.reasons,
            })
        })
        .collect();
    let output = JsonPlan {
        banner: PREDICTION_BANNER,
        catalog_snapshot_date: &report.catalog_snapshot_date,
        calibration_snapshot_date: &report.calibration_snapshot_date,
        nlist: report.nlist,
        viable_count: report.candidates.len(),
        rejected_count: report.rejected.len(),
        candidates,
        rejected,
        assumptions: &report.assumptions,
    };
    let text = serde_json::to_string_pretty(&output)
        .map_err(|error| format!("failed to serialize plan JSON: {error}"))?;
    println!("{text}");
    Ok(())
}

fn parse_flags(arguments: &[String], allowed: &[&str]) -> Result<BTreeMap<String, String>, String> {
    if arguments.len() % 2 != 0 {
        return Err("every flag requires exactly one value".to_string());
    }
    let mut parsed = BTreeMap::new();
    for pair in arguments.chunks_exact(2) {
        let flag = &pair[0];
        if !flag.starts_with("--") {
            return Err(format!("expected a --flag, found {flag:?}"));
        }
        if !allowed.contains(&flag.as_str()) {
            return Err(format!("unknown flag {flag:?}"));
        }
        if parsed.insert(flag.clone(), pair[1].clone()).is_some() {
            return Err(format!("duplicate flag {flag:?}"));
        }
    }
    Ok(parsed)
}

fn required<'a>(flags: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str, String> {
    flags
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("missing required flag {name}"))
}

fn optional<'a>(flags: &'a BTreeMap<String, String>, name: &str, default: &'a str) -> &'a str {
    flags.get(name).map_or(default, String::as_str)
}

fn parse_cloud(value: &str) -> Result<Cloud, String> {
    Cloud::parse(value)
        .ok_or_else(|| format!("invalid --cloud {value:?}; expected aws, gcp, or azure"))
}

fn parse_arch(value: &str) -> Result<ArchFilter, String> {
    match value {
        "any" => Ok(ArchFilter::Any),
        "x86_64" => Ok(ArchFilter::X86_64),
        "arm64" => Ok(ArchFilter::Arm64),
        _ => Err(format!(
            "invalid --arch {value:?}; expected any, x86_64, or arm64"
        )),
    }
}

fn arch_name(arch: Arch) -> &'static str {
    match arch {
        Arch::X86_64 => "x86_64",
        Arch::Arm64 => "arm64",
    }
}

fn parse_format(value: &str) -> Result<OutputFormat, String> {
    match value {
        "table" => Ok(OutputFormat::Table),
        "json" => Ok(OutputFormat::Json),
        _ => Err(format!(
            "invalid --format {value:?}; expected table or json"
        )),
    }
}

fn parse_quantizations(value: &str) -> Result<Vec<Quantization>, String> {
    match value {
        "rabitq-2bit" => Ok(vec![Quantization::RabitqTwoBit]),
        "sq8" => Ok(vec![Quantization::Sq8]),
        "f32" => Ok(vec![Quantization::F32]),
        "all" => Ok(vec![
            Quantization::RabitqTwoBit,
            Quantization::Sq8,
            Quantization::F32,
        ]),
        _ => Err(format!(
            "invalid --quantization {value:?}; expected rabitq-2bit, sq8, f32, or all"
        )),
    }
}

fn parse_nprobes(value: &str) -> Result<Vec<usize>, String> {
    if value.is_empty() {
        return Err("--nprobe must not be empty".to_string());
    }
    value
        .split(',')
        .map(|item| parse_usize("--nprobe", item))
        .collect()
}

fn parse_usize(flag: &str, value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| format!("invalid {flag} value {value:?}: {error}"))
}

fn optional_f64(flags: &BTreeMap<String, String>, name: &str) -> Result<Option<f64>, String> {
    flags
        .get(name)
        .map(|value| {
            value
                .parse::<f64>()
                .map_err(|error| format!("invalid {name} value {value:?}: {error}"))
        })
        .transpose()
}

fn optional_yes_no(
    flags: &BTreeMap<String, String>,
    name: &str,
    default: bool,
) -> Result<bool, String> {
    match flags.get(name).map(String::as_str) {
        None => Ok(default),
        Some("yes") => Ok(true),
        Some("no") => Ok(false),
        Some(value) => Err(format!(
            "invalid {name} value {value:?}; expected yes or no"
        )),
    }
}
