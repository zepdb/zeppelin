//! Deterministic object-store performance contracts.

pub mod contract;
pub mod dataset;
pub mod depth;
pub mod injection;
pub mod report;
pub mod scenario;
pub mod scenarios;

use std::path::PathBuf;

pub use scenario::{
    run_capture_entry, run_contracts_entry, run_selftest_entry, run_stability_entry,
};

/// Phase-1 scenario names in their stable execution order.
pub const PHASE1_SCENARIOS: [&str; 3] = ["warm_query_strong", "cold_query_strong", "upsert_single"];

/// Process-level runner configuration parsed from `ZEPPELIN_PERF_*`.
#[derive(Debug, Clone)]
pub struct PerfEnv {
    pub scenarios: Vec<String>,
    pub artifact_root: PathBuf,
    pub capture: bool,
    pub selftest: Option<String>,
    pub repeats: usize,
}

impl PerfEnv {
    /// Parse the complete Phase-1 environment contract.
    #[must_use]
    pub fn from_env() -> Self {
        let scenarios = match std::env::var("ZEPPELIN_PERF_SCENARIOS") {
            Ok(raw) => {
                let parsed = raw
                    .split(',')
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                assert!(
                    !parsed.is_empty(),
                    "ZEPPELIN_PERF_SCENARIOS must name at least one scenario"
                );
                parsed
            }
            Err(std::env::VarError::NotPresent) => PHASE1_SCENARIOS
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            Err(error) => panic!("failed to read ZEPPELIN_PERF_SCENARIOS: {error}"),
        };
        for name in &scenarios {
            assert!(
                PHASE1_SCENARIOS.contains(&name.as_str()),
                "unknown performance-contract scenario: {name}"
            );
        }

        let artifact_root = std::env::var_os("ZEPPELIN_PERF_ARTIFACTS")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/perf-contract"));
        let capture = parse_flag("ZEPPELIN_PERF_CAPTURE");
        let selftest = match std::env::var("ZEPPELIN_PERF_SELFTEST") {
            Ok(value) if value.is_empty() => {
                panic!("ZEPPELIN_PERF_SELFTEST cannot be empty when present")
            }
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => panic!("failed to read ZEPPELIN_PERF_SELFTEST: {error}"),
        };
        let repeats = match std::env::var("ZEPPELIN_PERF_REPEATS") {
            Ok(raw) => raw
                .parse::<usize>()
                .unwrap_or_else(|error| panic!("invalid ZEPPELIN_PERF_REPEATS={raw}: {error}")),
            Err(std::env::VarError::NotPresent) => 8,
            Err(error) => panic!("failed to read ZEPPELIN_PERF_REPEATS: {error}"),
        };
        assert!(
            repeats > 0,
            "ZEPPELIN_PERF_REPEATS must be greater than zero"
        );

        Self {
            scenarios,
            artifact_root,
            capture,
            selftest,
            repeats,
        }
    }
}

fn parse_flag(name: &str) -> bool {
    match std::env::var(name) {
        Ok(value) if value == "1" => true,
        Ok(value) => panic!("{name} must be exactly 1 when present, got {value:?}"),
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => panic!("failed to read {name}: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "environment parsing is exercised explicitly"]
    fn phase1_catalog_has_no_duplicates() {
        let unique = PHASE1_SCENARIOS
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), PHASE1_SCENARIOS.len());
    }
}
