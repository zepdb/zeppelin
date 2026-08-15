//! Deterministic object-store performance contracts.

pub mod branching;
pub mod contract;
pub mod dataset;
pub mod depth;
pub mod ground_truth;
pub mod ideal;
pub mod injection;
pub mod latency;
pub mod predict;
pub mod profiles;
pub mod report;
pub mod scenario;
pub mod scenarios;
mod security;

use std::path::PathBuf;

pub use branching::run_branching_census_entry;
pub use ideal::run_ideal_analysis_entry;
pub use latency::run_latency_validate_entry;
pub use predict::run_predict_entry;
pub use scenario::{
    run_capture_entry, run_contracts_entry, run_selftest_entry, run_stability_entry,
};

/// Phase-1 scenario names in their stable execution order.
pub const PHASE1_SCENARIOS: [&str; 3] = ["warm_query_strong", "cold_query_strong", "upsert_single"];

/// Phase-2 scenario names in their stable execution order.
pub const PHASE2_SCENARIOS: [&str; 14] = [
    "warm_query_eventual",
    "filtered_query",
    "filtered_query_bitmap",
    "fts_query",
    "hybrid_query",
    "as_of_query",
    "paginate",
    "fetch_strong",
    "upsert_batch",
    "delete_single",
    "compaction_cycle",
    "compaction_incremental",
    "gc_cycle",
    "hydration",
];

/// Security Phase-1 scenarios measured and frozen after central auth lands.
pub const SECURITY_PHASE1_SCENARIOS: [&str; 1] = ["secured_query"];

/// Security Phase-4 scenarios measured after mandatory constraints land.
pub const SECURITY_PHASE4_SCENARIOS: [&str; 1] = ["secured_filtered_query"];

/// Complete Tier-1 catalog run by default and in gating CI.
pub const ALL_SCENARIOS: [&str; 20] = [
    "warm_query_strong",
    "cold_query_strong",
    "upsert_single",
    "warm_query_eventual",
    "filtered_query",
    "filtered_query_bitmap",
    "fts_query",
    "hybrid_query",
    "as_of_query",
    "paginate",
    "fetch_strong",
    "upsert_batch",
    "delete_single",
    "compaction_cycle",
    "compaction_incremental",
    "gc_cycle",
    "hydration",
    "cold_query_sketch_adc",
    "secured_query",
    "secured_filtered_query",
];

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
    /// Parse the complete Tier-1 environment contract.
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
            Err(std::env::VarError::NotPresent) => ALL_SCENARIOS
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            Err(error) => panic!("failed to read ZEPPELIN_PERF_SCENARIOS: {error}"),
        };
        for name in &scenarios {
            assert!(
                ALL_SCENARIOS.contains(&name.as_str()),
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

pub(crate) fn require_minio() {
    let backend = match std::env::var("TEST_BACKEND") {
        Ok(backend) => Some(backend),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("failed to read TEST_BACKEND: {error}"),
    };
    let grouping = match std::env::var("ZEPPELIN_MAX_CLUSTERS_PER_OBJECT") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("failed to read ZEPPELIN_MAX_CLUSTERS_PER_OBJECT: {error}"),
    };
    if let Err(error) = validate_minio_environment(backend.as_deref(), grouping.as_deref()) {
        panic!("{error}");
    }
}

/// CAS-capable real object stores the performance contracts may run against.
/// Baselines were calibrated on MinIO; emulator runs exercise the same
/// contract shape (op-count censuses are substrate-neutral).
const CAS_BACKENDS: &[&str] = &["minio", "s3", "gcs", "azurite"];

fn validate_minio_environment(
    backend: Option<&str>,
    grouping_override: Option<&str>,
) -> Result<(), String> {
    match backend {
        Some(backend) if CAS_BACKENDS.contains(&backend) => {}
        Some(backend) => {
            return Err(format!(
                "performance contracts require a CAS-capable real store \
                 (TEST_BACKEND one of {CAS_BACKENDS:?}), got {backend:?}"
            ));
        }
        None => {
            return Err(format!(
                "performance contracts require a CAS-capable real store \
                 (TEST_BACKEND one of {CAS_BACKENDS:?})"
            ))
        }
    }
    if let Some(value) = grouping_override {
        return Err(format!(
            "performance contracts require ZEPPELIN_MAX_CLUSTERS_PER_OBJECT \
             to be unset, got {value:?}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_catalog_has_no_duplicates_and_keeps_phase1_prefix() {
        let unique = ALL_SCENARIOS
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), ALL_SCENARIOS.len());
        assert_eq!(&ALL_SCENARIOS[..PHASE1_SCENARIOS.len()], &PHASE1_SCENARIOS);
        let phase2_start = PHASE1_SCENARIOS.len();
        let phase2_end = phase2_start + PHASE2_SCENARIOS.len();
        assert_eq!(&ALL_SCENARIOS[phase2_start..phase2_end], &PHASE2_SCENARIOS);
        assert_eq!(ALL_SCENARIOS[phase2_end], "cold_query_sketch_adc");
        let phase1_security_start = phase2_end + 1;
        let phase1_security_end = phase1_security_start + SECURITY_PHASE1_SCENARIOS.len();
        assert_eq!(
            &ALL_SCENARIOS[phase1_security_start..phase1_security_end],
            &SECURITY_PHASE1_SCENARIOS
        );
        assert_eq!(
            &ALL_SCENARIOS[phase1_security_end..],
            &SECURITY_PHASE4_SCENARIOS
        );
        assert!(
            ALL_SCENARIOS.contains(&"secured_query"),
            "the Phase 1 security gate requires a frozen secured_query scenario"
        );
    }

    #[test]
    fn cluster_grouping_override_is_rejected() {
        let error = validate_minio_environment(Some("minio"), Some("4")).unwrap_err();
        assert_eq!(
            error,
            "performance contracts require ZEPPELIN_MAX_CLUSTERS_PER_OBJECT \
             to be unset, got \"4\""
        );
    }

    #[test]
    fn secured_query_contract_freezes_phase1_security_budget() {
        let contract = contract::load_contract("secured_query")
            .expect("secured_query must have a valid checked-in contract");
        let security = contract
            .assertions
            .security
            .expect("secured_query must freeze security assertions");
        assert_eq!(security.baseline_scenario, "warm_query_strong");
        assert_eq!(security.authn_authz_p50_delta_ns_max, 10_000);
        assert_eq!(
            security.delegated_authn_authz_p50_delta_ns_max,
            Some(80_000)
        );
        assert_eq!(security.added_get_ops, 0);
        assert_eq!(security.added_put_ops, 0);
    }

    #[test]
    fn secured_filtered_query_contract_tracks_the_caller_filtered_baseline() {
        let contract = contract::load_contract("secured_filtered_query").unwrap_or_else(|error| {
            panic!("secured_filtered_query must have a valid checked-in contract: {error}")
        });
        let Some(security) = contract.assertions.security else {
            panic!("secured_filtered_query must freeze security assertions");
        };

        assert_eq!(security.baseline_scenario, "filtered_query");
        assert_eq!(
            security.mandatory_filter,
            Some(contract::FilterMeasure {
                field: "cat".to_string(),
                value: "c0".to_string(),
            })
        );
        assert_eq!(security.query_p50_regression_percent_max, Some(5));
        assert_eq!(security.added_get_ops, 0);
        assert_eq!(security.added_put_ops, 0);
        assert!(
            matches!(
                contract.run.measure,
                contract::MeasureSpec::Query { filter: None, .. }
            ),
            "the secured scenario must supply its equivalent filter through policy"
        );
    }
}
