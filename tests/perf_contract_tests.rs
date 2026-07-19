mod common;
mod perf_contract;

#[tokio::test]
#[ignore = "requires MinIO and explicit performance-contract invocation"]
async fn contracts() {
    perf_contract::run_contracts_entry().await;
}

#[tokio::test]
#[ignore = "requires MinIO and explicit capture approval workflow"]
async fn capture() {
    perf_contract::run_capture_entry().await;
}

#[tokio::test]
#[ignore = "requires MinIO and intentional cost-regression injections"]
async fn perf_selftest() {
    perf_contract::run_selftest_entry().await;
}

#[tokio::test]
#[ignore = "requires MinIO and 100 isolated measurements per scenario"]
async fn depth_stability() {
    perf_contract::run_stability_entry().await;
}

#[tokio::test]
#[ignore = "requires MinIO for Tier 2 shape calibration and validation"]
async fn predict() {
    perf_contract::run_predict_entry().await;
}

#[tokio::test]
#[ignore = "requires MinIO for advisory Tier 3 latency validation"]
async fn latency_validate() {
    perf_contract::run_latency_validate_entry().await;
}

#[tokio::test]
#[ignore = "requires MinIO and explicit branching performance-contract invocation"]
async fn branching_census() {
    perf_contract::run_branching_census_entry().await;
}

#[tokio::test]
#[ignore = "requires MinIO and an explicit exhaustive ideal-analysis budget"]
async fn ideal_analysis() {
    let summary = perf_contract::run_ideal_analysis_entry().await;
    println!(
        "ideal-analysis report: {} (cycles={}, scenario_runs={})",
        summary.report.display(),
        summary.cycles_completed,
        summary.scenario_runs
    );
}
