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
