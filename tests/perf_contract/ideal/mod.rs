//! Exhaustive, source-derived S3 path analysis for the dedicated perf harness.

mod artifacts;
mod catalog;
mod direct;
mod http;
mod inventory;
mod maintenance;
mod namespace;
mod observe;
mod query;
mod runner;
mod variant_compaction;
mod variant_query;

pub use runner::IdealRunSummary;

pub async fn run_ideal_analysis_entry() -> IdealRunSummary {
    runner::run_ideal_analysis_entry().await
}
