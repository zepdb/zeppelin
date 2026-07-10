//! Strict checked-in ground-truth fixtures for Tier 2 calibration.

use std::fs;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroundTruthA {
    pub name: String,
    pub profile: String,
    pub bytes_per_query_mb: f64,
    pub gets_per_query: f64,
    pub clients: usize,
    pub measured_qps: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroundTruthB {
    pub name: String,
    pub profile: String,
    pub dataset: String,
    pub vectors: usize,
    pub dims: usize,
    pub nlist: usize,
    pub nprobe: usize,
    pub clients: usize,
    pub measured_rps: f64,
    pub mean_latency_s: f64,
    pub p95_latency_s: f64,
    pub p99_latency_s: f64,
    pub mean_precision: f64,
}

#[must_use]
pub fn load_gt_a() -> GroundTruthA {
    let truth: GroundTruthA = load("gt-a.toml");
    validate_positive("GT-A bytes_per_query_mb", truth.bytes_per_query_mb);
    validate_positive("GT-A gets_per_query", truth.gets_per_query);
    validate_positive("GT-A measured_qps", truth.measured_qps);
    assert!(truth.clients > 0, "GT-A clients must be nonzero");
    truth
}

#[must_use]
pub fn load_gt_b() -> GroundTruthB {
    let truth: GroundTruthB = load("gt-b.toml");
    assert!(truth.vectors > 0, "GT-B vectors must be nonzero");
    assert!(truth.dims > 0, "GT-B dims must be nonzero");
    assert!(truth.nlist > 0, "GT-B nlist must be nonzero");
    assert!(
        truth.nprobe > 0 && truth.nprobe <= truth.nlist,
        "GT-B nprobe must be in 1..=nlist"
    );
    assert!(truth.clients > 0, "GT-B clients must be nonzero");
    for (name, value) in [
        ("measured_rps", truth.measured_rps),
        ("mean_latency_s", truth.mean_latency_s),
        ("p95_latency_s", truth.p95_latency_s),
        ("p99_latency_s", truth.p99_latency_s),
        ("mean_precision", truth.mean_precision),
    ] {
        validate_positive(&format!("GT-B {name}"), value);
    }
    truth
}

fn load<T: serde::de::DeserializeOwned>(filename: &str) -> T {
    let path = fixture_dir().join(filename);
    let source = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read ground truth {}: {error}", path.display()));
    toml::from_str(&source)
        .unwrap_or_else(|error| panic!("invalid ground truth {}: {error}", path.display()))
}

fn validate_positive(name: &str, value: f64) {
    assert!(
        value.is_finite() && value > 0.0,
        "{name} must be finite and positive"
    );
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/perf_contract/ground_truth")
}
