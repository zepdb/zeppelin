//! Strict storage, node, pricing, and target-shape profile loading.
//!
//! A [`Profile`] bundles everything the analytic model in
//! [`super::model`] needs to turn per-query byte/op counters into QPS,
//! latency, and dollar predictions: object-store latency and bandwidth, the
//! request price sheet, the node fleet, and the closed-loop client count.
//! Profiles are TOML documents parsed with `deny_unknown_fields`; a typo is
//! a hard error, never a silently ignored knob.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

/// One complete modeling profile: storage behavior, node fleet, and clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// Stable identifier printed in reports and matched against filenames.
    pub name: String,
    /// Object-store latency, bandwidth, and request pricing.
    pub storage: StorageProfile,
    /// The query-node fleet the model amortizes cost over.
    pub node: NodeProfile,
    /// Closed-loop client population driving the predicted load.
    pub client: ClientProfile,
    /// Optional what-if target shape rendered as a sweep table.
    #[serde(default)]
    pub whatif: Option<WhatIfProfile>,
}

/// Object-store latency, per-connection and aggregate bandwidth, and prices.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageProfile {
    /// Time-to-first-byte percentiles in milliseconds.
    pub ttfb_ms: Percentiles,
    /// Sustained throughput of one connection in decimal megabytes/second.
    #[serde(rename = "per_conn_MBps")]
    pub per_conn_mbps: f64,
    /// Aggregate throughput available to one node in decimal megabytes/second.
    #[serde(rename = "agg_MBps_per_node")]
    pub agg_mbps_per_node: f64,
    /// Request and egress price sheet.
    pub price: StoragePrice,
}

/// A p50/p99 pair, both in the same unit as their parent field.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Percentiles {
    /// Median value.
    pub p50: f64,
    /// 99th-percentile value; must be at least `p50`.
    pub p99: f64,
}

/// Object-store request and egress prices in dollars.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoragePrice {
    /// Dollars per single GET request.
    pub get_per_req: f64,
    /// Dollars per single PUT request.
    pub put_per_req: f64,
    /// Dollars per GiB of egress; zero for same-region access.
    pub egress_per_gb: f64,
}

/// The query-node fleet: size, shape, hourly price, and CPU cost per query.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeProfile {
    /// Number of identical nodes serving queries.
    pub count: usize,
    /// vCPUs per node.
    pub vcpus: usize,
    /// Memory per node in GiB.
    pub mem_gb: usize,
    /// On-demand price per node-hour in dollars.
    pub price_hr: f64,
    /// Calibrated CPU service time per query in milliseconds.
    pub cpu_ms_per_query: f64,
}

/// Closed-loop client population used by the throughput identity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProfile {
    /// Number of always-busy closed-loop clients.
    pub closed_loop_clients: usize,
}

/// A target dataset shape swept across quantizations and probe counts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WhatIfProfile {
    /// Total stored vectors.
    pub vectors: usize,
    /// Vector dimensionality.
    pub dims: usize,
    /// IVF cluster count for the target corpus.
    pub nlist: usize,
    /// Probe counts to sweep, each in `1..=nlist`.
    pub nprobe: Vec<usize>,
    /// Quantization variant names to sweep; must match `row_bytes` keys.
    pub quantization: Vec<String>,
    /// Stored bytes per row for each quantization variant.
    pub row_bytes: BTreeMap<String, usize>,
    /// Variants whose rows are marked `(EXPECTED)` pending measurement.
    pub expected_quantization: Vec<String>,
}

/// Parses and validates a profile from TOML text.
///
/// `name_hint` names the source in panic messages (a filename or a catalog
/// entry id); it is not compared against `profile.name`.
///
/// # Panics
///
/// Panics when the document does not parse strictly or fails
/// [`validate_profile`].
#[must_use]
pub fn load_profile_from_str(name_hint: &str, source: &str) -> Profile {
    let profile = toml::from_str::<Profile>(source)
        .unwrap_or_else(|error| panic!("invalid profile {name_hint}: {error}"));
    validate_profile(&profile);
    profile
}

/// Reads, parses, and validates a profile TOML file.
///
/// # Panics
///
/// Panics when the file cannot be read, does not parse strictly, or fails
/// [`validate_profile`].
#[must_use]
pub fn load_profile_from_path(path: &Path) -> Profile {
    let source = std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read profile {}: {error}", path.display()));
    load_profile_from_str(&path.display().to_string(), &source)
}

/// Rejects non-finite, non-positive, or internally inconsistent profiles.
///
/// # Panics
///
/// Panics with a field-specific message on the first violated invariant.
pub fn validate_profile(profile: &Profile) {
    let storage = &profile.storage;
    assert!(
        storage.ttfb_ms.p50.is_finite() && storage.ttfb_ms.p50 > 0.0,
        "profile {} requires finite positive storage.ttfb_ms.p50",
        profile.name
    );
    assert!(
        storage.ttfb_ms.p99.is_finite() && storage.ttfb_ms.p99 >= storage.ttfb_ms.p50,
        "profile {} requires p99 >= p50",
        profile.name
    );
    for (field, value) in [
        ("per_conn_MBps", storage.per_conn_mbps),
        ("agg_MBps_per_node", storage.agg_mbps_per_node),
    ] {
        assert!(
            value.is_finite() && value > 0.0,
            "profile {} requires finite positive {field}",
            profile.name
        );
    }
    for (field, value) in [
        ("get_per_req", storage.price.get_per_req),
        ("put_per_req", storage.price.put_per_req),
        ("egress_per_gb", storage.price.egress_per_gb),
        ("price_hr", profile.node.price_hr),
        ("cpu_ms_per_query", profile.node.cpu_ms_per_query),
    ] {
        assert!(
            value.is_finite() && value >= 0.0,
            "profile {} requires finite nonnegative {field}",
            profile.name
        );
    }
    assert!(profile.node.count > 0, "profile node count must be nonzero");
    assert!(profile.node.vcpus > 0, "profile vCPU count must be nonzero");
    assert!(profile.node.mem_gb > 0, "profile memory must be nonzero");
    assert!(
        profile.client.closed_loop_clients > 0,
        "profile closed-loop client count must be nonzero"
    );

    if let Some(whatif) = &profile.whatif {
        assert!(whatif.vectors > 0, "what-if vectors must be nonzero");
        assert!(whatif.dims > 0, "what-if dims must be nonzero");
        assert!(whatif.nlist > 0, "what-if nlist must be nonzero");
        assert!(!whatif.nprobe.is_empty(), "what-if nprobe sweep is empty");
        assert!(
            whatif
                .nprobe
                .iter()
                .all(|nprobe| *nprobe > 0 && *nprobe <= whatif.nlist),
            "what-if nprobe values must be in 1..=nlist"
        );
        let quantization = whatif.quantization.iter().collect::<BTreeSet<_>>();
        assert_eq!(
            quantization.len(),
            whatif.quantization.len(),
            "what-if quantization sweep contains duplicates"
        );
        assert_eq!(
            quantization,
            whatif.row_bytes.keys().collect::<BTreeSet<_>>(),
            "what-if row_bytes must exactly cover quantization variants"
        );
        assert!(
            whatif.row_bytes.values().all(|bytes| *bytes > 0),
            "what-if row bytes must be nonzero"
        );
        assert!(
            whatif
                .expected_quantization
                .iter()
                .all(|name| quantization.contains(name)),
            "expected quantization must be part of the sweep"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
name = "unit-minimal"

[storage]
per_conn_MBps = 80.0
agg_MBps_per_node = 400.0

[storage.ttfb_ms]
p50 = 15.0
p99 = 60.0

[storage.price]
get_per_req = 0.0000004
put_per_req = 0.000005
egress_per_gb = 0.0

[node]
count = 1
vcpus = 4
mem_gb = 16
price_hr = 0.192
cpu_ms_per_query = 8.0

[client]
closed_loop_clients = 8
"#;

    #[test]
    fn minimal_profile_parses_and_validates() {
        let profile = load_profile_from_str("unit-minimal", MINIMAL);
        assert_eq!(profile.name, "unit-minimal");
        assert!(profile.whatif.is_none());
    }

    #[test]
    #[should_panic(expected = "invalid profile unit-typo")]
    fn unknown_fields_are_rejected() {
        let source = format!("{MINIMAL}\nnot_a_field = 1\n");
        let _ = load_profile_from_str("unit-typo", &source);
    }

    #[test]
    #[should_panic(expected = "requires p99 >= p50")]
    fn inverted_percentiles_are_rejected() {
        let source = MINIMAL.replace("p99 = 60.0", "p99 = 1.0");
        let _ = load_profile_from_str("unit-inverted", &source);
    }
}
