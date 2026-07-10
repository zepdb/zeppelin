//! Strict storage, node, pricing, and target-shape profile loading.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const SHIPPED_PROFILES: [&str; 3] = [
    "minio-local-docker",
    "s3-standard-intra-region",
    "s3-3node-wikidpr",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub name: String,
    pub storage: StorageProfile,
    pub node: NodeProfile,
    pub client: ClientProfile,
    #[serde(default)]
    pub whatif: Option<WhatIfProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageProfile {
    pub ttfb_ms: Percentiles,
    #[serde(rename = "per_conn_MBps")]
    pub per_conn_mbps: f64,
    #[serde(rename = "agg_MBps_per_node")]
    pub agg_mbps_per_node: f64,
    pub price: StoragePrice,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Percentiles {
    pub p50: f64,
    pub p99: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoragePrice {
    pub get_per_req: f64,
    pub put_per_req: f64,
    pub egress_per_gb: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeProfile {
    pub count: usize,
    pub vcpus: usize,
    pub mem_gb: usize,
    pub price_hr: f64,
    pub cpu_ms_per_query: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientProfile {
    pub closed_loop_clients: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WhatIfProfile {
    pub vectors: usize,
    pub dims: usize,
    pub nlist: usize,
    pub nprobe: Vec<usize>,
    pub quantization: Vec<String>,
    pub row_bytes: BTreeMap<String, usize>,
    pub expected_quantization: Vec<String>,
}

#[must_use]
pub fn load_profile(name: &str) -> Profile {
    assert!(
        SHIPPED_PROFILES.contains(&name),
        "unknown performance profile {name:?}"
    );
    let path = profile_dir().join(format!("{name}.toml"));
    let source = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read profile {}: {error}", path.display()));
    let profile = toml::from_str::<Profile>(&source)
        .unwrap_or_else(|error| panic!("invalid profile {}: {error}", path.display()));
    assert_eq!(
        profile.name, name,
        "profile name does not match its filename"
    );
    validate_profile(&profile);
    profile
}

#[must_use]
pub fn selected_profiles() -> Vec<Profile> {
    let names = match std::env::var("ZEPPELIN_PERF_PROFILE") {
        Ok(raw) => {
            let names = raw
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            assert!(
                !names.is_empty(),
                "ZEPPELIN_PERF_PROFILE must select at least one profile"
            );
            names
        }
        Err(std::env::VarError::NotPresent) => SHIPPED_PROFILES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        Err(error) => panic!("failed to read ZEPPELIN_PERF_PROFILE: {error}"),
    };
    let unique = names.iter().collect::<BTreeSet<_>>();
    assert_eq!(
        unique.len(),
        names.len(),
        "ZEPPELIN_PERF_PROFILE contains duplicate names"
    );
    names.into_iter().map(|name| load_profile(&name)).collect()
}

fn validate_profile(profile: &Profile) {
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

fn profile_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/perf_contract/profiles")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_shipped_profiles_parse_strictly() {
        for name in SHIPPED_PROFILES {
            let profile = load_profile(name);
            assert_eq!(profile.name, name);
        }
    }
}
