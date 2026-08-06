//! Strict storage, node, pricing, and target-shape profile loading.
//!
//! The profile types and validation moved to `zeppelin::sizing::profiles`
//! so the advisor binary can share them; this module re-exports them and
//! keeps the perf-contract-specific pieces: the shipped-profile registry,
//! the repo-relative loader, and the `ZEPPELIN_PERF_PROFILE` selector.

use std::collections::BTreeSet;
use std::path::PathBuf;

// Re-exported for API continuity: perf-contract code keeps importing the
// profile type family from this module even though it now lives in the lib.
#[allow(unused_imports)]
pub use zeppelin::sizing::profiles::{
    load_profile_from_path, validate_profile, ClientProfile, NodeProfile, Percentiles, Profile,
    StoragePrice, StorageProfile, WhatIfProfile,
};

pub const SHIPPED_PROFILES: [&str; 3] = [
    "minio-local-docker",
    "s3-standard-intra-region",
    "s3-3node-wikidpr",
];

#[must_use]
pub fn load_profile(name: &str) -> Profile {
    assert!(
        SHIPPED_PROFILES.contains(&name),
        "unknown performance profile {name:?}"
    );
    let path = profile_dir().join(format!("{name}.toml"));
    let profile = load_profile_from_path(&path);
    assert_eq!(
        profile.name, name,
        "profile name does not match its filename"
    );
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
