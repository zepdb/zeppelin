#![allow(dead_code)]

pub mod artifacts;
pub mod chaos;
pub mod faults;
pub mod generator;
pub mod model;
pub mod ops;
pub mod oracle;
pub mod runner;
pub mod s3_oracle;
pub mod vocab;

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use self::faults::FaultProfile;
use self::model::OracleMutation;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Deterministic,
    Chaos,
    Mixed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedAssignment {
    pub mode: RunMode,
    pub profile: Option<FaultProfile>,
}

/// Resolves the stable per-seed execution slot used by smoke and overnight
/// runs. An explicit profile always wins; otherwise Mixed mode preserves the
/// historical meanings of seeds 0, 1, and 2 before rotating new profiles.
#[must_use]
pub fn effective_seed_assignment(
    mode: RunMode,
    forced_profile: Option<FaultProfile>,
    seed: u64,
) -> SeedAssignment {
    if let Some(profile) = forced_profile {
        return SeedAssignment {
            mode: RunMode::Chaos,
            profile: Some(profile),
        };
    }

    match mode {
        RunMode::Deterministic => SeedAssignment {
            mode: RunMode::Deterministic,
            profile: None,
        },
        RunMode::Chaos => SeedAssignment {
            mode: RunMode::Chaos,
            profile: Some(FaultProfile::LegacyChaos),
        },
        RunMode::Mixed => match seed % 9 {
            0 | 2 => SeedAssignment {
                mode: RunMode::Deterministic,
                profile: None,
            },
            1 => SeedAssignment {
                mode: RunMode::Chaos,
                profile: Some(FaultProfile::LegacyChaos),
            },
            3 => scheduled(FaultProfile::PostCommit),
            4 => scheduled(FaultProfile::Network),
            5 => scheduled(FaultProfile::Crash),
            6 => scheduled(FaultProfile::Clock),
            7 => scheduled(FaultProfile::Sched),
            8 => scheduled(FaultProfile::SupportedFull),
            residue => unreachable!("seed modulo 9 produced residue {residue}"),
        },
    }
}

fn scheduled(profile: FaultProfile) -> SeedAssignment {
    SeedAssignment {
        mode: RunMode::Chaos,
        profile: Some(profile),
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PreserveMode {
    Always,
    OnFailure,
    Never,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerEnv {
    pub seconds: u64,
    pub seeds: Vec<u64>,
    pub max_ops: Option<u64>,
    pub artifacts: PathBuf,
    pub preserve: PreserveMode,
    pub selftest: Option<OracleMutation>,
    pub mode: RunMode,
    pub profile: Option<FaultProfile>,
    pub env_echo: BTreeMap<String, String>,
}

impl RunnerEnv {
    #[must_use]
    pub fn from_env() -> Self {
        let seconds = parse_u64_env("ZEPPELIN_ADVERSARIAL_SECONDS").unwrap_or(60);
        let seeds = parse_seeds();
        let max_ops = parse_u64_env("ZEPPELIN_ADVERSARIAL_MAX_OPS");
        let artifacts = std::env::var("ZEPPELIN_ADVERSARIAL_ARTIFACTS")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("target/adversarial"));
        let preserve = match std::env::var("ZEPPELIN_ADVERSARIAL_PRESERVE")
            .unwrap_or_else(|_| "on-failure".to_string())
            .as_str()
        {
            "always" => PreserveMode::Always,
            "on-failure" => PreserveMode::OnFailure,
            "never" => PreserveMode::Never,
            other => panic!("invalid ZEPPELIN_ADVERSARIAL_PRESERVE: {other}"),
        };
        let raw_mode =
            std::env::var("ZEPPELIN_ADVERSARIAL_MODE").unwrap_or_else(|_| "mixed".to_string());
        let mode = match raw_mode.as_str() {
            "deterministic" => RunMode::Deterministic,
            "chaos" => RunMode::Chaos,
            "mixed" => RunMode::Mixed,
            other => panic!("invalid ZEPPELIN_ADVERSARIAL_MODE: {other}"),
        };
        let selftest = std::env::var("ZEPPELIN_ADVERSARIAL_SELFTEST")
            .ok()
            .map(|value| OracleMutation::from_key(&value));
        let raw_profile = std::env::var("ZEPPELIN_ADVERSARIAL_PROFILE").ok();
        let profile = raw_profile.as_deref().map(FaultProfile::from_env);

        let mut env_echo = BTreeMap::new();
        env_echo.insert(
            "TEST_BACKEND".to_string(),
            std::env::var("TEST_BACKEND").unwrap_or_else(|_| "memory".to_string()),
        );
        env_echo.insert(
            "ZEPPELIN_ADVERSARIAL_SECONDS".to_string(),
            seconds.to_string(),
        );
        env_echo.insert(
            "ZEPPELIN_ADVERSARIAL_SEEDS".to_string(),
            seeds
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
        env_echo.insert(
            "ZEPPELIN_ADVERSARIAL_MAX_OPS".to_string(),
            max_ops
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unset".to_string()),
        );
        env_echo.insert(
            "ZEPPELIN_ADVERSARIAL_ARTIFACTS".to_string(),
            artifacts.display().to_string(),
        );
        env_echo.insert(
            "ZEPPELIN_ADVERSARIAL_PRESERVE".to_string(),
            format!("{preserve:?}"),
        );
        env_echo.insert("ZEPPELIN_ADVERSARIAL_MODE".to_string(), raw_mode);
        env_echo.insert(
            "ZEPPELIN_ADVERSARIAL_PROFILE".to_string(),
            raw_profile.unwrap_or_else(|| "unset".to_string()),
        );
        env_echo.insert(
            "ZEPPELIN_ADVERSARIAL_SELFTEST".to_string(),
            selftest
                .map(|mutation| mutation.key().to_string())
                .unwrap_or_else(|| "unset".to_string()),
        );

        Self {
            seconds,
            seeds,
            max_ops,
            artifacts,
            preserve,
            selftest,
            mode,
            profile,
            env_echo,
        }
    }

    #[must_use]
    pub fn for_oracle_selftest(&self, seed: u64) -> Self {
        let mut clone = self.clone();
        clone.seconds = 30;
        clone.seeds = vec![seed];
        clone.max_ops = Some(80);
        clone.preserve = PreserveMode::Never;
        clone.mode = RunMode::Deterministic;
        clone
    }
}

fn parse_u64_env(name: &str) -> Option<u64> {
    std::env::var(name).ok().map(|value| {
        value
            .parse::<u64>()
            .unwrap_or_else(|error| panic!("invalid {name}={value:?}: {error}"))
    })
}

fn parse_seeds() -> Vec<u64> {
    if let Some(seed) = parse_u64_env("ZEPPELIN_ADVERSARIAL_SEED") {
        return vec![seed];
    }

    let raw = std::env::var("ZEPPELIN_ADVERSARIAL_SEEDS").unwrap_or_else(|_| "3".to_string());
    if raw.contains(',') {
        let seeds: Vec<u64> = raw
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(|part| {
                part.parse::<u64>()
                    .unwrap_or_else(|error| panic!("invalid seed {part:?}: {error}"))
            })
            .collect();
        assert!(
            !seeds.is_empty(),
            "ZEPPELIN_ADVERSARIAL_SEEDS comma list cannot be empty"
        );
        return seeds;
    }

    let count = raw
        .parse::<u64>()
        .unwrap_or_else(|error| panic!("invalid ZEPPELIN_ADVERSARIAL_SEEDS={raw:?}: {error}"));
    assert!(count > 0, "ZEPPELIN_ADVERSARIAL_SEEDS count must be > 0");
    (0..count).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_mixed_assigns_only_supported_v1_fault_profiles() {
        for seed in 0..240 {
            let assignment = effective_seed_assignment(RunMode::Mixed, None, seed);
            if let Some(profile) = assignment.profile {
                let schedule = faults::FaultScheduler::for_seed(seed, profile);
                assert!(
                    schedule
                        .schedule()
                        .events
                        .iter()
                        .all(|event| event.contract_class() == faults::ContractClass::SupportedV1),
                    "seed {seed} assigned unsupported profile {profile:?}"
                );
            }
        }
        assert!((0..240).any(|seed| {
            effective_seed_assignment(RunMode::Mixed, None, seed).profile
                == Some(FaultProfile::SupportedFull)
        }));
    }
}
