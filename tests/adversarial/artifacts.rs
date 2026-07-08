use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use zeppelin::config::Config;
use zeppelin::storage::ZeppelinStore;

use super::generator::Coverage;
use super::model::Model;
use super::ops::{NamespaceSpec, OpRecord};
use super::oracle::Violation;
use super::{RunMode, RunnerEnv};

#[derive(Debug)]
pub struct RunArtifacts {
    root: PathBuf,
}

#[derive(Debug)]
pub struct SeedArtifacts {
    pub dir: PathBuf,
    ops: BufWriter<File>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FailureManifest {
    pub seed: u64,
    pub mode: RunMode,
    pub op_index: u64,
    pub violations: Vec<Violation>,
    pub preserved_prefix: String,
    pub repro_cmd: String,
    pub inspect_cmd: String,
}

impl RunArtifacts {
    pub fn create(env: &RunnerEnv) -> Self {
        fs::create_dir_all(&env.artifacts).unwrap_or_else(|error| {
            panic!(
                "failed to create adversarial artifact root {}: {error}",
                env.artifacts.display()
            )
        });
        let run_id = format!(
            "run-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before UNIX epoch")
                .as_secs()
        );
        let root = env.artifacts.join(run_id);
        fs::create_dir_all(&root).unwrap_or_else(|error| {
            panic!(
                "failed to create adversarial run dir {}: {error}",
                root.display()
            )
        });
        let run_json = serde_json::json!({
            "git_rev": git_rev(),
            "backend": env.env_echo.get("TEST_BACKEND"),
            "env": env.env_echo,
            "seeds": env.seeds,
            "mode": env.mode,
        });
        write_json(root.join("run.json"), &run_json);
        Self { root }
    }

    pub fn seed(
        &self,
        seed: u64,
        config: &Config,
        specs: &BTreeMap<String, NamespaceSpec>,
        mode: RunMode,
    ) -> SeedArtifacts {
        let dir = self.root.join(format!("seed-{seed}"));
        fs::create_dir_all(&dir)
            .unwrap_or_else(|error| panic!("failed to create seed dir {}: {error}", dir.display()));
        write_json(
            dir.join("config.json"),
            &serde_json::json!({
                "seed": seed,
                "mode": mode,
                "config": config,
                "namespace_specs": specs,
            }),
        );
        let ops_file = File::create(dir.join("ops.jsonl")).unwrap_or_else(|error| {
            panic!("failed to create ops.jsonl in {}: {error}", dir.display())
        });
        SeedArtifacts {
            dir,
            ops: BufWriter::new(ops_file),
        }
    }
}

impl SeedArtifacts {
    pub fn write_op(&mut self, rec: &OpRecord) {
        let encoded = serde_json::to_string(rec).expect("OpRecord must serialize");
        let _: OpRecord =
            serde_json::from_str(&encoded).expect("OpRecord JSONL line must deserialize");
        writeln!(self.ops, "{encoded}").expect("failed to write ops.jsonl line");
        self.ops.flush().expect("failed to flush ops.jsonl line");
    }

    pub fn write_model_final(&self, model: &Model) {
        write_json(self.dir.join("model-final.json"), model);
    }

    pub async fn write_s3_final(&self, store: &ZeppelinStore, namespaces: &[String]) {
        let mut output = String::new();
        for ns in namespaces {
            output.push_str(&format!("# {ns}\n"));
            let mut keys = store
                .list_prefix(&format!("{ns}/"))
                .await
                .unwrap_or_else(|error| panic!("failed to list S3 keys for {ns}: {error}"));
            keys.sort();
            for key in keys {
                output.push_str(&key);
                output.push('\n');
            }
        }
        fs::write(self.dir.join("s3-final.txt"), output)
            .expect("failed to write s3-final.txt artifact");
    }

    pub fn write_failure(&self, failure: &FailureManifest) {
        write_json(self.dir.join("failure.json"), failure);
    }

    pub fn write_coverage(&self, coverage: &Coverage) {
        write_json(self.dir.join("coverage.json"), coverage);
    }
}

fn write_json(path: impl AsRef<Path>, value: &impl Serialize) {
    let bytes = serde_json::to_vec_pretty(value).expect("artifact JSON must serialize");
    fs::write(path.as_ref(), bytes).unwrap_or_else(|error| {
        panic!(
            "failed to write artifact {}: {error}",
            path.as_ref().display()
        )
    });
}

fn git_rev() -> String {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("failed to run git rev-parse HEAD");
    assert!(
        output.status.success(),
        "git rev-parse HEAD failed with status {}",
        output.status
    );
    String::from_utf8(output.stdout)
        .expect("git rev-parse HEAD emitted non-UTF8")
        .trim()
        .to_string()
}
