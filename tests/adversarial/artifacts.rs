use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use zeppelin::config::Config;
use zeppelin::error::ZeppelinError;
use zeppelin::storage::ZeppelinStore;

use crate::common::counting::ClassStats;

use super::chaos::FiredFault;
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
    op_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FailureManifest {
    pub seed: u64,
    pub mode: RunMode,
    pub op_index: u64,
    pub violations: Vec<Violation>,
    pub preserved_prefix: String,
    pub fault_plan: Option<String>,
    pub repro_cmd: String,
    pub inspect_cmd: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct S3CaptureMetadata {
    objects: Vec<S3ObjectMeta>,
    capture_errors: Vec<S3CaptureError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SeedReport {
    pub seed: u64,
    pub mode: RunMode,
    pub dir: PathBuf,
    pub failed: bool,
    pub ops: u64,
    pub compactions: u64,
    pub violations: Vec<Violation>,
    pub wall_secs: f64,
    pub object_store: BTreeMap<String, ClassStats>,
    pub fired_faults: Vec<FiredFault>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3ObjectMeta {
    key: String,
    size: Option<u64>,
    captured: bool,
    missing: bool,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3CaptureError {
    namespace: Option<String>,
    key: Option<String>,
    operation: String,
    error: String,
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
        let mode_assignment = env
            .seeds
            .iter()
            .map(|seed| {
                let mode = match env.mode {
                    RunMode::Deterministic => RunMode::Deterministic,
                    RunMode::Chaos => RunMode::Chaos,
                    RunMode::Mixed if seed % 3 == 1 => RunMode::Chaos,
                    RunMode::Mixed => RunMode::Deterministic,
                };
                (seed.to_string(), mode)
            })
            .collect::<BTreeMap<_, _>>();
        let run_json = serde_json::json!({
            "git_rev": git_rev(),
            "dirty_tree": git_dirty(),
            "backend": env.env_echo.get("TEST_BACKEND"),
            "env": env.env_echo,
            "seeds": env.seeds,
            "mode": env.mode,
            "mode_assignment": mode_assignment,
        });
        write_json(root.join("run.json"), &run_json);
        Self { root }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[allow(clippy::too_many_arguments)]
    pub fn seed(
        &self,
        seed: u64,
        config: &Config,
        specs: &BTreeMap<String, NamespaceSpec>,
        mode: RunMode,
        fault_plan: Option<&str>,
        selftest_probe: Option<&str>,
        chaos_plan: Option<&serde_json::Value>,
    ) -> SeedArtifacts {
        let dir = self.root.join(format!("seed-{seed}"));
        fs::create_dir_all(&dir)
            .unwrap_or_else(|error| panic!("failed to create seed dir {}: {error}", dir.display()));
        write_json(
            dir.join("config.json"),
            &serde_json::json!({
                "seed": seed,
                "mode": mode,
                "fault_plan": fault_plan,
                "selftest_probe": selftest_probe,
                "chaos_plan": chaos_plan,
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
            op_count: 0,
        }
    }

    pub fn write_report(
        &self,
        env: &RunnerEnv,
        seeds: &[SeedReport],
        coverage: &Coverage,
        update_latest: bool,
    ) {
        let report = build_report(&self.root, env, seeds, coverage);
        let report_path = self.root.join("report.md");
        fs::write(&report_path, &report).unwrap_or_else(|error| {
            panic!("failed to write report {}: {error}", report_path.display())
        });
        if update_latest {
            fs::create_dir_all("tasks").expect("failed to create tasks directory");
            fs::write("tasks/overnight-adversarial-report.md", report)
                .expect("failed to update tasks/overnight-adversarial-report.md");
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
        self.op_count += 1;
    }

    #[must_use]
    pub fn op_count(&self) -> u64 {
        self.op_count
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
                let size = store
                    .head(&key)
                    .await
                    .unwrap_or_else(|error| panic!("failed to head S3 key {key}: {error}"))
                    .size;
                output.push_str(&format!("{key}\t{size}\n"));
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

    pub fn write_faults(&self, faults: &[FiredFault]) {
        let path = self.dir.join("faults.jsonl");
        let file = File::create(&path)
            .unwrap_or_else(|error| panic!("failed to create {}: {error}", path.display()));
        let mut writer = BufWriter::new(file);
        for fault in faults {
            let encoded = serde_json::to_string(fault).expect("FiredFault must serialize");
            writeln!(writer, "{encoded}")
                .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
        }
        writer
            .flush()
            .unwrap_or_else(|error| panic!("failed to flush {}: {error}", path.display()));
    }

    pub async fn capture_s3_metadata(
        &self,
        store: &ZeppelinStore,
        namespaces: &[String],
        dump_full: bool,
    ) {
        let capture_dir = self.dir.join("s3-capture");
        if let Err(error) = fs::create_dir_all(&capture_dir) {
            eprintln!(
                "failed to create S3 capture dir {}: {error}",
                capture_dir.display()
            );
            return;
        }
        let objects_dir = capture_dir.join("objects");
        let mut metadata = S3CaptureMetadata::default();
        if let Err(error) = fs::create_dir_all(&objects_dir) {
            metadata.capture_errors.push(S3CaptureError {
                namespace: None,
                key: None,
                operation: "create_objects_dir".to_string(),
                error: format!(
                    "failed to create S3 object capture dir {}: {error}",
                    objects_dir.display()
                ),
            });
        }

        for ns in namespaces {
            let mut keys = match store.list_prefix(&format!("{ns}/")).await {
                Ok(keys) => keys,
                Err(error) => {
                    metadata.capture_errors.push(S3CaptureError {
                        namespace: Some(ns.clone()),
                        key: None,
                        operation: "list_prefix".to_string(),
                        error: error.to_string(),
                    });
                    continue;
                }
            };
            keys.sort();
            for key in keys {
                let should_capture = dump_full || is_control_plane_key(ns, &key);
                if should_capture {
                    let object_meta = capture_full_object(
                        store,
                        &objects_dir,
                        ns,
                        &key,
                        &mut metadata.capture_errors,
                    )
                    .await;
                    metadata.objects.push(object_meta);
                } else {
                    let object_meta =
                        capture_head_only(store, ns, &key, &mut metadata.capture_errors).await;
                    metadata.objects.push(object_meta);
                }
            }
        }
        if let Err(error) = try_write_json(capture_dir.join("metadata.json"), &metadata) {
            eprintln!(
                "failed to write S3 capture metadata under {}: {error}",
                capture_dir.display()
            );
        }
    }
}

async fn capture_full_object(
    store: &ZeppelinStore,
    objects_dir: &Path,
    namespace: &str,
    key: &str,
    capture_errors: &mut Vec<S3CaptureError>,
) -> S3ObjectMeta {
    match store.get(key).await {
        Ok(bytes) => {
            let size = bytes.len() as u64;
            let path = objects_dir.join(key);
            let mut entry = S3ObjectMeta {
                key: key.to_string(),
                size: Some(size),
                captured: true,
                missing: false,
                error: None,
            };
            if let Some(parent) = path.parent() {
                if let Err(error) = fs::create_dir_all(parent) {
                    let message = format!(
                        "failed to create S3 capture parent {}: {error}",
                        parent.display()
                    );
                    record_capture_error(
                        capture_errors,
                        Some(namespace),
                        Some(key),
                        "create_parent_dir",
                        message.clone(),
                    );
                    entry.captured = false;
                    entry.error = Some(message);
                    return entry;
                }
            }
            if let Err(error) = fs::write(&path, bytes) {
                let message = format!(
                    "failed to write captured S3 key {}: {error}",
                    path.display()
                );
                record_capture_error(
                    capture_errors,
                    Some(namespace),
                    Some(key),
                    "write_object",
                    message.clone(),
                );
                entry.captured = false;
                entry.error = Some(message);
            }
            entry
        }
        Err(error) if is_not_found(&error) => missing_object_meta(key, error),
        Err(error) => errored_object_meta(namespace, key, "get", error, capture_errors),
    }
}

async fn capture_head_only(
    store: &ZeppelinStore,
    namespace: &str,
    key: &str,
    capture_errors: &mut Vec<S3CaptureError>,
) -> S3ObjectMeta {
    match store.head(key).await {
        Ok(meta) => S3ObjectMeta {
            key: key.to_string(),
            size: Some(meta.size as u64),
            captured: false,
            missing: false,
            error: None,
        },
        Err(error) if is_not_found(&error) => missing_object_meta(key, error),
        Err(error) => errored_object_meta(namespace, key, "head", error, capture_errors),
    }
}

fn missing_object_meta(key: &str, error: ZeppelinError) -> S3ObjectMeta {
    S3ObjectMeta {
        key: key.to_string(),
        size: None,
        captured: false,
        missing: true,
        error: Some(error.to_string()),
    }
}

fn errored_object_meta(
    namespace: &str,
    key: &str,
    operation: &str,
    error: ZeppelinError,
    capture_errors: &mut Vec<S3CaptureError>,
) -> S3ObjectMeta {
    let message = error.to_string();
    record_capture_error(
        capture_errors,
        Some(namespace),
        Some(key),
        operation,
        message.clone(),
    );
    S3ObjectMeta {
        key: key.to_string(),
        size: None,
        captured: false,
        missing: false,
        error: Some(message),
    }
}

fn record_capture_error(
    capture_errors: &mut Vec<S3CaptureError>,
    namespace: Option<&str>,
    key: Option<&str>,
    operation: &str,
    error: String,
) {
    capture_errors.push(S3CaptureError {
        namespace: namespace.map(str::to_string),
        key: key.map(str::to_string),
        operation: operation.to_string(),
        error,
    });
}

fn is_not_found(error: &ZeppelinError) -> bool {
    matches!(error, ZeppelinError::NotFound { .. })
}

pub fn read_seed_config(path: &Path) -> serde_json::Value {
    let bytes = fs::read(path.join("config.json")).unwrap_or_else(|error| {
        panic!("failed to read seed config in {}: {error}", path.display())
    });
    serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!("failed to parse seed config in {}: {error}", path.display())
    })
}

pub fn read_ops(path: &Path) -> Vec<OpRecord> {
    let text = fs::read_to_string(path.join("ops.jsonl"))
        .unwrap_or_else(|error| panic!("failed to read ops.jsonl in {}: {error}", path.display()));
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(idx, line)| {
            serde_json::from_str::<OpRecord>(line).unwrap_or_else(|error| {
                panic!(
                    "failed to parse ops.jsonl line {} in {}: {error}",
                    idx + 1,
                    path.display()
                )
            })
        })
        .collect()
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

fn try_write_json(path: impl AsRef<Path>, value: &impl Serialize) -> std::io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value).expect("artifact JSON must serialize");
    fs::write(path.as_ref(), bytes)
}

fn is_control_plane_key(namespace: &str, key: &str) -> bool {
    let Some(suffix) = key.strip_prefix(&format!("{namespace}/")) else {
        return false;
    };
    suffix == "manifest.json"
        || suffix == "lease.json"
        || suffix == "_gc/candidates.json"
        || suffix.starts_with("manifests/")
        || suffix.starts_with("snapshots/")
        || suffix.starts_with("_staging/")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fmt;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
    };

    use super::*;

    #[tokio::test]
    async fn capture_s3_metadata_records_missing_listed_key() {
        let temp_dir = tempfile::tempdir().expect("failed to create artifact temp dir");
        let ops = BufWriter::new(
            File::create(temp_dir.path().join("ops.jsonl")).expect("failed to create ops file"),
        );
        let artifacts = SeedArtifacts {
            dir: temp_dir.path().to_path_buf(),
            ops,
            op_count: 0,
        };
        let ns = "ns";
        let key = format!("{ns}/manifest.json");
        let inner = Arc::new(InMemory::new());
        let seed_store = ZeppelinStore::new(inner.clone());
        seed_store
            .put(&key, Bytes::from_static(b"{}"))
            .await
            .expect("failed to seed listed key");
        let capture_store = ZeppelinStore::new(Arc::new(DeleteOnListStore {
            inner,
            delete_on_list: Arc::new(Mutex::new(BTreeSet::from([key.clone()]))),
        }));

        artifacts
            .capture_s3_metadata(&capture_store, &[ns.to_string()], true)
            .await;

        let bytes = fs::read(temp_dir.path().join("s3-capture/metadata.json"))
            .expect("metadata.json should be written");
        let metadata: S3CaptureMetadata =
            serde_json::from_slice(&bytes).expect("metadata should decode");
        assert!(metadata.capture_errors.is_empty(), "{metadata:#?}");
        assert_eq!(metadata.objects.len(), 1, "{metadata:#?}");
        assert_eq!(metadata.objects[0].key, key);
        assert!(metadata.objects[0].missing, "{metadata:#?}");
        assert!(!metadata.objects[0].captured, "{metadata:#?}");
        assert!(metadata.objects[0].size.is_none(), "{metadata:#?}");
        assert!(
            metadata.objects[0]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("object not found")),
            "{metadata:#?}"
        );
    }

    #[derive(Debug)]
    struct DeleteOnListStore {
        inner: Arc<dyn ObjectStore>,
        delete_on_list: Arc<Mutex<BTreeSet<String>>>,
    }

    impl fmt::Display for DeleteOnListStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "DeleteOnListStore({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for DeleteOnListStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOpts,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn head(&self, location: &ObjectPath) -> OsResult<ObjectMeta> {
            self.inner.head(location).await
        }

        async fn delete(&self, location: &ObjectPath) -> OsResult<()> {
            self.inner.delete(location).await
        }

        fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'_, OsResult<ObjectMeta>> {
            let inner = self.inner.clone();
            let delete_on_list = self.delete_on_list.clone();
            self.inner
                .list(prefix)
                .then(move |result| {
                    let inner = inner.clone();
                    let delete_on_list = delete_on_list.clone();
                    async move {
                        if let Ok(meta) = &result {
                            let should_delete = {
                                let mut delete_on_list =
                                    delete_on_list.lock().expect("delete set mutex poisoned");
                                delete_on_list.remove(meta.location.as_ref())
                            };
                            if should_delete {
                                let _ = inner.delete(&meta.location).await;
                            }
                        }
                        result
                    }
                })
                .boxed()
        }

        async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }
}

fn build_report(root: &Path, env: &RunnerEnv, seeds: &[SeedReport], coverage: &Coverage) -> String {
    let mut error_codes = BTreeMap::<String, u64>::new();
    let mut latencies = BTreeMap::<String, Vec<u64>>::new();
    for seed in seeds {
        for rec in read_ops(&seed.dir) {
            latencies
                .entry(rec.op.kind().to_string())
                .or_default()
                .push(rec.duration_ms);
            if !(200..300).contains(&rec.status) {
                let code = rec
                    .response
                    .get("code")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("UNKNOWN")
                    .to_string();
                *error_codes.entry(code).or_default() += 1;
            }
        }
    }

    let failed = seeds.iter().filter(|seed| seed.failed).count();
    let ops = seeds.iter().map(|seed| seed.ops).sum::<u64>();
    let wall = seeds
        .iter()
        .map(|seed| seed.wall_secs)
        .sum::<f64>()
        .max(0.001);
    let mut out = String::new();
    out.push_str("# Adversarial Runner Report\n\n");
    out.push_str(&format!("- git rev: `{}`\n", git_rev()));
    out.push_str(&format!(
        "- dirty tree: `{}`\n",
        if git_dirty() { "true" } else { "false" }
    ));
    out.push_str(&format!("- date_unix_s: `{}`\n", now_unix_secs()));
    out.push_str(&format!(
        "- backend: `{}`\n",
        env.env_echo
            .get("TEST_BACKEND")
            .map(String::as_str)
            .unwrap_or("memory")
    ));
    out.push_str(&format!("- mode: `{:?}`\n", env.mode));
    out.push_str(&format!("- budget_s: `{}`\n", env.seconds));
    out.push_str(&format!("- run dir: `{}`\n\n", root.display()));

    out.push_str("## Seeds\n\n");
    out.push_str("| seed | mode | status | ops | compactions | faults | wall_s | ops/sec |\n");
    out.push_str("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |\n");
    for seed in seeds {
        out.push_str(&format!(
            "| {} | `{:?}` | {} | {} | {} | {} | {:.2} | {:.2} |\n",
            seed.seed,
            seed.mode,
            if seed.failed { "failed" } else { "passed" },
            seed.ops,
            seed.compactions,
            seed.fired_faults.len(),
            seed.wall_secs,
            seed.ops as f64 / seed.wall_secs.max(0.001)
        ));
    }
    out.push_str(&format!(
        "\nSummary: seeds={}, failed={}, ops={}, ops/sec={:.2}\n\n",
        seeds.len(),
        failed,
        ops,
        ops as f64 / wall
    ));

    out.push_str("## Violations\n\n");
    let mut any_violation = false;
    for seed in seeds {
        for violation in &seed.violations {
            any_violation = true;
            out.push_str(&format!(
                "- seed {} op {} `{:?}` `{}`: {}\n",
                seed.seed, violation.op_index, violation.id, violation.namespace, violation.detail
            ));
            let failure_path = seed.dir.join("failure.json");
            if failure_path.exists() {
                let failure = fs::read_to_string(&failure_path).unwrap_or_else(|error| {
                    panic!("failed to read {}: {error}", failure_path.display())
                });
                let failure: FailureManifest =
                    serde_json::from_str(&failure).unwrap_or_else(|error| {
                        panic!("failed to parse {}: {error}", failure_path.display())
                    });
                out.push_str(&format!("  - repro: `{}`\n", failure.repro_cmd));
                out.push_str(&format!("  - inspect: `{}`\n", failure.inspect_cmd));
            }
        }
    }
    if !any_violation {
        out.push_str("No violations recorded.\n");
    }
    out.push('\n');

    out.push_str("## Fired Faults\n\n");
    let mut any_fault = false;
    for seed in seeds {
        if seed.fired_faults.is_empty() {
            continue;
        }
        any_fault = true;
        let mut counts = BTreeMap::<String, u64>::new();
        for fault in &seed.fired_faults {
            *counts.entry(fault.site_id.clone()).or_default() += 1;
        }
        out.push_str(&format!("- seed {}:\n", seed.seed));
        for (site, count) in counts {
            out.push_str(&format!("  - `{site}`: {count}\n"));
        }
        for fault in seed.fired_faults.iter().take(5) {
            out.push_str(&format!(
                "  - sample `{}` call={} key=`{}`\n",
                fault.site_id, fault.call_ordinal, fault.key
            ));
        }
    }
    if !any_fault {
        out.push_str("No chaos faults fired.\n");
    }
    out.push('\n');

    out.push_str("## Error Codes\n\n");
    if error_codes.is_empty() {
        out.push_str("No non-2xx operation responses recorded.\n\n");
    } else {
        for (code, count) in error_codes {
            out.push_str(&format!("- `{code}`: {count}\n"));
        }
        out.push('\n');
    }

    out.push_str("## Operation Coverage\n\n");
    for kind in REQUIRED_OP_KINDS {
        let count = coverage.op_counts.get(*kind).copied().unwrap_or(0);
        let marker = if count == 0 { " ⚠" } else { "" };
        out.push_str(&format!("- `{kind}`: {count}{marker}\n"));
    }
    for (kind, count) in &coverage.op_counts {
        if !REQUIRED_OP_KINDS.contains(&kind.as_str()) {
            out.push_str(&format!("- `{kind}`: {count}\n"));
        }
    }
    out.push('\n');

    out.push_str("## Scenario Tag Coverage\n\n");
    for tag in REQUIRED_TAGS {
        let count = coverage.tag_counts.get(*tag).copied().unwrap_or(0);
        let marker = if count == 0 { " ⚠" } else { "" };
        out.push_str(&format!("- `{tag}`: {count}{marker}\n"));
    }
    for (tag, count) in &coverage.tag_counts {
        if !REQUIRED_TAGS.contains(&tag.as_str()) {
            out.push_str(&format!("- `{tag}`: {count}\n"));
        }
    }
    out.push('\n');

    out.push_str("## Latency\n\n");
    out.push_str("| op | p50_ms | p99_ms |\n");
    out.push_str("| --- | ---: | ---: |\n");
    for (kind, mut values) in latencies {
        values.sort_unstable();
        out.push_str(&format!(
            "| `{kind}` | {} | {} |\n",
            percentile(&values, 50),
            percentile(&values, 99)
        ));
    }
    out.push('\n');

    out.push_str("## Object-Store Totals\n\n");
    out.push_str("| class | get_ops | get_bytes | put_ops | put_bytes |\n");
    out.push_str("| --- | ---: | ---: | ---: | ---: |\n");
    for (class, stats) in object_store_totals(seeds) {
        out.push_str(&format!(
            "| `{class}` | {} | {} | {} | {} |\n",
            stats.get_ops, stats.get_bytes, stats.put_ops, stats.put_bytes
        ));
    }
    out
}

const REQUIRED_OP_KINDS: &[&str] = &[
    "create_namespace",
    "get_namespace",
    "upsert",
    "delete_vectors",
    "fetch_vectors",
    "query",
    "batch_query",
    "paginate_all",
    "invalid_probe",
    "compact_endpoint",
    "gc_cycle",
    "create_snapshot",
    "get_snapshot",
    "list_snapshots",
    "delete_snapshot",
    "clone_namespace",
    "patch_index_config",
    "hydrate",
    "delete_namespace",
    "probe_sandwich",
    "compact_inline",
];

const REQUIRED_TAGS: &[&str] = &[
    "delete-then-reupsert",
    "eventual-tombstone",
    "eventual",
    "batch",
    "pagination",
    "fts",
    "invalid-probe",
    "as-of-200",
    "as-of-410",
    "snapshot",
    "clone",
    "gc-cycle",
    "sandwich",
    "delete-recreate",
];

fn object_store_totals(seeds: &[SeedReport]) -> BTreeMap<String, ClassStats> {
    let mut totals = BTreeMap::<String, ClassStats>::new();
    for seed in seeds {
        for (class, stats) in &seed.object_store {
            let total = totals.entry(class.clone()).or_default();
            total.get_ops += stats.get_ops;
            total.get_bytes += stats.get_bytes;
            total.put_ops += stats.put_ops;
            total.put_bytes += stats.put_bytes;
        }
    }
    totals
}

fn percentile(values: &[u64], pct: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let idx = ((values.len() - 1) * pct) / 100;
    values[idx]
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

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs()
}

fn git_dirty() -> bool {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .expect("failed to run git status --porcelain");
    assert!(
        output.status.success(),
        "git status --porcelain failed with status {}",
        output.status
    );
    !output.stdout.is_empty()
}
