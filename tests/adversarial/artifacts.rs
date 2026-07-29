//! Durable adversarial-run artifacts and Markdown reporting.
//!
//! `faults.jsonl` belongs only to the legacy chaos injector. Scheduled fault
//! profiles use `timeline.jsonl`; all profiles also record canonical
//! `quiet:<step>` recovery events there.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use zeppelin::config::Config;
use zeppelin::error::ZeppelinError;
use zeppelin::storage::ZeppelinStore;

use crate::common::counting::{ArtifactClass, ClassStats};

use super::chaos::{FaultPlan, FiredFault};
use super::faults::{
    ContractClass, FaultContract, FaultKind, FaultProfile, FaultSchedule, ObservedResult,
    ProtectedAssumption, TimelineEvent,
};
use super::generator::Coverage;
use super::model::Model;
use super::ops::{NamespaceSpec, OpRecord};
use super::oracle::{Violation, ViolationId};
use super::security_program::SecurityProgramConfig;
use super::{effective_seed_assignment, RunMode, RunnerEnv, SeedAssignment};

#[derive(Debug, Clone)]
pub struct RunArtifacts {
    root: PathBuf,
    start_manifest: RunManifest,
}

#[derive(Debug, Clone, Serialize)]
struct RunManifest {
    git_rev: String,
    dirty_tree: bool,
    backend: Option<String>,
    env: BTreeMap<String, String>,
    configured_seeds: Vec<u64>,
    seeds: Vec<u64>,
    mode: RunMode,
    profile: Option<FaultProfile>,
    mode_assignment: BTreeMap<String, SeedAssignment>,
    seconds: u64,
}

#[derive(Debug)]
pub struct SeedArtifacts {
    pub dir: PathBuf,
    ops: BufWriter<File>,
    op_count: u64,
    next_op_index: u64,
    pending_ops: BTreeMap<u64, OpRecord>,
    fault_contracts: BTreeMap<String, FaultContract>,
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
    pub profile: Option<FaultProfile>,
    pub dir: PathBuf,
    pub failed: bool,
    pub ops: u64,
    pub compactions: u64,
    pub background_compactions: u64,
    pub violations: Vec<Violation>,
    pub wall_secs: f64,
    pub object_store: ObjectStorePhaseCensus,
    pub fired_faults: Vec<FiredFault>,
}

pub type ObjectStoreCensus = BTreeMap<ArtifactClass, ClassStats>;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ObjectStorePhaseCensus {
    pub in_run: ObjectStoreCensus,
    pub quiet_period: ObjectStoreCensus,
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
        let start_manifest = RunManifest::at_start(env);
        let artifacts = Self {
            root,
            start_manifest,
        };
        artifacts.write_run_manifest(&artifacts.start_manifest);
        artifacts
    }

    fn write_run_manifest(&self, manifest: &RunManifest) {
        write_json_atomically(self.root.join("run.json"), manifest);
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn write_watchdog_failure(
        &self,
        seed: u64,
        failure: &FailureManifest,
        watchdog: &serde_json::Value,
    ) {
        let seed_dir = self.root.join(format!("seed-{seed}"));
        fs::create_dir_all(&seed_dir).unwrap_or_else(|error| {
            panic!(
                "failed to create watchdog artifact dir {}: {error}",
                seed_dir.display()
            )
        });
        write_json(seed_dir.join("failure.json"), failure);
        write_json(seed_dir.join("watchdog.json"), watchdog);
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
        fault_schedule: Option<&FaultSchedule>,
    ) -> SeedArtifacts {
        self.seed_with_security(
            seed,
            config,
            specs,
            mode,
            fault_plan,
            selftest_probe,
            chaos_plan,
            fault_schedule,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn seed_with_security(
        &self,
        seed: u64,
        config: &Config,
        specs: &BTreeMap<String, NamespaceSpec>,
        mode: RunMode,
        fault_plan: Option<&str>,
        selftest_probe: Option<&str>,
        chaos_plan: Option<&serde_json::Value>,
        fault_schedule: Option<&FaultSchedule>,
        security_program: Option<&SecurityProgramConfig>,
    ) -> SeedArtifacts {
        let dir = self.root.join(format!("seed-{seed}"));
        fs::create_dir_all(&dir)
            .unwrap_or_else(|error| panic!("failed to create seed dir {}: {error}", dir.display()));
        let mut fault_contracts = fault_schedule
            .map(FaultSchedule::contracts)
            .unwrap_or_default();
        let chaos_contracts = chaos_plan
            .map(|value| {
                serde_json::from_value::<FaultPlan>(value.clone())
                    .unwrap_or_else(|error| panic!("failed to parse generated chaos plan: {error}"))
                    .contracts()
            })
            .unwrap_or_default();
        let selftest = fault_plan.is_some() || selftest_probe.is_some();
        let has_chaos_contracts = !chaos_contracts.is_empty();
        if selftest {
            fault_contracts.extend(chaos_contracts.into_iter().map(|contract| FaultContract {
                event_id: contract.event_id,
                contract_class: ContractClass::HarnessSelfTest,
                violated_assumptions: Vec::new(),
            }));
        } else {
            fault_contracts.extend(chaos_contracts);
        }
        if selftest && !has_chaos_contracts {
            fault_contracts.push(FaultContract {
                event_id: "oracle-selftest".to_string(),
                contract_class: ContractClass::HarnessSelfTest,
                violated_assumptions: Vec::new(),
            });
        }
        write_json(
            dir.join("config.json"),
            &serde_json::json!({
                "seed": seed,
                "mode": mode,
                "fault_plan": fault_plan,
                "selftest_probe": selftest_probe,
                "chaos_plan": chaos_plan,
                "fault_schedule": fault_schedule,
                "fault_contracts": fault_contracts,
                "config": config,
                "namespace_specs": specs,
                "principals": security_program
                    .map_or(&[][..], |program| program.principals.as_slice()),
                "security_ops": security_program
                    .map_or(&[][..], |program| program.security_ops.as_slice()),
                "protected_assumptions": security_program
                    .map_or(&[][..], |program| program.protected_assumptions.as_slice()),
                "security_program": security_program,
            }),
        );
        let ops_file = File::create(dir.join("ops.jsonl")).unwrap_or_else(|error| {
            panic!("failed to create ops.jsonl in {}: {error}", dir.display())
        });
        SeedArtifacts {
            dir,
            ops: BufWriter::new(ops_file),
            op_count: 0,
            next_op_index: 0,
            pending_ops: BTreeMap::new(),
            fault_contracts: fault_contracts
                .into_iter()
                .map(|contract| (contract.event_id.clone(), contract))
                .collect(),
        }
    }

    pub fn write_report(
        &self,
        _env: &RunnerEnv,
        seeds: &[SeedReport],
        coverage: &Coverage,
        update_latest: bool,
    ) {
        let completed_manifest = self.start_manifest.at_completion(seeds);
        self.write_run_manifest(&completed_manifest);
        let report = build_report(&self.root, &self.start_manifest, seeds, coverage);
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

impl RunManifest {
    fn at_start(env: &RunnerEnv) -> Self {
        let mode_assignment = env
            .seeds
            .iter()
            .map(|seed| {
                (
                    seed.to_string(),
                    effective_seed_assignment(env.mode, env.profile, *seed),
                )
            })
            .collect();
        Self {
            git_rev: git_rev(),
            dirty_tree: git_dirty(),
            backend: env.env_echo.get("TEST_BACKEND").cloned(),
            env: env.env_echo.clone(),
            configured_seeds: env.seeds.clone(),
            seeds: env.seeds.clone(),
            mode: env.mode,
            profile: env.profile,
            mode_assignment,
            seconds: env.seconds,
        }
    }

    fn at_completion(&self, reports: &[SeedReport]) -> Self {
        let mut completed = self.clone();
        completed.seeds.clear();
        let mut seen = BTreeSet::new();
        for report in reports {
            assert!(
                seen.insert(report.seed),
                "run completion contained duplicate seed report {}",
                report.seed
            );
            completed.seeds.push(report.seed);
            let key = report.seed.to_string();
            let assignment = SeedAssignment {
                mode: report.mode,
                profile: report.profile,
            };
            if let Some(configured) = self.mode_assignment.get(&key) {
                assert_eq!(
                    assignment, *configured,
                    "configured assignment for seed {} changed during the run",
                    report.seed
                );
            }
            completed.mode_assignment.insert(key, assignment);
        }
        completed
    }
}

impl SeedArtifacts {
    pub fn write_op(&mut self, rec: &OpRecord) {
        assert!(
            rec.index >= self.next_op_index,
            "op {} completed after it was already flushed",
            rec.index
        );
        assert!(
            self.pending_ops.insert(rec.index, rec.clone()).is_none(),
            "op {} completed more than once",
            rec.index
        );
        self.op_count += 1;
        let mut flushed = false;
        while let Some(rec) = self.pending_ops.remove(&self.next_op_index) {
            let encoded = serde_json::to_string(&rec).expect("OpRecord must serialize");
            let _: OpRecord =
                serde_json::from_str(&encoded).expect("OpRecord JSONL line must deserialize");
            writeln!(self.ops, "{encoded}").expect("failed to write ops.jsonl line");
            self.next_op_index += 1;
            flushed = true;
        }
        if flushed {
            self.ops.flush().expect("failed to flush ops.jsonl line");
        }
    }

    #[must_use]
    pub fn op_count(&self) -> u64 {
        self.op_count
    }

    #[must_use]
    pub fn completed_operation_ids(&self) -> Vec<u64> {
        (0..self.next_op_index)
            .chain(self.pending_ops.keys().copied())
            .collect()
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
                match store.head(&key).await {
                    Ok(metadata) => output.push_str(&format!("{key}\t{}\n", metadata.size)),
                    Err(ZeppelinError::NotFound { .. }) => {
                        output.push_str(&format!("{key}\tmissing_after_list\n"));
                    }
                    Err(error) => panic!("failed to head S3 key {key}: {error}"),
                }
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

    pub fn write_timeline(&self, timeline: &[TimelineEvent]) {
        let path = self.dir.join("timeline.jsonl");
        let file = File::create(&path)
            .unwrap_or_else(|error| panic!("failed to create {}: {error}", path.display()));
        let mut writer = BufWriter::new(file);
        let operations = read_ops(&self.dir)
            .into_iter()
            .map(|record| (record.index, record))
            .collect::<BTreeMap<_, _>>();
        for event in timeline {
            let mut value = serde_json::to_value(event).expect("TimelineEvent must serialize");
            let contract = self.fault_contracts.get(&event.event_id);
            let (contract_class, violated_assumptions) = match contract {
                Some(contract) => (
                    contract.contract_class,
                    contract.violated_assumptions.as_slice(),
                ),
                None if event.event_id.starts_with("quiet-")
                    && event.action.starts_with("quiet:") =>
                {
                    (ContractClass::SupportedV1, &[][..])
                }
                None => panic!(
                    "timeline event {} is missing persisted fault contract metadata",
                    event.event_id
                ),
            };
            let object = value
                .as_object_mut()
                .expect("TimelineEvent must serialize as an object");
            object.insert(
                "contract_class".to_string(),
                serde_json::to_value(contract_class).expect("ContractClass must serialize"),
            );
            object.insert(
                "violated_assumptions".to_string(),
                serde_json::to_value(violated_assumptions)
                    .expect("ProtectedAssumption must serialize"),
            );
            let operation = (!event.action.starts_with("quiet:"))
                .then(|| operations.get(&event.op_index))
                .flatten();
            let (actor, decision) = operation.map_or(("runner", "not_applicable"), |record| {
                (
                    record.op.actor().label(),
                    match record.status {
                        200..=299 => "allow",
                        401 => "unauthorized",
                        403 => "forbidden",
                        _ if record.outcome.starts_with("ambiguous:") => "indeterminate",
                        _ => "non_authz_error",
                    },
                )
            });
            object.insert("actor".to_string(), serde_json::json!(actor));
            object.insert("decision".to_string(), serde_json::json!(decision));
            let encoded = serde_json::to_string(&value).expect("timeline artifact must serialize");
            writeln!(writer, "{encoded}")
                .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
        }
        writer
            .flush()
            .unwrap_or_else(|error| panic!("failed to flush {}: {error}", path.display()));
    }

    pub fn write_resolutions(&self, resolutions: &[serde_json::Value]) {
        write_json(self.dir.join("resolutions.json"), resolutions);
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

fn read_timeline(path: &Path) -> Vec<TimelineEvent> {
    let path = path.join("timeline.jsonl");
    if !path.exists() {
        return Vec::new();
    }
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str(line).unwrap_or_else(|error| {
                panic!(
                    "failed to parse {} line {}: {error}",
                    path.display(),
                    index + 1
                )
            })
        })
        .collect()
}

fn write_json<T: Serialize + ?Sized>(path: impl AsRef<Path>, value: &T) {
    let bytes = serde_json::to_vec_pretty(value).expect("artifact JSON must serialize");
    fs::write(path.as_ref(), bytes).unwrap_or_else(|error| {
        panic!(
            "failed to write artifact {}: {error}",
            path.as_ref().display()
        )
    });
}

fn write_json_atomically<T: Serialize + ?Sized>(path: impl AsRef<Path>, value: &T) {
    let path = path.as_ref();
    let bytes = serde_json::to_vec_pretty(value).expect("artifact JSON must serialize");
    let parent = path.parent().unwrap_or_else(|| {
        panic!(
            "atomic artifact path {} has no parent directory",
            path.display()
        )
    });
    let file_name = path
        .file_name()
        .unwrap_or_else(|| panic!("atomic artifact path {} has no file name", path.display()));
    let temp_path = parent.join(format!(
        "{}.{}.tmp",
        file_name.to_string_lossy(),
        uuid::Uuid::new_v4()
    ));
    let mut temp = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .unwrap_or_else(|error| {
            panic!(
                "failed to create atomic artifact temp {}: {error}",
                temp_path.display()
            )
        });
    temp.write_all(&bytes).unwrap_or_else(|error| {
        panic!(
            "failed to write atomic artifact temp {}: {error}",
            temp_path.display()
        )
    });
    temp.sync_all().unwrap_or_else(|error| {
        panic!(
            "failed to sync atomic artifact temp {}: {error}",
            temp_path.display()
        )
    });
    drop(temp);
    fs::rename(&temp_path, path).unwrap_or_else(|error| {
        panic!(
            "failed to atomically replace artifact {} with {}: {error}",
            path.display(),
            temp_path.display()
        )
    });
}

fn try_write_json<T: Serialize + ?Sized>(path: impl AsRef<Path>, value: &T) -> std::io::Result<()> {
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
#[allow(clippy::items_after_test_module)]
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
    use crate::adversarial::faults::{
        Boundary, FaultEvent, FaultSemantics, ObservedResult, TargetSelector,
    };
    use crate::adversarial::ops::{ActorSel, ExecutionMetadata, Op};
    use crate::adversarial::security_program::SecurityProgramConfig;
    use crate::adversarial::PreserveMode;

    fn record(index: u64) -> OpRecord {
        OpRecord {
            index,
            wall_ms: index,
            op: Op::GetNamespace {
                actor: ActorSel::ADMIN,
                ns: "ordered".to_string(),
            },
            method: "GET".to_string(),
            path: "/v1/namespaces/ordered".to_string(),
            status: 200,
            response: serde_json::json!({}),
            outcome: "applied".to_string(),
            target_node: 0,
            execution: ExecutionMetadata::workload(),
            gen_after: None,
            duration_ms: 0,
            violations: Vec::new(),
        }
    }

    #[test]
    fn seed_artifacts_flush_out_of_order_completions_by_op_index() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ops.jsonl");
        let mut artifacts = SeedArtifacts {
            dir: dir.path().to_path_buf(),
            ops: BufWriter::new(File::create(&path).unwrap()),
            op_count: 0,
            next_op_index: 0,
            pending_ops: BTreeMap::new(),
            fault_contracts: BTreeMap::new(),
        };

        artifacts.write_op(&record(1));
        artifacts.write_op(&record(0));

        let indexes = fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<OpRecord>(line).unwrap().index)
            .collect::<Vec<_>>();
        assert_eq!(indexes, vec![0, 1]);
        assert_eq!(artifacts.op_count(), 2);
    }

    #[test]
    fn security_artifacts_persist_program_and_authz_metadata() {
        let dir = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 1,
            seeds: vec![7, 8],
            max_ops: Some(1),
            artifacts: dir.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Chaos,
            profile: Some(FaultProfile::Security),
            env_echo: BTreeMap::new(),
        };
        let run = RunArtifacts::create(&env);
        let program = SecurityProgramConfig::for_seed(
            "artifact-security",
            &["tenant-a".to_string(), "tenant-b".to_string()],
        );
        let mut seed = run.seed_with_security(
            7,
            &Config::default(),
            &BTreeMap::new(),
            RunMode::Chaos,
            None,
            None,
            None,
            None,
            Some(&program),
        );

        let config: serde_json::Value =
            serde_json::from_slice(&fs::read(seed.dir.join("config.json")).unwrap()).unwrap();
        assert_eq!(config["principals"].as_array().unwrap().len(), 5);
        assert_eq!(config["security_ops"].as_array().unwrap().len(), 20);
        assert_eq!(config["protected_assumptions"].as_array().unwrap().len(), 8);
        assert!(!config["security_program"].is_null());

        let mut authz = record(0);
        authz.op = Op::SecurityAdminProbe { actor: ActorSel(2) };
        authz.status = 403;
        authz.outcome = "not_applied".to_string();
        seed.write_op(&authz);
        seed.write_timeline(&[
            TimelineEvent {
                event_id: "quiet-06".to_string(),
                op_index: 0,
                wall_ms: 1,
                boundary: Boundary::Runner,
                action: "quiet:security-refresh".to_string(),
                key: None,
                semantics: FaultSemantics::WindowEnd,
                observed: ObservedResult::DefiniteNotApplied,
                recovery: None,
            },
            TimelineEvent {
                event_id: "quiet-12".to_string(),
                op_index: 1,
                wall_ms: 2,
                boundary: Boundary::ClientHttp,
                action: "quiet:exhaustive-sweep".to_string(),
                key: None,
                semantics: FaultSemantics::WindowEnd,
                observed: ObservedResult::DefiniteApplied,
                recovery: None,
            },
        ]);

        let timeline = fs::read_to_string(seed.dir.join("timeline.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(timeline[0]["actor"], "runner");
        assert_eq!(timeline[0]["decision"], "not_applicable");
        assert_eq!(timeline[1]["actor"], "runner");
        assert_eq!(timeline[1]["decision"], "not_applicable");

        let seed_dir = seed.dir.clone();
        let report = build_report(
            run.root(),
            &RunManifest::at_start(&env),
            &[SeedReport {
                seed: 7,
                mode: RunMode::Chaos,
                profile: Some(FaultProfile::Security),
                dir: seed_dir,
                failed: false,
                ops: 1,
                compactions: 0,
                background_compactions: 0,
                violations: Vec::new(),
                wall_secs: 1.0,
                object_store: ObjectStorePhaseCensus::default(),
                fired_faults: Vec::new(),
            }],
            &Coverage::default(),
        );
        assert!(report.contains("## Authorization Summary"));
        assert!(report
            .contains("| 7 | 0 | 1 | 0 | 0 | pass | pass | pass | pass | pass | pass | pass |"));
    }

    #[test]
    fn generated_artifacts_persist_and_report_contract_classification() {
        let dir = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 1,
            seeds: vec![7],
            max_ops: Some(1),
            artifacts: dir.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Chaos,
            profile: Some(FaultProfile::ProviderContractAbuse),
            env_echo: BTreeMap::new(),
        };
        let run = RunArtifacts::create(&env);
        let scheduler =
            super::super::faults::FaultScheduler::for_seed(7, FaultProfile::ProviderContractAbuse);
        let schedule = scheduler.schedule().clone();
        let event = schedule.events.first().unwrap();
        let seed = run.seed(
            7,
            &Config::default(),
            &BTreeMap::new(),
            RunMode::Chaos,
            None,
            None,
            None,
            Some(&schedule),
        );
        seed.write_timeline(&[TimelineEvent {
            event_id: event.id.clone(),
            op_index: event.start_op,
            wall_ms: 0,
            boundary: event.boundary,
            action: "provider abuse fired".to_string(),
            key: None,
            semantics: FaultSemantics::PostCommit,
            observed: ObservedResult::Corrupted,
            recovery: Some("research finding".to_string()),
        }]);

        let config: serde_json::Value =
            serde_json::from_slice(&fs::read(seed.dir.join("config.json")).unwrap()).unwrap();
        assert_eq!(
            config["fault_contracts"][0]["contract_class"],
            "provider_contract_abuse"
        );
        assert!(!config["fault_contracts"][0]["violated_assumptions"]
            .as_array()
            .unwrap()
            .is_empty());

        let timeline: serde_json::Value = serde_json::from_str(
            fs::read_to_string(seed.dir.join("timeline.jsonl"))
                .unwrap()
                .trim(),
        )
        .unwrap();
        assert_eq!(timeline["contract_class"], "provider_contract_abuse");
        assert!(!timeline["violated_assumptions"]
            .as_array()
            .unwrap()
            .is_empty());

        let report = build_report(
            run.root(),
            &RunManifest::at_start(&env),
            &[SeedReport {
                seed: 7,
                mode: RunMode::Chaos,
                profile: Some(FaultProfile::ProviderContractAbuse),
                dir: seed.dir,
                failed: true,
                ops: 0,
                compactions: 0,
                background_compactions: 0,
                violations: Vec::new(),
                wall_secs: 0.0,
                object_store: ObjectStorePhaseCensus::default(),
                fired_faults: Vec::new(),
            }],
            &Coverage::default(),
        );
        assert!(report.contains("contract class"));
        assert!(report.contains("ProviderContractAbuse"));
        assert!(report.contains("not a v1 product bug"));
        assert!(report.contains("research-finding"));
        assert!(report.contains("failed=0, research_findings=1"));
    }

    #[test]
    fn report_separates_in_run_and_quiet_period_object_store_censuses() {
        let dir = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 1,
            seeds: vec![7],
            max_ops: Some(1),
            artifacts: dir.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Deterministic,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let run = RunArtifacts::create(&env);
        let seed = run.seed(
            7,
            &Config::default(),
            &BTreeMap::new(),
            RunMode::Deterministic,
            None,
            None,
            None,
            None,
        );

        let report = build_report(
            run.root(),
            &RunManifest::at_start(&env),
            &[SeedReport {
                seed: 7,
                mode: RunMode::Deterministic,
                profile: None,
                dir: seed.dir,
                failed: false,
                ops: 0,
                compactions: 0,
                background_compactions: 0,
                violations: Vec::new(),
                wall_secs: 1.0,
                object_store: ObjectStorePhaseCensus {
                    in_run: BTreeMap::from([(
                        ArtifactClass::Manifest,
                        ClassStats {
                            get_ops: 3,
                            get_bytes: 30,
                            put_ops: 1,
                            put_bytes: 10,
                        },
                    )]),
                    quiet_period: BTreeMap::from([(
                        ArtifactClass::Manifest,
                        ClassStats {
                            get_ops: 5,
                            get_bytes: 50,
                            put_ops: 2,
                            put_bytes: 20,
                        },
                    )]),
                },
                fired_faults: Vec::new(),
            }],
            &Coverage::default(),
        );

        let in_run = report
            .split("## Object-Store In-Run Totals\n")
            .nth(1)
            .expect("report omitted in-run object-store totals")
            .split("## Object-Store Quiet-Period Totals\n")
            .next()
            .unwrap();
        assert!(in_run.contains("| `manifest` | 3 | 30 | 1 | 10 |"));

        let quiet_period = report
            .split("## Object-Store Quiet-Period Totals\n")
            .nth(1)
            .expect("report omitted quiet-period object-store totals");
        assert!(quiet_period.contains("| `manifest` | 5 | 50 | 2 | 20 |"));

        let combined = report
            .split("## Object-Store Totals\n")
            .nth(1)
            .expect("report omitted combined object-store totals");
        assert!(combined.contains("| `manifest` | 8 | 80 | 3 | 30 |"));
    }

    #[test]
    fn lost_write_selftest_persists_site_level_harness_contract() {
        let dir = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 1,
            seeds: vec![11],
            max_ops: Some(1),
            artifacts: dir.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Chaos,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let run = RunArtifacts::create(&env);
        let chaos_plan =
            serde_json::to_value(crate::adversarial::chaos::FaultPlan::lost_write_selftest())
                .unwrap();
        let seed = run.seed(
            11,
            &Config::default(),
            &BTreeMap::new(),
            RunMode::Chaos,
            Some("chaos-lost-write"),
            None,
            Some(&chaos_plan),
            None,
        );

        let config: serde_json::Value =
            serde_json::from_slice(&fs::read(seed.dir.join("config.json")).unwrap()).unwrap();
        assert_eq!(
            config["fault_contracts"],
            serde_json::json!([{
                "event_id": "chaos-lost-write",
                "contract_class": "harness_self_test",
                "violated_assumptions": [],
            }])
        );

        let report = build_report(
            run.root(),
            &RunManifest::at_start(&env),
            &[SeedReport {
                seed: 11,
                mode: RunMode::Chaos,
                profile: Some(FaultProfile::LegacyChaos),
                dir: seed.dir,
                failed: false,
                ops: 0,
                compactions: 0,
                background_compactions: 0,
                violations: Vec::new(),
                wall_secs: 0.0,
                object_store: ObjectStorePhaseCensus::default(),
                fired_faults: Vec::new(),
            }],
            &Coverage::default(),
        );
        assert!(report.contains("| 11 | `HarnessSelfTest` | `` | `no` |"));
    }

    #[test]
    fn run_json_records_the_supported_nine_slot_mode_and_profile_assignment() {
        let dir = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 1,
            seeds: (0..9).collect(),
            max_ops: Some(1),
            artifacts: dir.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Mixed,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let artifacts = RunArtifacts::create(&env);
        let run: serde_json::Value =
            serde_json::from_slice(&fs::read(artifacts.root().join("run.json")).unwrap()).unwrap();
        let assignments = &run["mode_assignment"];
        assert_eq!(assignments["0"]["mode"], "deterministic");
        assert!(assignments["0"]["profile"].is_null());
        assert_eq!(assignments["1"]["profile"], "legacy_chaos");
        assert_eq!(assignments["3"]["profile"], "post_commit");
        assert_eq!(assignments["7"]["profile"], "sched");
        assert_eq!(assignments["8"]["profile"], "supported_full");
    }

    #[test]
    fn completed_run_json_records_emitted_overnight_seeds() {
        let dir = tempfile::TempDir::new().unwrap();
        let env = RunnerEnv {
            seconds: 1,
            seeds: vec![0, 1, 2],
            max_ops: Some(1),
            artifacts: dir.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Mixed,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let artifacts = RunArtifacts::create(&env);
        let reports = (0..9)
            .map(|seed| {
                let seed_dir = artifacts.root().join(format!("seed-{seed}"));
                fs::create_dir_all(&seed_dir).unwrap();
                File::create(seed_dir.join("ops.jsonl")).unwrap();
                let assignment = effective_seed_assignment(env.mode, env.profile, seed);
                SeedReport {
                    seed,
                    mode: assignment.mode,
                    profile: assignment.profile,
                    dir: seed_dir,
                    failed: false,
                    ops: 0,
                    compactions: 0,
                    background_compactions: 0,
                    violations: Vec::new(),
                    wall_secs: 0.0,
                    object_store: ObjectStorePhaseCensus::default(),
                    fired_faults: Vec::new(),
                }
            })
            .collect::<Vec<_>>();
        artifacts.write_report(&env, &reports, &Coverage::default(), false);

        let run: serde_json::Value =
            serde_json::from_slice(&fs::read(artifacts.root().join("run.json")).unwrap()).unwrap();
        assert_eq!(run["configured_seeds"], serde_json::json!([0, 1, 2]));
        assert_eq!(run["seeds"], serde_json::json!([0, 1, 2, 3, 4, 5, 6, 7, 8]));
        assert_eq!(run["mode_assignment"]["3"]["profile"], "post_commit");
        assert_eq!(run["mode_assignment"]["7"]["profile"], "sched");
        assert_eq!(run["mode_assignment"]["8"]["profile"], "supported_full");
    }

    #[test]
    fn completion_preserves_start_provenance_and_atomically_replaces_run_json() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut env_echo = BTreeMap::new();
        env_echo.insert("TEST_BACKEND".to_string(), "start-backend".to_string());
        env_echo.insert("ZEPPELIN_ADVERSARIAL_SECONDS".to_string(), "17".to_string());
        env_echo.insert("START_ONLY".to_string(), "preserve-me".to_string());
        let env = RunnerEnv {
            seconds: 17,
            seeds: vec![0, 1, 2],
            max_ops: Some(1),
            artifacts: dir.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Mixed,
            profile: None,
            env_echo,
        };
        let artifacts = RunArtifacts::create(&env);
        let run_path = artifacts.root().join("run.json");
        let start_bytes = fs::read(&run_path).unwrap();
        let start: serde_json::Value = serde_json::from_slice(&start_bytes).unwrap();
        let start_witness = artifacts.root().join("run-start-witness.json");
        fs::hard_link(&run_path, &start_witness).unwrap();

        let mut completion_env = env.clone();
        completion_env.seeds = vec![99];
        completion_env.mode = RunMode::Deterministic;
        completion_env.profile = Some(FaultProfile::Crash);
        completion_env
            .env_echo
            .insert("TEST_BACKEND".to_string(), "mutated-backend".to_string());
        completion_env.env_echo.clear();

        let seed_dir = artifacts.root().join("seed-3");
        fs::create_dir_all(&seed_dir).unwrap();
        File::create(seed_dir.join("ops.jsonl")).unwrap();
        let reports = [SeedReport {
            seed: 3,
            mode: RunMode::Chaos,
            profile: Some(FaultProfile::PostCommit),
            dir: seed_dir,
            failed: false,
            ops: 0,
            compactions: 0,
            background_compactions: 0,
            violations: Vec::new(),
            wall_secs: 0.0,
            object_store: ObjectStorePhaseCensus::default(),
            fired_faults: Vec::new(),
        }];
        artifacts.write_report(&completion_env, &reports, &Coverage::default(), false);

        let completed_bytes = fs::read(&run_path).unwrap();
        let completed: serde_json::Value = serde_json::from_slice(&completed_bytes).unwrap();
        for key in [
            "git_rev",
            "dirty_tree",
            "backend",
            "env",
            "configured_seeds",
            "mode",
            "profile",
        ] {
            assert_eq!(completed[key], start[key], "start field {key} drifted");
        }
        for seed in ["0", "1", "2"] {
            assert_eq!(
                completed["mode_assignment"][seed], start["mode_assignment"][seed],
                "configured assignment for seed {seed} drifted"
            );
        }
        assert_eq!(completed["seeds"], serde_json::json!([3]));
        assert_eq!(
            completed["mode_assignment"]["3"],
            serde_json::json!({"mode": "chaos", "profile": "post_commit"})
        );
        let report = fs::read_to_string(artifacts.root().join("report.md")).unwrap();
        assert!(report.contains(&format!(
            "- git rev: `{}`",
            start["git_rev"].as_str().unwrap()
        )));
        assert!(report.contains("- backend: `start-backend`"));
        assert!(report.contains("- mode: `Mixed`"));
        assert!(report.contains("- budget_s: `17`"));
        assert!(!report.contains("mutated-backend"));
        assert_eq!(
            fs::read(&start_witness).unwrap(),
            start_bytes,
            "run.json completion must atomically replace, not truncate, its inode"
        );
        assert!(
            fs::read_dir(artifacts.root()).unwrap().all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")),
            "atomic run.json update left a temporary file behind"
        );
    }

    #[test]
    fn timeline_summary_does_not_count_unresolved_ambiguity_as_applied() {
        let dir = tempfile::TempDir::new().unwrap();
        let seed_dir = dir.path().join("seed-3");
        fs::create_dir_all(&seed_dir).unwrap();
        File::create(seed_dir.join("ops.jsonl")).unwrap();
        let recoveries = [
            "applied",
            "not_applied",
            "violation:I14",
            "restart+health-wait",
            "ambiguous:http_timeout",
            "stream errors after 128 bytes",
        ];
        let schedule = FaultSchedule {
            profile: FaultProfile::PostCommit,
            events: recoveries
                .iter()
                .enumerate()
                .map(|(index, _)| FaultEvent {
                    id: format!("ambiguous-{index}"),
                    start_op: u64::try_from(index).unwrap(),
                    end_op: None,
                    boundary: Boundary::ClientHttp,
                    target: TargetSelector::default(),
                    kind: FaultKind::DropResponse,
                })
                .collect(),
        };
        write_json(
            seed_dir.join("config.json"),
            &serde_json::json!({ "fault_schedule": schedule }),
        );
        let timeline = recoveries
            .iter()
            .enumerate()
            .map(|(index, recovery)| TimelineEvent {
                event_id: format!("ambiguous-{index}"),
                op_index: u64::try_from(index).unwrap(),
                wall_ms: u64::try_from(index).unwrap(),
                boundary: Boundary::ClientHttp,
                action: "DropResponse".to_string(),
                key: None,
                semantics: FaultSemantics::PostCommit,
                observed: ObservedResult::Ambiguous,
                recovery: Some((*recovery).to_string()),
            })
            .collect::<Vec<_>>();
        fs::write(
            seed_dir.join("timeline.jsonl"),
            timeline
                .iter()
                .map(|event| serde_json::to_string(event).unwrap())
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let env = RunnerEnv {
            seconds: 1,
            seeds: vec![3],
            max_ops: Some(1),
            artifacts: dir.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Mixed,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let manifest = RunManifest::at_start(&env);
        let report = build_report(
            dir.path(),
            &manifest,
            &[SeedReport {
                seed: 3,
                mode: RunMode::Chaos,
                profile: Some(FaultProfile::PostCommit),
                dir: seed_dir,
                failed: false,
                ops: 0,
                compactions: 0,
                background_compactions: 0,
                violations: Vec::new(),
                wall_secs: 0.0,
                object_store: ObjectStorePhaseCensus::default(),
                fired_faults: Vec::new(),
            }],
            &Coverage::default(),
        );

        assert!(report.contains(
            "| boundary | kind | events | applied | not applied | violation | unresolved |"
        ));
        assert!(report.contains("| `ClientHttp` | `DropResponse` | 6 | 1 | 1 | 1 | 3 |"));
    }

    #[test]
    #[should_panic(expected = "missing persisted fault-schedule context")]
    fn timeline_report_rejects_unscheduled_non_quiet_events() {
        let dir = tempfile::TempDir::new().unwrap();
        let seed_dir = dir.path().join("seed-3");
        fs::create_dir_all(&seed_dir).unwrap();
        File::create(seed_dir.join("ops.jsonl")).unwrap();
        write_json(
            seed_dir.join("config.json"),
            &serde_json::json!({ "fault_schedule": null }),
        );
        fs::write(
            seed_dir.join("timeline.jsonl"),
            serde_json::to_string(&TimelineEvent {
                event_id: "lost-schedule-event".to_string(),
                op_index: 3,
                wall_ms: 0,
                boundary: Boundary::ClientHttp,
                action: "DropResponse".to_string(),
                key: None,
                semantics: FaultSemantics::PostCommit,
                observed: ObservedResult::Ambiguous,
                recovery: Some("ambiguous:http_timeout".to_string()),
            })
            .unwrap(),
        )
        .unwrap();
        let env = RunnerEnv {
            seconds: 1,
            seeds: vec![3],
            max_ops: Some(1),
            artifacts: dir.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Mixed,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let manifest = RunManifest::at_start(&env);
        let _ = build_report(
            dir.path(),
            &manifest,
            &[SeedReport {
                seed: 3,
                mode: RunMode::Chaos,
                profile: Some(FaultProfile::PostCommit),
                dir: seed_dir,
                failed: false,
                ops: 0,
                compactions: 0,
                background_compactions: 0,
                violations: Vec::new(),
                wall_secs: 0.0,
                object_store: ObjectStorePhaseCensus::default(),
                fired_faults: Vec::new(),
            }],
            &Coverage::default(),
        );
    }

    #[test]
    fn report_surfaces_routing_delete_contention_and_exhaustion_proofs() {
        let dir = tempfile::TempDir::new().unwrap();
        let seed_dir = dir.path().join("seed-7");
        fs::create_dir_all(&seed_dir).unwrap();
        let mut node_0 = record(0);
        node_0.target_node = 0;
        let mut node_1 = record(1);
        node_1.target_node = 1;
        fs::write(
            seed_dir.join("ops.jsonl"),
            format!(
                "{}\n{}\n",
                serde_json::to_string(&node_0).unwrap(),
                serde_json::to_string(&node_1).unwrap()
            ),
        )
        .unwrap();
        let timeline = [
            TimelineEvent {
                event_id: "ops-second-node".to_string(),
                op_index: 60,
                wall_ms: 1,
                boundary: Boundary::Runner,
                action: "stop second node".to_string(),
                key: None,
                semantics: FaultSemantics::WindowEnd,
                observed: ObservedResult::DefiniteApplied,
                recovery: Some(
                    "namespace=ordered; lease_attempt_nodes=[0,1]; \
                     lease_publication=true; fenced_manifest=true; \
                     background_activity=true"
                        .to_string(),
                ),
            },
            TimelineEvent {
                event_id: "ops-second-node-incomplete".to_string(),
                op_index: 61,
                wall_ms: 2,
                boundary: Boundary::Runner,
                action: "stop second node".to_string(),
                key: None,
                semantics: FaultSemantics::WindowEnd,
                observed: ObservedResult::DefiniteApplied,
                recovery: Some(
                    "namespace=ordered; lease_attempt_nodes=[0,1]; \
                     lease_publication=true; background_activity=true"
                        .to_string(),
                ),
            },
            TimelineEvent {
                event_id: "ops-second-node-wrong-action".to_string(),
                op_index: 62,
                wall_ms: 3,
                boundary: Boundary::Runner,
                action: "unrelated runner event".to_string(),
                key: None,
                semantics: FaultSemantics::WindowEnd,
                observed: ObservedResult::DefiniteApplied,
                recovery: Some(
                    "namespace=ordered; lease_attempt_nodes=[0,1]; \
                     lease_publication=true; fenced_manifest=true; \
                     background_activity=true"
                        .to_string(),
                ),
            },
            TimelineEvent {
                event_id: "ops-delete-race".to_string(),
                op_index: 420,
                wall_ms: 4,
                boundary: Boundary::Runner,
                action: "delete namespace with in-flight upsert upsert_node=0 delete_node=1"
                    .to_string(),
                key: None,
                semantics: FaultSemantics::WindowEnd,
                observed: ObservedResult::DefiniteApplied,
                recovery: Some(
                    "barrier=wal_put_entered; delete_joined=true; barrier_released=true; \
                     upsert_joined=true; upsert_status=404; delete_status=202"
                        .to_string(),
                ),
            },
            TimelineEvent {
                event_id: "ops-resource-limits".to_string(),
                op_index: 8,
                wall_ms: 5,
                boundary: Boundary::Runner,
                action: "resource limits queries=1 disk_cache_bytes=2097152".to_string(),
                key: None,
                semantics: FaultSemantics::PreCall,
                observed: ObservedResult::DefiniteApplied,
                recovery: None,
            },
            TimelineEvent {
                event_id: "ops-exhaustion-burst".to_string(),
                op_index: 9,
                wall_ms: 6,
                boundary: Boundary::Runner,
                action: "fill disk cache with eight concurrent queries".to_string(),
                key: None,
                semantics: FaultSemantics::WindowEnd,
                observed: ObservedResult::DefiniteApplied,
                recovery: Some("completed=8 successful=4 load_shed=4 nodes={0, 1}".to_string()),
            },
        ];
        let schedule = FaultSchedule {
            profile: FaultProfile::Ops,
            events: timeline
                .iter()
                .map(|event| FaultEvent {
                    id: event.event_id.clone(),
                    start_op: event.op_index,
                    end_op: None,
                    boundary: event.boundary,
                    target: TargetSelector::default(),
                    kind: match event.event_id.as_str() {
                        "ops-second-node"
                        | "ops-second-node-incomplete"
                        | "ops-second-node-wrong-action" => {
                            FaultKind::StartSecondNode { for_ops: 1 }
                        }
                        "ops-delete-race" => FaultKind::DeleteNamespaceInFlight,
                        "ops-resource-limits" => FaultKind::ResourceExhaustion {
                            max_concurrent_queries: 1,
                            disk_cache_max_bytes: 2_097_152,
                        },
                        "ops-exhaustion-burst" => FaultKind::FillDiskCache,
                        other => panic!("unexpected operational test event {other}"),
                    },
                })
                .collect(),
        };
        write_json(
            seed_dir.join("config.json"),
            &serde_json::json!({ "fault_schedule": schedule }),
        );
        fs::write(
            seed_dir.join("timeline.jsonl"),
            timeline
                .iter()
                .map(|event| serde_json::to_string(event).unwrap())
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let env = RunnerEnv {
            seconds: 1,
            seeds: vec![7],
            max_ops: Some(2),
            artifacts: dir.path().to_path_buf(),
            preserve: PreserveMode::Never,
            selftest: None,
            mode: RunMode::Deterministic,
            profile: None,
            env_echo: BTreeMap::new(),
        };
        let manifest = RunManifest::at_start(&env);
        let report = build_report(
            dir.path(),
            &manifest,
            &[SeedReport {
                seed: 7,
                mode: RunMode::Deterministic,
                profile: None,
                dir: seed_dir,
                failed: false,
                ops: 2,
                compactions: 0,
                background_compactions: 1,
                violations: Vec::new(),
                wall_secs: 1.0,
                object_store: ObjectStorePhaseCensus::default(),
                fired_faults: Vec::new(),
            }],
            &Coverage::default(),
        );

        assert!(report.contains("## Operational Proofs"));
        assert!(report.contains("| 7 | 1 | 1 | 1 | 1 | 1 | 1 |"));
        assert!(report.contains(
            "Counts come from persisted operation targets and causal runner-timeline evidence."
        ));
        assert!(report.contains("## Fault Timeline Summary"));
        assert!(report
            .contains("| event id | op window | boundary | kind | action | observed | recovery |"));
    }

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
            next_op_index: 0,
            pending_ops: BTreeMap::new(),
            fault_contracts: BTreeMap::new(),
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

    #[tokio::test]
    async fn s3_final_records_key_deleted_after_list() {
        let temp_dir = tempfile::tempdir().expect("failed to create artifact temp dir");
        let ops = BufWriter::new(
            File::create(temp_dir.path().join("ops.jsonl")).expect("failed to create ops file"),
        );
        let artifacts = SeedArtifacts {
            dir: temp_dir.path().to_path_buf(),
            ops,
            op_count: 0,
            next_op_index: 0,
            pending_ops: BTreeMap::new(),
            fault_contracts: BTreeMap::new(),
        };
        let ns = "ns";
        let key = format!("{ns}/meta.json");
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
            .write_s3_final(&capture_store, &[ns.to_string()])
            .await;

        let final_inventory = fs::read_to_string(temp_dir.path().join("s3-final.txt"))
            .expect("s3-final.txt should be written");
        assert_eq!(
            final_inventory,
            format!("# {ns}\n{key}\tmissing_after_list\n")
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

fn build_report(
    root: &Path,
    manifest: &RunManifest,
    seeds: &[SeedReport],
    coverage: &Coverage,
) -> String {
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

    let failed = seeds
        .iter()
        .filter(|seed| seed.failed && seed_blocks_v1(seed))
        .count();
    let research_findings = seeds
        .iter()
        .filter(|seed| seed.failed && !seed_blocks_v1(seed))
        .count();
    let ops = seeds.iter().map(|seed| seed.ops).sum::<u64>();
    let wall = seeds
        .iter()
        .map(|seed| seed.wall_secs)
        .sum::<f64>()
        .max(0.001);
    let mut out = String::new();
    out.push_str("# Adversarial Runner Report\n\n");
    out.push_str(&format!("- git rev: `{}`\n", manifest.git_rev));
    out.push_str(&format!(
        "- dirty tree: `{}`\n",
        if manifest.dirty_tree { "true" } else { "false" }
    ));
    out.push_str(&format!("- date_unix_s: `{}`\n", now_unix_secs()));
    out.push_str(&format!(
        "- backend: `{}`\n",
        manifest.backend.as_deref().unwrap_or("memory")
    ));
    out.push_str(&format!("- mode: `{:?}`\n", manifest.mode));
    out.push_str(&format!("- budget_s: `{}`\n", manifest.seconds));
    out.push_str(&format!("- run dir: `{}`\n\n", root.display()));

    out.push_str("## Seeds\n\n");
    out.push_str(
        "| seed | mode | profile | status | ops | explicit compactions | bg compactions | faults | wall_s | ops/sec |\n",
    );
    out.push_str("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for seed in seeds {
        let profile = seed
            .profile
            .map_or_else(|| "none".to_string(), |profile| format!("{:?}", profile));
        out.push_str(&format!(
            "| {} | `{:?}` | `{}` | {} | {} | {} | {} | {} | {:.2} | {:.2} |\n",
            seed.seed,
            seed.mode,
            profile,
            if seed.failed && seed_blocks_v1(seed) {
                "failed"
            } else if seed.failed {
                "research-finding"
            } else {
                "passed"
            },
            seed.ops,
            seed.compactions,
            seed.background_compactions,
            seed.fired_faults.len() + read_timeline(&seed.dir).len(),
            seed.wall_secs,
            seed.ops as f64 / seed.wall_secs.max(0.001)
        ));
    }
    let explicit_compactions = seeds.iter().map(|seed| seed.compactions).sum::<u64>();
    let background_compactions = seeds
        .iter()
        .map(|seed| seed.background_compactions)
        .sum::<u64>();
    out.push_str(&format!(
        "\nSummary: seeds={}, failed={}, research_findings={}, ops={}, explicit_compactions={}, background_compactions={}, ops/sec={:.2}\n\n",
        seeds.len(),
        failed,
        research_findings,
        ops,
        explicit_compactions,
        background_compactions,
        ops as f64 / wall
    ));

    if seeds.iter().any(|seed| {
        read_ops(&seed.dir)
            .iter()
            .any(|record| record.op.tags().contains(&"security"))
    }) {
        out.push_str("## Authorization Summary\n\n");
        out.push_str(
            "| seed | allow | forbidden | unauthorized | staleness resolutions | I22 | I23 | I24 | I25 | I26 | I27 | I28 |\n",
        );
        out.push_str(
            "| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- | --- |\n",
        );
        for seed in seeds {
            let records = read_ops(&seed.dir);
            let security_records = records
                .iter()
                .filter(|record| record.op.tags().contains(&"security"))
                .collect::<Vec<_>>();
            let allow = security_records
                .iter()
                .filter(|record| (200..300).contains(&record.status))
                .count();
            let forbidden = security_records
                .iter()
                .filter(|record| record.status == 403)
                .count();
            let unauthorized = security_records
                .iter()
                .filter(|record| record.status == 401)
                .count();
            let staleness = security_records
                .iter()
                .filter(|record| {
                    matches!(record.op, super::ops::Op::UseRevokedCredential { .. })
                        && matches!(record.status, 200 | 401)
                })
                .count();
            let oracle_status = |id| {
                if seed.violations.iter().any(|violation| violation.id == id) {
                    "fail"
                } else {
                    "pass"
                }
            };
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                seed.seed,
                allow,
                forbidden,
                unauthorized,
                staleness,
                oracle_status(ViolationId::I22AuthzDecision),
                oracle_status(ViolationId::I23TenantLeak),
                oracle_status(ViolationId::I24RevocationFreshness),
                oracle_status(ViolationId::I25AuditEvidence),
                oracle_status(ViolationId::I26SecurityStateSanity),
                oracle_status(ViolationId::I27ConstraintDrop),
                oracle_status(ViolationId::I28PreservationBypass),
            ));
        }
        out.push('\n');
    }

    out.push_str("## Contract Classification\n\n");
    out.push_str("| seed | contract class | violated assumptions | blocking v1 gate |\n");
    out.push_str("| --- | --- | --- | --- |\n");
    let mut has_research_campaign = false;
    for seed in seeds {
        let contracts = seed_fault_contracts(seed);
        let classes = contract_classes(&contracts);
        let assumptions = contract_assumptions(&contracts);
        let blocks_v1 = classes.iter().all(|class| class.blocks_v1());
        has_research_campaign |= !blocks_v1;
        out.push_str(&format!(
            "| {} | `{}` | `{}` | `{}` |\n",
            seed.seed,
            classes
                .iter()
                .map(|class| format!("{class:?}"))
                .collect::<Vec<_>>()
                .join(","),
            assumptions
                .iter()
                .map(|assumption| format!("{assumption:?}"))
                .collect::<Vec<_>>()
                .join(","),
            if blocks_v1 { "yes" } else { "no" },
        ));
    }
    if has_research_campaign {
        out.push_str(
            "\nProvider-contract-abuse and future-architecture results are research findings only, not a v1 product bug or blocking v1 gate.\n\n",
        );
    } else {
        out.push('\n');
    }

    out.push_str("## Operational Proofs\n\n");
    out.push_str(
        "| seed | node 0 ops | node 1 ops | two-node contention | delete joins | resource limits | exhaustion bursts |\n",
    );
    out.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for seed in seeds {
        let proof = operational_report_proof(&seed.dir);
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} |\n",
            seed.seed,
            proof.node_0_ops,
            proof.node_1_ops,
            proof.two_node_contention,
            proof.delete_joins,
            proof.resource_limits,
            proof.exhaustion_bursts,
        ));
    }
    out.push_str(
        "\nCounts come from persisted operation targets and causal runner-timeline evidence.\n\n",
    );

    out.push_str("## Violations\n\n");
    let mut any_violation = false;
    for seed in seeds {
        for violation in &seed.violations {
            any_violation = true;
            let contracts = seed_fault_contracts(seed);
            let classes = contract_classes(&contracts);
            let assumptions = contract_assumptions(&contracts);
            let blocks_v1 = classes.iter().all(|class| class.blocks_v1());
            out.push_str(&format!(
                "- seed {} op {} `{:?}` `{}` [class=`{}` assumptions=`{}` v1_blocking=`{}`]: {}\n",
                seed.seed,
                violation.op_index,
                violation.id,
                violation.namespace,
                classes
                    .iter()
                    .map(|class| format!("{class:?}"))
                    .collect::<Vec<_>>()
                    .join(","),
                assumptions
                    .iter()
                    .map(|assumption| format!("{assumption:?}"))
                    .collect::<Vec<_>>()
                    .join(","),
                if blocks_v1 { "yes" } else { "no" },
                violation.detail
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
    for seed in seeds
        .iter()
        .filter(|seed| seed.profile == Some(FaultProfile::LegacyChaos))
    {
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
        out.push_str("No LegacyChaos faults fired.\n");
    }
    out.push('\n');

    let has_timeline = seeds
        .iter()
        .any(|seed| seed.dir.join("timeline.jsonl").exists());
    if has_timeline {
        out.push_str("## Fault Timeline\n\n");
        let mut summary = BTreeMap::<(String, String), TimelineSummary>::new();
        for seed in seeds {
            let timeline = read_timeline(&seed.dir);
            if timeline.is_empty() {
                continue;
            }
            let config = read_seed_config(&seed.dir);
            let schedule = config
                .get("fault_schedule")
                .filter(|value| !value.is_null())
                .map(|value| {
                    serde_json::from_value::<FaultSchedule>(value.clone()).unwrap_or_else(|error| {
                        panic!(
                            "failed to parse fault schedule in {}: {error}",
                            seed.dir.display()
                        )
                    })
                });
            let contexts = schedule.map_or_else(BTreeMap::new, |schedule| {
                schedule
                    .events
                    .into_iter()
                    .map(|event| {
                        let window = event.end_op.map_or_else(
                            || format!("{}+", event.start_op),
                            |end| format!("{}..{}", event.start_op, end),
                        );
                        let kind = fault_kind_name(&event.kind);
                        let class = event.contract_class();
                        let assumptions = event.violated_assumptions().to_vec();
                        (event.id, (window, kind, class, assumptions))
                    })
                    .collect()
            });
            out.push_str(&format!("### Seed {}\n\n", seed.seed));
            out.push_str(
                "| event id | op window | boundary | kind | action | observed | recovery | contract class | violated assumptions | blocking v1 |\n",
            );
            out.push_str("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");
            for event in timeline {
                let (window, kind, class, assumptions) = match contexts.get(&event.event_id) {
                    Some((window, kind, class, assumptions)) => (
                        window.as_str(),
                        kind.as_str(),
                        *class,
                        assumptions.as_slice(),
                    ),
                    None if event.event_id.starts_with("quiet-")
                        && event.action.starts_with("quiet:") =>
                    {
                        (
                            "recorded-only",
                            "QuietPeriod",
                            ContractClass::SupportedV1,
                            &[][..],
                        )
                    }
                    None => panic!(
                        "timeline event {} in {} is missing persisted fault-schedule context",
                        event.event_id,
                        seed.dir.display()
                    ),
                };
                let recovery = event.recovery.as_deref().unwrap_or("none");
                let boundary = format!("{:?}", event.boundary);
                let counts = summary
                    .entry((boundary.clone(), kind.to_string()))
                    .or_default();
                counts.observe(&event);
                out.push_str(&format!(
                    "| `{}` | `{}` | `{}` | `{}` | `{}` | `{:?}` | `{}` | `{:?}` | `{}` | `{}` |\n",
                    markdown_cell(&event.event_id),
                    markdown_cell(window),
                    boundary,
                    markdown_cell(kind),
                    markdown_cell(&event.action),
                    event.observed,
                    markdown_cell(recovery),
                    class,
                    assumptions
                        .iter()
                        .map(|assumption| format!("{assumption:?}"))
                        .collect::<Vec<_>>()
                        .join(","),
                    if class.blocks_v1() { "yes" } else { "no" },
                ));
            }
            out.push('\n');
        }
        out.push_str("## Fault Timeline Summary\n\n");
        out.push_str(
            "| boundary | kind | events | applied | not applied | violation | unresolved |\n",
        );
        out.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: |\n");
        for ((boundary, kind), counts) in summary {
            out.push_str(&format!(
                "| `{}` | `{}` | {} | {} | {} | {} | {} |\n",
                markdown_cell(&boundary),
                markdown_cell(&kind),
                counts.events,
                counts.applied,
                counts.not_applied,
                counts.violation,
                counts.unresolved,
            ));
        }
        out.push('\n');
    }

    out.push_str("## Indeterminate Resolutions\n\n");
    let mut any_resolution = false;
    for seed in seeds {
        let path = seed.dir.join("resolutions.json");
        if !path.exists() {
            continue;
        }
        let bytes = fs::read(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        let entries: Vec<serde_json::Value> = serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()));
        if entries.is_empty() {
            continue;
        }
        any_resolution = true;
        let mut counts = BTreeMap::<String, u64>::new();
        for entry in &entries {
            let resolved = entry
                .get("resolved")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            *counts.entry(resolved.to_string()).or_default() += 1;
        }
        out.push_str(&format!(
            "- seed {}: `{}` ({} entries)\n",
            seed.seed,
            path.display(),
            entries.len()
        ));
        for (resolved, count) in counts {
            out.push_str(&format!("  - `{resolved}`: {count}\n"));
        }
    }
    if !any_resolution {
        out.push_str("No indeterminate writes required resolution.\n");
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
            let marker = if manifest.profile == Some(FaultProfile::Security)
                && SECURITY_OP_KINDS.contains(&kind.as_str())
                && *count < 5
            {
                " ⚠ below security floor 5"
            } else {
                ""
            };
            out.push_str(&format!("- `{kind}`: {count}{marker}\n"));
        }
    }
    out.push('\n');

    out.push_str("## Security Oracle Coverage\n\n");
    for oracle in ["I22", "I23", "I24", "I25", "I26", "I27", "I28"] {
        let count = coverage
            .security_oracle_counts
            .get(oracle)
            .copied()
            .unwrap_or(0);
        let marker = if manifest.profile == Some(FaultProfile::Security) && count == 0 {
            " ⚠"
        } else {
            ""
        };
        out.push_str(&format!("- `{oracle}`: {count}{marker}\n"));
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

    render_object_store_totals(
        &mut out,
        "Object-Store In-Run Totals",
        object_store_totals(seeds.iter().map(|seed| &seed.object_store.in_run)),
    );
    render_object_store_totals(
        &mut out,
        "Object-Store Quiet-Period Totals",
        object_store_totals(seeds.iter().map(|seed| &seed.object_store.quiet_period)),
    );
    render_object_store_totals(
        &mut out,
        "Object-Store Totals",
        object_store_totals(
            seeds
                .iter()
                .flat_map(|seed| [&seed.object_store.in_run, &seed.object_store.quiet_period]),
        ),
    );
    out
}

#[derive(Debug, Default)]
struct TimelineSummary {
    events: u64,
    applied: u64,
    not_applied: u64,
    violation: u64,
    unresolved: u64,
}

impl TimelineSummary {
    fn observe(&mut self, event: &TimelineEvent) {
        self.events += 1;
        match event.observed {
            ObservedResult::DefiniteApplied => self.applied += 1,
            ObservedResult::DefiniteNotApplied => self.not_applied += 1,
            ObservedResult::Corrupted => self.violation += 1,
            ObservedResult::Ambiguous => match terminal_ambiguity_resolution(event) {
                Some(TerminalResolution::Applied) => self.applied += 1,
                Some(TerminalResolution::NotApplied) => self.not_applied += 1,
                Some(TerminalResolution::Violation) => self.violation += 1,
                None => self.unresolved += 1,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalResolution {
    Applied,
    NotApplied,
    Violation,
}

fn terminal_ambiguity_resolution(event: &TimelineEvent) -> Option<TerminalResolution> {
    match event.recovery.as_deref().map(str::trim) {
        Some("applied" | "inner mutation completed; acknowledgement replaced") => {
            Some(TerminalResolution::Applied)
        }
        Some("not_applied") => Some(TerminalResolution::NotApplied),
        Some(recovery) if recovery.starts_with("violation:") => Some(TerminalResolution::Violation),
        Some(recovery)
            if recovery
                .strip_prefix("violations=")
                .and_then(|count| count.parse::<u64>().ok())
                .is_some_and(|count| count > 0) =>
        {
            Some(TerminalResolution::Violation)
        }
        Some(_) | None => None,
    }
}

fn fault_kind_name(kind: &FaultKind) -> String {
    let debug = format!("{kind:?}");
    debug
        .split([' ', '{', '('])
        .next()
        .expect("FaultKind debug output must start with its variant")
        .to_string()
}

fn seed_fault_contracts(seed: &SeedReport) -> Vec<FaultContract> {
    if !seed.dir.join("config.json").exists() {
        return profile_contracts(seed.profile);
    }
    let config = read_seed_config(&seed.dir);
    if let Some(value) = config
        .get("fault_contracts")
        .filter(|value| !value.is_null())
    {
        let contracts: Vec<FaultContract> =
            serde_json::from_value(value.clone()).unwrap_or_else(|error| {
                panic!(
                    "failed to parse fault contracts in {}: {error}",
                    seed.dir.display()
                )
            });
        if !contracts.is_empty() {
            return contracts;
        }
    }
    if let Some(value) = config
        .get("fault_schedule")
        .filter(|value| !value.is_null())
    {
        let schedule: FaultSchedule =
            serde_json::from_value(value.clone()).unwrap_or_else(|error| {
                panic!(
                    "failed to parse legacy fault schedule in {}: {error}",
                    seed.dir.display()
                )
            });
        let contracts = schedule.contracts();
        if !contracts.is_empty() {
            return contracts;
        }
    }
    vec![FaultContract {
        event_id: "no-scheduled-faults".to_string(),
        contract_class: ContractClass::SupportedV1,
        violated_assumptions: Vec::new(),
    }]
}

fn profile_contracts(profile: Option<FaultProfile>) -> Vec<FaultContract> {
    let classes: &[ContractClass] = match profile {
        Some(FaultProfile::ProviderContractAbuse | FaultProfile::Content) => {
            &[ContractClass::ProviderContractAbuse]
        }
        Some(FaultProfile::FutureArchitecture | FaultProfile::Ops) => {
            &[ContractClass::FutureArchitecture]
        }
        Some(FaultProfile::Semantic) => &[ContractClass::ProviderContractAbuse],
        Some(FaultProfile::Full) => &[
            ContractClass::ProviderContractAbuse,
            ContractClass::FutureArchitecture,
        ],
        Some(
            FaultProfile::LegacyChaos
            | FaultProfile::PostCommit
            | FaultProfile::Network
            | FaultProfile::Crash
            | FaultProfile::Clock
            | FaultProfile::SupportedFull
            | FaultProfile::Security
            | FaultProfile::Branching
            | FaultProfile::Sched,
        )
        | None => &[ContractClass::SupportedV1],
    };
    classes
        .iter()
        .map(|class| FaultContract {
            event_id: "profile-classification".to_string(),
            contract_class: *class,
            violated_assumptions: match class {
                ContractClass::ProviderContractAbuse => vec![ProtectedAssumption::A2],
                ContractClass::FutureArchitecture => vec![ProtectedAssumption::A3],
                ContractClass::SupportedV1 | ContractClass::HarnessSelfTest => Vec::new(),
            },
        })
        .collect()
}

fn contract_classes(contracts: &[FaultContract]) -> BTreeSet<ContractClass> {
    contracts
        .iter()
        .map(|contract| contract.contract_class)
        .collect()
}

fn contract_assumptions(contracts: &[FaultContract]) -> BTreeSet<ProtectedAssumption> {
    contracts
        .iter()
        .flat_map(|contract| contract.violated_assumptions.iter().copied())
        .collect()
}

fn seed_blocks_v1(seed: &SeedReport) -> bool {
    seed_fault_contracts(seed)
        .iter()
        .all(|contract| contract.contract_class.blocks_v1())
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|").replace(['\r', '\n'], " ")
}

#[derive(Debug, Default, PartialEq, Eq)]
struct OperationalReportProof {
    node_0_ops: u64,
    node_1_ops: u64,
    two_node_contention: u64,
    delete_joins: u64,
    resource_limits: u64,
    exhaustion_bursts: u64,
}

fn operational_report_proof(seed_dir: &Path) -> OperationalReportProof {
    let mut proof = OperationalReportProof::default();
    for record in read_ops(seed_dir) {
        match record.target_node {
            0 => proof.node_0_ops += 1,
            1 => proof.node_1_ops += 1,
            invalid => panic!("persisted operation target node must be 0 or 1, got {invalid}"),
        }
    }
    for event in read_timeline(seed_dir) {
        let recovery = event.recovery.as_deref().unwrap_or_default();
        if event.action == "stop second node"
            && event.semantics == super::faults::FaultSemantics::WindowEnd
            && full_two_node_contention_proof(recovery)
        {
            proof.two_node_contention += 1;
        }
        if event
            .action
            .starts_with("delete namespace with in-flight upsert")
            && recovery.contains("delete_joined=true")
            && recovery.contains("barrier_released=true")
            && recovery.contains("upsert_joined=true")
        {
            proof.delete_joins += 1;
        }
        if event.action.starts_with("resource limits queries=") {
            proof.resource_limits += 1;
        }
        if event.action == "fill disk cache with eight concurrent queries"
            && recovery.contains("completed=8")
        {
            proof.exhaustion_bursts += 1;
        }
    }
    proof
}

fn full_two_node_contention_proof(recovery: &str) -> bool {
    let Some(recovery) = recovery.strip_prefix("namespace=") else {
        return false;
    };
    let Some((namespace, evidence)) = recovery.split_once("; ") else {
        return false;
    };
    !namespace.is_empty()
        && evidence
            == "lease_attempt_nodes=[0,1]; lease_publication=true; fenced_manifest=true; \
                background_activity=true"
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

const SECURITY_OP_KINDS: &[&str] = &[
    "create_key",
    "rotate_key",
    "revoke_key",
    "publish_grant_change",
    "tenant_boundary_probe",
    "use_revoked_credential",
    "forbidden_write_probe",
    "export_probe",
    "security_admin_probe",
    "audit_barrier",
    "mint_token",
    "use_token",
    "token_exceed_scope_probe",
    "use_expired_token",
    "revoke_parent_then_use_token",
    "create_lock",
    "release_lock",
    "delete_under_lock",
    "gc_under_lock",
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
    "sketch-adc-v4",
];

fn object_store_totals<'a>(
    censuses: impl IntoIterator<Item = &'a ObjectStoreCensus>,
) -> ObjectStoreCensus {
    let mut totals = ObjectStoreCensus::new();
    for census in censuses {
        for (class, stats) in census {
            let total = totals.entry(*class).or_default();
            total.get_ops += stats.get_ops;
            total.get_bytes += stats.get_bytes;
            total.put_ops += stats.put_ops;
            total.put_bytes += stats.put_bytes;
        }
    }
    totals
}

fn render_object_store_totals(out: &mut String, heading: &str, totals: ObjectStoreCensus) {
    out.push_str(&format!("## {heading}\n\n"));
    out.push_str("| class | get_ops | get_bytes | put_ops | put_bytes |\n");
    out.push_str("| --- | ---: | ---: | ---: | ---: |\n");
    let mut totals = totals.into_iter().collect::<Vec<_>>();
    totals.sort_by_key(|(class, _)| class.name());
    for (class, stats) in totals {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} |\n",
            class.name(),
            stats.get_ops,
            stats.get_bytes,
            stats.put_ops,
            stats.put_bytes
        ));
    }
    out.push('\n');
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
