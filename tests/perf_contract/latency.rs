//! Deterministic cloud-latency injection and advisory model validation.
//!
//! TTFB samples use a closed-form lognormal fit:
//! `mu = ln(p50)` and `sigma = (ln(p99) - ln(p50)) / 2.326`.
//! Each operation hashes the configured seed, a stable logical key, and that
//! key's ordinal before drawing from the distribution. Unlike ChaosStore,
//! transfer delay occurs after the inner call succeeds because the returned
//! byte count is only known then; this decorator injects no failures.
//!
//! Local MinIO latency is not subtracted. It is sub-millisecond beside the
//! cloud-scale injected delay, leaving an expected measurement floor of about
//! five percent. Per-connection transfer time is modeled, but aggregate
//! bandwidth contention remains exclusively in the Tier 2 analytic model.
//! COPY is server-side and therefore has no client transfer component.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use serde::Serialize;
use xxhash_rust::xxh3::xxh3_64;
use zeppelin::storage::{StorageCapabilities, ZeppelinStore};

use super::contract::{check_contract, load_contract, CostViolation};
use super::predict::{model_input_from_repeat, predict, Prediction};
use super::profiles::StorageProfile;
use super::profiles::{load_profile, Profile};
use super::report::RunArtifacts;
use super::scenario::{
    run_closed_loop_scenario, run_scenario, run_scenario_decorated, ClosedLoopOutcome,
    ScenarioOutcome, ScenarioSpec,
};
use super::{require_minio, scenarios, PerfEnv, ALL_SCENARIOS};

const BYTES_PER_MB: f64 = 1_000_000.0;
const DEFAULT_PROFILE: &str = "s3-standard-intra-region";
const DEFAULT_SEED: u64 = 42;
const SERIAL_REQUESTS: usize = 200;
const CLOSED_LOOP_REQUESTS: usize = 500;
const DEFAULT_SCENARIOS: [&str; 3] = ["warm_query_strong", "cold_query_strong", "fts_query"];

pub use zeppelin::sizing::lognormal::LognormalMs;

/// Latency parameters derived from one profile's storage block.
#[derive(Debug, Clone, Copy)]
pub struct LatencyParams {
    pub ttfb: LognormalMs,
    pub per_conn_mbps: f64,
}

impl LatencyParams {
    #[must_use]
    pub fn from_storage(storage: &StorageProfile) -> Self {
        assert!(
            storage.per_conn_mbps.is_finite() && storage.per_conn_mbps > 0.0,
            "latency transfer rate must be finite and positive"
        );
        Self {
            ttfb: LognormalMs::from_percentiles(storage.ttfb_ms.p50, storage.ttfb_ms.p99),
            per_conn_mbps: storage.per_conn_mbps,
        }
    }
}

/// Deterministic, serializable injection totals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LatencyLedgerSnapshot {
    pub seed: u64,
    pub total_injected_sleep_us: u64,
    pub per_class_op_count: BTreeMap<String, u64>,
}

#[derive(Debug, Default)]
struct LatencyState {
    ordinals: DashMap<String, u64>,
    op_counts: DashMap<String, u64>,
    total_sleep_us: AtomicU64,
    enabled: std::sync::atomic::AtomicBool,
}

/// Shared handle used to reset and serialize latency injection state.
#[derive(Debug, Clone)]
pub struct LatencyLedger {
    seed: u64,
    state: Arc<LatencyState>,
}

impl LatencyLedger {
    fn new(seed: u64) -> Self {
        Self {
            seed,
            state: Arc::new(LatencyState {
                enabled: std::sync::atomic::AtomicBool::new(true),
                ..LatencyState::default()
            }),
        }
    }

    /// Disable injection during deterministic dataset and cache setup.
    pub fn disable(&self) {
        self.state.enabled.store(false, Ordering::SeqCst);
    }

    /// Reset setup traffic so the artifact describes measured traffic only.
    pub fn reset_and_enable(&self) {
        self.state.ordinals.clear();
        self.state.op_counts.clear();
        self.state.total_sleep_us.store(0, Ordering::SeqCst);
        self.state.enabled.store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub fn snapshot(&self) -> LatencyLedgerSnapshot {
        LatencyLedgerSnapshot {
            seed: self.seed,
            total_injected_sleep_us: self.state.total_sleep_us.load(Ordering::SeqCst),
            per_class_op_count: self
                .state
                .op_counts
                .iter()
                .map(|entry| (entry.key().clone(), *entry.value()))
                .collect(),
        }
    }

    fn next_sample(&self, class: &'static str, key: &str, ttfb: LognormalMs) -> u64 {
        let logical_key = stable_logical_key(key);
        let ordinal_key = format!("{class}:{logical_key}");
        let ordinal = {
            let mut entry = self.state.ordinals.entry(ordinal_key.clone()).or_insert(0);
            let ordinal = *entry;
            *entry = ordinal
                .checked_add(1)
                .expect("latency per-key ordinal overflowed");
            ordinal
        };
        self.state
            .op_counts
            .entry(class.to_string())
            .and_modify(|count| *count = count.checked_add(1).expect("latency op count overflowed"))
            .or_insert(1);

        let mut input = Vec::with_capacity(16 + ordinal_key.len());
        input.extend_from_slice(&self.seed.to_le_bytes());
        input.extend_from_slice(ordinal_key.as_bytes());
        input.extend_from_slice(&ordinal.to_le_bytes());
        ttfb.sample_us(xxh3_64(&input))
    }

    fn record_sleep(&self, micros: u64) {
        self.state
            .total_sleep_us
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |total| {
                total.checked_add(micros)
            })
            .expect("latency injected-sleep total overflowed");
    }
}

#[derive(Debug)]
struct LatencyProfileStore {
    inner: Arc<dyn ObjectStore>,
    params: LatencyParams,
    ledger: LatencyLedger,
}

/// Wrap a real Zeppelin store with deterministic profile latency.
#[must_use]
pub fn latency_profile_store(
    store: &ZeppelinStore,
    params: LatencyParams,
    seed: u64,
) -> (ZeppelinStore, LatencyLedger) {
    let ledger = LatencyLedger::new(seed);
    let latency = LatencyProfileStore {
        inner: store.inner(),
        params,
        ledger: ledger.clone(),
    };
    (
        ZeppelinStore::new_with_capabilities(Arc::new(latency), StorageCapabilities::s3()),
        ledger,
    )
}

impl LatencyProfileStore {
    async fn ttfb(&self, class: &'static str, key: &str) -> bool {
        if !self.ledger.state.enabled.load(Ordering::SeqCst) {
            return false;
        }
        let micros = self.ledger.next_sample(class, key, self.params.ttfb);
        tokio::time::sleep(Duration::from_micros(micros)).await;
        self.ledger.record_sleep(micros);
        true
    }

    async fn transfer(&self, bytes: usize) {
        let micros = (bytes as f64 / self.params.per_conn_mbps).round() as u64;
        tokio::time::sleep(Duration::from_micros(micros)).await;
        self.ledger.record_sleep(micros);
    }
}

impl fmt::Display for LatencyProfileStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LatencyProfileStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LatencyProfileStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let bytes = payload.content_length();
        let injected = self.ttfb("put", location.as_ref()).await;
        let result = self.inner.put_opts(location, payload, opts).await?;
        if injected {
            self.transfer(bytes).await;
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        let _ = self.ttfb("put", location.as_ref()).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let injected = self.ttfb("get", location.as_ref()).await;
        let result = self.inner.get_opts(location, options).await?;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let inner = result.into_stream();
        let ledger = self.ledger.clone();
        let per_conn_mbps = self.params.per_conn_mbps;
        let payload = stream::unfold(
            (inner, 0_usize, injected),
            move |(mut inner, bytes_seen, inject_transfer)| {
                let ledger = ledger.clone();
                async move {
                    match inner.next().await {
                        Some(Ok(bytes)) => {
                            let total = bytes_seen
                                .checked_add(bytes.len())
                                .expect("latency GET byte count overflowed");
                            Some((Ok(bytes), (inner, total, inject_transfer)))
                        }
                        Some(Err(error)) => Some((Err(error), (inner, bytes_seen, false))),
                        None => {
                            if inject_transfer {
                                let micros = (bytes_seen as f64 / per_conn_mbps).round() as u64;
                                tokio::time::sleep(Duration::from_micros(micros)).await;
                                ledger.record_sleep(micros);
                            }
                            None
                        }
                    }
                }
            },
        )
        .boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes,
        })
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        let _ = self.ttfb("head", location.as_ref()).await;
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        let _ = self.ttfb("delete", location.as_ref()).await;
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, OsResult<Path>>,
    ) -> BoxStream<'a, OsResult<Path>> {
        if !self.ledger.state.enabled.load(Ordering::SeqCst) {
            return self.inner.delete_stream(locations);
        }
        let micros = self
            .ledger
            .next_sample("delete_batch", "<delete-batch>", self.params.ttfb);
        let ledger = self.ledger.clone();
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let delayed_locations = locations
            .then(move |result| {
                let ledger = ledger.clone();
                let first = Arc::clone(&first);
                async move {
                    if first.swap(false, Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_micros(micros)).await;
                        ledger.record_sleep(micros);
                    }
                    result
                }
            })
            .boxed();
        self.inner.delete_stream(delayed_locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, OsResult<ObjectMeta>> {
        if !self.ledger.state.enabled.load(Ordering::SeqCst) {
            return self.inner.list(prefix);
        }
        let key = prefix.map(ToString::to_string).unwrap_or_default();
        let micros = self.ledger.next_sample("list", &key, self.params.ttfb);
        let ledger = self.ledger.clone();
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        stream::once(async move {
            tokio::time::sleep(Duration::from_micros(micros)).await;
            ledger.record_sleep(micros);
            inner.list(prefix.as_ref()).collect::<Vec<_>>().await
        })
        .flat_map(stream::iter)
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        let key = prefix.map(ToString::to_string).unwrap_or_default();
        let _ = self.ttfb("list", &key).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        let key = format!("{from}->{to}");
        let _ = self.ttfb("copy", &key).await;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        let key = format!("{from}->{to}");
        let _ = self.ttfb("copy", &key).await;
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[derive(Debug, Serialize)]
struct LatencyLedgerArtifact {
    profile: String,
    seed: u64,
    scenarios: BTreeMap<String, LatencyLedgerSnapshot>,
}

#[derive(Debug)]
struct ScenarioValidation {
    name: String,
    serial_prediction: Prediction,
    throughput_prediction: Prediction,
    serial: TimingSummary,
    closed_loop: TimingSummary,
    throughput_qps: f64,
    serial_injected_us: u64,
    closed_injected_us: u64,
    injected_depth_deviations: usize,
}

#[derive(Debug, Clone, Copy)]
struct TimingSummary {
    samples: usize,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

/// Execute deterministic serial contracts and advisory concurrent timing under
/// one cloud storage profile.
pub async fn run_latency_validate_entry() {
    require_minio();
    let profile = selected_latency_profile();
    let seed = selected_latency_seed();
    let labels = selected_latency_scenarios();
    let mut env = PerfEnv::from_env();
    env.scenarios.clone_from(&labels);
    env.repeats = SERIAL_REQUESTS;
    assert!(!env.capture, "latency_validate cannot run in capture mode");
    let artifacts = RunArtifacts::create(&env, "latency_validate", &labels);

    let mut validations = Vec::with_capacity(labels.len());
    let mut deterministic_ledgers = BTreeMap::new();
    for name in &labels {
        let contract = load_contract(name)
            .unwrap_or_else(|error| panic!("invalid latency scenario contract {name}: {error}"));
        let mut spec = scenarios::build(&contract, SERIAL_REQUESTS);
        spec.repeats = SERIAL_REQUESTS;
        assert_closed_loop_supported(&spec);

        let mut control_spec = spec.clone();
        control_spec.repeats = 1;
        let control = run_scenario(&control_spec, None).await;
        let control_violations = check_contract(&contract, &control);
        assert!(
            control_violations.is_empty(),
            "latency control violated Tier 1 contract for {name}: {control_violations:#?}"
        );
        artifacts.write_scenario(name, &control, &[]);

        let (serial, serial_ledger) = run_serial_with_latency(&spec, &profile, seed).await;
        let mut injected_depth_deviations = 0;
        for (repeat_index, repeat) in serial.per_repeat.iter().enumerate() {
            let single = ScenarioOutcome {
                per_repeat: vec![repeat.clone()],
                expected: serial.expected.clone(),
                security: serial.security.clone(),
            };
            let violations = check_contract(&contract, &single);
            injected_depth_deviations += violations
                .iter()
                .filter(|violation| matches!(violation, CostViolation::Depth { .. }))
                .count();
            let semantic_violations = violations
                .into_iter()
                .filter(|violation| !matches!(violation, CostViolation::Depth { .. }))
                .collect::<Vec<_>>();
            assert!(
                semantic_violations.is_empty(),
                "latency injection changed Tier 1 counters for {name} repeat {repeat_index}: {semantic_violations:#?}"
            );
        }

        let serial_repeat = control
            .per_repeat
            .first()
            .expect("latency serial scenario produced no repeats");
        let serial_prediction = predict(&model_input_from_repeat(serial_repeat, 1), &profile);
        let throughput_prediction = predict(
            &model_input_from_repeat(serial_repeat, profile.client.closed_loop_clients),
            &profile,
        );
        let serial_timing = summarize_timings(
            &serial
                .per_repeat
                .iter()
                .map(|repeat| repeat.wall_elapsed_us)
                .collect::<Vec<_>>(),
        );
        let (closed, closed_ledger) = run_closed_with_latency(&spec, &profile, seed).await;
        let closed_timing = summarize_timings(&closed.request_wall_us);
        let throughput_qps = CLOSED_LOOP_REQUESTS as f64 * 1_000_000.0 / closed.elapsed_us as f64;
        validations.push(ScenarioValidation {
            name: name.clone(),
            serial_prediction,
            throughput_prediction,
            serial: serial_timing,
            closed_loop: closed_timing,
            throughput_qps,
            serial_injected_us: serial_ledger.total_injected_sleep_us,
            closed_injected_us: closed_ledger.total_injected_sleep_us,
            injected_depth_deviations,
        });
        assert!(
            deterministic_ledgers
                .insert(name.clone(), serial_ledger)
                .is_none(),
            "duplicate latency ledger for {name}"
        );
    }

    let report = artifacts.write_report();
    let markdown = render_validation(&profile, seed, &validations);
    let ledger = LatencyLedgerArtifact {
        profile: profile.name.clone(),
        seed,
        scenarios: deterministic_ledgers,
    };
    let (validation_path, ledger_path) = artifacts.write_latency_validation(&markdown, &ledger);
    println!("latency validation report: {}", report.display());
    println!("latency advisory table: {}", validation_path.display());
    println!("deterministic latency ledger: {}", ledger_path.display());
}

async fn run_serial_with_latency(
    spec: &ScenarioSpec,
    profile: &Profile,
    seed: u64,
) -> (ScenarioOutcome, LatencyLedgerSnapshot) {
    let ledger = Arc::new(Mutex::new(None::<LatencyLedger>));
    let snapshot = Arc::new(Mutex::new(None::<LatencyLedgerSnapshot>));
    let params = LatencyParams::from_storage(&profile.storage);
    let decorate = {
        let ledger = Arc::clone(&ledger);
        move |store: &ZeppelinStore| {
            let (store, handle) = latency_profile_store(store, params, seed);
            handle.disable();
            let previous = ledger
                .lock()
                .expect("latency ledger mutex poisoned")
                .replace(handle);
            assert!(previous.is_none(), "serial latency store decorated twice");
            store
        }
    };
    let reset = {
        let ledger = Arc::clone(&ledger);
        move || {
            ledger
                .lock()
                .expect("latency ledger mutex poisoned")
                .as_ref()
                .expect("serial latency ledger missing before measurement")
                .reset_and_enable();
        }
    };
    let capture = {
        let ledger = Arc::clone(&ledger);
        let snapshot = Arc::clone(&snapshot);
        move || {
            let ledger = ledger.lock().expect("latency ledger mutex poisoned");
            let handle = ledger
                .as_ref()
                .expect("serial latency ledger missing after measurement");
            let value = handle.snapshot();
            handle.disable();
            let previous = snapshot
                .lock()
                .expect("latency snapshot mutex poisoned")
                .replace(value);
            assert!(previous.is_none(), "serial latency ledger captured twice");
        }
    };
    let outcome = run_scenario_decorated(spec, &decorate, &reset, &capture).await;
    let captured = snapshot
        .lock()
        .expect("latency snapshot mutex poisoned")
        .take()
        .expect("serial latency ledger was not captured");
    (outcome, captured)
}

async fn run_closed_with_latency(
    spec: &ScenarioSpec,
    profile: &Profile,
    seed: u64,
) -> (ClosedLoopOutcome, LatencyLedgerSnapshot) {
    let ledger = Arc::new(Mutex::new(None::<LatencyLedger>));
    let snapshot = Arc::new(Mutex::new(None::<LatencyLedgerSnapshot>));
    let params = LatencyParams::from_storage(&profile.storage);
    let decorate = {
        let ledger = Arc::clone(&ledger);
        move |store: &ZeppelinStore| {
            let (store, handle) = latency_profile_store(store, params, seed);
            handle.disable();
            let previous = ledger
                .lock()
                .expect("latency ledger mutex poisoned")
                .replace(handle);
            assert!(
                previous.is_none(),
                "closed-loop latency store decorated twice"
            );
            store
        }
    };
    let reset = {
        let ledger = Arc::clone(&ledger);
        move || {
            ledger
                .lock()
                .expect("latency ledger mutex poisoned")
                .as_ref()
                .expect("closed-loop latency ledger missing before measurement")
                .reset_and_enable();
        }
    };
    let capture = {
        let ledger = Arc::clone(&ledger);
        let snapshot = Arc::clone(&snapshot);
        move || {
            let ledger = ledger.lock().expect("latency ledger mutex poisoned");
            let handle = ledger
                .as_ref()
                .expect("closed-loop latency ledger missing after measurement");
            let value = handle.snapshot();
            handle.disable();
            let previous = snapshot
                .lock()
                .expect("latency snapshot mutex poisoned")
                .replace(value);
            assert!(
                previous.is_none(),
                "closed-loop latency ledger captured twice"
            );
        }
    };
    let outcome = run_closed_loop_scenario(
        spec,
        profile.client.closed_loop_clients,
        CLOSED_LOOP_REQUESTS,
        &decorate,
        &reset,
        &capture,
    )
    .await;
    let captured = snapshot
        .lock()
        .expect("latency snapshot mutex poisoned")
        .take()
        .expect("closed-loop latency ledger was not captured");
    (outcome, captured)
}

fn summarize_timings(micros: &[u64]) -> TimingSummary {
    assert!(!micros.is_empty(), "latency timing set cannot be empty");
    let mut sorted = micros.to_vec();
    sorted.sort_unstable();
    let mean_ms =
        sorted.iter().map(|value| *value as f64).sum::<f64>() / sorted.len() as f64 / 1_000.0;
    TimingSummary {
        samples: sorted.len(),
        mean_ms,
        p50_ms: percentile_us(&sorted, 0.50) / 1_000.0,
        p95_ms: percentile_us(&sorted, 0.95) / 1_000.0,
        p99_ms: percentile_us(&sorted, 0.99) / 1_000.0,
    }
}

fn percentile_us(sorted: &[u64], quantile: f64) -> f64 {
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)] as f64
}

fn render_validation(profile: &Profile, seed: u64, rows: &[ScenarioValidation]) -> String {
    let mut out = String::new();
    out.push_str("# Tier 3 - Latency validation (advisory)\n\n");
    out.push_str(&format!("- profile: `{}`\n", profile.name));
    out.push_str(&format!("- xxh3 seed: `{seed}`\n"));
    out.push_str(&format!(
        "- serial requests per scenario: `{SERIAL_REQUESTS}`\n"
    ));
    out.push_str(&format!(
        "- closed-loop clients: `{}`; request budget: `{CLOSED_LOOP_REQUESTS}`\n",
        profile.client.closed_loop_clients
    ));
    out.push_str("- wall-clock deltas are advisory and never gate CI\n");
    out.push_str("- local MinIO latency is not subtracted; expect an approximately 5% floor\n");
    out.push_str("- injected transfer delay is per connection; aggregate bandwidth contention remains a Tier 2 term\n");
    out.push_str("- Tier 1 control depth remains gating; injected interval depth is advisory because sampled TTFB changes real overlap\n");
    out.push_str("- closed-loop depth is skipped because requests overlap\n\n");
    out.push_str("| Scenario | Metric | Predicted | Measured | Delta | Finding |\n");
    out.push_str("|---|---:|---:|---:|---:|---|\n");
    for row in rows {
        let metrics = [
            (
                "serial mean ms",
                row.serial_prediction.mean_latency_ms,
                row.serial.mean_ms,
                true,
            ),
            (
                "serial p50 ms",
                row.serial_prediction.p50_ms,
                row.serial.p50_ms,
                false,
            ),
            (
                "serial p99 ms",
                row.serial_prediction.p99_ms,
                row.serial.p99_ms,
                false,
            ),
            (
                "closed-loop QPS",
                row.throughput_prediction.qps,
                row.throughput_qps,
                false,
            ),
        ];
        for (metric, predicted, measured, mean_metric) in metrics {
            let delta = delta_percent(predicted, measured);
            let finding = if mean_metric && delta.abs() > 25.0 {
                "MODEL-GAP"
            } else {
                "advisory"
            };
            out.push_str(&format!(
                "| `{}` | {} | {:.2} | {:.2} | {:+.1}% | {} |\n",
                row.name, metric, predicted, measured, delta, finding
            ));
        }
    }

    out.push_str("\n## Closed-loop observations\n\n");
    out.push_str("| Scenario | Samples | Mean ms | p50 ms | p95 ms | p99 ms | Serial injected s | Closed injected s | Injected depth deviations |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for row in rows {
        out.push_str(&format!(
            "| `{}` | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.3} | {:.3} | {} |\n",
            row.name,
            row.closed_loop.samples,
            row.closed_loop.mean_ms,
            row.closed_loop.p50_ms,
            row.closed_loop.p95_ms,
            row.closed_loop.p99_ms,
            row.serial_injected_us as f64 / 1_000_000.0,
            row.closed_injected_us as f64 / 1_000_000.0,
            row.injected_depth_deviations,
        ));
    }
    out.push('\n');
    out
}

fn delta_percent(predicted: f64, measured: f64) -> f64 {
    assert!(predicted > 0.0, "latency prediction must be positive");
    (measured - predicted) / predicted * 100.0
}

fn selected_latency_profile() -> Profile {
    let name = match std::env::var("ZEPPELIN_PERF_LATENCY_PROFILE") {
        Ok(name) if !name.trim().is_empty() => name,
        Ok(_) => panic!("ZEPPELIN_PERF_LATENCY_PROFILE cannot be empty"),
        Err(std::env::VarError::NotPresent) => DEFAULT_PROFILE.to_string(),
        Err(error) => panic!("failed to read ZEPPELIN_PERF_LATENCY_PROFILE: {error}"),
    };
    load_profile(&name)
}

fn selected_latency_seed() -> u64 {
    match std::env::var("ZEPPELIN_PERF_LATENCY_SEED") {
        Ok(raw) => raw
            .parse::<u64>()
            .unwrap_or_else(|error| panic!("invalid ZEPPELIN_PERF_LATENCY_SEED={raw:?}: {error}")),
        Err(std::env::VarError::NotPresent) => DEFAULT_SEED,
        Err(error) => panic!("failed to read ZEPPELIN_PERF_LATENCY_SEED: {error}"),
    }
}

fn selected_latency_scenarios() -> Vec<String> {
    let selected = match std::env::var("ZEPPELIN_PERF_SCENARIOS") {
        Ok(raw) => raw
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>(),
        Err(std::env::VarError::NotPresent) => DEFAULT_SCENARIOS
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        Err(error) => panic!("failed to read ZEPPELIN_PERF_SCENARIOS: {error}"),
    };
    assert!(
        !selected.is_empty(),
        "ZEPPELIN_PERF_SCENARIOS must select at least one latency scenario"
    );
    for name in &selected {
        assert!(
            ALL_SCENARIOS.contains(&name.as_str()),
            "unknown latency scenario {name:?}"
        );
    }
    selected
}

fn assert_closed_loop_supported(spec: &ScenarioSpec) {
    use super::scenario::MeasureOp;
    assert!(
        matches!(
            &spec.measure,
            MeasureOp::Query { .. }
                | MeasureOp::FtsQuery { .. }
                | MeasureOp::HybridQuery { .. }
                | MeasureOp::AsOfQuery { .. }
                | MeasureOp::Fetch { .. }
        ),
        "latency_validate closed-loop pass does not support scenario {:?}",
        spec.name
    );
}

fn stable_logical_key(key: &str) -> String {
    if let Some((from, to)) = key.split_once("->") {
        return format!("{}->{}", stable_logical_key(from), stable_logical_key(to));
    }
    let filename = key.rsplit('/').next().unwrap_or(key);
    if filename.is_empty() {
        "<root>".to_string()
    } else if filename.ends_with(".wal") {
        "<wal>.wal".to_string()
    } else if filename
        .strip_suffix(".msgpack")
        .is_some_and(|stem| !stem.is_empty() && stem.bytes().all(|byte| byte.is_ascii_digit()))
    {
        "<generation>.msgpack".to_string()
    } else if filename.starts_with("test-") {
        "<namespace>".to_string()
    } else {
        filename.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampling_is_deterministic_and_tracks_ordinals() {
        let distribution = LognormalMs::from_percentiles(15.0, 60.0);
        let ledger = LatencyLedger::new(42);
        let first = ledger.next_sample("get", "random/cluster_0.bin", distribution);
        let second = ledger.next_sample("get", "other/cluster_0.bin", distribution);
        assert_ne!(first, second);
        ledger.reset_and_enable();
        assert_eq!(
            first,
            ledger.next_sample("get", "third/cluster_0.bin", distribution)
        );
    }

    #[test]
    fn transfer_constant_is_pinned() {
        // The exact quantile-fit assertions moved with LognormalMs to
        // zeppelin::sizing::lognormal, where the fit fields are visible.
        assert_eq!(BYTES_PER_MB as u64, 1_000_000);
    }
}
