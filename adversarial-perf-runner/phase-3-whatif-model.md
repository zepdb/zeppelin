# Phase 3 — Tier 2: Analytic What-If Layer (Profiles, Calibration, $/query)

**Sequence**: Phase 3 of 4. Prerequisites: Phases 1–2 merged (full contract catalog green; two standard dataset shapes `shape_small` 4096×64/nlist 8 and `shape_medium` 32768×128/nlist 32 measured).
**Required reading**: `adversarial-perf-runner/full_plan.md` (§7 in full, §9–§11), Phase 1–2 code as merged, `CLAUDE.md`.

## Context (self-contained recap)

Tier 1 (Phases 1–2) measures exact per-class S3 op/byte counts and sequential roundtrip depths per scenario, deterministically, on laptop MinIO. This phase multiplies those counters through parameterized **storage/hardware/pricing profiles** to predict QPS, p50/p99 latency, and **$/query** for deployments not yet run — real S3, multiple nodes, 21M vectors — and validates the model against two known ground-truth measurements before anyone trusts a prediction. Output is a report/table generator (an `#[ignore]` test entry), **not a service, not a simulator**. Recall/precision is explicitly not predicted: Tier 2 predicts cost/latency at a configured recall point; recall parity is the benchmark campaign's job.

Why this matters now: the RaBitQ quantization uplift (`tasks/July10Quant/00-README.md`) expects ~10× bytes/query reduction, and the campaign target is Qdrant's 111.9 QPS on wiki_dpr_e5 (21M × 768d, 3 × 4vCPU/16GB nodes) and Elasticsearch DiskBBQ's 32.4 QPS. This phase produces the table that says what Zeppelin's architecture should score there, and at what $/query, before the run happens.

## Current-state code map (verify at HEAD)

- `tests/perf_contract/` — Phases 1–2: `contract.rs` (checker), `scenario.rs` (runner), artifacts `counters.json` (per-class `ClassStats`) and `depth.json` (critical path **and stage structure**: spans grouped by depth level give per-stage op counts and bytes — this is Tier 2's latency input; if Phase 1 stored only the maximal chain, extend `depth.json` now to include per-stage totals `stages: [{ depth, ops, bytes, classes }]` — an artifacts-format addition, not new instrumentation).
- `tests/perf_contract/profiles/` — new this phase. `toml = "0.8"` already available to integration tests.
- Ground truths (provenance for fixtures):
  - **GT-A**: QPS loop accepted line 2026-07-05 — dbpedia100k, np16, 8 closed-loop workers, MinIO-in-docker: **70.6 MB/query, 5.64 GETs/query, measured 5.89 QPS**; MinIO aggregate bandwidth wall **410 MB/s** proven by raw boto3 probe (377 MB/s at 1 worker); analytic 410/70.6 = 5.81 matched measurement within 1.4%. Source: `tasks/opt-qps-log.md` (gitignored, working-dir) + project memory `qps-loop-2026-07`.
  - **GT-B**: `~/Documents/code/zeppelin-bench/vector-db-benchmark/results/zeppelin-default-glove-100-angular-search-0-2026-07-04-07-54-55.json` — glove-100-angular, parallel 8, nprobe 8: `rps 83.94`, `mean_time 0.0942 s`, `p95 0.1617 s`, `p99 0.3667 s`, `mean_precisions 0.8517`. (Companion files search-1..3 exist with other settings; use search-0 as the fixture, list the others in a comment.)
- Reference lines for the what-if table: Qdrant 111.9 QPS (wiki_dpr_e5 21M×768d, 3×4vCPU/16GB), Elasticsearch DiskBBQ 32.4 QPS.
- Pricing anchors (encode in profiles, cite AWS us-east-1 in comments): S3 GET $0.40/1M req, PUT $5.00/1M req, intra-region reads free egress; example node m6i.xlarge ≈ $0.192/hr.

## Deliverables

### 1. `tests/perf_contract/profiles/*.toml` — profile schema per full_plan.md §7.2

```toml
name = "minio-local-docker"
[storage]
ttfb_ms = { p50 = 1.0, p99 = 5.0 }
per_conn_MBps = 377.0          # measured single-worker boto3 probe
agg_MBps_per_node = 410.0      # the proven wall
[storage.price]
get_per_req = 0.0
put_per_req = 0.0
egress_per_gb = 0.0
[node]
count = 1
vcpus = 8
mem_gb = 16
price_hr = 0.0
cpu_ms_per_query = 0.0         # calibrated from GT-A; see calibration section
[client]
closed_loop_clients = 8
```

Ship three: `minio-local-docker` (above), `s3-standard-intra-region` (ttfb p50 15 / p99 60 ms, per_conn 80 MB/s, agg effectively unbounded — S3 scales per prefix; real prices; note S3-client internal retries, `src/storage/store.rs:235`–`242`, are un-modeled ≤ few-% $ effect), `s3-3node-wikidpr` (node count 3, 4 vCPU / 16 GB, target-shape parameters in `[whatif]` extension block: `vectors = 21_000_000`, `dims = 768`, plus sweep lists `nprobe = [...]`, `quantization = ["sq8", "rabitq-1bit", "rabitq-2bit"]` with `row_bytes` per variant — sq8 = dims, rabitq-1bit ≈ dims/8, rabitq-2bit ≈ dims/4, marked EXPECTED pending July10Quant results, cite its README §payoff).

`serde(deny_unknown_fields)`; loud parse failures.

### 2. `tests/perf_contract/ground_truth/*.toml` — fixtures

`gt-a.toml`: bytes_per_query_mb = 70.6, gets_per_query = 5.64, clients = 8, measured_qps = 5.89, profile = "minio-local-docker", provenance comment block. `gt-b.toml`: clients = 8, nprobe = 8, dataset = glove-100-angular (N = 1_183_514, dims = 100), measured rps/mean/p95/p99/precision from the results JSON, provenance path. **Honest caveat encoded as a comment and in the report**: GT-B's bytes/query were never measured; its input counters come from the shape model, so GT-B validates the closed-loop/latency machinery while GT-A anchors bytes→QPS.

### 3. `tests/perf_contract/predict.rs` — the model (full_plan.md §7.3, implement these equations exactly and document each in a doc comment)

```
stage_latency_i = ttfb.p50 + stage_bytes_i / min(per_conn_MBps × stage_ops_i, agg_share)
service_time    = cpu_ms_per_query + Σ stage_latency_i
QPS_bw_cap      = agg_MBps_per_node × node.count / bytes_per_query
QPS_closed      = clients / service_time
QPS_pred        = min(QPS_closed, QPS_bw_cap)
mean_latency    = clients / QPS_pred                    # inflates when bandwidth-bound
p50 ≈ service_time
p99 ≈ cpu_ms_per_query + Σ (ttfb.p99 + transfer_i)      # coarse; label ±50% in output
$/query         = Σ_class (get_ops×get_price + put_ops×put_price)
                  + get_bytes × egress_per_gb / 2^30
                  + node.count × price_hr / (QPS_pred × 3600)
```

Plus the **shape model**: express per-class ops/bytes as functions of (N, dims, nlist, nprobe, row_bytes): cluster bytes/query = probed_clusters × (N/nlist) × row_bytes (+ measured per-object overhead); manifest/bootstrap/sketch terms fitted as constants + linear terms from the two standard shapes. Fit on `shape_small`, no free parameters left when evaluating elsewhere.

### 4. Calibration + validation (asserted inside `predict`, not eyeballed)

1. Calibrate `cpu_ms_per_query` for `minio-local-docker` from GT-A (solve `QPS_pred(70.6 MB, clients=8) = 5.89`; agg = 410 is fixed, never fitted).
2. **GT-A validation**: `|QPS_pred − 5.89| / 5.89 ≤ 0.10` → hard assert.
3. **GT-B validation**: predict glove-100 @ nprobe 8, clients 8 on `minio-local-docker` using shape-model counters; assert QPS within ±20% and mean latency within ±25%. Print residuals either way.
4. **Shape-scaling check**: fit shape model on `shape_small` measurements, predict `shape_medium` per-class counters, compare against `shape_medium`'s actual Tier 1 `counters.json`: each class within ±10%, exact where closed-form → hard assert. This gates extrapolation to 21M.
5. All residuals go into the report; a failed assert fails the `predict` entry (report-only in CI means the job uploads the report but is non-gating — the *assertions* still make local/nightly runs trustworthy).

### 5. What-if output (report section + standalone table artifact)

`target/perf-contract/<run-id>/whatif.md` + a section appended to `report.md`:

- **wiki_dpr_e5 3-node table**: rows = quantization × nprobe sweep; columns = bytes/query, GETs/query, QPS_pred, p50/p99_pred, $/query (request cost and node cost split out), bottleneck (`bandwidth` | `closed-loop`). Reference lines printed under the table: `Qdrant 111.9 QPS`, `ES DiskBBQ 32.4 QPS`. Header: **PREDICTION — unvalidated at this scale; validated points: GT-A (±X% resid), GT-B (±Y% resid), shape-scaling (per-class resid table)**.
- Same table for `s3-standard-intra-region` single node at `shape_medium` (a near-term reality check someone can actually run).
- `ZEPPELIN_PERF_PROFILE=<name>[,<name>]` selects profiles; default = all shipped.

### 6. `predict` entry

`#[tokio::test] #[ignore] async fn predict()` — reads the latest Tier 1 artifacts (or runs the two standard-shape scenarios itself if `ZEPPELIN_PERF_ARTIFACTS` has none — prefer re-running: self-contained and deterministic), loads profiles + ground truths, calibrates, validates (hard asserts above), writes `whatif.md` + report section. Pure arithmetic — no new instrumentation, no network beyond the Tier 1 runs.

## Guardrails (binding; full_plan.md §11)

Zero production `src/` changes. No new dependencies (the model is closed-form arithmetic — no statrs/hdrhistogram; if you think you need a distribution crate, you are over-modeling: stop). No simulation loops. Never present a prediction without its validation residuals. Everything `#[ignore]`. Recall is out of scope. Contracts and Tier 1 semantics untouched. Commits imperative, 70-char wrap.

## Out of scope

Tier 3 latency injection (Phase 4). Re-running the vector-db-benchmark suite (zeppelin-bench owns that; when its next campaign runs, its measured numbers become GT-C — leave a `ground_truth/README` note saying exactly that). Auto-tuning/optimization search over the sweep axes (a table, not an optimizer).

## Acceptance criteria (all must pass before commit)

```bash
docker compose -f docker-compose.test.yml up -d

# 1. Predict end-to-end: calibration + GT-A/GT-B/shape-scaling asserts green,
#    whatif.md written with both tables and residuals
TEST_BACKEND=minio cargo test --test perf_contract_tests predict -- --ignored --nocapture

# 2. Determinism: two predict runs produce identical whatif.md (byte-compare)
TEST_BACKEND=minio cargo test --test perf_contract_tests predict -- --ignored --nocapture

# 3. Profile selection works
TEST_BACKEND=minio ZEPPELIN_PERF_PROFILE=s3-3node-wikidpr \
  cargo test --test perf_contract_tests predict -- --ignored --nocapture

# 4. Tier 1 untouched: contracts + selftest still green
TEST_BACKEND=minio cargo test --test perf_contract_tests contracts -- --ignored --nocapture
TEST_BACKEND=minio cargo test --test perf_contract_tests perf_selftest -- --ignored --nocapture

# 5. Default suite unaffected + lint
cargo test --lib && cargo test --tests
cargo clippy --tests -- -D warnings && cargo fmt --check
```

**Commit**: e.g. `Add perf-contract phase 3: analytic what-if model + $/query`, body stating the GT-A and GT-B residuals achieved and the headline wiki_dpr_e5 prediction row.
