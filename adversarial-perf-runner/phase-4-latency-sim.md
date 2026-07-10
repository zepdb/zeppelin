# Phase 4 — Tier 3: LatencyProfileStore + Model Cross-Validation

**Sequence**: Phase 4 of 4 (stretch tier — last deliberately). Prerequisites: Phases 1–3 merged (contract catalog green; Tier 2 `predict` validated against GT-A/GT-B).
**Required reading**: `adversarial-perf-runner/full_plan.md` (§8, §10–§11), `tests/adversarial/chaos.rs` (the decorator precedent — especially `FaultMode::Latency` at `:33` and `chaos_store` at `:161`), Phase 1–3 code as merged, `CLAUDE.md`.

## Context (self-contained recap)

Tier 1 counts S3 ops/bytes/depths deterministically (CI-gating). Tier 2 predicts QPS/latency/$ analytically from those counters plus storage profiles, validated at two ground-truth points. What neither can see: **timing interactions in the real code** — roundtrip chaining under real TTFB, prefetch overlap actually hiding latency (or not), `query_semaphore` queuing, connection-pool behavior. Tier 3 closes that gap on a laptop: a `LatencyProfileStore` decorator injects **deterministic, seeded, per-op** latency sampled from a profile's distributions, so real code + real MinIO experiences cloud-like timing. Measured end-to-end latency is then compared against Tier 2's prediction for the same profile — the deltas are the model's error bars, reported, **never CI-gating**. Wall-clock assertions are advisory by hard rule.

## Current-state code map (verify at HEAD)

- `tests/adversarial/chaos.rs` — the decorator shape to mirror (per-site matching, `Arc<dyn ObjectStore>` wrap via `ZeppelinStore::inner()`/`new()`, `src/storage/store.rs:438`/`:412`). ChaosStore's hard rule "faults fire before the inner call" exists for **failure-semantics soundness**; Tier 3 injects latency only (no failures), so the transfer-time component may legitimately sleep after the inner call (size known only then). Document this deviation and its reason in the module header.
- `tests/perf_contract/profiles/*.toml` — Phase 3 profiles; the `[storage]` block (`ttfb_ms { p50, p99 }`, `per_conn_MBps`) is Tier 3's input. No new schema.
- `tests/perf_contract/scenario.rs` — the runner; Tier 3 reuses scenarios verbatim, only the store chain and the measurement loop differ.
- `tests/perf_contract/predict.rs` — Phase 3 model; `latency_validate` calls into it to get the prediction for the same (scenario, profile) pair.
- `xxhash-rust` (xxh3) is a main dependency (`Cargo.toml`) — available to integration tests for per-op seed derivation.
- `scripts/perf-contract.sh` — Phase 2 driver with a reserved `--nightly` flag.

## Deliverables

### 1. `tests/perf_contract/latency.rs` — `LatencyProfileStore`

```rust
pub struct LatencyParams {              // derived from a profile's [storage] block
    pub ttfb: LognormalMs,              // fitted so its p50/p99 equal the profile's
    pub per_conn_mbps: f64,
}

pub fn latency_profile_store(store: &ZeppelinStore, params: LatencyParams, seed: u64)
    -> (ZeppelinStore, LatencyLedger);
```

- **Per-op delay** = `ttfb_sample + bytes / per_conn_mbps`. TTFB portion sleeps **before** the inner call; transfer portion sleeps after the inner call returns, before returning to the caller. Applies to GET/HEAD/PUT/LIST/COPY/DELETE (HEAD/LIST: ttfb only).
- **Deterministic sampling**: per-op RNG seeded from `xxh3_64(seed || key || per-key ordinal)` (per-key ordinal via a DashMap counter). Same run → identical delay sequence regardless of tokio interleaving. The lognormal is parameterized in closed form from (p50, p99): `μ = ln(p50)`, `σ = (ln(p99) − ln(p50)) / 2.326` — document the formula; no distribution crates (no new dependencies).
- `LatencyLedger`: total injected sleep, per-class op count, and the xxh3 seed — serialized into artifacts so determinism is checkable by diff (acceptance #3).
- Subtracting MinIO's real local latency is deliberately **not** attempted (sub-ms vs tens-of-ms injected; note the ≤ ~5% floor in the module header and in the report).

### 2. `latency_validate` entry (`tests/perf_contract_tests.rs`)

`#[tokio::test] #[ignore] async fn latency_validate()`:

1. Read `ZEPPELIN_PERF_LATENCY_PROFILE` (default `s3-standard-intra-region`) and `ZEPPELIN_PERF_SCENARIOS` (default: `warm_query_strong,cold_query_strong,fts_query` — the depth-diverse trio).
2. Per scenario: build the store chain `harness → LatencyProfileStore → depth_store → counting_store → start_test_server_full` (latency outermost so instrumented counters/depths are unchanged — Tier 1 semantics must hold identically under injection; assert the scenario's contract still passes as a sanity check).
3. **Serial pass** (clients = 1): ≥ 200 measured requests, record wall p50/p95/p99 and mean.
4. **Closed-loop pass**: `clients = profile.closed_loop_clients` concurrent request loops for a fixed request budget (≥ 500 total), record throughput and latency distribution. This is the one place in the whole runner where concurrent client traffic exists; depth assertions are **skipped** here (the §2.3 single-request precondition doesn't hold — say so in a comment).
5. Compare against Tier 2's prediction for the same (counters, profile): mean, p50, p99, throughput. Write `latency-validation.md` (table: predicted | measured | delta %) into artifacts and append to `report.md`.
6. **Advisory only**: the entry fails on setup/contract errors, never on latency deltas. Deltas > 25% mean are flagged `MODEL-GAP` in the report — that's a finding to investigate (e.g. prefetch overlap the analytic model credits wrongly), not a test failure.

### 3. Determinism of injection

Two runs with the same seed (fixed default `ZEPPELIN_PERF_LATENCY_SEED`, env-overridable) must produce byte-identical `LatencyLedger` artifacts (total injected sleep, per-class counts). Wall-clock measurements will differ (scheduler noise on top of injected floors) — only the ledger is asserted identical.

### 4. Nightly wiring

`scripts/perf-contract.sh --nightly`: run `contracts`, then `predict`, then `latency_validate` (default profile), copy the combined report to `tasks/perf-contract-report.md`. Exit code reflects `contracts` failures only (Tier 2 asserts count as failures; Tier 3 never does). Keep the 14-day artifact rotation; never `rm -rf`.

### 5. Report integration

`report.md` gains a "Tier 3 — Latency validation (advisory)" section: profile, seed, per-scenario predicted-vs-measured table, MODEL-GAP flags, and the injected-latency floor note. The three-tier story is now visible in one document: frozen counters (gate) → predictions (validated) → timing cross-check (advisory).

## Guardrails (binding; full_plan.md §11)

Zero production `src/` changes. No new dependencies (lognormal in closed form; no statrs). **Wall-clock is never CI-gating** — `latency_validate` must be impossible to wire as a gate (exit semantics above). Latency injection must not alter Tier 1 counters/depths (asserted in step 2). Everything `#[ignore]`. Explicit cleanup; no `rm -rf`. If closed-loop measurement proves too noisy on the laptop to be even advisory-useful, report that finding honestly and reduce scope to the serial pass — do not tune until it "looks right". Commits imperative, 70-char wrap.

## Out of scope

Failure injection (ChaosStore/adversarial territory). Bandwidth-contention modeling inside the decorator (a per-connection rate is injected; aggregate contention remains Tier 2's `agg_MBps` term — document). CPU-load simulation. Any CI gating of this tier. Real-S3 runs (when they happen, their measurements become new ground-truth fixtures for Tier 2 — leave that note in `ground_truth/README`).

## Acceptance criteria (all must pass before commit)

```bash
docker compose -f docker-compose.test.yml up -d

# 1. Latency validation end-to-end on the default profile (advisory report written)
TEST_BACKEND=minio cargo test --test perf_contract_tests latency_validate -- --ignored --nocapture

# 2. Tier 1 semantics unchanged under injection (asserted inside the entry; also:)
TEST_BACKEND=minio cargo test --test perf_contract_tests contracts -- --ignored --nocapture

# 3. Injection determinism: two runs, byte-identical LatencyLedger artifacts
TEST_BACKEND=minio ZEPPELIN_PERF_LATENCY_SEED=42 \
  cargo test --test perf_contract_tests latency_validate -- --ignored --nocapture
TEST_BACKEND=minio ZEPPELIN_PERF_LATENCY_SEED=42 \
  cargo test --test perf_contract_tests latency_validate -- --ignored --nocapture

# 4. Nightly driver end-to-end
TEST_BACKEND=minio ./scripts/perf-contract.sh --nightly
test -f tasks/perf-contract-report.md

# 5. Default suite unaffected + lint
cargo test --lib && cargo test --tests
cargo clippy --tests -- -D warnings && cargo fmt --check
```

**Commit**: e.g. `Add perf-contract phase 4: latency injection cross-validation`, body stating the predicted-vs-measured deltas observed for the default profile and any MODEL-GAP findings.
