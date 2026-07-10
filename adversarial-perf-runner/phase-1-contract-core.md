# Phase 1 — Contract Core: DepthStore, Dataset Generator, 3 Scenarios End-to-End

**Sequence**: Phase 1 of 4. No prerequisites.
**Required reading before writing any code**: `adversarial-perf-runner/full_plan.md` (§2 design decisions, §4 types, §5 contract format, §10 validation, §11 guardrails), `CLAUDE.md` (project rules), `tests/common/counting.rs`, `tests/common/server.rs`, `tests/common/harness.rs`, `tests/adversarial/runner.rs` (decorator-chain + inline-maintenance patterns only).

## Context (self-contained recap)

Zeppelin is an S3-native vector search engine; nodes are stateless and QPS = storage_bandwidth / bytes_per_query. This phase lands the **perf-contract runner** core: run the REAL query/write code against instrumented storage with synthetic datasets of known shape, and assert exact S3 operation counts, byte counts, and **sequential roundtrip depths** — never wall-clock. Contracts are checked-in TOML files; a regression fails CI; intentional frontier moves go through a human-approved re-baseline flow. This is the sibling of the correctness adversarial runner (`tests/adversarial/`), with parallel naming: `tests/perf_contract/`, `target/perf-contract/`, `ZEPPELIN_PERF_*`.

**Thin vertical slice**: this phase delivers the whole pipeline (dataset → instrumented server → measure → contract check → capture → artifacts → selftest) on exactly **3 scenarios**, not all instrumentation horizontally. The catalog lands in Phase 2.

## Current-state code map (verified at plan time; re-verify at HEAD)

- `tests/common/counting.rs` — `CountingStore`/`GetCounter` (`:359`/`:202`), per-`ArtifactClass` op+byte attribution; `classify()` (`:120`); `class_breakdown()` (`:299`); `counting_store(&ZeppelinStore)` (`:380`); `reset()` (`:347`). GET bytes = actual returned range (`:433`). **Reuse as-is; do not modify.**
- `tests/common/server.rs:482` — `start_test_server_full(store, namespace_name_prefix, config, spawn_compaction_loop) -> FullTestServer` (`:468`; exposes `store`, `compactor`, `manifest_cache`, `cache`, `base_url`). `cleanup_ns` at `:463`. **Reuse; do not modify.**
- `tests/common/harness.rs:10` — `TestHarness::new()` (random prefix, real S3/MinIO via `TEST_BACKEND`), `cleanup()` (`:107`).
- `src/storage/store.rs:438` — `ZeppelinStore::inner() -> Arc<dyn ObjectStore>`; `:412` `new(Arc<dyn ObjectStore>)`. Decorators chain here (pattern: `tests/adversarial/runner.rs:268` chains ChaosStore → CountingStore).
- `src/wal/writer.rs:337` — `WalWriter::append` doc contract: upload one immutable `.wal` fragment, then group-commit manifest CAS. History snapshot per publication: `src/wal/manifest.rs:683` (`history_key` → `{ns}/manifests/{gen:020}.msgpack`), `:1658` (`write_history_snapshot_for_commit`).
- `src/index/ivf_flat/search.rs:1917`–`1957` — cluster prefetch via `futures::future::join_all` (one parallel stage); `:2123`–`2156` SQ coarse phase. `src/index/ivf_flat/kmeans.rs:13`–`21` — k-means seed derived from exact input (deterministic).
- `src/config.rs:1221` — `cache.manifest_cache_ttl_ms` (default 500; wall-clock ⇒ contracts pin it to 0 or 3_600_000). `src/config.rs:253` — `query.rerank_coalesce_gap_bytes` (later sweep axis; not used this phase).
- MinIO: `docker-compose.test.yml`; tests hit real object storage — decorators over real storage are fine, fake stores are not.

## Deliverables

### 1. `tests/perf_contract_tests.rs` (crate root)

```rust
mod common;
mod perf_contract;

#[tokio::test] #[ignore] async fn contracts() { ... }      // run + assert all (or env-selected) scenarios
#[tokio::test] #[ignore] async fn capture() { ... }        // same run; write proposed/*.toml, assert nothing
#[tokio::test] #[ignore] async fn perf_selftest() { ... }  // injection matrix below
#[tokio::test] #[ignore] async fn depth_stability() { ... }// 100-repeat study (see §Deliverable 8)
```

### 2. `tests/perf_contract/mod.rs` — `PerfEnv`

Parse `ZEPPELIN_PERF_SCENARIOS` (default all), `ZEPPELIN_PERF_ARTIFACTS` (default `target/perf-contract`), `ZEPPELIN_PERF_CAPTURE`, `ZEPPELIN_PERF_SELFTEST`, `ZEPPELIN_PERF_REPEATS` (default 8). Unknown scenario names → panic (fail loud, CLAUDE.md rule 1).

### 3. `tests/perf_contract/depth.rs` — `DepthStore` (the new instrumentation)

Types exactly per full_plan.md §4.1: `SpanKind`, `OpSpan { kind, class, key, start_seq, end_seq, bytes, ok, wall_start_us, wall_end_us }`, `DepthTracker { reset, take_spans, critical_path }`, `depth_store(&ZeppelinStore) -> (ZeppelinStore, DepthTracker)`.

- One global `AtomicU64` event counter per tracker. `start_seq` = load at call entry; `end_seq` = `fetch_add(1) + 1` at completion. Record a span for **every** `ObjectStore` method (get_opts, head, put_opts, delete, copy, copy_if_not_exists, list, list_with_delimiter; list streams record one span closed when the stream is created — good enough for Phase 1, note it in a comment).
- `class` via `common::counting::classify` (reuse — do not duplicate the taxonomy).
- `critical_path(spans, kinds, cutoff_us)`: filter to `kinds` and (if `cutoff_us` given) spans with `wall_start_us <= cutoff_us`; chain rule `A → B iff end_seq(A) <= start_seq(B)`; `depth(B) = 1 + max depth(A)`; return `CriticalPath { depth: u32, chain: Vec<OpSpan> }` where `chain` is one representative span per level of a maximal chain (for diagnosis: "depth 3 via manifest.json → coarse_sketch.bin → cluster_4.bin"). Implementation: sort by `end_seq`, prefix-max over (end_seq, depth) — O(n log n). Unit-test the algorithm with hand-built spans: pure-parallel batch → depth 1; two-stage → 2; overlapping intervals never chain.
- **Soundness precondition** (doc-comment, enforced by the runner): exactly one client request in flight during measurement, no background loops. Depth is only meaningful under that precondition.

### 4. `tests/perf_contract/dataset.rs` — deterministic closed-form generator

Types per full_plan.md §4.2: `DatasetSpec { vectors, dims, nlist, seed, attrs: AttrShape, fts: FtsShape }` (Phase 1: only `AttrShape::None` / `FtsShape::None` implemented; enums exist now so Phase 2 only adds variants), `GeneratedDataset`, `DatasetExpectations { rows_per_cluster, cluster_f32_bytes, probe_clusters }`.

- `vectors % nlist == 0` enforced. K well-separated blob centers (e.g. center_j = 100.0 × one-hot-ish direction j), exactly N/K points per blob, per-point jitter uniform in ±0.5 (≪ center separation), `StdRng::seed_from_u64(seed)`, generation order fixed, **no HashMap anywhere** (BTreeMap only — canonicalization learning).
- `PlannedQuery`: query vectors at blob centers (probe set = nprobe nearest centers, computed in closed form from the center geometry) plus one midpoint query. Serialize expectations to `expected.json`.
- Determinism unit test: two generations from the same spec are byte-identical (serialize both, compare).

### 5. `tests/perf_contract/scenario.rs` — runner + cache-state control

`ScenarioSpec`, `CacheState { Cold, Warm { prime }, WarmHydrated }` (WarmHydrated variant exists, unimplemented panic until Phase 2), `MeasureOp`, `ScenarioOutcome` per full_plan.md §4.3. Lifecycle exactly per §4.3:

1. Generate dataset; compute expectations.
2. `TestHarness::new()` → `depth_store` → `counting_store` → `start_test_server_full(store, Some(prefix), config, /*spawn_compaction_loop=*/false)`.
3. Setup (never measured): create namespace via HTTP (`{harness.prefix}-{scenario}` naming, `api_ns` convention — no `/` in URL segments); upsert dataset in fixed-size batches (size = part of the spec, default 256); inline `server.compactor.compact(&ns).await` until `/compact/status` shows `ready && uncompacted_fragments == 0`; **read the manifest in-process and assert exactly `nlist` clusters with `rows_per_cluster` rows each — panic with a clear message if k-means did not recover the blobs** (generator bug, must be loud).
4. Cache state: `Cold` → boot a **second** `FullTestServer` over the same instrumented store (fresh `DiskCache` tempdir/caches; the setup server must never have executed a query — assert zero query ops issued during setup) and measure the first request there. `Warm` → run prime steps (default: the measured op once) on the same server.
5. `counter.reset(); tracker.reset();` then for `r in 0..repeats`: execute the measure op serially via reqwest; record the wall time at response completion; snapshot `class_breakdown()` + `take_spans()` + `critical_path` (GET-only and PUT+GET variants); reset between repeats. Cold scenarios force `repeats = 1`.
6. Assert all repeats identical (counters and depth; not wall fields) → `RepeatDrift` violation otherwise. Check contract. Write artifacts. `cleanup_ns` for every namespace + `harness.cleanup()` — never rely on `TestHarness::Drop` in loops.

Config construction: `Config::load(None)` + struct-field mutation only (no env vars — process-global). Pin `manifest_cache_ttl_ms` from the contract (`0` or `3_600_000` — reject other values at parse time). `max_wal_fragments_before_compact` high enough that write-scenario repeats never trigger compaction (spec field; assert post-run that fragment count < threshold).

### 6. `tests/perf_contract/contract.rs` — TOML spec, checker, capture

`ContractSpec` serde types mirroring full_plan.md §5 exactly: `schema_version`, `scenario`, `[dataset]`, `[ns_config]`, `[server_config]`, `[run]`, `[baseline] { git_rev, captured, approved_by, reason }`, `[assert.depth] <name> = { mode = "exact"|"max", value, why? }` (names: `get` = GET/HEAD only; `put_get` = GET+PUT), `[assert.gets]`/`[assert.puts]` (per-class exact counts; `total` allowed), `[assert.get_bytes]`/`[assert.put_bytes]` (per class: `{ exact }` or `{ min?, max, why }` — a band **requires** `why`), `[assert.put_keys]`/`[assert.get_keys]` (substring → exact count, via `gets_matching`/`puts_matching`).

- Loader: read from `tests/perf_contract/contracts/` via `env!("CARGO_MANIFEST_DIR")`; `toml = "0.8"` is already a main dependency (available to integration tests); unknown TOML keys → hard error (`serde(deny_unknown_fields)`).
- Checker: produce `Vec<CostViolation>` per full_plan.md §4.4; empty `approved_by` in a checked-in contract → violation (governance gate).
- Capture mode: run scenario, regenerate a complete contract (every class listed, measured values, `git_rev` from `git rev-parse HEAD`, `approved_by = ""`) into `target/perf-contract/<run-id>/proposed/<scenario>.toml`. **Never write into `tests/perf_contract/contracts/`.**

### 7. Three checked-in contracts + scenario builders (`tests/perf_contract/scenarios.rs`)

Implement `warm_query_strong`, `cold_query_strong`, `upsert_single` per full_plan.md §5.1–5.3 (dataset shapes and knobs as written there). Workflow for the checked-in values: implement → run `capture` → copy the proposed files into `tests/perf_contract/contracts/`, fill `approved_by = "anup"` and `reason = "initial freeze"` **and flag this in the commit message** (initial baselines are established by this phase; Anup's review of the commit is the approval). Replace the plan's illustrative counts with the real captured values; keep the `why` comments explaining each number (e.g. whether cluster GETs = nprobe or nprobe+1 due to speculative prefetch — discover empirically, document what you find).

### 8. `depth_stability` entry + study

For each of the 3 scenarios: 100 measured repeats, print the distribution of `critical_depth`. If constant → contract keeps `mode = "exact"`. If not: apply the post-response cutoff filter and re-check; if then constant → keep exact with the filter noted; if still varying → set `mode = "max"` with the observed max and a comment naming the source (e.g. speculative prefetch task start jitter). Write findings to `target/perf-contract/<run-id>/depth-stability.md` and summarize in the commit message. **If depth is unstable even as a max bound, stop and surface it — do not widen.**

### 9. `perf_selftest` — the injection matrix (mechanical proof the checker catches regressions)

Test-side decorators (in `perf_contract/` — chained *outside* DepthStore/CountingStore so injected ops are counted like real ones):

| Key (`ZEPPELIN_PERF_SELFTEST`) | Injection | Must fire |
|---|---|---|
| `extra-manifest-get` | decorator issues a duplicate inner GET whenever a `manifest.json` GET passes through | `OpCount` on class `manifest` (warm_query_strong) |
| `serialize-cluster-gets` | decorator holds a `tokio::sync::Mutex` across `get_opts` for `cluster_` keys (forces sequential completion) | `Depth` (warm_query_strong) |
| `extra-history-put` | decorator duplicates PUTs to keys containing `manifests/` | `KeyCount` on `manifests/` (upsert_single) |

The selftest entry runs all three keys plus one clean control (no injection — must pass), on MinIO. Assert the *specific* violation variant fires, and nothing fires in the control.

### 10. Artifacts + minimal report

Per full_plan.md §3: `run.json`, per-scenario `counters.json` / `spans.jsonl` / `depth.json` / `expected.json` / `violations.json`. Minimal `report.md`: run header + per-scenario row (status, depth + chain, per-class GET/PUT ops, total bytes) in the "Object-Store Totals" table style (`tests/adversarial/artifacts.rs:849`). Full report lands Phase 2.

## Guardrails (binding; repeat of full_plan.md §11)

Zero production `src/` changes. Zero modifications to `tests/common/*` and `tests/adversarial/*` (read-only reuse). No new dependencies. Everything `#[ignore]`. Never assert wall-clock. Never write into `tests/perf_contract/contracts/` from code. Artifacts only under `target/perf-contract/`. Explicit cleanup; no `rm -rf`. If a seam is missing, stop and report — don't add hooks. Fail loud — no fallbacks.

## Out of scope (later phases)

Remaining scenario catalog (filtered/FTS/as_of/paginate/compaction/GC/hydration — Phase 2); attrs/FTS dataset shapes (Phase 2); `scripts/perf-contract.sh` + full report + CI wiring (Phase 2); Tier 2 predict (Phase 3); Tier 3 latency injection (Phase 4).

## Acceptance criteria (all must pass before commit)

```bash
# 0. MinIO up
docker compose -f docker-compose.test.yml up -d

# 1. Contracts green (3 scenarios), zero violations
TEST_BACKEND=minio cargo test --test perf_contract_tests contracts -- --ignored --nocapture

# 2. Determinism double-run: run #1 and #2 produce byte-identical counters.json
#    and depth.json per scenario (compare with cmp/diff; wall fields live only
#    in spans.jsonl which is excluded)
TEST_BACKEND=minio cargo test --test perf_contract_tests contracts -- --ignored --nocapture

# 3. Capture writes complete proposed contracts (inspect one; every class listed)
TEST_BACKEND=minio ZEPPELIN_PERF_CAPTURE=1 \
  cargo test --test perf_contract_tests capture -- --ignored --nocapture

# 4. Selftest: 3 injections fire their exact violation; clean control passes
TEST_BACKEND=minio cargo test --test perf_contract_tests perf_selftest -- --ignored --nocapture

# 5. Depth stability study executed; findings file written
TEST_BACKEND=minio cargo test --test perf_contract_tests depth_stability -- --ignored --nocapture

# 6. Default suite unaffected + lint
cargo test --lib && cargo test --tests
cargo clippy --tests -- -D warnings && cargo fmt --check
```

**Commit** (after all green): message wrapped at 70 chars/line, e.g. `Add perf-contract runner phase 1: depth tracking + 3 contracts`, body noting the initial-baseline approval flag and depth-stability findings.
