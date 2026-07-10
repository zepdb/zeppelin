# Zeppelin Performance Contract Runner — Complete Plan

**Codename**: `perf-contract-runner`
**Sibling of**: the correctness adversarial runner (`tasks/adversarial-runner/full_plan.md`) — same house style, same guardrails, parallel naming (`tests/perf_contract/` ↔ `tests/adversarial/`, `target/perf-contract/` ↔ `target/adversarial/`, `ZEPPELIN_PERF_*` ↔ `ZEPPELIN_ADVERSARIAL_*`).
**Companion docs**: `adversarial-perf-runner/phase-{1..4}-*.md` (implementation tasks, executed strictly in order, one per codex run).

---

## 1. Context & Goal

Zeppelin's competitive story is performance-per-cost: object storage is the source of truth, nodes are stateless, and on a bandwidth-limited backend **QPS = storage_bandwidth / bytes_per_query**. This is empirically proven: MinIO-in-docker serves ~410 MB/s aggregate regardless of parallelism; the analytic prediction 410 / 70.6 MB = 5.81 QPS matched the measured 5.89 QPS within 1.4% (QPS loop, 2026-07-05; log at `tasks/opt-qps-log.md`, gitignored).

The adversarial runner guards **correctness** invariants (I1–I16 against a logical model). Nothing guards **cost** invariants:

- The architecture's core promise — *a strong query with warm centroids costs ≤ 2 sequential S3 roundtrip depths* — is asserted nowhere. Op **counts** are asserted in scattered tests; sequential **depth** (chain length, the thing that sets latency floor on real S3) is measured nowhere in the codebase.
- Hard-won optimization results (accepted line 2026-07-05: **5.64 GETs/query, 70.6 MB/query, 5.89 QPS** on dbpedia100k np16) live only in gitignored logs and memory files. They can silently rot with any refactor.
- Major perf work is queued: the RaBitQ quantization uplift (`tasks/July10Quant/00-README.md`) is expected to change bytes/query by ~10× (13–15 MB → 1.2–1.5 MB), and a benchmark campaign targets Qdrant (111.9 QPS on wiki_dpr_e5, 21M × 768d, 3 × 4vCPU/16GB nodes) and Elasticsearch DiskBBQ (32.4 QPS). We need to grade those experiments automatically and predict deployments we haven't run.

This project builds three tiers, in priority order (**scoping is settled — do not relitigate**; in particular, do NOT build a discrete-event simulator of CPU/tokio/cache dynamics — validation burden exceeds value, and real small-scale runs are cheap for that layer):

- **Tier 1 — Deterministic cost contracts (the core).** Run the REAL query/write/compaction code against instrumented storage with synthetic datasets of known shape; assert exact operation counts, byte counts, and sequential roundtrip depths — never wall-clock. Perfectly deterministic → CI-gating with zero flake budget. Contracts are versioned, checked-in TOML; re-baselining is an explicit human-approved flow, never automatic.
- **Tier 2 — Analytic what-if layer.** Multiply Tier 1's measured counters through parameterized storage/hardware/pricing profiles to predict QPS, p50/p99 latency, and $/query for deployments not yet run (real S3, 3 nodes, 21M vectors). Closed-loop client modeling (QPS = clients / mean_latency) mirrors vector-db-benchmark methodology. Validated against two ground-truth points (§7.4). Output: a report/table generator, not a service.
- **Tier 3 — Synthetic storage latency on real code (stretch, last).** Extend the ChaosStore pattern (`tests/adversarial/chaos.rs:30` already has `FaultMode::Latency`) into a `LatencyProfileStore` that injects deterministic, seeded, per-op-class latency, so the laptop runs real code + real MinIO but experiences cloud timing. Measured end-to-end latency validates Tier 2's predictions, including effects the analytic model can't see (roundtrip chaining, prefetch overlap, semaphore queuing). Wall-clock results here are advisory/reported, **never CI-gating**.

**Relationship to existing benches**: `~/Documents/code/zeppelin-devbench` (qpsbench frozen grader) and `~/Documents/code/zeppelin-bench` (vector-db-benchmark fork + working zeppelin adapter, July 4 glove-100 results) measure **wall-clock** on top of a running system. This runner sits **under** them: op/byte/depth counting and analytic prediction. It never duplicates their job, and its Tier 2 must reproduce their numbers to be trusted.

**Execution model**: GPT-5.5 codex implements one phase file at a time (`adversarial-perf-runner/phase-N-*.md`), each committed with tests green before the next begins. Claude validates each phase against §10.

---

## 2. Core design decisions (all verified against HEAD)

### 2.1 Count ops, bytes, and depths — never wall-clock (Tier 1)
Every Tier 1 assertion is an integer (op count, chain depth) or a byte count. Wall-clock appears only in reports (advisory) and in Tier 3. This is what makes the suite CI-gating with a zero flake budget — the same design choice the adversarial runner made with "never assert wall-clock GC boundaries".

### 2.2 Instrumentation is decorator-only; zero production `src/` changes
The plumbing already exists and is precedented:

- `tests/common/counting.rs:359` `CountingStore` / `:202` `GetCounter` — per-key and per-`ArtifactClass` GET/PUT op+byte attribution. The class taxonomy (`counting.rs:33`: Cluster/Attrs/Centroids/Bootstrap/Sq/Bitmap/Sketch/Fts/Wal/Manifest/Other) is pinned against the real key builders by `tests/counting_attribution_tests.rs`. GET bytes are the actual returned range (`counting.rs:433`), so ranged GETs (ZBP2 live-span layout) are measured correctly.
- `tests/adversarial/chaos.rs:161` `chaos_store` — the decorator-chaining pattern (`ZeppelinStore::inner()` at `src/storage/store.rs:438`, re-wrap via `ZeppelinStore::new` at `:412`). The adversarial runner chains ChaosStore → CountingStore (`tests/adversarial/runner.rs:268`).
- `tests/common/server.rs:482` `start_test_server_full(store, ns_prefix, config, spawn_compaction_loop)` boots a full HTTP server over a caller-supplied (instrumented) store and returns `FullTestServer` (`server.rs:468`) exposing `store`, `compactor`, `manifest_cache`, `cache` — everything needed to run maintenance inline and control cache state.
- `tests/common/harness.rs:10` `TestHarness` — real S3/MinIO backend, random prefix per run, explicit `cleanup()` (`:107`).

The perf runner adds exactly one new decorator (`DepthStore`, §2.3) and one latency decorator in Phase 4 — both test-side. **Zero production `src/` changes** is a hard guardrail, stronger than the adversarial runner's (which needed one additive `server.rs` function; that function now exists and we reuse it). Note the object_store S3 client retries internally (`src/storage/store.rs:235`–`242`, `max_retries: 2`) *below* any decorator — counters measure logical ops, not wire requests. On MinIO-local retries are ~never triggered; documented as a known (small) gap for real-S3 $ accounting (§7.2).

### 2.3 Roundtrip depth = interval-order critical path (the hard instrumentation problem)
"2 sequential S3 roundtrips per query" is a claim about **latency chaining**, not op count: how many storage roundtrips are serialized on the critical path. No task-local spans, no production hooks. Instead:

**`DepthStore`** (new, `tests/perf_contract/depth.rs`): an `ObjectStore` decorator with one global `AtomicU64` event counter. For every operation (GET/HEAD/PUT/LIST/COPY/DELETE) it records an `OpSpan { kind, class, key, start_seq, end_seq, bytes, wall_start_us, wall_end_us }` where `start_seq` is the counter value sampled at call entry and `end_seq` is `counter.fetch_add(1)+1` at completion. Post-hoc, over the spans of one measured request:

```
chains(B) : A → B  iff  end_seq(A) ≤ start_seq(B)      // A completed before B started
depth(B)  = 1 + max{ depth(A) : A → B }  (0-ary max = 0)
critical_depth = max over spans of depth
```

Computable in O(n log n) with a prefix-max over spans sorted by `end_seq`. This is exactly the wall-clock chaining semantics: if B starts after A completes, the wall clock paid two sequential roundtrips whether or not B logically needed A. Concurrent ops (overlapping intervals) never chain regardless of tokio interleaving — and the query path issues its parallel I/O via `futures::future::join_all` batches (`src/index/ivf_flat/search.rs:1917`–`1957` full-object prefetch; `:2123`–`2156` SQ coarse phase), which poll all inner futures before any completes, so a sequential-async-fn-of-parallel-stages yields a stable stage count.

**Soundness precondition** (enforced by the runner, asserted in artifacts): exactly one client request in flight during measurement, `spawn_compaction_loop: false`, no GC/hydration running. Under that precondition every observed happens-before edge belongs to the measured request's causal history.

**Residual nondeterminism, treated honestly**: fire-and-forget background work spawned *inside* a request (e.g. the Run-012 speculative cluster prefetch) can start before or after another op completes depending on scheduler timing, potentially extending a chain. Mitigations, in order:
1. The runner records the wall time at which the HTTP response completed; spans whose `wall_start_us` is after response completion are attributed to a separate `post_response` bucket and excluded from `critical_depth` (reported separately).
2. Phase 1 runs an empirical **depth-stability study**: ≥ 100 repeats per scenario; if `critical_depth` is not constant, the scenario's contract uses `depth_mode = "max"` (assert `critical_depth ≤ N`, still zero-flake if N is the observed max with the architecture's true bound) instead of `depth_mode = "exact"`. Every `max`-mode contract documents *why* in a comment.
3. If a scenario is unstable even as a bound, **stop and surface it** — do not paper over with tolerance.

The full chain (keys at each depth level) is written to artifacts and the report, so a depth regression is immediately diagnosable ("depth 3 via manifest.json → coarse_sketch.bin → cluster_4.bin").

### 2.4 Cache state is an explicit, named, reproducible precondition
"Cold" vs "warm" must be states you can construct, not adjectives. Definitions (each scenario declares one):

- **`cold`**: data written and compacted through a *setup* server; then a **fresh** `FullTestServer` boots over the same store (new `DiskCache` tempdir, new manifest cache, new in-memory caches) and the first measured request runs. Caveat, verified: the process-global `BOOTSTRAP_DECODED_CACHE` (`src/index/ivf_flat/build.rs`) never evicts and is keyed by segment — cold is only genuine because each run creates fresh namespaces/segments **and the setup server never executes a query**. The runner asserts this (setup issues zero query ops).
- **`warm`**: same server; a declared priming sequence runs first (default: the measured request itself, once), then counters and spans are reset (`GetCounter::reset()`, `counting.rs:347`; DepthStore equivalent) and measurement begins.
- **`warm_hydrated`**: warm plus an explicit hydration pass before priming.

Manifest cache TTL is wall-clock (`config.cache.manifest_cache_ttl_ms`, default 500 — `src/config.rs:1221`) and therefore nondeterministic mid-scenario. Contracts pin it per scenario to **0** (every request refetches — deterministic, used to *count* the manifest GET) or **3_600_000** (never expires within a scenario — deterministic, used for warm paths). Never the default.

Eviction is the other nondeterminism source (Redis-style sampled LRU in memory/disk caches). Contract datasets are sized so the working set fits every cache with ≥ 4× headroom — **nothing evicts during a contract scenario**. Eviction behavior is explicitly out of scope for Tier 1 (Tier 3 territory).

### 2.5 Determinism inventory (why exact counts are safe)
Verified sources of stability, cited so implementers don't re-derive them:

- k-means is deterministic: seed derived from the exact input; identical ordered inputs → identical centroids (`src/index/ivf_flat/kmeans.rs:13`–`21`, `:33`–`38`).
- Synthetic datasets are seeded `StdRng` and shaped so cluster membership is provable (§6) — cluster sizes, and hence blob sizes and probe sets, are known a priori.
- The runner executes ops **serially** (one in flight); group commit never batches two of our writes; compaction/GC run inline (`FullTestServer.compactor`, `zeppelin::compaction::gc::run_gc_cycle` — the adversarial runner's pattern, `tests/adversarial/runner.rs:1566`).
- ULIDs are fixed-length (26 chars), so WAL key shapes are stable; manifest JSON **byte size** varies slightly (generation digit count, timestamps) — byte assertions on Manifest/Wal classes use small explicit bands, everything else is exact (§5 tolerance policy).
- S3-client internal retries happen below the decorators (§2.2) and MinIO-local essentially never retries; a retry would not change our logical counts.

### 2.6 Contracts are versioned data, re-baselined only by humans
A contract is a checked-in TOML file (`tests/perf_contract/contracts/<scenario>.toml`, format in §5). The runner never writes into `contracts/`. The **capture** entry writes `target/perf-contract/<run-id>/proposed/<scenario>.toml` — a fully regenerated contract with fresh measured values, current `git_rev`, and empty `approved_by`. Re-baselining = a human diffs proposed vs checked-in, fills `approved_by` + `reason` (pointing at the change that moved the frontier, e.g. a July10Quant phase results file), and commits. CI fails on any contract mismatch — including improvements (an unexplained *drop* in bytes is also a contract breach: it means the frontier moved without a decision record). This is the governance answer to "quantization will change bytes by 10×": July10Quant phases 2–4 each end with a re-baseline commit (their README already treats measured gates as binding; this runner is where those numbers live from now on).

### 2.7 One scenario = one isolated world
Each scenario builds its own namespace(s) from its own dataset spec inside the shared harness prefix, runs, asserts, and cleans up (`cleanup_ns`, `tests/common/server.rs:463`). No scenario depends on another's state. Scenarios run sequentially in one test entry (shared MinIO, shared process — cheap), but each is independently runnable via `ZEPPELIN_PERF_SCENARIOS=<name>`.

---

## 3. Deliverables & file layout

```
tests/perf_contract_tests.rs          # crate root: #[ignore] entries only:
                                      #   contracts, capture, perf_selftest,
                                      #   predict (P3), latency_validate (P4)
tests/perf_contract/mod.rs            # module wiring + PerfEnv (env parsing)
tests/perf_contract/depth.rs          # DepthStore decorator + critical-path computation
tests/perf_contract/dataset.rs        # deterministic shape-parameterized dataset generator
tests/perf_contract/scenario.rs       # ScenarioSpec, cache-state control, runner loop
tests/perf_contract/scenarios.rs      # the scenario catalog (builders, one fn per scenario)
tests/perf_contract/contract.rs       # ContractSpec (TOML serde), checker, capture flow
tests/perf_contract/report.rs         # markdown report (mirrors adversarial conventions)
tests/perf_contract/predict.rs        # Tier 2 analytic model (Phase 3)
tests/perf_contract/latency.rs        # Tier 3 LatencyProfileStore (Phase 4)
tests/perf_contract/contracts/*.toml  # checked-in cost contracts (human-governed)
tests/perf_contract/profiles/*.toml   # Tier 2 storage/hardware/pricing profiles (Phase 3)
tests/perf_contract/ground_truth/*.toml  # recorded ground-truth fixtures (Phase 3)
scripts/perf-contract.sh              # driver (Phase 2)
tasks/perf-contract-report.md         # latest report copy (script output)
```

`tests/perf_contract/` is a support module (like `tests/common/`, `tests/adversarial/`), not a cargo test root — only `tests/perf_contract_tests.rs` is a crate root. TOML files are read at runtime via `env!("CARGO_MANIFEST_DIR")` paths; `toml = "0.8"` is already a main dependency (available to integration tests), **no new dependencies**.

### Env contract

| Var | Default | Meaning |
|---|---|---|
| `ZEPPELIN_PERF_SCENARIOS` | all | comma-separated scenario names to run |
| `ZEPPELIN_PERF_ARTIFACTS` | `target/perf-contract` | artifact root |
| `ZEPPELIN_PERF_CAPTURE` | — | `1`: write `proposed/*.toml` instead of asserting |
| `ZEPPELIN_PERF_SELFTEST` | — | injection key for `perf_selftest` |
| `ZEPPELIN_PERF_REPEATS` | 8 | measured repeats per scenario (each asserted identical) |
| `ZEPPELIN_PERF_PROFILE` | — | Phase 3: profile name(s) for `predict` |
| `ZEPPELIN_PERF_LATENCY_PROFILE` | — | Phase 4: profile for `latency_validate` |

Invocation: `TEST_BACKEND=minio cargo test --test perf_contract_tests <entry> -- --ignored --nocapture`. Everything `#[ignore]`d; default `cargo test` untouched.

### Artifacts

```
target/perf-contract/<run-id>/
  run.json                    # git rev, backend, env echo, scenario list
  report.md
  <scenario>/
    counters.json             # per-repeat per-class ClassStats + totals (GetCounter::class_breakdown)
    spans.jsonl               # every OpSpan of every measured repeat
    depth.json                # per-repeat critical_depth + the maximal chain (keys per level)
    expected.json             # closed-form dataset expectations (cluster sizes, probe sets)
    violations.json           # only on failure: expected vs actual, per assertion
  proposed/<scenario>.toml    # only in capture mode
```

---

## 4. Key types (implementers must not invent divergent shapes)

### 4.1 DepthStore (`tests/perf_contract/depth.rs`)

```rust
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub enum SpanKind { Get, Head, Put, List, Copy, Delete }

#[derive(Debug, Clone, Serialize)]
pub struct OpSpan {
    pub kind: SpanKind,
    pub class: ArtifactClass,      // reuse tests/common/counting.rs::classify
    pub key: String,
    pub start_seq: u64,            // global event counter at call entry
    pub end_seq: u64,              // global event counter after completion
    pub bytes: u64,                // returned range for GET, payload for PUT, 0 otherwise
    pub ok: bool,
    pub wall_start_us: u64,        // since tracker epoch (report-only, never asserted)
    pub wall_end_us: u64,
}

#[derive(Clone)]
pub struct DepthTracker { /* Arc<AtomicU64> counter + Arc<Mutex<Vec<OpSpan>>> */ }

impl DepthTracker {
    pub fn reset(&self);                       // clears spans (counter keeps ticking)
    pub fn take_spans(&self) -> Vec<OpSpan>;
    /// Longest chain under interval order (§2.3), restricted to `kinds`,
    /// excluding spans starting after `cutoff_us` (post-response filter).
    pub fn critical_path(spans: &[OpSpan], kinds: &[SpanKind], cutoff_us: Option<u64>)
        -> CriticalPath;   // { depth: u32, chain: Vec<OpSpan> /* one representative per level */ }
}

pub fn depth_store(store: &ZeppelinStore) -> (ZeppelinStore, DepthTracker);
```

Decorator chain per scenario (adversarial pattern, `runner.rs:268`): `harness.store → depth_store → counting_store → start_test_server_full`.

### 4.2 Dataset spec (`tests/perf_contract/dataset.rs`) — closed-form by construction

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSpec {
    pub vectors: usize,           // N — always a multiple of nlist
    pub dims: usize,              // d
    pub nlist: usize,             // K: num_centroids the namespace is created with
    pub seed: u64,
    pub attrs: AttrShape,         // None | Category { cardinality } (adds a filterable field)
    pub fts: FtsShape,            // None | Vocab { words, doc_len } (closed vocabulary)
}

pub struct GeneratedDataset {
    pub spec: DatasetSpec,
    pub vectors: Vec<GenVector>,          // upsert order fixed by seed
    pub blob_of: Vec<usize>,              // vector index -> blob (== expected cluster)
    pub queries: Vec<PlannedQuery>,       // query vector + a-priori probe set
    pub expected: DatasetExpectations,    // closed-form (serialized to expected.json)
}

pub struct DatasetExpectations {
    pub rows_per_cluster: usize,               // N / K exactly (balanced by construction)
    pub cluster_f32_bytes: u64,                // rows_per_cluster * d * 4
    pub probe_clusters: Vec<Vec<usize>>,       // per planned query: nearest blob ids in order
}
```

Construction rule: K blob centers with pairwise distance ≫ intra-blob spread (e.g. centers = scaled one-hot-ish directions, per-point jitter ≤ 1% of center separation), exactly N/K points per blob, deterministic `StdRng::seed_from_u64`. Property (asserted at setup, not assumed): after compaction, the segment has exactly K clusters and each cluster's row count == N/K — read the manifest in-process to verify, **fail the scenario setup loudly if k-means didn't recover the blobs** (then the dataset parameters are wrong; fix the generator, don't tolerate). Planned queries are placed at blob centers (probe set = the nprobe nearest centers, computable in closed form) and at midpoints (for multi-cluster probes). This is what makes `[assert.get_bytes] cluster.exact` computable a priori instead of merely reproducible.

### 4.3 Scenario (`tests/perf_contract/scenario.rs`)

```rust
pub struct ScenarioSpec {
    pub name: String,
    pub dataset: DatasetSpec,
    pub ns_config: NsConfig,        // quantization, nlist, fts/bitmap flags (raw JSON body)
    pub server_config: ServerKnobs, // nprobe, manifest_cache_ttl_ms ∈ {0, 3_600_000},
                                    // memory_cache_max_mb, coalesce gap, batch knobs
    pub cache_state: CacheState,    // Cold | Warm { prime: Vec<Step> } | WarmHydrated
    pub measure: MeasureOp,         // the ONE op measured (query/upsert/fetch/compact/gc/...)
    pub repeats: usize,             // default from env; every repeat asserted identical
}

pub struct ScenarioOutcome {
    pub per_repeat: Vec<RepeatCounters>,   // ClassStats map + totals + CriticalPath per kind
    pub expected: DatasetExpectations,
}
```

Runner lifecycle per scenario:

```
1. dataset = generate(spec.dataset)                       // closed-form expectations
2. TestHarness::new() → depth_store → counting_store →
   start_test_server_full(store, Some(prefix), config, spawn_compaction_loop: false)
3. Setup (never measured): create ns; upsert dataset in fixed batches; inline
   compactor.compact(&ns) until /compact/status shows ready && 0 uncompacted;
   verify manifest cluster counts == closed form (fail loud otherwise)
4. Cache state: Cold → boot a SECOND FullTestServer over the same instrumented
   store (fresh caches; setup server never queried); Warm → run prime steps
5. reset counters + tracker; for r in 0..repeats:
     execute measure op (serial; record response-complete wall time)
     snapshot per-repeat counters + spans; reset
   (Cold scenarios: repeats = 1 by definition; repeat-identity check skipped)
6. Assert repeats identical; check contract; write artifacts; cleanup_ns + harness.cleanup()
```

Mutating measure ops (upsert/delete) note: each repeat mutates state (fragment count grows). Contracts for writes therefore measure at a **pinned pre-state** — repeats stay below `max_wal_fragments_before_compact` so no repeat triggers compaction, and the contract documents that window.

### 4.4 Contract check results

```rust
pub enum CostViolation {
    OpCount   { class: String, kind: SpanKind, expected: u64, actual: u64 },
    KeyCount  { substring: String, kind: SpanKind, expected: u64, actual: u64 },
    Bytes     { class: String, bound: ByteBound, actual: u64 },
    Depth     { kinds: String, mode: DepthMode, limit: u32, actual: u32, chain: Vec<String> },
    RepeatDrift { repeat: usize, detail: String },     // repeats not identical
    BaselineDrift { field: String, detail: String },   // improvement without approval
}
```

---

## 5. The contract format (checked-in TOML) + worked examples

Format rules:
- **Exact by default.** Every op-count assertion is exact. Byte assertions are exact where the artifact size is closed-form or content-stable, banded (`min`/`max`) only for serialization-variable classes (Manifest JSON, Wal msgpack — §2.5) — and every band carries a `why` string. An assertion absent from the contract is not checked; the capture flow always emits the full set so omissions are visible in diffs.
- Per-class counts use the `ArtifactClass` taxonomy verbatim (`counting.rs:33`). Key-substring assertions (`GetCounter::gets_matching`/`puts_matching`, `counting.rs:213`/`:222`) cover keys the taxonomy folds into `other` (e.g. history snapshots `{ns}/manifests/{gen:020}.msgpack`, `src/wal/manifest.rs:683`).
- `[baseline]` is provenance, not data: `git_rev`, `captured`, `approved_by`, `reason`. CI refuses contracts with empty `approved_by`.

### 5.1 `warm_query_strong.toml` — the flagship roundtrip-depth contract

```toml
schema_version = 1
scenario = "warm_query_strong"

[dataset]
vectors = 4096
dims = 64
nlist = 8
seed = 7
attrs = "none"
fts = "none"

[ns_config]
quantization = "sq8"            # today's default; re-baselined by July10Quant phase 4

[server_config]
nprobe = 4
manifest_cache_ttl_ms = 3600000  # pinned-warm: no mid-measure manifest refetch
memory_cache_max_mb = 256

[run]
cache_state = "warm"             # prime = the measured query, once
measure = { op = "query", consistency = "strong", top_k = 10, query_index = 0 }
repeats = 8

[baseline]
git_rev = "<filled by capture>"
captured = "<date>"
approved_by = "anup"
reason = "freeze ZBP2 accepted line (tasks/opt-qps-log.md 2026-07-05)"

[assert.depth]
get = { mode = "exact", value = 2 }   # THE architecture promise: sketch selection is
                                      # resident; probe GETs are one parallel stage;
                                      # warm manifest+bootstrap cost zero roundtrips.
                                      # Phase 1's stability study may downgrade to
                                      # mode="max" with documented cause (§2.3).

[assert.gets]                         # ops per query, exact
manifest = 0
centroids = 0
bootstrap = 0
sketch = 0
cluster = 5                           # nprobe + speculative prefetch (+1) — illustrative;
wal = 0                               # capture fills real values
attrs = 0
sq = 0
fts = 0
bitmap = 0
other = 0

[assert.get_bytes]
cluster = { exact = 1310720, why = "closed-form: probed_objects x live-span bytes; capture pins" }
total   = { max = 1400000 }

[assert.puts]
total = 0                             # queries never write
```

### 5.2 `upsert_single.toml` — the write-path anatomy contract

```toml
schema_version = 1
scenario = "upsert_single"

[dataset]
vectors = 512
dims = 64
nlist = 4
seed = 11
attrs = "none"
fts = "none"

[server_config]
manifest_cache_ttl_ms = 0
max_wal_fragments_before_compact = 64   # repeats never trigger compaction (§4.3 note)

[run]
cache_state = "warm"
measure = { op = "upsert", batch = 1 }
repeats = 8

[assert.puts]                 # upsert = exactly 1 WAL PUT + 1 manifest CAS + 1 history PUT
wal = 1
manifest = 1                  # the CAS publication (src/wal/writer.rs:337 append contract)
[assert.put_keys]
"manifests/" = 1              # history snapshot (src/wal/manifest.rs:683,1658) — class `other`
[assert.gets]
manifest = 1                  # fresh read before CAS (TTL 0)
[assert.depth]
put_get = { mode = "exact", value = 4, why = "PUT wal -> GET manifest -> CAS PUT -> history PUT" }
```

(The depth value is the *measured claim to freeze*, not doctrine — capture establishes it; if the real chain is 3 because history piggybacks, the captured contract says 3 and the report explains the chain.)

### 5.3 `cold_query_strong.toml` — the bootstrap-cost contract

```toml
schema_version = 1
scenario = "cold_query_strong"

[dataset]
vectors = 4096
dims = 64
nlist = 8
seed = 7
attrs = "none"
fts = "none"

[ns_config]
quantization = "sq8"

[server_config]
nprobe = 4
manifest_cache_ttl_ms = 0

[run]
cache_state = "cold"          # fresh server over existing data; first query ever (§2.4)
measure = { op = "query", consistency = "strong", top_k = 10, query_index = 0 }
repeats = 1                   # cold is single-shot by definition

[assert.gets]
manifest = 1                  # pinned centroids+sketch arrive via bootstrap blob
bootstrap = 1
cluster = 5                   # illustrative — capture pins actual (sq calibration etc.)
sq = 1
[assert.get_bytes]
bootstrap = { exact = 526336, why = "closed-form from nlist x dims + sketch codes" }
manifest  = { min = 800, max = 4096, why = "JSON size varies with gen digits/timestamps" }
[assert.depth]
get = { mode = "exact", value = 4, why = "manifest -> bootstrap(+sq calib) -> sketch-select -> clusters" }
```

### 5.4 Scenario catalog (full set; Phase 1 implements the three above, Phase 2 the rest)

| Scenario | Measures | Key contracts |
|---|---|---|
| `warm_query_strong` | strong query, warm | depth ≤ 2, GETs/class, bytes |
| `warm_query_eventual` | eventual query, warm | + wal-fragment GET count (tombstone rule) |
| `cold_query_strong` | first query on fresh node | full bootstrap anatomy |
| `filtered_query` / `filtered_query_bitmap` | filter path ± bitmap index | attrs/bitmap GET counts |
| `fts_query` / `hybrid_query` | BM25 / fusion | global-fts single-GET promise (Run-009 #6) |
| `as_of_query` | PITR to retained gen | history GET count |
| `paginate` | 2-page cursor walk | page-2 costs ≤ page-1 (no re-scan) |
| `fetch_strong` | point fetch by id | minimal-GET promise |
| `upsert_single` / `upsert_batch` | write anatomy | 1 WAL PUT + CAS + history; batch ⇒ still 1 fragment |
| `delete_single` | tombstone write | same anatomy as upsert |
| `compaction_cycle` | inline compact of F fragments | GET/PUT per class; incremental vs retrain variants |
| `gc_cycle` | one GC pass, nothing eligible | list/delete op bounds |
| `hydration` | hydrate a compacted ns | GET ops == artifact count, no re-GETs |

Each Phase 2 scenario file documents its determinism notes (which knobs pinned, why) inline in the TOML as comments.

---

## 6. Synthetic dataset generator — determinism & closed form

Covered structurally in §4.2; binding requirements:

1. Same `DatasetSpec` → byte-identical vector stream, forever (seeded `StdRng`, no HashMap iteration anywhere in generation — `BTreeMap` only, per the canonicalization learning).
2. Blob separation is validated, not assumed: setup reads the manifest in-process after compaction and asserts K clusters × (N/K) rows. Violation = generator bug = loud failure.
3. `expected.json` carries every closed-form number the contract's `why` strings reference, so a reviewer can recompute `cluster.exact` bytes from the artifact alone.
4. Attribute and FTS shapes reuse the adversarial vocabulary discipline (`tests/adversarial/vocab.rs`): closed word list, stem-stable, so FTS scenarios have deterministic index sizes.
5. Two standard shapes ship in Phase 2 (`shape_small` = 4096×64×8, `shape_medium` = 32768×128×32) — the pair feeds Tier 2's shape-scaling validation (§7.4).

---

## 7. Tier 2 — analytic what-if layer (`predict.rs`, Phase 3)

### 7.1 Inputs
Per scenario: Tier 1's `counters.json` (per-class ops/bytes per query) **and** `depth.json`'s stage structure — the interval DAG already yields stages (all spans at depth *i* = stage *i*, with per-stage op counts and bytes). No new instrumentation; Tier 2 is pure arithmetic over Tier 1 artifacts plus a profile.

### 7.2 Profile schema (`tests/perf_contract/profiles/*.toml`)

```toml
name = "s3-standard-intra-region"
[storage]
ttfb_ms = { p50 = 15.0, p99 = 60.0 }          # per-request first-byte latency
per_conn_MBps = 80.0                            # single-stream transfer rate
agg_MBps_per_node = 100000.0                    # effectively unbounded for S3; 410 for minio-local
[storage.price]
get_per_req = 0.0000004                         # $0.40 / 1M GET
put_per_req = 0.000005                          # $5.00 / 1M PUT
egress_per_gb = 0.0                             # intra-region reads free
[node]
count = 1; vcpus = 4; mem_gb = 16; price_hr = 0.192
cpu_ms_per_query = 8.0                          # calibrated (§7.4), not guessed
[client]
closed_loop_clients = 8                         # vector-db-benchmark "parallel"
```

Shipped profiles: `minio-local-docker` (agg 410 MB/s — the proven wall; sub-ms TTFB), `s3-standard-intra-region`, `s3-3node-wikidpr` (the Qdrant-comparison deployment: 3 × 4vCPU/16GB, 21M × 768d shape parameters). Retry amplification on real S3 is noted as an un-modeled ≤ few-% effect on $ (§2.2).

### 7.3 Model (documented equations, no simulation)

```
stage_latency_i = ttfb.p50 + stage_bytes_i / min(per_conn_MBps × stage_ops_i, agg_share)
service_time    = cpu_ms_per_query + Σ_i stage_latency_i
QPS_bw_cap      = agg_MBps_per_node × nodes / bytes_per_query
QPS_closed      = closed_loop_clients / mean_latency       (vector-db-benchmark identity)
QPS_pred        = min(QPS_closed(service_time), QPS_bw_cap)
mean_latency    = clients / QPS_pred                       (inflates when bandwidth-bound)
p50 ≈ service_time;  p99 ≈ cpu + Σ_i (ttfb.p99 + transfer_i)   (coarse; labeled ±50%)
$/query         = Σ_class (get_ops × get_price + put_ops × put_price)
                  + get_bytes × egress_per_gb / 2^30
                  + nodes × price_hr / (QPS_pred × 3600)
```

Bytes/ops at target scale come from the **shape model**: per-class counters are expressed as functions of (N, d, K, nprobe, quantization row-bytes) fitted from the two standard shapes, then evaluated at the target (21M × 768 × K). Recall is **not** predicted — Tier 2 predicts cost/latency *at a configured recall point*; recall parity is the benchmark campaign's job (benchmark-north-star).

### 7.4 Calibration & validation protocol (binding acceptance for Phase 3)
Ground truths are checked-in fixtures (`ground_truth/*.toml`) with provenance comments:

- **GT-A (accepted line, 2026-07-05)**: dbpedia100k, np16, 8 workers, MinIO-local: 70.6 MB/q, 5.64 GETs/q, measured **5.89 QPS**. Source: `tasks/opt-qps-log.md` + memory `qps-loop-2026-07`. Calibrates `minio-local-docker` (agg = 410 fixed from the boto3 probe; fit `cpu_ms_per_query`); validation: **|QPS_pred − 5.89| / 5.89 ≤ 0.10**.
- **GT-B (glove-100, 2026-07-04)**: `~/Documents/code/zeppelin-bench/vector-db-benchmark/results/zeppelin-default-glove-100-angular-search-0-2026-07-04-07-54-55.json`: parallel 8, nprobe 8 → rps 83.94, mean 94.2 ms, p95 161.7 ms, p99 366.7 ms. Bytes/query for that run were not recorded → input counters come from the shape model (honest caveat: GT-B validates the closed-loop/latency machinery more than the bytes model, which GT-A anchors). Validation: **QPS within ±20%, mean latency within ±25%**; the closed-loop identity (8 / 0.0942 s = 84.9 ≈ 83.9) must hold in the model by construction.
- **Shape-scaling check**: fit the shape model on `shape_small`, predict `shape_medium`'s per-class counters, compare to `shape_medium`'s Tier 1 measurements: every class within ±10% or exact where closed-form. This gates any extrapolation to 21M.

Residuals are printed in the report; the what-if table (wiki_dpr_e5 21M, 3 nodes, S3, sweep over nprobe × quantization ∈ {sq8, rabitq-1bit, rabitq-2bit expected-bytes from July10Quant §payoff} × coalesce gap) is **report-only, clearly labeled PREDICTION**, with the Qdrant 111.9 QPS / ES DiskBBQ 32.4 QPS reference lines printed alongside.

---

## 8. Tier 3 — LatencyProfileStore (`latency.rs`, Phase 4)

Decorator in the ChaosStore mold (`tests/adversarial/chaos.rs:161`), latency-only, no failures:

```rust
pub struct LatencyProfile {           // parsed from the same profiles/*.toml [storage] block
    pub ttfb_ms: LognormalSpec,       // fitted so p50/p99 match the profile
    pub per_conn_MBps: f64,
}
pub fn latency_profile_store(store: &ZeppelinStore, profile: LatencyProfile, seed: u64)
    -> ZeppelinStore;
```

- Per-op sample: `delay = ttfb_sample + bytes / per_conn_MBps`. TTFB sleeps **before** the inner call; the transfer component sleeps after the inner call returns (size is only known then) and before returning to the caller. This deviates from ChaosStore's fire-before-only rule deliberately — that rule exists to keep failure semantics sound; there are no failures here. Document this in the module header.
- Deterministic sampling: rng seeded per-op from `xxh3(seed, key, per-key ordinal)` — same run → same delays, independent of tokio scheduling order.
- `latency_validate` entry: run a scenario under the profile, measure end-to-end wall latency (p50/p99 over ≥ 200 requests), compare against Tier 2's prediction for the same profile + counters, and report deltas. **Advisory only** — the entry never fails on latency numbers (it fails only on setup errors). This is where chaining, prefetch overlap, and `query_semaphore` queuing effects (visible in real code, invisible to §7.3's arithmetic) show up as Tier 2 model error, closing the loop.

---

## 9. Report, CI, and governance

**Report** (`report.md`, copied to `tasks/perf-contract-report.md` by the script): run header (git rev, backend, scenario count, pass/fail); per-scenario table (status | GET depth + chain | per-class GET/PUT ops | bytes | Δ vs baseline); the per-class op/byte table in the exact "Object-Store Totals" style the adversarial reports use (`tests/adversarial/artifacts.rs:849`); proposed re-baseline diffs when capture ran; Tier 2 prediction tables + calibration residuals (Phase 3); Tier 3 advisory latency deltas (Phase 4).

**CI**: the `contracts` entry is the **gating** job (MinIO service container, `docker-compose.test.yml` pattern), zero flake budget — a red run is a real cost regression or a missing re-baseline, both of which must block. `predict` runs report-only (artifact upload). `latency_validate` is nightly-only via `scripts/perf-contract.sh --nightly` (which mirrors `scripts/overnight-adversarial.sh` conventions: MinIO bootstrap, `cargo build --tests` before any clock, report copy, 14-day artifact rotation, no `rm -rf`).

**Re-baseline governance** (§2.6, restated as procedure): (1) intentional change lands (e.g. July10Quant phase); (2) `ZEPPELIN_PERF_CAPTURE=1` run; (3) human reviews `proposed/` diff, fills `approved_by` + `reason` (must reference the results file that justified the change); (4) commit contracts alongside or immediately after the change. The runner itself **never** writes to `tests/perf_contract/contracts/`.

---

## 10. Validation protocol (Claude, per phase gate)

1. Run the phase's acceptance commands **verbatim** (each phase file lists them), plus `cargo clippy --tests -- -D warnings && cargo fmt --check`.
2. Diff review vs guardrails: **zero `src/` changes, zero `tests/common/` and `tests/adversarial/` modifications** (read-only reuse), no dependency changes, all entries `#[ignore]`, artifacts only under `target/perf-contract/` (+ the single `tasks/perf-contract-report.md` copy).
3. **Hand-injected cost regression** (scratch branch, then discard): add one redundant `store.get` of `manifest.json` in the strong-query handler path — run `contracts` and prove `warm_query_strong` fails with `OpCount { class: "manifest", expected: 0, actual: 1 }` (and `cold_query_strong` with 1→2).
4. **Hand-injected depth regression** (scratch branch): serialize the cluster prefetch (replace the `join_all` at `src/index/ivf_flat/search.rs:1917` with a sequential loop) — prove the depth contract fails with the offending chain printed.
5. `perf_selftest` green: the in-harness injection matrix (extra-GET decorator, chain-serializing decorator, duplicate-history-PUT decorator) each trigger their expected `CostViolation`, and the clean control passes.
6. **Determinism double-run**: run `contracts` twice; `counters.json` and `depth.json` byte-identical across runs (wall_* fields excluded by construction — they live only in `spans.jsonl`).
7. Confirm default `cargo test --lib && cargo test --tests` runtime unchanged (± noise).
8. Phase 3 additionally: GT-A residual ≤ 10%, GT-B within its stated bands, shape-scaling check green — all asserted inside `predict`, not eyeballed.
9. Phase 4 additionally: two `latency_validate` runs with the same seed produce identical injected-delay sums (determinism of sampling); report shows Tier 2 vs Tier 3 deltas.

---

## 11. Guardrails & explicit non-goals (verbatim into every codex prompt)

- **Zero production `src/` changes.** All instrumentation is decorator-level over `ZeppelinStore::inner()`. If a seam is genuinely missing, stop and surface it — never add hooks unilaterally.
- **No new dependencies** (`toml`, `rand`, `serde_json`, `reqwest`, `tempfile`, `futures`, `dashmap`, `xxhash-rust` are all already available to integration tests).
- **Never assert wall-clock in Tier 1 or CI.** Wall-clock is report-only (Tier 1) or advisory (Tier 3).
- **Never auto-update contracts.** Capture writes to `target/…/proposed/` only.
- **No discrete-event simulator** of CPU/tokio/cache dynamics. No recall/precision prediction. No benchmark re-implementation (zeppelin-devbench/zeppelin-bench own wall-clock truth). No real-S3 CI runs.
- Tests hit real object storage (MinIO via `TEST_BACKEND=minio`); the counting/depth/latency **decorators over real storage are precedented and fine; a fake store is not**.
- Everything `#[ignore]`; nothing runs in default `cargo test` / `scripts/test.sh`.
- Explicit `cleanup_ns` + `harness.cleanup()`; never rely on `TestHarness::Drop` in loops. Never `rm -rf` in scripts.
- Commit style: imperative, wrapped at 70 chars/line.
- Where `tasks/July10Quant/` phases change artifact formats (sketch v4, 2-bit storage), contracts are **expected** to break — the phase's re-baseline commit is the fix, never a tolerance widening.

---

## 12. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Depth flaps under scheduler jitter (top risk) | Post-response span filter; Phase-1 100-repeat stability study; per-scenario `exact` vs `max` mode with documented cause; stop-and-surface if unstable as a bound (§2.3) |
| k-means doesn't recover blobs → closed form wrong | Setup asserts K×(N/K) cluster structure from the manifest; loud failure = fix generator (§6) |
| Serialization-size drift (manifest JSON digits) breaks byte exactness | Banded assertions with `why` strings on Manifest/Wal only; everything else exact (§5) |
| Baseline values rot as intentional work lands | Governance flow (§2.6/§9); July10Quant phases each end with a re-baseline commit; CI fails on unexplained improvements too |
| Tier 2 overfits to two ground truths | Shape-scaling check is a third, structural validation; residuals always printed; what-if tables labeled PREDICTION |
| GT-B lacks measured bytes/query | Documented: GT-B validates closed-loop/latency machinery; GT-A anchors bytes→QPS (§7.4) |
| Process-global BOOTSTRAP_DECODED_CACHE fakes cold state | Cold = fresh server + never-queried namespace; setup asserted query-free (§2.4) |
| Mutating measure ops drift state across repeats | Repeats stay under the compaction trigger; window documented in contract (§4.3) |
| S3-client internal retries invisible to counters | Below-decorator, documented ≤ few-% $ effect; MinIO-local never retries (§2.2) |

---

## 13. Phases

Executed strictly in order; each phase file self-contained for codex; commit per phase, acceptance green before commit.

| Phase | File | Content | Size |
|---|---|---|---|
| 1 | `phase-1-contract-core.md` | DepthStore + critical path, dataset generator (blobs), scenario runner with cache-state control, contract TOML load/check/capture, 3 scenarios end-to-end (`warm_query_strong`, `cold_query_strong`, `upsert_single`), `perf_selftest`, depth-stability study | ~1100 LOC |
| 2 | `phase-2-scenario-catalog.md` | Full scenario catalog (§5.4), attrs/FTS dataset shapes, two standard shapes, full report, `scripts/perf-contract.sh`, CI wiring docs, re-baseline flow polish | ~900 LOC |
| 3 | `phase-3-whatif-model.md` | Tier 2: profiles, model, GT-A/GT-B fixtures + calibration/validation, shape-scaling check, wiki_dpr_e5 3-node what-if + $/query tables | ~700 LOC |
| 4 | `phase-4-latency-sim.md` | Tier 3: LatencyProfileStore, deterministic sampling, `latency_validate`, Tier2-vs-Tier3 delta report, nightly wiring | ~500 LOC |
