# Phase 2 — Full Scenario Catalog, Report, Script, CI Wiring

**Sequence**: Phase 2 of 4. Prerequisite: Phase 1 merged (`tests/perf_contract/` core: DepthStore, dataset generator, scenario runner, contract checker, 3 green contracts, `perf_selftest`).
**Required reading**: `adversarial-perf-runner/full_plan.md` (§4–§6, §9, §11), the Phase-1 code as merged, `CLAUDE.md`, `scripts/overnight-adversarial.sh` (script conventions), `tests/adversarial/vocab.rs` (closed-vocabulary discipline), `tests/adversarial/artifacts.rs` report section (~`:849`).

## Context (self-contained recap)

The perf-contract runner freezes Zeppelin's cost frontier as deterministic, CI-gating contracts: exact S3 op counts, byte counts, and sequential roundtrip depths per scenario, measured by running the real code over decorator-instrumented storage (`DepthStore` → `CountingStore` → `start_test_server_full`, all test-side, zero `src/` changes). Phase 1 landed the vertical slice with 3 scenarios (`warm_query_strong`, `cold_query_strong`, `upsert_single`). This phase lands the **breadth**: the full scenario catalog covering every cost-bearing API surface, the dataset shapes those scenarios need, the full report, the driver script, and CI wiring. After this phase, any change that adds a GET to any major path fails CI.

## Current-state code map (verify at HEAD; Phase-1 files now exist)

- `tests/perf_contract/{mod,depth,dataset,scenario,scenarios,contract,report}.rs` — Phase-1 core. Extend; keep public shapes from full_plan.md §4 stable.
- `tests/common/counting.rs:33` — `ArtifactClass` taxonomy (Cluster/Attrs/Centroids/Bootstrap/Sq/Bitmap/Sketch/Fts/Wal/Manifest/Other); key-substring assertions cover `other`-class keys (history `manifests/`, `_gc/`, `lease.json`, `meta.json`).
- Maintenance seams (adversarial-runner precedent): inline compaction via `FullTestServer.compactor.compact(&ns)`; GC via `zeppelin::compaction::gc::run_gc_cycle(&store, ns, &GcConfig)` (usage pattern at `tests/adversarial/runner.rs:1566` with a zero-horizon `GcConfig`); hydration via the HTTP hydrate endpoint (see `tests/hydration_api_tests.rs`).
- Query surfaces to cover (route shapes: see `tests/` files named per feature — `facet_query_tests.rs`, `cursor_query_tests.rs`, `pitr_query_tests.rs`, `hybrid_query_fusion_tests.rs`, `fts_e2e_tests.rs`, `bitmap_tests.rs`, `eventual_tombstone_tests.rs` are the authoritative request-shape references).
- Eventual-consistency cost rule (why `warm_query_eventual` differs): eventual queries fetch only WAL fragments with `delete_count > 0` (`src/wal/reader.rs:135`) — the contract counts exactly those Wal GETs.
- Global FTS promise: 1 S3 GET for the global inverted index instead of N cluster scans (`src/fts/global_index.rs`, Run-009 #6) — `fts_query` freezes this.

## Deliverables

### 1. Dataset generator extensions (`dataset.rs`)

- `AttrShape::Category { cardinality }`: every vector gets `{"cat": "c<i mod cardinality>"}` — deterministic, uniform, so filter selectivity is closed-form (`N / cardinality` matches per value).
- `FtsShape::Vocab { words, doc_len }`: FTS field text drawn from a closed, stem-stable word list (copy the *discipline* of `tests/adversarial/vocab.rs`: fixed array, self-test asserting stem-stability with zeppelin's own tokenizer; do not import the adversarial module — perf_contract stays independent).
- Two standard shapes as named constants: `shape_small` = 4096×64, nlist 8; `shape_medium` = 32768×128, nlist 32. Both must satisfy the blob-recovery assertion. These two feed Phase 3's shape-scaling validation — keep their specs stable once committed.

### 2. Scenario catalog (`scenarios.rs`) + checked-in contracts

Implement per full_plan.md §5.4, one contract TOML each (captured values, `approved_by` filled via the Phase-1 initial-freeze flow, `why` comments on every band and every depth):

| Scenario | Measure | Contract highlights (capture pins numbers) |
|---|---|---|
| `warm_query_eventual` | eventual query after k deletes | Wal GETs == fragments with tombstones; no other class changes vs strong |
| `filtered_query` | strong query + attr filter (no bitmap) | attrs GET count; cluster GETs unchanged |
| `filtered_query_bitmap` | same ns with bitmap index | bitmap GETs; attrs GETs reduced/zero |
| `fts_query` | rank_by BM25 | **fts = 1** (global-index promise); cluster = 0 |
| `hybrid_query` | vector + BM25 fusion | union of both anatomies, no duplicates |
| `as_of_query` | strong query at retained generation | history GET count (`manifests/` substring), depth |
| `paginate` | 2-page cursor walk (page_size < matches) | page-2 GETs ≤ page-1 GETs (assert per page: measure pages as separate repeats) |
| `fetch_strong` | GET /vectors fetch by id | minimal anatomy (manifest? wal? — capture reveals; freeze it) |
| `upsert_batch` | one upsert of 64 vectors | still exactly 1 Wal PUT + 1 CAS + 1 history PUT |
| `delete_single` | delete 1 id | same write anatomy as upsert |
| `compaction_cycle` | inline compact of exactly F=4 fragments | GETs: wal = F (+manifest); PUTs per class = artifact census; use `read_fragments_from_refs_unchecked` path implicitly (real code) |
| `compaction_incremental` | second compact after +F fragments | centroids reused ⇒ fewer PUTs than full retrain (freeze the delta) |
| `gc_cycle` | one GC pass with nothing eligible | list ops bounded; delete ops = 0 |
| `hydration` | hydrate compacted ns into empty cache | GET ops == reachable artifact count; zero repeat GETs on 2nd hydrate |

Notes binding the implementation:
- Every scenario declares `cache_state` and pins `manifest_cache_ttl_ms` ∈ {0, 3_600_000} — never default.
- `paginate`'s two pages are recorded as two labeled measures inside one repeat (extend `MeasureOp` minimally: `QueryPages { pages: 2, page_size }` recording per-page counters). Keep everything else single-op.
- `compaction_cycle`/`gc_cycle`/`hydration` measure the maintenance call itself (in-process or HTTP), not a query; repeats = 1 with a fresh namespace per repeat if re-run requires fresh state (namespaces are cheap; determinism over convenience).
- Mutating scenarios stay under the compaction trigger across repeats (Phase-1 rule); assert post-run.
- Where a measured anatomy surprises you (e.g. an unexpected `other` GET), **investigate and document in the contract comment** — do not just freeze mystery ops. If it looks like a bug, stop and surface it; the first run of this catalog is itself an audit.

### 3. Full report (`report.rs`)

Per full_plan.md §9: run header (git rev, backend, scenarios run/passed/failed); per-scenario table (status | GET depth + chain rendering | per-class GET/PUT ops | GET/PUT bytes | Δ vs baseline where drifted); the per-class totals table in the adversarial "Object-Store Totals" style; a "Proposed re-baselines" section when capture ran (diff of changed fields, old → new); a "Post-response ops" section listing any spans excluded by the response-cutoff filter (visibility into background work). Copy semantics: the script (below) copies `report.md` to `tasks/perf-contract-report.md`.

### 4. `scripts/perf-contract.sh`

Mirror `scripts/overnight-adversarial.sh` conventions: MinIO bootstrap via `docker-compose.test.yml`; `cargo build --tests` before any timing-sensitive step; run `contracts` (and `capture` with `--capture`); copy report to `tasks/perf-contract-report.md`; rotate `target/perf-contract` at 14 days via targeted deletion (never `rm -rf`); exit code = number of failed scenarios; `--nightly` reserved (no-op until Phase 4 wires `latency_validate`).

### 5. CI wiring (documentation + workflow if repo has one)

Add a job spec (in the existing CI workflow file if present under `.github/workflows/`; otherwise document verbatim in `scripts/perf-contract.sh --help` and the report header): MinIO service container, `TEST_BACKEND=minio cargo test --test perf_contract_tests contracts -- --ignored`, **gating** (zero flake budget — any red is a real regression or a missing re-baseline). `capture` is never run in CI. Do not touch unrelated CI jobs.

### 6. Governance polish (`contract.rs`)

- Checked-in contract with empty `approved_by` → hard violation (Phase 1 rule; now also surfaced prominently in the report).
- **Improvement detection**: if measured < contracted on any exact assertion, that is still a violation (`BaselineDrift` with direction noted) — the frontier moved without a decision record; the fix is a re-baseline commit, not silence. Make the violation message say exactly that.
- `schema_version` bump rules documented in a module-level comment: additive fields = same version; semantic changes = bump + migrate all contracts in the same commit.

## Guardrails (binding; full_plan.md §11)

Zero production `src/` changes. Zero modifications to `tests/common/*` / `tests/adversarial/*`. No new dependencies. Everything `#[ignore]`. Never assert wall-clock. Code never writes into `tests/perf_contract/contracts/`. Explicit `cleanup_ns` + `harness.cleanup()`. No `rm -rf`. Unknown-op anatomies: investigate, document, or stop-and-surface — never freeze blind. Commits imperative, 70-char wrap.

## Out of scope

Tier 2 predict/profiles/ground truths (Phase 3). Tier 3 latency injection + `--nightly` payload (Phase 4). Eviction-behavior scenarios (out of Tier 1 entirely). Recall/quality assertions (adversarial runner + sentinel own those).

## Acceptance criteria (all must pass before commit)

```bash
docker compose -f docker-compose.test.yml up -d

# 1. Full catalog green (17 scenarios: 3 Phase-1 + 14 new)
TEST_BACKEND=minio cargo test --test perf_contract_tests contracts -- --ignored --nocapture

# 2. Determinism double-run (byte-identical counters.json/depth.json per scenario)
TEST_BACKEND=minio cargo test --test perf_contract_tests contracts -- --ignored --nocapture

# 3. Selftest still green (Phase-1 matrix unchanged)
TEST_BACKEND=minio cargo test --test perf_contract_tests perf_selftest -- --ignored --nocapture

# 4. Single-scenario selection works
TEST_BACKEND=minio ZEPPELIN_PERF_SCENARIOS=fts_query \
  cargo test --test perf_contract_tests contracts -- --ignored --nocapture

# 5. Script end-to-end (report copied, exit 0)
TEST_BACKEND=minio ./scripts/perf-contract.sh
test -f tasks/perf-contract-report.md

# 6. Default suite unaffected + lint
cargo test --lib && cargo test --tests
cargo clippy --tests -- -D warnings && cargo fmt --check
```

**Commit**: e.g. `Add perf-contract phase 2: full scenario catalog + report`, body listing each newly frozen anatomy in one line each (this commit is the auditable record of today's cost frontier).
