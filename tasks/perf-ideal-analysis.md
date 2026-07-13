# Ideal S3 performance analysis — exhaustive production paths

## Objective

Use Zeppelin's dedicated performance harness to find every distinct production
operation/state path that reaches object storage, measure each path in isolation,
recover its longest serial GET dependency chain, and identify concrete S3 work
that can be removed without weakening correctness.

This is not an adversarial campaign. The correctness/fault runner is already a
separate, trusted system and is outside this task.

## Hard scope

- Work only through `tests/perf_contract/**`, `tests/perf_contract_tests.rs`,
  `target/perf-contract/**`, and `ZEPPELIN_PERF_*` for the analyzer.
- Do not edit `tests/adversarial/**`, adversarial scripts/artifacts, or any
  `ZEPPELIN_ADVERSARIAL_*` behavior.
- Do not alter or revert commit
  `0082650ef4b99a79b5069123527e4ac246f77077`.
- The 18 checked-in contracts and their TOML baselines are frozen regression
  guards. Do not rebaseline, repurpose, or add the exhaustive catalog to
  `ALL_SCENARIOS`.
- Add a separate ignored `ideal_analysis` entrypoint.
- Use real MinIO through `TestHarness`; no storage mocks.
- Setup, priming, verification, reporting, and cleanup are outside every
  measured interval.
- Measure exactly one logical operation at a time. No unrelated background
  loop may run unless background work is the operation being measured and is
  explicitly awaited.
- Do not make a production change until a focused perf scenario proves a
  concrete `REDUCIBLE` chain.
- Never push. Keep analyzer, documentation, findings, and each accepted product
  optimization in separate local commits.

## Definitions

### Production path

A production path is a top-level HTTP or directly invoked maintenance action
plus the smallest pre-state that changes its ordered storage-call shape.

Two paths are distinct when operation or pre-state changes one or more of:

- the set or order of `ZeppelinStore` methods;
- full, range, multi-range, or conditional GET behavior;
- unconditional or conditional PUT behavior;
- recursive versus delimiter LIST behavior;
- DELETE/COPY behavior;
- serial dependency depth or parallel stage shape;
- transferred artifact classes or byte ranges.

Aliases that execute the same chain are one path with multiple source anchors,
not duplicate scenarios.

### Object-store adapter invocation

The observer records each `ObjectStore` adapter invocation beneath
`ZeppelinStore`:

- GET, including full/range/conditional request shape;
- PUT, including conditional mode;
- HEAD;
- recursive and delimiter LIST;
- DELETE;
- COPY/COPY-if-absent.

For every invocation retain its ordinal, typed request shape, adapter outcome,
artifact class, normalized key pattern, successful streamed bytes where
meaningful, and adapter elapsed time. The observer cannot see SDK/backend HTTP
retries or the individual HTTP pages behind a recursive LIST; that is a named
instrumentation gap, not transport-attempt accounting.

### Longest serial GET chain

The existing `DepthTracker` observes interval ordering below the storage
gateway. Within one isolated operation, GET `A` can precede GET `B` when
`A.end_seq <= B.start_seq`. Parallel siblings overlap and therefore do not
inflate serial depth.

Call this result the longest serial GET dependency chain. Do not claim semantic
parent-span lineage that the observer does not possess. HEAD remains a separate
physical verb and is not silently counted as GET in the ideal analysis.

## Frozen baseline

- revision: `0082650ef4b99a79b5069123527e4ac246f77077`
- command:

      TEST_BACKEND=minio cargo test --release \
        --test perf_contract_tests contracts -- --ignored --nocapture

- result: 18 scenarios passed, 0 failed
- report:
  `target/perf-contract/run-1783895918-187503000-11614/report.md`
- total baseline traffic: 192 GETs / 11,839,600 GET bytes and 37 PUTs /
  3,043,599 PUT bytes
- depth soundness: exactly one measured operation in flight; no unrelated
  background compaction, GC, or hydration

The baseline is the observer-effect gate. After analyzer changes, rerun the
same command and require the checked-in scenarios' counters/depth outputs to
remain unchanged.

The exhaustive analyzer reuses only 15 of the 18 frozen scenarios. The
`hydration` guard combines two detached triggers, while `compaction_cycle` and
`compaction_incremental` include a counted harness-oracle manifest GET before
the production operation. They remain unchanged in the 18-contract regression
gate. Hydration is an explicit gap; clean dedicated ideal scenarios replace the
two compaction guards.

## Deep perf-only module

Add `tests/perf_contract/ideal/` with one external entrypoint:

    async fn run_ideal_analysis_entry() -> IdealRunSummary;

The module owns:

- source-derived inventory and coverage validation;
- deterministic scenario catalog;
- isolated scenario lifecycle and budget loop;
- physical-operation normalization;
- serial GET-chain calculation;
- aggregation, ranking, and artifacts.

Suggested internal files:

- `ideal/mod.rs`
- `ideal/inventory.rs`
- `ideal/catalog.rs`
- `ideal/runner.rs`
- `ideal/artifacts.rs`

Do not introduce public async closures or a scenario trait when an enum-driven
dispatcher has only one implementation. `DepthStore`, HTTP invokers, and direct
domain invokers are the real adapters at the measurement seam.

## Source-derived inventory

Start at `src/storage/store.rs` and freeze every public async storage method:

- `probe_configured_endpoint` — classify explicitly as a TCP/config probe, not
  an S3 operation;
- `put`;
- `get`;
- `get_range`;
- `get_ranges`;
- `get_with_meta`;
- `get_if_none_match`;
- `put_if_match`;
- `put_if_not_exists`;
- `copy_if_not_exists`;
- `delete`;
- `list_prefix`;
- `list_common_prefixes`;
- `exists`;
- `head`;
- `delete_prefix`;
- `delete_prefix_paged`.

Trace callers upward into production operations. The inventory artifact must
map:

    production path + pre-state
      -> production file/function anchors
      -> ZeppelinStore methods
      -> expected physical verbs
      -> covering ideal scenario IDs

An executable validator must prove:

1. the public async storage-method list still matches `src/storage/store.rs`;
2. every S3 method/semantic variant has at least one production-path row;
3. every executable production-path row has a scenario, while every gap or
   no-production-caller row has a specific reason;
4. every referenced scenario exists exactly once;
5. every scenario claims at least one known production path;
6. exclusions carry a fail-loud reason.

## Required operation/state families

The source inventory is authoritative. At minimum audit and either cover or
explicitly exclude these families.

### Namespace and operational control plane

- create fresh and idempotent-existing namespace;
- cold and resident namespace metadata reads;
- namespace delimiter listing/registration scan;
- index-config conditional update;
- compaction status;
- namespace deletion tombstone publication, paged cleanup, and completion;
- readiness/startup storage probes;
- any unrouted/dead handler identified by the source audit.

### Writes and vector reads

- upsert single and batch;
- delete single and batch;
- compacted-only, WAL-only, and compacted-plus-WAL state;
- strong and eventual consistency;
- fetch hit/miss, projection shape, and relevant ID-count buckets;
- live WAL fragment counts where they change exact reads.

### Query families

- cold and warm ANN;
- filtered ANN with attributes and bitmap;
- FTS and hybrid;
- batch query;
- pagination;
- as-of generation, timestamp, and snapshot;
- meaningful top-k/nprobe/state variants;
- full/range/multi-range artifact reads used by the production query path.

### Maintenance and lifecycle

- full, incremental, and no-op compaction;
- HTTP compaction acceptance versus awaited compactor work;
- background-loop discovery/lease/compaction as an isolated owned operation;
- GC with nothing eligible, candidate discovery, eligible deletion, pending
  deletes, retained history, snapshots, and staging roots;
- hydration first/repeat, capacity refusal, bitmap, and FTS branches are
  explicit gaps until the detached worker exposes an owned completion seam;
- snapshot create/get/list/delete;
- clone from current/generation/timestamp/snapshot, including copy-if-absent;
- lease acquire/renew/release paths when not already represented by an owned
  maintenance scenario.

Unsafe failure-only paths or paths needing unavailable external coordination
may be excluded only with a specific inventory reason.

## Artifacts

Write a new `target/perf-contract/ideal-run-*` directory containing:

- `run.json` — entry, revision and dirty state, exact command, budget, FNV-1a
  catalog fingerprint, selected scenario count, elapsed milliseconds, cycles,
  scenario runs, fail-fast failure count, selected IDs, and soundness scope;
- `catalog.json` and `storage-methods.json` — executable catalog and storage
  gateway inventory;
- `inventory.json` and `inventory.md` — complete source coverage matrix;
- `scenario-samples.jsonl` — one normalized sample per scenario execution;
- `physical-totals.json` — representative-worst adapter
  invocation/byte totals by verb, mode, and class;
- `get-chains.json` — normalized longest serial GET chains;
- `ranked.json` — sample count, min/max GET work, distinct normalized-cost
  count, and representative worst sample;
- `normalized-costs.json` — latency-free deterministic comparison artifact;
- `report.md` — deterministic ranking and per-scenario review tables.

The complete catalog is 114 cases: 15 eligible frozen cases and 99 dedicated
ideal cases. A focused filter reports the selected subset count in `run.json`.

Each normalized GET-chain link includes:

- ordinal;
- artifact class;
- normalized key pattern;
- full/range/conditional shape;
- range bounds where present and successful streamed bytes;
- elapsed microseconds as context;
- adapter outcome.

Ranking order is deterministic:

1. serial GET depth descending;
2. total GET operations descending;
3. GET bytes descending;
4. scenario ID ascending.

Do not use latency as a deterministic tie-breaker.

## RED/GREEN implementation order

Work in vertical slices through the catalog/measurement/report interfaces.

1. RED: source inventory detects an unknown storage method and an uncovered
   production path. GREEN: typed inventory and coverage validation.
2. RED: parallel GET siblings incorrectly inflate depth. GREEN: pure GET
   serial-chain calculation with the existing interval-order semantics.
3. RED: adapter observation loses range/condition/list mode or failed-invocation
   evidence. GREEN: additive `DepthStore` span detail without changing existing
   contract calculations.
4. RED: ideal report ordering or normalization is nondeterministic. GREEN:
   stable typed samples and ranking.
5. RED: setup or cleanup traffic appears in a measured sample. GREEN: one
   explicit measurement gate and exact reconciliation.
6. RED: one required scenario family lacks coverage. GREEN: add its smallest
   isolated world and invoker; repeat family by family.
7. RED: budget loop can stop before a complete catalog pass. GREEN: always
   finish at least one full pass and only stop between scenarios/cycles while
   still writing complete artifacts.

Use real MinIO for storage-backed tests. Pure chain/ranking tests may use
literal span fixtures because their independent expected result is the test
authority, not a storage mock.

## Entry point and environment

Add only to `tests/perf_contract_tests.rs`:

    #[tokio::test]
    #[ignore = "requires MinIO and explicit ideal-analysis budget"]
    async fn ideal_analysis() {
        perf_contract::run_ideal_analysis_entry().await;
    }

Require:

- `TEST_BACKEND=minio`;
- positive `ZEPPELIN_PERF_IDEAL_SECONDS`;
- optional `ZEPPELIN_PERF_IDEAL_SCENARIOS` for focused reproduction;
- optional `ZEPPELIN_PERF_ARTIFACTS`, defaulting to
  `target/perf-contract`.

Reject capture/selftest modes and unknown scenario filters. A complete
exhaustive pass uses `ZEPPELIN_PERF_IDEAL_SECONDS=1`: the runner always finishes
the full selected catalog before checking the time budget. Longer sampling,
including `3600`, is optional validation rather than an analysis prerequisite.

## Validation gates

Run in order:

1. focused pure inventory/chain/report tests;
2. focused MinIO scenario tests;
3. ideal catalog coverage test;
4. `cargo fmt --check`;
5. release Clippy for affected integration targets with `-D warnings`;
6. unchanged 18-contract gate;
7. short complete-catalog smoke;
8. repeat smoke and compare normalized artifacts byte-for-byte;
9. one complete exhaustive catalog pass; an optional longer soak may follow.

The focused smoke blocks the exhaustive pass unless:

- every catalog scenario completes;
- every measured span belongs to exactly one scenario;
- setup/cleanup traffic is absent;
- every inventory row reconstructs to observed/explicitly expected verbs;
- normalized artifacts are deterministic;
- frozen contract counters and depths remain unchanged;
- artifact size is recorded during the handoff pass.

## Implemented validation

- analyzer commit: `db78911`
- pure integration target: 91 passed, 0 failed, 12 ignored
- release Clippy with `-D warnings`: passed
- complete catalog passes: 114/114 scenarios, 0 failures in each pass
- deterministic comparison:
  - `target/perf-contract/ideal-run-1783902682-899782000-83852`
  - `target/perf-contract/ideal-run-1783902694-472371000-84078`
  - `normalized-costs.json` is byte-identical, SHA-256
    `71dd3da7b5f2338b575057282c8cd1771378825a29f4fb14f6c611d0dddeb8dd`
  - every scenario has exactly one normalized cost vector
  - each artifact directory is 2.4 MB
- frozen observer-effect rerun:
  `target/perf-contract/run-1783902707-778639000-84294/report.md`
- all 18 frozen contracts passed; all 36 `counters.json` and `depth.json`
  files are byte-identical to the baseline
- no one-hour run was performed; one complete deterministic catalog pass is
  the required analysis input and longer sampling remains optional

## Analysis and optimization

For each ranked path:

1. establish the correctness lower bound;
2. compare observed serial depth, total calls, and bytes;
3. explain every chain link from production source;
4. distinguish required authority/CAS work from duplicate reads, avoidable
   metadata, over-wide ranges, or serial work that can safely be shared or
   parallelized;
5. classify as `MINIMAL`, `EXPLAINED`, `SUSPECT`, `REDUCIBLE`, or
   `INSTRUMENTATION GAP`.

Write `tasks/perf-ideal-findings.md` before any production edit. For each
`REDUCIBLE` path, reproduce it with the cheapest focused scenario and map the
avoidable operation to production file:line.

An optimization is accepted only when:

- a RED focused regression/perf test proves the old excess chain;
- the smallest production change preserves S3 authority and fail-loud
  correctness;
- relevant correctness tests pass;
- all 18 frozen contracts pass without rebaseline;
- the focused scenario shows an exact reduction in calls, bytes, or serial
  depth;
- no other measured path regresses or merely inherits the shifted work.

## Commit boundaries

1. dedicated perf inventory, exhaustive catalog, physical observation, chain
   reporting, and RED/GREEN tests;
2. corrected runbook/documentation;
3. exhaustive-pass findings only;
4. one separate commit per accepted production optimization.

No commit may contain a file below `tests/adversarial/`.

## Final acceptance

- the source-derived inventory covers or explicitly excludes every production
  S3 path and storage semantic variant;
- every executable inventory row has an isolated ideal scenario;
- total physical work and longest serial GET chains are auditable per state;
- one complete exhaustive catalog pass completes with deterministic artifacts;
- top chains receive evidence-backed lower-bound verdicts;
- accepted optimizations have exact before/after proof and preserve all frozen
  contracts;
- the adversarial runner is byte-for-byte untouched by this goal.
