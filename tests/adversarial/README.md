# Adversarial runner contract

The adversarial runner drives the production HTTP and object-store paths while
keeping fault control, model state, and verification in this integration-test
module. S3 remains authoritative; the harness never substitutes local state
for persisted state.

## Fault boundaries

Faults enter at four product-facing boundaries:

1. `ObjectStore` faults wrap GET, HEAD, LIST, PUT, COPY, and DELETE calls.
2. `ClientHttp` faults act between the workload client and the real server.
3. `Process` faults abort and restart the test server at durable write sites.
4. `Clock` faults change the shared test clock used by leases, GC, and TTLs.

`Runner` events are orchestration records for operational environment changes,
such as starting a second node. They do not replace one of the four injection
boundaries.

Every scheduled event has a deterministic id, logical start and optional end,
boundary, target selector, and `FaultKind`. Every observed effect is written as
a `TimelineEvent` with the logical operation index, boundary, action,
`ObservedResult`, and recovery evidence. Wall time is diagnostic only.

## Logical time and ambiguity

The scheduler advances only at workload operation boundaries. Delays, hold
windows, process restarts, and operational events use this logical index; they
must not use sleeps to choose an interleaving. A held foreground operation
keeps its original operation index, records its scheduled and actual join
points, and is joined once.

An acknowledgement loss or interrupted mutation becomes an indeterminate
model effect. The runner records the reason, prevents a conflicting assumed
state, and resolves the effect from authoritative storage during the quiet
period as `applied` or `not_applied`. An effect that cannot be resolved is an
I18 violation. Ambiguity must never be silently treated as success.

## Canonical quiet period

Every deterministic, LegacyChaos, and scheduled profile uses one ordered
`QuietPeriod::run` protocol:

1. quiesce the scheduler;
2. disable injectors and restore the clock/network;
3. release and join held operations;
4. stop the second node;
5. restart the primary if needed and wait for health;
6. stop and join background compaction;
7. resolve indeterminate effects;
8. force one inline compaction for each live namespace;
9. run two GC cycles with `keep_count=1`;
10. run S3, sketch-publication, lineage, and fencing oracles;
11. exhaustively fetch modeled ids and run model-derived queries.

Each step emits one `quiet:<step>` timeline event, including no-op or skipped
steps. There is no second quiesce implementation.

## Artifacts and rotation

Scheduled profiles write `timeline.jsonl`. `faults.jsonl` is reserved for the
legacy chaos injector. Reports contain a per-seed timeline table and a
run-level boundary/kind resolution summary. Failure reproduction commands
carry the active `ZEPPELIN_ADVERSARIAL_PROFILE` and replay the recorded
schedule.

Mixed mode uses a stable twelve-slot table. Residues 0 and 2 are deterministic,
residue 1 is LegacyChaos, and residues 3 through 11 are PostCommit, Network,
Crash, Clock, Content, Semantic, Sched, Ops, and Full. An explicit profile
overrides the table for every seed. Full takes one deterministic event from
each scheduled family so boundary faults can overlap in one run.

## Adding a fault kind

A new fault is incomplete unless the same change ships this quartet:

1. deterministic schedule generation in `FaultScheduler::for_seed`;
2. an implementation at the selected proxy, controller, clock, or runner
   boundary;
3. a model/oracle absorption rule for every ambiguous outcome;
4. an oracle mutation self-test pinned to the expected `ViolationId`.

The consolidated memory-backend matrix contains sixteen mutations. Each clean
control and mutation is capped at 80 workload operations. Run it with:

```bash
cargo test --release --test adversarial_workload_tests \
  oracle_selftest -- --ignored --nocapture
```

The expected local release-build wall time is at most three minutes. A slower
or incomplete matrix is a test failure to investigate, not a reason to remove
coverage.
