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
4. `Clock` faults change the shared wall clock used by persisted timestamps,
   leases, GC, and PITR horizons. Process-local TTLs, timeouts, latency
   measurements, hydration windows, and compaction upload elapsed time remain
   monotonic and must be immune to these jumps.

`Runner` events are orchestration records for operational environment changes.
`StartReadOnlyNode` is the supported multi-node topology: node 0 owns all
mutations and background work, while node 1 has no compaction loop and receives
only read operations. The legacy `StartSecondNode` event deliberately creates
two writers and is retained only for the explicit future-architecture campaign.
Runner events do not replace one of the four injection boundaries.

Every scheduled event has a deterministic id, logical start and optional end,
boundary, target selector, `FaultKind`, `ContractClass`, and protected-assumption
metadata. Every observed effect is written as a `TimelineEvent` with the same
classification plus the logical operation index, boundary, action,
`ObservedResult`, and recovery evidence. Wall time is diagnostic only.

## Contract classes and interpretation

- `SupportedV1` preserves the successful object-store, supported-provider,
  single-writer, and monotonic-duration contracts. Its findings block v1.
- `ProviderContractAbuse` simulates successful wrong-key/body operations,
  silent successful deletes, stale/wrong reads, and inconsistent LIST/HEAD.
  These findings certify or reject an adapter/provider; they are not v1 bugs.
- `FutureArchitecture` runs the retained two-writer topology. Its findings feed
  a future distributed-writer design and do not block v1.
- `HarnessSelfTest` deliberately corrupts a model/oracle and gates only its
  pinned self-test.

`config.json` stores a `fault_contracts` entry for every generated event;
`timeline.jsonl` repeats `contract_class` and `violated_assumptions` on every
recorded event. Reports label v1 failures separately from non-blocking research
findings. Legacy Phase 1-7 schedules remain decodable and replayable even though
they predate the duplicated metadata.

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
6. force an authoritative security-policy refresh, resolve security mutations,
   close bounded-staleness windows, and verify revocation/audit/policy state;
7. stop and join background compaction;
8. resolve indeterminate data effects;
9. force one inline compaction for each live namespace;
10. run two GC cycles with `keep_count=1`;
11. run S3, sketch-publication, lineage, and fencing oracles;
12. exhaustively fetch modeled ids and run model-derived queries, including
    per-tenant I23/I27 visibility checks when the security program is active.

Each step emits one `quiet:<step>` timeline event, including no-op or skipped
steps. There is no second quiesce implementation.

## Artifacts and rotation

Scheduled profiles write `timeline.jsonl`. `faults.jsonl` is reserved for the
legacy chaos injector. Reports contain a per-seed timeline table and a
run-level boundary/kind resolution summary. Failure reproduction commands
carry the active `ZEPPELIN_ADVERSARIAL_PROFILE` and replay the recorded
schedule.

Mixed mode uses a stable nine-slot supported table. Residues 0 and 2 are
deterministic, residue 1 is LegacyChaos, and residues 3 through 8 are
PostCommit, Network, Crash, Clock, Sched, and SupportedFull. SupportedFull
selects supported semantic and operational events individually and includes a
one-writer/read-only-node window; it never includes provider lies or a second
writer.

An explicit `ZEPPELIN_ADVERSARIAL_PROFILE` overrides the table for every seed.
The `security` profile adds a deterministic actor/credential registry, key,
grant, and delegated-token lifecycle operations, all tenant observation
surfaces, constrained-write, export, security-admin, and audit probes. It also
collects live structural retrieval receipts, re-verifies every receipt against
its retained manifest generation, rejects a tampered receipt copy, checks audit
links during the quiet period, and verifies the signed day anchor after graceful
shutdown. Token actors exercise narrowing, expiry under clock jumps, and
parent-revocation absorption. The profile uses supported faults at all four boundaries, never
provider lies or a second writer. Security artifacts persist redaction-safe
`principals`, token selectors, and `security_ops`; operation-linked timeline
events add actor and decision metadata. API-key and delegated bearer secrets
remain memory-only. Artifacts recorded before this profile continue to decode
with the implicit administrator.
Use `provider_contract_abuse` for broken provider/adapter research and
`future_architecture` for dual-writer research. The legacy `content`,
`semantic`, `ops`, and `full` profile names remain accepted for artifact replay
and explicitly requested historical campaigns, but none is selected by Mixed.

## Branch deletion smoke

The deletion-unification trace carries the real `BranchingOp::DeleteBranch`
and `BranchingOp::DeleteSourceWithBranches` values inside the runner's normal
`Op`/`OpRecord` replay vocabulary while sending every delete through the
authenticated HTTP namespace handler. The seeded schedule varies the number
of safe pre-grace source-delete attempts and writes standard `ops.jsonl` plus
coverage. Until production exposes an authorized fork-activation endpoint,
setup alone uses the explicitly labeled
`branching-test-support::activate_fork_for_test` seam. That setup must not be
treated as activation-path release evidence.

Each seed proves that source deletion returns the expected
`namespace_has_live_branches` 409 both before the persisted reader-safety
deadline and after the deadline while the root remains. An injected wall clock
then lets a second branch DELETE release the exact root without a sleep. The
server is gracefully restarted before that clock movement so the initial
request's pre-grace cleanup worker is joined rather than raced by the explicit
retry. Only after exact root release may the source DELETE return 202. Records
are written under `target/adversarial/branching-delete-smoke/seed-*/` by
default.

Run at least two pinned deterministic seeds against real MinIO:

```bash
TEST_BACKEND=minio \
ZEPPELIN_ADVERSARIAL_MODE=deterministic \
ZEPPELIN_ADVERSARIAL_SEEDS=0,2 \
ZEPPELIN_ADVERSARIAL_PRESERVE=never \
cargo test --features branching-test-support \
  --test adversarial_workload_tests branching_delete_smoke \
  -- --ignored --nocapture
```

## Product-fix admission

Before a runner finding can authorize a production `src/` change, its RCA must
record:

1. which protected assumptions the fault preserved or violated;
2. the current public/durability contract that forbids the observed result;
3. a conformant MinIO/S3 reproduction (or wrapper that still honors the pinned
   `ObjectStore` contract);
4. added storage operations, serialized stages, transferred bytes, CPU passes,
   coordination, or fallback behavior; and
5. the correction that instead belongs in the harness, adapter certification,
   or future-architecture plan.

Provider-abuse and future-architecture findings stop at classification and
reporting. A proposed synchronous S3 operation also requires an approved
before/after table of GET/HEAD/PUT/LIST counts by key class, transferred bytes,
GET-only and PUT+GET critical-path depth, and new ambiguity states. Captured
performance evidence never authorizes a TOML rebaseline by itself.

## Provider certification

The provider conformance suite certifies A1/A2 once per advertised backend:

```bash
TEST_BACKEND=minio cargo test --release --test provider_conformance_tests \
  -- --nocapture
```

It checks exact and atomic PUT/overwrite behavior, strong visibility,
conditional PUT and version tokens, GET/HEAD coherence, unique complete LIST,
copy/create-only semantics, and delete visibility. Run the same test against
every provider proposed for support; a failure means that adapter/provider is
unsupported until corrected or the product contract is deliberately changed.

## Adding a fault kind

A new fault is incomplete unless the same change ships this quartet:

1. deterministic schedule generation in `FaultScheduler::for_seed`;
2. an implementation at the selected proxy, controller, clock, or runner
   boundary;
3. a model/oracle absorption rule for every ambiguous outcome;
4. an oracle mutation self-test pinned to the expected `ViolationId`.

The schedule generator must also assign the fault's contract class and violated
assumptions. A new provider-abuse or future-architecture event must be reachable
only through an explicit campaign; a new supported event may join Mixed only
after its assumption audit.

The consolidated matrix contains twenty-eight mutations, including pinned
receipt-signature, Merkle-path, and audit-record-drop mutations for I29 in
addition to the security-oracle mutations for I22-I28. Each clean
control and mutation is capped at 80 workload operations. Run it with:

```bash
cargo test --release --test adversarial_workload_tests \
  oracle_selftest -- --ignored --nocapture
```

The expected local release-build wall time is at most three minutes. A slower
or incomplete matrix is a test failure to investigate, not a reason to remove
coverage.
