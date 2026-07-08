# 04 - Claude review follow-ups

This follow-up batch addresses the July 8 Claude-pending review over
`pitr-retention-gc`, `query-as-of`, `restore-as-clone`, and the TLA+ specs.
Each group lands as its own commit in the order below.

## Plan 1 - GC and pin hardening

Findings: F1, F2, F3, F4.

- Re-read retained manifest history before draining `pending_deletes`, so a
  history snapshot created after prune but before drain protects its keys.
- Validate `NamedSnapshot::create` against an existing retained history object.
- Apply `gc.skew_slop_secs` to PITR time-retention decisions.
- Document that `as_of` reads at the retention boundary can fail transiently
  if their generation is pruned and swept after resolution.

Verification:

- Storage-GC regression for a history snapshot injected between prune and drain.
- Manifest unit tests for missing snapshot generations and skew-slop retention.
- API spec update for the transient retention-boundary behavior.

## Plan 2 - as_of correctness and consolidation

Findings: F5, F6, F7, F8.

- Replace duplicated query/clone `as_of` resolution with one shared helper.
- Scan full retained history for timestamp resolution and choose the highest
  version whose commit time is at or before the timestamp.
- Reject unknown query parameters instead of silently serving live data.
- Pin and document the intended `as_of` behavior with eventual consistency.

Verification:

- Query and clone timestamp skew regression tests.
- Unknown query-param envelope test.
- Query test covering historical eventual reads or validation if strong-only is
  chosen for `as_of`.

## Plan 3 - restore-as-clone failure handling

Findings: F9, F10, with F11 and F12 included.

- Pin the resolved source generation during copy materialization and release it
  after success or failure.
- Clean up copied target objects and target metadata on materialization failure.
- Make clone copies bounded-concurrent inside the request.
- Return a storage/copy-specific error for target object collisions instead of
  `NAMESPACE_ALREADY_EXISTS`.

Verification:

- Clone copy-failure test proves no target meta or partial target objects remain.
- Source-GC-during-copy test proves the internal pin protects source artifacts.
- Retry-after-failure test proves no orphaned target blocks a later clone.
- Storage error test covers destination collision mapping.

## Plan 4 - TLA+ coverage and drift guard

Findings: F13, F14.

- Increase small state spaces for `RestoreCloneSafety` and
  `IncrementalArtifactClosure`.
- Add one script that runs the five July PITR/GC/clone/group-commit specs and
  their expected-negative variants.
- Re-record TLC stats after the constant changes.
- Add a spec-action-to-code cross-reference table to
  `formal-verifications/README.md`.

Verification:

- Run the TLA script locally when TLC is available, or record the exact command
  failure if the jar is absent.
- Run formatting/checks for touched Rust code after the implementation groups.
