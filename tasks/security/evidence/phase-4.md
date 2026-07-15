# Phase 4 evidence

Status: implementation, validation, the explicitly approved
`secured_filtered_query` contract, the single configured 30-minute soak, and
focused post-soak harness repair validation are complete. The independent
Standards and Spec audits are clear; only the Phase 4 commit remains.

## RED -> GREEN — mandatory constraints and observation surfaces

The primary public-seam suite is:

```text
TEST_BACKEND=minio cargo test --release --test security_filter_tests -- --nocapture
```

The Phase 4 work introduced exact MinIO coverage for policy-owned filters,
caller-filter conjunction, field masks, query/batch/facet/group/page/cursor/
explain/as_of/fetch surfaces, stamps, forbid-set, scoped delete, clone
non-widening, and the `AttributeAdmin` exception. The production filter seam is
also exercised by `tests/security_filter_prop.rs`, which proves conjunction
never widens a seeded corpus and projection never restores a masked field.

Focused GREEN and regression commands completed before the final audit pass:

```text
TEST_BACKEND=minio cargo test --release --test security_filter_tests
TEST_BACKEND=minio cargo test --release --test facet_query_tests
TEST_BACKEND=minio cargo test --release --test grouping_query_tests
TEST_BACKEND=minio cargo test --release --test cursor_query_tests
TEST_BACKEND=minio cargo test --release --test explain_query_tests
TEST_BACKEND=minio cargo test --release --test batch_query_tests
TEST_BACKEND=minio cargo test --release --test filter_underfill_tests
TEST_BACKEND=minio cargo test --release --test hybrid_query_fusion_tests
TEST_BACKEND=minio cargo test --release --test fts_e2e_tests
TEST_BACKEND=minio cargo test --release --test no_silent_partials_tests
```

The combined run passed 149 tests. The security audit/policy/API/kernel/
bootstrap regressions passed 91 tests, and `contract_tests` passed 14 tests.

## RED -> GREEN — missing fields at the write boundary

Target:

```text
TEST_BACKEND=minio cargo test --release --test security_filter_tests \
  attrless_upsert_cannot_satisfy_a_negative_mandatory_filter \
  -- --exact --nocapture
```

The original case rejected an absent attribute object, but `{}` and a map with
only unrelated fields reused query-side negative-predicate semantics and wrote
the row. Verbatim RED:

```text
assertion `left == right` failed: empty attributes constrained upsert: {"upserted":1}
  left: 200
 right: 403
test attrless_upsert_cannot_satisfy_a_negative_mandatory_filter ... FAILED
```

GREEN added a three-valued write-scope evaluator: missing leaves remain unknown
through AND/OR/NOT and only a definite match is writable. Query filter semantics
are unchanged. Two unit regressions and the exact MinIO target passed. The rest
of `security_filter_tests` passes 30/30 when the intentional identity-oracle RED
is skipped.

## RED -> GREEN — BM25 policy corpus versus caller candidates

Target:

```text
TEST_BACKEND=minio cargo test --release --test security_bm25_isolation_tests \
  caller_filter_only_narrows_candidates_not_mandatory_bm25_corpus_statistics \
  -- --exact --nocapture
```

Verbatim RED proved that collapsing the mandatory and caller filters before
building the lexical corpus changed score bits:

```text
left: [("acme-short", 1049840401)]
right: [("acme-short", 1046635403)]
test caller_filter_only_narrows_candidates_not_mandatory_bm25_corpus_statistics ... FAILED
```

GREEN builds IDF/document-length statistics from the full policy-visible
corpus and applies the effective policy-and-caller predicate only to scored
candidates. The exact MinIO target passed 1/1, and the complete BM25 isolation
suite passed 17/17.

## RED -> GREEN — fail loud on an unresolved active segment

Target:

```text
TEST_BACKEND=minio cargo test --release --test security_bm25_isolation_tests \
  mandatory_filtered_bm25_fails_when_active_segment_descriptor_is_missing \
  -- --exact --nocapture
```

Verbatim RED:

```text
malformed active segment reference must fail loud, got 200 OK:
{"results":[],"scanned_fragments":0,"scanned_segments":0}
test mandatory_filtered_bm25_fails_when_active_segment_descriptor_is_missing ... FAILED
```

GREEN routes ANN and BM25 active-segment resolution through one helper that
returns an index error when the authoritative active ID lacks its descriptor.
The exact MinIO target passed 1/1.

## RED -> GREEN — durable constraint audit settlement

A nested batch constraint denial originally returned a top-level HTTP 200 and
escaped audit annotation. The MinIO regression failed with:

```text
expected exactly one audit record, got []
```

The batch handler now records the nested denial count in typed audit params;
`security_constraint_audit_tests` passed 13/13, including forbid-set denial and
audit-PUT-failure barriers. A separate unit RED showed that
`Obligation::DurableAudit` alone did not retain settlement state:

```text
assertion failed: audit_request_required(Action::Query, &decision)
```

Settlement now follows the decision obligation itself rather than a duplicated
action/privilege switch. The exact unit target passed.

## RED -> GREEN — strict policy and OpenAPI shapes

An `AttributeAdmin`-only grant carrying constraints was accepted with HTTP 201
even though those constraints are not part of a separate `VectorUpsert` grant's
aggregation. The focused MinIO RED expected 400. GREEN rejects a constrained
`AttributeAdmin` grant unless the same selected grant also includes
`VectorUpsert`; the three constraint categories and combined positive control
passed, as did 13 policy unit tests.

`BatchQueryRequest` used `deny_unknown_fields` at runtime while OpenAPI omitted
`additionalProperties: false`. The focused contract assertion failed RED, then
passed after the schema was made equally strict.

## Historical RED — constrained upsert ID existence oracle (resolved below)

Target:

```text
TEST_BACKEND=minio cargo test --release --test security_filter_tests \
  constrained_upsert_cannot_reveal_hidden_id_collision \
  -- --exact --nocapture
```

The two worlds differ only in whether caller-chosen ID `probe` already belongs
to a hidden tenant. Verbatim RED:

```text
left: ((200, {"upserted":1}),
       (200, {"missing":[],"results":[{"id":"probe", ...}]}))
right: ((403, {"code":"constraint_violation", ...}),
        (200, {"missing":["probe"],"results":[]}))
test constrained_upsert_cannot_reveal_hidden_id_collision ... FAILED
```

With namespace-global caller-chosen IDs, arbitrary scoped creation, hidden
collision protection, and cross-world indistinguishability cannot all hold:
creating in the absent world makes a later scoped fetch distinguish it from the
hidden-collision world. This requires an explicit identity/write-semantics
decision before GREEN.

## Historical RED — shared-index ANN hidden-corpus influence (resolved below)

Target:

```text
TEST_BACKEND=minio cargo test --release --test security_ann_isolation_tests \
  mandatory_filter_ann_is_invariant_to_hidden_only_recompaction \
  -- --exact --nocapture
```

All-cluster control remains stable, and no hidden ID is returned, but hidden-only
writes plus recompaction change the one-probe policy-visible ANN/fusion frontier.
Verbatim RED includes the visible `acme-00` membership difference:

```text
left ANN:  [("acme-10", ...), ("acme-20", ...), ("acme-00", ...), ("acme-30", ...)]
right ANN: [("acme-10", ...), ("acme-20", ...), ("acme-30", ...)]
hidden-only writes and full recompaction changed policy-visible ANN/fusion IDs, order, or score bits
test mandatory_filter_ann_is_invariant_to_hidden_only_recompaction ... FAILED
```

Strict ordering noninterference requires either exact policy-visible retrieval
or immutable policy-slice ANN artifacts. Exact retrieval breaks the frozen
equivalent-caller-filter cost budget; policy-slice artifacts are a substantial
index design.

## Policy-owned retrieval artifacts RED -> GREEN

The selected structural fix is an immutable retrieval artifact per mandatory
policy scope, derived only from rows visible to that scope. It preserves the
normal indexed query path without allowing hidden rows to affect training,
candidate selection, BM25 statistics, order, or score bits.

- ANN descriptors bind the source segment, mandatory filter, and full
  `IndexingConfig` with a versioned SHA-256 key. Descriptor publication is
  create-only and last, so concurrent builders either publish the winner or
  load and validate it.
- Flat sources produce flat scoped artifacts; hierarchical sources preserve
  hierarchical topology. Empty scopes publish a typed empty descriptor.
- Stable BM25 policy corpora are immutable per source segment. Strong queries
  merge the current WAL frontier in manifest order, applying deletes before
  upserts; eventual queries apply effective tombstones without adding WAL rows.
  Caller filters narrow scored candidates but never rebuild corpus statistics.
- Restart tests prove identical results with zero scoped-artifact PUTs and zero
  source-cluster rescans after publication.
- The decoded corpus/ANN/FTS cache remains byte bounded. CPU-heavy corpus
  assembly, filtering, index building, and FTS encoding/decoding run behind
  `spawn_blocking`; async request workers retain storage I/O and orchestration.
- Scoped keys follow an explicit GC grammar. They remain reachable while their
  parent source segment is current or PITR-retained, then become normal
  age/identity-gated candidates after the parent leaves every root.

Exact GREEN:

```text
security_ann_isolation_tests: 11 passed
security_bm25_isolation_tests: 18 passed
retrieval_scope::tests: 3 passed
```

The ANN suite includes hidden-only recompaction invariance, restart reuse, and
hierarchical-topology preservation. The BM25 suite includes compacted and WAL
hidden-row invariance, strong/eventual merge ordering, caller-filter corpus
separation, restart reuse, and fail-loud descriptor validation.

## Scoped write identity RED -> GREEN

The caller-chosen-ID oracle RED compared two otherwise identical namespaces:
one where `probe` was absent and one where it existed outside the writer's
mandatory filter. Before the fix, the absent world created `probe` while the
hidden-collision world returned 403.

```text
TEST_BACKEND=minio cargo test --release --test security_filter_tests \
  constrained_upsert_cannot_reveal_hidden_id_collision -- --exact --nocapture
```

The structural contract now separates scoped create from scoped update:

- omitting `id` requests an opaque server-owned `zv1_...` identity;
- supplying `id` under a mandatory filter is update-only;
- absent and hidden explicit IDs return the same whole-batch 403 response;
- generated-ID collisions fail closed instead of overwriting a row;
- the existing manifest guard/CAS keeps the read-check-publication sequence
  atomic against concurrent writes.

Focused GREEN evidence:

```text
test constrained_upsert_cannot_reveal_hidden_id_collision ... ok
test constrained_create_without_id_returns_server_owned_identity ... ok
```

The create case covers both JSON and row-oriented MessagePack, validates the
returned ID-to-request-index mapping, and fetches the stamped row through the
same mandatory scope. `security_filter_tests` then passed 32/32, including the
legacy namespace/manifest migration. The OpenAPI contract test also passed with
the new request/response schemas and the columnar MessagePack update-only rule.

## G-PERF — approved and GREEN

The final exact-tree capture passed:

```text
TEST_BACKEND=minio ZEPPELIN_PERF_CAPTURE=1 \
  ZEPPELIN_PERF_SCENARIOS=secured_filtered_query \
  ZEPPELIN_PERF_ARTIFACTS=target/perf-contract/security-phase4-final-capture \
  cargo test --release --test perf_contract_tests capture \
  -- --ignored --exact --nocapture
```

Report:
`target/perf-contract/security-phase4-final-capture/run-1784072803-170651000-93870/report.md`.

```text
status: PASS
authn+authz p50 delta: 681 ns (budget 10,000 ns)
paired query regression: 0.00% (budget 5%)
object-store delta versus filtered_query: GET +0, PUT +0
warm census: manifest GET 1, cluster GET 8, total GET 9, PUT 0
warm bytes: 147684 GET, 0 PUT
GET depth: 3
```

This is the intended structural census: the warm policy-slice ANN reads the
manifest plus scope-local cluster ranges. The mandatory filter is compiled into
the artifact, so no attribute sidecars are read.

The user explicitly approved freezing this measured census. The final proposal
at
`target/perf-contract/security-phase4-final-capture/run-1784072803-170651000-93870/proposed/secured_filtered_query.toml`
replaced the stale scaffold with `approved_by = "anup"`, the audited rationale,
and the corrected depth explanation. No tolerance was widened. The checked-in
contract then passed:

```text
TEST_BACKEND=minio ZEPPELIN_PERF_SCENARIOS=secured_filtered_query \
  cargo test --release --test perf_contract_tests contracts \
  -- --ignored --exact --nocapture

secured_filtered_query security budget: p50_delta_ns=684
query_regression_bps=Some(0) added_get_ops=0 added_put_ops=0
test result: ok. 1 passed; 0 failed
```

Report: `target/perf-contract/run-1784072989-435605000-94380/report.md`.

## G-LIB, G-INT, and G-MAP

The exact post-audit tree passed:

```text
cargo test --release --lib
test result: ok. 501 passed; 0 failed

security_filter_tests: 32 passed
security_ann_isolation_tests: 11 passed
security_bm25_isolation_tests: 18 passed
security_clone_tests: 11 passed
security_constraint_audit_tests: 13 passed
security_filter_prop: 3 passed
contract_tests: 14 passed
```

The consolidated MinIO release command also passed
`historical_cursor_binding_tests`, `facet_query_tests`,
`grouping_query_tests`, `cursor_query_tests`, `explain_query_tests`,
`batch_query_tests`, `filter_underfill_tests`,
`hybrid_query_fusion_tests`, `fts_e2e_tests`, and
`no_silent_partials_tests` on the same tree. G-MAP is covered by
`server::tests::phase_four_constraint_consumers_are_exhaustive` in the
501-test library run plus the 14-test exact OpenAPI/fixture inventory.

The clone suite includes a real-MinIO storage barrier around target-manifest
CAS. A concurrent acknowledged target upsert wins; stale clone publication
returns `409 CONFLICT_RETRY` and never removes the row. Namespace incarnation
binding also prevents delete/recreate ABA.

## G-ADV

Before the soak, the adversarial runner was untouched, so the required standing
two-seed smoke ran instead of a runner self-test/replay matrix:

```text
TEST_BACKEND=minio ZEPPELIN_ADVERSARIAL_SECONDS=180 \
  ZEPPELIN_ADVERSARIAL_MODE=deterministic \
  ZEPPELIN_ADVERSARIAL_SEEDS=0,2 \
  ZEPPELIN_ADVERSARIAL_MAX_OPS=100 \
  ZEPPELIN_ADVERSARIAL_PRESERVE=always \
  ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/security-phase4-gadv \
  cargo test --release --test adversarial_workload_tests smoke \
  -- --ignored --nocapture

seed 0: failed=false ops=130 compactions=21
seed 2: failed=false ops=120 compactions=16
adversarial smoke: seeds=2 ops=250 compactions=37 failed=0
non_blocking_findings=0
```

This smoke is not the Phase 4 soak.

The sole soak later exposed and drove the harness-only held-call/process-crash
repair documented under G-SOAK. Post-repair, the exact recorded seed replay,
the capped seed-323 generation, the existing hold/quiescence regression, and
the complete non-ignored adversarial test target are GREEN. The latter passed
201 tests with 7 ignored campaign entry points.

Because the repair touched `tests/adversarial/`, the full G-ADV matrix was
rerun after it:

```text
TEST_BACKEND=minio \
  ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/security-phase4-post-soak-oracle \
  ZEPPELIN_ADVERSARIAL_PRESERVE=never \
  cargo test --release --test adversarial_workload_tests oracle_selftest \
  -- --ignored --exact --nocapture

test oracle_selftest ... ok
test result: ok. 1 passed; 0 failed

TEST_BACKEND=minio ZEPPELIN_ADVERSARIAL_SECONDS=180 \
  ZEPPELIN_ADVERSARIAL_MODE=deterministic \
  ZEPPELIN_ADVERSARIAL_SEEDS=0,2 \
  ZEPPELIN_ADVERSARIAL_MAX_OPS=100 \
  ZEPPELIN_ADVERSARIAL_PRESERVE=always \
  ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/security-phase4-post-soak-smoke \
  cargo test --release --test adversarial_workload_tests smoke \
  -- --ignored --exact --nocapture

seed 0: failed=false ops=130 compactions=21
seed 2: failed=false ops=120 compactions=16
adversarial smoke: seeds=2 ops=250 compactions=37 failed=0
non_blocking_findings=0
```

The oracle matrix detected every pinned mutation with its accepted invariant
and every clean control stayed clean. The post-repair smoke reproduced the
standing operation/compaction counts with zero findings. Neither bounded gate
is a soak.

## Independent final audit

Independent standards and spec reviewers audited the completed Phase 4 code.
They first found and drove fixes for CPU work left on async workers, a missing
module error type, an in-memory replacement for a required storage test, a GC
late-publication orphan race, and hierarchical configuration being collapsed
to flat. The final re-audit found no remaining spec defects.

After all-target clippy prompted boxing the ANN enum and bundling its build
arguments, the independent final-delta audit found one decoded-cache accounting
regression: boxed index handle bytes were omitted. The estimate now charges the
flat or hierarchical handle exactly once plus all existing dynamic capacities;
`boxed_scoped_ann_counts_heap_allocated_index_handle` is the regression. The
reviewer re-audited that fix and reported zero remaining defects.

The post-soak delta review first required the final-tree ignored oracle matrix,
the pinned two-seed smoke, and an exact-once proof stronger than a read-only
operation. The bounded G-ADV gates were rerun, and the replay regression now
uses a mutating upsert plus its authoritative generation checkpoint. The
complete non-ignored adversarial target and all final lint gates then passed on
that tree. Independent re-audit reported zero remaining Standards defects and
zero remaining Spec defects.

## G-LINT

```text
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 \
  cargo clippy --release --all-targets -- -D warnings
cargo fmt --check
git diff --check
git diff --cached --check
```

All four exited 0 on the final code tree.

## Files changed

- API/contracts: `api/zeppelin-api.yaml`, the Phase 4 v0.3.0 fixtures and
  fixture manifest, plus the strict route/fixture assertions in
  `tests/contract_tests.rs`.
- Policy/security domain: `src/security/{action,audit,constraints,decision,
  kernel,mod,policy,policy_cache,policy_store}.rs`.
- Retrieval/cache/storage lifecycle: `src/retrieval_scope.rs`,
  `src/cache/decoded_cache.rs`, `src/compaction/gc.rs`, `src/query.rs`,
  `src/index/filter.rs`, `src/wal/{manifest,mod,writer}.rs`, and the namespace
  incarnation/clone changes in `src/namespace/manager.rs`.
- Server/write/query wiring: `src/server/mod.rs`, the namespace/query/security/
  vectors handlers, `src/config.rs`, `src/error.rs`, `src/lib.rs`, and
  `src/types.rs`.
- Verification: the new Phase 4 security filter/property, ANN isolation, BM25
  isolation, clone/CAS, constraint-audit, and historical-cursor suites plus the
  focused existing regressions, perf-contract scenario machinery, and the
  held-call/process-crash recovery changes in `tests/adversarial/runner.rs`.
- Documentation/config: `docs/security-deployment.md` and
  `zeppelin.toml.example`.

No dependency was added. The unrelated untracked `tonier.MD` is excluded.

## Limitations and pending gates

- No product invariant finding was reported by the sole soak. Its seed-323
  termination was a harness assertion about a legal fault overlap; the focused
  replay and regression matrix are GREEN after the repair.
- The configured 30-minute soak terminated at 1,604.97 seconds when that
  assertion fired. Per the one-soak-per-phase rule, it was not rerun.
- The Phase 4 commit remains; every implementation, validation, review, and
  applicable soak gate is complete.

## G-SOAK

The single Phase 4 soak was configured for exactly 1,800 seconds and started
with:

```text
caffeinate -dimsu env \
  TEST_BACKEND=minio \
  ZEPPELIN_ADVERSARIAL_SECONDS=1800 \
  ZEPPELIN_ADVERSARIAL_MAX_OPS=500 \
  ZEPPELIN_ADVERSARIAL_MODE=mixed \
  ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/security-phase4-soak \
  cargo test --release --test adversarial_workload_tests overnight \
  -- --ignored --nocapture
```

Artifact root:
`target/adversarial/security-phase4-soak/run-1784073608`.

Seeds 0 through 322 completed with `failed=false`. At 1,604.97 seconds, seed
323 reached a legal overlap between `supported-full-sched-05` (`HoldCall`) and
`supported-full-crash-02` (`CrashAt hydration_get/pre`). The held query recorded
`ambiguous:server_crashed` at its logical join boundary, but the runner asserted
that a foreground held call could never complete through a process crash:

```text
tests/adversarial/runner.rs:3079
a foreground HoldCall cannot also complete through a process crash
```

This was a harness/orchestration defect, not a product invariant finding. The
runner now performs the existing restart and recovery probe when a held call
observes a crash in generated execution, exact replay, or quiescence. Exact
replay also preserves a recorded logical join when cache state or concurrent
crash timing lets the request itself finish before the hold waiter wins; the
operation is not executed twice and same-namespace work remains isolated.

Focused RED replay first failed at the original crash assertion and then at the
early-completion replay race. The same recorded artifact is GREEN after repair:

```text
TEST_BACKEND=minio \
  ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/security-phase4-seed323-green-3 \
  ZEPPELIN_ADVERSARIAL_PRESERVE=always \
  ZEPPELIN_ADVERSARIAL_REPLAY=target/adversarial/security-phase4-soak/run-1784073608/seed-323 \
  cargo test --release --test adversarial_workload_tests replay_seed \
  -- --ignored --nocapture

replay clean: ops=485 compactions=4 background_compactions=1
test result: ok. 1 passed; 0 failed
```

The permanent integration regression
`replay_preserves_terminal_join_when_hold_candidate_completes_early` forces a
recorded hold whose selector cannot match and uses a mutating upsert with an
authoritative `gen_after == 2` assertion. A second physical execution would
advance the manifest again and fail the test. The regression preserves the
recorded join boundary and passed 1/1.

A capped fresh seed-323 generation also completed with `failed=false`, 520
recorded operations including quiescence, 4 compactions, and no finding. After
the exact-once regression was strengthened, the complete non-ignored harness
target passed 201/201 with 7 ignored campaign entry points. That pass also
exposed an outdated clone fault-test assertion: Phase 4 intentionally retains an
activated bootstrap target after copy failure. The regression now verifies the
retained target remains publicly empty instead of incorrectly requiring its
manifest to be absent.

Two wrapper attempts were cancelled during pre-workload build diagnosis; no
adversarial workload started, so they were not soak executions. The campaign
above is the sole Phase 4 soak. No one-hour or second Phase 4 soak will run.

## G-COMMIT

Commit: pending (`SELF` after completion)

```text
Enforce mandatory filters, field masks and write constraints

Policy-carried constraints compiled in SecurityKernel and applied at
the shared query seam (query/batch/facets/grouping/cursors/explain/
as_of/fetch) and write path (stamping, forbid-set, scoped deletes);
cursors bind policy_version; proptest non-widening property.
```
