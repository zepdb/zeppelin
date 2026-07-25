# Branching release-gate evidence

This is an implementation and evidence ledger, not an enablement claim.
Branching remains disabled by default until every required release contract in
plan 10 is green.

All hashes below are commits on `main` and were re-derived after the
2026-07-24 commit-message reflow. Verify any of them with
`git merge-base --is-ancestor <hash> main`. The previous revision of this file
cited hashes from the pre-rebase `codex/branching` branch, none of which were
ancestors of `main`; anyone holding only `main` could not resolve a single one.

## Phase landmarks

| Phase | Scope | Commit on `main` |
| --- | --- | --- |
| 1 | Namespace branching lifecycle and GC model | `475d5cf`; correction `63cbb32` |
| 2 | Manifest artifact origins | `03bd4f9` |
| 3 | Physical read routing through origins | `93ef3af` |
| 4 | Branch roots and exact generations | `fc5e34b` |
| 5 | Resumable fork preparation lifecycle | `fb333ac` |
| 6 | Writable branch materialization | `eadbd04` |
| 7 | Branch-aware deletion | initial checkpoint `7295141`, completed through deletion unification ending at `ae5627c` |
| 8 | HTTP surface and activation governance | `b520ca3`, `936b82c`, `637fa6c`, `8bace74`; activation checkpoint `25fb950`, then recovery hardening |
| 9 | Client SDKs | **Not implemented.** `d39e954` is an intentionally empty marker |
| 10 | Integration, adversarial, performance | `ad77b75` |

## Deletion-unification slices

| Slice | Scope | Commit(s) on `main` | Status |
| --- | --- | --- | --- |
| 1 | Strong preservation probe and incarnation-bound head proof | `ca3c6b3`, `553314b`, `616fe78`; later proof `2029bd0` | Implemented |
| 2 | Graph-owned governed root deletion, trailing intent ordering, fencing, governance envelope, resumable primitives | `3999936` through `8aaa1cc`, inclusive — 24 commits | Implemented |
| 3 | HTTP, request cleanup, and background cleanup on the graph seam | `e232818` | Implemented |
| 4 | Durable visibility-removal marker and reader-safety grace | `d0b791c` | Implemented |
| 5 | Exact root release, acknowledgement, convergence, parent-incarnation integrity | `8406625` | Implemented |
| 6 | Budgeted deletion recovery from `NamespaceGraph::maintain` | `44d754f` | Implemented |
| 7 | Governed cancellation of never-active forks | `522eb56` | Implemented; invariant regressed and restored — see below |
| 8 | Origin-checked owned cleanup and foreign-artifact protection | `6c850f5`; later proof `cfae399` | Implemented |
| 9 | Ungoverned-path child-root guard and legacy/downgrade compatibility | `d4c79cd` | Implemented |
| 10 | Disclosure-filtered list/409 details, root-release audit progress, adversarial deletion bookkeeping, recovery readiness | `e4e85b6` through `ae5627c` | Implemented |

The Slice 2 entry deliberately describes the actual linear history. The plan
required one commit named `Route governed root deletion through graph`; no such
commit exists, and assigning an unrelated hash to that subject would overstate
traceability.

## First execution of the branching gate — 2026-07-24

The MinIO branching gate had never been executed before this pass. Running it
found three failures, two of which had been red since the commit that
introduced them.

```bash
TEST_BACKEND=minio cargo test --features branching-test-support \
  --test artifact_origin_tests --test branch_root_tests \
  --test branch_fork_tests --test branch_deletion_tests \
  --test branching_tests --no-fail-fast -- --test-threads=1
```

`--no-fail-fast` is required. Without it cargo stops at the first failing
binary, which is part of why these went unnoticed.

### Findings

1. **Never-active cancellation authority regressed.** `25fe84e` broadened the
   `maintain()` resume condition to every `Creating` namespace holding a
   deletion intent. That shadowed the Slice 7 guard, which became unreachable
   dead code, and unattended maintenance began completing cancellations that
   must await a freshly authorized request. `25fe84e` added no coverage for the
   broadened behavior and broke
   `branch_fork_tests::maintenance_reports_but_never_executes_an_authorized_cancellation_intent`.
   Fixed in `4f8583c` by resuming only intents carrying the activation nonce —
   the exact state whose retained policy guard maintenance must release.
   `maintenance_resumes_an_activation_cancelled_fork` now covers the nonce path
   that had none.
2. **`branching_tests::owned_clone_survives_materialization_and_deletion_of_its_branch_ancestry`
   had never run.** Added by `eadbd04`, it failed production
   config validation before reaching any branching work, leaving copy clone over
   a materialized branch unverified. Once running it exposed a second gap: the
   bounded deletion loop rejected retryable 409s, though governed deletion
   legitimately loses the namespace-lease and policy-head races to background
   maintenance. Both fixed in `5821c5f`.
3. **Stale owned-key assertion.** `artifact_origin_tests.rs` still expected the
   message text that Slice 8 replaced when `TargetOwnedDeletionKey::classify`
   began delegating to `NamespaceObjectKey::classify`. The guard itself was
   never broken. Fixed in `5821c5f`.

### Result after the fixes

| Suite | Result |
| --- | --- |
| `cargo test --lib` | 638 passed, 0 failed (was 635 / 3) |
| `artifact_origin_tests` | 17 passed, 0 failed |
| `branch_root_tests` | 15 passed, 0 failed |
| `branch_fork_tests` | 39 passed, 0 failed |
| `branch_deletion_tests` | 31 passed, 0 failed |
| `branching_tests` | 16 passed, 0 failed |
| `security_branching_tests` | 12 passed, 0 failed |
| `contract_tests` | 16 passed, 0 failed |
| `namespace_lifecycle_tests` | 19 passed, 0 failed |
| `security_preservation_tests` | 25 passed, 0 failed |
| `storage_gc_tests` | 77 passed, 0 failed |

Also clean: `cargo check --tests --features branching-test-support`,
`cargo fmt --all -- --check`, `git diff --check`. Clippy introduced no new
lints, verified by diffing against a clean `HEAD` worktree.

## Collateral defects fixed in the same pass

- **`cargo test --lib` was red on `main`.** `object_store` 0.11.2
  `LocalFileSystem::put_opts` answers `PutMode::Update` with `NotImplemented`
  (`local.rs:369`), so `StorageBackend::Local` cannot perform ETag CAS.
  Branching's `PolicyPublicationLease` release requires it and
  `PolicyStore::bootstrap` acquires that lease, so three cases failed —
  including `startup::licensed_file_boot_enables_rbac_routes`, which predates
  branching. The design's per-slice validation floor is `cargo test --lib`, so
  that floor had not been met. Fixed in `5821c5f`.
- **`/readyz` scanned the namespace graph on every probe.** `inspect_readiness`
  ran unbudgeted regardless of `branching.enabled`: one root listing plus a
  metadata *and* manifest read per namespace, plus a metadata read per branch
  root, with any propagated error returning 503. Readiness now answers from a
  snapshot published by the already budgeted maintenance pass, and performs no
  object-store work of its own (`4f8583c`).
  `branching_tests::readiness_probes_never_scan_the_namespace_graph` proves it:
  the same test fails against unmodified `HEAD` with five root listings — one
  per probe — and passes with zero listings, zero metadata reads, and zero
  manifest reads.
- **Branch listing did not require its entitlement.** `authorize_branch_list`
  had no `Feature::Branching` check, unlike fork and both activation paths, so
  an unlicensed deployment with `enabled = true` received 200s from a licensed
  route. Fixed in `4f8583c`.

## Open defects, not fixed

- **Concurrent policy writes exceed the request timeout.**
  `security_policy_tests::policy_cas_conflict_second_writer_retries` and
  `policy_cas_conflict_storm_is_bounded_and_retryable` fail on clean `HEAD`
  (confirmed in an unmodified worktree, so not a regression from this pass).
  Two concurrent grants return **408** instead of 201:
  `acquire_claimed_publication` calls `publication_lease.acquire()` with no
  retry, and `acquire()` fails immediately while the lease is held and
  unexpired, so the second writer blocks past the server request timeout. This
  is fallout from the global policy-publication lease introduced by `e5d9b36`.
  Choosing between a bounded wait and a fail-fast 409 carrying `Retry-After` is
  a design decision and was left open.
- **`cargo clippy --all-targets -- -D warnings` is red on `main`** with 58
  errors, predating this pass. Plan 10 lists it as a gate; it has never passed.

## Earlier branching evidence preserved

These records predate deletion unification unless stated otherwise. They are
useful focused evidence, not a substitute for the plan 10 gates.

| Area | Evidence |
| --- | --- |
| Namespace metadata/lifecycle | `475d5cf`, `fb333ac`; `branch_fork_tests` lifecycle and retry cases |
| Artifact origins and physical reads | `03bd4f9`, `93ef3af`; manifest origin tests |
| Branch roots and exact generations | `fc5e34b`, `fb333ac`; root-crash/retry MinIO cases |
| Direct-child ordering/list contracts | route/OpenAPI parity and redacted fork response fields including source/target generations |
| Fork security gates | policy non-widening, branching entitlement, distinct authorization, audited fork events, delegated fork capability, central route-map checks |
| Adversarial branching vocabulary | `BranchingOp` exposes stable replay kind, namespace, and actor accessors for all five planned operations |
| Target namespace branch status | namespace metadata carries only redacted branch ID/mode/depth/lifecycle/health/materialized/created-at fields |
| OpenAPI parity | `openapi_documents_exact_routed_surface` and `openapi_documents_bearer_security_for_every_protected_operation` pass |

## Phase 10 implementation closure

- **Production governance:** fork activation uses the persisted activation
  nonce, global policy-publication lease, policy-head CAS guard, durable
  activation evidence, activation-time no-widening reauthorization, and guarded
  cancellation/recovery paths.
- **Deletion and disclosure:** filtered child disclosure, root-release audit
  progress, grace-aware deletion bookkeeping, and recovery readiness are wired.
- **Writable materialization and clone:** `eadbd04` builds an exact
  authenticated logical view into target-owned artifacts, preserves concurrent
  target WAL, makes no-WAL manual materialization work, and supports copy clone
  from a foreign-backed branch. Verified end to end for the first time in this
  pass.
- **Adversarial harness:** the stable branching profile generates fork, list,
  divergent source/target writes and queries, compact, branch delete, and
  blocked source delete, with model tracking for incarnations, roots,
  generation, depth, materialization, lifecycle, grace, restart, foreign
  deletes, and the no-merge invariant.
- **Performance contract:** frozen tiny and one-million-logical-row fork census
  scenarios exist but remain `#[ignore]`d and unrun.
- **Operations:** `/readyz` exposes aggregate stalled Creating/Deleting intent
  counts and total roots from the published snapshot, Prometheus exports only
  bounded branch-state labels, and the operator guide documents config plus
  entitlement activation, source-delete blocking, copy clone, materialization
  cost, limits, readiness refresh cadence, and "fork only; no merge."

## Release gates still not claimed

None of the following ran in this pass or any earlier one:

- the named remote-mutation/lost-acknowledgement fault campaign;
- the deterministic two-seed MinIO smoke and recorded-artifact replay;
- the performance-contract branching census (`#[ignore]`d, MinIO-only), so
  zero-artifact-copy and corpus-size independence remain unproven by execution;
- the both-dataset IVF recall gate;
- TLA variant reruns;
- independent standards and spec reviews;
- the 1,800-second soak.

Branching therefore remains default-disabled, and explicit configuration plus a
valid branching entitlement are still required for route exposure. Optional SDK
work stays out of scope because phase 09 was never authorized in this checkout.

**This ledger is not release approval and must not be read as evidence that
branching is ready for production enablement.**
