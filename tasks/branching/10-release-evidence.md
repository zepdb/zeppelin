# Branching release-gate evidence

This is an evidence ledger, not an enablement claim. Branching remains disabled
by default until every required contract in plan 10 is green. The deletion
unification baseline recorded here is `937f84d` (`Guard ungoverned deletes and
legacy resume`). Slice 10 is still in progress and is therefore identified by
its planned commit subject, not by an uncommitted or provisional hash.

## Deletion-unification slices

| Slice | Scope | History/evidence | Status |
| --- | --- | --- | --- |
| 1 | Strong preservation probe and incarnation-bound head proof | `52fd1b2` (`Add strong preservation head probe`), `c1bd80b`, and `a903b64` | Implemented in current history; focused strong-probe coverage landed, but no durable exact command/result was found for this ledger update |
| 2 | Graph-owned governed root deletion, trailing intent ordering, fencing, governance envelope, and resumable primitives | Preparatory implementation series from `293ca35` through `8a33d45`, including `96295c5` (graph authorization envelope), `9c13bc8` (preservation boundary), `14ca4d4` (resume seam), `8da9ea8` (trailing intent), and `8a33d45` (evidence-bound lifecycle audit) | Implemented in current history; there is no single commit with the planned subject `Route governed root deletion through graph` |
| 3 | HTTP, request cleanup, and background cleanup switched to the graph-owned governance seam | `22c26fd` (`Switch namespace deletion to graph governance`) | Implemented; commit records deterministic CAS-race and lifecycle fixture coverage, but no durable exact command/result was found |
| 4 | Durable visibility removal marker and reader-safety grace | `e47afaf` (`Persist branch visibility removal and grace`) | Implemented with restart/config-change grace handling and typed grace-wait progress |
| 5 | Exact root release, acknowledgement, convergence, and parent-incarnation integrity | `5d7a828` (`Release branch roots after durable grace`) | Implemented; commit records deterministic MinIO crash, lost-reply, absence, replacement-incarnation, and metadata-last coverage |
| 6 | Budgeted deletion recovery from `NamespaceGraph::maintain` | `75ab434` (`Resume deletion intents in graph maintain`) | Implemented; commit records recovery-matrix, fault-injection, and production background-loop coverage |
| 7 | Governed cancellation of never-active forks | `19fcbf8` (`Cancel never-active forks through graph`) | Implemented with parent-lease ordering, fail-closed proof, crash recovery, and maintenance regressions |
| 8 | Origin-checked owned cleanup and foreign-artifact protection | `5969d9c` (`Enforce owned-key cleanup for branch deletes`) | Implemented in current history; exact validation output was not found in a durable evidence file |
| 9 | Ungoverned-path child-root guard and legacy/downgrade compatibility | `937f84d` (`Guard ungoverned deletes and legacy resume`) | Implemented with legacy intent/destruction-record compatibility and fail-closed downgrade documentation |
| 10 | Disclosure-filtered list/409 details, root-release audit progress, and adversarial deletion bookkeeping | In progress: `Wire disclosure, audit, adversarial deletes` | Not complete and not release evidence; the required two-seed smoke and full plan 07/08 validation lists are not yet recorded green |

The Slice 2 entry deliberately describes the actual linear history. The plan
required one commit named `Route governed root deletion through graph`, but no
such commit exists in the current history; assigning an unrelated hash to that
subject would overstate traceability.

## Earlier branching evidence preserved

These records predate deletion unification unless stated otherwise. They remain
useful focused evidence, but they are not a substitute for rerunning plan 10 on
the final Slice 10 commit.

| Area | Evidence |
| --- | --- |
| Namespace metadata/lifecycle | `3b14787`, `cb967eb`; `branch_fork_tests` lifecycle and retry cases |
| Artifact origins and physical reads | `4d30aaa`, `4612b7e`; manifest origin tests |
| Branch roots and exact generations | `d608135`, `cb967eb`; root-crash/retry MinIO cases |
| Foreign-branch materialization | `a5f82d0`, `a45c25f`; activated materialization MinIO case and compaction unit suite |
| Earlier graph deletion/root release | `d9d1c71`, `625cfb7`, `e833fc9`, `919d627`, `0d6f693`; source-child blocking, target-drop, pre-tombstone guard, and manifest-tolerant retry coverage, now superseded by the unified deletion path above |
| Direct-child ordering/list contracts | `33908b0`, `897ef9b`, `e8ecb73`, `fcae0a1`, `0f585a5`, `12cf7d7`, `b8eb8bf`, `b1c61db`; target-order MinIO case, route/OpenAPI parity, and redacted fork response fields including source/target generations, with focused JSON-shape coverage |
| Fork security gates | `a8ac7b8`, `3df5c20`, `0d43b7e`, `720d81e`, `bd65c94`, `b669470`, `a6bfb04`, `94077da`; policy non-widening, branching entitlement, distinct authorization, audited fork events, delegated fork capability, and central route-map checks; focused route/audit/delegation tests passed |
| Adversarial branching vocabulary | `43fc06d`; `BranchingOp` has stable replay kind, namespace, and actor accessors for all five planned operations; `cargo test --test adversarial_workload_tests adversarial::ops` passed 5/5 after `b210e0e` |
| Recorded branching integration gate | `TEST_BACKEND=minio cargo test --features branching-test-support --test branch_fork_tests -- --test-threads=1`; 20 passed in 60.92s on the then-current pre-unification HEAD |
| Target namespace branch status | `4e2122c`, `922da4b`; namespace metadata carries only redacted branch ID/mode/depth/lifecycle/health/materialized/created-at fields for branch targets, with focused redaction-shape coverage |
| OpenAPI target branch status | `c421edf`; `NamespaceResponse.branch` and the redacted `BranchStatusDescriptor` schema are documented in the versioned API contract |
| Branch auth contract | `9472471`, `6d97f9e`; branch-list GET and branch-create POST use canonical OpenAPI auth/error response references; `cargo test --test contract_tests openapi_documents_bearer_security_for_every_protected_operation -- --exact` passed 1/1 |
| OpenAPI route parity | `cargo test --test contract_tests openapi_documents_exact_routed_surface -- --exact` passed 1/1 after the branch contract additions |

## Current validation status

- The exact Slice 10 RED was reproduced against `937f84d` with:

  ```bash
  cargo test --lib \
    server::tests::phase_eight_audited_action_inventory_is_exact \
    -- --exact
  ```

  It fails because the runtime audited-action inventory contains
  `NamespaceFork` while the expected inventory omits it. This is RED evidence,
  not a green gate. The durable must-audit inventory must also require
  `NamespaceFork` before Slice 10 can claim audit completeness.

- No post-Slice-9 run of the complete plan 07 validation list, plan 08
  validation list, branching integration gate, or plan 10 release matrix was
  found in a durable evidence file. Those commands must be rerun and recorded
  against the final Slice 10 commit.
- No new Cargo command was run while preparing this ledger update.

## Historical adversarial findings preserved

- The harness serialization blocker was fixed in `b210e0e` by recording
  `ZeppelinError` as text in the oracle evidence payload. The two branching
  vocabulary tests passed, but the full adversarial fault matrix remained
  outstanding.
- A bounded two-seed MinIO release smoke reached workload execution but failed
  in then-existing model bookkeeping (`tests/adversarial/model.rs:487`): a
  maintenance acknowledgement arrived for an unmodelled generated clone
  namespace.
- After `29d06a3`, the same smoke advanced further but seed 0 still failed: the
  clone request returned 500, then a follow-up compact request created/served an
  empty target namespace and the model had no clone target to apply against.
  This recorded a reproducible clone/compact contract issue, not a
  branching-vocabulary failure. Neither historical smoke is evidence for the
  final branching profile required by plan 10.

## Remaining original Phase 10 blockers

- **Production activation/security governance:** public fork handling prepares a
  target but has no production policy-publication lease, activation guard or
  nonce, activation evidence, or activation-time no-widening recheck. Test
  support activation is not a production integration.
- **Slice 10 completion:** disclosure-denied children must be absent from list
  results and 409 names/counts/details; root-release progress must be fully
  represented in durable audit parameters; adversarial `DeleteBranch` and
  `DeleteSourceWithBranches` must model the legitimate 409-until-grace window.
- **Clone/materialization contract:** clone from an unmaterialized
  foreign-backed branch still fails closed; the owned-view builder and accurate
  persisted/public materialization status are incomplete.
- **Public HTTP/security matrix:** the complete real-MinIO branch API,
  authorization, disclosure, entitlement, no-widening, audit, preservation, and
  restart matrix has not been recorded green. Planned test targets
  `branching_tests`, `branch_compaction_tests`, `branch_api_tests`, and
  `security_branching_tests` do not exist under those names on this baseline;
  the final report must use the actual targets created by the implementation.
- **Adversarial/fault matrix:** branching operations are not yet wired through a
  complete state model, invariant oracle, remote-mutation/lost-ack fault matrix,
  branching profile, retained replay, and deterministic two-seed smoke.
- **Performance:** no frozen tiny/corpus-scale fork scenario proves zero artifact
  copying, corpus-size-independent fork cost, ancestry-read behavior, or full
  materialization cost.
- **Formal/recall/release evidence:** the invariant traceability table, current
  default TLA+ runs, all twelve intended-negative model variants, both-dataset
  IVF recall gate, full release validation matrix, and artifact-backed results
  are outstanding.
- **Operational enablement:** stalled `Creating`/`Deleting` and root-count
  readiness/metrics, explicit config/entitlement instructions, source-delete
  blocking, clone alternative, materialization cost, child/depth limits, and
  the explicit “fork only; no merge” documentation remain incomplete.
- **Final gates:** independent standards/spec reviews and the single 1,800-second
  branching soak must happen only after every focused, formal, adversarial,
  performance, recall, and validation gate is green.
- Optional SDK repositories remain out of scope because phase 09 was not
  separately authorized in this checkout.

Therefore this ledger must not be treated as release approval or as evidence
that branching is ready for production enablement.
