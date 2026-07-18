# Branching release-gate evidence

This is an evidence ledger, not an enablement claim. Branching remains
disabled by default until every required contract below is green.

## Verified slices

| Area | Evidence |
| --- | --- |
| Namespace metadata/lifecycle | `3b14787`, `cb967eb`; `branch_fork_tests` lifecycle and retry cases |
| Artifact origins and physical reads | `4d30aaa`, `4612b7e`; manifest origin tests |
| Branch roots and exact generations | `d608135`, `cb967eb`; root-crash/retry MinIO cases |
| Foreign-branch materialization | `a5f82d0`, `a45c25f`; activated materialization MinIO case and compaction unit suite |
| Graph deletion/root release | `d9d1c71`, `625cfb7`, `e833fc9`, `919d627`, `0d6f693`; source-child blocking, target-drop, pre-tombstone guard, and manifest-tolerant retry coverage |
| Direct-child ordering/list contracts | `33908b0`, `897ef9b`, `e8ecb73`, `fcae0a1`, `0f585a5`, `12cf7d7`, `b8eb8bf`, `b1c61db`; target-order MinIO case, route/OpenAPI parity, and redacted fork response fields including source/target generations, with focused JSON-shape coverage |
| Fork security gates | `a8ac7b8`, `3df5c20`, `0d43b7e`, `720d81e`, `bd65c94`, `b669470`, `a6bfb04`, `94077da`; policy non-widening, branching entitlement, distinct authorization, audited fork events, delegated fork capability, and central route-map checks; focused route/audit/delegation tests passed |
| Adversarial branching vocabulary | `43fc06d`; `BranchingOp` now has stable replay kind, namespace, and actor accessors with coverage for all five planned operations; `cargo test --test adversarial_workload_tests adversarial::ops` passed 5/5 after `b210e0e` |
| Current branching integration gate | `TEST_BACKEND=minio cargo test --features branching-test-support --test branch_fork_tests -- --test-threads=1`; 20 passed in 60.92s on current HEAD |
| Target namespace branch status | `4e2122c`, `922da4b`; namespace metadata now carries only redacted branch ID/mode/depth/lifecycle/health/materialized/created-at fields for branch targets, with focused redaction-shape coverage |
| OpenAPI target branch status | `c421edf`; `NamespaceResponse.branch` and the redacted `BranchStatusDescriptor` schema are now documented in the versioned API contract |
| Branch-list auth contract | `9472471`; branch-list GET now uses the canonical OpenAPI 401/403 response references; contract suite identified and fixed the 401 parity issue |

## Explicitly incomplete

- Clone from an unmaterialized foreign-backed branch still fails closed; a
  complete owned-view builder is required before enabling that path.
- Deletion is not yet the full governed, durable intent/evidence/grace-period
  protocol specified by plan 07.
- Branch HTTP handlers do not yet implement the complete approval, entitlement,
  audit, and per-target disclosure contract from plan 08.
- SDK repositories were not modified because the plan marks them optional and
  cross-repository edits were not authorized in this checkout.
- The complete public HTTP MinIO matrix and adversarial fault matrix from plan
  10 have not yet been run green.
- The harness serialization blocker was fixed in `b210e0e` by recording
  `ZeppelinError` as text in the oracle evidence payload. The two branching
  vocabulary tests now pass, but the full adversarial fault matrix remains
  outstanding.
- A bounded two-seed MinIO release smoke reached workload execution but
  failed in existing model bookkeeping (`tests/adversarial/model.rs:487`): a
  maintenance acknowledgement arrived for an unmodelled generated clone
  namespace. This prevents claiming the full smoke/fault gate green and needs
  a separate harness fix before rerunning.
- After `29d06a3`, the same smoke advances further but seed 0 still fails: the
  clone request returns 500, then a follow-up compact request creates/serves an
  empty target namespace and the model correctly has no clone target to apply
  against. This is now a reproducible clone/compact contract issue, not a
  branching-vocabulary failure; it remains outside the branching slice.

Therefore this ledger must not be treated as a release approval or as evidence
that the branching feature is ready for production enablement.
