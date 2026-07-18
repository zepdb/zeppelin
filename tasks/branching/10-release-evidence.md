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
| Graph deletion/root release | `d9d1c71`, `625cfb7`; source-child blocking and target-drop MinIO cases |
| Direct-child ordering/list contracts | `33908b0`, `897ef9b`, `e8ecb73`; target-order MinIO case and route/OpenAPI parity |
| Fork security gates | `a8ac7b8`, `3df5c20`, `0d43b7e`; policy non-widening and central route-map checks |

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

Therefore this ledger must not be treated as a release approval or as evidence
that the branching feature is ready for production enablement.
