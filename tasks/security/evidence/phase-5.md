# Phase 5 evidence

Status: complete. Implementation, exact-tree validation, both independent
review axes, and the sole 1,800-second soak are green.

## RED -> GREEN — security oracle mutation primitives

Command:

```text
cargo test --release --test adversarial_workload_tests \
  adversarial::security_program::tests:: -- --nocapture
```

Verbatim RED summary before the oracle implementations landed:

```text
running 6 tests

allowed forbidden operation must be detected
accepted revoked credential must be detected
plaintext credential material must be detected
missing durable audit record must be detected
quiet-period visible-set drift must be detected
cross-tenant id must be detected

failures:
    adversarial::security_program::tests::i22_rejects_an_allowed_response_for_a_forbidden_operation
    adversarial::security_program::tests::i23_rejects_any_id_outside_the_actor_visible_set
    adversarial::security_program::tests::i24_rejects_a_credential_after_the_refresh_barrier
    adversarial::security_program::tests::i25_rejects_a_success_without_durable_audit_evidence
    adversarial::security_program::tests::i26_rejects_secret_material_in_security_or_audit_objects
    adversarial::security_program::tests::i27_rejects_a_quiet_period_constraint_drop

test result: FAILED. 0 passed; 6 failed; 0 ignored
```

Classification: expected harness-oracle RED. No product finding.

GREEN at the same seam after implementation and the review fixes:

```text
test result: ok. 25 passed; 0 failed; 0 ignored
```

The added focused cases prove ordinary non-admin operations are predicted from
their exact action/scope grants, the execution seam selects the declared actor,
quiet events remain runner-owned even when their logical index collides with a
later op, and ambiguous security mutations retain per-request resolution
evidence without being consumed on a failed reconciliation.

The final spec re-review added two focused RED cases before its fixes:

```text
i22_models_clone_as_compound_unconstrained_authorization
left: Some(Authorized), right: Some(Forbidden)

i22_staleness_accepts_the_authorized_status_family
old authorized policy may return Created during absorption

test result: FAILED. 0 passed; 1 failed (each focused run)
```

They are GREEN on the final tree. Clone now requires unconstrained
`NamespaceClone` and `NamespaceRead` on the source plus unconstrained
`NamespaceCreate` on the target. Grant-change windows retain complete old/new
grant sets and compare authorization classes, not literal `200/403` pairs, so
valid `201`, `202`, and `204` success statuses remain legal during absorption.

The final standards re-review added four more focused RED cases before its
fixes:

```text
i22_models_clone_as_compound_unconstrained_authorization
left: Some(Authorized), right: Some(Forbidden)

i22_composes_overlapping_policy_and_credential_windows
the earlier cached removed-policy state remains legal

i22_export_probe_mirrors_any_surface_observation
left: Some(Forbidden), right: Some(Allow)

policy_model_fails_loudly_for_an_unconfigured_actor
test did not panic as expected
```

They are GREEN on the exact tree. The model now mirrors the production
policy-wide clone no-widening proof across every principal and derived action,
including filter conjuncts, server stamps, forbidden fields, and
`AttributeAdmin` bypass. Every applicable bounded-staleness window contributes
exact reachable whole-policy snapshots, while credential revocation composes
independently. Export expectations match the runner's either-surface
observation, and action/actor lookups use the production typed vocabulary and
fail loud.

The second independent re-review added these focused RED seams before its
fixes:

```text
i22_composes_same_actor_policy_transitions
both independently absorbed removals can leave no export surface authorized

i22_never_mixes_actors_from_different_whole_policy_versions
left: StalenessWindow [Authorized, Forbidden], right: Forbidden

i22_export_probe_mirrors_any_surface_observation
left: Allow, right: Authorized

policy_model_validates_every_configured_action_eagerly
test did not panic as expected

policy_model_rejects_duplicate_actor_selectors
test did not panic as expected

staleness_windows_use_the_configured_logical_bound
left: StalenessWindow [Allow, Unauthorized], right: Unauthorized

staleness_window_bound_overflow_fails_loudly
test did not panic as expected

i22_authorizes_security_ops_from_the_acting_principals_grants
left: Allow, right: Forbidden

grant_identity_uses_a_canonical_typed_action_set
assertion failed for the same reversed action set
```

They are GREEN on the regenerated tree. Definite policy publications advance
every reachable whole-policy branch; ambiguous publications retain both the
not-applied and applied whole-policy branches. This composes same-principal
changes without inventing cross-principal states that never shared an
authoritative version. The model now consumes the configured logical bound via
checked arithmetic, authorizes security reads/writes from the acting
principal, canonicalizes grant identity through typed action sets, rejects
duplicate actors and invalid action tails, and treats an authorized missing
export snapshot's downstream `404` as non-authz rather than a denial.

## G-GREEN / G-ADV — pinned oracle matrix

Each new mutation ran with a clean control and a mutated run capped at 80
workload ops (I27 completes its quiet-period check after the 80-op workload):

| mutation | sole pinned invariant | failing workload op |
|---|---|---:|
| `grant-model-desync` | `I22AuthzDecision` | 37 |
| `leaked-id-suppression` | `I23TenantLeak` | 22 |
| `revocation-misclassification` | `I24RevocationFreshness` | 80 |
| `audit-record-deletion` | `I25AuditEvidence` | 80 |
| `security-secret-leak` | `I26SecurityStateSanity` | 80 |
| `constraint-drop` | `I27ConstraintDrop` | 100 including quiet records |

The consolidated 22-mutation matrix passed:

```text
/usr/bin/time -p env \
  ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/phase5-matrix-final-rereview-4 \
  cargo test --release --test adversarial_workload_tests \
  oracle_selftest -- --ignored --nocapture

test result: ok. 1 passed; 0 failed; 0 ignored
real 6.15
```

Classification: expected harness-oracle mutations only. No product finding.

## G-INT — mixed and security profiles

Two-seed mixed smoke:

```text
TEST_BACKEND=minio ZEPPELIN_ADVERSARIAL_MODE=mixed \
  ZEPPELIN_ADVERSARIAL_SEEDS=0,1 \
  ZEPPELIN_ADVERSARIAL_SECONDS=240 \
  ZEPPELIN_ADVERSARIAL_MAX_OPS=400 \
  ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/phase5-mixed-final-rereview-4 \
  cargo test --release --test adversarial_workload_tests \
  smoke -- --ignored --nocapture

adversarial smoke: seeds=2 ops=860 compactions=52 \
background_compactions=0 failed=0 non_blocking_findings=0
test result: ok. 1 passed; 0 failed
```

Two-seed security smoke after final fault choreography:

```text
TEST_BACKEND=minio ZEPPELIN_ADVERSARIAL_PROFILE=security \
  ZEPPELIN_ADVERSARIAL_SEEDS=0,1 \
  ZEPPELIN_ADVERSARIAL_SECONDS=240 \
  ZEPPELIN_ADVERSARIAL_MAX_OPS=400 \
  ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/security-phase5-final-rereview-4 \
  cargo test --release --test adversarial_workload_tests \
  smoke -- --ignored --nocapture

adversarial smoke: seeds=2 ops=860 compactions=12 \
background_compactions=0 failed=0 non_blocking_findings=0
test result: ok. 1 passed; 0 failed
```

Artifact `target/adversarial/security-phase5-final-rereview-4/run-1784086807`
records all four `SupportedV1` boundaries for both seeds, with no violated
assumptions: process crash at op 20, client response loss at op 30, clock jump
at op 32, and policy-head GET failure at op 38. The process crash is at the
pre-CAS boundary after the immutable policy PUT. The current actor-4
credential created at op 31 expires after the `+120s` jump and is rejected at
op 35 with `401 credential_expired`; quiet refresh then reconciles both
ambiguous grant writes from the checksum-verified authoritative snapshot.
Each seed's `resolutions.json` now classifies both writes independently: op 20
is `not_applied` with no durable audit after the pre-CAS crash, while op 30 is
`not_applied` from its exact durable `error` audit at policy version 20; both
records bind the authoritative policy version 21. A successful ambiguous
mutation additionally requires typed `SecurityPolicyChange` old/new version
lineage before it can be classified `applied`.
`config.json` contains five
non-admin principals, all ten security operation kinds, and the four protected
assumptions; timeline events carry actor and decision fields; the report has an
Authorization Summary plus nonzero I22-I27 scenario coverage.

Legacy artifact replay, using a Phase 4 artifact with no `security_program`
field:

```text
TEST_BACKEND=minio \
  ZEPPELIN_ADVERSARIAL_REPLAY=target/adversarial/security-phase4-gadv-final/run-1784056542/seed-0 \
  cargo test --release --test adversarial_workload_tests \
  replay_seed -- --ignored --nocapture

replay clean: ... ops=130 compactions=21 background_compactions=0
test result: ok. 1 passed; 0 failed
```

## Harness regression gate

```text
CARGO_INCREMENTAL=0 cargo test --release \
  --test adversarial_workload_tests -- --nocapture

test result: ok. 231 passed; 0 failed; 7 ignored
```

## G-LIB / G-PERF / G-LINT on the post-audit-fix tree

```text
CARGO_INCREMENTAL=0 cargo test --release --lib
test result: ok. 502 passed; 0 failed

CARGO_INCREMENTAL=0 cargo test --release --test perf_contract_tests
test result: ok. 114 passed; 0 failed; 28 ignored

TEST_BACKEND=minio ZEPPELIN_PERF_SCENARIOS=secured_filtered_query \
  ZEPPELIN_PERF_ARTIFACTS=target/perf-contract/security-phase5-final-rereview-4 \
  cargo test --release --test perf_contract_tests contracts \
  -- --ignored --exact --nocapture

secured_filtered_query security budget: p50_delta_ns=680
query_regression_bps=Some(0) added_get_ops=0 added_put_ops=0
test result: ok. 1 passed; 0 failed

CARGO_INCREMENTAL=0 cargo clippy --release --all-targets -- -D warnings
Finished `release` profile

cargo fmt --all -- --check
git diff --check
both clean
```

The standing contract report is
`target/perf-contract/security-phase5-final-rereview-4/run-1784086858-219870000-78701/report.md`.
No performance contract or tolerance changed. The only production `src/`
diffs are the two explicit test-hook seams in `security/kernel.rs` and
`security/policy_cache.rs` (17 added/changed lines total).

## Independent review audit

The final spec reviewer returned **No findings** after verifying atomic
whole-policy branches, same-actor transition composition, configured checked
staleness bounds, ExportProbe downstream-404 handling, eager config/action
validation, acting-principal security authorization, canonical grant identity,
and all previously closed clone/I25/quiet/ambiguity findings.

The final standards/architecture reviewer independently returned **No
findings** on those seams plus policy-wide clone no-widening and the regenerated
artifact/evidence consistency. Neither reviewer edited files, ran tests, or ran
a soak.

## Soak preflight

The first soak command was rejected before runner startup because the env file
used `ZEPPELIN_ADVERSARIAL_SEEDS=0`. A scalar value is a seed count, so the
runner failed loud with `ZEPPELIN_ADVERSARIAL_SEEDS count must be > 0`; it
executed zero workload operations and created no `security-30m` artifact. This
is classified as a harness-configuration preflight failure, not an executed
soak. The env now uses `1`, meaning one emitted seed (`0`).

## Sole 1,800-second soak

The soak ran exactly once after implementation, validation, review fixes, and
both final independent review audits were green:

```text
test ! -d target/adversarial/security-30m &&
set -a &&
source tasks/security/adversarial_security_1h.env &&
set +a &&
test "$ZEPPELIN_ADVERSARIAL_SECONDS" = 1800 &&
test "$ZEPPELIN_ADVERSARIAL_SEEDS" = 1 &&
test -z "${ZEPPELIN_ADVERSARIAL_MAX_OPS:-}" &&
cargo test --release --test adversarial_workload_tests \
  overnight -- --ignored --nocapture

adversarial overnight: seeds=438 ops=229946 compactions=2198 \
background_compactions=957 failed=0 ops/sec=127.68
test result: ok. 1 passed; 0 failed
finished in 1814.23s
```

The authoritative report is
`target/adversarial/security-30m/run-1784087336/report.md`. It records
`budget_s=1800`, all 438 seeds as passed, `research_findings=0`, and no
violations. The persisted security-operation counts are all above the required
floor of five: `create_key=876`, `export_probe=438`,
`forbidden_write_probe=1314`, `publish_grant_change=876`, `revoke_key=438`,
`rotate_key=438`, `security_admin_probe=438`, `tenant_boundary_probe=3942`, and
`use_revoked_credential=1314`. Security-oracle coverage is nonzero for every
new invariant: I22=10,512, I23=4,380, I24=1,752, I25=876, I26=438, and I27=438.

The runner normalizes the scalar seed count `1` to the emitted configured seed
list `[0]`; the report then records the continuously rotated seeds 0 through
437. No second soak was run.

## Commit gate

- The Phase 5 commit remains.
