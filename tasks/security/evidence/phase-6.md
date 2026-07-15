# Phase 6 evidence

Status: implementation, exact-tree validation, final independent re-review, and
the sole 1,800-second soak are green. Phase 6 is ready to commit.

## G-RED -> G-GREEN

The phase suite was written before the entitlement vocabulary existed. Its first
public-module compile RED was preserved:

```text
cargo test --release --test security_license_tests

error[E0432]: unresolved imports `zeppelin::security::Entitlements`,
`zeppelin::security::Feature`
```

No verbatim output survived from one later intermediate compile attempt, so this
file does not reconstruct or paraphrase it. Independent review then drove these
preserved behavioral REDs at the affected public seams:

```text
route_map_complete
registered route /v1/security/principals must use the central security method wrapper

route_map_complete
registered route /v1/security/keys/:key_id must use the central security method wrapper

expired_signed_license_boot_exports_metric_and_audit_record
assertion failed: zeppelin::metrics::LICENSE_EXPIRY_SECONDS.get() < 0

release_builds_cannot_select_test_license_authority
panicked: test-support must not be a Cargo-selectable production feature

community_enforced_boot_rejects_zero_or_expired_credentials
panicked: community enforced boot must reject zero credentials
```

The final full library run also exposed a process-global gauge race between
parallel startup tests:

```text
startup::tests::expired_file_boot_exports_metric_and_durable_audit ... FAILED
assertion failed: crate::metrics::LICENSE_EXPIRY_SECONDS.get() < 0
test result: FAILED. 508 passed; 1 failed
```

All `build_app` and direct expiry-observer unit tests now share one async test
lock and shut down the owned observer before releasing it. The complete focused
suite is green against the required real MinIO backend:

```text
mc mb --ignore-existing \
  zeppelin-minio/zeppelin-security-phase6-license-suite

TEST_BACKEND=minio \
TEST_S3_BUCKET=zeppelin-security-phase6-license-suite \
cargo test --release --test security_license_tests -- --nocapture
test result: ok. 20 passed; 0 failed

mc rb --force \
  zeppelin-minio/zeppelin-security-phase6-license-suite
```

It covers community data-plane authn/authz, mounted management stubs, tamper and
wrong-key boot failure, strict parsing, expiry grace/freeze, enforcement and
constraints after expiry, an authenticated post-expiry RBAC denial that remains
`403 forbidden`, feature-required policy loading, composition-only gating,
release-authority source guards, parser fuzz/no-panic, and signing-tool
non-clobber behavior.

## Independent-audit fixes

The implementation incorporates every finding from the prior spec and standards
reviews:

- community composition uses bootstrap credentials and never constructs the
  licensed S3 policy registry;
- enforced community boot rejects zero or only-expired bootstrap credentials;
- `SecurityKernel::from_resolved_entitlements` is the composition seam, while
  the store-specific constructor and startup resolver injector are private;
- request processing asks the boot-composed audit sink whether it provides
  durability instead of checking `Feature::AuditS3` per request;
- every security route remains centrally wrapped while only mutation routers
  receive the expiry-freeze layer;
- the daily observer is owned by `BackgroundTasks`, receives shutdown, and is
  joined;
- the signing tool stages with create-new semantics, rejects aliases/existing
  output, verifies before publish, and uses a non-clobbering atomic hard link;
- community and licensed contract output is deterministic after dynamic key
  identifiers are normalized; and
- the expiry gauge tests serialize every in-process writer.

Release code has no unchecked constructor, Cargo-selectable test feature,
arbitrary-key verifier, public resolver injector, or reusable signed license
fixture. Broad integration tests construct the private entitlement value only
inside `tests/common/server.rs`: an exact `repr(C)` mirror, fixed enum
representation, size/alignment assertions, and one ownership-preserving unsafe
move. That code is compiled into test binaries, not the library or server. The
release-safety test also rejects production-key license fixtures because an
already signed all-features document would itself be transferable. Positive
signed startup stays under `cfg(test)`, using the permitted unit-test key.

## G-INT / G-MAP / contracts

Current-tree local results:

```text
security_policy_counting_tests: 11 passed
security_bootstrap_authority_tests: 13 passed
security_boot_tests: 17 passed
startup_fail_fast_tests: 3 passed
contract_tests: 14 passed
```

Current-tree MinIO API and policy regressions used a dedicated general bucket:

```text
mc mb --ignore-existing \
  zeppelin-minio/zeppelin-security-phase6-final-regression

TEST_BACKEND=minio \
TEST_S3_BUCKET=zeppelin-security-phase6-final-regression \
cargo test --release --test security_api_tests -- --nocapture
test result: ok. 25 passed; 0 failed

TEST_BACKEND=minio \
TEST_S3_BUCKET=zeppelin-security-phase6-final-regression \
cargo test --release --test security_policy_tests -- --nocapture
test result: ok. 32 passed; 0 failed

mc rb --force \
  zeppelin-minio/zeppelin-security-phase6-final-regression
```

The real signed-file startup graph used a second bucket so its root-level
`_security/` and `_audit/` cleanup could not collide with general fixtures:

```text
mc mb --ignore-existing zeppelin-minio/zeppelin-license-phase6-final

TEST_BACKEND=minio \
ZEPPELIN_LICENSE_TEST_BUCKET=zeppelin-license-phase6-final \
cargo test --release --lib \
  startup::tests::licensed_file_boot_enables_rbac_routes \
  -- --exact --nocapture
test result: ok. 1 passed; 508 filtered out

TEST_BACKEND=minio \
ZEPPELIN_LICENSE_TEST_BUCKET=zeppelin-license-phase6-final \
cargo test --release --lib \
  startup::tests::expired_file_boot_exports_metric_and_durable_audit \
  -- --exact --nocapture
test result: ok. 1 passed; 508 filtered out

mc rb --force zeppelin-minio/zeppelin-license-phase6-final
```

The counting gate remains zero object-store operations per warmed request, one
conditional head GET per refresh window, and one immutable snapshot PUT plus one
CAS head PUT per policy publication. Route completeness and real-engine contract
generation validate the mounted `feature_not_licensed` and `license_expired`
envelopes plus OpenAPI bearer coverage.

## G-LIB / release composition / tooling

```text
cargo test --release --lib
test result: ok. 509 passed; 0 failed

cargo check --release --all-features --all-targets
Finished `release` profile

cargo build --release --all-features \
  --bin zeppelin --bin zeppelin_license
Finished `release` profile

cargo rustc --release --all-features --lib -- --print cfg
feature="default"
feature="managed"
feature="profiling"
```

The all-features release cfg contains neither `test-support` nor `cfg(test)`.
`ControlPlaneResolver` compiles behind `managed` and fails loud with the typed
unimplemented error. No private signing key is present.

`zeppelin_license` supplies strict `sign` and `verify` commands. Positive
issuance requires the private key corresponding to the embedded public key;
that key is intentionally unavailable in the repository.

## G-ADV smoke

The runner was not modified. Its exact current-tree two-seed fully licensed
smoke used the test-binary-only composition fixture:

```text
TEST_BACKEND=minio \
TEST_S3_BUCKET=zeppelin-security-phase6-final-smoke \
ZEPPELIN_ADVERSARIAL_PROFILE=security \
ZEPPELIN_ADVERSARIAL_SEEDS=0,1 \
ZEPPELIN_ADVERSARIAL_SECONDS=240 \
ZEPPELIN_ADVERSARIAL_MAX_OPS=400 \
ZEPPELIN_ADVERSARIAL_ARTIFACTS=\
target/adversarial/security-phase6-safe-test-composition-smoke \
cargo test --release --test adversarial_workload_tests \
  smoke -- --ignored --exact --nocapture

adversarial smoke: seeds=2 ops=860 compactions=12 \
background_compactions=6 failed=0 non_blocking_findings=0
test result: ok. 1 passed; 0 failed
```

Report:
`target/adversarial/security-phase6-safe-test-composition-smoke/run-1784100639/report.md`.

## G-PERF

No contract or tolerance changed. Unit validation is 114 passed with 28
explicit environment gates ignored. The exact MinIO contract used a third
bucket and the test-binary-only composition fixture:

```text
TEST_BACKEND=minio \
TEST_S3_BUCKET=zeppelin-security-phase6-final-perf \
ZEPPELIN_PERF_SCENARIOS=secured_filtered_query \
ZEPPELIN_PERF_ARTIFACTS=\
target/perf-contract/security-phase6-safe-test-composition \
cargo test --release --test perf_contract_tests contracts \
  -- --ignored --exact --nocapture

p50_delta_ns=677 query_regression_bps=Some(0)
added_get_ops=0 added_put_ops=0
test result: ok. 1 passed; 0 failed
```

Report:
`target/perf-contract/security-phase6-safe-test-composition/run-1784100597-540533000-9951/report.md`.

## G-LINT

```text
cargo clippy --release --all-targets --all-features -- -D warnings
cargo fmt --check
git diff --check
```

All three are clean on the current tree.

## Final independent re-review

Two independent read-only re-reviews completed after the named MinIO suite and
authenticated post-expiry RBAC denial were added:

```text
Spec review: No findings.
Standards review: No findings.
```

No files were edited and no tests were run by either reviewer.

## Sole 1,800-second soak

The sole Phase 6 soak ran exactly once, after implementation, validation,
fixes, and both final independent re-reviews were green:

```text
TEST_BACKEND=minio \
TEST_S3_BUCKET=zeppelin-security-phase6-soak \
ZEPPELIN_ADVERSARIAL_MODE=chaos \
ZEPPELIN_ADVERSARIAL_PROFILE=security \
ZEPPELIN_ADVERSARIAL_SEEDS=1 \
ZEPPELIN_ADVERSARIAL_SECONDS=1800 \
ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/security-phase6-30m \
cargo test --release --test adversarial_workload_tests \
  overnight -- --ignored --exact --nocapture

adversarial overnight: seeds=409 ops=214741 compactions=2056 \
background_compactions=1048 failed=0 ops/sec=119.25
test result: ok. 1 passed; 0 failed
```

The report records `budget_s=1800`, `failed=0`, and
`research_findings=0`: `target/adversarial/security-phase6-30m/
run-1784101439/report.md`. The dedicated MinIO bucket was removed. This soak
must not be rerun.

## Files changed

Production and contract surface: `Cargo.toml`, `Cargo.lock`,
`api/zeppelin-api.yaml`, `contract/fixtures/v0.3.0/*`, `src/error.rs`,
`src/metrics.rs`, `src/main.rs`, `src/security/{audit.rs,audit_sink.rs,
entitlements.rs,kernel.rs,license.rs,mod.rs,policy.rs,policy_store.rs}`,
`src/server/mod.rs`, `src/startup.rs`, and `src/bin/zeppelin_license.rs`.

Test/harness surface: `tests/common/server.rs`, `tests/contract_tests.rs`,
`tests/perf_contract/security.rs`,
`tests/security_boot_tests.rs`, `tests/security_bootstrap_authority_tests.rs`,
`tests/security_license_tests.rs`, `tests/security_policy_counting_tests.rs`,
`tests/security_policy_tests.rs`, and `tests/startup_fail_fast_tests.rs`.

## Commit gate

All gates are green. The required one-phase commit message is recorded in
`tasks/security/phase-6-entitlements.md`.
