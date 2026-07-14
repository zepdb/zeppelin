# Phase 2 evidence

## RED — durable, redacted security audit pipeline

Command on `a0424d9741d24b16d9d07b17b439bfe5b6d3313e`:

```text
TEST_BACKEND=minio cargo test --release --test security_audit_tests -- --nocapture
```

Verbatim failing output:

```text
running 12 tests
test common::vectors::tests::test_clustered_vectors ... ok
test common::vectors::tests::test_random_vectors ... ok
test common::vectors::tests::test_with_attributes ... ok
test common::counting::tests::dedicated_perf_delete_stream_forwards_one_native_batch ... ok
test common::counting::tests::counting_delete_stream_forwards_once_and_preserves_input_order ... ok

thread 'config_update_audits_old_and_new' panicked at tests/security_audit_tests.rs:72:5:
assertion `left == right` failed: expected exactly one audit record for request audit-config-update-exact, got []
  left: 0
 right: 1
test config_update_audits_old_and_new ... FAILED

thread 'namespace_delete_is_must_audit' panicked at tests/security_audit_tests.rs:72:5:
assertion `left == right` failed: expected exactly one audit record for request audit-namespace-delete-exact, got []
  left: 0
 right: 1
test namespace_delete_is_must_audit ... FAILED

thread 'metrics_have_no_principal_labels' panicked at tests/security_audit_tests.rs:329:5:
assertion failed: metrics.contains("zeppelin_auth_failures_total")
test metrics_have_no_principal_labels ... FAILED

thread 'authn_failure_writes_audit_record' panicked at tests/security_audit_tests.rs:72:5:
assertion `left == right` failed: expected exactly one audit record for request audit-authn-failure-exact, got []
  left: 0
 right: 1
test authn_failure_writes_audit_record ... FAILED

thread 'denial_writes_audit_record' panicked at tests/security_audit_tests.rs:72:5:
assertion `left == right` failed: expected exactly one audit record for request audit-denial-exact, got []
  left: 0
 right: 1
test denial_writes_audit_record ... FAILED

thread 'no_secrets_in_audit' panicked at tests/security_audit_tests.rs:253:5:
redaction proof requires audit evidence
test authn_failure_writes_audit_record ... FAILED
test no_secrets_in_audit ... FAILED
test denial_writes_audit_record ... FAILED

thread 'audit_batching_bounds' panicked at tests/security_audit_tests.rs:295:5:
assertion `left == right` failed
  left: 0
 right: 2
test audit_batching_bounds ... FAILED

failures:

failures:
    audit_batching_bounds
    authn_failure_writes_audit_record
    config_update_audits_old_and_new
    denial_writes_audit_record
    metrics_have_no_principal_labels
    namespace_delete_is_must_audit
    no_secrets_in_audit

test result: FAILED. 5 passed; 7 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.51s
```

The failures are behavioral: the Phase 1 HTTP authorization and domain
operations behave as designed, but no `_audit/` objects or Phase 2 metric
families exist.

## RED — decision-owned durable-audit obligation

The implementation audit caught a second seam before Phase 2 was closed:
`AllowDecision.obligations` was still empty and the response barrier was driven
by a duplicate action switch. The exact-inventory test was added first.

Command:

```text
cargo test --release --lib security::decision::tests::phase_two_durable_audit_obligation_inventory_is_exact -- --exact --nocapture
```

Failing compiler evidence:

```text
error[E0061]: this function takes 0 arguments but 1 argument was supplied
   --> src/security/decision.rs:185:17
    |
185 |                 AllowDecision::boot(*action)
    |                 ^^^^^^^^^^^^^^^^^^^ ------- unexpected argument

error: could not compile `zeppelin` (lib test) due to 2 previous errors
```

GREEN changes `AllowDecision::boot(action)` to attach
`Obligation::DurableAudit` for exactly the five Phase 2 actions. The central
response settlement now reads the returned decision obligation; it has no
second must-audit action switch.

## RED → GREEN — timeout after a namespace tombstone

Post-review testing exposed a cancellation boundary: the outer timeout could
return `408` after namespace deletion had mutated authoritative state while
also cancelling the authorization future that owned response-side audit
settlement.

Command:

```text
TEST_BACKEND=minio cargo test --release --test security_audit_tests timeout_after_namespace_tombstone_still_writes_audit_record -- --exact --nocapture
```

RED failed at `tests/security_audit_tests.rs:73`: the request-specific audit
lookup expected exactly one record but found `[]` (`left: 0`, `right: 1`). The
targeted result was 0 passed, 1 failed, 14 filtered out in 1.83s.

GREEN moves the endpoint timeout inside authentication and authorization, so
it still limits domain work while response-side authorization observes and
audits a returned `408`. The same command passed 1 test, with 0 failures and 14
filtered out, in 1.97s. The record identifies `NamespaceDelete`, the exact
namespace, and error code `http_408`.

## RED → GREEN — untrusted vector-delete IDs

The vector-delete handler initially accepted arbitrary and oversized IDs and
projected up to ten of those client-controlled strings into audit parameters.
The regression tuple is `(arbitrary status, oversized status, WAL PUTs,
untrusted payload found in audit)`.

Command:

```text
TEST_BACKEND=minio cargo test --release --test validation_tests test_vector_delete_rejects_untrusted_ids_before_wal_and_audit -- --exact --nocapture
```

RED observed the exact tuple `(204, 204, 2, true)`, versus the required
`(400, 400, 0, false)`. GREEN applies the shared configured vector-ID byte
length and ASCII syntax validation before namespace/storage I/O and before
audit parameter projection; the target then observed exactly
`(400, 400, 0, false)` and passed 1/1. The complete `validation_tests` binary
passed 17/17, and the existing valid delete/fetch regression passed 1/1.

## RED → GREEN — test-server audit lifecycle

Detached test HTTP servers retained audit clients after `TestHarness::cleanup`
removed objects. Their timer could therefore flush a buffered record into
MinIO after cleanup returned.

Command:

```text
TEST_BACKEND=minio cargo test --release --test test_server_audit_lifecycle_tests harness_cleanup_stops_server_before_removing_buffered_audit_objects -- --exact --nocapture
```

RED found one scoped `_audit/...jsonl` object 1.2s after cleanup. GREEN makes
the harness own each helper server's graceful HTTP shutdown, server task, and
audit runtime: cleanup stops and joins them before deleting domain and scoped
audit objects. The target passes against both memory and MinIO, the complete
test binary passed 7/7 at this point, and a request after cleanup fails to
connect.

Final review tightened this lifecycle rather than allowing cleanup failures to
become warnings. Explicit cleanup aggregates server-shutdown, domain-prefix
delete, audit LIST, and every scoped audit DELETE error, attempts all cleanup
steps, then fails the test with the complete error set. The Drop fallback stays
best effort but logs domain and audit cleanup failures. A registered
`TestServerRuntime` preserves both HTTP-task and audit-runtime failures when
they occur together, and `FullTestServer::shutdown` awaits compaction, HTTP,
and audit outcomes before asserting their aggregate.

## RED → GREEN — cleanup scope independent of namespace prefix

One `start_test_server_on_store(..., None)` path used the optional namespace
prefix as its audit cleanup scope. RED found a surviving
`_audit/.../test-node-<uuid>/...` object after harness cleanup because that
node ID did not carry the harness's random prefix.

Target:

```text
TEST_BACKEND=minio cargo test --release --test test_server_audit_lifecycle_tests on_store_server_without_namespace_prefix_still_uses_harness_audit_scope -- --exact --nocapture
```

GREEN always derives helper-server audit scope from `harness.prefix`, while
leaving the independently optional namespace prefix unchanged. The target
passes against memory and MinIO. With this second lifecycle regression added,
the complete MinIO lifecycle binary passes 8/8. The regression narrows listed
keys to its harness-scoped node ID before issuing GETs, so concurrent tests may
create or remove unrelated audit objects without causing a false failure.

## RED → GREEN — production shutdown after a serve error

The initial process path propagated a TCP bind or Axum serve error before it
could drain the audit writer or stop and join background work. A settlement
test was introduced against the missing orchestration seam.

RED command:

```text
cargo test --release --bin zeppelin primary_server_error_still_drains_audit_and_joins_backgrounds -- --exact --nocapture
```

RED failed to compile with E0432 for unresolved
`super::settle_server_and_backgrounds` (followed by E0282), proving the
settlement seam did not exist. GREEN command:

```text
cargo test --release --bin zeppelin tests::primary_server_error_still_drains_audit_and_joins_backgrounds -- --exact --nocapture
```

GREEN passed 1/1. It preserves the primary serve error while proving that one
buffered audit object is drained and the background thread is joined before
the error is returned.

## RED → GREEN — bearer redaction in handler tracing spans

The first real SDK integration run exposed a production tracing leak: the
vector-upsert handler accepted a complete `HeaderMap`, and `#[instrument]`
recorded every unskipped argument on every event in the span. The emitted
`upsert_vectors` span therefore included the complete `Authorization: Bearer`
value. The ephemeral test bearer is intentionally redacted below rather than
being persisted as evidence.

Command:

```text
TEST_BACKEND=minio MINIO_ENDPOINT=http://127.0.0.1:9100 \
  cargo test --release --test observability_tests \
  bearer_secret_is_not_recorded_in_handler_spans -- --exact --nocapture
```

RED captured the authenticated public HTTP upsert and failed with:

```text
bearer material leaked into tracing output:
... upsert_vectors{... headers={..., "authorization":
    "Bearer [REDACTED]", ...}}: ... upserting vectors count=1
test result: FAILED. 0 passed; 1 failed; 16 filtered out
```

GREEN adds the complete header map to the handler span's skip set. The same
real MinIO-backed HTTP regression passed 1/1 with 16 filtered out and proves
that the generated administrator bearer is absent from captured tracing
output.

## Contract decisions

- Healthy namespace deletion retains the established `202 Accepted` contract;
  the phase plan's `200` reference is stale.
- The plan's `src/wal/batch_writer.rs` reference is stale. Phase 2 uses one
  owned Tokio channel/worker with oneshot durability barriers.
- Authentication failures use the existing typed anonymous principal sentinel;
  no presented key identifier or bearer material enters the record.
- `prev_hash` is serialized as explicit `null`; Phase 10 owns chaining and
  signed anchors.
- Enforced mode rejects `security.audit_s3 = false` because tracing alone
  cannot satisfy `must_audit` durability.
- Direct `_audit/` reads in these security regressions use `ZeppelinStore` to
  model legitimate operator evidence inspection; no production layer reaches
  through to `object_store`.

## GREEN

Initial focused schema, sink, lifecycle, and obligation tests, before the
post-review regressions above were added:

```text
cargo test --release --lib security::audit -- --nocapture
running 10 tests
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 451 filtered out

TEST_BACKEND=minio cargo test --release --test security_audit_tests -- --nocapture
running 14 tests
test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

Proven behavior:

- strict typed JSON audit records reserve `prev_hash: null`, expose no generic
  arbitrary-value parameter map, and retain at most ten vector IDs;
- authentication failures and authorization denials enqueue without awaiting
  object storage, while the five decision-owned durable-audit obligations wait
  for immutable S3 evidence before returning success;
- a failed namespace-delete audit PUT returns exact 500 `audit_unavailable`,
  leaves the namespace in retryable `deleting` state, and does not launch
  irreversible cleanup;
- a `408` produced after namespace deletion has tombstoned authoritative state
  is still finalized as an error audit record instead of being cancelled with
  the timed-out endpoint future;
- vector DELETE rejects arbitrary-syntax and oversized IDs before WAL writes or
  audit projection, while the existing valid delete/fetch behavior remains
  green;
- 257 buffered denials produce exactly two immutable JSONL objects, the 2s
  timer flushes partial batches, and graceful shutdown drains a partial batch;
- `TestHarness::cleanup` quiesces registered HTTP and audit tasks before remote
  cleanup, preventing buffered timer flushes from recreating scoped audit
  objects afterward;
- explicit test cleanup attempts and aggregates HTTP/audit shutdown,
  domain-prefix cleanup, audit LIST, and every scoped audit-object DELETE
  failure, while Drop logs failures from its necessarily best-effort fallback;
- full test-server shutdown awaits compaction, HTTP, and audit completion
  before reporting their combined result, including simultaneous failures;
- every helper server derives audit cleanup identity from the harness prefix,
  even when its independent namespace-name prefix is absent;
- a production bind/serve failure remains the returned primary error only
  after audit drain and background-thread shutdown have both been attempted;
- runtime-config update evidence contains exact old/new values and a counting
  store observes the `_audit/` PUT before the success response;
- authn, authz, audit-outcome, and audit-flush-failure counters increment with
  bounded labels and Prometheus text contains no principal label;
- authenticated vector-upsert tracing excludes the complete request header map,
  and a captured public HTTP regression proves bearer material is absent;
- `open_unsafe` boot produces an explicit audit record; tracing-only mode can
  never claim durable success.

Files changed:

- production: `src/config.rs`, `src/main.rs`, `src/metrics.rs`,
  `src/namespace/manager.rs`, `src/runtime_config.rs`,
  `src/security/{audit,audit_sink,decision,kernel,mod}.rs`,
  `src/server/mod.rs`, `src/server/handlers/{config,mod,namespace,vectors}.rs`,
  `src/startup.rs`, `src/storage/store.rs`;
- binary regression context: `src/main.rs` also contains the process-settlement
  test for audit drain and background join after a primary serve failure;
- test infrastructure/regressions: `tests/common/{counting,harness,server}.rs`,
  `tests/common/fault_injection.rs`,
  `tests/{api,attrs_laziness,batch_query,create_by_name,hydration_api,manifest_read_integrity,observability,restore_clone,security_audit,security_boot,startup_fail_fast,test_server_audit_lifecycle,validation,vector_fetch,warm_parity}_tests.rs`,
  `tests/perf_contract/{depth,scenario}.rs`;
- deployment/config: `docs/security-deployment.md`,
  `zeppelin.toml.example`.

Limitations and deferred ownership:

- Phase 10 fills the reserved per-node hash chain and signed daily anchors;
- Phase 5 adds runner oracle `I25AuditEvidence`; no adversarial-runner code is
  changed in this phase;
- SIEM streaming, Object Lock automation, and compliance certification remain
  deployment/non-goals exactly as stated in the phase plan;
- healthy namespace DELETE retains the existing `202 Accepted` contract.

## Gates

- G-RED: original seven behavioral failures preserved above, plus explicit
  supplemental REDs for the decision-owned obligation, timeout cancellation,
  untrusted vector-delete IDs, post-cleanup timer flush, prefix-independent
  cleanup scope, and production error-path settlement seams.
- G-GREEN: focused audit unit and MinIO suites green as shown above.
- G-LIB: fresh `cargo test --release --lib` — 461 passed.
- G-INT: final current-tree combined MinIO run passed 95/95: `api_tests` 25,
  `namespace_lifecycle_tests` 13, `observability_tests` 17,
  `security_audit_tests` 15, `test_server_audit_lifecycle_tests` 8, and
  `validation_tests` 17. The same exact six-binary command ran after the final
  lifecycle failure-aggregation and concurrent-inspection hardening.
- G-MAP: `security_api_tests route_map_complete --exact` — 1 passed; the
  route/action table is unchanged.
- G-PERF: all 19 frozen scenarios passed in
  `target/perf-contract/run-1784016060-536392000-81981/report.md`; secured query
  measured 417ns p50 authn+authz delta with +0 GET and +0 PUT. Each measured
  destructive delete separately asserted exactly one raw `_audit/` PUT while
  audit traffic remained excluded from frozen domain artifact/depth totals.
  No contract TOML changed. `perf_selftest` also passed with report
  `target/perf-contract/run-1784016093-931519000-82244/report.md`.
- G-LINT: the exact `cargo clippy --all-targets -- -D warnings` command passed
  on the current tree in a clean ARM64 Linux Rust 1.93.1 container using
  Docker-managed build volumes. Native macOS attempts were blocked before
  Rust execution by the host security loader: newly generated build scripts
  remained at `_dyld_start` with zero CPU. The same current tree also passed
  `cargo fmt --all -- --check` and `git diff --check HEAD` natively.
- G-COMMIT: one Phase 2 commit below; every message line is at most 70 chars.
- G-ADV/G-SOAK: not applicable; no adversarial files changed and Phase 2 has
  no soak gate.

## Commit

`SELF` — `Add security audit pipeline with S3 sink and must_audit barrier`
