# Phase 3 evidence

## RED -> GREEN — S3-authoritative bootstrap

Target:

```text
TEST_BACKEND=minio cargo test --release --test security_policy_tests bootstrap_publishes_v1_from_config -- --exact --nocapture
```

RED failed to compile with E0432 because `PolicyHead` and `PolicySnapshot`
did not exist. GREEN passed 1/1 on MinIO and proved version 1 head/snapshot
reachability, canonical checksum validity, digest-only bootstrap credentials,
and absence of the plaintext secret from the immutable snapshot.

```text
error[E0432]: unresolved imports `zeppelin::security::PolicyHead`, `zeppelin::security::PolicySnapshot`
 --> tests/security_policy_tests.rs:4:26
...
error: could not compile `zeppelin` (test "security_policy_tests") due to 1 previous error
```

After the types existed, the public storage seam remained RED:

```text
thread 'bootstrap_publishes_v1_from_config' ... panicked at tests/common/server.rs:905:14:
test security policy must load or bootstrap: NotFound { key: "test-.../_security/heads/policy.json" }
test bootstrap_publishes_v1_from_config ... FAILED
test result: FAILED. 0 passed; 1 failed; ... 6 filtered out
```

## RED -> GREEN — boot-config drift after bootstrap

Target:

```text
TEST_BACKEND=minio cargo test --release --test security_policy_tests s3_policy_supersedes_drifted_boot_config -- --exact --nocapture
```

RED returned 401 for the S3-authoritative credential on a second node whose
boot config had drifted. GREEN passed 1/1: both authentication and
authorization share the S3-backed compiled policy; the authoritative bearer
succeeds and the drifted config bearer returns `credential_unknown`.

```text
thread 's3_policy_supersedes_drifted_boot_config' ...:
assertion `left == right` failed
  left: 401
 right: 200
test s3_policy_supersedes_drifted_boot_config ... FAILED
```

The response-envelope assertion then exposed the missing normalized reason:

```text
assertion `left == right` failed
  left: Null
 right: "credential_unknown"
```

## RED -> GREEN — stale policy fails closed

Target:

```text
TEST_BACKEND=minio cargo test --release --test security_policy_tests stale_policy_fails_closed -- --exact --nocapture
```

RED preceded the policy cache and fault seam. GREEN passed 1/1 with a 1s
refresh interval: after two injected head-GET failures the protected request
returned 403 `security_stale`, the durable audit record carried policy version
1 and reason `security_stale`, and service recovered after the fault was
disabled.

```text
thread 'stale_policy_fails_closed' ...:
assertion `left == right` failed: stale case missing
  left: 401
 right: 403
test stale_policy_fails_closed ... FAILED
```

## RED -> GREEN — one-time API-key issuance

Target:

```text
TEST_BACKEND=minio cargo test --release --test security_policy_tests create_key_returns_secret_once_digest_stored -- --exact --nocapture
```

RED returned 404 from `POST /v1/security/principals`. The first GREEN run
reached an authenticated 403 but exposed a test expectation that bypassed the
existing public-reason redaction (`forbidden`, not the internal
`action_not_granted`). With that contract preserved, GREEN passed 1/1 on
MinIO: principal version 2 and key version 3 were CAS-published, the generated
credential authenticated immediately, list responses excluded secret
material, and the active snapshot contained exactly its SHA-256 digest.

```text
thread 'create_key_returns_secret_once_digest_stored' ...:
assertion `left == right` failed
  left: 404
 right: 201
test create_key_returns_secret_once_digest_stored ... FAILED
```

The public-reason compatibility RED was:

```text
assertion `left == right` failed
  left: String("forbidden")
 right: "action_not_granted"
```

## RED -> GREEN — bounded revocation propagation

Target:

```text
TEST_BACKEND=minio cargo test --release --test security_policy_tests revoked_key_denied_within_bound -- --exact --nocapture
```

RED compiled and ran against MinIO, then failed at the revocation response:
`DELETE /v1/security/keys/:key_id` returned 404 instead of 200. The test first
proves the issued credential authenticates on both the writer and a second
read-only node, then requires immediate same-node `credential_unknown` and
second-node enforcement within exactly two 1s refresh windows.

```text
thread 'revoked_key_denied_within_bound' ...:
assertion `left == right` failed
  left: 404
 right: 200
test revoked_key_denied_within_bound ... FAILED
```

GREEN passed 1/1. The mutating node installs the CAS-selected snapshot before
returning; the second node observes it through the conditional refresh loop and
denies within the 2x interval. A separately captured pre-revocation `Principal`
also fails authorization after write-through, proving authorization revalidates
the exact credential ID rather than trusting captured identity state.

Two adversarial-clock regressions then pinned cases hidden by the ordinary
writer/reader test. Before the fixes, the skewed-reader test exhausted its
two-second bound with this exact failure:

```text
explicit revoke remained clock-dependent after the refresh bound
```

An immediate revoke now persists `revokes_at: null`, while only a scheduled
rotation overlap persists `Some(deadline)`. The same focused MinIO test passes
with the reader clock one hour behind the writer. A second RED showed an actor
captured before its overlap expired successfully publishing a later policy
version after the test clock advanced. `PolicyCache` now owns the trusted clock
and samples it after each authoritative base load, so the retry fails with
`CredentialUnknown` and publishes no candidate.

```text
thread 'mutation_reauthorization_uses_fresh_clock_after_overlap_expiry' ...:
expired overlap credential must not publish a later mutation:
(AllowDecision { ... policy_version: PolicyVersion(2) ... },
 PolicyVersion(3),
 PolicyPrincipal { principal_id: PrincipalId("service:stale-retry"), ... })
test mutation_reauthorization_uses_fresh_clock_after_overlap_expiry ... FAILED
```

The final oracle also requires the pre-revoke 403 body to be exactly
`forbidden`, proving authentication succeeded, and checks the monotonic
deadline before accepting a propagated 401. A late `credential_unknown`
therefore cannot false-pass the two-window contract.

## RED -> GREEN — authoritative freshness origins

Initial-load target:

```text
TEST_BACKEND=minio cargo test --release --test security_policy_tests delayed_initial_snapshot_load_does_not_reset_freshness_origin -- --exact --nocapture
```

RED loaded a snapshot only after a 2.25-second delay, then failed with the exact
panic `a snapshot loaded beyond the 2x refresh bound must fail closed` (`0
passed; 1 failed`). GREEN passed 1/1 in 2.48s after `LoadedPolicy` began carrying
the monotonic instant captured immediately after the authoritative head GET.

```text
thread 'delayed_initial_snapshot_load_does_not_reset_freshness_origin' ...:
a snapshot loaded beyond the 2x refresh bound must fail closed
test delayed_initial_snapshot_load_does_not_reset_freshness_origin ... FAILED
test result: FAILED. 0 passed; 1 failed; ... 26 filtered out
```

Write-through target:

```text
cargo test --release --lib delayed_write_through_install_uses_cas_completion_origin -- --nocapture
```

The pure no-storage RED compared the installed freshness origin with an aged
CAS-completion instant and failed because the left side was the later
`Instant::now()`. GREEN passed 1/1 (`465 filtered out`) after production
write-through used `loaded.observed_at()`. Bootstrap, changed refresh, startup,
and publication now carry the same observation-origin rule through parse,
checksum verification, compilation, and lock acquisition.

```text
thread 'security::policy_cache::tests::delayed_write_through_install_uses_cas_completion_origin' ...:
assertion `left == right` failed
  left: Instant { tv_sec: 4078190, tv_nsec: 160898750 }
 right: Instant { tv_sec: 4078190, tv_nsec: 35857041 }
test security::policy_cache::tests::delayed_write_through_install_uses_cas_completion_origin ... FAILED
```

## RED -> GREEN — key rotation and emergency overlap revocation

Targets:

```text
TEST_BACKEND=minio cargo test --release --test security_policy_tests rotation_overlap_semantics -- --exact --nocapture
TEST_BACKEND=minio cargo test --release --test security_policy_tests rotation_positive_overlap_accepts_old_key_only_until_deadline -- --exact --nocapture
TEST_BACKEND=minio cargo test --release --test security_policy_tests rotation_overlap_predecessor_can_be_revoked_immediately -- --exact --nocapture
```

The zero-overlap and positive-overlap cases passed on MinIO after the rotation
route and atomic lineage transition landed. The emergency-revoke test then
produced a separate RED: DELETE returned 400 instead of the exact expected 200
because a predecessor with a future `revokes_at` was treated as already
revoked. GREEN passed 1/1 after distinguishing scheduled from effective
revocation: DELETE replaces a pending deadline with the clock-independent
immediate sentinel `None`, old authentication immediately returns 401
`credential_unknown`, the replacement remains valid, and policy version 3 is
published.

```text
thread 'rotation_overlap_semantics' ...:
assertion `left == right` failed
  left: 404
 right: 201
test rotation_overlap_semantics ... FAILED
```

```text
thread 'rotation_overlap_predecessor_can_be_revoked_immediately' ...:
assertion `left == right` failed
  left: 400
 right: 200
test rotation_overlap_predecessor_can_be_revoked_immediately ... FAILED
```

## RED -> GREEN — namespace RBAC and disjoint security administration

Targets:

```text
TEST_BACKEND=minio cargo test --release --test security_policy_tests namespace_rbac_grants -- --exact --nocapture
TEST_BACKEND=minio cargo test --release --test security_policy_tests security_admin_disjoint -- --exact --nocapture
```

GREEN passed both exact tests. Namespace-scoped query authority does not cross
namespaces, query does not imply upsert, and upsert does not imply namespace,
snapshot, or vector delete. A principal granted every Phase-3 data-plane
action but neither security admin action receives 403 from all security
mutation methods. Persisted `All` is compile-time frozen to the 21-action
Phase-3 universe so later enum growth cannot silently widen old snapshots.

```text
thread 'namespace_rbac_grants' ...:
assertion `left == right` failed
  left: 404
 right: 201
test namespace_rbac_grants ... FAILED
```

The first handler/body integration attempt also exposed this exact RED:

```text
thread 'namespace_rbac_grants' ...:
assertion `left == right` failed
  left: 400
 right: 200
```

The independent pre-commit evidence audit found that the
`security_admin_disjoint` RED output had not been preserved. A temporary
negative control widened every active grant to security administration; the
exact test failed at its first mutation, and the widening was reverted before
the full GREEN suite:

```text
thread 'security_admin_disjoint' ...:
assertion `left == right` failed: mutation path /v1/security/principals
  left: 201
 right: 403
test security_admin_disjoint ... FAILED
```

## RED -> GREEN — CAS serialization, retry bound, and audit versions

Targets:

```text
TEST_BACKEND=minio cargo test --release --test security_policy_tests policy_cas_conflict_second_writer_retries -- --exact --nocapture
TEST_BACKEND=minio cargo test --release --test security_policy_tests policy_cas_conflict_storm_is_bounded_and_retryable -- --exact --nocapture
TEST_BACKEND=minio cargo test --release --test security_policy_tests security_admin_events_use_actual_policy_versions -- --exact --nocapture
```

GREEN passed all three. A deterministic two-writer barrier observes exactly one
head precondition loss; both requests succeed after retry as versions 4 and 5,
and every referenced immutable snapshot remains reachable and checksum-valid.
Five forced CAS losses stop at the fixed retry bound, return retryable 409 with
`Retry-After: 1`, and leave the head unchanged. Mutation audit records carry the
fresh S3 base used for reauthorization and the actually selected new version,
not a stale middleware decision.

The independent evidence audit recovered the missing RED proof by temporarily
setting the bounded CAS-attempt count to one. The synchronized second writer
then failed exactly at the retry contract; the attempt count was restored to
five before GREEN:

```text
thread 'policy_cas_conflict_second_writer_retries' ...:
assertion `left == right` failed
  left: 409
 right: 201
test policy_cas_conflict_second_writer_retries ... FAILED
```

## RED -> GREEN — regressing authoritative heads fail loudly

The independent code audit found that a lower S3 head version was warned and
ignored rather than returned through the explicit refresh-failure path. RED
added a regression at the exact cache installation seam:

```text
error[E0599]: no function or associated item named `install_refreshed_into`
found for struct `policy_cache::PolicyCache`
test result: could not compile `zeppelin` (lib test)
```

GREEN returns `InvalidPolicy` with both versions, retains the last verified
snapshot without renewing `last_confirmed`, and therefore reaches the existing
error log plus bounded stale-deny behavior:

```text
cargo test --release --lib security::policy_cache::tests::regressing_authoritative_refresh_is_rejected -- --exact --nocapture
test security::policy_cache::tests::regressing_authoritative_refresh_is_rejected ... ok
```

## RED -> GREEN — exact audit decision ownership

Authentication first lacked an atomic result/version/freshness outcome:

```text
error[E0599]: no method named `authenticate_with_policy` found for struct `ApiKeyAdapter`
  --> tests/security_authn_tests.rs:63:29
```

The first interleaving and staleness regressions then captured the wrong
decision context:

```text
thread 'authn_failure_audits_the_snapshot_evaluated_before_policy_advances' ...:
assertion `left == right` failed
  left: Number(2)
 right: 1
```

```text
thread 'authentication_fails_closed_when_the_evaluated_snapshot_is_stale' ...:
assertion `left == right` failed
  left: 401
 right: 403
```

Both security-route rate-limit layers initially returned before audit:

```text
expected one audit record for request security-ip-rate-rejected, got []
  left: 0
 right: 1

expected one audit record for request security-principal-rate-rejected, got []
  left: 0
 right: 1
```

Fresh mutation authorization also lost the handler-time policy version on
both deny and post-allow build errors:

```text
thread 'security_admin_mutation_audits_fresh_authoritative_denial' ...:
  left: Number(1)
 right: 2
```

```text
thread 'security_admin_mutation_audits_latest_allow_before_build_error' ...:
  left: Number(1)
 right: 2
```

GREEN uses typed `SecurityOperationError` propagation and an atomic authn
outcome. It passed audit 19/19, authn 6/6, policy 32/32, and rate limiting
14/14. Cache-swap read denial and CAS retry-load error regressions additionally
prove the audit record owns the latest actual decision rather than a stale
middleware copy.

## GREEN — strict admin lifecycle and reserved-root preservation

Additional exact MinIO tests passed for malformed key IDs (400 with no policy
change), redacted grant list/delete version transitions, and namespace deletion
preserving every `_security/` and `_audit/` object:

```text
malformed_key_path_is_400_without_policy_change
grant_list_and_delete_publish_exact_versions
policy_survives_namespace_delete
```

The independent evidence audit recovered the missing reserved-root RED proof
by temporarily deleting `_security/` after namespace deletion. The exact test
detected the loss byte-for-byte; the negative control was then removed:

```text
thread 'policy_survives_namespace_delete' ...:
assertion `left == right` failed
  left: []
 right: ["_security/heads/policy.json", "_security/policies/<ulid>.json"]
test policy_survives_namespace_delete ... FAILED
```

## RED -> GREEN — S3 bootstrap remains authoritative

Suite:

```text
TEST_BACKEND=minio cargo test --release --test security_bootstrap_authority_tests -- --nocapture
```

RED cases showed startup validation rejecting an empty or expired bootstrap
list before consulting an existing S3 head:

```text
thread 'authoritative_policy_allows_enforced_restart_without_bootstrap_keys' ...:
S3 authority makes bootstrap credentials optional after first boot:
Config("invalid configuration:
- security.api_keys must contain at least one usable key when security.mode is enforced")
```

```text
thread 'first_boot_rejects_only_expired_bootstrap_credentials' ...:
first boot must not publish a policy with no usable credential:
LoadedPolicy { head: PolicyHead { version: PolicyVersion(1), ... },
snapshot: PolicySnapshot { ... state: Expired, ... } }
```

The structured warning seam was independently RED:

```text
thread 'existing_policy_ignores_expired_drifted_config_and_warns_redacted' ...:
ignored bootstrap drift must emit a structured warning
```

The deterministic two-writer first-boot race began at this compile RED:

```text
error[E0432]: unresolved import `common::fault_injection::synchronize_create_pair_matching`
  --> tests/security_bootstrap_authority_tests.rs:15:5
...
no `synchronize_create_pair_matching` in `common::fault_injection`
```

GREEN passed 13/13: an existing
verified S3 policy permits enforced restart without config keys, first boot
without one usable credential creates no objects and fails loudly, concurrent
bootstrap losers read the winner after exactly one create-only conflict, and
config drift emits only redacted counts. The log capture also passed 25
consecutive parallel full-binary runs after moving from a racing thread-scoped
callsite subscriber to one process-default subscriber with exact field matching.

## RED -> GREEN — principal-keyed rate limits and test-store isolation

RED compile command:

```text
cargo test --release --test security_rate_limit_tests two_principals_on_one_ip_have_independent_primary_buckets --no-run
```

It failed with E0609 for the four absent principal read/write RPS and burst
knobs. A subsequent RED returned 401 instead of 201 while provisioning a
second test principal and exposed that own-harness helpers shared raw MinIO
`_security/` state. The repair prefixes only each helper's security authority;
domain storage and externally supplied stores retain their exact semantics.

```text
error[E0609]: no field `principal_rate_limit_rps` on type `ServerConfig`
error[E0609]: no field `principal_rate_limit_burst` on type `ServerConfig`
error[E0609]: no field `principal_write_rate_limit_rps` on type `ServerConfig`
error[E0609]: no field `principal_write_rate_limit_burst` on type `ServerConfig`
error: could not compile `zeppelin` (test "security_rate_limit_tests") due to 4 previous errors
```

```text
thread 'two_keys_for_one_principal_share_one_primary_bucket' ...:
assertion `left == right` failed
  left: 401
 right: 201
```

GREEN:

```text
TEST_BACKEND=minio cargo test --release --test security_rate_limit_tests -- --nocapture
test result: ok. 14 passed; 0 failed

TEST_BACKEND=minio cargo test --release --test batch_query_tests batch_query_rate_limit_counts_entries -- --exact --nocapture
test result: ok. 1 passed; 0 failed

TEST_BACKEND=minio cargo test --release --test rate_limiting_tests -- --nocapture --test-threads=1
test result: ok. 11 passed; 0 failed
```

Two principals on one IP have independent primary buckets, two keys for one
principal share a bucket, and the secondary IP cap bounds their aggregate.
Batch entries charge both dimensions without reparsing transport headers;
failed authentication remains IP-limited.

## G-PERF — counting-store proof

```text
TEST_BACKEND=minio cargo test --release --test security_policy_counting_tests -- --nocapture
test result: ok. 11 passed; 0 failed
```

After warm-up and a counter reset, the authentication-plus-authorization
module seam reported this exact table:

```text
warmed_authn_authz observed_get=0 observed_head=0 observed_put=0 domain_get=0 domain_put=0
```

The observer totals include `_security/` control-plane keys, while the domain
totals deliberately exclude them; asserting both prevents either accounting
view from hiding request-path storage traffic.

A prefixed custom store initially misclassified the control-plane segments:

```text
thread 'common::counting::tests::scoped_control_plane_keys_remain_outside_domain_cost_totals' ...:
assertion failed: is_audit_key("test-scope/_audit/day/node/object.jsonl")
test common::counting::tests::scoped_control_plane_keys_remain_outside_domain_cost_totals ... FAILED
```

Two unchanged conditional head observations were separated by at least the
complete one-second refresh window and fetched zero policy objects. A successful
principal mutation produced this exact reset-before/after table:

```text
key_class                         before_get before_head before_put after_get after_head after_put create_put cas_put
_security/heads/policy.json       0          0           0          1         0          1         0          1
_security/policies/<ulid>.json    0          0           0          1         0          1         1          0
```

Raw per-key counters retain this control-plane traffic, while frozen domain
artifact totals remain zero. No perf-contract tolerance file changed.

The first full frozen-contract run exposed a multi-node harness credential
regression rather than a product budget failure:

```text
assertion `left == right` failed: query failed for ...-cold_query_strong:
{"code":"credential_unknown","error":"authentication required",..."status":401}
  left: 401
 right: 200
test contracts ... FAILED
```

Cold, hydrated, repeated-cold, and closed-loop server restarts now reuse the
first node's authoritative administrator bearer. The final 19-scenario run and
the clean control plus all three injected perf self-tests passed:

```text
secured_query security budget: mode=enforced credential=api_key p50_delta_ns=438 added_get_ops=0 added_put_ops=0
performance-contract report: target/perf-contract/security-phase3/run-1784032333-077703000-54937/report.md
test contracts ... ok
test perf_selftest ... ok
```

`git diff --exit-code -- tests/perf_contract/contracts` exited 0. No tolerance,
profile, or contract TOML changed.

## RED -> GREEN — refresh concurrency (Loom)

RED:

```text
RUSTFLAGS="--cfg loom" cargo test --manifest-path loom-tests/Cargo.toml --test security_policy_cache delayed_changed_snapshot_load_cannot_reopen_past_freshness_bound -- --nocapture
```

The delayed changed-snapshot model returned `Allow` where `DenyStale` was
required: the cache stamped install time after a newer revoke became visible.

```text
assertion `left == right` failed
  left: Allow
 right: DenyStale
```

GREEN carries the monotonic timestamp captured immediately after the
conditional head GET through both refresh outcomes. The full Loom suite passed
7/7: the existing lease/cache models passed 3/3 and the security-policy cache
models passed 4/4, covering atomic swaps, stale unchanged reads, delayed changed
loads, and delayed write-through. The added write-through model first failed to
compile with `E0061` because the install seam did not accept a
`cas_completed_at`; GREEN retains that instant across arbitrary installation
delay and stale-denies past the two-window bound.

```text
error[E0061]: this method takes 2 arguments but 3 arguments were supplied
   --> tests/security_policy_cache.rs:266:30
266 | writer_cache.install(2, false, cas_completed_at);
```

## GREEN — strict policy deserialization fuzzing

The `policy_json` target deserializes arbitrary `PolicyHead`,
`PolicyPrincipal`, `PolicyKey`, and `PolicySnapshot` bytes, injects an unknown
field after every successful parse and requires rejection, verifies checksums,
and compiles checksum-valid policies. Four real JSON corpus seeds cover every
record family.

```text
cargo check --manifest-path fuzz/Cargo.toml --bin policy_json
ASAN_OPTIONS=detect_leaks=0 fuzz/target/aarch64-apple-darwin/release/policy_json -runs=10000 fuzz/corpus/policy_json
```

Both commands exited 0; the 10,000-run ASAN pass found no crash.

## GREEN — SecurityPolicy TLA+

`SecurityPolicy.tla` models immutable publication, expected-ETag CAS, refresh,
authorization, and revocation. Positive TLC explored 1,746,864 generated states
(298,991 distinct, depth 14) with no invariant violation. The existing
`MultiWriterLease` model also remained green (487,660 generated / 151,505
distinct, depth 26). Both negative controls are wired into
`scripts/run-july-tla.sh`: `SecurityPolicy.no-cas.cfg` violated
`CasHeadVersionNeverRegresses` after 78 generated / 66 distinct states at
depth 4, and `SecurityPolicy.stale-open.cfg` violated
`NoAllowPastVisibleRevocationBound` after 5,844 generated / 2,891 distinct
states at depth 6. The full July script exited 0 with `All July TLA checks
matched their expected result.`

## G-MAP and engine-emitted contracts

The OpenAPI document parses with all internal references resolved and contains
exactly the ten routed `/v1/security/*` method operations. Tagged grant schemas,
one-time create/rotate secrets, redacted repeated reads, policy metadata, and
400/404/409 canonical errors are pinned by ten request/response fixture pairs.

```text
TEST_BACKEND=minio cargo test --release --test contract_tests -- --nocapture
test result: ok. 13 passed; 0 failed
```

Every one of the ten security operations also advertises the actual 429
middleware outcome. The contract was RED before those references landed:

```text
assertion `left == right` failed: unexpected statuses for get /v1/security/principals
  left: {200, 401, 403}
 right: {200, 401, 403, 429}
```

Fixture regeneration and a compare-only rerun both passed against the real
engine. The route/fixture set equality assertion prevents either surface from
drifting independently. The route completeness test initially omitted the bare
`delete(security_handler::revoke_key)` registration because its source parser
recognized only chained `.delete(...)` calls. After reproducing the exact
missing pair `DELETE /v1/security/keys/:key_id`, the parser was tightened to
recognize both forms; router completeness passed 1/1, the action inventory
passed 1/1, and the route-map unit set passed 4/4. A fixture scan found no
environment or user secrets; create/rotate responses intentionally carry only
the fixed deterministic contract credentials.

## Consolidated Phase-3 MinIO suites

```text
TEST_BACKEND=minio cargo test --release \
  --test security_policy_tests \
  --test security_bootstrap_authority_tests \
  --test security_policy_counting_tests \
  --test security_rate_limit_tests \
  --test security_api_tests \
  --test security_audit_tests \
  --test security_authn_tests \
  --test security_boot_tests \
  --test security_kernel_tests \
  --test multi_writer_lease_tests \
  --test namespace_lifecycle_tests \
  --test api_tests \
  --test contract_tests \
  --test attrs_laziness_tests \
  --test batch_query_tests \
  --test hydration_api_tests \
  --test warm_parity_tests -- --nocapture
```

The product/security-tree run passed 269/269 across 17 binaries:

```text
api=27 attrs_laziness=18 batch_query=12 contract=13 hydration_api=13
multi_writer_lease=20 namespace_lifecycle=15 security_api=25
security_audit=19 security_authn=6 security_boot=16
security_bootstrap_authority=13 security_kernel=2
security_policy_counting=11 security_policy=32 security_rate_limit=14
warm_parity=13
```

## G-LIB

```text
cargo test --release --lib
test result: ok. 467 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## G-ADV

Both `tests/adversarial/runner.rs` and `tests/adversarial/faults/mod.rs`
changed for the shared enforced-security harness, so the master plan's full
conditional gate applied instead of smoke-only. After both soak-discovered
harness repairs, the complete adversarial test binary passed 201 tests with 7
ignored:

```text
TEST_BACKEND=minio cargo test --release --test adversarial_workload_tests
test result: ok. 201 passed; 0 failed; 7 ignored
```

The final repaired-tree conditional gates were:

```text
cargo test --release --test adversarial_workload_tests oracle_selftest -- --ignored --nocapture
test oracle_selftest ... ok

TEST_BACKEND=minio ZEPPELIN_ADVERSARIAL_SECONDS=180 \
  ZEPPELIN_ADVERSARIAL_MODE=deterministic \
  ZEPPELIN_ADVERSARIAL_SEEDS=0,2 \
  ZEPPELIN_ADVERSARIAL_MAX_OPS=100 \
  ZEPPELIN_ADVERSARIAL_PRESERVE=always \
  ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/security-phase3-gadv \
  cargo test --release --test adversarial_workload_tests smoke -- --ignored --nocapture
seed 0: failed=false ops=130 compactions=21
seed 2: failed=false ops=120 compactions=16
adversarial smoke: seeds=2 ops=250 compactions=37 failed=0 non_blocking_findings=0

TEST_BACKEND=minio \
  ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/security-phase3-replay \
  ZEPPELIN_ADVERSARIAL_PRESERVE=always \
  ZEPPELIN_ADVERSARIAL_REPLAY=target/adversarial/run-1783919745/seed-83 \
  cargo test --release --test adversarial_workload_tests replay_seed -- --ignored --nocapture
replay clean: dir=target/adversarial/run-1783919745/seed-83 ops=530 compactions=47 background_compactions=0
```

Fresh artifacts are under
`target/adversarial/security-phase3-gadv/run-1784043727` and
`target/adversarial/security-phase3-replay/run-1784043737`.

## G-LINT

```text
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo clippy --all-targets -- -D warnings
cargo fmt --check
git diff --check
git diff --cached --check
```

All four exited 0 on the repaired source/test tree. A parallel clippy attempt
was interrupted after two duplicate library metadata checks went idle with
zero CPU; the single-job all-target rerun completed normally in 17m10s. The
lightweight format and diff checks were rerun after this evidence update.

## G-SOAK

```text
scripts/overnight-adversarial.sh 3600
```

The first one-hour run, `target/adversarial/run-1784033378`, correctly failed
with this summary:

```text
Summary: seeds=317, failed=1, research_findings=0, ops=166104,
explicit_compactions=4606, background_compactions=1838, ops/sec=46.71
```

Seed 242 operation 439 triggered I11: the exact-error
`weights-len-mismatch` probe expected 400 `VALIDATION_ERROR` but received the
canonical 503 `CONCURRENCY_LIMIT`. Product admission ordering was intentional:
a pending held query owned the sole global query permit. The harness scheduler
only modeled namespace conflicts and therefore dispatched an exact-error probe
that could not reach validation. RED
`pending_query_hold_defers_exact_error_probe_but_not_ordinary_query` reproduced
the false positive. GREEN made exact-error probes defer behind a held global
query-admission resource while ordinary queries still exercise 503 responses.
Focused seed 242 then passed 470 operations and 6 compactions under
`target/adversarial/phase3-seed242-fix/run-1784037470/seed-242`.

The focused rerun then exposed a second harness-only schedule defect: seed 29
started a read-only node inside a global S3 read partition. The new node
correctly failed closed while loading authoritative `_security/` policy, but
the helper treated that expected startup failure as a panic. RED
`composite_node_starts_avoid_global_read_partitions` captured the schedule;
GREEN relocates node starts to the first operation after a global read
partition without shortening the fault window. Focused seed 29 passed 150
operations and 4 compactions with zero failures under
`target/adversarial/phase3-seed29-bootstrap-fix/run-1784038079`.

With both repairs staged, the exact one-hour command was rerun against a frozen
source/test tree. It exited 0 and wrote
`target/adversarial/run-1784038915/report.md`:

```text
Summary: seeds=343, failed=0, research_findings=0, ops=179817,
explicit_compactions=4976, background_compactions=1718, ops/sec=50.56
```

No `failure.json` exists in the run. The script copied the report byte-for-byte
to `tasks/overnight-adversarial-report.md`.

The independent pre-commit audit ran after this successful soak and found the
regressing-head fail-loud issue documented above plus missing RED/HEAD-count
evidence. After the narrow cache fix, the complete policy suite (32/32),
library suite (467/467), counting suite (11/11), adversarial binary (201 passed,
7 ignored), oracle self-test, deterministic smoke, and exact replay all passed.
The user explicitly waived another one-hour Phase-3 soak; no second post-audit
soak was run.

## Files changed

- API/contracts: `api/zeppelin-api.yaml`,
  `contract/fixtures/v0.3.0/manifest.json`, and the ten
  `contract/fixtures/v0.3.0/security_*.{req,resp}.json` pairs.
- Policy/security domain: `src/security/{action,audit,authn,decision,kernel,mod,policy,policy_cache,policy_store,principal,resource,route_map}.rs`.
- Runtime/storage wiring: `src/config.rs`, `src/error.rs`, `src/startup.rs`,
  `src/storage/{mod,store}.rs`, `src/server/mod.rs`,
  `src/server/handlers/{mod,query,security}.rs`.
- Formal/fuzz verification: `formal-verifications/tla/SecurityPolicy.tla`,
  its three cfg files, `scripts/run-july-tla.sh`,
  `loom-tests/tests/security_policy_cache.rs`, `fuzz/Cargo.toml`,
  `fuzz/fuzz_targets/policy_json.rs`, and the four JSON corpus seeds.
- Phase suites: `tests/security_{api,audit,authn,boot,kernel,policy,policy_counting,rate_limit}_tests.rs`
  plus `tests/security_bootstrap_authority_tests.rs`.
- Shared/regression harnesses: `tests/common/{counting,fault_injection,server}.rs`,
  `tests/adversarial/{faults/mod,runner}.rs`,
  `tests/perf_contract/scenario.rs`,
  `tests/{attrs_laziness,batch_query,contract,hydration_api,warm_parity}_tests.rs`.
- Evidence: `tasks/security/evidence/phase-3.md`.

The staged Phase-3 inventory is exactly 76 paths. No dependency was added. The
fuzz manifest only registers the new target. No performance tolerance,
profile, or contract baseline was widened.

## Limitations and deferred scope

- Each policy version rewrites one complete immutable snapshot; per-entity
  policy objects remain a later scale optimization.
- Revocation is immediate on the mutating node and bounded by two refresh
  windows on other nodes; it is not a synchronous cluster-wide barrier.
- After the first successful publication, S3 is authoritative and boot keys
  are only bootstrap/recovery input; drift is ignored with a redacted warning.
- Principal rate-limit buckets are node-local disposable state, consistent
  with stateless nodes; they are not a distributed quota.
- Mandatory filters and field masks (Phase 4), security adversarial operations
  (Phase 5), delegation (Phase 7), preservation (Phase 8), OIDC, and receipts
  (Phase 10) remain outside Phase 3.

## G-COMMIT

Commit: `SELF`

```text
Move security policy to CAS-published S3 snapshots with RBAC

Immutable _security/ policy versions + CAS head; bootstrap from boot
config; key create/rotate/revoke admin routes (must_audit); tested
2x-refresh revocation bound failing closed on staleness; namespace
RBAC grants; principal-keyed rate limiting; SecurityPolicy.tla, loom
and fuzz coverage.
```
