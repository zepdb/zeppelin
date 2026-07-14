# Phase 1 evidence

## RED — required security mode

Command:

```text
cargo test --release --test security_boot_tests boot_fails_without_mode -- --exact --nocapture
```

Verbatim failure on `ea8c7ec`:

```text
error[E0599]: no function or associated item named `from_str` found for struct `zeppelin::config::Config` in the current scope
    --> tests/security_boot_tests.rs:7:25
     |
   7 |     let error = Config::from_str("").expect_err("missing [security] must fail closed");
     |                         ^^^^^^^^ function or associated item not found in `zeppelin::config::Config`
     |
note: if you're trying to build a new `zeppelin::config::Config`, consider using `zeppelin::config::Config::load` which returns `Result<zeppelin::config::Config, ZeppelinError>`
    --> /Users/aghatage/Documents/code/zeppelin/src/config.rs:2139:5
     |
2139 |     pub fn load(path: Option<&str>) -> Result<Self> {
     |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
help: there is an associated function `from` with a similar name
     |
   7 -     let error = Config::from_str("").expect_err("missing [security] must fail closed");
   7 +     let error = Config::from("").expect_err("missing [security] must fail closed");
     |

For more information about this error, try `rustc --explain E0599`.
error: could not compile `zeppelin` (test "security_boot_tests") due to 1 previous error
```

## RED — enforced mode requires a key

Command:

```text
cargo test --release --test security_boot_tests boot_fails_enforced_no_keys -- --exact --nocapture
```

Verbatim failure:

```text
running 1 test

thread 'boot_fails_enforced_no_keys' (68982556) panicked at tests/security_boot_tests.rs:20:5:
assertion `left == right` failed
  left: "config error: failed to parse config: TOML parse error at line 1, column 2\n  |\n1 | [security]\n  |  ^^^^^^^^\nunknown field `security`, expected one of `server`, `storage`, `cache`, `indexing`, `compaction`, `logging`, `wal`, `query`, `gc`\n"
 right: "config error: invalid configuration:\n- security.api_keys must contain at least one usable key when security.mode is enforced"
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test boot_fails_enforced_no_keys ... FAILED

failures:

failures:
    boot_fails_enforced_no_keys

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s

error: test failed, to rerun pass `--test security_boot_tests`
```

## RED — full-shaped authorization decisions

Command:

```text
cargo test --release --test security_kernel_tests configured_grant_produces_full_shaped_allow_decision -- --exact --nocapture
```

Verbatim failure:

```text
error[E0432]: unresolved imports `zeppelin::security::Decision`, `zeppelin::security::DenyReason`, `zeppelin::security::NamespaceId`, `zeppelin::security::Principal`, `zeppelin::security::PrincipalId`, `zeppelin::security::RequestContext`, `zeppelin::security::Resource`, `zeppelin::security::SecurityKernel`
 --> tests/security_kernel_tests.rs:5:13
  |
5 |     Action, Decision, DenyReason, NamespaceId, Principal, PrincipalId, RequestContext, Resource,
  |             ^^^^^^^^  ^^^^^^^^^^  ^^^^^^^^^^^  ^^^^^^^^^  ^^^^^^^^^^^  ^^^^^^^^^^^^^^  ^^^^^^^^ no `Resource` in `security`
  |             |         |           |            |          |            |
  |             |         |           |            |          |            no `RequestContext` in `security`
  |             |         |           |            |          no `PrincipalId` in `security`
  |             |         |           |            no `Principal` in `security`
  |             |         |           no `NamespaceId` in `security`
  |             |         no `DenyReason` in `security`
  |             no `Decision` in `security`
6 |     SecurityKernel,
  |     ^^^^^^^^^^^^^^ no `SecurityKernel` in `security`

For more information about this error, try `rustc --explain E0432`.
error: could not compile `zeppelin` (test "security_kernel_tests") due to 1 previous error
warning: build failed, waiting for other jobs to finish...
```

## RED — canonical bearer authentication

Command:

```text
cargo test --release --test security_authn_tests canonical_bearer_authenticates_named_principal -- --exact --nocapture
```

Verbatim failure:

```text
   Compiling zeppelin v0.1.0 (/Users/aghatage/Documents/code/zeppelin)
error[E0432]: unresolved imports `zeppelin::security::ApiKeyAdapter`, `zeppelin::security::CredentialAdapter`
 --> tests/security_authn_tests.rs:6:26
  |
6 | use zeppelin::security::{ApiKeyAdapter, CredentialAdapter};
  |                          ^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^ no `CredentialAdapter` in `security`
  |                          |
  |                          no `ApiKeyAdapter` in `security`

For more information about this error, try `rustc --explain E0432`.
error: could not compile `zeppelin` (test "security_authn_tests") due to 1 previous error
```

## RED — protected readiness at the HTTP seam

Command:

```text
TEST_BACKEND=minio cargo test --release --test security_api_tests healthz_public_readyz_gated -- --exact --nocapture
```

Verbatim failure:

```text
running 1 test

thread 'healthz_public_readyz_gated' (69026156) panicked at tests/security_api_tests.rs:46:5:
assertion `left == right` failed
  left: 200
 right: 401
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test healthz_public_readyz_gated ... FAILED

failures:

failures:
    healthz_public_readyz_gated

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 5 filtered out; finished in 0.56s

error: test failed, to rerun pass `--test security_api_tests`
```

## RED — malformed digests fail loud

Command:

```text
cargo test --release --test security_boot_tests boot_fails_bad_key_digest -- --exact --nocapture
```

Verbatim failure (only a deliberately malformed test digest is rendered):

```text
running 1 test

thread 'boot_fails_bad_key_digest' (69005583) panicked at tests/security_boot_tests.rs:94:6:
API key digests must be exactly 32 encoded bytes: Config { server: ServerConfig { host: "0.0.0.0", port: 8080, request_timeout_secs: 30, max_concurrent_queries: 64, max_batch_size: 50000, max_query_batch_size: 256, max_top_k: 10000, shutdown_timeout_secs: 30, max_dimensions: 65536, max_vector_id_length: 1024, max_request_body_mb: 512, default_top_k: 10, rate_limit_rps: 100, rate_limit_burst: 200, write_rate_limit_rps: 50, write_rate_limit_burst: 100, rate_limit_idle_ttl_secs: 600, trusted_proxies: [] }, storage: StorageConfig { backend: S3, bucket: "zeppelin", s3_region: None, s3_endpoint: None, s3_access_key_id: None, s3_secret_access_key: None, s3_allow_http: false, fail_fast: true }, cache: CacheConfig { dir: "/var/cache/zeppelin", max_size_gb: 50, memory_cache_max_mb: 256, wal_fragment_cache_max_mb: 128, decoded_artifact_cache_max_mb: 64, manifest_cache_ttl_ms: 500, namespace_registry_ttl_ms: 5000, hydration_enabled: false, hydration_policy: SessionWindow, hydration_heat_queries: 3, hydration_heat_window_secs: 60, hydration_parallelism: 4, hydration_max_segment_fraction: 0.5 }, indexing: IndexingConfig { default_num_centroids: 256, target_rows_per_cluster: 3000, max_num_centroids: 4096, default_nprobe: 32, default_probe_fraction: 0.1875, max_nprobe: 256, kmeans_max_iterations: 25, kmeans_convergence_epsilon: 0.0001, balance_max_ratio: 4.0, balance_repair_rounds: 8, oversample_factor: 3, quantization: Scalar, pq_m: 8, hierarchical: false, leaf_size: None, bitmap_index: true, fts_index: false, bm25_max_full_scan_clusters: 500, bm25_max_full_scan_vectors: 100000 }, compaction: CompactionConfig { interval_secs: 30, max_wal_fragments_before_compact: 100, max_wal_age_before_compact_secs: 300, max_wal_bytes_before_compact: 67108864, retrain_imbalance_threshold: 5.0, max_pending_deletes: 1000, max_old_segments: 10, lease_duration_secs: 300 }, logging: LoggingConfig { level: "info", format: "json" }, wal: WalConfig, query: QueryConfig { rerank_coalesce_gap_bytes: Some(1048576), cost_latency_profile: None }, gc: GcConfig { horizon_secs: 900, compaction_upload_window_secs: 300, skew_slop_secs: 5, allow_unsafe_short_horizon: false, manifest_history_keep_count: 128, pitr_retention_secs: 0 }, security: SecurityConfig { mode: Enforced, readyz_public: false, policy_refresh_secs: 5, license_path: "", api_keys: [ApiKeyConfig { key_id: "zpk1_bad_digest", name: "bad-digest", sha256_hex: "not-a-sha256-digest", actions: ["Query"], namespaces: ["*"], expires_at: None }] } }
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test boot_fails_bad_key_digest ... FAILED

failures:

failures:
    boot_fails_bad_key_digest

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 4 filtered out; finished in 0.00s

error: test failed, to rerun pass `--test security_boot_tests`
```

## RED — duplicate key identifiers are rejected

Command:

```text
cargo test --release --test security_boot_tests boot_fails_dup_key_id -- --exact --nocapture
```

Verbatim failure (only test digests are rendered):

```text
running 1 test

thread 'boot_fails_dup_key_id' (68998600) panicked at tests/security_boot_tests.rs:71:6:
duplicate public key identifiers must be ambiguous and invalid: Config { server: ServerConfig { host: "0.0.0.0", port: 8080, request_timeout_secs: 30, max_concurrent_queries: 64, max_batch_size: 50000, max_query_batch_size: 256, max_top_k: 10000, shutdown_timeout_secs: 30, max_dimensions: 65536, max_vector_id_length: 1024, max_request_body_mb: 512, default_top_k: 10, rate_limit_rps: 100, rate_limit_burst: 200, write_rate_limit_rps: 50, write_rate_limit_burst: 100, rate_limit_idle_ttl_secs: 600, trusted_proxies: [] }, storage: StorageConfig { backend: S3, bucket: "zeppelin", s3_region: None, s3_endpoint: None, s3_access_key_id: None, s3_secret_access_key: None, s3_allow_http: false, fail_fast: true }, cache: CacheConfig { dir: "/var/cache/zeppelin", max_size_gb: 50, memory_cache_max_mb: 256, wal_fragment_cache_max_mb: 128, decoded_artifact_cache_max_mb: 64, manifest_cache_ttl_ms: 500, namespace_registry_ttl_ms: 5000, hydration_enabled: false, hydration_policy: SessionWindow, hydration_heat_queries: 3, hydration_heat_window_secs: 60, hydration_parallelism: 4, hydration_max_segment_fraction: 0.5 }, indexing: IndexingConfig { default_num_centroids: 256, target_rows_per_cluster: 3000, max_num_centroids: 4096, default_nprobe: 32, default_probe_fraction: 0.1875, max_nprobe: 256, kmeans_max_iterations: 25, kmeans_convergence_epsilon: 0.0001, balance_max_ratio: 4.0, balance_repair_rounds: 8, oversample_factor: 3, quantization: Scalar, pq_m: 8, hierarchical: false, leaf_size: None, bitmap_index: true, fts_index: false, bm25_max_full_scan_clusters: 500, bm25_max_full_scan_vectors: 100000 }, compaction: CompactionConfig { interval_secs: 30, max_wal_fragments_before_compact: 100, max_wal_age_before_compact_secs: 300, max_wal_bytes_before_compact: 67108864, retrain_imbalance_threshold: 5.0, max_pending_deletes: 1000, max_old_segments: 10, lease_duration_secs: 300 }, logging: LoggingConfig { level: "info", format: "json" }, wal: WalConfig, query: QueryConfig { rerank_coalesce_gap_bytes: Some(1048576), cost_latency_profile: None }, gc: GcConfig { horizon_secs: 900, compaction_upload_window_secs: 300, skew_slop_secs: 5, allow_unsafe_short_horizon: false, manifest_history_keep_count: 128, pitr_retention_secs: 0 }, security: SecurityConfig { mode: Enforced, readyz_public: false, policy_refresh_secs: 5, license_path: "", api_keys: [ApiKeyConfig { key_id: "zpk1_duplicate", name: "first", sha256_hex: "0000000000000000000000000000000000000000000000000000000000000000", actions: ["Query"], namespaces: ["*"], expires_at: None }, ApiKeyConfig { key_id: "zpk1_duplicate", name: "second", sha256_hex: "1111111111111111111111111111111111111111111111111111111111111111", actions: ["Query"], namespaces: ["*"], expires_at: None }] } }
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test boot_fails_dup_key_id ... FAILED

failures:

failures:
    boot_fails_dup_key_id

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s

error: test failed, to rerun pass `--test security_boot_tests`
```

## RED — unknown action names fail loud

Command:

```text
cargo test --release --test security_boot_tests boot_fails_bad_action_name -- --exact --nocapture
```

Verbatim failure (the rendered value contains only a test digest, never a
bearer secret):

```text
running 1 test

thread 'boot_fails_bad_action_name' (68989885) panicked at tests/security_boot_tests.rs:41:6:
unknown action names must fail loud: Config { server: ServerConfig { host: "0.0.0.0", port: 8080, request_timeout_secs: 30, max_concurrent_queries: 64, max_batch_size: 50000, max_query_batch_size: 256, max_top_k: 10000, shutdown_timeout_secs: 30, max_dimensions: 65536, max_vector_id_length: 1024, max_request_body_mb: 512, default_top_k: 10, rate_limit_rps: 100, rate_limit_burst: 200, write_rate_limit_rps: 50, write_rate_limit_burst: 100, rate_limit_idle_ttl_secs: 600, trusted_proxies: [] }, storage: StorageConfig { backend: S3, bucket: "zeppelin", s3_region: None, s3_endpoint: None, s3_access_key_id: None, s3_secret_access_key: None, s3_allow_http: false, fail_fast: true }, cache: CacheConfig { dir: "/var/cache/zeppelin", max_size_gb: 50, memory_cache_max_mb: 256, wal_fragment_cache_max_mb: 128, decoded_artifact_cache_max_mb: 64, manifest_cache_ttl_ms: 500, namespace_registry_ttl_ms: 5000, hydration_enabled: false, hydration_policy: SessionWindow, hydration_heat_queries: 3, hydration_heat_window_secs: 60, hydration_parallelism: 4, hydration_max_segment_fraction: 0.5 }, indexing: IndexingConfig { default_num_centroids: 256, target_rows_per_cluster: 3000, max_num_centroids: 4096, default_nprobe: 32, default_probe_fraction: 0.1875, max_nprobe: 256, kmeans_max_iterations: 25, kmeans_convergence_epsilon: 0.0001, balance_max_ratio: 4.0, balance_repair_rounds: 8, oversample_factor: 3, quantization: Scalar, pq_m: 8, hierarchical: false, leaf_size: None, bitmap_index: true, fts_index: false, bm25_max_full_scan_clusters: 500, bm25_max_full_scan_vectors: 100000 }, compaction: CompactionConfig { interval_secs: 30, max_wal_fragments_before_compact: 100, max_wal_age_before_compact_secs: 300, max_wal_bytes_before_compact: 67108864, retrain_imbalance_threshold: 5.0, max_pending_deletes: 1000, max_old_segments: 10, lease_duration_secs: 300 }, logging: LoggingConfig { level: "info", format: "json" }, wal: WalConfig, query: QueryConfig { rerank_coalesce_gap_bytes: Some(1048576), cost_latency_profile: None }, gc: GcConfig { horizon_secs: 900, compaction_upload_window_secs: 300, skew_slop_secs: 5, allow_unsafe_short_horizon: false, manifest_history_keep_count: 128, pitr_retention_secs: 0 }, security: SecurityConfig { mode: Enforced, readyz_public: false, policy_refresh_secs: 5, license_path: "", api_keys: [ApiKeyConfig { key_id: "zpk1_admin", name: "bootstrap-admin", sha256_hex: "0000000000000000000000000000000000000000000000000000000000000000", actions: ["Qrye"], namespaces: ["*"], expires_at: None }] } }
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test boot_fails_bad_action_name ... FAILED

failures:

failures:
    boot_fails_bad_action_name

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

error: test failed, to rerun pass `--test security_boot_tests`
```

## RED — strict config completion and startup mode gauge

Commands:

```text
cargo test --release --test security_boot_tests boot_fails_noncanonical_key_id -- --exact --nocapture
cargo test --release --test security_boot_tests load_without_path_fails_required_security_contract -- --exact --nocapture
cargo test --release --test security_boot_tests startup_exports_open_unsafe_security_mode_gauge -- --exact --nocapture
```

Verbatim failing excerpts:

```text
test boot_fails_noncanonical_key_id ... FAILED

assertion `left == right` failed
  left: "config error: invalid configuration:\n- security.api_keys must contain at least one usable key when security.mode is enforced"
 right: "config error: missing required [security] section; set security.mode to \"enforced\" or \"open_unsafe\""
test load_without_path_fails_required_security_contract ... FAILED

error[E0425]: cannot find value `SECURITY_MODE` in module `zeppelin::metrics`
   --> tests/security_boot_tests.rs:304:28
    |
304 |         zeppelin::metrics::SECURITY_MODE
    |                            ^^^^^^^^^^^^^ not found in `zeppelin::metrics`
```

## Config/startup completion — GREEN

Focused security configuration, authentication, and kernel tests:

```text
cargo test --release --test security_boot_tests --test security_authn_tests --test security_kernel_tests -- --nocapture
```

```text
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

Existing configuration, startup, and affected compaction fixtures:

```text
cargo test --release --lib config::tests -- --nocapture
cargo test --release --lib startup::tests -- --nocapture
cargo test --release --lib compaction::tests::gc_upload_window_drives_horizon_floor_and_compactor_abort_window -- --exact --nocapture
```

```text
test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 418 filtered out
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 437 filtered out
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 441 filtered out
```

Targeted formatting and whitespace checks exited 0:

```text
rustfmt --edition 2021 --check src/config.rs src/startup.rs src/metrics.rs src/main.rs src/compaction/mod.rs tests/security_boot_tests.rs tests/security_authn_tests.rs
git diff --check -- src/config.rs src/startup.rs src/metrics.rs src/main.rs src/compaction/mod.rs tests/security_boot_tests.rs tests/security_authn_tests.rs zeppelin.toml.example
```

Files changed by this slice:

- `src/config.rs`
- `src/startup.rs`
- `src/metrics.rs`
- `src/main.rs`
- `src/compaction/mod.rs` (test fixture only)
- `tests/security_boot_tests.rs`
- `tests/security_authn_tests.rs`
- `zeppelin.toml.example`
- `tasks/security/evidence/phase-1.md`

Limitations: phase-1 grants remain boot-static until phase 3; `open_unsafe`
is an explicit development escape hatch and emits the WARN banner plus
`zeppelin_security_mode{mode="open_unsafe"} 1`. This was an intermediate
slice; the single phase commit is identified in the final commit section.

## GREEN

All required Phase 1 gates are green on the final tree:

- G-GREEN: focused kernel, config, authentication, HTTP, contract, harness,
  and offline-evaluator regressions pass.
- G-LIB: 445 release-mode library tests pass.
- G-INT/G-MAP: 125 MinIO-backed phase/regression tests pass, including the
  mechanical route map.
- G-ADV: 16-mutation oracle self-test, literal two-seed smoke, and an exact
  530-op legacy-artifact replay pass.
- G-PERF: 416 ns p50 added CPU, 0 GET delta, 0 PUT delta.
- Fuzz: production bearer parser checks and completes 10,000 sanitizer-backed
  executions without a panic or noncanonical success.
- G-LINT: all-target Clippy with warnings denied, format check, and diff check
  pass.

## Files changed

- Security kernel and server wiring: `src/security/*.rs`, `src/server/mod.rs`,
  protected handlers, `src/config.rs`, `src/startup.rs`, `src/error.rs`,
  `src/metrics.rs`, `src/lib.rs`, and `src/main.rs`.
- Explicit non-server config: `src/bin/recall_eval.rs` and the affected
  compaction fixture.
- Configuration/dependencies: `Cargo.toml`, `Cargo.lock`, and
  `zeppelin.toml.example`.
- Public contract: `api/zeppelin-api.yaml`, `tests/contract_tests.rs`, and
  `contract/fixtures/v0.3.0/`.
- Security tests/fuzzing: `tests/security_*`,
  `tests/test_harness_security_tests.rs`, `fuzz/Cargo.toml`, and
  `fuzz/fuzz_targets/bearer_parse.rs`.
- Test infrastructure: `tests/common/server.rs`, adversarial runner/model/gate,
  the pre-security replay fixture, and every integration client affected by
  generated per-server credentials.
- Performance contract: `tests/perf_contract/security.rs`, frozen
  `contracts/secured_query.toml`, contract/checker/report plumbing, and ideal
  catalog/inventory entries.
- Evidence: `tasks/security/evidence/phase-1.md`; new operational learnings were
  appended to the local gitignored `tasks/learnings.md` ledger.

## Limitations

- Phase 1 grants are immutable boot configuration until Phase 3 replaces them
  with S3-authoritative policy and principal/key registries.
- API keys are the only credential adapter implemented in this phase; the
  trait seam is present for later identity mechanisms.
- `open_unsafe` is an explicit development-only posture, never a default. It
  emits the required WARN and mode gauge.
- Audit obligations, constraints, delegation, entitlements, and verifiable
  receipts remain intentionally empty plumbing for their designated phases.

## Commit

`SELF` — the single phase commit containing this evidence. Resolve the exact
hash with `git rev-parse HEAD`; embedding a commit's own hash inside its tree is
self-referential and would change that hash.

## Contract/OpenAPI slice — RED

Commands:

```text
cargo test --release --test contract_tests openapi_documents_bearer_security_for_every_protected_operation -- --exact --nocapture
cargo test --release --test contract_tests contract_fixture_inventory_is_complete -- --exact --nocapture
```

Verbatim failing excerpts before the contract implementation:

```text
thread 'openapi_documents_bearer_security_for_every_protected_operation' (69045393) panicked at tests/contract_tests.rs:131:5:
OpenAPI must make bearerAuth the default security requirement
test openapi_documents_bearer_security_for_every_protected_operation ... FAILED

thread 'contract_fixture_inventory_is_complete' (69045599) panicked at tests/contract_tests.rs:198:9:
request fixture missing: /Users/aghatage/Documents/code/zeppelin/contract/fixtures/v0.3.0/unauthenticated_401.req.json
test contract_fixture_inventory_is_complete ... FAILED
```

The pre-existing exporter also used an anonymous `reqwest::Client`; once
server auth landed, the new real-engine run proved that the exporter itself
needed the harness-provided administrator bearer.

## Contract/OpenAPI slice — GREEN

Fixture regeneration against the real MinIO-backed server:

```text
TEST_BACKEND=minio UPDATE_CONTRACT_FIXTURES=1 cargo test --release --test contract_tests contract_fixtures_match_real_engine_output -- --exact --nocapture
```

```text
running 1 test
test contract_fixtures_match_real_engine_output ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out; finished in 0.44s
```

Full contract comparison and static OpenAPI gates:

```text
TEST_BACKEND=minio cargo test --release --test contract_tests -- --nocapture
```

```text
running 9 tests
test openapi_documents_exact_routed_surface ... ok
test contract_fixture_inventory_is_complete ... ok
test openapi_documents_bearer_security_for_every_protected_operation ... ok
test contract_fixtures_match_real_engine_output ... ok

test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.08s
```

Additional hygiene:

```text
ruby -e 'require "yaml"; doc = YAML.load_file("api/zeppelin-api.yaml"); abort "bearerAuth missing" unless doc.dig("components", "securitySchemes", "bearerAuth", "scheme") == "bearer"; puts "OpenAPI YAML parse/security scheme: ok"'
rustfmt --edition 2021 --config skip_children=true --check tests/contract_tests.rs
git diff --check -- tests/contract_tests.rs api/zeppelin-api.yaml contract/fixtures/v0.3.0
```

The YAML probe printed `OpenAPI YAML parse/security scheme: ok`; both
format/diff checks exited 0. The phase-wide `cargo fmt --all --check` was still
red while the parallel harness migration was in progress, entirely outside
this slice; the harness owner was given the exact formatting handoff.

The build-time profiling surface was also compiled and checked explicitly:

```text
cargo test --release --features profiling --test contract_tests openapi_documents -- --nocapture
```

```text
running 2 tests
test openapi_documents_exact_routed_surface ... ok
test openapi_documents_bearer_security_for_every_protected_operation ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 7 filtered out; finished in 0.00s
```

## Contract/OpenAPI slice — files

- `api/zeppelin-api.yaml`
- `tests/contract_tests.rs`
- `contract/fixtures/v0.3.0/manifest.json`
- `contract/fixtures/v0.3.0/{unauthenticated_401,forbidden_403,readyz_gated_401}.{req,resp}.json`
- `tasks/security/evidence/phase-1.md`

No commit was created by this slice.

## Bearer parser fuzz target — RED

Command:

```text
cargo check --manifest-path fuzz/Cargo.toml --bin fuzz_bearer_parse
```

Verbatim failure before exposing the production parser seam:

```text
error[E0599]: no method named `authenticate_bearer` found for reference `&'static ApiKeyAdapter` in the current scope
  --> fuzz_targets/fuzz_bearer_parse.rs:37:18
   |
37 |     if adapter().authenticate_bearer(header, now).is_ok() {
   |                  ^^^^^^^^^^^^^^^^^^^
   |
help: there is a method `authenticate` with a similar name

error: could not compile `zeppelin-fuzz` (bin "fuzz_bearer_parse") due to 1 previous error
```

The target was then renamed to the plan-required
`fuzz_targets/bearer_parse.rs` path before the GREEN run.

## Bearer parser fuzz target — GREEN

Command:

```text
cargo check --manifest-path fuzz/Cargo.toml --bin bearer_parse
```

```text
Checking zeppelin-fuzz v0.0.0 (/Users/aghatage/Documents/code/zeppelin/fuzz)
Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.14s
```

Nightly sanitizer-backed fuzz smoke:

```text
cargo +nightly fuzz run bearer_parse -- -runs=10000
#10000 DONE   cov: 194 ft: 194 corp: 3/9b lim: 98 exec/s: 0 rss: 73Mb
Done 10000 runs in 0 second(s)
```

## Existing-suite credential migration — RED

Command:

```text
TEST_BACKEND=minio cargo test --release --test api_tests test_create_namespace_returns_uuid_and_warning -- --exact --nocapture
```

The first existing integration case run after auth enforcement reached the
real server without a credential:

```text
assertion `left == right` failed
  left: 401
 right: 201

test test_create_namespace_returns_uuid_and_warning ... FAILED
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 23 filtered out; finished in 0.28s
```

## Existing-suite credential migration — GREEN

After threading the harness-generated administrator bearer through the shared,
adversarial, and performance clients, the same public-seam test passed:

```text
test test_create_namespace_returns_uuid_and_warning ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 23 filtered out; finished in 0.28s
```

All integration targets then compiled with the migrated helper shapes:

```text
cargo check --tests
exit status: 0 (all integration targets; elapsed 6m 30s)
```

The adversarial workload target also compiled in the release profile:

```text
cargo test --release --test adversarial_workload_tests --no-run
Finished `release` profile [optimized + debuginfo] target(s) in 27.31s
```

## Merged-boundary security regressions — RED

The phase-wide integration gate caught unsupported methods being intercepted by
route security instead of retaining Axum's canonical 405:

```text
TEST_BACKEND=minio cargo test --release --test security_api_tests --test security_boot_tests --test api_tests --test error_envelope_tests --test malformed_request_tests --test namespace_lifecycle_tests --test observability_tests -- --nocapture

thread 'test_list_namespaces_disabled' panicked at tests/api_tests.rs:74:5:
assertion `left == right` failed
  left: 500
 right: 405
test result: FAILED. 23 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.75s
```

Axum 0.7 implicitly dispatches HEAD to every GET handler, but the initial
classifier did not inherit the GET security class:

```text
cargo test --release --lib security::route_map::tests::implicit_head_inherits_every_get_classification -- --exact --nocapture

assertion `left == right` failed: HEAD /healthz must inherit its GET security class
  left: None
 right: Some(Public)
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 442 filtered out; finished in 0.00s
```

The credential-expiry boundary was incorrectly classified as authorization
instead of authentication when reached through a kernel denial:

```text
cargo test --release --test security_authn_tests expired_authorization_decision_stays_an_authentication_failure -- --exact --nocapture

assertion `left == right` failed
  left: 403
 right: 401
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 4 filtered out; finished in 0.00s
```

Finally, authentication ran outside the existing IP rate limiter, so repeated
missing credentials remained unlimited:

```text
TEST_BACKEND=minio cargo test --release --test security_api_tests authentication_failures_remain_ip_rate_limited -- --exact --nocapture

assertion `left == right` failed
  left: 401
 right: 429
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 18 filtered out; finished in 0.57s
```

## Merged-boundary security regressions — GREEN

Security is attached to registered `MethodRouter` endpoints, leaving Axum's
method-not-allowed fallback canonical while still protecting implicit HEAD.
Authentication remains inside request ID, rate-limit, and HTTP-metrics layers;
authentication and authorization share one server-derived clock instant.

```text
cargo test --release --lib security -- --nocapture

running 6 tests
test security::action::tests::action_inventory_has_twenty_variants ... ok
test security::authn::tests::bearer_parser_requires_canonical_shape ... ok
test security::route_map::tests::implicit_head_inherits_every_get_classification ... ok
test security::route_map::tests::readiness_public_override_is_explicit ... ok
test security::route_map::tests::route_inventory_has_no_duplicate_method_path_pairs ... ok
test server::tests::security_request_context_never_defaults_a_missing_request_id ... ok
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 438 filtered out
```

```text
cargo test --release --test security_boot_tests --test security_authn_tests --test security_kernel_tests -- --nocapture

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## Offline evaluator config regression — RED → GREEN

Removing the implicit production config default initially made the offline
recall evaluator call a nonexistent explicit-config seam:

```text
error[E0425]: cannot find function `recall_eval_config` in this scope
```

The evaluator now constructs and validates an explicit `open_unsafe` config;
it does not synthesize server credentials or weaken production boot:

```text
cargo test --release --bin recall_eval tests::offline_evaluator_config_does_not_require_server_credentials -- --exact --nocapture

running 1 test
test tests::offline_evaluator_config_does_not_require_server_credentials ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out
```

## G-PERF — RED

Before the security scenario was admitted to the frozen catalog:

```text
perf_contract::tests::full_catalog_has_no_duplicates_and_keeps_phase1_prefix
panicked at tests/perf_contract/mod.rs:
the Phase 1 security gate requires a frozen secured_query scenario
test result: FAILED. 0 passed; 1 failed
```

## G-PERF — GREEN

The authoritative delta is the complete production API-key authentication,
route classification, and authorization seam minus measurement-loop overhead.
An `open_unsafe` comparator is deliberately not used because it would still
execute classification and kernel authorization and therefore undercount the
phase's total added CPU cost.

```text
TEST_BACKEND=minio cargo test --release --test perf_contract_tests contracts -- --ignored --nocapture

secured_query security budget: mode=enforced credential=api_key
p50_delta_ns=416 added_get_ops=0 added_put_ops=0
performance-contract report:
target/perf-contract/run-1784009359-443305000-25976/report.md
test result: ok. 1 passed; 0 failed
```

Budget result: 416 ns p50 added CPU is below the frozen 10,000 ns ceiling;
GET and PUT deltas are exactly zero. All 19 frozen scenarios passed without a
tolerance or baseline edit.

## G-LIB — GREEN

```text
cargo test --release --lib

running 445 tests
test result: ok. 445 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

```text
cargo clippy --release --lib -- -D warnings
Finished `release` profile [optimized + debuginfo]
```

## G-INT and G-MAP — GREEN

```text
TEST_BACKEND=minio cargo test --release --test security_api_tests --test security_boot_tests --test api_tests --test error_envelope_tests --test malformed_request_tests --test namespace_lifecycle_tests --test observability_tests -- --nocapture

api_tests:                 24 passed
error_envelope_tests:      14 passed
malformed_request_tests:   22 passed
namespace_lifecycle_tests: 12 passed
observability_tests:       15 passed
security_api_tests:        22 passed
security_boot_tests:       16 passed
total:                    125 passed, 0 failed
```

`security_api_tests::route_map_complete` passed inside this matrix. It checks
every registered method against the central map and requires every route
declaration to use the security wrapper. Runtime probes also preserved the
canonical 404 fallback, implicit HEAD authorization, and Axum 405 response.

The real-engine contract and optional profiling surfaces were rechecked after
the final generated-credential migration:

```text
TEST_BACKEND=minio cargo test --release --test contract_tests -- --nocapture
test result: ok. 9 passed; 0 failed

cargo test --release --features profiling --test contract_tests openapi_documents -- --nocapture
test result: ok. 2 passed; 0 failed
```

## G-ADV — GREEN

The oracle mutation matrix detected all 16 injected failures:

```text
cargo test --release --test adversarial_workload_tests oracle_selftest -- --ignored --nocapture
test oracle_selftest ... ok
test result: ok. 1 passed; 0 failed; 202 filtered out; finished in 5.72s
```

The literal two-pinned-seed MinIO gate passed after removing the runner's stale
three-seed assertion:

```text
TEST_BACKEND=minio ZEPPELIN_ADVERSARIAL_SECONDS=180 ZEPPELIN_ADVERSARIAL_MODE=deterministic ZEPPELIN_ADVERSARIAL_SEEDS=0,2 ZEPPELIN_ADVERSARIAL_MAX_OPS=100 ZEPPELIN_ADVERSARIAL_PRESERVE=always ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/security-phase1-gadv-commit-minio cargo test --release --test adversarial_workload_tests smoke -- --ignored --nocapture

seed 0: failed=false ops=130 compactions=21 background_compactions=0
seed 2: failed=false ops=120 compactions=16 background_compactions=0
adversarial smoke: seeds=2 ops=250 compactions=37 background_compactions=0 failed=0 non_blocking_findings=0
test smoke ... ok
```

One actual pre-security recorded artifact then decoded through the replay-only
compatibility migration and replayed exactly with an implicit generated
administrator. Production config remains fail-closed:

```text
TEST_BACKEND=minio ZEPPELIN_ADVERSARIAL_ARTIFACTS=target/adversarial/security-phase1-gadv-legacy-replay-minio ZEPPELIN_ADVERSARIAL_PRESERVE=always ZEPPELIN_ADVERSARIAL_REPLAY=target/adversarial/run-1783919745/seed-83 cargo test --release --test adversarial_workload_tests replay_seed -- --ignored --nocapture

replay clean: dir=target/adversarial/run-1783919745/seed-83 ops=530 compactions=47 background_compactions=0
test replay_seed ... ok
```

Artifacts:

- `target/adversarial/security-phase1-gadv-commit-minio/run-1784010183/`
- `target/adversarial/security-phase1-gadv-legacy-replay-minio/run-1784009811/`

## G-LINT — RED → GREEN

The first all-target pass exposed `field_reassign_with_default` in legacy test
fixture builders now compiling against the expanded `Config`. Those builders
were rewritten as equivalent struct initializers; no runtime setting changed.

```text
cargo clippy --all-targets -- -D warnings

error: field assignment outside of initializer for an instance created with Default::default()
error: could not compile `zeppelin` (test "attrs_laziness_tests")
```

Final hygiene commands:

```text
cargo clippy --all-targets -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s)

cargo fmt --all -- --check
git diff --check
```

All three commands exited 0.

## Supplemental toolchain observation

A debug-profile focused hard-abort test compiled, then its macOS process
stalled in `_dyld_start` before entering Rust and was stopped. This was not a
product/test failure: the release-profile oracle matrix, two-seed MinIO smoke,
and replay all executed normally and are the required G-ADV gate above.

## Final acceptance-audit regressions — RED → GREEN

### Body-derived namespace scope

The first central route map classified namespace collection create as a
system resource and clone checked only its path-derived source. The two focused
tests exposed that the body-derived target reached domain/storage work instead
of being denied:

```text
namespace_create_is_scoped_to_requested_name
left: 201
right: 403

clone_requires_target_namespace_create_scope
left: 404
right: 403
```

Both handlers now refine the route-level action through the same
`SecurityKernel` after decoding the requested target and before any storage
I/O. They pass inside the final 22-test `security_api_tests` binary and the
125-test G-INT matrix above.

### Malformed credential work profile

The deterministic timing-profile test was written before the normalization
seam and failed to compile:

```text
error[E0432]: unresolved imports `super::credential_candidate`,
`super::DUMMY_KEY_ID`, `super::DUMMY_SECRET`
error: could not compile `zeppelin` (lib test) due to 1 previous error
```

Malformed credentials now normalize to a fixed dummy key and canonical
43-character secret, then execute the same hash, lookup, base64 decode, and
constant-time digest comparison shape as an unknown key:

```text
cargo test --release --lib security::authn::tests::malformed_bearers_normalize_to_fixed_unknown_work -- --exact --nocapture
test security::authn::tests::malformed_bearers_normalize_to_fixed_unknown_work ... ok
test result: ok. 1 passed; 0 failed
```

### Inspect posture and legacy replay compatibility

The inspect contract test initially failed before an explicit inspection
posture existed:

```text
error[E0425]: cannot find function `inspection_config` in this scope
error: could not compile `zeppelin` (test "adversarial_workload_tests")
```

Inspection alone now boots `open_unsafe`, so its printed loopback URL remains
usable for manual curl without printing a credential. Scheduled work and
replay remain enforced. The focused test passed 1/1.

The committed pre-security artifact fixture then reproduced the rollout
compatibility failure:

```text
failed to parse replay seed config: missing field `security`
test legacy_replay_config_without_security_decodes_with_implicit_admin ... FAILED
```

The artifact-only decoder inserts an explicit enforced, empty bootstrap
security object only when the historical field is absent. A present invalid
value still fails, and production `Config` has no serde default:

```text
cargo test --release --test adversarial_workload_tests legacy_replay_config_without_security_decodes_with_implicit_admin -- --nocapture
test adversarial::runner::outcome_tests::legacy_replay_config_without_security_decodes_with_implicit_admin ... ok
test result: ok. 1 passed; 0 failed
```

The full 530-op historical replay is recorded in G-ADV above.

### Generated-credential cache regression

The final harness audit found one decoded-artifact-cache test reusing the
first server's client against a newly credentialed server. It now constructs
the client from the second server's generated bearer:

```text
TEST_BACKEND=minio cargo test --release --test decoded_artifact_cache_tests global_fts_decode_is_reused_and_cache_clear_preserves_results -- --exact --nocapture
test global_fts_decode_is_reused_and_cache_clear_preserves_results ... ok
test result: ok. 1 passed; 0 failed
```

### Final fuzz and hygiene

```text
cargo +nightly fuzz run bearer_parse -- -runs=10000
#10000 DONE   cov: 257 ft: 257 corp: 2/8b
Done 10000 runs in 0 second(s)

cargo clippy --all-targets -- -D warnings
Finished `dev` profile [unoptimized + debuginfo]
cargo fmt --all -- --check
git diff --check HEAD
```

All final hygiene commands exited 0. The two fuzzer-generated corpus files are
run artifacts and are deliberately excluded from the phase commit.
