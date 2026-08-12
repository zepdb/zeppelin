# tests — harness and conventions

All integration tests go through `TestHarness` (`common/harness.rs`). Each
harness gets a random `test-{uuid}` prefix for isolation and cleans up on drop.

## Backends

`TEST_BACKEND` selects the store. **The default is `memory`**, not S3:

| Value | Store | CAS |
| --- | --- | --- |
| `memory` (default) | `InMemory` | yes |
| `minio` | S3 → `http://localhost:9000` | yes |
| `s3` | real S3 | yes |
| `local` | `LocalFileSystem` in a `TempDir` | **no** |

Anything concurrency-, CAS-, or origin-routing-shaped needs `minio`. A green
`memory` run is weak evidence for those.

Bringing up MinIO without Docker:

```bash
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  minio server /tmp/miniodata --address 127.0.0.1:9000 &
mc alias set zeptest http://127.0.0.1:9000 minioadmin minioadmin
mc mb --ignore-existing zeptest/zeppelin-test
```

Signed-license startup tests additionally need an isolated
`ZEPPELIN_LICENSE_TEST_BUCKET`.

## Tests that hard-require MinIO

Several suites **assert** `TEST_BACKEND == "minio"` rather than being
`#[ignore]`d, so they *fail* (not skip) on a default run — e.g. all of
`artifact_origin_tests.rs`. This is intentional fail-loud style, but it means
`cargo test` is not green out of the box. Know which failures are
environmental before debugging.

Performance-contract and recall scenarios are `#[ignore]`d and need explicit
invocation plus MinIO.

## Data generation traps

- **`random_vectors()` is fixed-seed**: it returns identical IDs *and values*
  across calls. Testing dedup/merge across fragments therefore needs distinct
  pools or unique ID prefixes per fragment, or you get distance ties and
  flaky assertions.
- `common/vectors.rs` — generation; `common/assertions.rs` — S3 state checks;
  `common/counting.rs` — object-operation census; `common/fault_injection.rs`
  — deterministic CAS/fault barriers. Prefer these over sleeps; the design
  rules forbid sleep-based synchronization.

## Known-flaky / environment-dependent

- `test_active_queries_returns_to_zero` — shared Prometheus metrics across test
  binaries; passes in isolation.
- `adversarial::runner::outcome_tests::content_seed_127_classifies_durable_torn_manifest_loudly`
  — fails under full-suite parallel load against a single local MinIO; passes
  in isolation. Reproduced identically on a pre-2026-08-12 baseline, so it is
  load-timing, not a regression.
- `security::policy_publication::tests::*` (2) and
  `startup::tests::licensed_file_boot_enables_rbac_routes` — fail under
  `cargo test --lib` without MinIO because the `Local` backend has no ETag
  CAS. See `../src/security/CLAUDE.md`.

## Branching gate

```bash
TEST_BACKEND=minio cargo test --features branching-test-support \
  --test artifact_origin_tests --test branch_root_tests \
  --test branch_fork_tests --test branch_deletion_tests \
  --test branching_tests -- --test-threads=1
```

Note `tasks/branching/10-integration-adversarial-performance.md` names two
binaries that do **not** exist — `branch_compaction_tests` and
`branch_api_tests`. That coverage lives in `branch_fork_tests`,
`branching_tests`, and `security_branching_tests`. Run the command above, not
the one in the plan.

Use `--no-fail-fast` when running several suites, or cargo stops at the first
failing binary and you never see the rest.

## Contract tests

`cargo test --test contract_tests` asserts OpenAPI ↔ routed-surface parity.
Adding a route without updating `api/zeppelin-api.yaml` fails here.
