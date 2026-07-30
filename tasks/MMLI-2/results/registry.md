# MMLI-2 Phase 3 — Artifact-Family Registry

Pinned input revision: `3e8cc4a6fdccd8cb10de55d0ec0a3b130b0f5915`

## Family semantics

| Family | Deferred delete | GC ownership | Branch locality |
| --- | --- | --- | --- |
| `Metadata` | no | `ControlProtocol` | no |
| `Manifest` | no | `ControlProtocol` | no |
| `Lease` | no | `ControlProtocol` | no |
| `ManifestHistory` | no | `ControlProtocol` | no |
| `Snapshot` | no | `ControlProtocol` | no |
| `Wal` | yes | `ManifestReferenced` | yes |
| `Segment` | yes | `ManifestReferenced` | yes |
| `Staging` | no | `StagingProtocol` | no |
| `Gc` | no | `ControlProtocol` | no |
| `BranchVisibilityRemoved` | no | `ControlProtocol` | no |

No inspected call site disagreed with the encoded semantics.

## Golden fixture

`tests/fixtures/mmli2/phase3_reachable_keys.txt` is the byte-exact expected
union for a representative manifest containing a WAL fragment, a product-
quantized segment with bitmap, FTS, sketch, bootstrap, membership, and grouped
cluster sidecars, a pending delete, an active staging root, and retained-history
roots.

`tests/fixtures/mmli2/phase3_family_conformance.tsv` is consumed by both the
production `NamespaceObjectFamily::ALL` completeness test and the test-side
counting attribution test. It has one deliberate `ArtifactClass` row for each
of the 10 registered families.

## Validation

- `CARGO_INCREMENTAL=0 cargo test --lib storage`
  - 16 passed, 0 failed.
- `CARGO_INCREMENTAL=0 cargo test --lib
  artifact_family_registry_reachability_matches_golden_fixture`
  - 1 passed, 0 failed.
- `CARGO_INCREMENTAL=0 cargo test --test counting_attribution_tests
  namespace_family_attribution_is_deliberate_and_complete`
  - 1 passed, 0 failed.
- `TEST_BACKEND=minio CARGO_INCREMENTAL=0 cargo test --test
  storage_gc_tests`
  - 78 passed, 0 failed.
- `TEST_BACKEND=minio CARGO_INCREMENTAL=0 cargo test --test
  artifact_origin_tests`
  - Compiled successfully; 0 tests selected because the file is gated by
    `branching-test-support`.
- `TEST_BACKEND=minio CARGO_INCREMENTAL=0 cargo test --features
  branching-test-support --test artifact_origin_tests -- --test-threads=1`
  - 18 passed, 0 failed.
- `CARGO_INCREMENTAL=0 cargo clippy --all-targets -- -D warnings`
  - passed.
- `CARGO_INCREMENTAL=0 cargo fmt --all -- --check`
  - passed.
- Family-prefix literal gate and `git diff --check`
  - passed with no output.
