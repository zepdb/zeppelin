# Contributing to Zeppelin

Issues and pull requests are welcome.

## Before you send a PR

Run the three gates locally:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --lib
```

For changes touching storage, WAL, compaction, or concurrency, also run the
MinIO-backed integration pass (see the README's Development section for
setup):

```bash
TEST_BACKEND=minio cargo test --tests
```

## Ground rules

- **No silent fallbacks.** Code crashes explicitly on errors; no default
  values for things that should be configured.
- **S3 is the source of truth.** Never trust local state over object-store
  state; local cache is disposable.
- **Immutable artifacts.** WAL fragments and segments are write-once.
- **Tests hit real object storage** (MinIO or S3) — no mocks for storage
  operations.
- Changes to IVF partitioning or probe policy must run the pinned recall
  gate (`tests/ivf_recall_gate.rs`); it is the binding quality authority.
- New dependencies need justification in the PR description.

Module-level invariants live in per-directory `CLAUDE.md` files (for
example `src/wal/CLAUDE.md`) — read the one for the module you are editing.

## Format changes

Any persisted-format change must bump the corresponding row in the
[`src/format.rs` registry](src/format.rs), add an explicit decoder arm for the
new accepted version, and add or update its golden fixture in the compatibility
corpus under `tests/fixtures/artifacts/`. Drive the fixture decision from
`FORMATS`, regenerate it twice, and require byte-identical output before
submitting. Never reinterpret an unknown version as an older layout.
