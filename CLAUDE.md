# Zeppelin — Project Rules

## What is this?
Zeppelin is an S3-native vector search engine. Object storage is the source of truth. Nodes are stateless. IVF-Flat indexing gives us 2 sequential S3 roundtrips per query.

## Architecture Rules

1. **No fallbacks.** Code should crash explicitly on errors. No silent degradation, no swallowing errors, no default values for things that should be configured. If something fails, let it fail loud.

2. **S3 is the source of truth.** Never trust local state over S3 state. The manifest on S3 is always authoritative. Local cache is disposable.

3. **Immutable artifacts.** WAL fragments and segments are write-once. Never modify them in place. The manifest tracks what exists.

4. **Single writer per namespace.** One process handles a namespace's write path at a time: HTTP vector writes are unfenced, and each `WalWriter` keeps its group-commit queue and committed-manifest memo process-local (`WalWriter::append`, `WalWriter::group`, `WalWriter::commit_pending_group`). Manifest CAS is the cross-process safety boundary, not permission to round-robin writes. Strong reads are not part of this affinity rule because `read_manifest_for_query` remotely verifies the manifest; `strong_query_within_ttl_observes_manifest_advanced_on_s3` covers cross-node freshness. See README "Running more than one node."

5. **Let the compiler help.** Use strong types. Prefer newtypes over raw strings/numbers. Make invalid states unrepresentable.

## Coding Style

- Use `thiserror` for error types. Every module gets its own error variant in `ZeppelinError`.
- Use `tracing` for logging. Structured fields, not format strings.
- Async everywhere — `tokio` runtime. No blocking calls on async threads.
- Storage tests use concrete `object_store` backends, including the default
  in-memory backend. Use MinIO, GCS, or Azurite when substrate semantics matter;
  do not replace storage operations with mocks.
- `#[must_use]` on functions that return values that shouldn't be ignored.
- Prefer `bytes::Bytes` for data passing between layers.

## File Organization

- `src/storage/` — object_store wrapper. Nothing above this layer touches object_store directly.
- `src/wal/` — write-ahead log. Fragment serialization, manifest management.
- `src/namespace/` — namespace CRUD and metadata.
- `src/index/` — vector indexing. Trait-based, IVF-Flat is the v1 implementation.
- `src/cache/` — local disk cache. LRU eviction, pinned centroids.
- `src/compaction/` — background WAL → segment compaction.
- `src/server/` — axum HTTP handlers. Thin layer over domain logic.
- `src/security/` — kernel, policy, audit. Fail-closed.
- `src/fts/` — BM25 lexical retrieval.
- `src/format.rs` and `src/format/` — persisted-format registry, decoder/version coverage, and golden-corpus fixture generation.

### Per-module guides

Each listed directory has its own `CLAUDE.md` with the traps and invariants that
module's rustdoc doesn't cover. The standalone format registry is included
directly because it is the authority for persisted wire formats. **Read the
relevant entry before you start** — they exist to stop rediscovery of bugs
already paid for.

| Module | Read it for |
| --- | --- |
| [`src/storage/CLAUDE.md`](src/storage/CLAUDE.md) | backend CAS capabilities, owned-key classification |
| [`src/wal/CLAUDE.md`](src/wal/CLAUDE.md) | MessagePack rules, checksum canonicalization, artifact origins |
| [`src/namespace/CLAUDE.md`](src/namespace/CLAUDE.md) | governed deletion, the branch graph, `/readyz` cost |
| [`src/index/CLAUDE.md`](src/index/CLAUDE.md) | the recall gate, IVF defaults, quantization findings |
| [`src/compaction/CLAUDE.md`](src/compaction/CLAUDE.md) | GC ownership, branch materialization cost |
| [`src/cache/CLAUDE.md`](src/cache/CLAUDE.md) | disposability, hydration's branch-safety contract |
| [`src/fts/CLAUDE.md`](src/fts/CLAUDE.md) | `bm25_term_score` arg order, the two index shapes, tokenizer rules |
| [`src/server/CLAUDE.md`](src/server/CLAUDE.md) | axum 0.7 syntax, router split, gated routes |
| [`src/security/CLAUDE.md`](src/security/CLAUDE.md) | configured composition, the policy publication lease |
| [`src/format.rs`](src/format.rs) | persisted families, accepted versions, decoder probes, golden-fixture coverage |
| [`tests/CLAUDE.md`](tests/CLAUDE.md) | `TEST_BACKEND`, MinIO setup, known-flaky list |

Size hints for orientation: `wal/manifest.rs` (~12.5k), `compaction/gc.rs`
(~7.6k), `index/ivf_flat/{build,search}.rs` (~6.7k/~6.0k),
`namespace/graph.rs` (~5.9k), `server/handlers/query.rs` and
`compaction/mod.rs` (~5.8k each), `config.rs` and `query.rs` (~5.3k/~5.2k),
and the central `format.rs` registry (~1.4k). Read their rustdoc headers before
opening a large file wholesale.

## Build and test quickstart

```bash
cargo check --tests --features branching-test-support   # ~9 min cold
cargo test --lib                                        # fast; green on default memory backend
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

Integration tests default to the concrete in-memory object store. Use `minio`
for anything CAS-, concurrency-, or origin-shaped, and
`gcs` (patched fake-gcs-server) / `azurite` for the non-S3 substrates. MinIO
and both emulators run natively without Docker — see
[`tests/CLAUDE.md`](tests/CLAUDE.md) and `scripts/emulators/README.md`.

`cargo test --lib` is green with `TEST_BACKEND` unset. The two
`security::policy_publication` tests construct their own in-memory CAS store.
`startup::rbac_config_boot_enables_rbac_routes` still requires MinIO and skips
when `TEST_BACKEND` is not `minio` because its startup configuration otherwise
uses the local backend, which has no ETag CAS.

## Where the plans live

`tasks/` is gitignored and holds local executable plans and evidence, not
scratch notes. The 2026-08-15 backlog is under `tasks/pending/`: plans 01, 04,
and 07–19 are archived in `done/`; plans 02, 03, 05, and 06 are explicitly
deferred until the pinned wikidpr datasets and their dependencies are
available; plan 20 is the final runnable guide correction in that pass. The
unexecuted August 12 performance plans live in
`tasks/optimizations_august_12/`. `tasks/security/` contains phase evidence,
not phase plans. Do not cite tokenizer, storage-format, or memoization plan
directories: none exists in this checkout, and no in-repo archive is recorded.

The multi-substrate track (`tasks/multi-substrate/`) is **executed**: GCS and
Azure transports are in and gated on emulators, with `08-release-evidence.md`
as the evidence ledger. `tasks/learnings.md` is the running bug/pattern log —
append to it.

Branching specifically: `tasks/branching/` currently contains only
`10-release-evidence.md`, which is an **implementation ledger, not a release
approval**. Branching is default-disabled. Focused MinIO suites have run, but
the full fault campaign, deterministic smoke/replay, green performance census,
both-dataset recall gate, TLA reruns, independent reviews, and soak are not
complete.

## Testing

- Integration tests use `TestHarness` from `tests/common/harness.rs`.
- Default test backend is `memory`; set `TEST_BACKEND=minio` for MinIO.
- Each test gets a random prefix for isolation.
- Tests clean up after themselves (drop impl on TestHarness).
- Use `tests/common/vectors.rs` for generating test data.
- Use `tests/common/assertions.rs` for verifying S3 state.

## Dependencies

Only add dependencies listed in the plan. If you need something new, justify it.

## Learnings

See `tasks/learnings.md` (local, gitignored) for the full list of bugs encountered and patterns to avoid — append new learnings there as work proceeds. Key rules:

1. **No bincode with `#[serde(untagged)]` or `#[serde(skip_serializing_if)]`.** Any type in the serialization tree with these attributes must use a self-describing format (JSON, MessagePack, CBOR). Check nested types, not just top-level structs.
2. **Check framework syntax against the pinned version.** Axum 0.7 uses `:param`, axum 0.8 uses `{param}`. If parameterized routes 404 but static routes work, suspect syntax mismatch.
3. **S3 keys and URL paths have different rules.** S3 keys allow `/`; URL `:param` segments do not. Use separate helpers (`object_store_key()` for storage, `api_ns()` for URLs) and consider validating namespace names at creation time.
4. **Never compute checksums from non-deterministic serialization.** `HashMap` iteration order is not stable across JSON round-trips. Canonicalize via `BTreeMap` before hashing.
5. **`random_vectors()` reuses IDs across calls.** When testing dedup/merge, use unique ID prefixes per fragment.
6. **Keep `TempDir` alive for the lifetime of anything using its path.** `TempDir::drop()` deletes the directory. Return it from setup functions so callers hold the handle.
7. **Use unique temp filenames for atomic writes under concurrency.** `{file}.{uuid}.tmp` avoids races when multiple tasks write to the same cache key.
8. **Enable `S3ConditionalPut::ETagMatch` in the S3 builder.** Without it, `put_opts` with `PutMode::Update` returns `NotImplemented` — CAS is silently broken. Always set `.with_conditional_put(S3ConditionalPut::ETagMatch)`.
9. **Two-layer defense for distributed writes: fencing check + CAS.** Neither alone prevents zombie writes. Fencing has a TOCTOU gap; CAS alone doesn't detect stale tokens. Both layers are essential.
10. **Lease release must be best-effort.** A process whose lease expired and was taken over must handle release gracefully (Ok or non-fatal error), never block or deadlock.
11. **The pinned IVF recall gate is the quality authority.** Changes to flat
    partitioning or omitted-probe policy must run
    `tests/ivf_recall_gate.rs` against both `wikidpr1m` and `wikidpr2m`.
    Offline evaluators must call the production `partition_vectors` seam, not
    carry an independent IVF implementation.
12. **Flat IVF defaults are scale-aware and no-spill.** Resolve nlist from a
    3,000-row target within the default 256–4096 bounds, and resolve an
    omitted nprobe as 3/16 of the active flat segment's clusters with a
    runtime floor of 32. Every logical row has exactly one stored location;
    do not add duplicate-row spill or query dedup without a new measured plan.
