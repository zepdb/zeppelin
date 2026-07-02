# Zeppelin — Project Rules

## What is this?
Zeppelin is an S3-native vector search engine. Object storage is the source of truth. Nodes are stateless. IVF-Flat indexing gives us 2 sequential S3 roundtrips per query.

## Architecture Rules

1. **No fallbacks.** Code should crash explicitly on errors. No silent degradation, no swallowing errors, no default values for things that should be configured. If something fails, let it fail loud.

2. **S3 is the source of truth.** Never trust local state over S3 state. The manifest on S3 is always authoritative. Local cache is disposable.

3. **Immutable artifacts.** WAL fragments and segments are write-once. Never modify them in place. The manifest tracks what exists.

4. **Single writer per namespace.** No distributed coordination for v1. One process writes to a namespace at a time. S3 read-after-write consistency handles the rest.

5. **Let the compiler help.** Use strong types. Prefer newtypes over raw strings/numbers. Make invalid states unrepresentable.

## Coding Style

- Use `thiserror` for error types. Every module gets its own error variant in `ZeppelinError`.
- Use `tracing` for logging. Structured fields, not format strings.
- Async everywhere — `tokio` runtime. No blocking calls on async threads.
- Tests hit real object storage (S3 or MinIO). No mocks for storage operations.
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

## Testing

- All tests use `TestHarness` from `tests/common/harness.rs`.
- Default test backend is real S3 (set `TEST_BACKEND=minio` for MinIO).
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
3. **S3 keys and URL paths have different rules.** S3 keys allow `/`; URL `:param` segments do not. Use separate helpers (`key()` for S3, `api_ns()` for URLs) and consider validating namespace names at creation time.
4. **Never compute checksums from non-deterministic serialization.** `HashMap` iteration order is not stable across JSON round-trips. Canonicalize via `BTreeMap` before hashing.
5. **`random_vectors()` reuses IDs across calls.** When testing dedup/merge, use unique ID prefixes per fragment.
6. **Keep `TempDir` alive for the lifetime of anything using its path.** `TempDir::drop()` deletes the directory. Return it from setup functions so callers hold the handle.
7. **Use unique temp filenames for atomic writes under concurrency.** `{file}.{uuid}.tmp` avoids races when multiple tasks write to the same cache key.
8. **Enable `S3ConditionalPut::ETagMatch` in the S3 builder.** Without it, `put_opts` with `PutMode::Update` returns `NotImplemented` — CAS is silently broken. Always set `.with_conditional_put(S3ConditionalPut::ETagMatch)`.
9. **Two-layer defense for distributed writes: fencing check + CAS.** Neither alone prevents zombie writes. Fencing has a TOCTOU gap; CAS alone doesn't detect stale tokens. Both layers are essential.
10. **Lease release must be best-effort.** A process whose lease expired and was taken over must handle release gracefully (Ok or non-fatal error), never block or deadlock.
