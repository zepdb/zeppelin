# Zeppelin Legitimization Report

Date: 2026-07-01
Branch: `fable/legitimize-repo` (from `main` @ d45d236)
Plan: `cleanup-zeppelin.md`

## Environment

- `rustc 1.93.0 (254b59607 2026-01-19)` / `cargo 1.93.0`
- `cargo-deny 0.19.9`, `cargo-audit 0.22.2`, `cargo-machete 0.9.2`
- Docker 27.5.1; MinIO via `docker-compose.test.yml` on `localhost:9000`

## How this pass was run

A prior session left ~19 files modified but uncommitted on `main`, with a report
claiming full validation. This pass treated those claims as suspect: every
success criterion was independently re-executed, the live service was exercised
over HTTP against MinIO (including failure injection), and additional
docs/hardening gaps were found and fixed. Work is committed as three logical
checkpoints on `fable/legitimize-repo` (not pushed).

## Baseline findings (inherited, verified fixed)

- `S3_ALLOW_HTTP` was parsed but dropped when `ClientOptions` was rebuilt,
  breaking MinIO HTTP endpoints (`src/storage/store.rs`).
- The test harness implicitly loaded the local ignored `.env`, leaking
  developer S3 settings into test runs. It now defaults to the in-memory
  backend and supports explicit `memory` / `local` / `s3` / `minio`.
- The IVF recall property asserted an unstable threshold; replaced with a
  deterministic single-probe self-recall property.
- CI's MinIO job ran only `storage_tests`; now runs the full `--tests` suite.
- `deny.toml` used the pre-0.19 cargo-deny schema.
- README/SKILL/OpenAPI had drifted from the implementation.

## New findings in this pass (fixed)

1. **Config example drift**: `.env.example` and `zeppelin.toml.example`
   documented `max_batch_size = 10000` and `max_request_body_mb = 50`; actual
   defaults in `src/config.rs` are `50_000` and `512`. Corrected.
2. **OpenAPI missing 415/422**: axum's `Json` extractor returns plain-text
   `415` (missing/wrong Content-Type) and `422` (well-formed JSON that fails
   deserialization) on the create-namespace / upsert / delete-vectors paths;
   the spec only documented JSON `400`s. Added `UnsupportedMediaType` and
   `UnprocessableEntity` response components. (The query path deserializes
   manually and returns JSON `400` for everything — verified live.)
3. **SKILL.md Embed API framing**: implied the Embed API might not be part of
   this repo; it lives in `embed_service/` (FastAPI, all-MiniLM-L6-v2). Reworded.
4. **Unused direct dependencies**: `tower` and `url` had no source references
   (cargo-machete + grep verified). Removed; lockfile shrank accordingly.

## Validation results (all executed this pass)

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo check --all-targets` | PASS |
| `cargo clippy --all-targets -- -D warnings` | PASS |
| `cargo test --lib` | PASS — 239 tests |
| `cargo test --tests` (default in-memory backend) | PASS — 519 total across 30 binaries |
| `TEST_BACKEND=minio cargo test --tests` | PASS — 519 total; 7 of 8 full runs clean, one run had a single non-reproducing failure (see Remaining #9) |
| `docker build -t zeppelin:legitimize .` | PASS |
| `cargo deny check` | PASS (advisories/bans/licenses/sources ok) |
| `cargo audit` | PASS — 4 allowed warnings (below) |
| Live HTTP lifecycle vs MinIO | PASS (details below) |

### Live runtime validation (release binary, MinIO backend)

Happy path: `healthz` 200 → `readyz` 200 (`s3_connected:true`) → create
namespace 201 (server-generated UUID + warning; client-supplied `name`
ignored) → upsert 3 vectors 200 → strong query returns correct
cosine-distance ordering → delete vector 204 → strong query no longer returns
it → filtered query (`{"op":"eq"}`) works → delete namespace 204 → get 404.

BM25: namespace created with `full_text_search: {"text": {}}` → BM25
`rank_by: ["text","BM25","quick fox"]` returns the matching doc with a
positive score. Combining `vector` and `rank_by` → 400. Note: FTS fields
must be configured at namespace creation; unknown request fields (e.g. a
guessed `schema` key) are silently ignored — see Remaining Risks #4.

Compaction: with `ZEPPELIN_MAX_WAL_FRAGMENTS=3` and a 5s interval, 5 WAL
fragments compacted into 1 segment and queries transitioned from
`scanned_fragments:5, scanned_segments:0` to `frags:0, segs:1` with identical
results. (Default threshold is 1000 fragments, so compaction correctly does
not fire in small-scale smoke tests.)

Failure injection: with an unreachable S3 endpoint the server starts, logs a
startup scan warning, `healthz` stays 200, and `readyz` returns 503
`{"s3_connected":false,"status":"not_ready"}` after object_store's ~5s retry
budget (10 retries).

### Hardening probes (all verified live)

| Probe | Result |
| --- | --- |
| Dimension mismatch (upsert & query) | 400, clear message |
| `top_k = 0` / `top_k = 999999` | 400 (`> 0`, `<= 10000`) |
| `nprobe = 100000` | 400 (max 128) |
| Empty vector id / empty `vectors` / empty `ids` | 400 |
| Vector id > 1024 chars | 400 |
| Malformed JSON (upsert / query) | 400 |
| `null` in vector array | 400 (query path), typed serde error |
| Missing namespace (get/delete/upsert/query/delete-vectors) | 404 |
| `GET /v1/namespaces` (list) | 405 — disabled as intended |
| `dimensions: 0` / `dimensions: 70000` | 400 (1..=65536) |
| Duplicate ids in one upsert | accepted; last-write-wins on query |
| Re-upsert same id | overwrites vector + attributes (spec-conformant) |
| Unknown filter `op` | 400 listing the 11 valid operators |
| `rank_by` wrong arity / unknown algorithm | 400 |
| Neither `vector` nor `rank_by` | 400 |
| 8 concurrent single-vector upserts, one namespace | all 8 durable (CAS manifest) |
| Control chars in `x-request-id` header | 400 from hyper; server unaffected |
| 8KB `x-request-id` | handled, server unaffected |

## cargo audit — allowed warnings (documented in deny.toml)

- `bincode 1.3.3` unmaintained (RUSTSEC-2025-0141) — direct dep; persisted-format migration required to replace.
- `rustls-pemfile 2.2.0` unmaintained (RUSTSEC-2025-0134) — transitive via `object_store 0.11`.
- `anyhow 1.0.101` unsound `downcast_mut` (RUSTSEC-2026-0190) — only via optional `pprof` profiling feature; not in default build.
- `memmap2 0.9.9` unsound (RUSTSEC-2026-0186) — same optional `pprof` path.

## Commits on this branch

1. `b6a7487` — Fix S3_ALLOW_HTTP handling, update deps (prometheus 0.14,
   pprof 0.15), migrate deny.toml, remove unused `tower`/`url`.
2. `d3e29b5` — Test harness multi-backend support, deterministic IVF recall
   property, local-backend CAS coverage, rate-limit test config.
3. `a3991d3` — README/SKILL/OpenAPI/CI/config-example alignment, including
   new 415/422 spec responses and corrected config defaults.

Untracked pre-existing artifacts (`REPORT.md`, `result_*.txt`, `states/`)
were left untouched; they are prior analysis outputs, not part of this change.
`cleanup-zeppelin.md` and this report are intentionally untracked deliverables
unless you want them committed.

## Remaining known issues (prioritized)

1. **bincode 1.x migration** (medium): unmaintained; any type in its
   serialization tree must avoid `#[serde(untagged)]`/`skip_serializing_if`.
   Migrate persisted formats to the MessagePack path already used by WAL/manifest.
2. **415/422 are plain text** (low): axum `Json` rejections don't match the
   JSON `ErrorResponse` envelope. Now documented in the spec; if a uniform
   envelope is wanted, add a custom extractor (the query path already does this).
3. **Legacy dead handler** (low): `create_namespace_with_name` +
   `CreateNamespaceWithNameRequest` are `#[allow(dead_code)]`, explicitly kept
   for rollback of the UUID-namespace change. Remove once UUID naming is settled.
4. **Unknown request fields silently ignored** (low): serde default behavior;
   e.g. a typo'd `full_text_search` key yields a namespace without FTS.
   Consider `deny_unknown_fields` on request DTOs — an API-behavior change,
   so it needs its own PR and spec note.
5. **`prometheus 0.13→0.14` transitive duplicates** (cleanup): cargo-deny
   warns about duplicate crate versions (non-blocking).
6. **AWS S3 not exercised** (info): validation used MinIO only. Real-S3
   behavior (`S3ConditionalPut::ETagMatch` CAS) is covered by tests when
   `TEST_BACKEND=s3` with real credentials.
7. **Rust version policy** (info): `rust-version = "1.85"`, Dockerfile pins
   `rust:1.85-bookworm`, CI uses `stable`. A `1.85`-pinned CI job would prove
   the MSRV claim; today it is asserted, not enforced.
8. **Long-running gates not run** (info): Miri, Loom, fuzzing, mutation
   testing, benchmarks remain follow-up work (nightly/scheduled CI candidates).
9. **One-in-eight MinIO flake** (low): a single test failed once during 8
   full MinIO-backed suite runs and never reproduced (7 subsequent
   `--no-fail-fast` runs were clean). Project notes identify
   `test_active_queries_returns_to_zero` as sensitive to Prometheus metrics
   shared across concurrently running test binaries; it passes in isolation.
   Fixing it properly means per-test metric registries — a follow-up PR.

## Recommended next PRs

1. Migrate persisted binary formats off `bincode 1.x`.
2. Uniform JSON error envelope for 415/422 (custom `Json` extractor).
3. MSRV check job (`dtolnay/rust-toolchain@1.85`) in CI.
4. Remove the legacy named-namespace handler after the UUID rollout settles.
5. Scheduled CI job for stress/fuzz/bench MinIO-backed runs.

## Assumptions

- The UUID-namespace + disabled-list-endpoint design (commit 5e779e...5e7b) is
  intentional security posture; docs were aligned to it rather than reverted.
- `REPORT.md`, `result_*.txt`, `states/` are TLA+/review artifacts the owner
  wants kept; nothing was deleted.
- MinIO parity is an acceptable stand-in for S3 in local validation.
